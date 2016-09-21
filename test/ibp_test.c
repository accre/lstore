/*
   Copyright 2016 Vanderbilt University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#define _log_module_index 140

#include <apr.h>
#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <gop/gop.h>
#include <tbx/apr_wrapper.h>
#include <tbx/assert_result.h>
#include <tbx/fmttypes.h>
#include <tbx/log.h>
#include <tbx/random.h>
#include <tbx/type_malloc.h>
#include <ibp/ibp.h>
#include "iovec_sync.h"
#include "ibp_sync.h"
#include "ibp_errno.h"

#define A_DURATION 900

IBP_DptInfo depotinfo;
struct ibp_depot *src_depot_list;
struct ibp_depot *dest_depot_list;
int src_n_depots;
int dest_n_depots;
int ibp_timeout;
int sync_transfer;
int failed_tests;
int disk_cs_type = CHKSUM_DEFAULT;
ibp_off_t disk_blocksize = 0;

typedef struct {
    char *buffer;
    int size;
    int pos;
    int nbytes;
    tbx_iovec_t v;
} rw_arg_t;

tbx_ns_chksum_t *ncs;
ibp_context_t *ic = NULL;

//*************************************************************************
// base_async_test - simple single allocation test of the async routines
//*************************************************************************

void base_async_test(ibp_depot_t *depot)
{
//  int size = 115;
//  int block = 10;
    int size = 10*1024*1024;
    int block = 5000;
//  char buffer[size+1];
//  char buffer_cmp[size+1];
    char *buffer, *buffer_cmp;
    char c;
    char block_buf[block+1];
    tbx_tbuf_t *buf[block+1];
    ibp_attributes_t attr;
    ibp_capset_t caps;
    ibp_cap_t *cap;
    int err, i, offset, bcount, remainder;
    apr_time_t start_time, end_time;
    double dt;
    gop_op_generic_t *op;
    gop_opque_t *q;

//printf("Skipping base_async_test. ########################################################\n");
//return;

    for (i=0; i<block+1; i++) buf[i] = tbx_tbuf_create();

    buffer = (char *)malloc(size+1);
    buffer_cmp = (char *)malloc(size+1);
    assert(buffer != NULL);
    assert(buffer_cmp != NULL);

    printf("base_async_test:  Starting simple test\n");
    fflush(stdout);

    //** Create the list for handling the commands
    q = gop_opque_new();
    opque_start_execution(q);  //** and start executing the commands

    //** Create the allocation used for test
    ibp_attributes_set(&attr, time(NULL) + A_DURATION, IBP_HARD, IBP_BYTEARRAY);
    op = ibp_alloc_gop(ic, &caps, size, depot, &attr, disk_cs_type, disk_blocksize, ibp_timeout);
    gop_opque_add(q, op);
    err = opque_waitall(q);
    printf("base_async_test: after allocate: nfinished=%d\n", gop_opque_tasks_finished(q));
    if (err != OP_STATE_SUCCESS) {
        printf("base_async_test: ibp_allocate error! * ibp_errno=%d\n", err);
        abort();
    }

    printf("base_async_test: rcap=%s\n", ibp_cap_get(&caps, IBP_READCAP));
    printf("base_async_test: wcap=%s\n", ibp_cap_get(&caps, IBP_WRITECAP));
    printf("base_async_test: mcap=%s\n", ibp_cap_get(&caps, IBP_MANAGECAP));

    //** Init the buffers
    buffer[size] = '\0';
    memset(buffer, '*', size);
    buffer_cmp[size] = '\0';
    memset(buffer_cmp, '_', size);
    block_buf[block] = '\0';
    memset(block_buf, '0', block);

    //-------------------------------
    //** Do the initial upload
//  op = ibp_write_gop(ic, ibp_cap_get(&caps, IBP_WRITECAP), 0, size, buffer_cmp, ibp_timeout);
//  gop_opque_add(q, op);
//  err = opque_waitall(q);
//  if (err != OP_STATE_SUCCESS) {
//     printf("base_async_test: Initial ibp_write error! * ibp_errno=%d\n", err);
//     abort();
//  }

    bcount = size / (2*block);
    remainder = size - bcount * (2*block);

    //** Now do the striping **
    offset = (bcount-1)*(2*block);      // Now store the data in chunks
    cap = ibp_cap_get(&caps, IBP_WRITECAP);
    for (i=0; i<bcount; i++) {
        c = 'A' + (i%27);
        memset(&(buffer_cmp[offset]), c, 2*block);

        tbx_tbuf_single(buf[i], 2*block, &(buffer_cmp[offset]));
        op = ibp_write_gop(ic, cap, offset, buf[i], 0, 2*block, ibp_timeout);
        gop_opque_add(q, op);

        offset = offset - 2*block;
    }

    if (remainder>0)  {
        offset = bcount*2*block;
        memset(&(buffer_cmp[offset]), '@', remainder);
        tbx_tbuf_single(buf[bcount], remainder, &(buffer_cmp[offset]));
        op = ibp_write_gop(ic, cap, offset, buf[bcount], 0, remainder, ibp_timeout);
        gop_opque_add(q, op);
    }

    //** Now wait for them to complete
    printf("base_async_test: waiting for upload to finish\n");
    fflush(stdout);
    start_time = apr_time_now();
    err = opque_waitall(q);
    end_time = apr_time_now() - start_time;
    dt = apr_time_as_msec(end_time) / 1000.0;
    printf("base_async_test: after upload nfinished=%d dt=%lf\n", gop_opque_tasks_finished(q), dt);
    fflush(stdout);
    if (err != OP_STATE_SUCCESS) {
        printf("base_async_test: Error in stripe write! * ibp_errno=%d\n", err);
        abort();
    }

    //-------------------------------
    bcount = size / block;
    remainder = size % block;

    //** Generate the Read list
    offset = 0;      // Now read the data in chunks
    cap = ibp_cap_get(&caps, IBP_READCAP);
    for (i=0; i<bcount; i++) {
        tbx_tbuf_single(buf[i], block, &(buffer[offset]));
        op = ibp_read_gop(ic, cap, offset, buf[i], 0, block, ibp_timeout);
        gop_opque_add(q, op);

        offset = offset + block;
    }

    if (remainder>0)  {
//printf("read remainder: rem=%d offset=%d\n", remainder, offset);
        tbx_tbuf_single(buf[bcount], remainder, &(buffer[offset]));
        op = ibp_read_gop(ic, cap, offset, buf[bcount], 0, remainder, ibp_timeout);
        gop_opque_add(q, op);
    }

    //** Now wait for them to complete
    printf("base_async_test: waiting for downloads to finish\n");
    fflush(stdout);
    start_time = apr_time_now();
    err = opque_waitall(q);
    end_time = apr_time_now() - start_time;
    dt = apr_time_as_msec(end_time) / 1000.0;
    printf("base_async_test: after download nfinished=%d dt=%lf\n", gop_opque_tasks_finished(q), dt);
    fflush(stdout);

    if (err != OP_STATE_SUCCESS) {
        printf("base_async_test: Error in stripe read! * ibp_errno=%d\n", err);
        abort();
    }

    //-------------------------------

    //** Do the comparison **
    i = strcmp(buffer, buffer_cmp);
    if (i != 0) {
        failed_tests++;
        printf("base_async_test: FAILED! strcmp = %d\n", i);
    }

//  printf("base_async_test: buffer_cmp=%s\n", buffer_cmp);
//  printf("base_async_test:     buffer=%s\n", buffer);
//  printf("base_async_test:block_buffer=%s\n", block_buf);


    //-------------------------------


    //** Remove the allocation **
    op = ibp_remove_gop(ic, ibp_cap_get(&caps, IBP_MANAGECAP), ibp_timeout);
    gop_opque_add(q, op);
    err = opque_waitall(q);
    printf("base_async_test: after remove nfinished=%d\n", gop_opque_tasks_finished(q));
    if (err != OP_STATE_SUCCESS) {
        printf("base_async_test: Error removing the allocation!  ibp_errno=%d\n", err);
        abort();
    }

//  free_oplist(iol);
    opque_finished_submission(q);

//err=log_level();
//set_log_level(20);
    gop_opque_free(q, OP_DESTROY);
//set_log_level(err);

    free(buffer);
    free(buffer_cmp);

    for (i=0; i<block+1; i++) tbx_tbuf_destroy(buf[i]);

    printf("base_async_test: PASSED\n");
    fflush(stdout);

}

//*************************************************************************
// base_tbx_iovec_test - simple single allocation test of the iovec routines
//*************************************************************************

void base_tbx_iovec_test(ibp_depot_t *depot)
{
//  int size = 115;
//  int block = 10;
    int size = 10*1024*1024;
    int block = 5000;
//  int block = 500000;
    char *buffer, *buffer_cmp;
    char c;
    char block_buf[block+1];
    tbx_tbuf_t *buf;
    ibp_tbx_iovec_t *vec;
    ibp_attributes_t attr;
    ibp_capset_t caps;
    ibp_cap_t *cap;
    int err, i, offset, bcount, remainder, n_ele;
    apr_time_t start_time, end_time;
    double dt;
    gop_op_generic_t *op;
    gop_opque_t *q;

//printf("Skipping base_tbx_iovec_test. ########################################################\n");
//return;

    buf = tbx_tbuf_create();
    buffer = (char *)malloc(size+1);
    buffer_cmp = (char *)malloc(size+1);
    assert(buffer != NULL);
    assert(buffer_cmp != NULL);

    //** Make the space for the iovec
    bcount = size / block;
    remainder = size % block;
    n_ele = bcount;
    if (remainder > 0) n_ele++;
    tbx_type_malloc_clear(vec, ibp_tbx_iovec_t, n_ele);

    printf("base_tbx_iovec_test:  Starting simple test\n");
    fflush(stdout);

    //** Create the list for handling the commands
    q = gop_opque_new();
    opque_start_execution(q);  //** and start executing the commands
    //** Create the allocation used for test
    ibp_attributes_set(&attr, A_DURATION, IBP_HARD, IBP_BYTEARRAY);
    op = ibp_alloc_gop(ic, &caps, size, depot, &attr, disk_cs_type, disk_blocksize, ibp_timeout);
    gop_opque_add(q, op);
    err = opque_waitall(q);
    printf("base_tbx_iovec_test: after allocate: nfinished=%d\n", gop_opque_tasks_finished(q));
    if (err != OP_STATE_SUCCESS) {
        printf("base_tbx_iovec_test: ibp_allocate error! * ibp_errno=%d\n", err);
        abort();
    }

    printf("base_tbx_iovec_test: rcap=%s\n", ibp_cap_get(&caps, IBP_READCAP));
    printf("base_tbx_iovec_test: wcap=%s\n", ibp_cap_get(&caps, IBP_WRITECAP));
    printf("base_tbx_iovec_test: mcap=%s\n", ibp_cap_get(&caps, IBP_MANAGECAP));

    //** Init the buffers
    buffer[size] = '\0';
    memset(buffer, '*', size);
    buffer_cmp[size] = '\0';
    memset(buffer_cmp, '_', size);
    block_buf[block] = '\0';
    memset(block_buf, '0', block);

    //-------------------------------
    //** Do the initial upload

    bcount = size / (2*block);
    remainder = size - bcount * (2*block);

    //** Determine the number of elements
    n_ele = bcount;
    if (remainder > 0) n_ele++;

    //** Now do the striping **
    offset = 0;      // Now store the data in chunks
    cap = ibp_cap_get(&caps, IBP_WRITECAP);
    for (i=0; i<bcount; i++) {
        c = 'A' + (i%27);
        memset(&(buffer_cmp[offset]), c, 2*block);

        vec[i].offset = offset;
        vec[i].len = 2*block;

        offset = offset + 2*block;
    }
    if (remainder>0)  {
        memset(&(buffer_cmp[offset]), '@', remainder);
        vec[bcount].offset = offset;
        vec[bcount].len = remainder;
    }

    tbx_tbuf_single(buf, size, buffer_cmp);
    op = ibp_vec_write_gop(ic, cap, n_ele, vec, buf, 0, size, 60);
    gop_opque_add(q, op);

    //** Now wait for them to complete
    printf("base_tbx_iovec_test: waiting for upload to finish\n");
    fflush(stdout);
    start_time = apr_time_now();
    err = opque_waitall(q);
    end_time = apr_time_now() - start_time;
    dt = apr_time_as_msec(end_time) / 1000.0;
    printf("base_tbx_iovec_test: after upload nfinished=%d dt=%lf\n", gop_opque_tasks_finished(q), dt);
    fflush(stdout);
    if (err != OP_STATE_SUCCESS) {
        printf("base_tbx_iovec_test: Error in stripe write! * ibp_errno=%d\n", err);
        abort();
    }

    //-------------------------------
    bcount = size / block;
    remainder = size % block;

    //** Determine the number of elements
    n_ele = bcount;
    if (remainder > 0) n_ele++;

    //** Generate the Read list
    offset = 0;      // Now read the data in chunks
    cap = ibp_cap_get(&caps, IBP_READCAP);
    for (i=0; i<bcount; i++) {
        vec[i].offset = offset;
        vec[i].len = block;

        offset = offset + block;
    }

    if (remainder>0)  {
        vec[bcount].offset = offset;
        vec[bcount].len = remainder;
    }


    tbx_tbuf_single(buf, size, buffer);
    op = ibp_vec_read_gop(ic, cap, n_ele, vec, buf, 0, size, 60);
    gop_opque_add(q, op);

    //** Now wait for them to complete
    printf("base_tbx_iovec_test: waiting for downloads to finish\n");
    fflush(stdout);
    start_time = apr_time_now();
    err = opque_waitall(q);
    end_time = apr_time_now() - start_time;
    dt = apr_time_as_msec(end_time) / 1000.0;
    printf("base_tbx_iovec_test: after download nfinished=%d dt=%lf\n", gop_opque_tasks_finished(q), dt);
    fflush(stdout);
    if (err != OP_STATE_SUCCESS) {
        printf("base_tbx_iovec_test: Error in stripe read! * ibp_errno=%d\n", err);
        abort();
    }

    //-------------------------------

    //** Do the comparison **
    i = strcmp(buffer, buffer_cmp);
    if (i != 0) {
        failed_tests++;
        printf("base_tbx_iovec_test: FAILED! strcmp = %d\n", i);
    }

    //-------------------------------


    //** Remove the allocation **
    op = ibp_remove_gop(ic, ibp_cap_get(&caps, IBP_MANAGECAP), ibp_timeout);
    gop_opque_add(q, op);
    err = opque_waitall(q);
    printf("base_tbx_iovec_test: after remove nfinished=%d\n", gop_opque_tasks_finished(q));
    if (err != OP_STATE_SUCCESS) {
        printf("base_tbx_iovec_test: Error removing the allocation!  ibp_errno=%d\n", err);
        abort();
    }

    opque_finished_submission(q);

//err=log_level();
//set_log_level(20);
    gop_opque_free(q, OP_DESTROY);
//set_log_level(err);

    free(buffer);
    free(buffer_cmp);
    tbx_tbuf_destroy(buf);

    printf("base_tbx_iovec_test: PASSED\n");
    fflush(stdout);
}


//*************************************************************************
// perform_big_alloc_tests - Verifies if the depot supports > 2GB allocations
//*************************************************************************

void perform_big_alloc_tests(ibp_depot_t *depot)
{
    int bufsize = 1024;
    ibp_off_t aoff = 2;
    aoff = (aoff << 30) + 1;
    ibp_off_t asize = aoff + bufsize;
    char buffer[bufsize+1], buffer_cmp[bufsize+1];
    tbx_tbuf_t *buf;
    int err;
    ibp_capstatus_t astat;
    ibp_attributes_t attr;
    ibp_capset_t caps;
    ibp_cap_t *cap;
    gop_op_generic_t *op;
    gop_opque_t *q;
    ibp_timer_t timer;
    gop_op_status_t status;

    printf("perform_big_alloc_test:  Starting simple test offset=" I64T "\n", aoff);
    fflush(stdout);

    ibp_timer_set(&timer, ibp_timeout, ibp_timeout);
    buf = tbx_tbuf_create();

    //** Create the list for handling the commands
    q = gop_opque_new();
    opque_start_execution(q);  //** and start executing the commands

    //** Create the allocation used for test
    ibp_attributes_set(&attr, time(NULL) + A_DURATION, IBP_HARD, IBP_BYTEARRAY);
    op = ibp_alloc_gop(ic, &caps, asize, depot, &attr, disk_cs_type, disk_blocksize, ibp_timeout);
    gop_opque_add(q, op);
    err = opque_waitall(q);
    if (err != OP_STATE_SUCCESS) {
        failed_tests++;
        status = gop_get_status(op);
        printf("perform_big_alloc_test: FAILED ibp_allocate error! * nfailed=%d ibp_errno=%d\n", err, status.error_code);
        gop_opque_free(q, OP_DESTROY);
        return;
    }

    printf("perform_big_alloc_test: rcap=%s\n", ibp_cap_get(&caps, IBP_READCAP));
    printf("perform_big_alloc_test: wcap=%s\n", ibp_cap_get(&caps, IBP_WRITECAP));
    printf("perform_big_alloc_test: mcap=%s\n", ibp_cap_get(&caps, IBP_MANAGECAP));

    //** Verify the size
    memset(&astat, 0, sizeof(astat));
    err = IBP_manage(ibp_cap_get(&caps, IBP_MANAGECAP), &timer, IBP_PROBE, 0, &astat);
    if (err == 0) {
        if ((astat.currentSize != 0) || (astat.maxSize != asize)) {
            failed_tests++;
            printf("perform_big_alloc_test: ibp_manage FAILED with initial size/pos!\n");
            printf("perform_big_alloc_test: current size = " I64T " should be 0\n", astat.currentSize);
            printf("perform_big_alloc_test:     max size = " I64T " should be " I64T "\n", astat.maxSize, asize);
        }
    } else {
        gop_opque_free(q, OP_DESTROY);
        failed_tests++;
        printf("perform_big_alloc_test: ibp_manage call 1  FAILED error = %d * ibp_errno=%d\n", err, IBP_errno);
    }

    //** Init the buffers
    buffer[bufsize] = '\0';
    memset(buffer, '*', bufsize);
    buffer_cmp[bufsize] = '\0';
    memset(buffer_cmp, '_', bufsize);

    //** Do the upload
    tbx_tbuf_single(buf, bufsize, buffer_cmp);
    op = ibp_write_gop(ic, ibp_cap_get(&caps, IBP_WRITECAP), aoff, buf, 0, bufsize, ibp_timeout);
    gop_opque_add(q, op);
    err = opque_waitall(q);
    if (err != OP_STATE_SUCCESS) {
        failed_tests++;
        status = gop_get_status(op);
        printf("perform_big_alloc_test: FAILED Initial ibp_write error! * ibp_errno=%d\n", status.error_code);
        gop_opque_free(q, OP_DESTROY);
        return;
    }

    //** Verify the size/data pos
    memset(&astat, 0, sizeof(astat));
    err = IBP_manage(ibp_cap_get(&caps, IBP_MANAGECAP), &timer, IBP_PROBE, 0, &astat);
    if (err == 0) {
        if ((astat.currentSize != asize) || (astat.maxSize != asize)) {
            failed_tests++;
            printf("perform_big_alloc_test: ibp_manage FAILED with final size/pos!\n");
            printf("perform_big_alloc_test: current size = " I64T " should be " I64T "\n", astat.currentSize, asize);
            printf("perform_big_alloc_test:     max size = " I64T " should be " I64T "\n", astat.maxSize, asize);
        }
    } else {
        gop_opque_free(q, OP_DESTROY);
        failed_tests++;
        printf("perform_big_alloc_test: ibp_manage call 2 FAILED error = %d * ibp_errno=%d\n", err, IBP_errno);
    }


    //** Attempt to Read it back
    cap = ibp_cap_get(&caps, IBP_READCAP);
    tbx_tbuf_single(buf, bufsize, buffer);
    op = ibp_read_gop(ic, cap, aoff, buf, 0, bufsize, ibp_timeout);
    gop_opque_add(q, op);
    err = opque_waitall(q);
    if (err != OP_STATE_SUCCESS) {
        failed_tests++;
        status = gop_get_status(op);
        printf("perform_big_alloc_test: FAILED Error in read! * ibp_errno=%d\n", status.error_code);
        gop_opque_free(q, OP_DESTROY);
        return;
    }

    //-------------------------------

    //** Do the comparison **
    err = strcmp(buffer, buffer_cmp);
    if (err == 0) {
        printf("perform_big_alloc_test: Success!\n");
    } else {
        failed_tests++;
        printf("perform_big_alloc_test: FAILED! strcmp = %d\n", err);
        gop_opque_free(q, OP_DESTROY);
        return;
    }



    //** Remove the allocation **
    op = ibp_remove_gop(ic, ibp_cap_get(&caps, IBP_MANAGECAP), ibp_timeout);
    gop_opque_add(q, op);
    err = opque_waitall(q);
    if (err != OP_STATE_SUCCESS) {
        printf("perform_big_alloc_test: Error removing the allocation!  ibp_errno=%d\n", err);
        gop_opque_free(q, OP_DESTROY);
        return;
    }

    opque_finished_submission(q);

    gop_opque_free(q, OP_DESTROY);

    tbx_tbuf_destroy(buf);

    printf("perform_big_alloc_tests: Passed!\n");
}

//*************************************************************************
// perform_manage_truncate_tests - Checks the IBP_CHNG/IBP_TRUNCATE modes
//*************************************************************************

void perform_manage_truncate_tests(ibp_depot_t *depot)
{
    ibp_off_t bufsize = 1024*1024;
    ibp_off_t asize = 2*bufsize;
    ibp_off_t pos;
    char buffer[bufsize+1], buffer_cmp[bufsize+1];
    tbx_tbuf_t *buf;
    int err, dt;
    ibp_capstatus_t astat;
    ibp_capstatus_t astat2;
    ibp_attributes_t attr;
    ibp_capset_t caps;
    ibp_cap_t *cap;
    gop_op_generic_t *op;
    gop_opque_t *q;
    ibp_timer_t timer;
    gop_op_status_t status;

    printf("perform_manage_truncate_test: Starting test\n");
    fflush(stdout);

    buf = tbx_tbuf_create();
    ibp_timer_set(&timer, ibp_timeout, ibp_timeout);

    //** Create the list for handling the commands
    q = gop_opque_new();
    opque_start_execution(q);  //** and start executing the commands

    //** Create the allocation used for test
    ibp_attributes_set(&attr, time(NULL) + A_DURATION, IBP_HARD, IBP_BYTEARRAY);
    op = ibp_alloc_gop(ic, &caps, asize, depot, &attr, disk_cs_type, disk_blocksize, ibp_timeout);
    gop_opque_add(q, op);
    err = opque_waitall(q);
    if (err != OP_STATE_SUCCESS) {
        failed_tests++;
        status = gop_get_status(op);
        printf("perform_manage_truncte_test: FAILED ibp_allocate error! * nfailed=%d ibp_errno=%d\n", err, status.error_code);
        abort();
        gop_opque_free(q, OP_DESTROY);
        return;
    }

    printf("perform_manage_truncate_test: rcap=%s\n", ibp_cap_get(&caps, IBP_READCAP));
    printf("perform_manage_truncate_test: wcap=%s\n", ibp_cap_get(&caps, IBP_WRITECAP));
    printf("perform_manage_truncate_test: mcap=%s\n", ibp_cap_get(&caps, IBP_MANAGECAP));

    //** Init the buffers
    pos = bufsize/3;
    buffer[bufsize] = '\0';
    memset(buffer, '*', bufsize);
    buffer[pos-1] = 'A';
    buffer[pos] = 'R';
    buffer[bufsize-1] = 'T';
    buffer_cmp[bufsize] = '\0';
    memset(buffer_cmp, '_', bufsize);


    //** Do the initial upload
    tbx_tbuf_single(buf, bufsize, buffer);
    op = ibp_write_gop(ic, ibp_cap_get(&caps, IBP_WRITECAP), 0, buf, 0, bufsize, ibp_timeout);
    gop_opque_add(q, op);
    err = opque_waitall(q);
    if (err != OP_STATE_SUCCESS) {
        failed_tests++;
        status = gop_get_status(op);
        printf("perform_big_alloc_test: FAILED Initial ibp_write error! * ibp_errno=%d\n", status.error_code);
        abort();
        gop_opque_free(q, OP_DESTROY);
        return;
    }

    //** Verify the size/data pos
    memset(&astat, 0, sizeof(astat));
    err = IBP_manage(ibp_cap_get(&caps, IBP_MANAGECAP), &timer, IBP_PROBE, 0, &astat);
    if (err == 0) {
        if ((astat.currentSize != bufsize) || (astat.maxSize != asize)) {
            failed_tests++;
            printf("perform_manage_truncate_test: ibp_manage FAILED with initial upload check of size/pos!\n");
            printf("perform_manage_truncate_test: current size = " I64T " should be " I64T "\n", astat.currentSize, bufsize);
            printf("perform_manage_truncate_test:     max size = " I64T " should be " I64T "\n", astat.maxSize, asize);
            abort();
        }
    } else {
        gop_opque_free(q, OP_DESTROY);
        failed_tests++;
        printf("perform_manage_truncate_test: ibp_manage FAILED on initial upload check with error = %d * ibp_errno=%d\n", err, IBP_errno);
        abort();
    }

    //** Now attempt to IBP_CHNG the size which would cause a data loss
//  ibp_attributes_set(&(astat.attrib), time(NULL) + A_DURATION, IBP_HARD, IBP_BYTEARRAY);
    astat2 = astat;
    ibp_attributes_set(&(astat2.attrib), -1, -1, -1);
    astat2.maxSize = bufsize/2;
    astat.maxSize = bufsize/2;
    err = IBP_manage(ibp_cap_get(&caps, IBP_MANAGECAP), &timer, IBP_CHNG, 0, &astat2);
    if (err == 0) {
        failed_tests++;
        printf("perform_manage_truncate_test: Oops!  Successfully used IBP_CHNG to result in data loss! error = %d * ibp_errno=%d\n", err, IBP_errno);
        abort();
    }

    //** Verify the original size/data pos is intact
    memset(&astat2, 0, sizeof(astat2));
    err = IBP_manage(ibp_cap_get(&caps, IBP_MANAGECAP), &timer, IBP_PROBE, 0, &astat2);
    dt = astat.attrib.duration - astat2.attrib.duration;
    if (err == 0) {
        if ((astat2.currentSize != bufsize) || (astat2.maxSize != asize)) {
            failed_tests++;
            printf("perform_manage_truncate_test: ibp_manage FAILED with initial IBP_CHNG check of size/pos!\n");
            printf("perform_manage_truncate_test: current size = " I64T " should be " I64T "\n", astat2.currentSize, bufsize);
            printf("perform_manage_truncate_test:     max size = " I64T " should be " I64T "\n", astat2.maxSize, asize);
            abort();
        } else if ((dt > 10) || (dt < 0) || (astat2.attrib.reliability != IBP_HARD) || (astat2.attrib.type != IBP_BYTEARRAY)) {
            failed_tests++;
            printf("perform_manage_truncate_test: ibp_manage FAILED with initial IBP_CHNG check!\n");
            printf("perform_manage_truncate_test:     duration = %d should be ~%d\n", astat2.attrib.duration, astat.attrib.duration);
            printf("perform_manage_truncate_test:     reliability = %d should be IBP_HARD (%d)\n", astat2.attrib.reliability, IBP_HARD);
            printf("perform_manage_truncate_test:     type = %d should be IBP_BYTE_ARRAY (%d)\n", astat2.attrib.type, IBP_BYTEARRAY);
            abort();
        }
    } else {
        gop_opque_free(q, OP_DESTROY);
        failed_tests++;
        printf("perform_manage_truncate_test: ibp_manage FAILED on initial IBP_CHNG upload check with error = %d * ibp_errno=%d\n", err, IBP_errno);
        abort();
    }

    //** Now shorten the size via IBP_CHNG which should be OK
//  ibp_attributes_set(&(astat.attrib), time(NULL) + A_DURATION, IBP_HARD, IBP_BYTEARRAY);
    astat2 = astat;
    ibp_attributes_set(&(astat2.attrib), -1, -1, -1);
    astat.maxSize = bufsize+1;
    astat2.maxSize = bufsize+1;
    err = IBP_manage(ibp_cap_get(&caps, IBP_MANAGECAP), &timer, IBP_CHNG, 0, &astat2);
    if (err != 0) {
        failed_tests++;
        printf("perform_manage_truncate_test: Cant shrink allocation using IBP_CHNG and no data loss! error = %d * ibp_errno=%d\n", err, IBP_errno);
        fflush(stdout);
        abort();
    }

    //** Verify the new size/data pos is intact
    memset(&astat2, 0, sizeof(astat2));
    err = IBP_manage(ibp_cap_get(&caps, IBP_MANAGECAP), &timer, IBP_PROBE, 0, &astat2);
    dt = astat.attrib.duration - astat2.attrib.duration;
    if (err == 0) {
        if ((astat2.currentSize != bufsize) || (astat2.maxSize != (bufsize+1))) {
            failed_tests++;
            printf("perform_manage_truncate_test: ibp_manage FAILED with 2nd IBP_CHNG check of size/pos!\n");
            printf("perform_manage_truncate_test: current size = " I64T " should be " I64T "\n", astat2.currentSize, bufsize);
            printf("perform_manage_truncate_test:     max size = " I64T " should be " I64T "\n", astat2.maxSize, asize);
            abort();
        } else if ((dt > 10) || (dt < 0) || (astat2.attrib.reliability != IBP_HARD) || (astat2.attrib.type != IBP_BYTEARRAY)) {
            failed_tests++;
            printf("perform_manage_truncate_test: ibp_manage FAILED with initial IBP_CHNG check!\n");
            printf("perform_manage_truncate_test:     duration = %d should be ~%d\n", astat2.attrib.duration, astat.attrib.duration);
            printf("perform_manage_truncate_test:     reliability = %d should be IBP_HARD (%d)\n", astat2.attrib.reliability, IBP_HARD);
            printf("perform_manage_truncate_test:     type = %d should be IBP_BYTE_ARRAY (%d)\n", astat2.attrib.type, IBP_BYTEARRAY);
            abort();
        }
    } else {
        gop_opque_free(q, OP_DESTROY);
        failed_tests++;
        printf("perform_manage_truncate_test: ibp_manage FAILED on 2nd IBP_CHNG upload check with error = %d * ibp_errno=%d\n", err, IBP_errno);
        fflush(stdout);
        abort();
    }


    //** Use IBP_TRUNCATE to shorten the allocation.  This should lose data but is OK
    op = ibp_truncate_gop(ic, ibp_cap_get(&caps, IBP_MANAGECAP), pos, ibp_timeout);
    gop_opque_add(q, op);
    err = opque_waitall(q);
    if (err != OP_STATE_SUCCESS) {
        failed_tests++;
        printf("perform_manage_truncate_test: Cant shrink allocation using IBP_TRUNCATE with data loss! error = %d * ibp_errno=%d\n", err, IBP_errno);
        fflush(stdout);
        abort();
    }

    //** Verify the new size/data pos is intact
    memset(&astat, 0, sizeof(astat));
    err = IBP_manage(ibp_cap_get(&caps, IBP_MANAGECAP), &timer, IBP_PROBE, 0, &astat);
    if (err == 0) {
        if ((astat.currentSize != pos) || (astat.maxSize != pos)) {
            failed_tests++;
            printf("perform_manage_truncate_test: ibp_manage FAILED with IBP_TRUNCATE check of size/pos!\n");
            printf("perform_manage_truncate_test: current size = " I64T " should be " I64T "\n", astat.currentSize, pos);
            printf("perform_manage_truncate_test:     max size = " I64T " should be " I64T "\n", astat.maxSize, pos);
            fflush(stdout);
            abort();
        }
    } else {
        gop_opque_free(q, OP_DESTROY);
        failed_tests++;
        printf("perform_manage_truncate_test: ibp_manage FAILED on IBP_TRUNCATE upload check with error = %d * ibp_errno=%d\n", err, IBP_errno);
        abort();
    }


    //** Attempt to Read it back
    cap = ibp_cap_get(&caps, IBP_READCAP);
    tbx_tbuf_single(buf, pos, buffer_cmp);
    op = ibp_read_gop(ic, ibp_cap_get(&caps, IBP_READCAP), 0, buf, 0, pos, ibp_timeout);
    gop_opque_add(q, op);
    err = opque_waitall(q);
    if (err != OP_STATE_SUCCESS) {
        failed_tests++;
        status = gop_get_status(op);
        printf("perform_manage_truncate_test: FAILED Error in read! * ibp_errno=%d cap=%s\n", status.error_code, cap);
        gop_opque_free(q, OP_DESTROY);
        abort();
    }

    //-------------------------------

    //** Do the comparison **
    err = strncmp(buffer, buffer_cmp, pos);
    if (err == 0) {
        printf("perform_manage_truncate_test: Success!\n");
    } else {
        failed_tests++;
        printf("perform_manage_truncate_test: FAILED! strcmp = %d\n", err);
        gop_opque_free(q, OP_DESTROY);
        abort();
        return;
    }


    //** Remove the allocation **
    op = ibp_remove_gop(ic, ibp_cap_get(&caps, IBP_MANAGECAP), ibp_timeout);
    gop_opque_add(q, op);
    err = opque_waitall(q);
    if (err != OP_STATE_SUCCESS) {
        printf("perform_manage_truncate_test: Error removing the allocation!  ibp_errno=%d\n", err);
        gop_opque_free(q, OP_DESTROY);
        abort();
        return;
    }

    opque_finished_submission(q);

    gop_opque_free(q, OP_DESTROY);

    tbx_tbuf_destroy(buf);

    printf("perform_manage_truncate_tests: Passed!\n");
}

//*********************************************************************************
// my_next_block - My routine for getting data
//*********************************************************************************

int my_next_block(tbx_tbuf_t *tb, size_t pos, tbx_tbuf_var_t *tbv)
{
    rw_arg_t *a = (rw_arg_t *)tb->arg;
    size_t nbytes;
    int n2, p2, n3, btb;

    //** Figure out the bytes.  a->nbytes is the max size to transfer/call
    if ((pos + tbv->nbytes) >= tb->buf.total_bytes) {
        nbytes = tb->buf.total_bytes - pos;
    } else {
        nbytes = a->size - pos;
        if (nbytes > tbv->nbytes) nbytes = tbv->nbytes;
    }

    btb = tb->buf.total_bytes;
    p2 = pos;
    n2 = nbytes;
    n3 = tbv->nbytes;
    log_printf(5, "my_next_block: a->size=%d pos=%d requested_nbytes=%d nbytes=%d buf_total_bytes=%d\n", a->size, p2, n3, n2, btb);

    tbv->nbytes = nbytes;

    if ((int)pos < a->size) {
        tbv->buffer = &(tbv->priv.single);
        tbv->buffer->iov_base = &(a->buffer[pos]);
        tbv->buffer->iov_len = nbytes;
        tbv->n_iov = 1;
    } else {
        tbv->buffer = NULL;
        tbv->n_iov = 0;
        tbv->nbytes = 0;
    }

    return(IBP_OK);
}

//*********************************************************************************
//  perform_user_rw_tests - Perform R/W tests using a user supplied callback
//          function for getting buffer/data
//*********************************************************************************

void perform_user_rw_tests(ibp_depot_t *depot)
{
    int bufsize = 1024*1024;
//  int bufsize = 100;
    char buffer[bufsize], rbuf[bufsize];
    tbx_tbuf_t *buf;
    ibp_attributes_t attr;
    ibp_capset_t caps;
    rw_arg_t rw_arg;
    int err, i, b, nbytes, size;

    buf = tbx_tbuf_create();

    //** Fill the buffer **
    memset(rbuf, 0, bufsize);
    nbytes = 1024;
    for (i=0; i<bufsize; i=i+nbytes) {
        size = nbytes;
        if ((i+nbytes) >= bufsize) size = bufsize - i;
        b = i % 27;
        b = b + 'A';
        memset(&(buffer[i]), b, size);
    }
    buffer[bufsize-1] = '\0';

    //*** Make the allocation used in the tests ***
    ibp_attributes_set(&attr, time(NULL) + A_DURATION, IBP_HARD, IBP_BYTEARRAY);
    err = ibp_sync_command(ibp_alloc_gop(ic, &caps, bufsize, depot, &attr, disk_cs_type, disk_blocksize, ibp_timeout));
    if (err != IBP_OK) {
        failed_tests++;
        printf("perform_user_rw_tests:  FAILED creating initial allocation for tests!! error=%d\n", err);
        return;
    }

    //** Perform the upload **
    rw_arg.buffer = buffer;
    rw_arg.size = bufsize;
    rw_arg.nbytes = nbytes + 1;
    tbx_tbuf_fn(buf, bufsize, (void *)&rw_arg, my_next_block);
    err = ibp_sync_command(ibp_write_gop(ic, ibp_cap_get(&caps, IBP_WRITECAP), 0, buf, 0, bufsize, ibp_timeout));
    if (err != IBP_OK) {
        failed_tests++;
        printf("perform_user_rw_tests: FAILED! Error during upload!  ibp_errno=%d\n", err);
        return;
    }

    //** Now download it **
    rw_arg.buffer = rbuf;
    rw_arg.size = bufsize;
    rw_arg.nbytes = nbytes -1;
    tbx_tbuf_fn(buf, bufsize, (void *)&rw_arg, my_next_block);
    err = ibp_sync_command(ibp_read_gop(ic, ibp_cap_get(&caps, IBP_READCAP), 0, buf, 0, bufsize, ibp_timeout));
    if (err != IBP_OK) {
        failed_tests++;
        printf("perform_user_rw_tests: FAILED Error during upload2!  ibp_errno=%d\n", err);
        return;
    }

    //** Check to see if they are the same ***
    if (strcmp(buffer, rbuf) == 0) {
        printf("perform_user_rw_tests: Success!\n");
    } else {
        failed_tests++;
        printf("perform_user_rw_tests: FAILED!!!!!  buffers differ!\n");
        printf("perform_user_rw_tests: wbuf=%50s\n", buffer);
        printf("perform_user_rw_tests: rbuf=%50s\n", rbuf);
    }

    //** Remove the allocation **
    err = ibp_sync_command(ibp_remove_gop(ic, ibp_cap_get(&caps, IBP_MANAGECAP), ibp_timeout));
    if (err != IBP_OK) {
        printf("perform_user_rw_tests: Error removing the allocation!  ibp_errno=%d\n", err);
        abort();
    }

    tbx_tbuf_destroy(buf);
}

//*********************************************************************************
// perform_splitmerge_tests - Tests the ability to split/merge allocations
//*********************************************************************************

void perform_splitmerge_tests(ibp_depot_t *depot)
{
    int bufsize = 2048;
    char wbuf[bufsize+1], rbuf[bufsize+1];
    ibp_attributes_t attr;
    ibp_capset_t mcaps, caps, caps2;
    ibp_capstatus_t probe;
    int err, max_size, curr_size, dummy;
    ibp_timer_t timer;
    int fstart = failed_tests;

    printf("perform_splitmerge_tests:  Starting tests!\n");

    ibp_timer_set(&timer, ibp_timeout, ibp_timeout);

    //** Initialize the buffers **
    memset(wbuf, '0', sizeof(wbuf));
    wbuf[1023]='1';
    wbuf[1024]='2';
    wbuf[bufsize] = '\0';
    memset(rbuf, 0, sizeof(rbuf));

    //*** Make the allocation used in the tests ***
    ibp_attributes_set(&attr, time(NULL) + A_DURATION, IBP_HARD, IBP_BYTEARRAY);
    err = ibp_sync_command(ibp_alloc_gop(ic, &mcaps, bufsize, depot, &attr, disk_cs_type, disk_blocksize, ibp_timeout));
    if (err != IBP_OK) {
        failed_tests++;
        printf("perform_splitmerge_tests: FAILED Error creating initial allocation for tests!! error=%d\n", err);
        return;
    }

    printf("perform_splitmerge_tests: Split-Merge master cap..............\n");
    printf("perform_splitmerge_tests: Read: %s\n", mcaps.readCap);
    printf("perform_splitmerge_tests: Write: %s\n", mcaps.writeCap);
    printf("perform_splitmerge_tests: Manage: %s\n", mcaps.manageCap);

    //** Fill the master with data **
    err = IBP_store(ibp_cap_get(&mcaps, IBP_WRITECAP), &timer, wbuf, bufsize);
    if (err != bufsize) {
        failed_tests++;
        printf("perform_splitmerge_tests: FAILED Error with master IBP_store! wrote=%d err=%d\n", err, IBP_errno);
    }

    //** Split the allocation
    err = ibp_sync_command(ibp_split_alloc_gop(ic, ibp_cap_get(&mcaps, IBP_MANAGECAP), &caps, 1024, &attr, disk_cs_type, disk_blocksize, ibp_timeout));
    if (err != IBP_OK) {
        failed_tests++;
        printf("perform_splitmerge_tests:  FAILED Error creating child allocation for tests!! error=%d\n", err);
        return;
    }

    //** Check the new size of the master
    err = ibp_sync_command(ibp_probe_gop(ic, ibp_cap_get(&mcaps, IBP_MANAGECAP), &probe, ibp_timeout));
    if (err != IBP_OK) {
        failed_tests++;
        printf("perform_splitmerge_tests:  FAILED Error probing master allocation for tests!! error=%d\n", err);
        return;
    }

    ibp_cap_getstatus(&probe, &dummy, &dummy, &curr_size, &max_size, &attr);
    if ((curr_size != 1024) && (max_size != 1024)) {
        failed_tests++;
        printf("perform_splitmerge_tests:  FAILED Error with master allocation size!! curr_size=%d * max_size=%d should both be 1024\n", curr_size, max_size);
        return;
    }

    //** Check the size of the child allocation
    err = ibp_sync_command(ibp_probe_gop(ic, ibp_cap_get(&caps, IBP_MANAGECAP), &probe, ibp_timeout));
    if (err != IBP_OK) {
        failed_tests++;
        printf("perform_splitmerge_tests:  FAILED Error probing child allocation for tests!! error=%d\n", err);
        return;
    }

    ibp_cap_getstatus(&probe, &dummy, &dummy, &curr_size, &max_size, &attr);
    if ((curr_size != 0) && (max_size != 1024)) {
        failed_tests++;
        printf("perform_splitmerge_tests:  FAILED Error with master allocation size!! curr_size=%d * max_size=%d should be 0 and 1024\n", curr_size, max_size);
        return;
    }

    //** Verify the master data
    wbuf[1024] = '\0';
    err = IBP_load(ibp_cap_get(&mcaps, IBP_READCAP), &timer, rbuf, 1024, 0);
    if (err == 1024) {
        if (strcmp(rbuf, wbuf) != 0) {
            failed_tests++;
            printf("FAILED Read some data with the mastercap but it wasn't correct!\n");
            printf("Original=%s\n", wbuf);
            printf("     Got=%s\n", rbuf);
        }
    } else {
        failed_tests++;
        printf("Oops! FAILED reading master cap! err=%d\n", err);
    }

    //** Load data into the child
    err = IBP_store(ibp_cap_get(&caps, IBP_WRITECAP), &timer, wbuf, 1024);
    if (err != 1024) {
        failed_tests++;
        printf("perform_splitmerge_tests: FAILED Error with child IBP_store! wrote=%d err=%d\n", err, IBP_errno);
    }

    //** Read it back
    memset(rbuf, 0, sizeof(rbuf));
    err = IBP_load(ibp_cap_get(&caps, IBP_READCAP), &timer, rbuf, 1024, 0);
    if (err == 1024) {
        if (strcmp(rbuf, wbuf) != 0) {
            failed_tests++;
            printf("FAILED Read some data with the childcap but it wasn't correct!\n");
            printf("Original=%s\n", wbuf);
            printf("     Got=%s\n", rbuf);
        }
    } else {
        failed_tests++;
        printf("Oops! FAILED reading child cap! err=%d\n", err);
    }

    //** Split the master again but htis time make it to big so it should fail
    err = ibp_sync_command(ibp_split_alloc_gop(ic, ibp_cap_get(&mcaps, IBP_MANAGECAP), &caps2, 2048, &attr, disk_cs_type, disk_blocksize, ibp_timeout));
    if (err == IBP_OK) {
        failed_tests++;
        printf("perform_splitmerge_tests: FAILED  Error created child allocation when I shouldn't have! error=%d\n", err);
        return;
    }

    //** Check the size of the master to make sure it didn't change
    err = ibp_sync_command(ibp_probe_gop(ic, ibp_cap_get(&mcaps, IBP_MANAGECAP), &probe, ibp_timeout));
    if (err != IBP_OK) {
        failed_tests++;
        printf("perform_splitmerge_tests:  FAILED Error probing master allocation2 for tests!! error=%d\n", err);
        return;
    }

    ibp_cap_getstatus(&probe, &dummy, &dummy, &curr_size, &max_size, &attr);
    if ((curr_size != 1024) && (max_size != 1024)) {
        failed_tests++;
        printf("perform_splitmerge_tests: FAILED  Error with master allocation size2!! curr_size=%d * max_size=%d should both be 1024\n", curr_size, max_size);
        return;
    }

//GOOD!!!!!!!!!!!!!!!!!

    //** Merge the 2 allocations
    err = ibp_sync_command(ibp_merge_alloc_gop(ic, ibp_cap_get(&mcaps, IBP_MANAGECAP), ibp_cap_get(&caps, IBP_MANAGECAP), ibp_timeout));
    if (err != IBP_OK) {
        failed_tests++;
        printf("perform_splitmerge_tests: FAILED Error with merge! error=%d\n", err);
        return;
    }

    //** Verify the child is gone
    err = ibp_sync_command(ibp_probe_gop(ic, ibp_cap_get(&caps, IBP_MANAGECAP), &probe, ibp_timeout));
    if (err == IBP_OK) {
        failed_tests++;
        printf("perform_splitmerge_tests: Oops! FIALED Child allocation is available after merge! ccap=%s\n", ibp_cap_get(&caps, IBP_MANAGECAP));
        return;
    }


    //** Verify the max/curr size of the master
    err = ibp_sync_command(ibp_probe_gop(ic, ibp_cap_get(&mcaps, IBP_MANAGECAP), &probe, ibp_timeout));
    if (err != IBP_OK) {
        failed_tests++;
        printf("perform_splitmerge_tests:  FAILED Error with probe of mcapafter mergs! ccap=%s err=%d\n", ibp_cap_get(&mcaps, IBP_MANAGECAP), err);
        return;
    }

//GOOD!!!!!!!!!!!!!!!!!

    ibp_cap_getstatus(&probe, &dummy, &dummy, &curr_size, &max_size, &attr);
    if ((curr_size != 1024) && (max_size != 2048)) {
        failed_tests++;
        printf("perform_splitmerge_tests: FAILED Error with master allocation size after merge!! curr_size=%d * max_size=%d should both 1024 and 2048\n", curr_size, max_size);
        return;
    }

    //** Lastly Remove the master allocation **
    err = ibp_sync_command(ibp_remove_gop(ic, ibp_cap_get(&mcaps, IBP_MANAGECAP), ibp_timeout));
    if (err != IBP_OK) {
        printf("perform_splitmerge_tests: FAILED Error removing master allocation!  ibp_errno=%d\n", err);
    }

    if (fstart == failed_tests) {
        printf("perform_splitmerge_tests:  Passed!\n");
    } else {
        printf("perform_splitmerge_tests:  FAILED!\n");
    }
}

//*********************************************************************************
// perform_pushpull_tests - Tests the ability to perform push/pull copy operations
//*********************************************************************************

void perform_pushpull_tests(ibp_depot_t *depot1, ibp_depot_t *depot2)
{
    int bufsize = 2048;
    char wbuf[bufsize+1], rbuf[bufsize+1];
    ibp_attributes_t attr;
    ibp_capset_t caps1, caps2;
    int err;
    ibp_timer_t timer;
    int start_nfailed = failed_tests;

    printf("perform_pushpull_tests:  Starting tests!\n");

    ibp_timer_set(&timer, ibp_timeout, ibp_timeout);

    //** Initialize the buffers **
    memset(wbuf, '0', sizeof(wbuf));
    wbuf[1023]='1';
    wbuf[1024]='2';
    wbuf[bufsize] = '\0';
    memset(rbuf, 0, sizeof(rbuf));

    //*** Make the allocation used in the tests ***
    ibp_attributes_set(&attr, time(NULL) + A_DURATION, IBP_HARD, IBP_BYTEARRAY);
    err = ibp_sync_command(ibp_alloc_gop(ic, &caps1, bufsize, depot1, &attr, disk_cs_type, disk_blocksize, ibp_timeout));
    if (err != IBP_OK) {
        failed_tests++;
        printf("perform_pushpull_tests: Oops! FAILED Error creating allocation 1 for tests!! error=%d\n", err);
        return;
    }

    err = ibp_sync_command(ibp_alloc_gop(ic, &caps2, bufsize, depot2, &attr, disk_cs_type, disk_blocksize, ibp_timeout));
    if (err != IBP_OK) {
        failed_tests++;
        printf("perform_pushpull_tests: Oops! FAILED Error creating allocation 2 for tests!! error=%d\n", err);
        return;
    }

    //** Fill caps1="1" with data **
    err = IBP_store(ibp_cap_get(&caps1, IBP_WRITECAP), &timer, "1", 1);
    if (err != 1) {
        failed_tests++;
        printf("perform_pushpull_tests: Oops! FAILED Error with master IBP_store! wrote=%d err=%d\n", err, IBP_errno);
    }

    //** Append it to cap2="1"
    err = ibp_sync_command(ibp_copy_gop(ic, IBP_PUSH, NS_TYPE_SOCK, NULL, ibp_cap_get(&caps1, IBP_READCAP),
                    ibp_cap_get(&caps2, IBP_WRITECAP), 0, -1, 1, ibp_timeout, ibp_timeout, ibp_timeout));
    if (err != IBP_OK) {
        failed_tests++;
        printf("perform_pushpull_tests: Oops! FAILED Error with copy 1!! error=%d\n", err);
        return;
    }

    //** Append cap2 to cap1="11"
    err = ibp_sync_command(ibp_copy_gop(ic, IBP_PULL, NS_TYPE_SOCK, NULL, ibp_cap_get(&caps1, IBP_WRITECAP),
                    ibp_cap_get(&caps2, IBP_READCAP), -1, 0, 1, ibp_timeout, ibp_timeout, ibp_timeout));
    if (err != IBP_OK) {
        failed_tests++;
        printf("perform_pushpull_tests: Oops! FAILED Error with copy 2!! error=%d\n", err);
        return;
    }

    //** Append it to cap2="111"
    err = ibp_sync_command(ibp_copy_gop(ic, IBP_PUSH, NS_TYPE_SOCK, NULL, ibp_cap_get(&caps1, IBP_READCAP),
                    ibp_cap_get(&caps2, IBP_WRITECAP), 0, -1, 2, ibp_timeout, ibp_timeout, ibp_timeout));
    if (err != IBP_OK) {
        failed_tests++;
        printf("perform_pushpull_tests: Oops! FAILED Error with copy 3!! error=%d\n", err);
        return;
    }

    //** Change  caps1="123"
    err = IBP_write(ibp_cap_get(&caps1, IBP_WRITECAP), &timer, "23", 2, 1);
    if (err != 2) {
        failed_tests++;
        printf("perform_pushpull_tests: Oops! FAILED Error with IBP_store 2! wrote=%d err=%d\n", err, IBP_errno);
    }

    //** offset it to also make cap2="123"
    err = ibp_sync_command(ibp_copy_gop(ic, IBP_PUSH, NS_TYPE_SOCK, NULL, ibp_cap_get(&caps1, IBP_READCAP),
                    ibp_cap_get(&caps2, IBP_WRITECAP), 1, 1, 2, ibp_timeout, ibp_timeout, ibp_timeout));
    if (err != IBP_OK) {
        failed_tests++;
        printf("perform_pushpull_tests: Oops! FAILED Error with copy 4!! error=%d\n", err);
        return;
    }

    //** Now read them back and check them
    //** verify caps1
    memset(rbuf, 0, sizeof(rbuf));
    memcpy(wbuf, "123", 4);
    err = IBP_load(ibp_cap_get(&caps1, IBP_READCAP), &timer, rbuf, 3, 0);
    if (err == 3) {
        if (strcmp(rbuf, wbuf) != 0) {
            failed_tests++;
            printf("perform_pushpull_tests: FAILED Read some data with the cap1 but it wasn't correct!\n");
            printf("perform_pushpull_tests: Original=%s\n", wbuf);
            printf("perform_pushpull_tests:      Got=%s\n", rbuf);
        }
    } else {
        failed_tests++;
        printf("perform_pushpull_tests: Oops! FAILED reading cap1! err=%d\n", err);
    }

    //** and also caps2
    memset(rbuf, 0, sizeof(rbuf));
    err = IBP_load(ibp_cap_get(&caps2, IBP_READCAP), &timer, rbuf, 3, 0);
    if (err == 3) {
        if (strcmp(rbuf, wbuf) != 0) {
            failed_tests++;
            printf("perform_pushpull_tests: FAILED Read some data with the cap2 but it wasn't correct!\n");
            printf("perform_pushpull_tests: Original=%s\n", wbuf);
            printf("perform_pushpull_tests:      Got=%s\n", rbuf);
        }
    } else {
        failed_tests++;
        printf("perform_pushpull_tests: Oops! FAILED reading cap2! err=%d\n", err);
    }

    //** Lastly Remove the allocations **
    err = ibp_sync_command(ibp_remove_gop(ic, ibp_cap_get(&caps1, IBP_MANAGECAP), ibp_timeout));
    if (err != IBP_OK) {
        printf("perform_pushpull_tests: Oops! FAILED Error removing allocation 1!  ibp_errno=%d\n", err);
    }

    err = ibp_sync_command(ibp_remove_gop(ic, ibp_cap_get(&caps2, IBP_MANAGECAP), ibp_timeout));
    if (err != IBP_OK) {
        printf("perform_pushpull_tests: Oops! FAILED Error removing allocation 2!  ibp_errno=%d\n", err);
    }

    if (start_nfailed == failed_tests) {
        printf("perform_pushpull_tests: Passed!\n");
    } else {
        printf("perform_pushpull_tests: Oops! FAILED!\n");
    }
}

//*********************************************************************************
// perform_transfer_buffer_tests - Testst the transfer buffer next an copy routines
//*********************************************************************************

void perform_transfer_buffer_tests()
{
    int err;
    err = tbx_tbuf_test();

    if (err == 0) {
        printf("perform_transfer_buffer_tests: Passed!\n");
    } else {
        printf("perform_transfer_buffer_tests: Oops! FAILED!\n");
        failed_tests++;
    }

}

//*********************************************************************************
//*********************************************************************************
//*********************************************************************************

int main(int argc, char **argv)
{
    ibp_depotinfo_t *depotinfo;
    ibp_depot_t depot1, depot2;
    ibp_attributes_t attr;
    ibp_timer_t timer;
    ibp_capset_t *caps, *caps2, *caps4;
    ibp_capset_t caps3, caps5;
    ibp_capstatus_t astat;
    ibp_proxy_capstatus_t *alias_stat;
    int err, i, len, offset, start_option;
    int bufsize = 1024*1024;
    char wbuf[bufsize];
    char rbuf[bufsize];
    char *host1, *host2;
    int port1, port2, blocksize;
    int no_async;
    int net_cs_type;
    ibp_rid_t rid1, rid2;
    tbx_chksum_t cs;
    tbx_ns_chksum_t *ns_cs = NULL;

    memset(&caps3, 0, sizeof(caps3));
    memset(&caps5, 0, sizeof(caps5));

    printf("start!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n");
    fflush(stdout);

    ncs = NULL;
    failed_tests = 0;

    if (argc < 6) {
        printf("ibp_test [-d loglevel] [-config ibp.cfg] [-network_chksum type blocksize] [-disk_chksum type blocksize]\n");
        printf("          [-no-async] host1 port1 rid1 host2 port2 rid2\n");
        printf("\n");
        printf("     2 depots are requred to test depot-depot copies.\n");
        printf("     Can use the same depot if necessary\n");
        printf("\n");
        printf("-network_chksum type blocksize - Enable network checksumming for transfers.\n");
        printf("                      type should be SHA256, SHA512, SHA1, or MD5.\n");
        printf("                      blocksize determines how many bytes to send between checksums in kbytes.\n");
        printf("-disk_chksum type blocksize - Enable Disk checksumming.\n");
        printf("                      type should be NONE, SHA256, SHA512, SHA1, or MD5.\n");
        printf("                      blocksize determines how many bytes to send between checksums in kbytes.\n");
        printf("\n");
        printf("\n");
        return(-1);
    }

    ibp_errno_init();

    tbx_random_startup();


    printf("1111111111111111111111111111111111111111111111111111111111111111\n");
    fflush(stdout);
    ic = ibp_context_create();  //** Initialize IBP

    printf("22222222222222222222222222222222222111111111111111111111111111111\n");
    fflush(stdout);

    //*** Read in the arguments ***
    i = 1;
    no_async = 0;
    do {
        start_option = i;

        if (strcmp(argv[i], "-d") == 0) {
            i++;
            tbx_set_log_level(atoi(argv[i]));
            i++;
        } else if (strcmp(argv[i], "-config") == 0) { //** Read the config file
            i++;
            ibp_config_load_file(ic, argv[i], NULL);
            log_printf(0, "cmode=%d\n", ibp_context_connection_mode_get(ic));
            i++;
        } else if (strcmp(argv[i], "-no-async") == 0) { //** Skip the async test
            i++;
            no_async = 1;
        } else if (strcmp(argv[i], "-network_chksum") == 0) { //** Add checksum capability
            i++;
            net_cs_type = tbx_chksum_type_name(argv[i]);
            if (net_cs_type == -1) {
                printf("Invalid chksum type.  Got %s should be SHA1, SHA256, SHA512, or MD5\n", argv[i]);
                abort();
            }
            tbx_chksum_set(&cs, net_cs_type);
            i++;

            blocksize = atoi(argv[i])*1024;
            i++;
            ns_cs = tbx_ns_chksum_new();
            tbx_ns_chksum_set(ns_cs, &cs, blocksize);
            ncs = ns_cs;
            ibp_context_chksum_set(ic, ncs);
            log_printf(0, "network_chksum enabled type=%d bs=%d\n", net_cs_type, blocksize);
        } else if (strcmp(argv[i], "-disk_chksum") == 0) { //** Add checksum capability
            i++;
            disk_cs_type = tbx_chksum_type_name(argv[i]);
            if (disk_cs_type < CHKSUM_DEFAULT) {
                printf("Invalid chksum type.  Got %s should be SHA1, SHA256, SHA512, or MD5\n", argv[i]);
                abort();
            }
            i++;

            disk_blocksize = atoi(argv[i])*1024;
            i++;
        }
    } while (start_option < i);

    set_ibp_sync_context(ic);

    host1 = argv[i];
    i++;
    port1 = atoi(argv[i]);
    i++;
    rid1 = ibp_str2rid(argv[i]);
    i++;

    host2 = argv[i];
    i++;
    port2 = atoi(argv[i]);
    i++;
    rid2 = ibp_str2rid(argv[i]);
    i++;

    //*** Print the ibp client version ***
    printf("\n");
    printf("================== IBP Client Version =================\n");
    printf("%s\n", ibp_version());

    //*** Init the structures ***
    ibp_timeout = 30;
    ibp_depot_set(&depot1, host1, port1, rid1);
    ibp_depot_set(&depot2, host2, port2, rid2);
//  ibp_attributes_set(&attr, time(NULL) + 60, IBP_HARD, IBP_BYTEARRAY);
    ibp_attributes_set(&attr, 60, IBP_HARD, IBP_BYTEARRAY);
    ibp_timer_set(&timer, ibp_timeout, ibp_timeout);

//printf("Before allocate\n"); fflush(stdout);

    //*** Perform single allocation
    caps = IBP_allocate(&depot1, &timer, bufsize, &attr);

    if (caps == NULL) {
        printf("Error!!!! ibp_errno = %d\n", IBP_errno);
        return(1);
    } else {
        printf("Read: %s\n", caps->readCap);
        printf("Write: %s\n", caps->writeCap);
        printf("Manage: %s\n", caps->manageCap);
    }

    printf("ibp_manage(IBP_PROBE):-----------------------------------\n");
    memset(&astat, 0, sizeof(astat));
    err = IBP_manage(ibp_cap_get(caps, IBP_MANAGECAP), &timer, IBP_PROBE, 0, &astat);
    printf("after probe\n########################################");
    fflush(stdout);
    if (err == 0) {
        printf("read count = %d\n", astat.readRefCount);
        printf("write count = %d\n", astat.writeRefCount);
        printf("current size = " I64T " \n", astat.currentSize);
        printf("max size = " I64T "\n", astat.maxSize);
        printf("duration = %ld\n", astat.attrib.duration - time(NULL));
        printf("reliability = %d\n", astat.attrib.reliability);
        printf("type = %d\n", astat.attrib.type);
    } else {
        failed_tests++;
        printf("ibp_manage FAILED error = %d * ibp_errno=%d\n", err, IBP_errno);
    }


    printf("ibp_manage(IBP_DECR for write cap): Checking for neg counts -----------------------------------\n");
    err = IBP_manage(ibp_cap_get(caps, IBP_MANAGECAP), &timer, IBP_DECR, IBP_WRITECAP, &astat);
    if (err != OP_STATE_SUCCESS) {
        printf("ibp_manage error = %d * ibp_errno=%d\n", err, IBP_errno);
    }
    err = IBP_manage(ibp_cap_get(caps, IBP_MANAGECAP), &timer, IBP_PROBE, 0, &astat);
    if (err == 0) {
        if ((astat.readRefCount != 1) && (astat.writeRefCount != 0)) {
            failed_tests++;
            printf("FAILED IBP_DECR test read count should be 1 and write count should be 0!\n");
        }
        printf("read count = %d\n", astat.readRefCount);
        printf("write count = %d\n", astat.writeRefCount);
        printf("current size = " I64T "\n", astat.currentSize);
        printf("max size = " I64T "\n", astat.maxSize);
        printf("duration = %lu\n", astat.attrib.duration - time(NULL));
        printf("reliability = %d\n", astat.attrib.reliability);
        printf("type = %d\n", astat.attrib.type);
    } else {
        failed_tests++;
        printf("ibp_manage FAILED error = %d * ibp_errno=%d\n", err, IBP_errno);
    }


    printf("ibp_manage(IBP_CHNG - incresing size to 2MB and changing duration to 20 sec):-----------------------------------\n");
//  ibp_attributes_set(&(astat.attrib), time(NULL) + 20, IBP_HARD, IBP_BYTEARRAY);
    ibp_attributes_set(&(astat.attrib), 20, IBP_HARD, IBP_BYTEARRAY);
    astat.maxSize = 2*1024*1024;
    err = IBP_manage(ibp_cap_get(caps, IBP_MANAGECAP), &timer, IBP_CHNG, 0, &astat);
    if (err != OP_STATE_SUCCESS) {
        printf("ibp_manage error = %d * ibp_errno=%d\n", err, IBP_errno);
    }
    err = IBP_manage(ibp_cap_get(caps, IBP_MANAGECAP), &timer, IBP_PROBE, 0, &astat);
    if (err == 0) {
        printf("read count = %d\n", astat.readRefCount);
        printf("write count = %d\n", astat.writeRefCount);
        printf("current size = " I64T "\n", astat.currentSize);
        printf("max size = " I64T "\n", astat.maxSize);
        printf("duration = %lu\n", astat.attrib.duration - time(NULL));
        printf("reliability = %d\n", astat.attrib.reliability);
        printf("type = %d\n", astat.attrib.type);
    } else {
        failed_tests++;
        printf("ibp_manage FAILED error = %d * ibp_errno=%d\n", err, IBP_errno);
    }

    //**** Basic Write tests ****
    printf("write tests..................................\n");
    for (i=0; i<bufsize; i++) wbuf[i] = '0';
    len = 3*bufsize/4;
log_printf(0, "CHECK WRITE1 START len=%d\n", len);
    err = IBP_store(ibp_cap_get(caps, IBP_WRITECAP), &timer, wbuf, len);
log_printf(0, "CHECK WRITE1 END len=%d\n", len);
    if (err != len) {
        failed_tests++;
        printf("FAILED Error with IBP_store1! wrote=%d err=%d\n", err, IBP_errno);
    }

    len = bufsize - len;
    err = IBP_store(ibp_cap_get(caps, IBP_WRITECAP), &timer, wbuf, len);
    if (err != len) {
        failed_tests++;
        printf("FAILED Error with IBP_store2! wrote=%d err=%d\n", err, IBP_errno);
    }

    for (i=0; i<bufsize; i++) wbuf[i] = '1';
    len = bufsize/2;
    offset = 10;
    err = IBP_write(ibp_cap_get(caps, IBP_WRITECAP), &timer, wbuf, len, offset);
    if (err != len) {
        failed_tests++;
        printf("FAILED Error with IBP_Write! wrote=%d err=%d\n", err, IBP_errno);
    }


    printf("ibp_load test...............................\n");
    len = bufsize;
    offset = 0;
    err = IBP_load(ibp_cap_get(caps, IBP_READCAP), &timer, rbuf, len, offset);
    if (err != len) {
        failed_tests++;
        printf("FAILED Error with IBP_load! wrote=%d err=%d\n", err, IBP_errno);
    } else {
        rbuf[50] = '\0';
        printf("rbuf=%s\n", rbuf);
    }

    printf("ibp_copy test................................\n");
    //*** Perform single allocation
    caps2 = IBP_allocate(&depot2, &timer, bufsize, &attr);
    if (caps2 == NULL) {
        failed_tests++;
        printf("FAILED Error with allocation of dest cap!!!! ibp_errno = %d\n", IBP_errno);
        return(1);
    } else {
        printf("dest Read: %s\n", caps2->readCap);
        printf("dest Write: %s\n", caps2->writeCap);
        printf("dest Manage: %s\n", caps2->manageCap);
    }
    err = IBP_copy(ibp_cap_get(caps, IBP_READCAP), ibp_cap_get(caps2, IBP_WRITECAP), &timer, &timer, 1024, 0);
    if (err != 1024) {
        failed_tests++;
        printf("ibp_copy FAILED size = %d * ibp_errno=%d\n", err, IBP_errno);
    }

    printf("ibp_phoebus_copy test................................\n");
    //*** Perform single allocation
    caps4 = IBP_allocate(&depot2, &timer, bufsize, &attr);
    if (caps4 == NULL) {
        printf("FAILED Error with allocation of dest cap!!!! ibp_errno = %d\n", IBP_errno);
        failed_tests++;
        return(1);
    } else {
        printf("dest Read: %s\n", caps4->readCap);
        printf("dest Write: %s\n", caps4->writeCap);
        printf("dest Manage: %s\n", caps4->manageCap);
    }
    err = IBP_phoebus_copy(NULL, ibp_cap_get(caps, IBP_READCAP), ibp_cap_get(caps4, IBP_WRITECAP), &timer, &timer, 1024, 0);
    if (err != 1024) {
        failed_tests++;
        printf("ibp_phoebus_copy FAILED size = %d * ibp_errno=%d\n", err, IBP_errno);
    }

    //** Remove the cap
    err = IBP_manage(ibp_cap_get(caps4, IBP_MANAGECAP), &timer, IBP_DECR, IBP_READCAP, &astat);
    if (err != 0) {
        failed_tests++;
        printf("FAILED deleting phoebus dest cap error = %d * ibp_errno=%d\n", err, IBP_errno);
    }
    ibp_capset_destroy(caps4);
    caps4 = NULL;

    printf("ibp_manage(IBP_DECR):-Removing allocations----------------------------------\n");
    err = IBP_manage(ibp_cap_get(caps, IBP_MANAGECAP), &timer, IBP_DECR, IBP_READCAP, &astat);
    if (err != 0) {
        failed_tests++;
        printf("ibp_manage(decr) FAILED for caps1 error = %d * ibp_errno=%d\n", err, IBP_errno);
    }
    err = IBP_manage(ibp_cap_get(caps2, IBP_MANAGECAP), &timer, IBP_DECR, IBP_READCAP, &astat);
    if (err != 0) {
        failed_tests++;
        printf("ibp_manage(decr) FAILED for caps2 error = %d * ibp_errno=%d\n", err, IBP_errno);
    }

    printf("ibp_status: IBP_ST_INQ--------------------------------------------------------\n");
    depotinfo = IBP_status(&depot1, IBP_ST_INQ, &timer, "ibp", 10,11,12);
    if (depotinfo != NULL) {
        printf("rid=%d * duration=%ld\n", depotinfo->rid, depotinfo->Duration);
        printf("hc=%lld hs=%lld ha=%lld\n",depotinfo->HardConfigured, depotinfo->HardServed, depotinfo->HardAllocable);
        printf("tc=%lld ts=%lld tu=%lld\n", depotinfo->TotalConfigured, depotinfo->TotalServed, depotinfo->TotalUsed);
    } else {
        failed_tests++;
        printf("ibp_status FAILED error=%d\n", IBP_errno);
    }
    free(depotinfo);

    //** Perform some basic async R/W alloc/remove tests
    if (no_async == 1) {
        printf("=======================>>>>>> Skipping base_async and iovec tests <<<<<<<<=========================\n");
    } else {
        base_async_test(&depot1);
        base_tbx_iovec_test(&depot1);
    }

    //** Now do a few of the extra tests for async only
    //*** Print the depot version ***
    err = ibp_sync_command(ibp_version_gop(ic, &depot1, rbuf, sizeof(rbuf), ibp_timeout));
    if (err == IBP_OK) {
        printf("Printing depot version information......................................\n");
        printf("%s\n", rbuf);
    } else {
        failed_tests++;
        printf("FAILED getting ibp_version. err=%d\n", err);
    }


    //*** Query the depot resources ***
    ibp_ridlist_t *rlist;
    rlist = ibp_ridlist_create();
    err = ibp_sync_command(ibp_query_resources_gop(ic, &depot1, rlist, ibp_timeout));
    if (err == IBP_OK) {
        printf("Number of resources: %d\n", ibp_ridlist_size_get(rlist));
        for (i=0; i<ibp_ridlist_size_get(rlist); i++) {
            printf("  %d: %s\n", i, ibp_rid2str(ibp_ridlist_element_get(rlist, i), rbuf));
        }
    } else {
        failed_tests++;
        printf("FAILED querying depot resource list. err=%d\n", err);
    }
    ibp_ridlist_destroy(rlist);

    perform_user_rw_tests(&depot1);  //** Perform the "user" version of the R/W functions

    //-----------------------------------------------------------------------------------------------------
    //** check ibp_rename ****

    printf("Testing IBP_RENAME...............................................\n");
    caps = IBP_allocate(&depot1, &timer, bufsize, &attr);
    if (caps == NULL) {
        failed_tests++;
        printf("FAILED!!!! ibp_errno = %d\n", IBP_errno);
        return(1);
    } else {
        printf("Original Cap..............\n");
        printf("Read: %s\n", caps->readCap);
        printf("Write: %s\n", caps->writeCap);
        printf("Manage: %s\n", caps->manageCap);
    }

    //** Upload the data
    char *data = "This is a test....";
    len = strlen(data)+1;
    err = IBP_store(ibp_cap_get(caps, IBP_WRITECAP), &timer, data, len);
    if (err != len) {
        failed_tests++;
        printf("FAILED with IBP_store1! wrote=%d err=%d\n", err, IBP_errno);
    }

    //** Rename the allocation
log_printf(0, "BEFORE ibp_rename_gop\n"); tbx_log_flush();
    err = ibp_sync_command(ibp_rename_gop(ic, caps2, ibp_cap_get(caps, IBP_MANAGECAP), ibp_timeout));
log_printf(0, "AFTER ibp_rename_gop\n"); tbx_log_flush();
    if (err != IBP_OK) {
        failed_tests++;
        printf("FAILED with ibp_rename. err=%d\n", err);
    } else {
        printf("Renamed Cap..............\n");
        printf("Read: %s\n", caps2->readCap);
        printf("Write: %s\n", caps2->writeCap);
        printf("Manage: %s\n", caps2->manageCap);
    }

    //** Try reading the original which should fail
    rbuf[0] = '\0';
    err = IBP_load(ibp_cap_get(caps, IBP_READCAP), &timer, rbuf, len, 0);
    if (err != len) {
        printf("Can't read the original cap after the rename which is good!  Got err err=%d\n", err);
    } else {
        failed_tests++;
        printf("Oops! FAILED The read of the original cap succeeded! rbuf=%s\n", rbuf);
    }


    //** Try reading with the new cap
    rbuf[0] = '\0';
    err = IBP_load(ibp_cap_get(caps2, IBP_READCAP), &timer, rbuf, len, 0);
    if (err == len) {
        if (strcmp(rbuf, data) == 0) {
            printf("Read using the new cap the original data!\n");
        } else {
            failed_tests++;
            printf("FAILED Read some data with the new cap but it wasn't correct!\n");
            printf("Original=%s\n", data);
            printf("     Got=%s\n", wbuf);
        }
    } else {
        failed_tests++;
        printf("Oops! FAILED reading with new cap! err=%d\n", err);
    }

    //** Remove the cap
    err = IBP_manage(ibp_cap_get(caps2, IBP_MANAGECAP), &timer, IBP_DECR, IBP_READCAP, &astat);
    if (err != 0) {
        failed_tests++;
        printf("FAILED deleting new cap after rename caps2 error = %d * ibp_errno=%d\n", err, IBP_errno);
    }
    printf("Completed ibp_rename test...........................\n");

//WORKS to here

    //-----------------------------------------------------------------------------------------------------
    //** check ibp_alias_allocate/manage ****

//**** GOOD
    printf("Testing IBP_alias_ALLOCATE/MANAGE...............................................\n");
    caps = IBP_allocate(&depot1, &timer, bufsize, &attr);
    if (caps == NULL) {
        failed_tests++;
        printf("FAILED!!!! ibp_errno = %d\n", IBP_errno);
        return(1);
    } else {
        printf("Original Cap..............\n");
        printf("Read: %s\n", caps->readCap);
        printf("Write: %s\n", caps->writeCap);
        printf("Manage: %s\n", caps->manageCap);
    }

    err = ibp_sync_command(ibp_proxy_alloc_gop(ic, caps2, ibp_cap_get(caps, IBP_MANAGECAP), 0, 0, 0, ibp_timeout));
    if (err != IBP_OK) {
        failed_tests++;
        printf("FAILED with ibp_alias_alloc. err=%d\n", err);
    } else {
        printf("Alias Cap..............\n");
        printf("Read: %s\n", caps2->readCap);
        printf("Write: %s\n", caps2->writeCap);
        printf("Manage: %s\n", caps2->manageCap);
    }


    err = IBP_manage(ibp_cap_get(caps, IBP_MANAGECAP), &timer, IBP_PROBE, 0, &astat);
    if (err == 0) {
        printf("Actual cap info\n");
        printf(" read count = %d\n", astat.readRefCount);
        printf(" write count = %d\n", astat.writeRefCount);
        printf(" current size = " I64T "\n", astat.currentSize);
        printf(" max size = " I64T "\n", astat.maxSize);
        printf(" duration = %lu\n", astat.attrib.duration - time(NULL));
        printf(" reliability = %d\n", astat.attrib.reliability);
        printf(" type = %d\n", astat.attrib.type);
    } else {
        failed_tests++;
        printf("ibp_manage FAILED error = %d * ibp_errno=%d\n", err, IBP_errno);
    }

    err = IBP_manage(ibp_cap_get(caps2, IBP_MANAGECAP), &timer, IBP_PROBE, 0, &astat);
    if (err == 0) {
        printf("Using alias to get actual cap info\n");
        printf(" read count = %d\n", astat.readRefCount);
        printf(" write count = %d\n", astat.writeRefCount);
        printf(" current size = " I64T "\n", astat.currentSize);
        printf(" max size = " I64T "\n", astat.maxSize);
        printf(" duration = %lu\n", astat.attrib.duration - time(NULL));
        printf(" reliability = %d\n", astat.attrib.reliability);
        printf(" type = %d\n", astat.attrib.type);
    } else {
        failed_tests++;
        printf("ibp_manage FAILED error = %d * ibp_errno=%d\n", err, IBP_errno);
    }

    alias_stat = new_ibp_proxy_capstatus();
    err = ibp_sync_command(ibp_proxy_probe_gop(ic, ibp_cap_get(caps2, IBP_MANAGECAP), alias_stat, ibp_timeout));
    if (err != IBP_OK) {
        failed_tests++;
        printf("FAILED with ibp_alias_probe. err=%d\n", err);
    } else {
        int rc, wc, pdt;
        ibp_off_t poffset, psize;
        get_ibp_proxy_capstatus(alias_stat, &rc, &wc, &poffset, &psize, &pdt);
        printf("Alias stat..............\n");
        printf(" read count = %d\n", rc);
        printf(" write count = %d\n", wc);
        printf(" offset = " I64T "\n", poffset);
        printf(" size = " I64T "\n", psize);
        printf(" duration = %lu\n", pdt - time(NULL));
    }
    destroy_ibp_proxy_capstatus(alias_stat);

    err = ibp_sync_command(ibp_proxy_alloc_gop(ic, &caps3, ibp_cap_get(caps, IBP_MANAGECAP), 10, 40, 0, ibp_timeout));
    if (err != IBP_OK) {
        failed_tests++;
        printf("FAILED with ibp_alias_alloc_op. err=%d\n", err);
    } else {
        printf("Alias Cap with range 10-50.............\n");
        printf("Read: %s\n", caps3.readCap);
        printf("Write: %s\n", caps3.writeCap);
        printf("Manage: %s\n", caps3.manageCap);
    }

    err = IBP_manage(ibp_cap_get(&caps3, IBP_MANAGECAP), &timer, IBP_PROBE, 0, &astat);
    if (err == 0) {
        printf("Using limited alias to get actual cap info\n");
        printf(" read count = %d\n", astat.readRefCount);
        printf(" write count = %d\n", astat.writeRefCount);
        printf(" current size = " I64T "\n", astat.currentSize);
        if (astat.maxSize == 40) {
            printf(" max size = " I64T " *** This should be 40\n", astat.maxSize);
        } else {
            failed_tests++;
            printf(" ERROR max size = " I64T " *** This should be 40\n", astat.maxSize);
        }
        printf(" duration = %lu\n", astat.attrib.duration - time(NULL));
        printf(" reliability = %d\n", astat.attrib.reliability);
        printf(" type = %d\n", astat.attrib.type);
    } else {
        failed_tests++;
        printf("ibp_manage FAILED error = %d * ibp_errno=%d\n", err, IBP_errno);
    }

    printf("*append* using the full alias..................................\n");
    for (i=0; i<bufsize; i++) wbuf[i] = '0';
    err = IBP_store(ibp_cap_get(caps2, IBP_WRITECAP), &timer, wbuf, bufsize);
    if (err != bufsize) {
        failed_tests++;
        printf("FAILED with IBP_store! wrote=%d err=%d\n", err, IBP_errno);
    }

    printf("write using the limited alias..................................\n");
    data = "This is a test.";
    len = strlen(data)+1;
    err = IBP_write(ibp_cap_get(&caps3, IBP_WRITECAP), &timer, data, len, 0);
    if (err != len) {
        failed_tests++;
        printf("FAILED with IBP_Write! wrote=%d err=%d\n", err, IBP_errno);
    }

    memcpy(&(wbuf[10]), data, strlen(data)+1);
    len = 10 + strlen(data)+1;
    err = IBP_load(ibp_cap_get(caps2, IBP_READCAP), &timer, rbuf, len, 0);
    if (err == len) {
        if (strcmp(rbuf, wbuf) == 0) {
            printf("Read using the new full alias the original data!\n");
            printf("  read=%s\n", rbuf);
        } else {
            failed_tests++;
            printf("FAILED Read some data with the new cap but it wasn't correct!\n");
            printf("Original=%s\n", wbuf);
            printf("     Got=%s\n", rbuf);
        }
    } else {
        failed_tests++;
        printf("Oops! FAILED reading with new cap! err=%d\n", err);
    }


    //** Try to R/W beyond the end of the limited cap **
    printf("attempting to R/W beyond the end of the limited alias......\n");
    data = "This is a test.";
    len = strlen(data)+1;
    err = IBP_write(ibp_cap_get(&caps3, IBP_WRITECAP), &timer, data, len, 35);
    if (err != len) {
        printf("Correctly got an IBP_Write error! wrote=%d err=%d\n", err, IBP_errno);
    } else {
        failed_tests++;
        printf("Oops! FAILED Was able to write beyond the end of the limited cap with new cap!\n");
    }

    err = IBP_load(ibp_cap_get(&caps3, IBP_READCAP), &timer, rbuf, len, 35);
    if (err != len) {
        printf("Correctly got an IBP_read error! wrote=%d err=%d\n", err, IBP_errno);
    } else {
        failed_tests++;
        printf("Oops! FAILED Was able to read beyond the end of the limited cap with new cap!\n");
    }

    //** Perform a alias->alias copy.  The src alias is restricted
    printf("Testing restricted alias->full alias depot-depot copy\n");
    caps4 = IBP_allocate(&depot1, &timer, bufsize, &attr);
    if (caps == NULL) {
        failed_tests++;
        printf("FAILED alias-alias allocate Error!!!! ibp_errno = %d\n", IBP_errno);
        return(1);
    } else {
        printf("Depot-Depot copy OriginalDestiniation Cap..............\n");
        printf("Read: %s\n", caps4->readCap);
        printf("Write: %s\n", caps4->writeCap);
        printf("Manage: %s\n", caps4->manageCap);
    }

    err = ibp_sync_command(ibp_proxy_alloc_gop(ic, &caps5, ibp_cap_get(caps4, IBP_MANAGECAP), 0, 0, 0, ibp_timeout));
    if (err != IBP_OK) {
        failed_tests++;
        printf("FAILED Error with ibp_alias_alloc_op. err=%d\n", err);
    } else {
        printf("Destination Alias Cap with full range.............\n");
        printf("Read: %s\n", caps5.readCap);
        printf("Write: %s\n", caps5.writeCap);
        printf("Manage: %s\n", caps5.manageCap);
    }

//BAD!!!!!!!!!!!
    //** Perform the copy
    data = "This is a test.";
    len = strlen(data)+1;
    err = IBP_copy(ibp_cap_get(&caps3, IBP_READCAP), ibp_cap_get(&caps5, IBP_WRITECAP), &timer, &timer, len, 0);
    if (err != len) {
        printf("ibp_copy size = %d * ibp_errno=%d\n", err, IBP_errno);
    }

    //** Load it back and verify **
    err = IBP_load(ibp_cap_get(&caps5, IBP_READCAP), &timer, rbuf, len, 0);
    if (err == len) {
        if (strcmp(rbuf, data) == 0) {
            printf("Read using the new full alias the original data!\n");
            printf("  read=%s\n", rbuf);
        } else {
            failed_tests++;
            printf("FAILED Read some data with the new cap but it wasn't correct!\n");
            printf("Original=%s\n", data);
            printf("     Got=%s\n", rbuf);
        }
    } else {
        failed_tests++;
        printf("Oops! FAILED Failed reading with new cap! err=%d\n", err);
    }

    //** Remove the cap5 (full alias)
    err = ibp_sync_command(ibp_proxy_remove_gop(ic, ibp_cap_get(&caps5, IBP_MANAGECAP), ibp_cap_get(caps4, IBP_MANAGECAP), ibp_timeout));
    if (err != IBP_OK) {
        failed_tests++;
        printf("FAILED Error dest deleting alias cap error = %d\n", err);
    }

    //** Remove the dest cap
    err = IBP_manage(ibp_cap_get(caps4, IBP_MANAGECAP), &timer, IBP_DECR, IBP_READCAP, &astat);
    if (err != 0) {
        failed_tests++;
        printf("FAILED Error deleting dest caps error = %d * ibp_errno=%d\n", err, IBP_errno);
    }

    printf("completed alias depot->depot copy test\n");

    //** Try to remove the cap2 (full alias) with a bad cap
    err =ibp_sync_command(ibp_proxy_remove_gop(ic, ibp_cap_get(caps2, IBP_MANAGECAP), ibp_cap_get(&caps3, IBP_MANAGECAP), ibp_timeout));
    if (err != IBP_OK) {
        printf("Correctly detected error deleting alias cap with an invalid master cap error = %d\n", err);
    } else {
        failed_tests++;
        printf("Oops! FAILED Was able to delete the alias with an invalid master cap!!!!!!!!\n");
    }

    //** Remove the cap2 (full alias)
    err = ibp_sync_command(ibp_proxy_remove_gop(ic, ibp_cap_get(caps2, IBP_MANAGECAP), ibp_cap_get(caps, IBP_MANAGECAP), ibp_timeout));
    if (err != IBP_OK) {
        failed_tests++;
        printf("FAILED deleting alias cap error = %d\n", err);
    }

    printf("Try to read the deleted full falias.  This should generate an error\n");
    err = IBP_load(ibp_cap_get(caps2, IBP_READCAP), &timer, rbuf, len, 35);
    if (err != len) {
        printf("Correctly got an IBP_read error! wrote=%d err=%d\n", err, IBP_errno);
    } else {
        failed_tests++;
        printf("Oops! FAILED Was able to write beyond the end of the limited cap with new cap!\n");
    }

    //** Remove the limited alias (cap3)
    err = ibp_sync_command(ibp_proxy_remove_gop(ic, ibp_cap_get(&caps3, IBP_MANAGECAP), ibp_cap_get(caps, IBP_MANAGECAP), ibp_timeout));
    if (err != IBP_OK) {
        failed_tests++;
        printf("FAILED Error deleting the limited alias cap  error = %d\n", err);
    }

    //** Remove the original cap
    err = IBP_manage(ibp_cap_get(caps, IBP_MANAGECAP), &timer, IBP_DECR, IBP_READCAP, &astat);
    if (err != 0) {
        failed_tests++;
        printf("FAILED Error deleting original caps error = %d * ibp_errno=%d\n", err, IBP_errno);
    }

//GOOD!!!!!!!!!!!!!!!!!!!!

    printf("finished testing IBP_alias_ALLOCATE/MANAGE...............................................\n");

    perform_splitmerge_tests(&depot1);
    perform_pushpull_tests(&depot1, &depot2);
    perform_big_alloc_tests(&depot1);
    perform_manage_truncate_tests(&depot1);
    perform_transfer_buffer_tests();


    printf("\n\n");
    printf("Final network connection counter: %d\n", tbx_network_counter(NULL));
    printf("Tests that failed: %d\n", failed_tests);

    ibp_context_destroy(ic);

    if (ns_cs != NULL) tbx_ns_chksum_del(ns_cs);
    tbx_random_shutdown();

    return(0);
}

