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

#define _log_module_index 171

#include <assert.h>
#include <tbx/apr_wrapper.h>
#include <tbx/assert_result.h>
#include <math.h>
#include <apr_time.h>

#include <gop/opque.h>
#include <tbx/log.h>
#include <tbx/iniparse.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>
#include <tbx/random.h>
#include <lio/cache.h>
#include <lio/lio.h>
#include "cache.h"
#include "ex3/system.h"


typedef struct {
    char *ex_fname;
    char *lio_fname;
    char *local_fname;
    int n_parallel;
    int preallocate;
    ex_off_t buffer_size;
    ex_off_t file_size;
    int read_lag;
    int update_interval;
    int do_final_check;
    int do_flush_check;
    int timeout;
    double read_fraction;
    double min_size;
    double max_size;
    double ln_min;
    double ln_max;
    int read_sigma;
    int write_sigma;
    int seed;
    int rw_mode;
    int n_targets;
} rw_config_t;

typedef struct {
    ex_off_t offset;
    ex_off_t len;
} tile_t;

typedef struct {
    int base_index;
    int curr;
    int *span;
} test_index_t;

typedef struct {
    int global_index;
    int local_index;
    int type;
    tbx_tbuf_t tbuf;
    ex_tbx_iovec_t iov;
    char *buffer;
} task_slot_t;

typedef struct {
    lio_segment_t   *seg;
    lio_fd_t    *fd;
    FILE        *fd_local;
    data_attr_t *da;
    lio_exnode_exchange_t  *exp;
    lio_exnode_t *ex;
    task_slot_t *task_list;
    char *wc_span;
    tbx_stack_t *free_slots;
    lio_path_tuple_t tuple;
    test_index_t read_index;
    test_index_t write_index;
    ex_off_t    total_bytes;
    ex_off_t    data_start;
    int         rw_mode;
    int         timeout;
    int         tile_start;
    int         index;
} target_t;

//*** Globals used in the test
//static target_t *target;
static rw_config_t rwc;
static char *tile_data;
static tile_t *base_tile;
static int  tile_size;
static int total_scan_size;
static int last_tile_index;
static ex_off_t tile_bytes;
static ex_off_t max_task_bytes;
static ex_off_t total_scan_bytes;

//** Different modes
#define RW_SEGMENT 0
#define RW_LIO_AIO 1
#define RW_LIO_WQ  2
#define RW_LOCAL   3

static int my_tbx_log_level = 10;

//*************************************************************************
//** My random routines for reproducible runs
//*************************************************************************

void my_random_seed(unsigned int seed)
{
    srandom(seed);
}

//*************************************************************************

int my_get_random(void *vbuf, int nbytes)
{
    char *buf = (char *)vbuf;
    int i;
    unsigned short int v;
    unsigned short int *p;
    int ncalls = nbytes / sizeof(v);
    int nrem = nbytes % sizeof(v);

    for (i=0; i<ncalls; i++) {
        p = (unsigned short int *)&(buf[i*sizeof(v)]);
        *p = random();
    }

    if (nrem > 0) {
        v = random();
        memcpy(&(buf[ncalls*sizeof(v)]), &v, nrem);
    }

    return(0);
}

//*************************************************************************

double my_random_double(double lo, double hi)
{
    double dn, n;
    uint64_t rn;

    rn = 0;
    my_get_random(&rn, sizeof(rn));
    dn = (1.0 * rn) / (UINT64_MAX + 1.0);
    n = lo + (hi - lo) * dn;

    return(n);
}

//*******************************************************************

int64_t my_random_int(int64_t lo, int64_t hi)
{
    int64_t n, dn;

    dn = hi - lo + 1;
    n = lo + dn * my_random_double(0, 1);

    return(n);
}

//*************************************************************************
//  I/O abstraction routines
//*************************************************************************

typedef struct {
   FILE *fd;
   off_t offset;
   off_t len;
   void *buffer;
} local_op_t;

//*************************************************************************

gop_op_status_t local_flush_fn(void *arg, int id)
{
    int err;

    local_op_t *op = (local_op_t *)arg;
    err = fflush(op->fd);
    return((err == 0) ? gop_success_status : gop_failure_status);
}

//*************************************************************************

gop_op_status_t local_truncate_fn(void *arg, int id)
{
    int err;

    local_op_t *op = (local_op_t *)arg;
    err = ftruncate(fileno(op->fd), op->offset);
    return((err == 0) ? gop_success_status : gop_failure_status);
}

//*************************************************************************

gop_op_status_t local_read_fn(void *arg, int id)
{
    int n;

    local_op_t *op = (local_op_t *)arg;
    n = pread(fileno(op->fd), op->buffer, op->len, op->offset);
    return((n == op->len) ? gop_success_status : gop_failure_status);
}

//*************************************************************************

gop_op_status_t local_write_fn(void *arg, int id)
{
    int n;

    local_op_t *op = (local_op_t *)arg;
    n = pwrite(fileno(op->fd), op->buffer, op->len, op->offset);
    return((n == op->len) ? gop_success_status : gop_failure_status);
}

//*************************************************************************
//  local_rw_op - Makes the OP structure for the local R/W.
//      This makes some assumptions about the iov and tbuf.  Namely that
//      they are single ops.
//*************************************************************************

local_op_t *local_rw_op(target_t *t, int n_iov, ex_tbx_iovec_t *iov, tbx_tbuf_t *tbuf, ex_off_t boff)
{
    local_op_t *op;

    tbx_type_malloc_clear(op, local_op_t, 1);
    op->fd = t->fd_local;
    op->offset = iov->offset;
    op->len = iov->len;
    op->buffer = tbuf->buf.iov->iov_base;

    return(op);
}

//*************************************************************************

gop_op_generic_t *io_read_gop(target_t *t, int n_iov, ex_tbx_iovec_t *iov, tbx_tbuf_t *tbuf, ex_off_t boff)
{
    switch (t->rw_mode) {
        case RW_SEGMENT:
            return(segment_read(t->seg, t->da, NULL, n_iov, iov, tbuf, boff, t->timeout));
        case RW_LIO_AIO:
        case RW_LIO_WQ:
            return(lio_read_ex_gop(t->fd, n_iov, iov, tbuf, boff, NULL));
        case RW_LOCAL:
            return(gop_tp_op_new(lio_gc->tpc_unlimited, NULL, local_read_fn, local_rw_op(t, n_iov, iov, tbuf, boff), free, 1));
    }

    return(gop_dummy(gop_failure_status));
}

//*************************************************************************

gop_op_generic_t *io_write_gop(target_t *t, int n_iov, ex_tbx_iovec_t *iov, tbx_tbuf_t *tbuf, ex_off_t boff)
{
    switch (t->rw_mode) {
        case RW_SEGMENT:
            return(segment_write(t->seg, t->da, NULL, n_iov, iov, tbuf, boff, t->timeout));
        case RW_LIO_AIO:
        case RW_LIO_WQ:
            return(lio_write_ex_gop(t->fd, n_iov, iov, tbuf, boff, NULL));
        case RW_LOCAL:
            return(gop_tp_op_new(lio_gc->tpc_unlimited, NULL, local_write_fn, local_rw_op(t, n_iov, iov, tbuf, boff), free, 1));
    }

    return(gop_dummy(gop_failure_status));
}

//*************************************************************************

ex_off_t io_size(target_t *t)
{
    switch (t->rw_mode) {
        case RW_SEGMENT:
            return(segment_size(t->seg));
        case RW_LIO_AIO:
        case RW_LIO_WQ:
            return(lio_size(t->fd));
        case RW_LOCAL:
            fseek(t->fd_local, 0, SEEK_END);
            return(ftell(t->fd_local));
    }

    return(-1);
}

//*************************************************************************

gop_op_generic_t *io_truncate_gop(target_t *t, ex_off_t new_size)
{
    local_op_t *op;

    switch (t->rw_mode) {
        case RW_SEGMENT:
            return(lio_segment_truncate(t->seg, t->da, new_size, t->timeout));
        case RW_LIO_AIO:
        case RW_LIO_WQ:
            return(lio_truncate_gop(t->fd, new_size));
        case RW_LOCAL:
            tbx_type_malloc_clear(op, local_op_t, 1);
            op->fd = t->fd_local;
            op->offset = new_size;
            return(gop_tp_op_new(lio_gc->tpc_unlimited, NULL, local_truncate_fn, op, free, 1));
    }

    return(gop_dummy(gop_failure_status));
}

//*************************************************************************

gop_op_generic_t *io_flush_gop(target_t *t)
{
    local_op_t *op;

    switch (t->rw_mode) {
        case RW_SEGMENT:
            return(segment_flush(t->seg, t->da, 0, segment_size(t->seg)+1, t->timeout));
        case RW_LIO_AIO:
        case RW_LIO_WQ:
            return(lio_flush_gop(t->fd, 0, lio_size(t->fd)+1));
        case RW_LOCAL:
            tbx_type_malloc_clear(op, local_op_t, 1);
            op->fd = t->fd_local;
            return(gop_tp_op_new(lio_gc->tpc_unlimited, NULL, local_flush_fn, op, free, 1));
    }

    return(gop_dummy(gop_failure_status));
}

//*************************************************************************

void io_cache_drop(target_t *t)
{
    switch (t->rw_mode) {
        case RW_SEGMENT:
            lio_segment_cache_pages_drop(t->seg, 0, segment_size(t->seg)+1);
            break;
        case RW_LIO_AIO:
        case RW_LIO_WQ:
            lio_cache_pages_drop(t->fd, 0, lio_size(t->fd)+1);
            break;
        case RW_LOCAL:
            break;
    }
}

//*************************************************************************

void io_open(target_t *t, rw_config_t *rwc)
{
    int err;
    char fname[4096];

    switch (t->rw_mode) {
        case RW_SEGMENT:
            //** Open the file
            t->exp = lio_exnode_exchange_load_file(rwc->ex_fname);
            //** and parse it
            t->ex = lio_exnode_create();
            lio_exnode_deserialize(t->ex, t->exp, lio_gc->ess);

            //** Get the default view to use
            t->seg = lio_exnode_default_get(t->ex);
            if (t->seg == NULL) {
                printf("[ti=%d] No default segment!  Aborting!\n", t->index);
                tbx_log_flush();
                fflush(stdout);
                abort();
            }
            return;
        case RW_LIO_AIO:
        case RW_LIO_WQ:
            if (rwc->n_targets == 1) {
                strcpy(fname, rwc->lio_fname);
            } else {
                snprintf(fname, sizeof(fname), "%s.%d", rwc->lio_fname, t->index);
            }
            t->tuple = lio_path_resolve(lio_gc->auto_translate, fname);
            err = gop_sync_exec(lio_open_gop(t->tuple.lc, t->tuple.creds, t->tuple.path, lio_fopen_flags("w+"), NULL, &t->fd, t->timeout));
            if (err != OP_STATE_SUCCESS) {
                printf("[ti=%d] Failed opening LIO file(%s)!  Aborting!\n", t->index, t->tuple.path);
                tbx_log_flush();
                fflush(stdout);
                abort();
            }

            if (t->rw_mode > 0) lio_wq_enable(t->fd, rwc->n_parallel);
            return;
        case RW_LOCAL:
            if (rwc->n_targets == 1) {
                strncpy(fname, rwc->local_fname, sizeof(fname));
printf("fname=%s local=%s\n", fname, rwc->local_fname);
            } else {
                snprintf(fname, sizeof(fname), "%s.%d", rwc->local_fname, t->index);
printf("fname=%s local=%s\n", fname, rwc->local_fname);
            }
            t->fd_local = fopen(fname, "w+");
            if (t->fd_local == NULL) {
                printf("[ti=%d] Failed opening LOCAL file(%s)!  Aborting!\n", t->index, rwc->local_fname);
                tbx_log_flush();
                fflush(stdout);
                abort();
            }
            return;
    }
}

//*************************************************************************

void io_close(target_t *t, rw_config_t *rwc)
{
    int err;

    //** Truncate the file to back to 0
    err = gop_sync_exec(io_truncate_gop(t,0));
    if (err != OP_STATE_SUCCESS) {
        printf("[ti=%d] Error truncating the file!\n", t->index);
        tbx_log_flush();
        fflush(stdout);
    }

    switch (t->rw_mode) {
        case RW_SEGMENT:
            //** Clean up
            gop_sync_exec(segment_flush(t->seg, t->da, 0, segment_size(t->seg)+1, t->timeout));
            lio_exnode_destroy(t->ex);
            lio_exnode_exchange_destroy(t->exp);
            break;
        case RW_LIO_AIO:
        case RW_LIO_WQ:
            gop_sync_exec(lio_close_gop(t->fd));
            lio_path_release(&t->tuple);
            break;
        case RW_LOCAL:
            fclose(t->fd_local);
            break;
    }
}

//*************************************************************************
//*************************************************************************

//*************************************************************************
// gernerate_task_list - Creates the task list
//*************************************************************************

void generate_task_list()
{
    int inc_size = 100;
    int i, max_size, loops;
    ex_off_t offset, len, last_offset;
    double d;

    max_size = inc_size;
    base_tile = (tile_t *)malloc(sizeof(tile_t)*max_size);

    last_offset = rwc.file_size % rwc.buffer_size;
    last_tile_index = -1;
    max_task_bytes = -1;
    i = 0;
    offset = 0;
    do {
        if (i >= max_size) {
            max_size += inc_size;
            base_tile = (tile_t *)realloc(base_tile, sizeof(tile_t)*max_size);
        }
        base_tile[i].offset = offset;
        if (rwc.min_size == rwc.max_size) {
            len = rwc.min_size;
        } else {
            d = my_random_double(rwc.ln_min, rwc.ln_max);
            len = exp(d);
        }
        base_tile[i].len = len;

        offset += len;

        if (offset < rwc.buffer_size) log_printf(10, "i=%d off=" XOT " len=" XOT "\n", i, base_tile[i].offset, base_tile[i].len);
        if (offset < last_offset) last_tile_index = i;
        if (max_task_bytes < len) max_task_bytes = len;
        i++;
    } while (offset < rwc.buffer_size);

    //** If it's to big truncate the size
    if (offset > 1.01*rwc.buffer_size) {
        offset -= len;
        len = 1.01*rwc.buffer_size - offset;
        base_tile[i-1].len = len;
        offset += len;
    }

    //** Print the last slot if needed
    log_printf(10, "i=%d off=" XOT " len=" XOT "\n", i-1, base_tile[i-1].offset, base_tile[i-1].len);

    tile_size = i;
    tile_bytes = offset;
    loops = rwc.file_size / rwc.buffer_size;
    total_scan_size = loops*tile_size;
    total_scan_bytes = loops*tile_bytes;
    if (last_tile_index > 0) {
        total_scan_size += last_tile_index + 1;
        total_scan_bytes += base_tile[last_tile_index].offset + base_tile[last_tile_index].len;
    }

    //** Adjust the size of the base_tile array to the correct size;
    base_tile = (tile_t *)realloc(base_tile, sizeof(tile_t)*tile_size);

    //** Make the actual buffer and fill it with random data
    tbx_type_malloc_clear(tile_data, char, tile_bytes);
    my_get_random(tile_data, tile_bytes);

    //** Want to do the reading after thew write phase completes
    if (rwc.read_lag < 0) rwc.read_lag = total_scan_size;

    log_printf(0, "--------- Task Breakdown ----------\n");
    d = tile_bytes / 1024.0 / 1024.0;
    log_printf(0, "Tile size: %lfMB (" XOT " bytes)\n", d, tile_bytes);
    log_printf(0, "Tile ops: %d (Tile loops:%d  Last tile index: %d)\n", tile_size, loops, last_tile_index);

    d = total_scan_bytes / 1024.0 / 1024.0;
    log_printf(0, "Total size: %lfMB\n", d);
    log_printf(0, "Total ops: %d (write)", total_scan_size);
    if (rwc.read_fraction) {
        slog_printf(0, " + %d (read) = %d\n", total_scan_size, 2*total_scan_size);
    } else {
        slog_printf(0, "\n");
    }
    log_printf(0, "--------------------------------------------\n");
}

//*************************************************************************
// make_test_indices - Makes the test indices for keeping track of the R/W
//   task position
//*************************************************************************

void make_test_indices(target_t *t)
{
    int i;

    if (rwc.read_sigma > total_scan_size) rwc.read_sigma = total_scan_size;
    if (rwc.write_sigma > total_scan_size) rwc.write_sigma = total_scan_size;

    //** Figure out how many bytes we'll actually be using based on the offset
    t->data_start = base_tile[t->tile_start].offset;
    t->total_bytes = 0;
    for (i=0; i<total_scan_size; i++) {
        t->total_bytes += base_tile[(i+t->tile_start)%tile_size].len;
    }
    tbx_type_malloc(t->wc_span, char, total_scan_size);
    memset(t->wc_span, '0', total_scan_size);

    tbx_type_malloc_clear(t->read_index.span, int, rwc.read_sigma);
    tbx_type_malloc_clear(t->write_index.span, int, rwc.write_sigma);

    t->read_index.base_index = 0;
    t->read_index.curr = (rwc.read_fraction == 0) ? tile_size : 0;
    t->write_index.base_index = 0;
    t->write_index.curr = 0;

    tbx_type_malloc_clear(t->task_list, task_slot_t, rwc.n_parallel);
    for (i=0; i<rwc.n_parallel; i++) {
        tbx_type_malloc_clear(t->task_list[i].buffer, char, max_task_bytes);
    }
}

//*************************************************************************
// cleanup_test_indices - Cleans up the test indices
//*************************************************************************

void cleanup_test_indices(target_t *t)
{
    int i;

    for (i=0; i<rwc.n_parallel; i++) {
        free(t->task_list[i].buffer);
    }
    free(t->task_list);
    free(t->wc_span);
    free(t->read_index.span);
    free(t->write_index.span);
}

//*************************************************************************
// compare_buffers_print - FInds the 1st index where the buffers differ
//*************************************************************************

void compare_buffers_print(char *b1, char *b2, int len, ex_off_t offset, int ti)
{
    int i, mode, last, ok;
    ex_off_t start, end, k;

    mode = (b1[0] == b2[0]) ? 0 : 1;
    start = offset;
    last = len - 1;

    log_printf(0, "[ti=%d] Printing comparision breakdown -- Single byte matches are suppressed (len=%d)\n", ti, len);
    for (i=0; i<len; i++) {
        if (mode == 0) {  //** Matching range
            if ((b1[i] != b2[i]) || (last == i)) {
                end = offset + i-1;
                k = end - start + 1;
                log_printf(0, "[ti=%d]   MATCH : " XOT " -> " XOT " (" XOT " bytes)\n", ti, start, end, k);

                start = offset + i;
                mode = 1;
            }
        } else {
            if ((b1[i] == b2[i]) || (last == i)) {
                ok = 0;  //** Suppress single byte matches
                if (last != i) {
                    if (b1[i+1] == b2[i+1]) ok = 1;
                }
                if ((ok == 1) || (last == i)) {
                    end = offset + i-1;
                    k = end - start + 1;
                    log_printf(0, "[ti=%d]   DIFFER: " XOT " -> " XOT " (" XOT " bytes)\n", ti, start, end, k);

                    start = offset + i;
                    mode = 0;
                }
            }
        }
    }

    return;
}

//*************************************************************************
// compare_check - Compares buffers not tile aligned
//*************************************************************************

int compare_check(target_t *t, ex_off_t offset, ex_off_t nbytes, char *buffer, int do_print)
{
    ex_off_t off, len, n;
    int err;

    off = (t->data_start + offset) % tile_bytes;
printf("[ti=%d] ts=%d ds=" XOT "\n", t->index, t->tile_start, t->data_start);
    len = tile_bytes - off;
    n = (nbytes > len) ? len : nbytes;
    err = memcmp(buffer, tile_data + off, n);
    if (err && do_print) compare_buffers_print(buffer, tile_data + off, n, offset, t->index);
printf("[ti=%d] 1.wrap around off=" XOT " len=" XOT " nbytes=" XOT "\n", t->index, offset, len, nbytes);
    if ( nbytes <= len) return(err);  //** No wraparound

    //** Got a wraparound
    off = len;
    len = nbytes - len;
printf("[ti=%d] 2.wrap around off=" XOT " len=" XOT " nbytes=" XOT "\n", t->index, offset+off, len, nbytes);
    err = memcmp(buffer + off, tile_data, len);
    if (err && do_print) compare_buffers_print(buffer+off, tile_data, len, offset+off, t->index);
    return(err);
}


//*************************************************************************
// perform_final_verify - Does a final read verify of the data
//*************************************************************************

int perform_final_verify(target_t *t)
{
    char *buffer;
    int err, fail;
    ex_off_t off, len;
    ex_tbx_iovec_t iov;
    tbx_tbuf_t tbuf;
    apr_time_t dt, dt2;
    double rate, dsec;
    int ll = 15;

    tbx_type_malloc(buffer, char, tile_bytes);

    tbx_tbuf_single(&tbuf, tile_bytes, buffer);

//set_tbx_log_level(20);

printf("[ti=%d]  total_bytes=" XOT "\n", t->index, t->total_bytes);
    off = 0;
    fail = 0;
    log_printf(0, "[ti=%d] -------------- Starting final verify -----------------------\n", t->index);
    tbx_log_flush();
    dt = apr_time_now();

    len = tile_bytes;
    do {
        log_printf(ll, "[ti=%d] checking offset=" XOT "\n", t->index, off);
        memset(buffer, 'A', tile_bytes);
        ex_iovec_single(&iov, off, len);
        tbx_log_flush();
        dt2 = apr_time_now();

        err = gop_sync_exec(io_read_gop(t, 1, &iov, &tbuf, 0));
        dt2 = apr_time_now() - dt2;
        dsec = dt2;
        dsec = dsec / APR_USEC_PER_SEC;
        rate = (1.0*tile_bytes) / ( 1024.0 * 1024.0 * dsec);
        log_printf(0, "[ti=%d] gop er=%d: Time: %lf secs  (%lf MB/s)\n", t->index, err, dsec, rate);

        log_printf(ll, "[ti=%d] gop err=%d\n", t->index, err);
        tbx_log_flush();

        if (err != OP_STATE_SUCCESS) {
            fail++;
            log_printf(0, "[ti=%d] ERROR with read! block offset=" XOT "\n", t->index, off);
        }
        err = compare_check(t, off, len, buffer, 0);
        log_printf(ll, "[ti=%d] memcmp=%d\n", t->index, err);
        tbx_log_flush();

//err = 0;
        if (err != 0) {
            fail++;
            log_printf(0, "[ti=%d] ERROR with compare! block offset=" XOT "\n", t->index, off);
            compare_check(t, off, len, buffer, 1);
        }

        off += tile_bytes;
        len = t->total_bytes - off;
        if (len > tile_bytes) len = tile_bytes;
    } while (len > 0);

    dt = apr_time_now() - dt;
    log_printf(0, "[ti=%d] -------------- Completed final verify -----------------------\n", t->index);

    dsec = dt;
    dsec = dsec / APR_USEC_PER_SEC;
    rate = (1.0*t->total_bytes) / ( 1024.0 * 1024.0 * dsec);
    log_printf(0, "[ti=%d] Time: %lf secs  (%lf MB/s)\n", t->index, dsec, rate);

    if (fail == 0) {
        log_printf(0, "[ti=%d] PASSED\n", t->index);
    } else {
        log_printf(0, "[ti=%d] FAILED\n", t->index);
    }

    tbx_log_flush();

    free(buffer);

    return(fail);
}

//*************************************************************************
// find_write_task - Finds a write task to perform
//*************************************************************************

gop_op_generic_t *find_write_task(target_t *t, task_slot_t *tslot)
{
    int flip, i, j, n, slot;
    ex_off_t offset;
    gop_op_generic_t *gop;

    if (t->write_index.curr >= total_scan_size) return(NULL);  //** Nothing left to do

    t->write_index.curr++;

    flip = my_random_int(0, rwc.write_sigma-1);
    n = -1;
    for (i=0; i< rwc.write_sigma; i++) {
        j = (flip + i) % rwc.write_sigma;  //** Used to map the slot to the span range
        slot = (t->write_index.base_index + j) % rwc.write_sigma;
        if (t->write_index.span[slot] == 0) {
            n = t->write_index.base_index + j;
            break;
        }
    }

    log_printf(my_tbx_log_level, "[ti=%d] span global=%d flip=%d i=%d base=%d spanindex=%d\n", t->index, n, flip, i, t->write_index.base_index, (n%rwc.write_sigma));
    tbx_log_flush();

    t->write_index.span[n%rwc.write_sigma] = 1;
    tslot->global_index = n;
    log_printf(my_tbx_log_level, "[ti=%d] span slot=%d curr=%d base=%d global=%d\n", t->index, n, t->write_index.curr, t->write_index.base_index, tslot->global_index);
    tbx_log_flush();

    tslot->local_index = (tslot->global_index + t->tile_start) % tile_size;
    tslot->type = 0;

    slot = tslot->local_index;
    offset = base_tile[slot].offset - t->data_start;
    if (offset<0) offset += tile_bytes;
    offset += ((tslot->global_index) / tile_size) * tile_bytes;
    tbx_tbuf_single(&(tslot->tbuf), base_tile[slot].len, &(tile_data[base_tile[slot].offset]));
    ex_iovec_single(&(tslot->iov), offset, base_tile[slot].len);

    gop = io_write_gop(t, 1, &(tslot->iov), &(tslot->tbuf), 0);
    gop_set_private(gop, (void *)tslot);

    n = total_scan_size - rwc.write_sigma;
    log_printf(my_tbx_log_level, "[ti=%d] span_update max=%d base=%d\n", t->index, n, t->write_index.base_index);
    tbx_log_flush();
    for (i=0; i<rwc.write_sigma; i++) {
        slot = t->write_index.base_index % rwc.write_sigma;
        log_printf(my_tbx_log_level, "[ti=%d] find_write_task: span_update slot=%d i=%d base=%d span[slot]=%d\n", t->index, slot, i, t->write_index.base_index, t->write_index.span[slot]);
        tbx_log_flush();

        if (t->write_index.span[slot] == 0) break;
        if ( t->write_index.base_index >= n) break;  //** Don't move the window beyond the end
        t->write_index.span[slot] = 0;
        t->write_index.base_index++;
    }

    return(gop);
}

//*************************************************************************
// find_read_task - Finds a read task to perform
//*************************************************************************

gop_op_generic_t *find_read_task(target_t *t, task_slot_t *tslot, int write_done)
{
    int flip, i, j, n, slot;
    ex_off_t offset;
    gop_op_generic_t *gop;

    if (t->read_index.curr >= total_scan_size) return(NULL);  //** Nothing left to do

    flip = my_random_int(0, rwc.read_sigma-1);

    n = -1;
    for (i=0; i< rwc.read_sigma; i++) {
        j = (flip + i) % rwc.read_sigma;  //** Used to map the slot to the span range
        slot = (t->read_index.base_index + j) % rwc.read_sigma;
        log_printf(my_tbx_log_level, "[ti=%d] span i=%d j=%d flip=%d base=%d spanslot=%d span[slot]=%d\n", t->index, i, j, flip,  t->read_index.base_index, slot, t->read_index.span[slot]);
        tbx_log_flush();
        if (t->read_index.span[slot] == 0) {
            slot =  t->read_index.base_index + j;
            log_printf(my_tbx_log_level, "[ti=%d] slot=%d wc=%c\n", t->index, slot, t->wc_span[slot]);
            tbx_log_flush();

            if (t->wc_span[slot] == '1') {
                n = t->read_index.base_index + j;
                break;
            }
        }
    }

    log_printf(my_tbx_log_level, "[ti=%d] span global=%d flip=%d base=%d spanindex=%d\n", t->index, n, flip, t->read_index.base_index, (n%rwc.read_sigma));
    tbx_log_flush();

    if ((n<0) && (write_done >= total_scan_size)) {
        log_printf(0, "[ti=%d] ERROR!! No viable taks found!! Printing wc_span table (READ base=%d curr=%d  ---- WRITE base=%d curr=%d\n",
                   t->index, t->read_index.base_index, t->read_index.curr, t->write_index.base_index, t->write_index.curr);
        for (i=0; i<rwc.read_sigma; i++) {
            slot = t->read_index.base_index + i;
            j = slot % rwc.read_sigma;
            log_printf(0, "[ti=%d]  i=%d slot=%d wc_span[slot]=%c read_slot=%d\n", t->index, i, slot, t->wc_span[slot], t->read_index.span[j]);
        }
    }


    if (n < 0) return(NULL);  //** Nothing to do

    t->read_index.curr++;

    t->read_index.span[n%rwc.read_sigma] = 1;
    tslot->global_index = n;
    tslot->local_index = (tslot->global_index + t->tile_start) % tile_size;
    tslot->type = 1;

    log_printf(my_tbx_log_level, "[ti=%d] span slot=%d curr=%d base=%d global=%d\n", t->index, n, t->read_index.curr, t->read_index.base_index, tslot->global_index);
    tbx_log_flush();

    slot = tslot->local_index;
    offset = base_tile[slot].offset - t->data_start;
    if (offset<0) offset += tile_bytes;
    offset += ((tslot->global_index) / tile_size) * tile_bytes;
    memset(tslot->buffer, 'A', base_tile[slot].len);
    tbx_tbuf_single(&(tslot->tbuf), base_tile[slot].len, tslot->buffer);
    ex_iovec_single(&(tslot->iov), offset, base_tile[slot].len);

    gop = io_read_gop(t, 1, &(tslot->iov), &(tslot->tbuf), 0);
    log_printf(my_tbx_log_level, "[ti=%d] global=%d off=" XOT " len=" XOT " gop=%p\n", t->index, tslot->global_index, offset, base_tile[slot].len, gop);
    tbx_log_flush();

    gop_set_private(gop, (void *)tslot);

    //** Move up the read base index
    n = total_scan_size - rwc.read_sigma;
    for (i=0; i<rwc.read_sigma; i++) {
        slot = t->read_index.base_index % rwc.read_sigma;

        if (t->read_index.span[slot] == 0) break;
        if (t->read_index.base_index >= n) break;  //** Don't move the window beyond the end
        t->read_index.span[slot] = 0;
        t->read_index.base_index++;
    }

    return(gop);
}

//*************************************************************************
// complete_write_task - Completes the task
//*************************************************************************

void complete_write_task(target_t *t, task_slot_t *tslot)
{
    log_printf(my_tbx_log_level, "[ti=%d] global=%d\n", t->index, tslot->global_index);
    t->wc_span[tslot->global_index] = '1';
}

//*************************************************************************
// complete_read_task - Completes the task
//*************************************************************************

int complete_read_task(target_t *t, task_slot_t *tslot)
{
    int err, len, off, n;
    ex_off_t goff;

    n = tslot->local_index;
    len = base_tile[n].len;
    off = base_tile[n].offset;
    err = memcmp(tslot->buffer, &(tile_data[off]), len);

    log_printf(my_tbx_log_level, "[ti=%d] Marking global=%d as complete off=%d len=%d\n", t->index, tslot->global_index, off, len);
    if (err != 0) {
        goff = (n / tile_size) * tile_bytes;
        log_printf(0, "[ti=%d] ERROR with compare! global=%d local=%d off=" XOT " len=%d global_off=" XOT "\n", t->index, tslot->global_index, tslot->local_index, tslot->iov.offset, len, goff);
        compare_buffers_print(tslot->buffer, &(tile_data[off]), len, goff, t->index);
    }

    return((err == 0) ? 0 : 1);
}

//*************************************************************************
// rw_test - Does the actual test
//*************************************************************************

void *rw_test_thread(apr_thread_t *th, void *arg)
{
    target_t *t = (target_t *)arg;
    int i, success, fail, compare_fail, j;
    int write_done, read_done;
    double flip, dtask, dops;
    gop_op_generic_t *gop;
    gop_opque_t *q;
    task_slot_t *slot;
    double op_rate, mb_rate, dsec, dt;
    apr_time_t dtw, dtr, dtt, ds_start, ds_begin, ds, dstep;
    gop_op_status_t status;

    //** Truncate the file back to the correct size if needed
    if (rwc.preallocate == 1) {
        log_printf(0, "[ti=%d] Preallocating all space\n", t->index);
        tbx_log_flush();
        j = gop_sync_exec(io_truncate_gop(t, t->total_bytes));
        if (j != OP_STATE_SUCCESS) {
            printf("[ti=%d] Error truncating the file!\n", t->index);
            tbx_log_flush();
            fflush(stdout);
            abort();
        }
    }

    q = gop_opque_new();

    t->free_slots = tbx_stack_new();  //** Slot 0 is hard coded below
    for (i=1; i<rwc.n_parallel; i++) tbx_stack_push(t->free_slots, &(t->task_list[i]));

//exit(1);

    //** Do the test
    i = 0;
    success = 0;
    fail = 0;
    compare_fail = 0;
    slot = &(t->task_list[0]);

    dops = (rwc.read_fraction > 0) ? 2*total_scan_size : total_scan_size;
    j = dops;
    log_printf(0,"[ti=%d] -------------- Starting R/W Test (%d ops) -----------------------\n", t->index, j);

    dtw = apr_time_now();
    dtt = apr_time_now();
    ds_start = apr_time_now();
    ds_begin = ds_start;

    dtr = 0;
    write_done = 0;
    read_done = 0;
    do {
        if ((i%rwc.update_interval) == 0) {
            dtask = (double )i/dops * 100.0;
            ds = apr_time_now() - ds_begin;
            dsec = (1.0*ds) / APR_USEC_PER_SEC;
            dstep = apr_time_now() - ds_start;
            dt = (1.0*dstep) / APR_USEC_PER_SEC;
            ds_start = apr_time_now();
            log_printf(0, "[ti=%d] i=%d (%lf%%) (write: %d  read: %d) (t=%lf dt=%lf)\n", t->index, i, dtask, t->write_index.curr, t->read_index.curr, dsec, dt);
        }

        gop = NULL;
        flip = my_random_double(0, 1);
        if (((flip < rwc.read_fraction) && (i>rwc.read_lag)) || (t->write_index.curr >= total_scan_size)) { //** Read op
            gop = find_read_task(t, slot, write_done);
            if (dtr == 0) dtr = apr_time_now();
        }

        if (gop == NULL) { //** Write op
            gop = find_write_task(t, slot);
        }

        tbx_log_flush();

        if (gop != NULL) {
            log_printf(1, "[ti=%d] SUBMITTING i=%d gid=%d mode=%d global=%d off=" XOT " len=" XOT "\n", t->index, i, gop_id(gop), slot->type, slot->global_index, slot->iov.offset, slot->iov.len);
            i++;
            gop_opque_add(q, gop);
        } else {
            log_printf(my_tbx_log_level, "[ti=%d] Nothing to do. Waiting for write to complete.  Read: curr=%d done=%d  Write: curr=%d done=%d nleft=%d\n", t->index, t->read_index.curr, read_done, t->write_index.curr, write_done, gop_opque_tasks_left(q));
            tbx_stack_push(t->free_slots, slot);
        }

        if ((tbx_stack_count(t->free_slots) == 0) || (gop == NULL)) {
            gop = opque_waitany(q);

            slot = gop_get_private(gop);
            status = gop_get_status(gop);

            if (gop_completed_successfully(gop) != OP_STATE_SUCCESS) {
                fail++;
                log_printf(0, "[ti=%d] FINISHED ERROR gid=%d status=%d mode=%d global=%d off=" XOT " len=" XOT "\n", t->index, gop_id(gop), status.op_status, slot->type, slot->global_index, slot->iov.offset, slot->iov.len);
            } else {
                success++;
                log_printf(1, "[ti=%d] FINISHED SUCCESS gid=%d status=%d mode=%d global=%d off=" XOT " len=" XOT "\n", t->index, gop_id(gop), status.op_status, slot->type, slot->global_index, slot->iov.offset, slot->iov.len);
            }

            if (slot->type == 0) {
                write_done++;
                if (write_done >= total_scan_size) dtw = apr_time_now() - dtw;
                complete_write_task(t, slot);
            } else {
                read_done++;
                if (read_done >= total_scan_size) dtr = apr_time_now() - dtr;
                compare_fail += complete_read_task(t, slot);
            }

            gop_free(gop, OP_DESTROY);
        } else {
            log_printf(my_tbx_log_level, "[ti=%d] tbx_stack_popping slot i=%d\n", t->index, i);
            slot = (task_slot_t *)tbx_stack_pop(t->free_slots);
        }

        log_printf(my_tbx_log_level, "[ti=%d] read_index.curr=%d (%d done) write_index.curr=%d (%d done) total_scan_size=%d\n", t->index, t->read_index.curr, read_done, t->write_index.curr, write_done, total_scan_size);
    } while ((t->read_index.curr < total_scan_size) || (t->write_index.curr < total_scan_size));

    //** Wait for the remaining tasks to complete
    while ((gop = opque_waitany(q)) != NULL) {
        slot = gop_get_private(gop);
        status = gop_get_status(gop);

        if (gop_completed_successfully(gop) != OP_STATE_SUCCESS) {
            fail++;
            log_printf(0, "[ti=%d] FINISHED ERROR gid=%d status=%d mode=%d global=%d off=" XOT " len=" XOT "\n", t->index, gop_id(gop), status.op_status, slot->type, slot->global_index, slot->iov.offset, slot->iov.len);
        } else {
            success++;
            log_printf(1, "[ti=%d] FINISHED SUCCESS gid=%d status=%d mode=%d global=%d off=" XOT " len=" XOT "\n", t->index, gop_id(gop), status.op_status, slot->type, slot->global_index, slot->iov.offset, slot->iov.len);
        }

        if (slot->type == 0) {
            write_done++;
            if (write_done >= total_scan_size) dtw = apr_time_now() - dtw;
            complete_write_task(t, slot);
        } else {
            read_done++;
            if (read_done >= total_scan_size) dtr = apr_time_now() - dtr;
            compare_fail += complete_read_task(t, slot);
        }

        log_printf(my_tbx_log_level, "[ti=%d] stragglers -- read_index.curr=%d (%d done) write_index.curr=%d (%d done) total_scan_size=%d\n", t->index, t->read_index.curr, read_done, t->write_index.curr, write_done, total_scan_size);

        gop_free(gop, OP_DESTROY);
    }

    dtt = apr_time_now() - dtt;

    log_printf(0,"[ti=%d] -------------- Completed R/W Test -----------------------\n", t->index);

    if ((fail == 0) && (compare_fail == 0))  {
        log_printf(0, "[ti=%d] R/W test:  PASSED (%d tasks)\n", t->index, success);
    } else {
        log_printf(0, "[ti=%d] R/W test:  FAILED (%d gops, %d compares)\n", t->index, fail, compare_fail);
    }
    fail += compare_fail;

    dsec = (1.0*dtw) / APR_USEC_PER_SEC;
    op_rate = (1.0*total_scan_size) / dsec;
    mb_rate = (1.0*t->total_bytes) / (dsec * 1024.0 * 1024.0);
    log_printf(0, "[ti=%d] Write performance: %lf s total --  %lf ops/s -- %lf MB/s\n", t->index, dsec, op_rate, mb_rate);

    if (rwc.read_fraction > 0) {
        dsec = (1.0*dtr) / APR_USEC_PER_SEC;
        op_rate = (1.0*total_scan_size) / dsec;
        mb_rate = (1.0*t->total_bytes) / (dsec * 1024.0 * 1024.0);
        log_printf(0, "[ti=%d] Read performance:  %lf s total --  %lf ops/s -- %lf MB/s\n", t->index, dsec, op_rate, mb_rate);

        dsec = (1.0*dtt) / APR_USEC_PER_SEC;
        op_rate = (2.0*total_scan_size) / dsec;
        mb_rate = (2.0*t->total_bytes) / (dsec * 1024.0 * 1024.0);
        log_printf(0, "[ti=%d] Run performance:   %lf s total --  %lf ops/s -- %lf MB/s\n", t->index, dsec, op_rate, mb_rate);
    }

    log_printf(0,"[ti=%d] ----------------------------------------------------------\n", t->index);

    gop_opque_free(q, OP_DESTROY);

    //** and the final verify if needed
    if (rwc.do_final_check > 0) fail += perform_final_verify(t);

    if (rwc.do_flush_check > 0) {
        log_printf(0, "[ti=%d] ============ Flushing data and doing a last verification =============\n", t->index);
        tbx_log_flush();
        dtt = apr_time_now();
        gop_sync_exec(io_flush_gop(t));
        dtt = apr_time_now() - dtt;
        dsec = (1.0*dtt) / APR_USEC_PER_SEC;
        log_printf(0, "[ti=%d] ============ Flush completed (%lf s) Dropping pages as well =============\n", t->index, dsec);
        tbx_log_flush();
        io_cache_drop(t);  //** Drop all the pages so they have to be reloaded on next test
        fail += perform_final_verify(t);
    }

    tbx_stack_free(t->free_slots, 0);
    apr_thread_exit(th, fail);
    return(NULL);
}

//*************************************************************************
// rw_load_options - Loads the test options form the config file
//*************************************************************************

void rw_load_options(tbx_inip_file_t *fd, char *group)
{
    //** Parse the global params
    rwc.n_parallel = tbx_inip_get_integer(fd, group, "parallel", 1);
    rwc.n_targets = tbx_inip_get_integer(fd, group, "n_targets", 1);
    rwc.preallocate = tbx_inip_get_integer(fd, group, "preallocate", 0);
    rwc.buffer_size = tbx_inip_get_integer(fd, group, "buffer_size", 10*1024*1024);
    rwc.file_size = tbx_inip_get_integer(fd, group, "file_size", 10*1024*1024);
    rwc.do_final_check = tbx_inip_get_integer(fd, group, "do_final_check", 1);
    rwc.do_flush_check = tbx_inip_get_integer(fd, group, "do_flush_check", 1);
    rwc.timeout = tbx_inip_get_integer(fd, group, "timeout", 10);
    rwc.update_interval = tbx_inip_get_integer(fd, group, "update_interval", 1000);
    rwc.ex_fname = tbx_inip_get_string(fd, group, "ex_file", "");
    rwc.lio_fname = tbx_inip_get_string(fd, group, "lio_file", "");
    rwc.local_fname = tbx_inip_get_string(fd, group, "local_file", "");
    rwc.read_lag = tbx_inip_get_integer(fd, group, "read_lag", 10);
    rwc.read_fraction = tbx_inip_get_double(fd, group, "read_fraction", 0.0);
    rwc.seed = tbx_inip_get_integer(fd, group, "seed", 1);
    rwc.rw_mode = tbx_inip_get_integer(fd, group, "rw_mode", RW_SEGMENT);

    my_random_seed(rwc.seed);

    rwc.min_size = tbx_inip_get_double(fd, group, "min_size", 1024*1024);
    if (rwc.min_size == 0) rwc.min_size = 1;
    rwc.max_size = tbx_inip_get_double(fd, group, "max_size", 10*1024*1024);
    if (rwc.max_size == 0) rwc.max_size = 1;
    rwc.ln_min = log(rwc.min_size);
    rwc.ln_max = log(rwc.max_size);

    rwc.read_sigma = tbx_inip_get_integer(fd, group, "read_sigma", 50);
    rwc.write_sigma = tbx_inip_get_integer(fd, group, "write_sigma", 50);

    tbx_inip_destroy(fd);
}

//*************************************************************************
// rw_print_options - Prints the options to fd
//*************************************************************************

void rw_print_options(FILE *fd, char *group)
{
    char ppbuf[100];

    fprintf(fd, "[%s]\n", group);
    fprintf(fd, "rw_mode=%d\n", rwc.rw_mode);
    fprintf(fd, "preallocate=%d\n", rwc.preallocate);
    fprintf(fd, "parallel=%d\n", rwc.n_parallel*rwc.n_targets);
    fprintf(fd, "n_targets=%d\n", rwc.n_targets);
    fprintf(fd, "buffer_size=%s\n", tbx_stk_pretty_print_int_with_scale(rwc.buffer_size, ppbuf));
    fprintf(fd, "file_size=%s\n", tbx_stk_pretty_print_int_with_scale(rwc.file_size, ppbuf));
    fprintf(fd, "do_final_check=%d\n", rwc.do_final_check);
    fprintf(fd, "do_flush_check=%d\n", rwc.do_flush_check);
    fprintf(fd, "timeout=%d\n", rwc.timeout);
    fprintf(fd, "update_interval=%d\n", rwc.update_interval);
    fprintf(fd, "ex_file=%s\n", rwc.ex_fname);
    fprintf(fd, "lio_file=%s\n", rwc.lio_fname);
    fprintf(fd, "local_file=%s\n", rwc.local_fname);
    fprintf(fd, "seed=%d\n", rwc.seed);
    fprintf(fd, "read_lag=%d\n", rwc.read_lag);
    fprintf(fd, "read_fraction=%lf\n", rwc.read_fraction);
    fprintf(fd, "min_size=%s\n", tbx_stk_pretty_print_double_with_scale(1024, rwc.min_size, ppbuf));
    fprintf(fd, "max_size=%s\n", tbx_stk_pretty_print_double_with_scale(1024, rwc.max_size, ppbuf));
    fprintf(fd, "read_sigma=%d\n", rwc.read_sigma);
    fprintf(fd, "write_sigma=%d\n", rwc.write_sigma);

    fprintf(fd, "\n");
}

//*************************************************************************
// lio_rw_test_exec - Runs the I/O tester
//*************************************************************************

int lio_rw_test_exec(int rw_mode, char *section)
{
    int err, test_errors, i;
    apr_pool_t *mpool;
    apr_thread_t **workers;
    target_t *target, *t;
    apr_status_t value;
    lio_cache_stats_get_t cs;
    int tbufsize = 10240;
    char text_buffer[tbufsize];

    apr_pool_create(&mpool, NULL);

    rwc.timeout = lio_gc->timeout;

    if (lio_gc->ifd == NULL) {
        printf("ex_rw_test:  Missing config file!\n");
        tbx_log_flush();
        fflush(stdout);
        return(-1);
    }

    if (section == NULL) section = "rw_params";

    //** Lastly load the R/W test params
    rw_load_options(lio_gc->ifd, section);

    rwc.n_parallel /= rwc.n_targets;
    if (rwc.n_parallel <= 0) rwc.n_parallel = 1;

    if (rw_mode > 0) rwc.rw_mode = rw_mode;

    //** Print the options to the screen
    printf("Configuration options: %s\n", section);
    printf("------------------------------------------------------------------\n");
    rw_print_options(stdout, section);
    printf("------------------------------------------------------------------\n\n");

    log_printf(0, "Generating tasks and random data\n");
    tbx_log_flush();
    generate_task_list();
    log_printf(0, "Completed task and data generation\n\n");
    tbx_log_flush();

    tbx_type_malloc_clear(workers, apr_thread_t *, rwc.n_targets);
    tbx_type_malloc_clear(target, target_t, rwc.n_targets);
    for (i=0; i<rwc.n_targets; i++) {
        t = target + i;
        t->index = i;
        t->tile_start = my_random_int(0, tile_size-1);
        t->da = lio_gc->da;
        t->timeout = rwc.timeout;
        t->rw_mode = rwc.rw_mode;

        //** Open the target
        io_open(t, &rwc);

        //** Truncate the file
        if (io_size(t) > 0) {
            err = gop_sync_exec(io_truncate_gop(t, 0));
            if (err != OP_STATE_SUCCESS) {
                printf("Error truncating the remote file!\n");
                tbx_log_flush();
                fflush(stdout);
                return(-3);
            }
        }

        make_test_indices(t);
        tbx_thread_create_assert(&(workers[i]), NULL, rw_test_thread, (void *)t, mpool);
    }

    //** Wait for everything to complete
    test_errors = 0;
    for (i=0; i<rwc.n_targets; i++) {
        apr_thread_join(&value, workers[i]);
        test_errors += value;

        io_close(&target[i], &rwc);
        cleanup_test_indices(&target[i]);
    }

    printf("--------------------- Cache Stats ------------------------\n");
    lio_cache_stats_get(lio_gc->cache, &cs);
    i = 0;
    lio_cache_stats_get_print(&cs, text_buffer, &i, tbufsize);
    printf("%s", text_buffer);
    printf("----------------------------------------------------------\n");

    apr_pool_destroy(mpool);
    free(workers);
    free(target);

    printf("tpc_unlimited=%d\n", lio_gc->tpc_unlimited->n_ops);

    return(test_errors);
}


