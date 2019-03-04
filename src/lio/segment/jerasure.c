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

//***********************************************************************
// Routines for managing a Jerasure segment
//    This segment drivers supports all the erasure methods in the Jerasure
//    library from Jim Plank:
//        http://web.eecs.utk.edu/~plank/plank/www/software.html
//***********************************************************************

#define _log_module_index 178

#include <apr_errno.h>
#include <apr_pools.h>
#include <apr_thread_cond.h>
#include <apr_thread_mutex.h>
#include <apr_time.h>
#include <assert.h>
#include <gop/gop.h>
#include <gop/opque.h>
#include <gop/tp.h>
#include <gop/types.h>
#include <inttypes.h>
#include <lio/segment.h>
#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <tbx/append_printf.h>
#include <tbx/assert_result.h>
#include <tbx/atomic_counter.h>
#include <tbx/iniparse.h>
#include <tbx/interval_skiplist.h>
#include <tbx/log.h>
#include <tbx/string_token.h>
#include <tbx/transfer_buffer.h>
#include <tbx/type_malloc.h>
#include <zlib.h>

#include "blacklist.h"
#include "ds.h"
#include "erasure_tools.h"
#include "ex3.h"
#include "ex3/header.h"
#include "ex3/system.h"
#include "segment/jerasure.h"
#include "segment/lun.h"
#include "service_manager.h"

#define JE_MAGIC_SIZE 4

// Forward declaration
const lio_segment_vtable_t lio_jeraseseg_vtable;

typedef struct {
    lio_segment_t *child_seg;
    lio_erasure_plan_t *plan;
    gop_thread_pool_context_t *tpc;
    lio_blacklist_t *blacklist;
    ex_off_t max_parity;
    int write_errors;
    int soft_errors;
    int hard_errors;
    int method;
    int n_data_devs;
    int n_parity_devs;
    int n_devs;
    int magic_cksum;
    int chunk_size;
    int chunk_size_with_magic;
    int stripe_size;
    int stripe_size_with_magic;
    int data_size;
    int parity_size;
    int paranoid_check;
    int w;
} segjerase_priv_t;

typedef struct {
    lio_segment_t *seg;
    data_attr_t *da;
    lio_segment_rw_hints_t *rw_hints;
    ex_tbx_iovec_t  *iov;
    ex_off_t    boff;
    ex_off_t    nbytes;
    tbx_tbuf_t  *buffer;
    int         nstripes;
    int         n_iov;
    int         rw_mode;
    int timeout;
} segjerase_rw_t;

typedef struct {
    int start_stripe;
    int iov_start;
    int nstripes;
    int straddle_start;
    int straddle_count;
} segjerase_io_t;

typedef struct {
    lio_segment_t *seg;
    data_attr_t *da;
    tbx_log_fd_t *fd;
    lio_inspect_args_t *args;
    ex_off_t bufsize;
    int max_replaced;
    int inspect_mode;
    int timeout;
    int rerror;
    int werror;
    int bad_stripes;
} segjerase_inspect_t;

typedef struct {
    segjerase_inspect_t *si;
    ex_off_t lo;
    ex_off_t hi;
    int do_print;
    int bad_stripes;
} segjerase_full_t;

typedef struct {
    lio_segment_t *sseg;
    lio_segment_t *dseg;
    gop_op_generic_t *gop;
} segjerase_clone_t;


//***********************************************************************
// je_cksum_calc - Calculates a magic checksum
//***********************************************************************

void je_cksum_calc(char *magic, char **ptr, int n_devs, int chunk_size)
{
    unsigned long cksum;
    unsigned char *m = (unsigned char *)magic;
    int i;

    cksum = adler32(0L, Z_NULL, 0);
    for (i=0; i<n_devs; i++) cksum = adler32(cksum, (unsigned char *)ptr[i], chunk_size);
    for (i=0; i<JE_MAGIC_SIZE; i++) {
        m[i] = cksum & 255;
        cksum >>= 8;
    }

}

//***********************************************************************
// je_cksum_compare - Does a magic calculation and checksum comparison
//***********************************************************************

int je_cksum_compare(char *magic, char **ptr, int n_devs, int chunk_size)
{
    char magic_calc[JE_MAGIC_SIZE];

    je_cksum_calc(magic_calc, ptr, n_devs, chunk_size);
    return((memcmp(magic, magic_calc, JE_MAGIC_SIZE) == 0) ? 0 : 1);
}

//***********************************************************************
// jerase_control_check - Does a check that the parity and data are all
//   in sync by failing  "good" chunks and doing a repair veriying the
//   the new data matches the "good" chunks.
//***********************************************************************

int jerase_control_check(lio_erasure_plan_t *plan, int chunk_size, int n_devs, int n_parity, int *badmap, char **ptr, char **eptr, char **pwork, char *magic)
{
    int erasures[n_devs+1];  //** Leave space for a control failure
    int i, n, n_control, n_ctl_max, control_index, errors;
    int control[n_parity];

    //** Determine the max number of controls
    n_ctl_max = 0;
    for (i=0; i<n_devs; i++)  n_ctl_max += badmap[i];

    //** IF we have magic and no bad blocks we can just do a checksum
    if ((magic != NULL) && (n_ctl_max == 0)) {
        return(je_cksum_compare(magic, ptr, n_devs, chunk_size));
    }

    //** Not using cksum magic or we have bad blocks that need to be reconstructed
    n_ctl_max = (magic == NULL) ? n_parity - n_ctl_max : 0;


    //** Form the erasures array
    control_index = -1;
    errors = 0;
    do {
        memcpy(eptr, ptr, sizeof(char *)*n_devs);  //** Set the base blocks using the actual data
        n_control = 0;
        n = 0;
        for (i=0; i<n_devs; i++) {
            if (((badmap[i] == 0) && (n_control < n_ctl_max) && (i > control_index)) || (badmap[i] == 1)) {
                erasures[n] = i;
                log_printf(5, "bad=%d map=%d\n", i, badmap[i]);

                if ((magic == NULL) && (badmap[i] == 0) && (n_control < n_ctl_max)) {
                    control[n_control] = i;  //** This is a control chunk
                    eptr[i] = pwork[n_control];
                    n_control++;
                    control_index = i;
                }
                n++;
            }
        }
        erasures[n] = -1;  //** It's "-1" terminated

        //** and do the decoding
        plan->decode_block(plan, eptr, chunk_size, erasures);

        if (magic != NULL) { //** Can do a chksum for validation
            log_printf(10, "magic ptr=%p\n", magic);
            return(je_cksum_compare(magic, eptr, n_devs, chunk_size));
        } else if (n_control <= 0) {  //** No cksum so do it via controls
            if (n_ctl_max > 0) {
                control_index = n_devs-1;
            } else {
                return(0);  //** No control used so just return success.  This means we had n_parity_dev erasures.
            }
        }

        //** Check if we have a match
        for (i=0; i<n_control; i++) {
            if (memcmp(ptr[control[i]], eptr[control[i]], chunk_size) != 0) {
                errors++;
                log_printf(5, "ctl=%d dev=%d BAD\n", i, control[i]);
                return(errors);  //** Got an error so kick out
            }
        }
    } while (control_index < n_devs-1);

    return(errors);
}

//***********************************************************************
//  jerase_brute_recurse - Recursively tries to find a match
//***********************************************************************

int jerase_brute_recurse(int level, int *index, lio_erasure_plan_t *plan, int chunk_size, int n_devs, int n_parity, int n_bad_devs, int *badmap, char **ptr, char **eptr, char **pwork, char *magic)
{
    int i, start, n, nbytes;
    char *tptr[n_parity];
    char logbuf[4096];
    if (level == n_bad_devs) {  //** Do an erasure check
        memset(badmap, 0, sizeof(int)*n_devs);
        for (i=0; i<n_bad_devs; i++) {  //** Overwrite the "failed" blocks using the work buffers
            tptr[i] = ptr[index[i]];
            ptr[index[i]] = pwork[i];
            badmap[index[i]] = 1;
        }

        //** Perform the check
        n = jerase_control_check(plan, chunk_size, n_devs, n_parity, badmap, ptr, eptr, &pwork[n_bad_devs], magic);

        logbuf[0] = 0;
        nbytes = 0;
        tbx_append_printf(logbuf, &nbytes, sizeof(logbuf), "jerase_control_check=%d devmap: ", n);
        for (i=0; i<n_devs; i++) {
            tbx_append_printf(logbuf, &nbytes, sizeof(logbuf), " %d", badmap[i]);
        }
        log_printf(1, "%s\n", logbuf);

        for (i=0; i<n_bad_devs; i++) {  //** Restore the original pointers
            ptr[index[i]] = tptr[i];
        }

        return(n);
    } else {
        start = (level == 0) ? 0 : index[level-1]+1;
        for (i = start; i<n_devs; i++) {
            index[level] = i;
            if (jerase_brute_recurse(level+1, index, plan, chunk_size, n_devs, n_parity, n_bad_devs, badmap, ptr, eptr, pwork, magic) == 0) return(0);
        }
    }

    return(1);  //** If made it here then no luck
}

//*************************************************************************
//  jerase_brute_recovery - Does a brute force attempt to recover the data
//     Since we don't have clue which device is bad we use 1 device as a control
//     to detect correctness.  This means we can only correct n_parity_devs-1 failures.
//**************************************************************************

int jerase_brute_recovery(lio_erasure_plan_t *plan, int chunk_size, int n_devs, int n_parity_devs, int *badmap, char **ptr, char **eptr, char **pwork, char *magic)
{
    int i, ncheck;
    int index[n_parity_devs];

    //** See if we get lucky and the initial badmap is good
    if (jerase_control_check(plan, chunk_size, n_devs, n_parity_devs, badmap, ptr, eptr, pwork, magic) == 0) return(0);

    //** No luck have to run through the perms
    memset(badmap, 0, sizeof(int)*n_devs);

    ncheck = (magic != NULL) ? n_parity_devs+1 : n_parity_devs;  //** If we have magic we don't need a control
    for (i=1; i<ncheck; i++) {  //** Cycle through checking for 1 failure, then double failure combo, etc
        memset(index, 0, sizeof(int)*n_parity_devs);
        if (jerase_brute_recurse(0, index, plan, chunk_size, n_devs, n_parity_devs, i, badmap, ptr, eptr, pwork, magic) == 0) return(0);  //** Found it so kick out
    }

    return(1);  //** No luck
}


//***********************************************************************
//  segjerase_inspect_full_func - Does a full byte-level verification of the
//     provided byte range and optionally corrects things.
//***********************************************************************

gop_op_status_t segjerase_inspect_full_func(void *arg, int id)
{
    segjerase_full_t *sf = (segjerase_full_t *)arg;
    segjerase_inspect_t *si = sf->si;
    segjerase_priv_t *s = (segjerase_priv_t *)si->seg->priv;
    gop_op_status_t status;
    gop_opque_t *q;
    lio_ex3_inspect_command_t ic;
    bool do_fix;
    int i, err, j, k, d, nstripes, total_stripes, stripe, bufstripes, n_empty, retry;
    int  fail_quick, n_iov, good_magic, unrecoverable_count, bad_count, repair_errors, erasure_errors;
    int magic_count[s->n_devs], match, index, magic_used;
    int magic_devs[s->n_devs*s->n_devs];
    int max_iov, skip, last_bad, tmp, oops;
    int badmap[s->n_devs], badmap_brute[s->n_devs], badmap_last[s->n_devs], bm_brute_used, used;
    int stripe_used[4], stripe_diag_size, stripe_buffer_size;
    int stripe_start_error[4], stripe_error[4], dstripe;
    ex_off_t nbytes, bufsize, boff, base_offset;
    tbx_tbuf_t tbuf_read, tbuf;
    char stripe_msg[4][2048], *stripe_msg_label[4];
    char *buffer, *ptr[s->n_devs], parity[s->n_parity_devs*s->chunk_size];
    char *eptr[s->n_devs], *pwork[s->n_parity_devs], *stripe_magic, *check_magic;
    char empty_magic[JE_MAGIC_SIZE];
    char magic_key[s->n_devs*JE_MAGIC_SIZE];
    char print_buffer[2048];
    char ppbufr[128], ppbufw[128], ppbufp[128];
    tbx_iovec_t *iov;
    ex_tbx_iovec_t ex_read;
    ex_tbx_iovec_t *ex_iov;
    apr_time_t now, clr_dt;
    double dtt, dtr, dtw, dtp, rater, ratew, ratep;

    // Give vals a garbage value since Alan gave it nothing
    tmp = INT_MIN;
    status = gop_success_status;

    stripe_diag_size = 4;
    stripe_buffer_size = 2048;

    memset(empty_magic, 0, JE_MAGIC_SIZE);
    q = gop_opque_new();
    status = gop_success_status;

    fail_quick = si->inspect_mode & INSPECT_FAIL_ON_ERROR;

    do_fix = 0;
    ic = si->inspect_mode & INSPECT_COMMAND_BITS;
    if ((ic == INSPECT_QUICK_REPAIR) || (ic == INSPECT_SCAN_REPAIR) || (ic == INSPECT_FULL_REPAIR)) do_fix = 1;

    base_offset = sf->lo / s->data_size;
    base_offset = base_offset * s->stripe_size_with_magic;
    nbytes = sf->hi - sf->lo + 1;
    total_stripes = nbytes / s->data_size;
    log_printf(0, "lo=" XOT " hi= " XOT " nbytes=" XOT " total_stripes=%d data_size=%d\n", sf->lo, sf->hi, nbytes, total_stripes, s->data_size);
    bufsize = (ex_off_t)total_stripes * (ex_off_t)s->stripe_size_with_magic;
    if (bufsize > si->bufsize) bufsize = si->bufsize;
    tbx_type_malloc(buffer, char, bufsize);
    bufstripes = bufsize / s->stripe_size_with_magic;

    max_iov = bufstripes;
    tbx_type_malloc(ex_iov, ex_tbx_iovec_t, bufstripes);
    tbx_type_malloc(iov, tbx_iovec_t, bufstripes);

    for (i=0; i < s->n_parity_devs; i++) pwork[i] = &(parity[i*s->chunk_size]);
    memset(badmap_last, 0, sizeof(int)*s->n_devs);

    repair_errors = 0;
    bad_count = 0;
    erasure_errors = 0;
    unrecoverable_count = 0;
    n_empty = 0;
    bm_brute_used = 0;
    last_bad = -2;
    si->rerror = si->werror = 0;

    stripe_msg_label[0] = "empty";
    stripe_msg_label[1] = "magic";
    stripe_msg_label[2] = "r-mismatch";
    stripe_msg_label[3] = "u-mismatch";
    for (k=0; k<stripe_diag_size; k++) {
        stripe_used[k] = 0;
        stripe_msg[k][0] = 0;
        stripe_start_error[k] = -1;
        stripe_error[k] = 0;
    }

    for (stripe=0; stripe<total_stripes; stripe += bufstripes) {
        retry = 0;
retry:
        dtt = apr_time_now();
        ex_read.offset = base_offset + (ex_off_t)stripe*s->stripe_size_with_magic;
        nstripes = stripe + bufstripes;
        if (nstripes > total_stripes) nstripes = total_stripes;
        nstripes = nstripes - stripe;
        ex_read.len = (ex_off_t)nstripes * s->stripe_size_with_magic;

        log_printf(0, "stripe=%d nstripes=%d total_stripes=%d offset=" XOT " len=" XOT "\n", stripe, nstripes, total_stripes, ex_read.offset, ex_read.len);
        if (sf->do_print == 1) info_printf(si->fd, 1, XIDT ": checking stripes: (%d, %d)\n", segment_id(si->seg), stripe, stripe+nstripes-1);

        //** Read the data in
        tbx_tbuf_single(&tbuf_read, ex_read.len, buffer);
        clr_dt = apr_time_now();
        memset(buffer, 0, bufsize);
        clr_dt = apr_time_now() - clr_dt;
        log_printf(5, "sid=" XIDT " clr_dt=%" PRId64 "\n", segment_id(si->seg), apr_time_sec(clr_dt));
        now = apr_time_now();
        err = gop_sync_exec(segment_read(s->child_seg, si->da, NULL, 1, &ex_read, &tbuf_read, 0, si->timeout));
        now = apr_time_now() - now;
        dtr = (double)now / APR_USEC_PER_SEC;
        rater = (double)(nstripes*s->chunk_size*s->n_data_devs)/dtr;
        if (err != OP_STATE_SUCCESS) si->rerror++;


        now = apr_time_now();
        n_iov = 0;
        nbytes = 0;
        if ((s->magic_cksum == 0) && (do_fix == 1)) {     //** Old school magic so *everything* gets written back with the updated magic
            n_iov = 1;
            nbytes = ex_read.len;
            ex_iov[0].offset = ex_read.offset;
            ex_iov[0].len = nbytes;
            iov[0].iov_base = buffer;
            iov[0].iov_len = nbytes;
        }
        for (i=0; i<nstripes; i++) {  //** Now check everything
            magic_used = 0;
            boff = i*s->stripe_size_with_magic;

            for (k=0; k < s->n_devs; k++) {
                match = -1;
                for (j=0; j<magic_used; j++) {
                    if (memcmp(&(magic_key[j*JE_MAGIC_SIZE]), &(buffer[boff + k*s->chunk_size_with_magic]), JE_MAGIC_SIZE) == 0) {
                        match = j;
                        magic_devs[j*s->n_devs + magic_count[j]] = k;
                        magic_count[j]++;
                        break;
                    }
                }

                if (match == -1) {
                    magic_devs[magic_used*s->n_devs] = k;
                    magic_count[magic_used] = 1;
                    memcpy(&(magic_key[magic_used*JE_MAGIC_SIZE]), &(buffer[boff + k*s->chunk_size_with_magic]), JE_MAGIC_SIZE);
                    magic_used++;
                }
            }

            //** See who has the quorum
            match = magic_count[0];
            index = 0;
            for (k=1; k<magic_used; k++) {
                if (match<magic_count[k]) {
                    match = magic_count[k];
                    index = k;
                }
            }

            stripe_magic = &magic_key[index*JE_MAGIC_SIZE];
            check_magic = (s->magic_cksum == 0) ? NULL : stripe_magic;
            good_magic = memcmp(empty_magic, stripe_magic, JE_MAGIC_SIZE);
            if (good_magic == 0) {
                log_printf(0, "Empty stripe.  empty chunks: %d magic_used=%d stripe=%d\n", magic_count[index], magic_used, stripe+i);
                stripe_error[0] = 1;
                if (magic_count[index] == s->n_devs) { //** Completely empty stripe so skip to the next loop
                    used = 0;
                    n_empty++;
                    goto next;
                }
            }

            if (magic_used > 1) log_printf(5, "n_magic=%d stripe=%d\n", magic_used, stripe+i);
            skip = 0;
            tmp = bad_count;
            used = 0;

            if (((good_magic == 0) && (magic_count[index] != s->n_devs)) || (magic_count[index] < s->n_data_devs)) {
                if (retry == 0) {
                    retry++;
                    log_printf(0, "unrecoverable error detected attempting to retry read stripe=%d i=%d good_magic=%d magic_count=%d\n", stripe,i, good_magic, magic_count[index]);
                    if (sf->do_print == 1) {
                        info_printf(si->fd, 1, XIDT ": unrecoverable error detected attempting to RETRY read stripe=%d i=%d good_magic=%d magic_count=%d\n",
                            segment_id(si->seg), stripe,i, good_magic, magic_count[index]);
                    }
                    apr_sleep(apr_time_from_sec(10));  //** Sleep just a bit to hopefully let things settle
                    goto retry;
                }
                unrecoverable_count++;
                bad_count++;
                log_printf(0, "unrecoverable error stripe=%d i=%d good_magic=%d magic_count=%d\n", stripe,i, good_magic, magic_count[index]);

                //** Mark the missing/bad blocks
                memset(badmap, 0, sizeof(int)*s->n_devs);
                for (k=0; k < magic_used; k++) {
                    if (k != index) {
                        match = k*s->n_devs;
                        for (j=0; j< magic_count[k]; j++) { //** Copy the magic over and mark the dev as bad
                            badmap[magic_devs[match+j]] = 1;
                        }
                    }
                }

                tbx_append_printf(stripe_msg[1], &stripe_used[1], stripe_buffer_size, "Unrecoverable error!  Matching magic:%d  Need:%d", magic_count[index], s->n_data_devs);
                stripe_error[1] = 1;
            } else {  //** Either all the data is good or we have a have a few bad blocks
                //** Make the decoding structure
                for (k=0; k < s->n_devs; k++) {
                    ptr[k] = &(buffer[boff + JE_MAGIC_SIZE + k*s->chunk_size_with_magic]);
                }

                //** Mark the missing/bad blocks
                memset(badmap, 0, sizeof(int)*s->n_devs);
                for (k=0; k < magic_used; k++) {
                    if (k != index) {
                        match = k*s->n_devs;
                        for (j=0; j< magic_count[k]; j++) { //** Copy the magic over and mark the dev as bad
                            badmap[magic_devs[match+j]] = 1;
                        }
                    }
                }

                log_printf(10, "check_magic_ptr=%p\n", check_magic);
                if (jerase_control_check(s->plan, s->chunk_size, s->n_devs, s->n_parity_devs, badmap, ptr, eptr, pwork, check_magic) != 0) {  //** See if everything checks out
                    //** Got an error so see if we can brute force a fix
                    bad_count++;
                    erasure_errors++;  //** Internal erasure error. Inconsistent data on disk

                    if (bm_brute_used == 1) memcpy(badmap, badmap_brute, sizeof(int)*s->n_devs);  //** Copy over the last brute force bad map
                    if (jerase_brute_recovery(s->plan, s->chunk_size, s->n_devs, s->n_parity_devs, badmap, ptr, eptr, pwork, check_magic) == 0) {
                        bm_brute_used = 1;
                        memcpy(badmap_brute, badmap, sizeof(int)*s->n_devs);  //** Got a correctable error


                        tbx_append_printf(stripe_msg[2], &stripe_used[2], stripe_buffer_size, "Recoverable same magic. devmap:");
                        for (d=0; d<s->n_devs; d++) {
                            tbx_append_printf(stripe_msg[2], &stripe_used[2], stripe_buffer_size, " %d", badmap[d]);
                        }
                        stripe_error[2] = 1;

                        tbx_append_printf(stripe_msg[2], &stripe_used[2], stripe_buffer_size, "\nPrinting magic table --n_magic=%d\n",magic_used);
                        for (k=0; k < magic_used; k++) {
                            match = k*s->n_devs;
                            tbx_append_printf(stripe_msg[2], &stripe_used[2], stripe_buffer_size, "%d: count=%d empty=%d devs=",k, magic_count[k], memcmp(empty_magic, &(magic_key[k*JE_MAGIC_SIZE]), JE_MAGIC_SIZE));
                            for (j=0; j< magic_count[k]; j++) { //** Copy the magic over and mark the dev as bad
                                tbx_append_printf(stripe_msg[2], &stripe_used[2], stripe_buffer_size, " %d", magic_devs[match+j]);
                            }
                            tbx_append_printf(stripe_msg[2], &stripe_used[2], stripe_buffer_size, "\n");
                        }
                    } else {
                        tbx_append_printf(stripe_msg[3], &stripe_used[3], stripe_buffer_size, "Unrecoverable data+parity mismatch.");
                        stripe_error[3] = 1;
                        skip = 1;
                        unrecoverable_count++;
                    }
                } else if (magic_count[index] != s->n_devs) {
                    bad_count++;   //** bad magic error only
                } else {
                    if (s->magic_cksum != 0) skip = 1;  //** All is good nothing to store
                }
                if ((skip == 0) && (do_fix == 1)) { //** Got some data to update
                    if (s->magic_cksum == 0) { //** Got to dump everything back to get the correct magic
                        je_cksum_calc(stripe_magic, eptr, s->n_devs, s->chunk_size);
                    }
                    for (k=0; k< s->n_devs; k++) {  //** Store the updated data back in the buffer with consistent magic
                        if ((badmap[k] == 1) || (s->magic_cksum == 0)) {
                            memcpy(&(buffer[boff + k*s->chunk_size_with_magic]), stripe_magic, JE_MAGIC_SIZE);
                            if (eptr[k] != ptr[k]) memcpy(ptr[k], eptr[k], s->chunk_size);  //** Need to copy the data/parity back to the buffer

                            if (s->magic_cksum != 0) {  //** If no adler32's for the magic we have to dump everything
                                ex_iov[n_iov].offset = ex_read.offset + i*s->stripe_size_with_magic + k*s->chunk_size_with_magic;

                                log_printf(0, "offset=" XOT " k=%d n_iov=%d max_iov=%d\n", ex_iov[n_iov].offset, k, n_iov, max_iov);
                                ex_iov[n_iov].len = s->chunk_size_with_magic;
                                iov[n_iov].iov_base = &(buffer[boff + k*s->chunk_size_with_magic]);
                                log_printf(0, "memcmp=%d\n", memcmp(iov[n_iov].iov_base, &(buffer[boff + index*s->chunk_size_with_magic]), JE_MAGIC_SIZE));
                                iov[n_iov].iov_len = ex_iov[n_iov].len;
                                nbytes += ex_iov[n_iov].len;
                                n_iov++;
                                if (n_iov >= max_iov) {
                                    max_iov = 1.5 * max_iov + 1;
                                    ex_iov = (ex_tbx_iovec_t *)realloc(ex_iov, sizeof(ex_tbx_iovec_t) * max_iov);
                                    iov = (tbx_iovec_t *)realloc(iov, sizeof(tbx_iovec_t) * max_iov);
                                }
                            }
                        }
                    }
                }
            }

next:  //** Jump to here if an empty stripe

            if ((tbx_stack_get_info_level(si->fd) > 1) && (tmp != bad_count)) {   //** Print some diag info if needed
                oops = (((repair_errors+unrecoverable_count) > 0) && (fail_quick > 0) && (i== nstripes-1)) ? 1 : 0;

                if ((stripe+i-1 != last_bad) || (memcmp(badmap_last, badmap, sizeof(int)*s->n_devs) != 0)) {
                    tbx_append_printf(print_buffer, &used, sizeof(print_buffer), XIDT ": [DEVMAP] stripe=%d   devmap:", segment_id(si->seg), stripe+i);
                    for (k=0; k<s->n_devs; k++) {
                        tbx_append_printf(print_buffer, &used, sizeof(print_buffer), " %d", badmap[k]);
                    }
                    info_printf(si->fd, 1, "%s\n", print_buffer);
                    memcpy(badmap_last, badmap, sizeof(int)*s->n_devs);
                }
                last_bad = stripe+i;


                for (k=0; k<stripe_diag_size; k++) {
                    if (stripe_error[k] == 1) {
                        if ((stripe_start_error[k] == -1) || (oops == 1)) {  //** 1st time for error
                            stripe_start_error[k] = stripe+i;
                            info_printf(si->fd, 1, XIDT ": [START:%s] stripe=%d %s\n", segment_id(si->seg), stripe_msg_label[k], stripe+i, stripe_msg[k]);
                        }

                    } else if (stripe_start_error[k] != -1) { //** End of error
                        dstripe = stripe+i-1 - stripe_start_error[k] + 1;
                        info_printf(si->fd, 1, XIDT ": [END:  %s] stripe=%d (%d-%d=%d) %s\n", segment_id(si->seg), stripe_msg_label[k], stripe+i-1, stripe_start_error[k], stripe+i-1, dstripe, stripe_msg[k]);
                        stripe_start_error[k] = -1;
                        stripe_msg[k][0] = 0;
                    }

                    stripe_used[k] = 0;
                    stripe_error[k] = 0;
                }
            }

        }

        now = apr_time_now() - now;
        dtp = (double)now / APR_USEC_PER_SEC;
        ratep = (dtp == 0) ? 0 : (double)(nstripes*s->chunk_size*s->n_data_devs)/dtp;

        //** Perform any updates if needed
        now = apr_time_now();
        if (n_iov > 0) {
            tbx_tbuf_vec(&tbuf, nbytes, n_iov, iov);
            err = gop_sync_exec(segment_write(s->child_seg, si->da, NULL, n_iov, ex_iov, &tbuf, 0, si->timeout));
            log_printf(0, "gop_status=%d nbytes=" XOT " n_iov=%d\n", err, nbytes, n_iov);
            if (err != OP_STATE_SUCCESS) {
                if (sf->do_print == 1) info_printf(si->fd, 1, XIDT ": Write update error for stripe! Probably a corrupt allocation.\n", segment_id(si->seg));
                repair_errors++;
                si->werror++;
            }
        }
        now = apr_time_now() - now;
        dtw = (double)now / APR_USEC_PER_SEC;
        ratew = (dtw == 0) ? 0 : (double)(nbytes)/dtw;

        dtt = apr_time_now() - dtt;
        dtt = (double)dtt / APR_USEC_PER_SEC;

        if (sf->do_print == 1) {
            info_printf(si->fd, 1, XIDT ": R[%lf sec %s/s] P[%lf sec %s/s] W[%lf sec %s/s] T[%lf sec] bad stripe count: %d  --- Repair errors: %d   Unrecoverable errors:%d  Empty stripes: %d   Silent errors: %d\n",
                        segment_id(si->seg), dtr, tbx_stk_pretty_print_double_with_scale(1024, rater, ppbufr), dtp, tbx_stk_pretty_print_double_with_scale(1024, ratep, ppbufp),
                        dtw, tbx_stk_pretty_print_double_with_scale(1024, ratew, ppbufw), dtt, bad_count, repair_errors, unrecoverable_count, n_empty, erasure_errors);
        }

        if (((repair_errors+unrecoverable_count) > 0) && (fail_quick > 0)) {
            log_printf(1,"FAIL_QUICK:  Hit an unrecoverable error\n");
            break;
        }
    }

    free(buffer);
    free(ex_iov);
    free(iov);
    gop_opque_free(q, OP_DESTROY);

    sf->bad_stripes = bad_count;
    status.error_code = INSPECT_RESULT_FULL_CHECK;
    if ((unrecoverable_count > 0) || (repair_errors > 0)) status.error_code |= INSPECT_RESULT_HARD_ERROR;
    if ((do_fix == 0) && (bad_count > (unrecoverable_count+repair_errors))) status.error_code |= INSPECT_RESULT_SOFT_ERROR;

    status.op_status = OP_STATE_SUCCESS;
    if ((unrecoverable_count > 0) || (repair_errors > 0) || ((bad_count > 0) && (do_fix == 0))) status.op_status = OP_STATE_FAILURE;
    return(status);
}

//***********************************************************************
// segjerase_inspect_full - Generates the command for doing a full scan and repair
//***********************************************************************

gop_op_generic_t *segjerase_inspect_full(segjerase_inspect_t *si, int do_print, ex_off_t lo, ex_off_t hi)
{
    segjerase_priv_t *s = (segjerase_priv_t *)si->seg->priv;
    segjerase_full_t *sf;
    gop_op_generic_t *gop;

    tbx_type_malloc(sf, segjerase_full_t, 1);
    sf->si = si;
    sf->lo = lo;
    sf->hi = hi;
    sf->do_print = do_print;
    sf->bad_stripes = 0;

    gop = gop_tp_op_new(s->tpc, NULL, segjerase_inspect_full_func, (void *)sf, free, 1);
    gop_set_private(gop, sf);

    return(gop);
}

//***********************************************************************
// segjerase_inspect_scan - Just runs through the segment checking that the
//    magic tags are consistent and optionally corrects them
//***********************************************************************

gop_op_status_t segjerase_inspect_scan(segjerase_inspect_t *si)
{
    segjerase_priv_t *s = (segjerase_priv_t *)si->seg->priv;
    gop_op_status_t status;
    gop_opque_t *q;
    gop_op_generic_t *gop;
    ex_off_t fsize, off, lo, hi;
    int maxstripes, curr_stripe, i, j, moff, magic_stripe, n_iov, start_stripe, stripe, total_stripes, n_empty;
    char *magic, empty_magic[JE_MAGIC_SIZE];
    int start_bad, do_fix, err, bad_count, empty, bufsize;
    tbx_iovec_t *iov;
    ex_tbx_iovec_t *ex_iov;
    tbx_tbuf_t tbuf;
    segjerase_full_t *sf;
    int error_code = 0;

    memset(empty_magic, 0, JE_MAGIC_SIZE);

    q = gop_opque_new();
//  opque_start_execution(q);
    status = gop_success_status;

    do_fix = 0;
    i = si->inspect_mode & INSPECT_COMMAND_BITS;
    if ((i == INSPECT_QUICK_REPAIR) || (i == INSPECT_SCAN_REPAIR) || (i == INSPECT_FULL_REPAIR)) do_fix = 1;

    fsize = segment_size(si->seg);
    maxstripes = 1024;
    bufsize = s->n_devs * maxstripes * JE_MAGIC_SIZE;
    tbx_type_malloc(magic, char, bufsize);
    tbx_type_malloc(iov, tbx_iovec_t, s->n_devs*maxstripes);
    tbx_type_malloc(ex_iov, ex_tbx_iovec_t, s->n_devs*maxstripes);

    memset(magic, 0, bufsize);

    magic_stripe = s->n_devs * JE_MAGIC_SIZE;
    total_stripes = fsize / s->data_size;
    start_stripe = 0;
    curr_stripe = 0;
    n_iov = 0;
    off = 0;
    bad_count = 0;
    n_empty = 0;

    log_printf(0, "fsize=" XOT " data_size=%d total_stripes=%d\n", fsize, s->data_size, total_stripes);

    for (stripe=0; stripe <= total_stripes; stripe++) {
        if ((curr_stripe >= maxstripes) || (stripe == total_stripes)) {
            info_printf(si->fd, 1, XIDT ": checking stripes: (%d, %d)\n", segment_id(si->seg), start_stripe, start_stripe+curr_stripe-1);

            log_printf(0, "i=%d n_iov=%d size=%d\n", i, n_iov, n_iov*JE_MAGIC_SIZE);
            tbx_tbuf_vec(&tbuf, n_iov*JE_MAGIC_SIZE, n_iov, iov);
            gop_sync_exec(segment_read(s->child_seg, si->da, NULL, n_iov, ex_iov, &tbuf, 0, si->timeout));

            //** Check for errors and fire off repairs
            moff = 0;
            start_bad = -1;
            for (i=0; i<curr_stripe; i++) {
                err = 0;
                for (j=1; j < s->n_devs; j++) {
                    if (memcmp(&(magic[moff]), &(magic[moff+j*JE_MAGIC_SIZE]), JE_MAGIC_SIZE) != 0) {
                        err = 1;
                        break;
                    }
                }

                log_printf(0, " i=%d err=%d\n", i, err);
                empty = 0;
                if (err == 0) {
                    if (memcmp(empty_magic, &(magic[moff]), JE_MAGIC_SIZE) == 0) {
                        empty = 1;
                        n_empty++;
                        info_printf(si->fd, 10, "Empty stripe=%d s=%d i=%d\n", start_stripe+i, start_stripe, i);
                    }
                }
                log_printf(0, " i=%d after empty check empty=%d\n", i, empty);

                if (err == 0) {  //** It's good see if we need to generate a new task
                    if ((do_fix == 1) && (start_bad != -1)) {
                        lo = (start_stripe + start_bad) * s->data_size;
                        hi = (start_stripe + i) * s->data_size - 1;
                        gop = segjerase_inspect_full(si, 0, lo, hi);
                        gop_opque_add(q, gop);
                        start_bad = -1;
                    }
                } else {
                    bad_count++;
                    if (start_bad == -1) start_bad = i;
                    log_printf(0, " i=%d bad=%d start_bad=%d\n", i, bad_count, start_bad);

                }

                moff += magic_stripe;
            }

            //** Handle any leftover errors
            if ((do_fix == 1) && (start_bad != -1)) {
                lo = (start_stripe + start_bad) * s->data_size;
                hi = (start_stripe + curr_stripe) * s->data_size - 1;
                gop = segjerase_inspect_full(si, 0, lo, hi);
                gop_opque_add(q, gop);
            }

            //** Wait for any pending tasks to complete

            if (gop_opque_task_count(q) > 0) {
                err = opque_waitall(q);
                if (err != OP_STATE_SUCCESS) {
                    status.op_status = OP_STATE_FAILURE;
                }

                //** Cycle through getting all the error code
                while ((gop = opque_waitany(q)) != NULL) {
                    status = gop_get_status(gop);
                    error_code |= status.error_code;
                    sf = gop_get_private(gop);
                    bad_count += sf->bad_stripes;
                    gop_free(gop, OP_DESTROY);
                }
            }

            //** Reset for the next round
            curr_stripe = 0;
            start_stripe = stripe;
            n_iov = 0;

            info_printf(si->fd, 1, XIDT ": bad stripe count: %d  Empty stripes: %d\n", segment_id(si->seg), bad_count, n_empty);
        }

        if (stripe < total_stripes) {
            //** Generate the iolist
            for (i=0; i< s->n_devs; i++) {
                ex_iov[n_iov].offset = off;
                ex_iov[n_iov].len = JE_MAGIC_SIZE;
                iov[n_iov].iov_base = &(magic[n_iov*JE_MAGIC_SIZE]);
                iov[n_iov].iov_len = JE_MAGIC_SIZE;
                n_iov++;
                off += s->chunk_size_with_magic;
            }

            curr_stripe++;
        }
    }

    free(magic);
    free(iov);
    free(ex_iov);

    gop_opque_free(q, OP_DESTROY);

    si->bad_stripes = bad_count;
    if ((do_fix == 0) && (bad_count > 0)) {
        status = gop_failure_status;
    }

    log_printf(0, "do_fix=%d bad_count=%d\n", do_fix, bad_count);

    return(status);
}

//***********************************************************************
// segjerase_inspect_func - Checks that all the segments are available and they are the right size
//     and corrects them if requested
//***********************************************************************

gop_op_status_t segjerase_inspect_func(void *arg, int id)
{
    segjerase_inspect_t *si = (segjerase_inspect_t *)arg;
    segjerase_priv_t *s = (segjerase_priv_t *)si->seg->priv;
    segjerase_full_t *sf;
    gop_op_status_t status;
    int option, total_stripes, child_replaced, loop, i, migrate_errors;
    gop_op_generic_t *gop;
    int max_loops = 10;

    info_printf(si->fd, 1, XIDT ": jerase segment maps to child " XIDT "\n", segment_id(si->seg), segment_id(s->child_seg));
    info_printf(si->fd, 1, XIDT ": segment information: method=%s data_devs=%d parity_devs=%d chunk_size=%d  used_size=" XOT " magic_cksum=%d write_errors=%d mode=%d\n",
                segment_id(si->seg), JE_method[s->method], s->n_data_devs, s->n_parity_devs, s->chunk_size, segment_size(s->child_seg),  s->magic_cksum, s->write_errors, si->inspect_mode);

    //** Issue the inspect for the underlying LUN
    info_printf(si->fd, 1, XIDT ": Inspecting child segment...\n", segment_id(si->seg));
    gop = segment_inspect(s->child_seg, si->da, si->fd, si->inspect_mode, si->bufsize, si->args, si->timeout);
    gop_waitall(gop);
    status = gop_get_status(gop);
    gop_free(gop, OP_DESTROY);
    si->max_replaced = (status.error_code & INSPECT_RESULT_COUNT_MASK);  //** NOTE: This needs to be checks for edge cases.
    si->bad_stripes = -1;
    child_replaced = si->max_replaced;
    migrate_errors = status.error_code & INSPECT_RESULT_MIGRATE_ERROR;

    log_printf(5, "status: %d %d migerr=%d\n", status.op_status, status.error_code, migrate_errors);

    //** Kick out if we can't fix anything
    if (((status.op_status != OP_STATE_SUCCESS) && (child_replaced > s->n_parity_devs)) || (migrate_errors != 0)) {
        status.op_status = OP_STATE_FAILURE;
        goto fail;
    }

    log_printf(5, "child_replaced =%d ndata=%d\n", child_replaced, s->n_parity_devs);
    total_stripes = segment_size(si->seg) / s->data_size;

    //** The INSPECT_QUICK_* options are handled by the LUN driver. If force_reconstruct is set then we probably need to do a scan
    option = si->inspect_mode & INSPECT_COMMAND_BITS;
//  force_reconstruct = si->inspect_mode & INSPECT_FORCE_REPAIR;
    log_printf(5, "child_replaced=%d option=%d inspect_mode=%d INSPECT_QUICK_REPAIR=%d\n", child_replaced, option, si->inspect_mode, INSPECT_QUICK_REPAIR);
    if (((child_replaced > 0) || (s->magic_cksum == 0) || (s->write_errors > 0)) && (option == INSPECT_QUICK_REPAIR)) {
        info_printf(si->fd, 1, XIDT ": Child segment repaired or existing write errors.  Forcing a full file check.\n", segment_id(si->seg));
        si->inspect_mode -= option;
        option = INSPECT_FULL_REPAIR;
        si->inspect_mode += option;
    }

    option = si->inspect_mode & INSPECT_COMMAND_BITS;

    switch (option) {
    case (INSPECT_SCAN_CHECK):
    case (INSPECT_SCAN_REPAIR):
        info_printf(si->fd, 1, XIDT ": Total number of stripes:%d\n", segment_id(si->seg), total_stripes);
        status = segjerase_inspect_scan(si);
        break;
    case (INSPECT_FULL_CHECK):
    case (INSPECT_FULL_REPAIR):
        info_printf(si->fd, 1, XIDT ": Total number of stripes:%d\n", segment_id(si->seg), total_stripes);
        loop = 0;
        child_replaced = 0;
        do {
            gop =  segjerase_inspect_full(si, 1, 0, segment_size(si->seg));
            gop_waitall(gop);
            status = gop_get_status(gop);
            sf = gop_get_private(gop);
            si->bad_stripes = sf->bad_stripes;
            gop_free(gop, OP_DESTROY);

            if ((status.op_status != OP_STATE_SUCCESS) && (loop < max_loops-1)) {
                if (((si->inspect_mode & INSPECT_FIX_READ_ERROR) && (si->rerror > 0)) ||
                        ((si->inspect_mode & INSPECT_FIX_WRITE_ERROR) && (si->werror > 0))) {
                    loop++;
                    info_printf(si->fd, 1, "seg = " XIDT "Encountered Read or write errors.  Attempting to correct them.  loop=%d write_errors=%d read_errors=%d\n", segment_id(si->seg), loop, si->rerror, si->werror);
                    si->rerror = si->werror = 0;

                    //** Issue the inspect for the underlying LUN
                    info_printf(si->fd, 1, "seg = " XIDT "Inspecting child segment...\n", segment_id(si->seg));
                    gop = segment_inspect(s->child_seg, si->da, si->fd, si->inspect_mode, si->bufsize, si->args, si->timeout);
                    gop_waitall(gop);
                    status = gop_get_status(gop);
                    gop_free(gop, OP_DESTROY);
                    ///si->max_replaced += (status.error_code & INSPECT_RESULT_COUNT_MASK);  //** NOTE: This needs to be checks for edge cases.
                    child_replaced = 0;
                    log_printf(5, "n_dev_rows=%d\n", si->args->n_dev_rows);
                    for (i=0; i<si->args->n_dev_rows; i++) {
                        log_printf(5, "dev_row_replaced[%d]=%d\n", i, si->args->dev_row_replaced[i]);
                        if (si->args->dev_row_replaced[i] > child_replaced) child_replaced = si->args->dev_row_replaced[i];
                    }
                    if (child_replaced > si->max_replaced) si->max_replaced = child_replaced;

                    log_printf(5, "status: %d %d\n", status.op_status, status.error_code);

                    //** Kick out if we can't fix anything
                    if ((status.op_status != OP_STATE_SUCCESS) || (child_replaced > s->n_parity_devs)) {
                        status.op_status = OP_STATE_FAILURE;
                        loop = 10000;
                    }

                } else {
                    loop = 10000;  //** Kick out
                }
            } else {
                loop = 10000;  //** Kick out
            }
        } while (loop < max_loops);

        if (status.op_status == OP_STATE_SUCCESS)  {
            s->write_errors = 0;  //** Clear the write errors since we did a full successfull check
            if (option == INSPECT_FULL_REPAIR) s->magic_cksum = 1;  //** Did a successfull full repair which would convert s to cksum magics
        }
        break;
    }

fail:
    if (si->max_replaced > s->n_data_devs) {
        status.op_status = OP_STATE_FAILURE;
    } else if (((child_replaced > 0) && ((si->inspect_mode & INSPECT_FORCE_REPAIR) == 0)) || (migrate_errors != 0)) {
        status.op_status = OP_STATE_FAILURE;
    }

    if (status.op_status == OP_STATE_SUCCESS) {
        info_printf(si->fd, 1, XIDT ": status: SUCCESS (%d devices, %d stripes)\n", segment_id(si->seg), si->max_replaced, si->bad_stripes);
    } else {
        info_printf(si->fd, 1, XIDT ": status: FAILURE (%d devices, %d stripes)\n", segment_id(si->seg), si->max_replaced, si->bad_stripes);
    }

    return(status);
}

//***********************************************************************
//  segjerase_inspect_func - Does the actual segment inspection operations
//***********************************************************************

gop_op_generic_t *segjerase_inspect(lio_segment_t *seg, data_attr_t *da, tbx_log_fd_t *fd, int mode, ex_off_t bufsize, lio_inspect_args_t *args, int timeout)
{
    segjerase_priv_t *s = (segjerase_priv_t *)seg->priv;
    gop_op_generic_t *gop;
    gop_op_status_t err;
    segjerase_inspect_t *si;
    lio_ex3_inspect_command_t option;

    gop = NULL;
    option = mode & INSPECT_COMMAND_BITS;
    switch (option) {
    case (INSPECT_QUICK_CHECK):
    case (INSPECT_SCAN_CHECK):
    case (INSPECT_FULL_CHECK):
    case (INSPECT_QUICK_REPAIR):
    case (INSPECT_SCAN_REPAIR):
    case (INSPECT_FULL_REPAIR):
        tbx_type_malloc(si, segjerase_inspect_t, 1);
        si->seg = seg;
        si->da = da;
        si->fd = fd;
        si->inspect_mode = mode;
        si->bufsize = bufsize;
        si->timeout = timeout;
        si->args = args;
        gop = gop_tp_op_new(s->tpc, NULL, segjerase_inspect_func, (void *)si, free, 1);
        break;
    case (INSPECT_MIGRATE):
        info_printf(fd, 1, XIDT ": jerase segment maps to child " XIDT "\n", segment_id(seg), segment_id(s->child_seg));
        gop = segment_inspect(s->child_seg, da, fd, mode, bufsize, args, timeout);
        break;
    case (INSPECT_SOFT_ERRORS):
        err.error_code = s->soft_errors;
        err.op_status = (err.error_code == 0) ? OP_STATE_SUCCESS : OP_STATE_FAILURE;
        gop = gop_dummy(err);
        break;
    case (INSPECT_HARD_ERRORS):
        err.error_code = s->hard_errors;
        err.op_status = (err.error_code == 0) ? OP_STATE_SUCCESS : OP_STATE_FAILURE;
        gop = gop_dummy(err);
        break;
    case (INSPECT_WRITE_ERRORS):
        gop = segment_inspect(s->child_seg, da, fd, mode, bufsize, args, timeout);
        break;
    case (INSPECT_NO_CHECK):
        break;
    }

    return(gop);
}

//*******************************************************************************
// segjerase_clone_func - Does the clone function
//*******************************************************************************

gop_op_status_t segjerase_clone_func(void *arg, int id)
{
    segjerase_clone_t *cop = (segjerase_clone_t *)arg;
    gop_op_status_t status;

    status = (gop_waitall(cop->gop) == OP_STATE_SUCCESS) ? gop_success_status : gop_failure_status;
    gop_free(cop->gop, OP_DESTROY);

    return(status);
}


//***********************************************************************
// segjerase_clone - Clones a segment
//***********************************************************************

gop_op_generic_t *segjerase_clone(lio_segment_t *seg, data_attr_t *da, lio_segment_t **clone_seg, int mode, void *attr, int timeout)
{
    lio_segment_t *clone;
    lio_segment_t *child = NULL;
    lio_erasure_plan_t *cplan = NULL;
    segjerase_priv_t *ss = (segjerase_priv_t *)seg->priv;
    segjerase_priv_t *sd;
    segjerase_clone_t *cop;
    ex_off_t nbytes;
    int use_existing = (*clone_seg != NULL) ? 1 : 0;

    tbx_type_malloc(cop, segjerase_clone_t, 1);

    //** Make the base segment
    if (use_existing == 0) *clone_seg = segment_jerasure_create(seg->ess);
    clone = *clone_seg;
    sd = (segjerase_priv_t *)clone->priv;

    //** Do a base copy
    if (use_existing == 1) {
        cplan = sd->plan;
        child = sd->child_seg;
    }
    *sd = *ss;

    if (mode == CLONE_STRUCTURE) sd->magic_cksum = 1;  //** If only cloning the structure we always enble storing a cksum for the magic

    log_printf(15, "use_existing=%d sseg=" XIDT " dseg=" XIDT "\n", use_existing, segment_id(seg), segment_id(clone));
    if (use_existing == 1) {
        sd->child_seg = child;
        sd->plan = cplan;
    } else {   //** Need to contstruct a plan
        sd->child_seg = NULL;

        //** Make the ET plan
        nbytes = sd->n_data_devs * sd->chunk_size;
        sd->plan = et_generate_plan(nbytes, sd->method, sd->n_data_devs, sd->n_parity_devs, sd->w, -1, -1);
        if (sd->plan == NULL) {
            log_printf(0, "seg=" XIDT " No plan generated!\n", segment_id(seg));
            return(gop_dummy(gop_failure_status));
        } else {
            sd->plan->form_encoding_matrix(sd->plan);
            sd->plan->form_decoding_matrix(sd->plan);
        }

        //** Copy the header
        if (seg->header.name != NULL) clone->header.name = strdup(seg->header.name);
    }

    cop->sseg = seg;
    cop->dseg = clone;
    //** Jerase doesn't manage any data.  The child does so clone it
    cop->gop = segment_clone(ss->child_seg, da, &(sd->child_seg), mode, attr, timeout);
    log_printf(5, "child_clone gid=%d\n", gop_id(cop->gop));

    return(gop_tp_op_new(ss->tpc, NULL, segjerase_clone_func, (void *)cop, free, 1));
}


//***********************************************************************
//  segjerase_read_func - Reads the stripes
//    NOTE: 1) Assumes only 1 writer/stripe!  Otherwise you get a race condition.
//          2) Assumes a single iov/stripe
//          These should be enfoced automatically if called from the segment_cache driver
//***********************************************************************

gop_op_status_t segjerase_read_func(void *arg, int id)
{
    segjerase_rw_t *sw = (segjerase_rw_t *)arg;
    segjerase_priv_t *s = (segjerase_priv_t *)sw->seg->priv;
    gop_op_status_t status, op_status, check_status;
    ex_off_t lo, boff, poff, len, parity_len, parity_used, curr_bytes;
    int i, j, k, m, stripe, magic_used, slot, n_iov, nstripes, curr_stripe, iov_start, magic_stripe, magic_off, quick;
    char *parity, *magic, *ptr[s->n_devs], *eptr[s->n_devs];
    char pbuff[s->n_parity_devs*s->chunk_size];
    char *pwork[s->n_parity_devs];
    char magic_key[s->n_devs*JE_MAGIC_SIZE], empty_magic[JE_MAGIC_SIZE], *stripe_magic;
    int magic_count[s->n_devs], data_ok, match, index;
    int magic_devs[s->n_devs*s->n_devs];
    int badmap[s->n_devs], badmap_brute[s->n_devs], bm_brute_used;
    int soft_error, hard_error, do_recover, paranoid_mode;
    char **straddle_buffer, *straddle_ptr;
    ex_off_t *straddle_offset;
    int straddle_size, straddle_used, straddle_count;
    tbx_tbuf_t straddle_tbuf;
    gop_opque_t *q;
    gop_op_generic_t *gop;
    ex_tbx_iovec_t *ex_iov;
    tbx_tbuf_t *tbuf;
    lio_segment_rw_hints_t *rw_hints;
    tbx_iovec_t *iov;
    segjerase_io_t *info;
    tbx_tbuf_var_t tbv;
    int loop;

    loop = 0;
    memset(empty_magic, 0, JE_MAGIC_SIZE);

    // Init vals with garbage since Alan didn't
    status = gop_success_status;
    op_status = gop_success_status;
    nstripes = INT_MIN;
    len = INT_MIN;
    lo = INT_MIN;

tryagain:  //** We first try allowing blacklisting to proceed as normal and then start over if that fails

    q = gop_opque_new();
    tbx_tbuf_var_init(&tbv);
    magic_stripe = JE_MAGIC_SIZE*s->n_devs;
    status = gop_success_status;
    soft_error = 0;
    hard_error = 0;
    bm_brute_used = 0;

    straddle_buffer = NULL;
    straddle_offset = NULL;
    straddle_ptr = NULL;
    straddle_size = -1;
    straddle_used = -1;

    //** Make the space for the parity
    parity_len = sw->nstripes * s->parity_size;
    if (s->max_parity < parity_len) {
        parity_len = s->max_parity;
        for (i=0; i < sw->n_iov; i++) {
            len = sw->iov[i].len;
            nstripes = len / s->data_size;
            j = nstripes*s->parity_size;
            if (j > parity_len) parity_len = j;
        }

        if (parity_len > s->max_parity) {
            log_printf(1, "seg=" XIDT " Parity to small.  Growing to parity_len=" XOT " s->max_parity=" XOT "\n", segment_id(sw->seg), parity_len, s->max_parity);
        }
    }
    tbx_type_malloc(parity, char, parity_len);

    tbx_type_malloc_clear(magic, char, magic_stripe*sw->nstripes);
    tbx_type_malloc(ex_iov, ex_tbx_iovec_t, sw->n_iov);
    tbx_type_malloc(iov, tbx_iovec_t, 2*sw->nstripes*s->n_devs);
    tbx_type_malloc(tbuf, tbx_tbuf_t, sw->n_iov);
    tbx_type_malloc_clear(rw_hints, lio_segment_rw_hints_t, sw->n_iov);
    tbx_type_malloc(info, segjerase_io_t, sw->n_iov);

    //** Set up the blacklist structure
    if (sw->rw_hints == NULL) {
        match = (loop == 0) ? s->n_parity_devs : 0;
    } else {
        log_printf(5, "rw_hints->lun_max_blacklist=%d loop=%d\n", rw_hints->lun_max_blacklist, loop);

        if (loop == 0) {
            match = (sw->rw_hints->lun_max_blacklist > s->n_parity_devs) ? s->n_parity_devs : sw->rw_hints->lun_max_blacklist;
        } else {
            match = 0;
        }
    }

    for (i=0; i<sw->n_iov; i++) rw_hints[i].lun_max_blacklist = match;

    log_printf(5, "rw_hints=%p lun_max_blacklist=%d\n", sw->rw_hints, rw_hints[0].lun_max_blacklist);

    for (i=0; i < s->n_parity_devs; i++) pwork[i] = &(pbuff[i*s->chunk_size]);

    //** Cycle through the tasks
    parity_used = 0;
    curr_stripe = 0;
    n_iov = 0;
    curr_bytes = 0;
    for (i=0; i<=sw->n_iov; i++) {
        if (i<sw->n_iov) {  //** Kludgy check so we don't have to copy the waitany code twice
            lo = sw->iov[i].offset / s->data_size;
            lo = lo * s->stripe_size_with_magic;
            len = sw->iov[i].len;
            nstripes = len / s->data_size;
            j = nstripes*s->parity_size;
        } else {
            j = 0;
        }

        if (((j+parity_used) > parity_len) || (i==sw->n_iov)) {  //** Filled the buffer so wait for the current tasks to complete
            while ((gop = opque_waitany(q)) != NULL) {
                slot = gop_get_myid(gop);
                check_status = gop_get_status(gop);
                paranoid_mode = (check_status.error_code != 0) ? 1 : s->paranoid_check;  //** Force paranoid check for underlying read issues

                //** Make the magic table to determine which magic has a quorum
                iov_start = info[slot].iov_start;
                for (stripe=0; stripe < info[slot].nstripes; stripe++) {
                    magic_used = 0;
                    for (k=0; k < s->n_devs; k++) {
                        match = -1;
                        for (j=0; j<magic_used; j++) {
                            if (memcmp(&(magic_key[j*JE_MAGIC_SIZE]), iov[iov_start + 2*k].iov_base, JE_MAGIC_SIZE) == 0) {
                                match = j;
                                magic_devs[j*s->n_devs + magic_count[j]] = k;
                                log_printf(15, "(m) mindex=%d count=%d dev=%d (slot=%d)\n", j, magic_count[j]+1, k, j*s->n_devs + magic_count[j]);
                                magic_count[j]++;
                                break;
                            }
                        }

                        if (match == -1) {
                            magic_devs[magic_used*s->n_devs] = k;
                            magic_count[magic_used] = 1;
                            memcpy(&(magic_key[magic_used*JE_MAGIC_SIZE]), iov[iov_start + 2*k].iov_base, JE_MAGIC_SIZE);
                            magic_used++;
                        }
                    }

                    //** See who has the quorum
                    match = magic_count[0];
                    index = 0;
                    for (k=1; k<magic_used; k++) {
                        if (match<magic_count[k]) {
                            match = magic_count[k];
                            index = k;
                        }
                    }

                    data_ok = 1;
                    if (magic_count[index] != s->n_devs) {
                        j = 0;
                        match = index*s->n_devs;
                        for (k=0; k<magic_count[index]; k++) {
                            if (magic_devs[match+k] < s->n_data_devs) j++;
                        }
                        if (j != s->n_data_devs) data_ok = 0;
                    } else if (memcmp(empty_magic, &(magic_key[index*JE_MAGIC_SIZE]), JE_MAGIC_SIZE) == 0) {
                        data_ok = ((magic_count[index] == s->n_devs) && (check_status.error_code < s->n_parity_devs)) ? 2 : -1;
                    }

                    do_recover = 0;
                    if (data_ok == 1) {   //** All magics are the same
                        if (paranoid_mode == 1) do_recover = 1;  //** Paranoid mode
                    } else if (data_ok == 2) { //** no data stored so blank it
                        for (k=0; k<s->n_data_devs; k++) {
                            memset(iov[iov_start + 2*k + 1].iov_base, 0, s->chunk_size);
                        }
                    } else {  //** Missing some blocks
                        op_status = gop_get_status(gop);
                        if ((magic_count[index] < s->n_data_devs) || (data_ok == -1)) {  //** Unrecoverable error
                            log_printf(5, "seg=" XIDT " ERROR with read off=" XOT " len=" XOT " n_parity=%d good=%d error_code=%d\n",
                                       segment_id(sw->seg), sw->iov[slot].offset, sw->iov[slot].len, s->n_parity_devs, magic_count[index], op_status.error_code);
                            status.op_status = OP_STATE_FAILURE;
                            status.error_code = op_status.error_code;
                            hard_error = 1;
                        } else {  //** Recoverable
                            log_printf(5, "seg=" XIDT " recoverable write error off=" XOT " len= "XOT " n_parity=%d good=%d error_code=%d magic_used=%d index=%d magic_count[index]=%d\n",
                                       segment_id(sw->seg), sw->iov[slot].offset, sw->iov[slot].len, s->n_parity_devs, magic_count[index], op_status.error_code, magic_used, index, magic_count[index]);
                            if (hard_error == 0) status.error_code = op_status.error_code;
                            soft_error = 1;
                            do_recover = 1;
                        }
                    }

                    if (do_recover == 1) {
                        //** Make the decoding structure
                        for (k=0; k < s->n_devs; k++) {
                            ptr[k] = iov[iov_start + 2*k + 1].iov_base;
                        }

                        //** Mark the missing/bad blocks
                        memset(badmap, 0, sizeof(int)*s->n_devs);
                        for (k=0; k < magic_used; k++) {
                            if (k != index) {
                                match = k*s->n_devs;
                                for (j=0; j< magic_count[k]; j++) { //** Copy the magic over and mark the dev as bad
                                    badmap[magic_devs[match+j]] = 1;
                                }
                            }
                        }

                        stripe_magic = (s->magic_cksum == 0) ? NULL : &magic_key[index*JE_MAGIC_SIZE];  //** Determine how we validate
                        if (jerase_control_check(s->plan, s->chunk_size, s->n_devs, s->n_parity_devs, badmap, ptr, eptr, pwork, stripe_magic) != 0) {  //** See if everything checks out
                            //** Got an error so see if we can brute force a fix
                            if (bm_brute_used == 1) memcpy(badmap, badmap_brute, sizeof(int)*s->n_devs);  //** Copy over the last brute force bad map
                            if (jerase_brute_recovery(s->plan, s->chunk_size, s->n_devs, s->n_parity_devs, badmap, ptr, eptr, pwork, stripe_magic) == 0) {
                                bm_brute_used = 1;
                                memcpy(badmap_brute, badmap, sizeof(int)*s->n_devs);  //** Got a correctable error
                                for (k=0; k<s->n_data_devs; k++) {
                                    if (eptr[k] != ptr[k]) memcpy(ptr[k], eptr[k], s->chunk_size);  //** Need to copy the data back to the buffer
                                }
                            } else {
                                log_printf(5, "seg=" XIDT " ERROR with read off=" XOT " len=" XOT " n_parity=%d good=%d error_code=%d\n",
                                           segment_id(sw->seg), sw->iov[slot].offset, sw->iov[slot].len, s->n_parity_devs, magic_count[index], op_status.error_code);
                                status.op_status = OP_STATE_FAILURE;
                                status.error_code = op_status.error_code;
                                hard_error = 1;
                            }
                        }
                    }

                    iov_start += 2*s->n_devs;
                }

                //**Have to copy data spanning multiple pages back
                if (info[slot].straddle_count > 0) {
                    m = info[slot].straddle_start;
                    for (k=0; k<info[slot].straddle_count; k++) {
                        tbx_tbuf_single(&straddle_tbuf, s->data_size, straddle_buffer[m+k]);
                        tbx_tbuf_copy(&straddle_tbuf, 0, sw->buffer, straddle_offset[m+k], s->data_size, 1);
                    }
                }
                gop_free(gop, OP_DESTROY);
            }

            parity_used = 0;
            curr_stripe = 0;
            n_iov = 0;
        }

        if (i < sw->n_iov) {  //** Kludgy check so we don't have to copy the waitany code twice
            //** Construct the next ops base
            ex_iov[i].offset = lo;
            ex_iov[i].len = nstripes * s->stripe_size_with_magic;

            iov_start = n_iov;

            info[i].iov_start = iov_start;
            info[i].start_stripe = curr_stripe;
            info[i].nstripes = nstripes;
            info[i].straddle_start = (i>0) ? info[i-1].straddle_start + info[i-1].straddle_count : 0;
            straddle_count = 0;

            //** Cycle through the stripes for encoding
            boff = sw->boff + curr_bytes;
            magic_off = curr_stripe*magic_stripe;
            for (j=0; j<nstripes; j++) {
                tbv.nbytes = s->data_size;
                tbx_tbuf_next(sw->buffer, boff, &tbv);
                quick = 1;
                if ((tbv.n_iov != 1) || ((int)tbv.nbytes != s->data_size)) {
                    quick = 0;
                    log_printf(0, "OOPS n_iov=%d nbytes=%d boff=" XOT " iov=%d\n", tbv.n_iov, (int)tbv.nbytes, boff, i);
                    if (straddle_used >= straddle_size) {
                        straddle_size = 2*straddle_size + 10;
                        straddle_offset = realloc(straddle_offset, sizeof(ex_off_t) * straddle_size);
                        straddle_buffer = realloc(straddle_buffer, sizeof(char *) * straddle_size);
                    }

                    straddle_used++;
                    tbx_type_malloc(straddle_ptr, char, s->data_size);
                    straddle_buffer[straddle_used] = straddle_ptr;
                    straddle_offset[straddle_used] = boff;
                    straddle_count++;
                    log_printf(0, "OOPS STRADDLE used=%d boff=" XOT " count=%d\n", straddle_used, straddle_offset[straddle_used], straddle_count);
                }

                //** Make the encoding and transfer data structs
                poff = 0;
                for (k=0; k<s->n_data_devs; k++) {
                    iov[n_iov].iov_base = &(magic[magic_off]);
                    iov[n_iov].iov_len = JE_MAGIC_SIZE;
                    n_iov++;
                    magic_off += JE_MAGIC_SIZE;

                    if (quick == 1) {
                        iov[n_iov].iov_base = tbv.buffer[0].iov_base + poff;
                        iov[n_iov].iov_len = s->chunk_size;
                        n_iov++;
                    } else {
                        iov[n_iov].iov_base = straddle_ptr + poff;
                        iov[n_iov].iov_len = s->chunk_size;
                        n_iov++;
                    }
                    poff += s->chunk_size;
                }

                poff = 0;
                for (k=0; k<s->n_parity_devs; k++) {
                    iov[n_iov].iov_base = &(magic[magic_off]);
                    iov[n_iov].iov_len = JE_MAGIC_SIZE;
                    n_iov++;
                    magic_off += JE_MAGIC_SIZE;
                    iov[n_iov].iov_base = &(parity[curr_stripe*s->parity_size + poff]);
                    iov[n_iov].iov_len = s->chunk_size;
                    n_iov++;
                    poff += s->chunk_size;
                }

                curr_stripe++;
                boff += s->data_size;
            }

            info[i].straddle_count = straddle_count;
            boff = sw->boff + curr_bytes;
            tbx_tbuf_vec(&(tbuf[i]), ex_iov[i].len, n_iov - iov_start, &(iov[iov_start]));
            gop = segment_read(s->child_seg, sw->da, &(rw_hints[i]), 1, &(ex_iov[i]), &(tbuf[i]), 0, sw->timeout);
            gop_set_myid(gop, i);
            gop_opque_add(q, gop);
            curr_bytes += len;
            parity_used += nstripes * s->parity_size;
        }
    }


    //** Clean up
    free(parity);
    free(magic);
    free(ex_iov);
    free(iov);
    free(tbuf);
    free(rw_hints);
    free(info);

    if (straddle_size > 0) {
        for (i=0; i<(straddle_used+1); i++) free(straddle_buffer[i]);
        free(straddle_buffer);
        free(straddle_offset);
    }

    gop_opque_free(q, OP_DESTROY);

    //** See if we need to retry without blacklisting enabled
    if ((hard_error > 0) && (s->blacklist) && (loop == 0)) {
        log_printf(5, "sid=" XIDT " RETRY Looks like we failed to read with blacklisting enabled so trying again\n", segment_id(sw->seg));
        loop++;
        goto tryagain;
    }

    if ((soft_error+hard_error) > 0) {
        segment_lock(sw->seg);
        if (soft_error > 0) s->soft_errors++;
        if (hard_error > 0) s->hard_errors++;
        segment_unlock(sw->seg);
    }

    return(status);
}

//***********************************************************************
//  segjerase_write_func - Writes the stripes
//    NOTE: 1) Assumes only 1 writer/stripe!  Otherwise you get a race condition.
//          2) Assumes a single iov/stripe
//          These should be enfoced automatically if called from the segment_cache driver
//***********************************************************************

gop_op_status_t segjerase_write_func(void *arg, int id)
{
    segjerase_rw_t *sw = (segjerase_rw_t *)arg;
    segjerase_priv_t *s = (segjerase_priv_t *)sw->seg->priv;
    gop_op_status_t status, op_status;
    ex_off_t lo, boff, poff, len, parity_len, parity_used, curr_bytes;
    int i, j, k, n_iov, nstripes, curr_stripe, pstripe, iov_start, quick;
    int soft_error, hard_error;
    char **straddle_buffer, *straddle_ptr;
    ex_off_t *straddle_offset;
    int straddle_size, straddle_used;
    tbx_tbuf_t straddle_tbuf;
    char *parity, *magic, **ptr, *stripe_magic, *empty;
    gop_opque_t *q;
    gop_op_generic_t *gop;
    ex_tbx_iovec_t *ex_iov;
    tbx_tbuf_t *tbuf;
    tbx_iovec_t *iov;
    lio_segment_rw_hints_t *rw_hints;
    tbx_tbuf_var_t tbv;
    int loop;
    loop = 0;

    // Give some garbage initial values since Alan didn't give any
    nstripes = INT_MIN;
    len = INT_MIN;
    lo = INT_MIN;

tryagain: //** In case blacklisting failed we'll retry with it disabled

    q = gop_opque_new();
    tbx_tbuf_var_init(&tbv);
    status = gop_success_status;
    soft_error = 0;
    hard_error = 0;

    straddle_buffer = NULL;
    straddle_offset = NULL;
    straddle_ptr = NULL;
    straddle_size = -1;
    straddle_used = -1;

    //** Make the space for the parity
    parity_len = sw->nstripes * s->parity_size;
    if (s->max_parity < parity_len) {
        parity_len = s->max_parity;
        for (i=0; i < sw->n_iov; i++) {
            len = sw->iov[i].len;
            nstripes = len / s->data_size;
            j = nstripes*s->parity_size;
            if (j > parity_len) parity_len = j;
        }

        if (parity_len > s->max_parity) {
            log_printf(1, "Parity to small.  Growing to parity_len=" XOT " s->max_parity=" XOT "\n", parity_len, s->max_parity);
        }
    }
    tbx_type_malloc(parity, char, parity_len + s->chunk_size);
    empty = NULL;

    tbx_type_malloc_clear(magic, char, JE_MAGIC_SIZE*sw->nstripes);
    tbx_type_malloc(ptr, char *, sw->nstripes*s->n_devs);
    tbx_type_malloc(ex_iov, ex_tbx_iovec_t, sw->n_iov);
    tbx_type_malloc(iov, tbx_iovec_t, 2*sw->nstripes*s->n_devs);
    tbx_type_malloc(tbuf, tbx_tbuf_t, sw->n_iov);
    tbx_type_malloc_clear(rw_hints, lio_segment_rw_hints_t, sw->n_iov);


    //** Set up the blacklist structure
    if (sw->rw_hints == NULL) {
        k = (loop == 0) ? s->n_parity_devs : 0;
    } else {
        log_printf(0, "rw_hints->lun_max_blacklist=%d loop=%d\n", rw_hints->lun_max_blacklist, loop);

        if (loop == 0) {
            k = (sw->rw_hints->lun_max_blacklist > s->n_parity_devs) ? s->n_parity_devs : sw->rw_hints->lun_max_blacklist;
        } else {
            k = 0;
        }
    }

    for (i=0; i<sw->n_iov; i++) rw_hints[i].lun_max_blacklist = k;

    //** Cycle through the tasks
    parity_used = 0;
    curr_stripe = 0;
    pstripe = 0;
    n_iov = 0;
    curr_bytes = 0;
    for (i=0; i<=sw->n_iov; i++) {
        if (i<sw->n_iov) {  //** Kludgy check so we don't have to copy the waitany code twice
            lo = sw->iov[i].offset / s->data_size;
            lo = lo * s->stripe_size_with_magic;
            len = sw->iov[i].len;
            nstripes = len / s->data_size;
            j = nstripes*s->parity_size;
        } else {
            j = 0;
        }

        if (((j+parity_used) > parity_len) || (i==sw->n_iov)) {  //** Filled the buffer so wait for the current tasks to complete
            while ((gop = opque_waitany(q)) != NULL) {
                j = gop_get_myid(gop);
                if (gop_completed_successfully(gop) != OP_STATE_SUCCESS) {
                    op_status = gop_get_status(gop);
                    if (op_status.error_code > s->n_parity_devs) {
                        log_printf(5, "seg=" XIDT " ERROR with write off=" XOT " len= "XOT " n_parity=%d n_failed=%d\n",
                                   segment_id(sw->seg), sw->iov[j].offset, sw->iov[j].len, s->n_parity_devs, op_status.error_code);
                        status = op_status;
                        hard_error = 1;
                    } else {
                        log_printf(5, "seg=" XIDT " recoverable write error off=" XOT " len= "XOT " n_parity=%d n_failed=%d\n",
                                   segment_id(sw->seg), sw->iov[j].offset, sw->iov[j].len, s->n_parity_devs, op_status.error_code);
                        status.error_code = op_status.error_code;
                        soft_error = 1;
                    }
                }

                gop_free(gop, OP_DESTROY);
            }
            parity_used = 0;
            curr_stripe = 0;
            pstripe = 0;
            n_iov = 0;
        }

        if (i < sw->n_iov) {  //** Kludgy check so we don't have to copy the waitany code twice
            //** Construct the next ops base
            ex_iov[i].offset = lo;
            ex_iov[i].len = nstripes * s->stripe_size_with_magic;

            iov_start = n_iov;

            //** Cycle through the stripes for encoding
            boff = sw->boff + curr_bytes;
            for (j=0; j<nstripes; j++) {
                tbv.nbytes = s->data_size;
                tbx_tbuf_next(sw->buffer, boff, &tbv);
                quick = 1;
                if ((tbv.n_iov != 1) || ((int)tbv.nbytes != s->data_size)) {
                    log_printf(0, "OOPS n_iov=%d nbytes=%d boff=" XOT " iov=%d\n", tbv.n_iov, (int)tbv.nbytes, boff, i);
                    if (straddle_used >= straddle_size) {
                        straddle_size = 2*straddle_size + 10;
                        straddle_offset = realloc(straddle_offset, sizeof(ex_off_t) * straddle_size);
                        straddle_buffer = realloc(straddle_buffer, sizeof(char *) * straddle_size);
                    }

                    straddle_used++;
                    tbx_type_malloc(straddle_ptr, char, s->data_size);
                    straddle_buffer[straddle_used] = straddle_ptr;
                    straddle_offset[straddle_used] = boff;
                    tbx_tbuf_single(&straddle_tbuf, s->data_size, straddle_ptr);
                    tbx_tbuf_copy(sw->buffer, boff, &straddle_tbuf, 0, s->data_size, 1);
                    log_printf(0, "OOPS STRADDLE used=%d boff=" XOT "\n", straddle_used, straddle_offset[straddle_used]);
                }

                //** Make the encoding and transfer data structs
                stripe_magic = &(magic[curr_stripe*JE_MAGIC_SIZE]);
                poff = 0;
                for (k=0; k<s->n_data_devs; k++) {
                    if (tbv.buffer[0].iov_base != NULL) {
			if (quick == 1) {
	                        ptr[pstripe + k] = tbv.buffer[0].iov_base + poff;
			} else {
	                        ptr[pstripe + k] = straddle_ptr + poff;
			}
                    } else { //** Got an error page
                        if (empty == NULL) {
                            empty = &(parity[parity_len]);
                            memset(empty, 0, s->chunk_size);
                        }
                        //ptr[pstripe+k] = NULL;  //** Segfault for debugging QWERT
                        log_printf(0, "seg=" XIDT " ERROR NULL ptr! dev=%d\n", segment_id(sw->seg), k);
                        fprintf(stderr, "seg=" XIDT " ERROR NULL ptr! dev=%d\n", segment_id(sw->seg), k);
                        ptr[pstripe + k] = empty;
                    }
                    iov[n_iov].iov_base = stripe_magic;
                    iov[n_iov].iov_len = JE_MAGIC_SIZE;
                    n_iov++;
                    iov[n_iov].iov_base = ptr[pstripe+k];
                    iov[n_iov].iov_len = s->chunk_size;
                    n_iov++;
                    poff += s->chunk_size;
                }

                for (k=0; k<s->n_parity_devs; k++) {
                    ptr[pstripe + s->n_data_devs + k] =  &(parity[parity_used]);
                    iov[n_iov].iov_base = stripe_magic;
                    iov[n_iov].iov_len = JE_MAGIC_SIZE;
                    n_iov++;
                    iov[n_iov].iov_base = ptr[pstripe + s->n_data_devs + k];
                    iov[n_iov].iov_len = s->chunk_size;
                    n_iov++;
                    parity_used += s->chunk_size;
                }

                //** Encode the data
                s->plan->encode_block(s->plan, &(ptr[pstripe]), s->chunk_size);

                //** Calculate the magic/cksum
                je_cksum_calc(stripe_magic, &ptr[pstripe], s->n_devs, s->chunk_size);

                curr_stripe++;
                pstripe += s->n_devs;
                boff += s->data_size;
            }

            boff = sw->boff + curr_bytes;
            tbx_tbuf_vec(&(tbuf[i]), nstripes*s->stripe_size_with_magic, n_iov - iov_start, &(iov[iov_start]));
            gop = segment_write(s->child_seg, sw->da, &(rw_hints[i]), 1, &(ex_iov[i]), &(tbuf[i]), 0, sw->timeout);
            gop_set_myid(gop, i);
            gop_opque_add(q, gop);
            curr_bytes += len;
        }
    }


    //** Clean up
    free(parity);
    free(magic);
    free(ptr);
    free(ex_iov);
    free(iov);
    free(tbuf);
    free(rw_hints);

    if (straddle_size > 0) {
        for (i=0; i<(straddle_used+1); i++) free(straddle_buffer[i]);
        free(straddle_buffer);
        free(straddle_offset);
    }

    gop_opque_free(q, OP_DESTROY);

    //** See if we need to retry without blacklisting enabled
    if ((hard_error > 0) && (s->blacklist) && (loop == 0)) {
        log_printf(5, "sid=" XIDT " RETRY Looks like we failed to write with blacklisting enabled so trying again\n", segment_id(sw->seg));
        loop++;
        goto tryagain;
    }

    if ((soft_error+hard_error) > 0) {
        segment_lock(sw->seg);
        s->write_errors = 1;
        s->paranoid_check = 1;
        if (soft_error > 0) s->soft_errors++;
        if (hard_error > 0) s->hard_errors++;
        segment_unlock(sw->seg);
    }


    return(status);
}

//***********************************************************************
// segjerase_write - Performs a segment write operation
//***********************************************************************

gop_op_generic_t *segjerase_write(lio_segment_t *seg, data_attr_t *da, lio_segment_rw_hints_t *rw_hints, int n_iov, ex_tbx_iovec_t *iov, tbx_tbuf_t *buffer, ex_off_t boff, int timeout)
{
    segjerase_priv_t *s = (segjerase_priv_t *)seg->priv;
    segjerase_rw_t *sw;
    gop_op_generic_t *gop;
    ex_off_t rem_pos, rem_len, nbytes;
    int i, nstripes;

    //** 1st verify the ops occur on whole rows
    nbytes = 0;
    nstripes = 0;
    for (i=0; i<n_iov; i++) {
        nbytes += iov[i].len;
        nstripes += iov[i].len / s->data_size;
        rem_pos = iov[i].offset % s->data_size;
        rem_len = iov[i].len % s->data_size;
        if ((rem_pos != 0) || (rem_len != 0)) {
            log_printf(1, "seg=" XIDT " offset/len not on stripe boundary!  data_size=%d off[%d]=%" PRId64 " len[%d]=%" PRId64 "\n",
                       segment_id(seg), s->data_size, i, iov[i].offset, i, iov[i].len);
            return(NULL);
        }
    }

    //** I/O is on stripe boundaries so proceed
    tbx_type_malloc(sw, segjerase_rw_t, 1);
    sw->seg = seg;
    sw->da = da;
    sw->rw_hints = rw_hints;
    sw->nbytes = nbytes;
    sw->nstripes= nstripes;
    sw->n_iov = n_iov;
    sw->iov = iov;
    sw->boff = boff;
    sw->buffer = buffer;
    sw->timeout = timeout;
    sw->rw_mode = 1;
    gop = gop_tp_op_new(s->tpc, NULL, segjerase_write_func, (void *)sw, free, 1);

    return(gop);
}


//***********************************************************************
// segjerase_read - Performs a segment read operation
//***********************************************************************

gop_op_generic_t *segjerase_read(lio_segment_t *seg, data_attr_t *da, lio_segment_rw_hints_t *rw_hints, int n_iov, ex_tbx_iovec_t *iov, tbx_tbuf_t *buffer, ex_off_t boff, int timeout)
{
    segjerase_priv_t *s = (segjerase_priv_t *)seg->priv;
    segjerase_rw_t *sw;
    gop_op_generic_t *gop;
    ex_off_t rem_pos, rem_len, nbytes;
    int i, nstripes;

    //** 1st verify the ops occur on whole rows
    nstripes = 0;
    nbytes = 0;
    for (i=0; i<n_iov; i++) {
        nbytes += iov[i].len;
        nstripes += iov[i].len / s->data_size;
        rem_pos = iov[i].offset % s->data_size;
        rem_len = iov[i].len % s->data_size;
        if ((rem_pos != 0) || (rem_len != 0)) {
            log_printf(1, "seg=" XIDT " offset/len not on stripe boundary!  data_size=%d off[%d]=%" PRId64 " len[%d]=%" PRId64 "\n",
                       segment_id(seg), s->data_size, i, iov[i].offset, i, iov[i].len);
            return(NULL);
        }
    }


    //** I/O is on stripe boundaries so proceed

    tbx_type_malloc(sw, segjerase_rw_t, 1);
    sw->seg = seg;
    sw->da = da;
    sw->rw_hints = rw_hints;
    sw->n_iov = n_iov;
    sw->nbytes = nbytes;
    sw->nstripes = nstripes;
    sw->iov = iov;
    sw->boff = boff;
    sw->buffer = buffer;
    sw->timeout = timeout;
    sw->rw_mode = 1;
    gop = gop_tp_op_new(s->tpc, NULL, segjerase_read_func, (void *)sw, free, 1);

    return(gop);
}



//***********************************************************************
// segjerase_flush - Flushes a segment
//***********************************************************************

gop_op_generic_t *segjerase_flush(lio_segment_t *seg, data_attr_t *da, ex_off_t lo, ex_off_t hi, int timeout)
{
    return(gop_dummy(gop_success_status));
}

//***********************************************************************
// segjerase_remove - Removes the segment.
//***********************************************************************

gop_op_generic_t *segjerase_remove(lio_segment_t *seg, data_attr_t *da, int timeout)
{
    segjerase_priv_t *s = (segjerase_priv_t *)seg->priv;

    return(segment_remove(s->child_seg, da, timeout));
}

//***********************************************************************
// segjerase_truncate - Truncates (or grows) the segment
//***********************************************************************

gop_op_generic_t *segjerase_truncate(lio_segment_t *seg, data_attr_t *da, ex_off_t new_size, int timeout)
{
    segjerase_priv_t *s = (segjerase_priv_t *)seg->priv;
    ex_off_t tweaked_size, abs_size;

    //** Round to the nearest whole row
    abs_size = (new_size < 0) ? -new_size : new_size;  //** If new_Size is negative then we have a reserve call

    tweaked_size = abs_size / s->data_size;
    if ((abs_size % s->data_size) > 0) tweaked_size++;
    tweaked_size *= s->stripe_size_with_magic;

    if (new_size == 0) s->magic_cksum = 1;  //** Enable magic_cksums if not already set

    if (new_size < 0) tweaked_size = - tweaked_size;  //** Reserve call
    return(lio_segment_truncate(s->child_seg, da, tweaked_size, timeout));
}

//***********************************************************************
// segjerase_block_size - Returns the segment block size.
//***********************************************************************

ex_off_t segjerase_block_size(lio_segment_t *seg)
{
    segjerase_priv_t *s = (segjerase_priv_t *)seg->priv;
    return(s->data_size);
}

//***********************************************************************
// segjerase_size - Returns the segment size.
//***********************************************************************

ex_off_t segjerase_size(lio_segment_t *seg)
{
    segjerase_priv_t *s = (segjerase_priv_t *)seg->priv;
    ex_off_t nbytes;

    nbytes = segment_size(s->child_seg) / s->stripe_size_with_magic;
    nbytes = nbytes * s->data_size;

    return(nbytes);
}

//***********************************************************************
// segjerase_signature - Generates the segment signature
//***********************************************************************

int segjerase_signature(lio_segment_t *seg, char *buffer, int *used, int bufsize)
{
    segjerase_priv_t *s = (segjerase_priv_t *)seg->priv;

    tbx_append_printf(buffer, used, bufsize, "jerase(\n");
    tbx_append_printf(buffer, used, bufsize, "    method=%s\n", JE_method[s->method]);
    tbx_append_printf(buffer, used, bufsize, "    n_data_devs=%d\n", s->n_data_devs);
    tbx_append_printf(buffer, used, bufsize, "    n_parity_devs=%d\n", s->n_parity_devs);
    tbx_append_printf(buffer, used, bufsize, "    chunk_size=%d\n", s->chunk_size);
    tbx_append_printf(buffer, used, bufsize, "    magic_cksum=%d\n", s->magic_cksum);
    tbx_append_printf(buffer, used, bufsize, "    w=%d\n", s->w);
    tbx_append_printf(buffer, used, bufsize, ")\n");

    return(segment_signature(s->child_seg, buffer, used, bufsize));
}


//***********************************************************************
// segjerase_serialize_text -Convert the segment to a text based format
//***********************************************************************

int segjerase_serialize_text(lio_segment_t *seg, lio_exnode_exchange_t *exp)
{
    segjerase_priv_t *s = (segjerase_priv_t *)seg->priv;
    int bufsize=10*1024;
    char segbuf[bufsize];
    char *etext;
    int sused;
    lio_exnode_exchange_t *child_exp;

    segbuf[0] = 0;
    child_exp = lio_exnode_exchange_create(EX_TEXT);

    sused = 0;

    //** Store the child segment 1st
    segment_serialize(s->child_seg, child_exp);

    //** Store the segment header
    tbx_append_printf(segbuf, &sused, bufsize, "[segment-" XIDT "]\n", seg->header.id);
    if ((seg->header.name != NULL) && (strcmp(seg->header.name, "") != 0)) {
        etext = tbx_stk_escape_text("=", '\\', seg->header.name);
        tbx_append_printf(segbuf, &sused, bufsize, "name=%s\n", etext);
        free(etext);
    }
    tbx_append_printf(segbuf, &sused, bufsize, "type=%s\n", SEGMENT_TYPE_JERASURE);

    //** And the params
    tbx_append_printf(segbuf, &sused, bufsize, "segment=" XIDT "\n", segment_id(s->child_seg));
    tbx_append_printf(segbuf, &sused, bufsize, "method=%s\n", JE_method[s->method]);
    tbx_append_printf(segbuf, &sused, bufsize, "n_data_devs=%d\n", s->n_data_devs);
    tbx_append_printf(segbuf, &sused, bufsize, "n_parity_devs=%d\n", s->n_parity_devs);
    tbx_append_printf(segbuf, &sused, bufsize, "chunk_size=%d\n", s->chunk_size);
    tbx_append_printf(segbuf, &sused, bufsize, "w=%d\n", s->w);
    tbx_append_printf(segbuf, &sused, bufsize, "max_parity=" XOT "\n", s->max_parity);
    tbx_append_printf(segbuf, &sused, bufsize, "magic_cksum=%d\n", s->magic_cksum);

    if (s->write_errors > 0) {
        tbx_append_printf(segbuf, &sused, bufsize, "write_errors=%d\n", s->write_errors);
    }

    //** Merge the exnodes together
    exnode_exchange_append_text(exp, segbuf);
    exnode_exchange_append(exp, child_exp);

    //** Clean up the child
    lio_exnode_exchange_destroy(child_exp);

    return(0);
}

//***********************************************************************
// segjerase_serialize_proto -Convert the segment to a protocol buffer
//***********************************************************************

int segjerase_serialize_proto(lio_segment_t *seg, lio_exnode_exchange_t *exp)
{
    return(-1);
}

//***********************************************************************
// segjerase_serialize -Convert the segment to a more portable format
//***********************************************************************

int segjerase_serialize(lio_segment_t *seg, lio_exnode_exchange_t *exp)
{
    if (exp->type == EX_TEXT) {
        return(segjerase_serialize_text(seg, exp));
    } else if (exp->type == EX_PROTOCOL_BUFFERS) {
        return(segjerase_serialize_proto(seg, exp));
    }

    return(-1);
}

//***********************************************************************
// segjerase_deserialize_text -Read the text based segment
//***********************************************************************

int segjerase_deserialize_text(lio_segment_t *seg, ex_id_t id, lio_exnode_exchange_t *exp)
{
    segjerase_priv_t *s = (segjerase_priv_t *)seg->priv;
    lio_seglun_priv_t *slun;
    int bufsize=1024;
    int nbytes;
    char seggrp[bufsize];
    char *text;
    tbx_inip_file_t *fd;

    //** Parse the ini text
    fd = exp->text.fd;

    //** Make the segment section name
    snprintf(seggrp, bufsize, "segment-" XIDT, id);

    //** Get the segment header info
    seg->header.id = id;
    seg->header.type = SEGMENT_TYPE_JERASURE;
    seg->header.name = tbx_inip_get_string(fd, seggrp, "name", "");

    //** Load the child segemnt (should be a LUN segment)
    id = tbx_inip_get_integer(fd, seggrp, "segment", 0);
    if (id == 0) {
        return (-1);
    }

    s->child_seg = load_segment(seg->ess, id, exp);
    if (s->child_seg == NULL) {
        return(-2);
    }

    //** Load the params
    s->write_errors = tbx_inip_get_integer(fd, seggrp, "write_errors", 0);
    if ((s->paranoid_check == 0) && (s->write_errors > 0)) s->paranoid_check = 1;

    s->magic_cksum = tbx_inip_get_integer(fd, seggrp, "magic_cksum", 0);
    if (s->magic_cksum == 0) {
        if (segment_size(s->child_seg) == 0) s->magic_cksum = 1;  //** If empty file enable adler32 magic
    }
    s->n_data_devs = tbx_inip_get_integer(fd, seggrp, "n_data_devs", 6);
    s->n_parity_devs = tbx_inip_get_integer(fd, seggrp, "n_parity_devs", 3);
    s->n_devs = s->n_data_devs + s->n_parity_devs;
    s->w = tbx_inip_get_integer(fd, seggrp, "w", -1);
    s->max_parity = tbx_inip_get_integer(fd, seggrp, "max_parity", 16*1024*1024);
    s->chunk_size = tbx_inip_get_integer(fd, seggrp, "chunk_size", 16*1024);
    s->stripe_size = s->chunk_size * s->n_devs;
    s->data_size = s->chunk_size * s->n_data_devs;
    s->parity_size = s->chunk_size * s->n_parity_devs;
    s->chunk_size_with_magic = s->chunk_size + JE_MAGIC_SIZE;
    s->stripe_size_with_magic = s->chunk_size_with_magic * s->n_devs;
    text = tbx_inip_get_string(fd, seggrp, "method", (char *)JE_method[CAUCHY_GOOD]);
    s->method = et_method_type(text);
    free(text);
    if (s->method < 0) return(-3);

    //** From the seg we can determine the other params (and sanity check input)
    if (strcmp(s->child_seg->header.type, SEGMENT_TYPE_LUN) != 0) {
        log_printf(0, "Child segment not type LUN!  got=%s\n", s->child_seg->header.type);
        return(-4);
    }
    slun = (lio_seglun_priv_t *)s->child_seg->priv;

    if (slun->n_devices != (s->n_data_devs + s->n_parity_devs)) {
        log_printf(0, "Child n_devices(%d) != n_data_devs(%d) + n_parity_devs(%d)!\n", slun->n_devices, s->n_data_devs, s->n_parity_devs);
        return(-5);
    }

    if (slun->chunk_size != (s->chunk_size + JE_MAGIC_SIZE)) {
        log_printf(0, "Child chunk_size(%" PRId64 ") != JE chunksize(%d) + JE_MAGIC_SIZE(%d)!\n", slun->chunk_size, s->chunk_size, JE_MAGIC_SIZE);
        tbx_inip_destroy(fd);
        return(-6);
    }

    nbytes = s->n_data_devs * s->chunk_size;
    s->plan = et_generate_plan(nbytes, s->method, s->n_data_devs, s->n_parity_devs, s->w, -1, -1);
    if (s->plan == NULL) {
        log_printf(0, "seg=" XIDT " No plan generated!\n", segment_id(seg));
        return(-7);
    }
    s->plan->form_encoding_matrix(s->plan);
    s->plan->form_decoding_matrix(s->plan);

    return(0);
}

//***********************************************************************
// segjerase_deserialize_proto - Read the prot formatted segment
//***********************************************************************

int segjerase_deserialize_proto(lio_segment_t *seg, ex_id_t id, lio_exnode_exchange_t *exp)
{
    return(-1);
}

//***********************************************************************
// segjerase_deserialize -Convert from the portable to internal format
//***********************************************************************

int segjerase_deserialize(lio_segment_t *seg, ex_id_t id, lio_exnode_exchange_t *exp)
{
    if (exp->type == EX_TEXT) {
        return(segjerase_deserialize_text(seg, id, exp));
    } else if (exp->type == EX_PROTOCOL_BUFFERS) {
        return(segjerase_deserialize_proto(seg, id, exp));
    }

    return(-1);
}


//***********************************************************************
// segjerasue_destroy - Destroys a Jerasure segment struct (not the data)
//***********************************************************************

void segjerase_destroy(tbx_ref_t *ref)
{
    tbx_obj_t *obj = container_of(ref, tbx_obj_t, refcount);
    lio_segment_t *seg = container_of(obj, lio_segment_t, obj);

    segjerase_priv_t *s = (segjerase_priv_t *)seg->priv;

    //** Check if it's still in use
    log_printf(15, "seg->id=" XIDT "\n", segment_id(seg));

    //** Destroy the child segment as well
    if (s->child_seg != NULL) {
        tbx_obj_put(&s->child_seg->obj);
    }

    if (s->plan != NULL) et_destroy_plan(s->plan);

    ex_header_release(&(seg->header));

    apr_thread_mutex_destroy(seg->lock);
    apr_thread_cond_destroy(seg->cond);
    apr_pool_destroy(seg->mpool);

    //** Do final cleanup
    free(s);
    free(seg);

    return;
}

//***********************************************************************
// segment_jerasure_create - Creates a Jerasure segment
//***********************************************************************

lio_segment_t *segment_jerasure_create(void *arg)
{
    lio_service_manager_t *es = (lio_service_manager_t *)arg;
    segjerase_priv_t *s;
    lio_segment_t *seg;
    int *paranoid;

    //** Make the space
    tbx_type_malloc_clear(seg, lio_segment_t, 1);
    tbx_type_malloc_clear(s, segjerase_priv_t, 1);

    seg->priv = s;

    generate_ex_id(&(seg->header.id));
    tbx_obj_init(&seg->obj, (tbx_vtable_t *) &lio_jeraseseg_vtable);
    seg->header.type = SEGMENT_TYPE_JERASURE;
    assert_result(apr_pool_create(&(seg->mpool), NULL), APR_SUCCESS);
    apr_thread_mutex_create(&(seg->lock), APR_THREAD_MUTEX_DEFAULT, seg->mpool);
    apr_thread_cond_create(&(seg->cond), seg->mpool);

    seg->ess = es;
    s->tpc = lio_lookup_service(es, ESS_RUNNING, ESS_TPC_UNLIMITED);
    s->child_seg = NULL;

    //** Pluck the paranoid setting for Jerase
    paranoid = lio_lookup_service(es, ESS_RUNNING, "jerase_paranoid");
    s->paranoid_check = (paranoid == NULL) ? 0 : *paranoid;
    s->magic_cksum = 1;

    //** Also snag whether we're blacklisting
    s->blacklist = lio_lookup_service(es, ESS_RUNNING, "blacklist");

    return(seg);
}



//***********************************************************************
// segment_jerasure_load - Loads a Jerasure segment from ini/ex3
//***********************************************************************

lio_segment_t *segment_jerasure_load(void *arg, ex_id_t id, lio_exnode_exchange_t *ex)
{
    lio_segment_t *seg = segment_jerasure_create(arg);
    if (segment_deserialize(seg, id, ex) != 0) {
        seg = NULL;
    }
    return(seg);
}

const lio_segment_vtable_t lio_jeraseseg_vtable = {
    .base.name = "segment_jerasure",
    .base.free_fn = segjerase_destroy,
    .read = segjerase_read,
    .write = segjerase_write,
    .inspect = segjerase_inspect,
    .truncate = segjerase_truncate,
    .remove = segjerase_remove,
    .flush = segjerase_flush,
    .clone = segjerase_clone,
    .signature = segjerase_signature,
    .size = segjerase_size,
    .block_size = segjerase_block_size,
    .serialize = segjerase_serialize,
    .deserialize = segjerase_deserialize,
};

