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
// Log structured segment support
//***********************************************************************

#define _log_module_index 180

#include <apr_errno.h>
#include <apr_pools.h>
#include <apr_thread_cond.h>
#include <apr_thread_mutex.h>
#include <gop/gop.h>
#include <gop/opque.h>
#include <gop/tp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/append_printf.h>
#include <tbx/assert_result.h>
#include <tbx/atomic_counter.h>
#include <tbx/iniparse.h>
#include <tbx/interval_skiplist.h>
#include <tbx/log.h>
#include <tbx/skiplist.h>
#include <tbx/stack.h>
#include <tbx/string_token.h>
#include <tbx/transfer_buffer.h>
#include <tbx/type_malloc.h>

#include "ex3.h"
#include "ex3/compare.h"
#include "ex3/header.h"
#include "ex3/system.h"
#include "segment/log.h"

// Forward declaration
const lio_segment_vtable_t lio_seglog_vtable;

typedef struct {
    lio_segment_t *seg;
    ex_tbx_iovec_t rex;
    ex_tbx_iovec_t wex;
    ex_off_t seg_offset;
    ex_off_t lo;
    ex_off_t hi;
    ex_off_t len;
} slog_changes_t;

typedef struct {
    lio_segment_t *seg;
    data_attr_t *da;
    lio_segment_rw_hints_t *rw_hints;
    ex_tbx_iovec_t  *iov;
    ex_off_t    boff;
    tbx_tbuf_t  *buffer;
    int         n_iov;
    int         rw_mode;
    int timeout;
} seglog_rw_t;

typedef struct {
    lio_segment_t *sseg;
    lio_segment_t *dseg;
    data_attr_t *da;
    void *attr;
    int mode;
    int timeout;
    int trunc;
} seglog_clone_t;

typedef struct {
    lio_segment_t *seg;
    data_attr_t *da;
    ex_off_t new_size;
    int timeout;
} seglog_truncate_t;

typedef struct {
    lio_segment_t *seg;
    data_attr_t *da;
    char *buffer;
    ex_off_t bufsize;
    int truncate_old_log;
    int timeout;
} seglog_merge_t;

//***********************************************************************
// _slog_find_base - Recursives though the semgents base until it finds
//   the root, non-log segment base and returns it.
//***********************************************************************

lio_segment_t *_slog_find_base(lio_segment_t *sseg)
{
    lio_segment_t *seg;
    lio_seglog_priv_t *s;

    seg = sseg;
    while (strcmp(lio_segment_type(seg), SEGMENT_TYPE_LOG) == 0) {
        s = (lio_seglog_priv_t *)seg->priv;
        seg = s->base_seg;
    }

    return(seg);
}


//***********************************************************************
//  slog_truncate_range - Inserts a truncate range into the log.
//      NOTE: This does not do any flushing or updating of any of the
//             segments.  It strictly updates the ISL mappings.
//***********************************************************************

int _slog_truncate_range(lio_segment_t *seg, lio_slog_range_t *r)
{
    int bufmax = 100;
    lio_seglog_priv_t *s = (lio_seglog_priv_t *)seg->priv;
    ex_off_t lo, hi;
    tbx_isl_iter_t it;
    lio_slog_range_t *ir;
    lio_slog_range_t *r_table[bufmax+1];
    int n, i;

    lo = r->hi+1;  //** This is the new size
    hi = s->file_size;
    it = tbx_isl_iter_search(s->mapping, (tbx_sl_key_t *)&lo, (tbx_sl_key_t *)&hi);
    ir = (lio_slog_range_t *)tbx_isl_next(&it);

    log_printf(15, "seg=" XIDT " truncating new_size=" XOT " initial_intervals=%d\n", segment_id(seg), lo, tbx_isl_count(s->mapping));
    if (ir == NULL) {
        s->file_size = r->hi + 1;
        free(r);
        return(0);
    }

    //** The 1st range is possibly truncated
    n = 0;
    if (ir->lo <= r->hi) { //** Straddles boundary
        tbx_isl_remove(s->mapping, (tbx_sl_key_t *)&(ir->lo), (tbx_sl_key_t *)&(ir->hi), (tbx_sl_data_t *)ir);
        ir->hi = r->hi;
        tbx_isl_insert(s->mapping, (tbx_sl_key_t *)&(ir->lo), (tbx_sl_key_t *)&(ir->hi), (tbx_sl_data_t *)ir);

        //** Restart the iter cause of the deletion
        it = tbx_isl_iter_search(s->mapping, (tbx_sl_key_t *)&lo, (tbx_sl_key_t *)&hi);
        tbx_isl_next(&it);
    } else {  //** Completely dropped
        r_table[n] = ir;
        n++;
    }

    //** Cycle through the intervals to remove
    while ((r_table[n] = (lio_slog_range_t *)tbx_isl_next(&it)) != NULL) {
        log_printf(15, "i=%d dropping interval lo=" XOT " hi=" XOT "\n", n, r_table[n]->lo, r_table[n]->hi);
        n++;
        if (n == bufmax) {
            for (i=0; i<n; i++) {
                tbx_isl_remove(s->mapping, (tbx_sl_key_t *)&(r_table[i]->lo), (tbx_sl_key_t *)&(r_table[i]->hi), (tbx_sl_data_t *)r_table[i]);
                free(r_table[i]);
            }
            it = tbx_isl_iter_search(s->mapping, (tbx_sl_key_t *)&lo, (tbx_sl_key_t *)&hi);
            n = 0;
        }
    }

    if (n>0) {
        for (i=0; i<n; i++) {
            tbx_isl_remove(s->mapping, (tbx_sl_key_t *)&(r_table[i]->lo), (tbx_sl_key_t *)&(r_table[i]->hi), (tbx_sl_data_t *)r_table[i]);
            free(r_table[i]);
        }
    }

    //** Adjust the size
    s->file_size = r->hi + 1;

    //** and free the range ptr
    free(r);

    log_printf(15, "new_log_intervals=%d\n", tbx_isl_count(s->mapping));

    return(0);
}

//***********************************************************************
//  slog_insert_range - Inserts a range into the log.
//      NOTE: This does not do any flushing or updating of any of the
//             segments.  It strictly updates the ISL mappings.
//***********************************************************************

int _slog_insert_range(lio_segment_t *seg, lio_slog_range_t *r)
{
    lio_seglog_priv_t *s = (lio_seglog_priv_t *)seg->priv;
    ex_off_t irlo;
    tbx_isl_iter_t it;
    lio_slog_range_t *ir, *ir2;

    //** If a truncate just do it and return
    if (r->lo == -1) return(_slog_truncate_range(seg, r));

    it = tbx_isl_iter_search(s->mapping, (tbx_sl_key_t *)&(r->lo), (tbx_sl_key_t *)&(r->hi));
    while ((ir = (lio_slog_range_t *)tbx_isl_next(&it)) != NULL) {
        tbx_isl_remove(s->mapping, (tbx_sl_key_t *)&(ir->lo), (tbx_sl_key_t *)&(ir->hi), (tbx_sl_data_t *)ir);
        if (ir->lo < r->lo) {  //** Need to truncate the 1st portion (and maybe the end)
            if (ir->hi > r->hi) {  //** Straddles r
                tbx_type_malloc(ir2, lio_slog_range_t, 1);  //** Do the end first
                ir2->lo = r->hi+1;
                ir2->hi = ir->hi;
                ir2->data_offset = ir->data_offset + (ir2->lo - ir->lo);
                tbx_isl_insert(s->mapping, (tbx_sl_key_t *)&(ir2->lo), (tbx_sl_key_t *)&(ir2->hi), (tbx_sl_data_t *)ir2);

                ir->hi = r->lo-1;  //** Now do the front portion
                tbx_isl_insert(s->mapping, (tbx_sl_key_t *)&(ir->lo), (tbx_sl_key_t *)&(ir->hi), (tbx_sl_data_t *)ir);
            } else {  //** Truncate 1st half
                ir->hi = r->lo-1;
                tbx_isl_insert(s->mapping, (tbx_sl_key_t *)&(ir->lo), (tbx_sl_key_t *)&(ir->hi), (tbx_sl_data_t *)ir);
            }
        } else if (ir->hi <= r->hi) {  //** Completely contained in r so drop
            free(ir);
        } else {  //** Drop the 1st half keeping the end
            irlo = ir->lo;
            ir->lo = r->hi+1;
            ir->data_offset = ir->data_offset + (ir->lo - irlo);
            tbx_isl_insert(s->mapping, (tbx_sl_key_t *)&(ir->lo), (tbx_sl_key_t *)&(ir->hi), (tbx_sl_data_t *)ir);
        }

        it = tbx_isl_iter_search(s->mapping, (tbx_sl_key_t *)&(r->lo), (tbx_sl_key_t *)&(r->hi));
    }

    //** Check if this range can be combined with the previous
    irlo = r->lo - 1;
    it = tbx_isl_iter_search(s->mapping, (tbx_sl_key_t *)&irlo, (tbx_sl_key_t *)&(r->hi));
    ir = (lio_slog_range_t *)tbx_isl_next(&it);
    if (ir != NULL) {
        irlo = ir->data_offset + ir->hi - ir->lo + 1;
        if (irlo == r->data_offset) {  //** Can combine ranges
            r->lo = ir->lo;
            r->data_offset = ir->data_offset;
            tbx_isl_remove(s->mapping, (tbx_sl_key_t *)&(ir->lo), (tbx_sl_key_t *)&(ir->hi), (tbx_sl_data_t *)ir);
        }
    }

    //** Insert the new range
    tbx_isl_insert(s->mapping, (tbx_sl_key_t *)&(r->lo), (tbx_sl_key_t *)&(r->hi), (tbx_sl_data_t *)r);

    log_printf(15, "r->lo=" XOT " r->hi=" XOT " r->data_offset=" XOT " curr_file_size=" XOT "\n", r->lo, r->hi, r->data_offset, s->file_size);

    //** Adjust the file size if needed
    if (r->hi >= s->file_size) s->file_size = r->hi + 1;

    return(0);
}

//***********************************************************************
// seglog_write_func - Does the actual log write operation
//***********************************************************************

gop_op_status_t seglog_write_func(void *arg, int id)
{
    seglog_rw_t *sw = (seglog_rw_t *)arg;
    lio_seglog_priv_t *s = (lio_seglog_priv_t *)sw->seg->priv;
    tbx_tbuf_t tbuf;
    gop_op_status_t status;
    int err, i;
    ex_off_t nbytes, table_offset, data_offset;
    ex_tbx_iovec_t ex_iov_data, ex_iov_table;
    lio_slog_range_t r[sw->n_iov], *range;
    gop_opque_t *q;
    gop_op_generic_t *gop;

    log_printf(15, "seg=%p n_iov=%d offset[0]=" XOT " len[0]=" XOT "\n", sw->seg, sw->n_iov, sw->iov[0].offset, sw->iov[0].len);
    q = gop_opque_new();

    segment_lock(sw->seg);

    //** First figure out how many bytes are being written and make space for the output
    nbytes = 0;
    for (i=0; i < sw->n_iov; i++) {
        r[i].lo = sw->iov[i].offset;
        r[i].hi = sw->iov[i].len;  //** Don't forget that on disk this is the len NOT range end
        r[i].data_offset = s->data_size + nbytes;
        nbytes += sw->iov[i].len;
    }

    //** Reserve the space in the table and data segments
    table_offset = s->log_size;
    s->log_size += sw->n_iov*sizeof(lio_slog_range_t);
    data_offset = s->data_size;
    s->data_size += nbytes;

    segment_unlock(sw->seg);

    //** Do the table and data writes
    ex_iovec_single(&ex_iov_data, data_offset, nbytes);
    gop = segment_write(s->data_seg, sw->da, sw->rw_hints, 1, &ex_iov_data, sw->buffer, sw->boff, sw->timeout);
    gop_opque_add(q, gop);

    i = sw->n_iov*sizeof(lio_slog_range_t);
    ex_iovec_single(&ex_iov_table, table_offset, i);
    tbx_tbuf_single(&tbuf, i, (char *)r);
    gop = segment_write(s->table_seg, sw->da, sw->rw_hints, 1, &ex_iov_table, &tbuf, 0, sw->timeout);
    gop_opque_add(q, gop);

    err = opque_waitall(q);
    if (err != OP_STATE_SUCCESS) {
        status = gop_failure_status;
        segment_lock(sw->seg);
        s->hard_errors++;
        segment_unlock(sw->seg);
    } else {
        status = gop_success_status;

        //** Update the mappings
        segment_lock(sw->seg);
        for (i=0; i < sw->n_iov; i++) {
            tbx_type_malloc(range, lio_slog_range_t, 1);
            range->lo = r[i].lo;
            range->hi = r[i].lo + r[i].hi - 1;
            range->data_offset = r[i].data_offset;
            _slog_insert_range(sw->seg, range);
        }
        segment_unlock(sw->seg);
    }

    gop_opque_free(q, OP_DESTROY);

    return(status);
}

//***********************************************************************
// seglog_write - Performs a segment write operation
//***********************************************************************

gop_op_generic_t *seglog_write(lio_segment_t *seg, data_attr_t *da, lio_segment_rw_hints_t *rw_hints, int n_iov, ex_tbx_iovec_t *iov, tbx_tbuf_t *buffer, ex_off_t boff, int timeout)
{
    lio_seglog_priv_t *s = (lio_seglog_priv_t *)seg->priv;
    seglog_rw_t *sw;
    gop_op_generic_t *gop;

    tbx_type_malloc(sw, seglog_rw_t, 1);
    sw->seg = seg;
    sw->da = da;
    sw->rw_hints = rw_hints;
    sw->n_iov = n_iov;
    sw->iov = iov;
    sw->boff = boff;
    sw->buffer = buffer;
    sw->timeout = timeout;
    sw->rw_mode = 1;
    gop = gop_tp_op_new(s->tpc, NULL, seglog_write_func, (void *)sw, free, 1);

    return(gop);
}

//***********************************************************************
// seglog_read_func - Does the actual log read operation
//***********************************************************************

gop_op_status_t seglog_read_func(void *arg, int id)
{
    seglog_rw_t *sw = (seglog_rw_t *)arg;
    lio_seglog_priv_t *s = (lio_seglog_priv_t *)sw->seg->priv;
    tbx_isl_iter_t it;
    lio_slog_range_t *ir;
    gop_opque_t *q;
    gop_op_generic_t *gop;
    int i, err, n_iov, slot;
    gop_op_status_t status;
    ex_off_t lo, hi, prev_end;
    ex_off_t bpos, pos, range_offset;
    ex_tbx_iovec_t *ex_iov, *iov;

    q = gop_opque_new();
    iov = sw->iov;

    //** Do the mapping of where to retreive the data
    segment_lock(sw->seg);

    //** First figure out how many ex_iov's are needed
    n_iov = 0;
    for (i=0; i< sw->n_iov; i++) {
        lo = iov[i].offset;
        hi = lo + iov[i].len - 1;
        n_iov += 2*tbx_isl_range_count(s->mapping, (tbx_sl_key_t *)&lo, (tbx_sl_key_t *)&hi) + 1;
    }
    n_iov += 10;  //** Just to be safe
    tbx_type_malloc(ex_iov, ex_tbx_iovec_t, n_iov);

    //** Now generate the actual task list
    bpos = sw->boff;
    slot = 0;
    for (i=0; i < sw->n_iov; i++) {
        lo = iov[i].offset;
        hi = lo + iov[i].len - 1;
        pos = lo;
        prev_end = -1;
        it = tbx_isl_iter_search(s->mapping, (tbx_sl_key_t *)&lo, (tbx_sl_key_t *)&hi);
        while ((ir = (lio_slog_range_t *)tbx_isl_next(&it)) != NULL) {
            if (prev_end == -1) {  //** 1st time through
                if (ir->lo > lo) {  //** Have a hole
                    prev_end = lo;
                } else {  //** Read from log 1st
                    prev_end = ir->lo - 1;
                }
            }

            if (prev_end != ir->lo-1) { //** We have a hole so get it from the base
                ex_iov[slot].offset = pos;
                ex_iov[slot].len = ir->lo - pos;
                gop = segment_read(s->base_seg, sw->da, sw->rw_hints, 1, &(ex_iov[slot]), sw->buffer, bpos, sw->timeout);
                gop_opque_add(q, gop);
                pos = pos + ex_iov[slot].len;
                bpos = bpos + ex_iov[slot].len;
                slot++;
            }

            if (ir->lo < lo) {  //** Need to read the middle portion
                range_offset = lo - ir->lo;
                ex_iov[slot].offset = ir->data_offset + range_offset;
                if (ir->hi > hi) {  //** Straddles the range so get the rest
                    ex_iov[slot].len = hi - pos + 1;
                } else {  //** Read til the range end
                    ex_iov[slot].len = ir->hi - pos + 1;
                }
                gop = segment_read(s->data_seg, sw->da, sw->rw_hints, 1, &(ex_iov[slot]), sw->buffer, bpos, sw->timeout);
            } else if (ir->hi <= hi) {  //** Completely contained in r
                ex_iov[slot].offset = ir->data_offset;
                ex_iov[slot].len = ir->hi - pos + 1;
                gop = segment_read(s->data_seg, sw->da, sw->rw_hints, 1, &(ex_iov[slot]), sw->buffer, bpos, sw->timeout);
            } else {  //** Drop the last half
                ex_iov[slot].offset = ir->data_offset;
                ex_iov[slot].len = hi - pos + 1;
                gop = segment_read(s->data_seg, sw->da, sw->rw_hints, 1, &(ex_iov[slot]), sw->buffer, bpos, sw->timeout);
            }

            gop_opque_add(q, gop);
            pos = pos + ex_iov[slot].len;
            bpos = bpos + ex_iov[slot].len;
            prev_end = ir->hi;
            slot++;
        }

        if (prev_end == -1) { //** We have a hole so get it from the base
            ex_iov[slot].offset = lo;
            ex_iov[slot].len = hi - lo + 1;
            gop = segment_read(s->base_seg, sw->da, sw->rw_hints, 1, &(ex_iov[slot]), sw->buffer, bpos, sw->timeout);
            gop_opque_add(q, gop);
            bpos = bpos + ex_iov[slot].len;
            slot++;
        } else if (prev_end < hi) {    //** Check if we read from the base on the end
            ex_iov[slot].offset = pos;
            ex_iov[slot].len = hi - (prev_end+1) + 1;
            gop = segment_read(s->base_seg, sw->da, sw->rw_hints, 1, &(ex_iov[slot]), sw->buffer, bpos, sw->timeout);
            gop_opque_add(q, gop);
            bpos = bpos + ex_iov[slot].len;
            slot++;
        }
    }

    segment_unlock(sw->seg);

    err = opque_waitall(q);
    if (err != OP_STATE_SUCCESS) {
        status = gop_failure_status;
        segment_lock(sw->seg);
        s->hard_errors++;
        segment_unlock(sw->seg);
    } else {
        status = gop_success_status;
    }

    gop_opque_free(q, OP_DESTROY);
    free(ex_iov);

    return(status);
}

//***********************************************************************
// seglog_read - Read from a log segment
//***********************************************************************

gop_op_generic_t *seglog_read(lio_segment_t *seg, data_attr_t *da, lio_segment_rw_hints_t *rw_hints, int n_iov, ex_tbx_iovec_t *iov, tbx_tbuf_t *buffer, ex_off_t boff, int timeout)
{
    lio_seglog_priv_t *s = (lio_seglog_priv_t *)seg->priv;
    seglog_rw_t *sw;
    gop_op_generic_t *gop;

    tbx_type_malloc(sw, seglog_rw_t, 1);
    sw->seg = seg;
    sw->da = da;
    sw->rw_hints = rw_hints;
    sw->n_iov = n_iov;
    sw->iov = iov;
    sw->boff = boff;
    sw->buffer = buffer;
    sw->timeout = timeout;
    sw->rw_mode = 0;
    gop = gop_tp_op_new(s->tpc, NULL, seglog_read_func, (void *)sw, free, 1);

    return(gop);
}


//***********************************************************************
// slog_load - Loads the intitial mapping table
//***********************************************************************

int _slog_load(lio_segment_t *seg)
{
    lio_seglog_priv_t *s = (lio_seglog_priv_t *)seg->priv;
    int timeout = 20;
    ex_off_t i;
    int last_bad, err_count;
    gop_op_generic_t *gop;
    data_attr_t *da;
    lio_slog_range_t *r;
    ex_tbx_iovec_t ex_iov;
    tbx_tbuf_t tbuf;

    da = ds_attr_create(s->ds);

    s->file_size = segment_size(s->base_seg);
    s->log_size = segment_size(s->table_seg);
    s->data_size = segment_size(s->data_seg);

    log_printf(15, "INITIAL:  fsize=" XOT " lsize=" XOT " dsize=" XOT "\n", s->file_size, s->log_size, s->data_size);

    last_bad = 0;
    err_count = 0;
    for (i=0; i<s->log_size; i += sizeof(lio_slog_range_t)) {
        tbx_type_malloc_clear(r, lio_slog_range_t, 1);
        ex_iovec_single(&ex_iov, i, sizeof(lio_slog_range_t));
        tbx_tbuf_single(&tbuf, sizeof(lio_slog_range_t), (char *)r);
        gop = segment_read(s->table_seg, da, NULL, 1, &ex_iov, &tbuf, 0, timeout);
        if (gop_waitall(gop) == OP_STATE_SUCCESS) {
            if (((r->lo == 0) && (r->hi == 0) && (r->data_offset == 0)) || ((r->hi == 0) && (r->lo != -1))) {  //** This is a failed write so ignore it
                log_printf(0, "seg=" XIDT " Blank/bad range!  offset=" XOT "\n", segment_id(seg), i);
                last_bad = 1;
                err_count++;
                free(r);
            } else {
                log_printf(15, "r->lo=" XOT " r->len(hi)=" XOT " r->data_offset=" XOT "\n", r->lo, r->hi, r->data_offset);

                r->hi = r->lo + r->hi - 1;  //** On disk this is actually the length
                _slog_insert_range(seg, r);
                last_bad = 0;
            }
        } else {
            log_printf(0, "seg=" XIDT " Error loading range!  offset=" XOT "\n", segment_id(seg), i);
            last_bad = 1;
            err_count++;
            free(r);
        }

        gop_free(gop, OP_DESTROY);
    }

    log_printf(15, "FINAL:  fsize=" XOT " lsize=" XOT " dsize=" XOT "\n", s->file_size, s->log_size, s->data_size);

    ds_attr_destroy(s->ds, da);

    if (err_count > 0) {
        if ((err_count == 1) && (last_bad == 1)) {
            return(0);
        } else {
            return(1);
        }
    }

    return(0);
}

//***********************************************************************
// slog_changes - Accumulates up the changes made in the logs over the
//    given range.
//***********************************************************************

ex_off_t slog_changes(lio_segment_t *seg, ex_off_t lo, ex_off_t hi, tbx_stack_t *stack)
{
    lio_seglog_priv_t *s = (lio_seglog_priv_t *)seg->priv;
    tbx_isl_iter_t it;
    lio_slog_range_t *ir;
    slog_changes_t *clog;
    ex_off_t nbytes, prev_end;
    int log_base;

    log_base = (strcmp(lio_segment_type(s->base_seg), SEGMENT_TYPE_LOG) == 0) ? 1 : 0;
    nbytes = 0;

    segment_lock(seg);
    prev_end = -1;
    it = tbx_isl_iter_search(s->mapping, (tbx_sl_key_t *)&lo, (tbx_sl_key_t *)&hi);
    while ((ir = (lio_slog_range_t *)tbx_isl_next(&it)) != NULL) {
        if (prev_end == -1) {  //** 1st time through
            if (ir->lo > lo) {  //** Have a hole
                prev_end = lo;
            } else {  //** Read from log 1st
                prev_end = ir->lo - 1;
            }
        }

        if ((prev_end != ir->lo-1) && (log_base == 1)) { //** We have a hole so get it from the base
            nbytes += nbytes + slog_changes(s->base_seg, prev_end, ir->lo-1, stack);
        }

        tbx_type_malloc(clog, slog_changes_t, 1);

        if (ir->lo < lo) {  //** Need to read the middle portion
            clog->lo = lo;
            clog->seg_offset = ir->data_offset + lo - ir->lo;
            if (ir->hi > hi) {  //** Straddles the range so get the rest
                clog->hi = hi;
            } else {  //** Read til the range end
                clog->hi = ir->hi;
            }
        } else if (ir->hi <= hi) {  //** Completely contained in r
            clog->seg_offset = ir->data_offset;
            clog->lo = ir->lo;
            clog->hi = ir->hi;
        } else {  //** Drop the last half
            clog->seg_offset = ir->data_offset;
            clog->lo = ir->lo;
            clog->hi = hi;
        }


        clog->len = clog->hi - clog->lo + 1;
        clog->seg = seg;
        nbytes += clog->len;
        prev_end = ir->hi;
        tbx_stack_move_to_bottom(stack);
        tbx_stack_insert_below(stack, clog);
    }

    if (log_base == 1) {
        if (prev_end == -1) { //** We have a hole so get it from the base
            nbytes += nbytes + slog_changes(s->base_seg, lo, hi, stack);
        } else if (prev_end < hi) {    //** Check if we read from the base on the end
            nbytes += nbytes + slog_changes(s->base_seg, prev_end, hi, stack);
        }
    }

    segment_unlock(seg);

    return(nbytes);
}

//***********************************************************************
// seglog_clone_func - Clone data from the segment
//***********************************************************************

gop_op_status_t seglog_clone_func(void *arg, int id)
{
    seglog_clone_t *slc = (seglog_clone_t *)arg;
    lio_seglog_priv_t *ss = (lio_seglog_priv_t *)slc->sseg->priv;
    lio_seglog_priv_t *sd = (lio_seglog_priv_t *)slc->dseg->priv;
    lio_segment_t *base;
    gop_op_generic_t *gop;
    ex_off_t nbytes_base, nbytes_log;
    int bufsize = 50*1024*1024;
    ex_off_t dt, pos, rpos, wpos, rlen, dlen;
    tbx_tbuf_t *wbuf, *rbuf, *tmpbuf;
    tbx_tbuf_t tbuf1, tbuf2;
    int err, do_lio_segment_copy_gop;
    char *buffer = NULL;
    slog_changes_t *clog;
    gop_opque_t *q1 = NULL;
    gop_opque_t *q2, *q;
    tbx_stack_t *stack;
    gop_op_status_t status;

    do_lio_segment_copy_gop = 0;
    stack = NULL;
    q2 = NULL;

    q = gop_opque_new();
    opque_start_execution(q);

    //** SEe if we are using an old seg.  If so we need to trunc it first
    if (slc->trunc == 1) {
        gop_opque_add(q, lio_segment_truncate(sd->table_seg, slc->da, 0, slc->timeout));
        gop_opque_add(q, lio_segment_truncate(sd->data_seg, slc->da, 0, slc->timeout));
        gop_opque_add(q, lio_segment_truncate(sd->base_seg, slc->da, 0, slc->timeout));
    }

    //** Go ahead and start cloning the log
    gop_opque_add(q, segment_clone(ss->table_seg, slc->da, &(sd->table_seg), CLONE_STRUCTURE, slc->attr, slc->timeout));
    gop_opque_add(q, segment_clone(ss->data_seg, slc->da, &(sd->data_seg), CLONE_STRUCTURE, slc->attr, slc->timeout));

    //** Check and see how much data will be transferred if using base
    base = _slog_find_base(ss->base_seg);

    opque_waitall(q);

    if (slc->mode == CLONE_STRUCTURE) {  //** Only cloning the structure
        gop_opque_add(q, segment_clone(base, slc->da, &(sd->base_seg), CLONE_STRUCTURE, slc->attr, slc->timeout));
    } else {
        nbytes_base = segment_size(base);
        stack = tbx_stack_new();
        sd->file_size = segment_size(slc->sseg);
        nbytes_log = slog_changes(slc->sseg, 0, sd->file_size, stack);

        //** Make a crude estimate of the time.
        //** I assume depot-depot copies are 5x faster than going through the client
        //** Have to Read + write the log data hence the 2x below
        dt = 2*nbytes_log + nbytes_base / 5;
        log_printf(15, "dt=" XOT " nbytes_log=" XOT " nbytes_base=" XOT " ss->file_size=" XOT "\n", dt, nbytes_log, nbytes_base, ss->file_size);
        if (dt > ss->file_size) {
            do_lio_segment_copy_gop = 1;
            gop_opque_add(q, segment_clone(base, slc->da, &(sd->base_seg), CLONE_STRUCTURE, slc->attr, slc->timeout));
        }
    }

    //** Wait for the initial cloning to complete
    err = opque_waitall(q);

    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "Error during intial cloning phase:  src=" XIDT "\n", segment_id(slc->sseg));
        if (stack != NULL) tbx_stack_free(stack, 1);
        gop_opque_free(q, OP_DESTROY);
        return(gop_failure_status);
    }

    //** Now copy the data if needed
    if (do_lio_segment_copy_gop == 1) {  //** lio_segment_copy_gop() method
        tbx_type_malloc(buffer, char, bufsize);
        gop_opque_add(q, lio_segment_copy_gop(ss->tpc, slc->da, NULL, slc->sseg, slc->dseg, 0, 0, ss->file_size, bufsize, buffer, 0, slc->timeout));
    } else if (slc->mode == CLONE_STRUCT_AND_DATA) {  //** Use the incremental log+base method
        //** First clone the base struct and data
        gop_opque_add(q, segment_clone(base, slc->da, &(sd->base_seg), CLONE_STRUCT_AND_DATA, slc->attr, slc->timeout));

        //** Now do the logs
        //** Set up the buffers
        tbx_type_malloc(buffer, char, 2*bufsize);
        tbx_tbuf_single(&tbuf1, bufsize, buffer);
        tbx_tbuf_single(&tbuf2, bufsize, &(buffer[bufsize]));
        rbuf = &tbuf1;
        wbuf = &tbuf2;
        rlen = 0;
        q1 = q;
        q2 = gop_opque_new();
        while ((clog = (slog_changes_t *)tbx_stack_get_current_data(stack)) != NULL) {
            pos = 0;
            rpos = clog->seg_offset;
            wpos = clog->lo;
            while (pos < clog->len) {
                dlen = clog->len - pos;
                ex_iovec_single(&(clog->rex), rpos, dlen);
                gop = segment_read(clog->seg, slc->da, NULL, 1, &(clog->rex), rbuf, rlen, slc->timeout);
                gop_opque_add(q1, gop);

                ex_iovec_single(&(clog->wex), wpos, dlen);
                gop = segment_write(clog->seg, slc->da, NULL, 1, &(clog->wex), wbuf, rlen, slc->timeout);
                gop_opque_add(q2, gop);

                pos += dlen;
                rpos += dlen;
                wpos += dlen;
                rlen += dlen;

                if (rlen <= 0) {  //** Time to flush the data
                    err = opque_waitall(q1);  //** Wait for the data to be R/W
                    gop_opque_free(q1, OP_DESTROY);  //** Free the space
                    if (err != OP_STATE_SUCCESS) {
                        log_printf(1, "Error during log copy phase:  src=" XIDT "\n", segment_id(slc->sseg));
                        tbx_stack_free(stack, 1);
                        free(buffer);
                        gop_opque_free(q2, OP_DESTROY);
                        return(gop_failure_status);
                    }

                    //** Swap the tasks and start executing them
                    tmpbuf = rbuf;
                    rbuf = wbuf;
                    wbuf = tmpbuf;
                    q1 = q2;
                    q2 = gop_opque_new();  //** Make a new que but don't start execution.  Have to wait for the reads to finish
                    opque_start_execution(q1);  //** Start the previous write tasks executing while we add read tasks to it

                    rlen = 0;
                }
            }

            tbx_stack_move_down(stack);
        }

    }

    status = gop_success_status;
    err = opque_waitall(q);
    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "Error during log copy phase:  src=" XIDT "\n", segment_id(slc->sseg));
        status = gop_failure_status;
    }

    //** Make sure we don't have an empty log
    if (q2 != NULL) {
        if (gop_opque_task_count(q2) > 0) {
            err = opque_waitall(q2);  //** Wait for the data to be R/W
            if (err != OP_STATE_SUCCESS) {
                log_printf(1, "Error during log copy phase:  src=" XIDT "\n", segment_id(slc->sseg));
                status = gop_failure_status;
            }
        }
    }

    //** Clean up
    if (stack != NULL) tbx_stack_free(stack, 1);
    if (buffer != NULL) free(buffer);
    if (q2 == NULL) {
        gop_opque_free(q, OP_DESTROY);
    } else {
        gop_opque_free(q1, OP_DESTROY);
        gop_opque_free(q2, OP_DESTROY);
    }

    return(status);
}

//***********************************************************************
// seglog_clone - Clones a segment
//***********************************************************************

gop_op_generic_t *seglog_clone(lio_segment_t *seg, data_attr_t *da, lio_segment_t **clone_seg, int mode, void *attr, int timeout)
{
    lio_segment_t *clone;
    lio_seglog_priv_t *sd;
    gop_op_generic_t *gop;
    seglog_clone_t *slc;
    int use_existing = (*clone_seg != NULL) ? 1 : 0;

    //** Make the base segment
//log_printf(0, " before clone create\n");
    if (use_existing == 0) *clone_seg = segment_log_create(seg->ess);
//log_printf(0, " after clone create\n");
    clone = *clone_seg;
    sd = (lio_seglog_priv_t *)clone->priv;

    log_printf(15, "use_existing=%d sseg=" XIDT " dseg=" XIDT "\n", use_existing, segment_id(seg), segment_id(clone));

    //** Copy the header
    if ((seg->header.name != NULL) && (use_existing == 0)) clone->header.name = strdup(seg->header.name);

    tbx_type_malloc(slc, seglog_clone_t, 1);
    slc->sseg = seg;
    slc->dseg = clone;
    slc->da = da;
    slc->mode = mode;
    slc->timeout = timeout;
    slc->attr = attr;
    slc->trunc = use_existing;
    gop = gop_tp_op_new(sd->tpc, NULL, seglog_clone_func, (void *)slc, free, 1);

    return(gop);
}

//***********************************************************************
//  seglog_truncate_func - Does the actual segment truncate operation
//***********************************************************************

gop_op_status_t seglog_truncate_func(void *arg, int id)
{
    seglog_truncate_t *st = (seglog_truncate_t *)arg;
    lio_seglog_priv_t *s = (lio_seglog_priv_t *)st->seg->priv;
    gop_op_status_t status;
    tbx_tbuf_t tbuf_table, tbuf_data;
    ex_tbx_iovec_t ex_iov_table, ex_iov_data;
    ex_off_t table_offset, data_offset;
    char c = 0;
    int err;
    lio_slog_range_t *r;
    gop_opque_t *q;
    gop_op_generic_t *gop = NULL;

    q = gop_opque_new();

    segment_lock(st->seg);
    if (st->new_size < s->file_size) { //** Shrink operation
        table_offset = s->log_size;
        tbx_type_malloc(r, lio_slog_range_t, 1);
        r->lo = -1;
        r->hi = st->new_size - 1;
        r->data_offset = -1;
        ex_iovec_single(&ex_iov_table, table_offset, sizeof(lio_slog_range_t));
        tbx_tbuf_single(&tbuf_table, sizeof(lio_slog_range_t), (char *)r);
        gop = segment_write(s->table_seg, st->da, NULL, 1, &ex_iov_table, &tbuf_table, 0, st->timeout);
        gop_opque_add(q, gop);

        s->log_size += sizeof(lio_slog_range_t);
    } else if (st->new_size > s->file_size) {  //** Grow operation
        table_offset = s->log_size;
        data_offset = s->data_size;
        tbx_type_malloc(r, lio_slog_range_t, 1);
        r->lo = s->file_size - 1;
        r->hi = st->new_size - s->file_size;  //** On disk this is the len
        r->data_offset = data_offset;
        ex_iovec_single(&ex_iov_table, table_offset, sizeof(lio_slog_range_t));
        tbx_tbuf_single(&tbuf_table, sizeof(lio_slog_range_t), (char *)r);
        gop = segment_write(s->table_seg, st->da, NULL, 1, &ex_iov_table, &tbuf_table, 0, st->timeout);
        gop_opque_add(q, gop);

        //** Don't need to write all the blanks.  Just the last byte.
        ex_iovec_single(&ex_iov_data, data_offset + r->hi - 1, 1);
        tbx_tbuf_single(&tbuf_data, 1, &c);
        gop = segment_write(s->data_seg, st->da, NULL, 1, &ex_iov_data, &tbuf_data, 0, st->timeout);
        gop_opque_add(q, gop);

        s->log_size += sizeof(lio_slog_range_t);
        s->data_size += st->new_size - s->file_size;

    }
    segment_unlock(st->seg);

    //** If nothing to do exit
    if (gop == NULL) {
        gop_opque_free(q, OP_DESTROY);
        return(gop_success_status);
    }

    //** Perform the updates
    err = opque_waitall(q);
    if (err != OP_STATE_SUCCESS) {
        status = gop_failure_status;
        segment_lock(st->seg);
        s->hard_errors++;
        segment_unlock(st->seg);
    } else {
        status = gop_success_status;

        //** Update the mappings
        segment_lock(st->seg);
        if (st->new_size < s->file_size) { //** Grow op so tweak the r
            r->hi = r->lo + r->hi - 1;
        }

        _slog_insert_range(st->seg, r);
        segment_unlock(st->seg);
    }

    gop_opque_free(q, OP_DESTROY);

    return(status);
}

//***********************************************************************
// seglog_truncate - Expands or contracts a linear segment
//***********************************************************************

gop_op_generic_t *seglog_truncate(lio_segment_t *seg, data_attr_t *da, ex_off_t new_size, int timeout)
{
    lio_seglog_priv_t *s = (lio_seglog_priv_t *)seg->priv;
    seglog_truncate_t *st;

    if (new_size < 0) { //** Reserve space call which is ignored at this time
        return(gop_dummy(gop_success_status));
    }

    tbx_type_malloc_clear(st, seglog_truncate_t, 1);

    st->seg = seg;
    st->new_size = new_size;
    st->timeout = timeout;
    st->da = da;

    return(gop_tp_op_new(s->tpc, NULL, seglog_truncate_func, (void *)st, free, 1));
}

//***********************************************************************
// seglog_remove - Removes the log segment from disk
//***********************************************************************

gop_op_generic_t *seglog_remove(lio_segment_t *seg, data_attr_t *da, int timeout)
{
    lio_seglog_priv_t *s = (lio_seglog_priv_t *)seg->priv;
    gop_opque_t *q = gop_opque_new();

    gop_opque_add(q, segment_remove(s->table_seg, da, timeout));
    gop_opque_add(q, segment_remove(s->data_seg, da, timeout));
    gop_opque_add(q, segment_remove(s->base_seg, da, timeout));
    return(opque_get_gop(q));
}


//***********************************************************************
// seglog_inspect - Inspects the log segment
//***********************************************************************

gop_op_generic_t *seglog_inspect(lio_segment_t *seg, data_attr_t *da, tbx_log_fd_t *fd, int mode, ex_off_t bufsize, lio_inspect_args_t *args, int timeout)
{
    lio_seglog_priv_t *s = (lio_seglog_priv_t *)seg->priv;
    gop_opque_t *q = gop_opque_new();

    gop_opque_add(q, segment_inspect(s->table_seg, da, fd, mode, bufsize, args, timeout));
    gop_opque_add(q, segment_inspect(s->data_seg, da, fd, mode, bufsize, args, timeout));
    gop_opque_add(q, segment_inspect(s->base_seg, da, fd, mode, bufsize, args, timeout));
    return(opque_get_gop(q));
}


//***********************************************************************
// seglog_flush - Flushes a segment
//***********************************************************************

gop_op_generic_t *seglog_flush(lio_segment_t *seg, data_attr_t *da, ex_off_t lo, ex_off_t hi, int timeout)
{
    lio_seglog_priv_t *s = (lio_seglog_priv_t *)seg->priv;
    gop_opque_t *q = gop_opque_new();

    gop_opque_add(q, segment_flush(s->table_seg, da, 0, s->log_size, timeout));
    gop_opque_add(q, segment_flush(s->data_seg, da, 0, s->data_size, timeout));
    return(opque_get_gop(q));
}

//***********************************************************************
// seglog_size - Returns the segment size.
//***********************************************************************

ex_off_t seglog_size(lio_segment_t *seg)
{
    lio_seglog_priv_t *s = (lio_seglog_priv_t *)seg->priv;
    ex_off_t size;

    segment_lock(seg);
    size = s->file_size;
    segment_unlock(seg);

    return(size);
}

//***********************************************************************
// seglog_block_size - Returns the blocks size which is always 1
//***********************************************************************

ex_off_t seglog_block_size(lio_segment_t *seg, int btype)
{
//  lio_seglog_priv_t *s = (lio_seglog_priv_t *)seg->priv;

    return(1);
}

//***********************************************************************
// seglog_signature - Generates the segment signature
//***********************************************************************

int seglog_signature(lio_segment_t *seg, char *buffer, int *used, int bufsize)
{
    lio_seglog_priv_t *s = (lio_seglog_priv_t *)seg->priv;

    tbx_append_printf(buffer, used, bufsize, "log(log)\n");
    segment_signature(s->table_seg, buffer, used, bufsize);

    tbx_append_printf(buffer, used, bufsize, "log(data)\n");
    segment_signature(s->data_seg, buffer, used, bufsize);

    tbx_append_printf(buffer, used, bufsize, "log(base)\n");
    segment_signature(s->base_seg, buffer, used, bufsize);

    return(0);
}


//***********************************************************************
// seglog_serialize_text -Convert the segment to a text based format
//***********************************************************************

int seglog_serialize_text(lio_segment_t *seg, lio_exnode_exchange_t *exp)
{
    lio_seglog_priv_t *s = (lio_seglog_priv_t *)seg->priv;
    int bufsize=1024*1024;
    char segbuf[bufsize];
    char *etext;
    int sused;
    lio_exnode_exchange_t *child_exp;

    segbuf[0] = 0;

    sused = 0;

    //** Store the segment header
    tbx_append_printf(segbuf, &sused, bufsize, "[segment-" XIDT "]\n", seg->header.id);
    if ((seg->header.name != NULL) && (strcmp(seg->header.name, "") != 0)) {
        etext = tbx_stk_escape_text("=", '\\', seg->header.name);
        tbx_append_printf(segbuf, &sused, bufsize, "name=%s\n", etext);
        free(etext);
    }
    tbx_append_printf(segbuf, &sused, bufsize, "type=%s\n", SEGMENT_TYPE_LOG);

    //** And the children segments
    tbx_append_printf(segbuf, &sused, bufsize, "log=" XIDT "\n", segment_id(s->table_seg));
    child_exp = lio_exnode_exchange_create(EX_TEXT);
    segment_serialize(s->table_seg, child_exp);
    exnode_exchange_append(exp, child_exp);
    exnode_exchange_free(child_exp);

    tbx_append_printf(segbuf, &sused, bufsize, "data=" XIDT "\n", segment_id(s->data_seg));
    segment_serialize(s->data_seg, child_exp);
    exnode_exchange_append(exp, child_exp);
    exnode_exchange_free(child_exp);

    tbx_append_printf(segbuf, &sused, bufsize, "base=" XIDT "\n", segment_id(s->base_seg));
    segment_serialize(s->base_seg, child_exp);
    exnode_exchange_append(exp, child_exp);
    exnode_exchange_free(child_exp);

    //** And finally the the container
    exnode_exchange_append_text(exp, segbuf);
    lio_exnode_exchange_destroy(child_exp);

    return(0);
}

//***********************************************************************
// seglog_serialize_proto -Convert the segment to a protocol buffer
//***********************************************************************

int seglog_serialize_proto(lio_segment_t *seg, lio_exnode_exchange_t *exp)
{
    return(-1);
}

//***********************************************************************
// seglog_serialize -Convert the segment to a more portable format
//***********************************************************************

int seglog_serialize(lio_segment_t *seg, lio_exnode_exchange_t *exp)
{
    if (exp->type == EX_TEXT) {
        return(seglog_serialize_text(seg, exp));
    } else if (exp->type == EX_PROTOCOL_BUFFERS) {
        return(seglog_serialize_proto(seg, exp));
    }

    return(-1);
}


//***********************************************************************
// seglog_deserialize_text -Read the text based segment
//***********************************************************************

int seglog_deserialize_text(lio_segment_t *seg, ex_id_t id, lio_exnode_exchange_t *exp)
{
    lio_seglog_priv_t *s = (lio_seglog_priv_t *)seg->priv;
    int bufsize=1024;
    char seggrp[bufsize];
    tbx_inip_file_t *fd;

    //** Parse the ini text
    fd = exp->text.fd;

    //** Make the segment section name
    snprintf(seggrp, bufsize, "segment-" XIDT, id);

    //** Get the segment header info
    seg->header.id = id;
    seg->header.type = SEGMENT_TYPE_LOG;
    seg->header.name = tbx_inip_get_string(fd, seggrp, "name", "");

    //** Load the child segments
    id = tbx_inip_get_integer(fd, seggrp, "log", 0);
    if (id == 0) return (-1);
    s->table_seg = load_segment(seg->ess, id, exp);
    if (s->table_seg == NULL) return(-2);

    id = tbx_inip_get_integer(fd, seggrp, "data", 0);
    if (id == 0) return (-1);
    s->data_seg = load_segment(seg->ess, id, exp);
    if (s->data_seg == NULL) return(-2);

    id = tbx_inip_get_integer(fd, seggrp, "base", 0);
    if (id == 0) return (-1);
    s->base_seg = load_segment(seg->ess, id, exp);
    if (s->base_seg == NULL) return(-2);

    //** Load the log table which will also set the size
    _slog_load(seg);

    log_printf(15, "seglog_deserialize_text: seg=" XIDT "\n", segment_id(seg));
    return(0);
}


//***********************************************************************
// seglog_deserialize_proto - Read the prot formatted segment
//***********************************************************************

int seglog_deserialize_proto(lio_segment_t *seg, ex_id_t id, lio_exnode_exchange_t *exp)
{
    return(-1);
}

//***********************************************************************
// seglog_deserialize -Convert from the portable to internal format
//***********************************************************************

int seglog_deserialize(lio_segment_t *seg, ex_id_t id, lio_exnode_exchange_t *exp)
{
    if (exp->type == EX_TEXT) {
        return(seglog_deserialize_text(seg, id, exp));
    } else if (exp->type == EX_PROTOCOL_BUFFERS) {
        return(seglog_deserialize_proto(seg, id, exp));
    }

    return(-1);
}


//***********************************************************************
// seglog_destroy - Destroys a llog segment struct (not the data)
//***********************************************************************

void seglog_destroy(tbx_ref_t *ref)
{
    tbx_obj_t *obj = container_of(ref, tbx_obj_t, refcount);
    lio_segment_t *seg = container_of(obj, lio_segment_t, obj);
    int i, n;
    tbx_isl_iter_t it;
    lio_slog_range_t **r_list;
    lio_seglog_priv_t *s = (lio_seglog_priv_t *)seg->priv;

    //** Check if it's still in use
    log_printf(15, "seglog_destroy: seg->id=" XIDT "\n", segment_id(seg));

    //** Destroy the child segments
    if (s->table_seg != NULL) {
        tbx_obj_put(&s->table_seg->obj);
    }
    if (s->data_seg != NULL) {
        tbx_obj_put(&s->data_seg->obj);
    }
    if (s->base_seg != NULL) {
        tbx_obj_put(&s->base_seg->obj);
    }

    //** Now free the mapping table
    n = tbx_isl_count(s->mapping);
    tbx_type_malloc_clear(r_list, lio_slog_range_t *, n);
    it = tbx_isl_iter_search(s->mapping, (tbx_sl_key_t *)NULL, (tbx_sl_key_t *)NULL);
    for (i=0; i<n; i++) {
        r_list[i] = (lio_slog_range_t *)tbx_isl_next(&it);
    }
    tbx_isl_del(s->mapping);

    for (i=0; i<n; i++) free(r_list[i]);
    free(r_list);

    free(s);

    ex_header_release(&(seg->header));

    apr_thread_mutex_destroy(seg->lock);
    apr_thread_cond_destroy(seg->cond);
    apr_pool_destroy(seg->mpool);

    free(seg);
}

//***********************************************************************
// segment_log_create - Creates a log segment
//***********************************************************************

lio_segment_t *segment_log_create(void *arg)
{
    lio_service_manager_t *es = (lio_service_manager_t *)arg;
    lio_seglog_priv_t *s;
    lio_segment_t *seg;

    tbx_type_malloc_clear(seg, lio_segment_t, 1);
    tbx_type_malloc_clear(s, lio_seglog_priv_t, 1);

    s->mapping = tbx_isl_new(&skiplist_compare_ex_off, NULL, NULL, NULL);
    seg->priv = s;
    s->file_size = 0;

    generate_ex_id(&(seg->header.id));
    tbx_obj_init(&seg->obj, (tbx_vtable_t *) &lio_seglog_vtable);
    seg->header.type = SEGMENT_TYPE_LOG;

    assert_result(apr_pool_create(&(seg->mpool), NULL), APR_SUCCESS);
    apr_thread_mutex_create(&(seg->lock), APR_THREAD_MUTEX_DEFAULT, seg->mpool);
    apr_thread_cond_create(&(seg->cond), seg->mpool);

    seg->ess = es;
    s->tpc = lio_lookup_service(es, ESS_RUNNING, ESS_TPC_UNLIMITED);
    s->ds = lio_lookup_service(es, ESS_RUNNING, ESS_DS);

    return(seg);
}

//***********************************************************************
// segment_log_load - Loads a log segment from ini/ex3
//***********************************************************************

lio_segment_t *segment_log_load(void *arg, ex_id_t id, lio_exnode_exchange_t *ex)
{
    lio_segment_t *seg = segment_log_create(arg);
    if (segment_deserialize(seg, id, ex) != 0) {
        seg = NULL;
    }
    return(seg);
}

//***********************************************************************
// slog_make - Adds a log level using the given base segment
//    The empty log and data must be provided.
//***********************************************************************

lio_segment_t *slog_make(lio_service_manager_t *sm, lio_segment_t *table, lio_segment_t *data, lio_segment_t *base)
{
    lio_segment_t *seg;
    lio_seglog_priv_t *s;
    lio_segment_create_fn_t *screate;

    screate = lio_lookup_service(sm, SEG_SM_CREATE, SEGMENT_TYPE_LOG);
    if (screate == NULL) return(NULL);

    seg = (*screate)(sm);
    s = (lio_seglog_priv_t *)seg->priv;
    s->table_seg = table;
    s->data_seg = data;
    s->base_seg = base;
    s->file_size = segment_size(base);

    return(seg);
}

//***********************************************************************
// seglog_merge_with_base_func - Merges the log with the base
//***********************************************************************

gop_op_status_t seglog_merge_with_base_func(void *arg, int id)
{
    seglog_merge_t *sm = (seglog_merge_t *)arg;
    lio_seglog_priv_t *s = (lio_seglog_priv_t *)sm->seg->priv;
    gop_opque_t *qin, *qout;
    gop_op_generic_t *gop;
    ex_tbx_iovec_t *ex_in, *ex_out;
    lio_slog_range_t *r;
    tbx_tbuf_t tbuf;
    ex_off_t pos, len, blen, bpos;
    int i, n_iov, err;
    tbx_isl_iter_t it;

    qin = gop_opque_new();
    qout = gop_opque_new();

    n_iov = tbx_isl_count(s->mapping);
    tbx_type_malloc(ex_in, ex_tbx_iovec_t, n_iov);
    tbx_type_malloc(ex_out, ex_tbx_iovec_t, n_iov);
    tbx_tbuf_single(&tbuf, sm->bufsize, sm->buffer);

    segment_lock(sm->seg);

    it = tbx_isl_iter_search(s->mapping, (tbx_sl_key_t *)NULL, (tbx_sl_key_t *)NULL);
    pos = 0;
    for (i=0; i<=n_iov; i++) {
        if (i < n_iov) {
            r = (lio_slog_range_t *)tbx_isl_next(&it);
            ex_in[i].offset = r->data_offset;
            len = r->hi - r->lo + 1;
            ex_in[i].len = len;
            ex_out[i].offset = r->lo;
            ex_out[i].len = len;
            log_printf(15, "i=%d in.offset=" XOT " in.len=" XOT " out.offset=" XOT " pos=" XOT " bufsize=" XOT "\n", i, ex_in[i].offset, len, ex_out[i].offset, pos, sm->bufsize);
        } else {
            len = sm->bufsize;
        }

        if ((len > sm->bufsize) || ((pos+len) > sm->bufsize)) {  //** Buffer fill so flush it
            log_printf(15, "i=%d flushing\n", i);
            err = opque_waitall(qin);
            if (err != OP_STATE_SUCCESS) {
                segment_unlock(sm->seg);
                log_printf(1, "seg=" XIDT " Error reading segment!\n", segment_id(sm->seg));
                return(gop_failure_status);
            }
            err = opque_waitall(qout);
            if (err != OP_STATE_SUCCESS) {
                segment_unlock(sm->seg);
                log_printf(1, "seg=" XIDT " Error writing segment!\n", segment_id(sm->seg));
                return(gop_failure_status);
            }
            gop_opque_free(qout, OP_DESTROY);
            qout = gop_opque_new();
            pos = 0;
        }

        if (i == n_iov) break;  //** Exit loop

        if (len > sm->bufsize) { //** Got to do it in chunks
            log_printf(15, "i=%d len>bufsize so chunking\n", i);

            for (bpos=0; bpos<len; bpos += sm->bufsize) {
                blen = (bpos+sm->bufsize) > len ? len - bpos : len;
                ex_in[i].len = blen;
                ex_out[i].len = blen;
                log_printf(15, "i=%d bpos=" XOT " in.offset=" XOT " out.offset=" XOT " len=" XOT "\n", i, bpos, ex_in[i].len, ex_out[i].len, blen);

                gop = segment_read(s->data_seg, sm->da, NULL, 1, &(ex_in[i]), &tbuf, 0, sm->timeout);
                gop_opque_add(qin, gop);
                err = opque_waitall(qin);
                if (err != OP_STATE_SUCCESS) {
                    segment_unlock(sm->seg);
                    log_printf(1, "seg=" XIDT " Error reading segment!\n", segment_id(sm->seg));
                    return(gop_failure_status);
                }

                gop = segment_write(s->base_seg, sm->da, NULL, 1, &(ex_out[i]), &tbuf, 0, sm->timeout);
                gop_opque_add(qout, gop);
                if (err != OP_STATE_SUCCESS) {
                    segment_unlock(sm->seg);
                    log_printf(1, "seg=" XIDT " Error writing segment!\n", segment_id(sm->seg));
                    return(gop_failure_status);
                }

                ex_in[i].offset += blen;
                ex_out[i].offset += blen;
            }
            pos = 0;
        } else {
            gop = segment_read(s->data_seg, sm->da, NULL, 1, &(ex_in[i]), &tbuf, pos, sm->timeout);
            gop_opque_add(qin, gop);
            gop = segment_write(s->base_seg, sm->da, NULL, 1, &(ex_out[i]), &tbuf, pos, sm->timeout);
            gop_opque_add(qout, gop);
            pos = pos + ex_in[i].len;
        }
    }

    //** If needed get rid of the old log
    err = OP_STATE_SUCCESS;
    if (sm->truncate_old_log == 1) {
        log_printf(15, "truncating old log\n");
        gop_opque_add(qin, lio_segment_truncate(s->table_seg, sm->da, 0, sm->timeout));
        gop_opque_add(qin, lio_segment_truncate(s->data_seg, sm->da, 0, sm->timeout));

        //** empty the log table
        tbx_type_malloc(r, lio_slog_range_t, 1);
        r->lo = -1;
        r->hi = 0 - 1;
        r->data_offset = -1;
        _slog_truncate_range(sm->seg, r);  //** r is released in the truncate call

        //** Wait of everything to complete
        err = opque_waitall(qin);

        s->log_size = 0;
        s->data_size = 0;
    }

    segment_unlock(sm->seg);

    gop_opque_free(qin, OP_DESTROY);
    gop_opque_free(qout, OP_DESTROY);

    free(ex_in);
    free(ex_out);

    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "seg=" XIDT " Error truncating table/data logs!\n", segment_id(sm->seg));
        return(gop_failure_status);
    }

    log_printf(15, "seg=" XIDT " SUCCESS!\n", segment_id(sm->seg));

    return(gop_success_status);
}


//***********************************************************************
// lio_slog_merge_with_base_gop - Merges the log (table/data segments) with the base.
//   If truncate_old_log == 1 then the old log is truncated back to 0.
//   Otherwise the old log is not touched and left intact (and superfluos).
//***********************************************************************

gop_op_generic_t *lio_slog_merge_with_base_gop(lio_segment_t *seg, data_attr_t *da, ex_off_t bufsize, char *buffer, int truncate_old_log, int timeout)
{
    seglog_merge_t *st;
    lio_seglog_priv_t *s = (lio_seglog_priv_t *)seg->priv;

    tbx_type_malloc_clear(st, seglog_merge_t, 1);

    st->seg = seg;
    st->truncate_old_log = truncate_old_log;
    st->bufsize = bufsize;
    st->buffer = buffer;
    st->timeout = timeout;
    st->da = da;

    return(gop_tp_op_new(s->tpc, NULL, seglog_merge_with_base_func, (void *)st, free, 1));
}

const lio_segment_vtable_t lio_seglog_vtable = {
    .base.name = "segment_log",
    .base.free_fn = seglog_destroy,
    .read = seglog_read,
    .write = seglog_write,
    .inspect = seglog_inspect,
    .truncate = seglog_truncate,
    .remove = seglog_remove,
    .flush = seglog_flush,
    .clone = seglog_clone,
    .signature = seglog_signature,
    .size = seglog_size,
    .block_size = seglog_block_size,
    .serialize = seglog_serialize,
    .deserialize = seglog_deserialize,
};
