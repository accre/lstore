/*
Advanced Computing Center for Research and Education Proprietary License
Version 1.0 (April 2006)

Copyright (c) 2006, Advanced Computing Center for Research and Education,
 Vanderbilt University, All rights reserved.

This Work is the sole and exclusive property of the Advanced Computing Center
for Research and Education department at Vanderbilt University.  No right to
disclose or otherwise disseminate any of the information contained herein is
granted by virtue of your possession of this software except in accordance with
the terms and conditions of a separate License Agreement entered into with
Vanderbilt University.

THE AUTHOR OR COPYRIGHT HOLDERS PROVIDES THE "WORK" ON AN "AS IS" BASIS,
WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, TITLE, FITNESS FOR A PARTICULAR
PURPOSE, AND NON-INFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Vanderbilt University
Advanced Computing Center for Research and Education
230 Appleton Place
Nashville, TN 37203
http://www.accre.vanderbilt.edu
*/

//***********************************************************************
// Log structured segment support
//***********************************************************************

#define _log_module_index 180

#include "ex3_abstract.h"
#include "ex3_system.h"
#include "ex3_compare.h"
#include "interval_skiplist.h"
#include "log.h"
#include "segment_log.h"
#include "segment_log_priv.h"
#include "type_malloc.h"
#include "string_token.h"
#include "append_printf.h"

typedef struct {
    segment_t *seg;
    ex_iovec_t rex;
    ex_iovec_t wex;
    ex_off_t seg_offset;
    ex_off_t lo;
    ex_off_t hi;
    ex_off_t len;
} slog_changes_t;

typedef struct {
    segment_t *seg;
    data_attr_t *da;
    segment_rw_hints_t *rw_hints;
    ex_iovec_t  *iov;
    ex_off_t    boff;
    tbuffer_t  *buffer;
    int         n_iov;
    int         rw_mode;
    int timeout;
} seglog_rw_t;

typedef struct {
    segment_t *sseg;
    segment_t *dseg;
    data_attr_t *da;
    void *attr;
    int mode;
    int timeout;
    int trunc;
} seglog_clone_t;

typedef struct {
    segment_t *seg;
    data_attr_t *da;
    ex_off_t new_size;
    int timeout;
} seglog_truncate_t;

typedef struct {
    segment_t *seg;
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

segment_t *_slog_find_base(segment_t *sseg)
{
    segment_t *seg;
    seglog_priv_t *s;

    seg = sseg;
    while (strcmp(segment_type(seg), SEGMENT_TYPE_LOG) == 0) {
        s = (seglog_priv_t *)seg->priv;
        seg = s->base_seg;
    }

    return(seg);
}


//***********************************************************************
//  slog_truncate_range - Inserts a truncate range into the log.
//      NOTE: This does not do any flushing or updating of any of the
//             segments.  It strictly updates the ISL mappings.
//***********************************************************************

int _slog_truncate_range(segment_t *seg, slog_range_t *r)
{
    int bufmax = 100;
    seglog_priv_t *s = (seglog_priv_t *)seg->priv;
    ex_off_t lo, hi;
    interval_skiplist_iter_t it;
    slog_range_t *ir;
    slog_range_t *r_table[bufmax+1];
    int n, i;

    lo = r->hi+1;  //** This is the new size
    hi = s->file_size;
    it = iter_search_interval_skiplist(s->mapping, (skiplist_key_t *)&lo, (skiplist_key_t *)&hi);
    ir = (slog_range_t *)next_interval_skiplist(&it);

    log_printf(15, "seg=" XIDT " truncating new_size=" XOT " initial_intervals=%d\n", segment_id(seg), lo, interval_skiplist_count(s->mapping));
    if (ir == NULL) {
        s->file_size = r->hi + 1;
        free(r);
        return(0);
    }

    //** The 1st range is possibly truncated
    n = 0;
    if (ir->lo <= r->hi) { //** Straddles boundary
        remove_interval_skiplist(s->mapping, (skiplist_key_t *)&(ir->lo), (skiplist_key_t *)&(ir->hi), (skiplist_data_t *)ir);
        ir->hi = r->hi;
        insert_interval_skiplist(s->mapping, (skiplist_key_t *)&(ir->lo), (skiplist_key_t *)&(ir->hi), (skiplist_data_t *)ir);

        //** Restart the iter cause of the deletion
        it = iter_search_interval_skiplist(s->mapping, (skiplist_key_t *)&lo, (skiplist_key_t *)&hi);
        ir = (slog_range_t *)next_interval_skiplist(&it);
    } else {  //** Completely dropped
        r_table[n] = ir;
        n++;
    }

    //** Cycle through the intervals to remove
    while ((r_table[n] = (slog_range_t *)next_interval_skiplist(&it)) != NULL) {
        log_printf(15, "i=%d dropping interval lo=" XOT " hi=" XOT "\n", n, r_table[n]->lo, r_table[n]->hi);
        n++;
        if (n == bufmax) {
            for (i=0; i<n; i++) {
                remove_interval_skiplist(s->mapping, (skiplist_key_t *)&(r_table[i]->lo), (skiplist_key_t *)&(r_table[i]->hi), (skiplist_data_t *)r_table[i]);
                free(r_table[i]);
            }
            it = iter_search_interval_skiplist(s->mapping, (skiplist_key_t *)&lo, (skiplist_key_t *)&hi);
            n = 0;
        }
    }

    if (n>0) {
        for (i=0; i<n; i++) {
            remove_interval_skiplist(s->mapping, (skiplist_key_t *)&(r_table[i]->lo), (skiplist_key_t *)&(r_table[i]->hi), (skiplist_data_t *)r_table[i]);
            free(r_table[i]);
        }
    }

    //** Adjust the size
    s->file_size = r->hi + 1;

    //** and free the range ptr
    free(r);

    log_printf(15, "new_log_intervals=%d\n", interval_skiplist_count(s->mapping));

    return(0);
}

//***********************************************************************
//  slog_insert_range - Inserts a range into the log.
//      NOTE: This does not do any flushing or updating of any of the
//             segments.  It strictly updates the ISL mappings.
//***********************************************************************

int _slog_insert_range(segment_t *seg, slog_range_t *r)
{
    seglog_priv_t *s = (seglog_priv_t *)seg->priv;
    ex_off_t irlo;
    interval_skiplist_iter_t it;
    slog_range_t *ir, *ir2;

    //** If a truncate just do it and return
    if (r->lo == -1) return(_slog_truncate_range(seg, r));

    it = iter_search_interval_skiplist(s->mapping, (skiplist_key_t *)&(r->lo), (skiplist_key_t *)&(r->hi));
    while ((ir = (slog_range_t *)next_interval_skiplist(&it)) != NULL) {
        remove_interval_skiplist(s->mapping, (skiplist_key_t *)&(ir->lo), (skiplist_key_t *)&(ir->hi), (skiplist_data_t *)ir);
        if (ir->lo < r->lo) {  //** Need to truncate the 1st portion (and maybe the end)
            if (ir->hi > r->hi) {  //** Straddles r
                type_malloc(ir2, slog_range_t, 1);  //** Do the end first
                ir2->lo = r->hi+1;
                ir2->hi = ir->hi;
                ir2->data_offset = ir->data_offset + (ir2->lo - ir->lo);
                insert_interval_skiplist(s->mapping, (skiplist_key_t *)&(ir2->lo), (skiplist_key_t *)&(ir2->hi), (skiplist_data_t *)ir2);

                ir->hi = r->lo-1;  //** Now do the front portion
                insert_interval_skiplist(s->mapping, (skiplist_key_t *)&(ir->lo), (skiplist_key_t *)&(ir->hi), (skiplist_data_t *)ir);
            } else {  //** Truncate 1st half
                ir->hi = r->lo-1;
                insert_interval_skiplist(s->mapping, (skiplist_key_t *)&(ir->lo), (skiplist_key_t *)&(ir->hi), (skiplist_data_t *)ir);
            }
        } else if (ir->hi <= r->hi) {  //** Completely contained in r so drop
            free(ir);
        } else {  //** Drop the 1st half keeping the end
            irlo = ir->lo;
            ir->lo = r->hi+1;
            ir->data_offset = ir->data_offset + (ir->lo - irlo);
            insert_interval_skiplist(s->mapping, (skiplist_key_t *)&(ir->lo), (skiplist_key_t *)&(ir->hi), (skiplist_data_t *)ir);
        }

        it = iter_search_interval_skiplist(s->mapping, (skiplist_key_t *)&(r->lo), (skiplist_key_t *)&(r->hi));
    }

    //** Check if this range can be combined with the previous
    irlo = r->lo - 1;
    it = iter_search_interval_skiplist(s->mapping, (skiplist_key_t *)&irlo, (skiplist_key_t *)&(r->hi));
    ir = (slog_range_t *)next_interval_skiplist(&it);
    if (ir != NULL) {
        irlo = ir->data_offset + ir->hi - ir->lo + 1;
        if (irlo == r->data_offset) {  //** Can combine ranges
//log_printf(0, "Combining ranges r=(" XOT ", " XOT ", " XOT ") ir=(" XOT ", " XOT ", " XOT ")\n", r->lo, r->hi, r->data_offset, ir->lo, ir->hi, ir->data_offset);
            r->lo = ir->lo;
            r->data_offset = ir->data_offset;
            remove_interval_skiplist(s->mapping, (skiplist_key_t *)&(ir->lo), (skiplist_key_t *)&(ir->hi), (skiplist_data_t *)ir);
        }
    }

    //** Insert the new range
    insert_interval_skiplist(s->mapping, (skiplist_key_t *)&(r->lo), (skiplist_key_t *)&(r->hi), (skiplist_data_t *)r);

    log_printf(15, "r->lo=" XOT " r->hi=" XOT " r->data_offset=" XOT " curr_file_size=" XOT "\n", r->lo, r->hi, r->data_offset, s->file_size);

    //** Adjust the file size if needed
    if (r->hi >= s->file_size) s->file_size = r->hi + 1;

    return(0);
}

//***********************************************************************
// seglog_write_func - Does the actual log write operation
//***********************************************************************

op_status_t seglog_write_func(void *arg, int id)
{
    seglog_rw_t *sw = (seglog_rw_t *)arg;
    seglog_priv_t *s = (seglog_priv_t *)sw->seg->priv;
    tbuffer_t tbuf;
    op_status_t status;
    int err, i;
    ex_off_t nbytes, table_offset, data_offset;
    ex_iovec_t ex_iov_data, ex_iov_table;
    slog_range_t r[sw->n_iov], *range;
    opque_t *q;
    op_generic_t *gop;

    log_printf(15, "seg=" XIDT " n_iov=%d offset[0]=" XOT " len[0]=" XOT "\n", sw->seg, sw->n_iov, sw->iov[0].offset, sw->iov[0].len);
    q = new_opque();

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
    s->log_size += sw->n_iov*sizeof(slog_range_t);
    data_offset = s->data_size;
    s->data_size += nbytes;

    segment_unlock(sw->seg);

    //** Do the table and data writes
    ex_iovec_single(&ex_iov_data, data_offset, nbytes);
    gop = segment_write(s->data_seg, sw->da, sw->rw_hints, 1, &ex_iov_data, sw->buffer, sw->boff, sw->timeout);
    opque_add(q, gop);

    i = sw->n_iov*sizeof(slog_range_t);
    ex_iovec_single(&ex_iov_table, table_offset, i);
    tbuffer_single(&tbuf, i, (char *)r);
    gop = segment_write(s->table_seg, sw->da, sw->rw_hints, 1, &ex_iov_table, &tbuf, 0, sw->timeout);
    opque_add(q, gop);

    err = opque_waitall(q);
    if (err != OP_STATE_SUCCESS) {
        status = op_failure_status;
        segment_lock(sw->seg);
        s->hard_errors++;
        segment_unlock(sw->seg);
    } else {
        status = op_success_status;

        //** Update the mappings
        segment_lock(sw->seg);
        for (i=0; i < sw->n_iov; i++) {
            type_malloc(range, slog_range_t, 1);
            range->lo = r[i].lo;
            range->hi = r[i].lo + r[i].hi - 1;
            range->data_offset = r[i].data_offset;
            _slog_insert_range(sw->seg, range);
        }
        segment_unlock(sw->seg);
    }

    opque_free(q, OP_DESTROY);

    return(status);
}

//***********************************************************************
// seglog_write - Performs a segment write operation
//***********************************************************************

op_generic_t *seglog_write(segment_t *seg, data_attr_t *da, segment_rw_hints_t *rw_hints, int n_iov, ex_iovec_t *iov, tbuffer_t *buffer, ex_off_t boff, int timeout)
{
    seglog_priv_t *s = (seglog_priv_t *)seg->priv;
    seglog_rw_t *sw;
    op_generic_t *gop;

    type_malloc(sw, seglog_rw_t, 1);
    sw->seg = seg;
    sw->da = da;
    sw->rw_hints = rw_hints;
    sw->n_iov = n_iov;
    sw->iov = iov;
    sw->boff = boff;
    sw->buffer = buffer;
    sw->timeout = timeout;
    sw->rw_mode = 1;
    gop = new_thread_pool_op(s->tpc, NULL, seglog_write_func, (void *)sw, free, 1);

    return(gop);
}

//***********************************************************************
// seglog_read_func - Does the actual log read operation
//***********************************************************************

op_status_t seglog_read_func(void *arg, int id)
{
    seglog_rw_t *sw = (seglog_rw_t *)arg;
    seglog_priv_t *s = (seglog_priv_t *)sw->seg->priv;
    interval_skiplist_iter_t it;
    slog_range_t *ir;
    opque_t *q;
    op_generic_t *gop;
    int i, err, n_iov, slot;
    op_status_t status;
    ex_off_t lo, hi, prev_end;
    ex_off_t bpos, pos, range_offset;
    ex_iovec_t *ex_iov, *iov;

    q = new_opque();
    iov = sw->iov;

    //** Do the mapping of where to retreive the data
    segment_lock(sw->seg);

    //** First figure out how many ex_iov's are needed
    n_iov = 0;
    for (i=0; i< sw->n_iov; i++) {
        lo = iov[i].offset;
        hi = lo + iov[i].len - 1;
        n_iov += 2*count_interval_skiplist(s->mapping, (skiplist_key_t *)&lo, (skiplist_key_t *)&hi) + 1;
    }
    n_iov += 10;  //** Just to be safe
    type_malloc(ex_iov, ex_iovec_t, n_iov);

    //** Now generate the actual task list
    bpos = sw->boff;
    slot = 0;
    for (i=0; i < sw->n_iov; i++) {
        lo = iov[i].offset;
        hi = lo + iov[i].len - 1;
        pos = lo;
        prev_end = -1;
        it = iter_search_interval_skiplist(s->mapping, (skiplist_key_t *)&lo, (skiplist_key_t *)&hi);
        while ((ir = (slog_range_t *)next_interval_skiplist(&it)) != NULL) {
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
                opque_add(q, gop);
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

            opque_add(q, gop);
            pos = pos + ex_iov[slot].len;
            bpos = bpos + ex_iov[slot].len;
            prev_end = ir->hi;
            slot++;
        }

        if (prev_end == -1) { //** We have a hole so get it from the base
            ex_iov[slot].offset = lo;
            ex_iov[slot].len = hi - lo + 1;
            gop = segment_read(s->base_seg, sw->da, sw->rw_hints, 1, &(ex_iov[slot]), sw->buffer, bpos, sw->timeout);
            opque_add(q, gop);
            bpos = bpos + ex_iov[slot].len;
            slot++;
        } else if (prev_end < hi) {    //** Check if we read from the base on the end
            ex_iov[slot].offset = pos;
            ex_iov[slot].len = hi - (prev_end+1) + 1;
            gop = segment_read(s->base_seg, sw->da, sw->rw_hints, 1, &(ex_iov[slot]), sw->buffer, bpos, sw->timeout);
            opque_add(q, gop);
            bpos = bpos + ex_iov[slot].len;
            slot++;
        }
    }

    segment_unlock(sw->seg);

    err = opque_waitall(q);
    if (err != OP_STATE_SUCCESS) {
        status = op_failure_status;
        segment_lock(sw->seg);
        s->hard_errors++;
        segment_unlock(sw->seg);
    } else {
        status = op_success_status;
    }

    opque_free(q, OP_DESTROY);
    free(ex_iov);

    return(status);
}

//***********************************************************************
// seglog_read - Read from a log segment
//***********************************************************************

op_generic_t *seglog_read(segment_t *seg, data_attr_t *da, segment_rw_hints_t *rw_hints, int n_iov, ex_iovec_t *iov, tbuffer_t *buffer, ex_off_t boff, int timeout)
{
    seglog_priv_t *s = (seglog_priv_t *)seg->priv;
    seglog_rw_t *sw;
    op_generic_t *gop;

    type_malloc(sw, seglog_rw_t, 1);
    sw->seg = seg;
    sw->da = da;
    sw->rw_hints = rw_hints;
    sw->n_iov = n_iov;
    sw->iov = iov;
    sw->boff = boff;
    sw->buffer = buffer;
    sw->timeout = timeout;
    sw->rw_mode = 0;
    gop = new_thread_pool_op(s->tpc, NULL, seglog_read_func, (void *)sw, free, 1);

    return(gop);
}


//***********************************************************************
// slog_load - Loads the intitial mapping table
//***********************************************************************

int _slog_load(segment_t *seg)
{
    seglog_priv_t *s = (seglog_priv_t *)seg->priv;
    int timeout = 20;
    ex_off_t i;
    int last_bad, err_count;
    op_generic_t *gop;
    data_attr_t *da;
    slog_range_t *r;
    ex_iovec_t ex_iov;
    tbuffer_t tbuf;

    da = ds_attr_create(s->ds);

    s->file_size = segment_size(s->base_seg);
    s->log_size = segment_size(s->table_seg);
    s->data_size = segment_size(s->data_seg);

    log_printf(15, "INITIAL:  fsize=" XOT " lsize=" XOT " dsize=" XOT "\n", s->file_size, s->log_size, s->data_size);

    last_bad = 0;
    err_count = 0;
    for (i=0; i<s->log_size; i += sizeof(slog_range_t)) {
        type_malloc_clear(r, slog_range_t, 1);
        ex_iovec_single(&ex_iov, i, sizeof(slog_range_t));
        tbuffer_single(&tbuf, sizeof(slog_range_t), (char *)r);
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

ex_off_t slog_changes(segment_t *seg, ex_off_t lo, ex_off_t hi, Stack_t *stack)
{
    seglog_priv_t *s = (seglog_priv_t *)seg->priv;
    interval_skiplist_iter_t it;
    slog_range_t *ir;
    slog_changes_t *clog;
    ex_off_t nbytes, prev_end;
    int log_base;

    log_base = (strcmp(segment_type(s->base_seg), SEGMENT_TYPE_LOG) == 0) ? 1 : 0;
    nbytes = 0;

    segment_lock(seg);
    prev_end = -1;
    it = iter_search_interval_skiplist(s->mapping, (skiplist_key_t *)&lo, (skiplist_key_t *)&hi);
    while ((ir = (slog_range_t *)next_interval_skiplist(&it)) != NULL) {
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

        type_malloc(clog, slog_changes_t, 1);

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
        move_to_bottom(stack);
        insert_below(stack, clog);
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

op_status_t seglog_clone_func(void *arg, int id)
{
    seglog_clone_t *slc = (seglog_clone_t *)arg;
    seglog_priv_t *ss = (seglog_priv_t *)slc->sseg->priv;
    seglog_priv_t *sd = (seglog_priv_t *)slc->dseg->priv;
    segment_t *base;
    op_generic_t *gop;
    ex_off_t nbytes_base, nbytes_log;
    int bufsize = 50*1024*1024;
    ex_off_t dt, pos, rpos, wpos, rlen, dlen;
    tbuffer_t *wbuf, *rbuf, *tmpbuf;
    tbuffer_t tbuf1, tbuf2;
    int err, do_segment_copy;
    char *buffer = NULL;
    slog_changes_t *clog;
    opque_t *q1, *q2, *q;
    Stack_t *stack;
    op_status_t status;

    do_segment_copy = 0;
    stack = NULL;
    q2 = NULL;

    q = new_opque();
    opque_start_execution(q);

    //** SEe if we are using an old seg.  If so we need to trunc it first
    if (slc->trunc == 1) {
        opque_add(q, segment_truncate(sd->table_seg, slc->da, 0, slc->timeout));
        opque_add(q, segment_truncate(sd->data_seg, slc->da, 0, slc->timeout));
        opque_add(q, segment_truncate(sd->base_seg, slc->da, 0, slc->timeout));
    }

    //** Go ahead and start cloning the log
    opque_add(q, segment_clone(ss->table_seg, slc->da, &(sd->table_seg), CLONE_STRUCTURE, slc->attr, slc->timeout));
    opque_add(q, segment_clone(ss->data_seg, slc->da, &(sd->data_seg), CLONE_STRUCTURE, slc->attr, slc->timeout));

    //** Check and see how much data will be transferred if using base
    base = _slog_find_base(ss->base_seg);

    opque_waitall(q);

    if (slc->mode == CLONE_STRUCTURE) {  //** Only cloning the structure
        opque_add(q, segment_clone(base, slc->da, &(sd->base_seg), CLONE_STRUCTURE, slc->attr, slc->timeout));
    } else {
        nbytes_base = segment_size(base);
        stack = new_stack();
        sd->file_size = segment_size(slc->sseg);
        nbytes_log = slog_changes(slc->sseg, 0, sd->file_size, stack);

        //** Make a crude estimate of the time.
        //** I assume depot-depot copies are 5x faster than going through the client
        //** Have to Read + write the log data hence the 2x below
        dt = 2*nbytes_log + nbytes_base / 5;
        log_printf(15, "dt=" XOT " nbytes_log=" XOT " nbytes_base=" XOT " ss->file_size=" XOT "\n", dt, nbytes_log, nbytes_base, ss->file_size);
        if (dt > ss->file_size) {
            do_segment_copy = 1;
            opque_add(q, segment_clone(base, slc->da, &(sd->base_seg), CLONE_STRUCTURE, slc->attr, slc->timeout));
        }
    }

    //** Wait for the initial cloning to complete
    err = opque_waitall(q);

    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "Error during intial cloning phase:  src=" XIDT "\n", segment_id(slc->sseg));
        if (stack != NULL) free_stack(stack, 1);
        opque_free(q, OP_DESTROY);
        return(op_failure_status);
    }

    //** Now copy the data if needed
    if (do_segment_copy == 1) {  //** segment_copy() method
        type_malloc(buffer, char, bufsize);
        opque_add(q, segment_copy(ss->tpc, slc->da, NULL, slc->sseg, slc->dseg, 0, 0, ss->file_size, bufsize, buffer, 0, slc->timeout));
    } else if (slc->mode == CLONE_STRUCT_AND_DATA) {  //** Use the incremental log+base method
        //** First clone the base struct and data
        opque_add(q, segment_clone(base, slc->da, &(sd->base_seg), CLONE_STRUCT_AND_DATA, slc->attr, slc->timeout));

        //** Now do the logs
        //** Set up the buffers
        type_malloc(buffer, char, 2*bufsize);
        tbuffer_single(&tbuf1, bufsize, buffer);
        tbuffer_single(&tbuf2, bufsize, &(buffer[bufsize]));
        rbuf = &tbuf1;
        wbuf = &tbuf2;
        rlen = 0;
        q1 = q;
        q2 = new_opque();
        while ((clog = (slog_changes_t *)get_ele_data(stack)) != NULL) {
            pos = 0;
            rpos = clog->seg_offset;
            wpos = clog->lo;
            while (pos < clog->len) {
                dlen = clog->len - pos;
                ex_iovec_single(&(clog->rex), rpos, dlen);
                gop = segment_read(clog->seg, slc->da, NULL, 1, &(clog->rex), rbuf, rlen, slc->timeout);
                opque_add(q1, gop);

                ex_iovec_single(&(clog->wex), wpos, dlen);
                gop = segment_write(clog->seg, slc->da, NULL, 1, &(clog->wex), wbuf, rlen, slc->timeout);
                opque_add(q2, gop);

                pos += dlen;
                rpos += dlen;
                wpos += dlen;
                rlen += dlen;

                if (rlen <= 0) {  //** Time to flush the data
                    err = opque_waitall(q1);  //** Wait for the data to be R/W
                    opque_free(q1, OP_DESTROY);  //** Free the space
                    if (err != OP_STATE_SUCCESS) {
                        log_printf(1, "Error during log copy phase:  src=" XIDT "\n", segment_id(slc->sseg));
                        free_stack(stack, 1);
                        free(buffer);
                        opque_free(q2, OP_DESTROY);
                        return(op_failure_status);
                    }

                    //** Swap the tasks and start executing them
                    tmpbuf = rbuf;
                    rbuf = wbuf;
                    wbuf = tmpbuf;
                    q1 = q2;
                    q2 = new_opque();  //** Make a new que but don't start execution.  Have to wait for the reads to finish
                    opque_start_execution(q1);  //** Start the previous write tasks executing while we add read tasks to it

                    rlen = 0;
                }
            }

            move_down(stack);
        }

    }

    status = op_success_status;
    err = opque_waitall(q);
    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "Error during log copy phase:  src=" XIDT "\n", segment_id(slc->sseg));
        status = op_failure_status;
    }

    //** Make sure we don't have an empty log
    if (q2 != NULL) {
        if (opque_task_count(q2) > 0) {
            err = opque_waitall(q2);  //** Wait for the data to be R/W
            if (err != OP_STATE_SUCCESS) {
                log_printf(1, "Error during log copy phase:  src=" XIDT "\n", segment_id(slc->sseg));
                status = op_failure_status;
            }
        }
    }

    //** Flag them as being used
    if (slc->trunc == 0) {
        atomic_inc(sd->table_seg->ref_count);
        atomic_inc(sd->data_seg->ref_count);
        atomic_inc(sd->base_seg->ref_count);
    }

    //** Clean up
    if (stack != NULL) free_stack(stack, 1);
    if (buffer != NULL) free(buffer);
    if (q2 == NULL) {
        opque_free(q, OP_DESTROY);
    } else {
        opque_free(q1, OP_DESTROY);
        opque_free(q2, OP_DESTROY);
    }

    return(status);
}

//***********************************************************************
// seglog_clone - Clones a segment
//***********************************************************************

op_generic_t *seglog_clone(segment_t *seg, data_attr_t *da, segment_t **clone_seg, int mode, void *attr, int timeout)
{
    segment_t *clone;
    seglog_priv_t *sd;
    op_generic_t *gop;
    seglog_clone_t *slc;
    int use_existing = (*clone_seg != NULL) ? 1 : 0;

    //** Make the base segment
//log_printf(0, " before clone create\n");
    if (use_existing == 0) *clone_seg = segment_log_create(seg->ess);
//log_printf(0, " after clone create\n");
    clone = *clone_seg;
    sd = (seglog_priv_t *)clone->priv;

    log_printf(15, "use_existing=%d sseg=" XIDT " dseg=" XIDT "\n", use_existing, segment_id(seg), segment_id(clone));

    //** Copy the header
    if ((seg->header.name != NULL) && (use_existing == 0)) clone->header.name = strdup(seg->header.name);

    type_malloc(slc, seglog_clone_t, 1);
    slc->sseg = seg;
    slc->dseg = clone;
    slc->da = da;
    slc->mode = mode;
    slc->timeout = timeout;
    slc->attr = attr;
    slc->trunc = use_existing;
    gop = new_thread_pool_op(sd->tpc, NULL, seglog_clone_func, (void *)slc, free, 1);

    return(gop);
}

//***********************************************************************
//  seglog_truncate_func - Does the actual segment truncate operation
//***********************************************************************

op_status_t seglog_truncate_func(void *arg, int id)
{
    seglog_truncate_t *st = (seglog_truncate_t *)arg;
    seglog_priv_t *s = (seglog_priv_t *)st->seg->priv;
    op_status_t status;
    tbuffer_t tbuf_table, tbuf_data;
    ex_iovec_t ex_iov_table, ex_iov_data;
    ex_off_t table_offset, data_offset;
    char c = 0;
    int err;
    slog_range_t *r;
    opque_t *q;
    op_generic_t *gop = NULL;

    q = new_opque();

    segment_lock(st->seg);
    if (st->new_size < s->file_size) { //** Shrink operation
        table_offset = s->log_size;
        type_malloc(r, slog_range_t, 1);
        r->lo = -1;
        r->hi = st->new_size - 1;
        r->data_offset = -1;
        ex_iovec_single(&ex_iov_table, table_offset, sizeof(slog_range_t));
        tbuffer_single(&tbuf_table, sizeof(slog_range_t), (char *)r);
        gop = segment_write(s->table_seg, st->da, NULL, 1, &ex_iov_table, &tbuf_table, 0, st->timeout);
        opque_add(q, gop);

        s->log_size += sizeof(slog_range_t);
    } else if (st->new_size > s->file_size) {  //** Grow operation
        table_offset = s->log_size;
        data_offset = s->data_size;
        type_malloc(r, slog_range_t, 1);
        r->lo = s->file_size - 1;
        r->hi = st->new_size - s->file_size;  //** On disk this is the len
        r->data_offset = data_offset;
        ex_iovec_single(&ex_iov_table, table_offset, sizeof(slog_range_t));
        tbuffer_single(&tbuf_table, sizeof(slog_range_t), (char *)r);
        gop = segment_write(s->table_seg, st->da, NULL, 1, &ex_iov_table, &tbuf_table, 0, st->timeout);
        opque_add(q, gop);

        //** Don't need to write all the blanks.  Just the last byte.
        ex_iovec_single(&ex_iov_data, data_offset + r->hi - 1, 1);
        tbuffer_single(&tbuf_data, 1, &c);
        gop = segment_write(s->data_seg, st->da, NULL, 1, &ex_iov_data, &tbuf_data, 0, st->timeout);
        opque_add(q, gop);

        s->log_size += sizeof(slog_range_t);
        s->data_size += st->new_size - s->file_size;

    }
    segment_unlock(st->seg);

    //** If nothing to do exit
    if (gop == NULL) {
        opque_free(q, OP_DESTROY);
        return(op_success_status);
    }

    //** Perform the updates
    err = opque_waitall(q);
    if (err != OP_STATE_SUCCESS) {
        status = op_failure_status;
        segment_lock(st->seg);
        s->hard_errors++;
        segment_unlock(st->seg);
    } else {
        status = op_success_status;

        //** Update the mappings
        segment_lock(st->seg);
        if (st->new_size < s->file_size) { //** Grow op so tweak the r
            r->hi = r->lo + r->hi - 1;
        }

        _slog_insert_range(st->seg, r);
        segment_unlock(st->seg);
    }

    opque_free(q, OP_DESTROY);

    return(status);
}

//***********************************************************************
// seglog_truncate - Expands or contracts a linear segment
//***********************************************************************

op_generic_t *seglog_truncate(segment_t *seg, data_attr_t *da, ex_off_t new_size, int timeout)
{
    seglog_priv_t *s = (seglog_priv_t *)seg->priv;
    seglog_truncate_t *st;

    if (new_size < 0) { //** Reserve space call which is ignored at this time
        return(gop_dummy(op_success_status));
    }

    type_malloc_clear(st, seglog_truncate_t, 1);

    st->seg = seg;
    st->new_size = new_size;
    st->timeout = timeout;
    st->da = da;

    return(new_thread_pool_op(s->tpc, NULL, seglog_truncate_func, (void *)st, free, 1));
}

//***********************************************************************
// seglog_remove - Removes the log segment from disk
//***********************************************************************

op_generic_t *seglog_remove(segment_t *seg, data_attr_t *da, int timeout)
{
    seglog_priv_t *s = (seglog_priv_t *)seg->priv;
    opque_t *q = new_opque();

    opque_add(q, segment_remove(s->table_seg, da, timeout));
    opque_add(q, segment_remove(s->data_seg, da, timeout));
    opque_add(q, segment_remove(s->base_seg, da, timeout));
    return(opque_get_gop(q));
}


//***********************************************************************
// seglog_inspect - Inspects the log segment
//***********************************************************************

op_generic_t *seglog_inspect(segment_t *seg, data_attr_t *da, info_fd_t *fd, int mode, ex_off_t bufsize, inspect_args_t *args, int timeout)
{
    seglog_priv_t *s = (seglog_priv_t *)seg->priv;
    opque_t *q = new_opque();

    opque_add(q, segment_inspect(s->table_seg, da, fd, mode, bufsize, args, timeout));
    opque_add(q, segment_inspect(s->data_seg, da, fd, mode, bufsize, args, timeout));
    opque_add(q, segment_inspect(s->base_seg, da, fd, mode, bufsize, args, timeout));
    return(opque_get_gop(q));
}


//***********************************************************************
// seglog_flush - Flushes a segment
//***********************************************************************

op_generic_t *seglog_flush(segment_t *seg, data_attr_t *da, ex_off_t lo, ex_off_t hi, int timeout)
{
    seglog_priv_t *s = (seglog_priv_t *)seg->priv;
    opque_t *q = new_opque();

    opque_add(q, segment_flush(s->table_seg, da, 0, s->log_size, timeout));
    opque_add(q, segment_flush(s->data_seg, da, 0, s->data_size, timeout));
    return(opque_get_gop(q));
}

//***********************************************************************
// seglog_size - Returns the segment size.
//***********************************************************************

ex_off_t seglog_size(segment_t *seg)
{
    seglog_priv_t *s = (seglog_priv_t *)seg->priv;
    ex_off_t size;

    segment_lock(seg);
    size = s->file_size;
    segment_unlock(seg);

    return(size);
}

//***********************************************************************
// seglog_block_size - Returns the blocks size which is always 1
//***********************************************************************

ex_off_t seglog_block_size(segment_t *seg)
{
//  seglog_priv_t *s = (seglog_priv_t *)seg->priv;

    return(1);
}

//***********************************************************************
// seglog_signature - Generates the segment signature
//***********************************************************************

int seglog_signature(segment_t *seg, char *buffer, int *used, int bufsize)
{
    seglog_priv_t *s = (seglog_priv_t *)seg->priv;

    append_printf(buffer, used, bufsize, "log(log)\n");
    segment_signature(s->table_seg, buffer, used, bufsize);

    append_printf(buffer, used, bufsize, "log(data)\n");
    segment_signature(s->data_seg, buffer, used, bufsize);

    append_printf(buffer, used, bufsize, "log(base)\n");
    segment_signature(s->base_seg, buffer, used, bufsize);

    return(0);
}


//***********************************************************************
// seglog_serialize_text -Convert the segment to a text based format
//***********************************************************************

int seglog_serialize_text(segment_t *seg, exnode_exchange_t *exp)
{
    seglog_priv_t *s = (seglog_priv_t *)seg->priv;
    int bufsize=1024*1024;
    char segbuf[bufsize];
    char *etext;
    int sused;
    exnode_exchange_t *child_exp;

    segbuf[0] = 0;

    sused = 0;

    //** Store the segment header
    append_printf(segbuf, &sused, bufsize, "[segment-" XIDT "]\n", seg->header.id);
    if ((seg->header.name != NULL) && (strcmp(seg->header.name, "") != 0)) {
        etext = escape_text("=", '\\', seg->header.name);
        append_printf(segbuf, &sused, bufsize, "name=%s\n", etext);
        free(etext);
    }
    append_printf(segbuf, &sused, bufsize, "type=%s\n", SEGMENT_TYPE_LOG);
    append_printf(segbuf, &sused, bufsize, "ref_count=%d\n", seg->ref_count);


    //** And the children segments
    append_printf(segbuf, &sused, bufsize, "log=" XIDT "\n", segment_id(s->table_seg));
    child_exp = exnode_exchange_create(EX_TEXT);
    segment_serialize(s->table_seg, child_exp);
    exnode_exchange_append(exp, child_exp);
    exnode_exchange_free(child_exp);

    append_printf(segbuf, &sused, bufsize, "data=" XIDT "\n", segment_id(s->data_seg));
    segment_serialize(s->data_seg, child_exp);
    exnode_exchange_append(exp, child_exp);
    exnode_exchange_free(child_exp);

    append_printf(segbuf, &sused, bufsize, "base=" XIDT "\n", segment_id(s->base_seg));
    segment_serialize(s->base_seg, child_exp);
    exnode_exchange_append(exp, child_exp);
    exnode_exchange_free(child_exp);

    //** And finally the the container
    exnode_exchange_append_text(exp, segbuf);
    exnode_exchange_destroy(child_exp);

    return(0);
}

//***********************************************************************
// seglog_serialize_proto -Convert the segment to a protocol buffer
//***********************************************************************

int seglog_serialize_proto(segment_t *seg, exnode_exchange_t *exp)
{
    return(-1);
}

//***********************************************************************
// seglog_serialize -Convert the segment to a more portable format
//***********************************************************************

int seglog_serialize(segment_t *seg, exnode_exchange_t *exp)
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

int seglog_deserialize_text(segment_t *seg, ex_id_t id, exnode_exchange_t *exp)
{
    seglog_priv_t *s = (seglog_priv_t *)seg->priv;
    int bufsize=1024;
    char seggrp[bufsize];
    inip_file_t *fd;

    //** Parse the ini text
    fd = exp->text.fd;

    //** Make the segment section name
    snprintf(seggrp, bufsize, "segment-" XIDT, id);

    //** Get the segment header info
    seg->header.id = id;
    seg->header.type = SEGMENT_TYPE_LOG;
    seg->header.name = inip_get_string(fd, seggrp, "name", "");

    //** Load the child segments
    id = inip_get_integer(fd, seggrp, "log", 0);
    if (id == 0) return (-1);
    s->table_seg = load_segment(seg->ess, id, exp);
    if (s->table_seg == NULL) return(-2);
    atomic_inc(s->table_seg->ref_count);

    id = inip_get_integer(fd, seggrp, "data", 0);
    if (id == 0) return (-1);
    s->data_seg = load_segment(seg->ess, id, exp);
    if (s->data_seg == NULL) return(-2);
    atomic_inc(s->data_seg->ref_count);

    id = inip_get_integer(fd, seggrp, "base", 0);
    if (id == 0) return (-1);
    s->base_seg = load_segment(seg->ess, id, exp);
    if (s->base_seg == NULL) return(-2);
    atomic_inc(s->base_seg->ref_count);

    //** Load the log table which will also set the size
    _slog_load(seg);

    log_printf(15, "seglog_deserialize_text: seg=" XIDT "\n", segment_id(seg));
    return(0);
}


//***********************************************************************
// seglog_deserialize_proto - Read the prot formatted segment
//***********************************************************************

int seglog_deserialize_proto(segment_t *seg, ex_id_t id, exnode_exchange_t *exp)
{
    return(-1);
}

//***********************************************************************
// seglog_deserialize -Convert from the portable to internal format
//***********************************************************************

int seglog_deserialize(segment_t *seg, ex_id_t id, exnode_exchange_t *exp)
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

void seglog_destroy(segment_t *seg)
{
    int i, n;
    interval_skiplist_iter_t it;
    slog_range_t **r_list;
    seglog_priv_t *s = (seglog_priv_t *)seg->priv;

    //** Check if it's still in use
    log_printf(15, "seglog_destroy: seg->id=" XIDT " ref_count=%d\n", segment_id(seg), seg->ref_count);

    if (seg->ref_count > 0) return;

    //** Destroy the child segments
    if (s->table_seg != NULL) {
        atomic_dec(s->table_seg->ref_count);
        segment_destroy(s->table_seg);
    }
    if (s->data_seg != NULL) {
        atomic_dec(s->data_seg->ref_count);
        segment_destroy(s->data_seg);
    }
    if (s->base_seg != NULL) {
        atomic_dec(s->base_seg->ref_count);
        segment_destroy(s->base_seg);
    }

    //** Now free the mapping table
    n = interval_skiplist_count(s->mapping);
    type_malloc_clear(r_list, slog_range_t *, n);
    it = iter_search_interval_skiplist(s->mapping, (skiplist_key_t *)NULL, (skiplist_key_t *)NULL);
    for (i=0; i<n; i++) {
        r_list[i] = (slog_range_t *)next_interval_skiplist(&it);
    }
    destroy_interval_skiplist(s->mapping);

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

segment_t *segment_log_create(void *arg)
{
    service_manager_t *es = (service_manager_t *)arg;
    seglog_priv_t *s;
    segment_t *seg;

    type_malloc_clear(seg, segment_t, 1);
    type_malloc_clear(s, seglog_priv_t, 1);

    s->mapping = create_interval_skiplist(&skiplist_compare_ex_off, NULL, NULL, NULL);
    seg->priv = s;
    s->file_size = 0;

    generate_ex_id(&(seg->header.id));
    atomic_set(seg->ref_count, 0);
    seg->header.type = SEGMENT_TYPE_LOG;

    assert_result(apr_pool_create(&(seg->mpool), NULL), APR_SUCCESS);
    apr_thread_mutex_create(&(seg->lock), APR_THREAD_MUTEX_DEFAULT, seg->mpool);
    apr_thread_cond_create(&(seg->cond), seg->mpool);

    seg->ess = es;
    s->tpc = lookup_service(es, ESS_RUNNING, ESS_TPC_UNLIMITED);
    s->ds = lookup_service(es, ESS_RUNNING, ESS_DS);

    seg->fn.read = seglog_read;
    seg->fn.write = seglog_write;
    seg->fn.inspect = seglog_inspect;
    seg->fn.truncate = seglog_truncate;
    seg->fn.remove = seglog_remove;
    seg->fn.flush = seglog_flush;
    seg->fn.clone = seglog_clone;
    seg->fn.signature = seglog_signature;
    seg->fn.size = seglog_size;
    seg->fn.block_size = seglog_block_size;
    seg->fn.serialize = seglog_serialize;
    seg->fn.deserialize = seglog_deserialize;
    seg->fn.destroy = seglog_destroy;

    return(seg);
}

//***********************************************************************
// segment_log_load - Loads a log segment from ini/ex3
//***********************************************************************

segment_t *segment_log_load(void *arg, ex_id_t id, exnode_exchange_t *ex)
{
    segment_t *seg = segment_log_create(arg);
    if (segment_deserialize(seg, id, ex) != 0) {
        segment_destroy(seg);
        seg = NULL;
    }
    return(seg);
}

//***********************************************************************
// slog_make - Adds a log level using the given base segment
//    The empty log and data must be provided.
//***********************************************************************

segment_t *slog_make(service_manager_t *sm, segment_t *table, segment_t *data, segment_t *base)
{
    segment_t *seg;
    seglog_priv_t *s;
    segment_create_t *screate;

    screate = lookup_service(sm, SEG_SM_CREATE, SEGMENT_TYPE_LOG);
    if (screate == NULL) return(NULL);

    seg = (*screate)(sm);
    s = (seglog_priv_t *)seg->priv;
    s->table_seg = table;
    s->data_seg = data;
    s->base_seg = base;
    s->file_size = segment_size(base);

    return(seg);
}

//***********************************************************************
// seglog_merge_with_base_func - Merges the log with the base
//***********************************************************************

op_status_t seglog_merge_with_base_func(void *arg, int id)
{
    seglog_merge_t *sm = (seglog_merge_t *)arg;
    seglog_priv_t *s = (seglog_priv_t *)sm->seg->priv;
    opque_t *qin, *qout;
    op_generic_t *gop;
    ex_iovec_t *ex_in, *ex_out;
    slog_range_t *r;
    tbuffer_t tbuf;
    ex_off_t pos, len, blen, bpos;
    int i, n_iov, err;
    interval_skiplist_iter_t it;

    qin = new_opque();
    qout = new_opque();

    n_iov = interval_skiplist_count(s->mapping);
    type_malloc(ex_in, ex_iovec_t, n_iov);
    type_malloc(ex_out, ex_iovec_t, n_iov);
    tbuffer_single(&tbuf, sm->bufsize, sm->buffer);

    segment_lock(sm->seg);

    it = iter_search_interval_skiplist(s->mapping, (skiplist_key_t *)NULL, (skiplist_key_t *)NULL);
    pos = 0;
    for (i=0; i<=n_iov; i++) {
        if (i < n_iov) {
            r = (slog_range_t *)next_interval_skiplist(&it);
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
                return(op_failure_status);
            }
            err = opque_waitall(qout);
            if (err != OP_STATE_SUCCESS) {
                segment_unlock(sm->seg);
                log_printf(1, "seg=" XIDT " Error writing segment!\n", segment_id(sm->seg));
                return(op_failure_status);
            }
            opque_free(qout, OP_DESTROY);
            qout = new_opque();
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
                opque_add(qin, gop);
                err = opque_waitall(qin);
                if (err != OP_STATE_SUCCESS) {
                    segment_unlock(sm->seg);
                    log_printf(1, "seg=" XIDT " Error reading segment!\n", segment_id(sm->seg));
                    return(op_failure_status);
                }

                gop = segment_write(s->base_seg, sm->da, NULL, 1, &(ex_out[i]), &tbuf, 0, sm->timeout);
                opque_add(qout, gop);
                if (err != OP_STATE_SUCCESS) {
                    segment_unlock(sm->seg);
                    log_printf(1, "seg=" XIDT " Error writing segment!\n", segment_id(sm->seg));
                    return(op_failure_status);
                }

                ex_in[i].offset += blen;
                ex_out[i].offset += blen;
            }
            pos = 0;
        } else {
            gop = segment_read(s->data_seg, sm->da, NULL, 1, &(ex_in[i]), &tbuf, pos, sm->timeout);
            opque_add(qin, gop);
            gop = segment_write(s->base_seg, sm->da, NULL, 1, &(ex_out[i]), &tbuf, pos, sm->timeout);
            opque_add(qout, gop);
            pos = pos + ex_in[i].len;
        }
    }

    //** If needed get rid of the old log
    err = OP_STATE_SUCCESS;
    if (sm->truncate_old_log == 1) {
        log_printf(15, "truncating old log\n");
        opque_add(qin, segment_truncate(s->table_seg, sm->da, 0, sm->timeout));
        opque_add(qin, segment_truncate(s->data_seg, sm->da, 0, sm->timeout));

        //** empty the log table
        type_malloc(r, slog_range_t, 1);
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

    opque_free(qin, OP_DESTROY);
    opque_free(qout, OP_DESTROY);

    free(ex_in);
    free(ex_out);

    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "seg=" XIDT " Error truncating table/data logs!\n", segment_id(sm->seg));
        return(op_failure_status);
    }

    log_printf(15, "seg=" XIDT " SUCCESS!\n", segment_id(sm->seg));

    return(op_success_status);
}


//***********************************************************************
// slog_merge_with_base - Merges the log (table/data segments) with the base.
//   If truncate_old_log == 1 then the old log is truncated back to 0.
//   Otherwise the old log is not touched and left intact (and superfluos).
//***********************************************************************

op_generic_t *slog_merge_with_base(segment_t *seg, data_attr_t *da, ex_off_t bufsize, char *buffer, int truncate_old_log, int timeout)
{
    seglog_merge_t *st;
    seglog_priv_t *s = (seglog_priv_t *)seg->priv;

    type_malloc_clear(st, seglog_merge_t, 1);

    st->seg = seg;
    st->truncate_old_log = truncate_old_log;
    st->bufsize = bufsize;
    st->buffer = buffer;
    st->timeout = timeout;
    st->da = da;

    return(new_thread_pool_op(s->tpc, NULL, seglog_merge_with_base_func, (void *)st, free, 1));
}

