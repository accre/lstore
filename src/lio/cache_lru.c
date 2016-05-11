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

//*************************************************************************
//*************************************************************************

#define _log_module_index 143

#include "cache.h"
#include "cache_lru_priv.h"
#include <tbx/type_malloc.h>
#include <tbx/log.h>
#include "ex3_compare.h"
#include <tbx/apr_wrapper.h>

tbx_atomic_unit32_t lru_dummy = -1000;

//*************************************************************************
// _lru_max_bytes - REturns the max amount of space to use
//*************************************************************************

ex_off_t _lru_max_bytes(cache_t *c)
{
    cache_lru_t *cp = (cache_lru_t *)c->fn.priv;

    return(cp->max_bytes);
}

//*************************************************************************
// lru_dirty_thread - Thread to handle flushing due to dirty ratio
//*************************************************************************

void *lru_dirty_thread(apr_thread_t *th, void *data)
{
    cache_t *c = (cache_t *)data;
    cache_lru_t *cp = (cache_lru_t *)c->fn.priv;
    double df;
    int n, i;
    ex_id_t *id;
    segment_t *seg;
    opque_t *q;
    op_generic_t *gop;
    cache_segment_t *s;
    tbx_sl_iter_t it;
    segment_t **flush_list;

    cache_lock(c);

    log_printf(15, "Dirty thread launched\n");
    while (c->shutdown_request == 0) {
        apr_thread_cond_timedwait(cp->dirty_trigger, c->lock, cp->dirty_max_wait);

        df = cp->max_bytes;
        df = c->stats.dirty_bytes / df;

        log_printf(15, "Dirty thread running.  dirty fraction=%lf dirty bytes=" XOT " inprogress=%d  cached segments=%d\n", df, c->stats.dirty_bytes, cp->flush_in_progress, tbx_list_key_count(c->segments));

        cp->flush_in_progress = 1;
        q = new_opque();

        n = tbx_list_key_count(c->segments);
        tbx_type_malloc(flush_list, segment_t *, n);

        it = tbx_list_iter_search(c->segments, NULL, 0);
        tbx_list_next(&it, (tbx_list_key_t **)&id, (tbx_list_data_t **)&seg);
        i = 0;
        while (seg != NULL) {
            log_printf(15, "Flushing seg=" XIDT " i=%d\n", *id, i);
            tbx_flush_log();
            flush_list[i] = seg;
            s = (cache_segment_t *)seg->priv;
            tbx_atomic_set(s->cache_check_in_progress, 1);  //** Flag it as being checked
            gop = cache_flush_range(seg, s->c->da, 0, -1, s->c->timeout);
            gop_set_myid(gop, i);
            opque_add(q, gop);
            i++;

            tbx_list_next(&it, (tbx_list_key_t **)&id, (tbx_list_data_t **)&seg);
        }
        cache_unlock(c);

        //** Flag the tasks as they complete
        opque_start_execution(q);
        while ((gop = opque_waitany(q)) != NULL) {
            i = gop_get_myid(gop);
            segment_lock(flush_list[i]);
            s = (cache_segment_t *)flush_list[i]->priv;

            log_printf(15, "Flush completed seg=" XIDT " i=%d\n", segment_id(flush_list[i]), i);
            tbx_flush_log();
            tbx_atomic_set(s->cache_check_in_progress, 0);  //** Flag it as being finished
            segment_unlock(flush_list[i]);

            gop_free(gop, OP_DESTROY);
        }
        opque_free(q, OP_DESTROY);

        cache_lock(c);

        cp->flush_in_progress = 0;
        free(flush_list);

        df = cp->max_bytes;
        df = c->stats.dirty_bytes / df;
        log_printf(15, "Dirty thread sleeping.  dirty fraction=%lf dirty bytes=" XOT " inprogress=%d\n", df, c->stats.dirty_bytes, cp->flush_in_progress);
//     apr_thread_cond_timedwait(cp->dirty_trigger, c->lock, cp->dirty_max_wait);
    }

    log_printf(15, "Dirty thread Exiting\n");

    cache_unlock(c);

    return(NULL);

}


//*************************************************************************
// lru_adjust_dirty - Adjusts the dirty ratio and if needed trigger a flush
//*************************************************************************

void lru_adjust_dirty(cache_t *c, ex_off_t tweak)
{
    cache_lru_t *cp = (cache_lru_t *)c->fn.priv;

    cache_lock(c);
    c->stats.dirty_bytes += tweak;
    if (c->stats.dirty_bytes > cp->dirty_bytes_trigger) {
        if (cp->flush_in_progress == 0) {
            cp->flush_in_progress = 1;
            apr_thread_cond_signal(cp->dirty_trigger);
        }
    }
    cache_unlock(c);
}

//*************************************************************************
//  _lru_new_page - Creates the physical page
//*************************************************************************

cache_page_t *_lru_new_page(cache_t *c, segment_t *seg)
{
    cache_lru_t *cp = (cache_lru_t *)c->fn.priv;
    cache_segment_t *s = (cache_segment_t *)seg->priv;
    page_lru_t *lp;
    cache_page_t *p;

    tbx_type_malloc_clear(lp, page_lru_t, 1);
    p = &(lp->page);
    p->curr_data = &(p->data[0]);
    p->current_index = 0;
    tbx_type_malloc_clear(p->curr_data->ptr, char, s->page_size);

    cp->bytes_used += s->page_size;

    p->priv = (void *)lp;
    p->seg = seg;
//  p->offset = -1;
    p->offset = tbx_atomic_dec(lru_dummy);
    tbx_atomic_set(p->bit_fields, C_EMPTY);  //** This way it's not accidentally deleted

    //** Store my position
    tbx_stack_push(cp->stack, p);
    lp->ele = tbx_get_ptr(cp->stack);

    log_printf(15, " seg=" XIDT " page created initial->offset=" XOT " page_size=" XOT " data[0]=%p bytes_used=" XOT " stack_size=%d\n", segment_id(seg), p->offset, s->page_size, p->curr_data->ptr, cp->bytes_used, tbx_stack_size(cp->stack));
    return(p);
}

//*************************************************************************
// _lru_process_waiters - Checks if watiers can be handled
//*************************************************************************

void _lru_process_waiters(cache_t *c)
{
    cache_lru_t *cp = (cache_lru_t *)c->fn.priv;
    lru_page_wait_t *pw = NULL;
    cache_cond_t *cache_cond;
    ex_off_t bytes_free, bytes_needed;

    if (tbx_stack_size(cp->pending_free_tasks) > 0) {  //**Check on pending free tasks 1st
        while ((cache_cond = (cache_cond_t *)tbx_stack_pop(cp->pending_free_tasks)) != NULL) {
            log_printf(15, "waking up pending task cache_cond=%p stack_size left=%d\n", cache_cond, tbx_stack_size(cp->pending_free_tasks));
            apr_thread_cond_signal(cache_cond->cond);    //** Wake up the paused thread
        }
//     return;
    }

    if (tbx_stack_size(cp->waiting_stack) > 0) {  //** Also handle the tasks waiting for flushes to complete
        bytes_free = _lru_max_bytes(c) - cp->bytes_used;

        tbx_stack_move_to_top(cp->waiting_stack);
        pw = tbx_get_ele_data(cp->waiting_stack);
        bytes_needed = pw->bytes_needed;
        while ((bytes_needed <= bytes_free) && (pw != NULL)) {
            bytes_free -= bytes_needed;
            tbx_delete_current(cp->waiting_stack, 1, 0);
            log_printf(15, "waking up waiting stack pw=%p\n", pw);

            apr_thread_cond_signal(pw->cond);    //** Wake up the paused thread

            //** Get the next one if available
            pw = tbx_get_ele_data(cp->waiting_stack);
            bytes_needed = (pw == NULL) ? bytes_free + 1 : pw->bytes_needed;
        }
    }

}

//*************************************************************************
//  lru_pages_release - Releases the page using the LRU algorithm.
//    Returns 0 if the page still exits and 1 if it was removed.
//*************************************************************************

int lru_pages_release(cache_t *c, cache_page_t **page, int n_pages)
{
    cache_lru_t *cp = (cache_lru_t *)c->fn.priv;
    cache_segment_t *s;
    page_lru_t *lp;
    cache_page_t *p;
    int bits, i;

    cache_lock(c);

    for (i=0; i<n_pages; i++) {
        p = page[i];
        bits = tbx_atomic_get(p->bit_fields);
        log_printf(15, "seg=" XIDT " p->offset=" XOT " bits=%d bytes_used=" XOT "\n", segment_id(p->seg), p->offset, bits, cp->bytes_used);
        if ((bits & C_TORELEASE) > 0) {
            log_printf(15, "DESTROYING seg=" XIDT " p->offset=" XOT " bits=%d bytes_used=" XOT "cache_pages=%d\n", segment_id(p->seg), p->offset, bits, cp->bytes_used, tbx_stack_size(cp->stack));
            s = (cache_segment_t *)p->seg->priv;
            lp = (page_lru_t *)p->priv;

            cp->bytes_used -= s->page_size;
            if (lp->ele != NULL) {
                tbx_stack_move_to_ptr(cp->stack, lp->ele);
                tbx_delete_current(cp->stack, 0, 0);
            } else {
                cp->limbo_pages--;
                log_printf(15, "seg=" XIDT " limbo page p->offset=" XOT " limbo=%d\n", segment_id(p->seg), p->offset, cp->limbo_pages);
            }

            if (p->offset > -1) {
                tbx_list_remove(s->pages, &(p->offset), p);  //** Have to do this here cause p->offset is the key var
            }
            if (p->data[0].ptr) free(p->data[0].ptr);
            if (p->data[1].ptr) free(p->data[1].ptr);
            free(lp);
        }
    }

    //** Now check if we can handle some waiters
    _lru_process_waiters(c);

    cache_unlock(c);

    return(0);
}

//*************************************************************************
//  lru_pages_destroy - Destroys the page list.  Since this is called from a
//    forced cache page requests it's possible that another empty page request
//    created the page already.  If so we just need to drop this page cause
//    it wasnn't added to the segment (remove_from_segment=0)
//*************************************************************************

void lru_pages_destroy(cache_t *c, cache_page_t **page, int n_pages, int remove_from_segment)
{
    cache_lru_t *cp = (cache_lru_t *)c->fn.priv;
    cache_segment_t *s;
    page_lru_t *lp;
    cache_page_t *p;
//  cache_cond_t *cache_cond;
    int i;
    int cr, cw, cf, count;
    cache_lock(c);

    log_printf(15, " START cp->bytes_used=" XOT "\n", cp->bytes_used);

    for (i=0; i<n_pages; i++) {
        p = page[i];
        s = (cache_segment_t *)p->seg->priv;

        cr = tbx_atomic_get(p->access_pending[CACHE_READ]);
        cw = tbx_atomic_get(p->access_pending[CACHE_WRITE]);
        cf = tbx_atomic_get(p->access_pending[CACHE_FLUSH]);
        count = cr +cw + cf;

//     cache_cond = (cache_cond_t *)tbx_pch_data(&(p->cond_pch));
//     if (cache_cond == NULL) {  //** No one listening so free normally
        if (count == 0) {  //** No one is listening
            log_printf(15, "lru_pages_destroy i=%d p->offset=" XOT " seg=" XIDT " remove_from_segment=%d limbo=%d\n", i, p->offset, segment_id(p->seg), remove_from_segment, cp->limbo_pages);
            cp->bytes_used -= s->page_size;
            lp = (page_lru_t *)p->priv;

            if (lp->ele != NULL) {
                tbx_stack_move_to_ptr(cp->stack, lp->ele);
                tbx_delete_current(cp->stack, 0, 0);
            }

            if (remove_from_segment == 1) {
                s = (cache_segment_t *)p->seg->priv;
                tbx_list_remove(s->pages, &(p->offset), p);  //** Have to do this here cause p->offset is the key var
            }

            if (p->data[0].ptr) free(p->data[0].ptr);
            if (p->data[1].ptr) free(p->data[1].ptr);
            free(lp);
        } else {  //** Someone is listening so trigger them and also clear the bits so it will be released
            tbx_atomic_set(p->bit_fields, C_TORELEASE);
            log_printf(15, "lru_pages_destroy i=%d p->offset=" XOT " seg=" XIDT " remove_from_segment=%d cr=%d cw=%d cf=%d limbo=%d\n", i, p->offset, segment_id(p->seg), remove_from_segment, cr, cw, cf, cp->limbo_pages);
        }
    }

    log_printf(15, " AFTER LOOP cp->bytes_used=" XOT "\n", cp->bytes_used);

    log_printf(15, " END cp->bytes_used=" XOT "\n", cp->bytes_used);

    cache_unlock(c);
}

//*************************************************************************
//  lru_page_access - Updates the access time for the cache block
//*************************************************************************

int lru_page_access(cache_t *c, cache_page_t *p, int rw_mode, ex_off_t request_len)
{
    cache_lru_t *cp = (cache_lru_t *)c->fn.priv;
    page_lru_t *lp = (page_lru_t *)p->priv;

    if (rw_mode == CACHE_FLUSH) return(0);  //** Nothing to do for a flush

    //** Only update the position if the page is linked.
    //** Otherwise the page is destined to be dropped
    if (lp->ele != NULL) {
        cache_lock(c);
        if (lp->ele != NULL) {  //** Recehck with the lock on
            tbx_stack_move_to_ptr(cp->stack, lp->ele);
            tbx_stack_unlink_current(cp->stack, 1);
            tbx_stack_move_to_top(cp->stack);
            tbx_stack_insert_link_above(cp->stack, lp->ele);
        }
        cache_unlock(c);
    }

    return(0);
}

//*************************************************************************
//  _lru_free_mem - Frees page memory OPPORTUNISTICALLY
//   Returns the pending bytes to free.  Aborts as soon as it encounters
//   a page it has to flush or can't access
//*************************************************************************

int _lru_free_mem(cache_t *c, segment_t *pseg, ex_off_t bytes_to_free)
{
    cache_lru_t *cp = (cache_lru_t *)c->fn.priv;
    cache_segment_t *s;
    cache_page_t *p;
    page_lru_t *lp;
    tbx_stack_ele_t *ele;
    apr_thread_mutex_t *plock;
    ex_off_t total_bytes, pending_bytes;
    int gotlock, count, bits, err;

    total_bytes = 0;
    err = 0;

    log_printf(15, "START seg=" XIDT " bytes_to_free=" XOT " bytes_used=" XOT " stack_size=%d\n", segment_id(pseg), bytes_to_free, cp->bytes_used, tbx_stack_size(cp->stack));

    tbx_stack_move_to_bottom(cp->stack);
    ele = tbx_get_ptr(cp->stack);
    while ((total_bytes < bytes_to_free) && (ele != NULL) && (err == 0)) {
        p = (cache_page_t *)tbx_get_stack_ele_data(ele);
        lp = (page_lru_t *)p->priv;
        plock = p->seg->lock;
        gotlock = apr_thread_mutex_trylock(plock);
        if ((gotlock == APR_SUCCESS) || (p->seg == pseg)) {
            bits = tbx_atomic_get(p->bit_fields);
            if ((bits & C_TORELEASE) == 0) { //** Skip it if already flagged for removal
                count = tbx_atomic_get(p->access_pending[CACHE_READ]) + tbx_atomic_get(p->access_pending[CACHE_WRITE]) + tbx_atomic_get(p->access_pending[CACHE_FLUSH]);
                if (count == 0) { //** No one is using it
                    s = (cache_segment_t *)p->seg->priv;
                    if ((bits & C_ISDIRTY) == 0) {  //** Don't have to flush it
                        total_bytes += s->page_size;
                        log_printf(15, "lru_free_mem: freeing page seg=" XIDT " p->offset=" XOT " bits=%d\n", segment_id(p->seg), p->offset, bits);
                        tbx_list_remove(s->pages, &(p->offset), p);  //** Have to do this here cause p->offset is the key var
                        tbx_delete_current(cp->stack, 1, 0);
                        if (p->data[0].ptr) free(p->data[0].ptr);
                        if (p->data[1].ptr) free(p->data[1].ptr);
                        free(lp);
                    } else {         //** Got to flush the page first
                        err = 1;
                    }
                } else {
                    err = 1;
                }
            }
            if (gotlock == APR_SUCCESS) apr_thread_mutex_unlock(plock);
        } else {
            err = 1;
        }

        if ((total_bytes < bytes_to_free) && (err == 0)) ele = tbx_get_ptr(cp->stack);
    }

    cp->bytes_used -= total_bytes;
    pending_bytes = bytes_to_free - total_bytes;
    log_printf(15, "END seg=" XIDT " bytes_to_free=" XOT " pending_bytes=" XOT " bytes_used=" XOT "\n", segment_id(pseg), bytes_to_free, pending_bytes, cp->bytes_used);

    return(pending_bytes);
}

//*************************************************************************
// lru_attempt_free_mem - Attempts to forcefully Free page memory
//   Returns the total number of bytes freed
//*************************************************************************

ex_off_t _lru_attempt_free_mem(cache_t *c, segment_t *page_seg, ex_off_t bytes_to_free)
{
    cache_lru_t *cp = (cache_lru_t *)c->fn.priv;
    cache_segment_t *s;
    segment_t *pseg;
    cache_page_t *p;
    page_lru_t *lp;
    tbx_stack_ele_t *ele;
    op_generic_t *gop;
    opque_t *q;
    ex_off_t total_bytes, freed_bytes, pending_bytes, *poff;
    ex_off_t *segid;
    ex_off_t min_off, max_off;
    tbx_list_iter_t sit;
    int count, bits, cw, flush_count;
    tbx_list_t *table;
    page_table_t *ptable;
    tbx_pch_t pch, pt_pch;

    log_printf(15, "START seg=" XIDT " bytes_to_free=" XOT " bytes_used=" XOT " stack_size=%d\n", segment_id(page_seg), bytes_to_free, cp->bytes_used, tbx_stack_size(cp->stack));

    freed_bytes = 0;
    pending_bytes = 0;
    total_bytes = 0;

    //** cache_lock(c) is already acquired
    pch = tbx_pch_reserve(cp->free_pending_tables);
    table = *(tbx_list_t **)tbx_pch_data(&pch);

    //** Get the list of pages to free
    tbx_stack_move_to_bottom(cp->stack);
    ele = tbx_stack_unlink_current(cp->stack, 1);
    while ((total_bytes < bytes_to_free) && (ele != NULL)) {
        p = (cache_page_t *)tbx_get_stack_ele_data(ele);
        lp = (page_lru_t *)p->priv;

        bits = tbx_atomic_get(p->bit_fields);
        log_printf(15, "checking page for release seg=" XIDT " p->offset=" XOT " bits=%d\n", segment_id(p->seg), p->offset, bits);
        tbx_flush_log();

        if ((bits & C_TORELEASE) == 0) { //** Skip it if already flagged for removal
            ptable = (page_table_t *)tbx_list_search(table, (tbx_list_key_t *)&(segment_id(p->seg)));
            if (ptable == NULL) {  //** Have to make a new segment entry
                pt_pch = tbx_pch_reserve(cp->free_page_tables);
                ptable = (page_table_t *)tbx_pch_data(&pt_pch);
                ptable->seg = p->seg;
                ptable->id = segment_id(p->seg);
                ptable->pch = pt_pch;
                tbx_list_insert(table, &(ptable->id), ptable);
            }

            cp->limbo_pages++;
            log_printf(15, "UNLINKING seg=" XIDT " p->offset=" XOT " bits=%d limbo=%d\n", segment_id(p->seg), p->offset, bits, cp->limbo_pages);

            tbx_atomic_inc(p->access_pending[CACHE_READ]);  //** Do this so it's not accidentally deleted
            tbx_stack_push(ptable->stack, p);
            s = (cache_segment_t *)p->seg->priv;
            total_bytes += s->page_size;
            free(lp->ele);
            lp->ele = NULL;  //** Mark it as removed from the list so a page_release doesn't free also
        }

        if (total_bytes < bytes_to_free) ele = tbx_stack_unlink_current(cp->stack, 1);
    }


    if (total_bytes == 0) {  //** Nothing to do so exit
        log_printf(15, "Nothing to do so exiting\n");
        tbx_pch_release(cp->free_pending_tables, &pch);
        return(0);
    }

    cache_unlock(c);  //** Don't need the cache lock for the next part

    q = new_opque();
    opque_start_execution(q);

    //** Now cycle through the segments to be freed
    pending_bytes = 0;
    sit = tbx_list_iter_search(table, tbx_list_first_key(table), 0);
    tbx_list_next(&sit, (tbx_list_key_t **)&segid, (tbx_list_data_t **)&ptable);
    while (ptable != NULL) {
        //** Verify the segment is still valid.  If not then just delete everything
        pseg = tbx_list_search(c->segments, segid);
        if (pseg != NULL) {
            segment_lock(ptable->seg);
            min_off = s->total_size;
            max_off = -1;

            s = (cache_segment_t *)ptable->seg->priv;
            while ((p = tbx_stack_pop(ptable->stack)) != NULL) {
                tbx_atomic_dec(p->access_pending[CACHE_READ]); //** Removed my access control from earlier
                flush_count = tbx_atomic_get(p->access_pending[CACHE_FLUSH]);
                cw = tbx_atomic_get(p->access_pending[CACHE_WRITE]);
                count = tbx_atomic_get(p->access_pending[CACHE_READ]) + cw + flush_count;
                bits = tbx_atomic_get(p->bit_fields);
                if (count != 0) { //** Currently in use so wait for it to be released
                    if (cw > 0) {  //** Got writes so need to wait until they complete otherwise the page may not get released
                        bits = bits | C_TORELEASE;  //** Mark it for release
                        tbx_atomic_set(p->bit_fields, bits);
                        _cache_drain_writes(p->seg, p);  //** Drain the write ops
                        bits = tbx_atomic_get(p->bit_fields);  //** Get the bit fields to see if it's dirty
                    }

                    if (flush_count == 0) {  //** Make sure it's not already being flushed
                        if ((bits & C_ISDIRTY) != 0) {  //** Have to flush it don't have to track it cause the flush will do the release
                            if (min_off > p->offset) min_off = p->offset;
                            if (max_off < p->offset) max_off = p->offset;
                        }
                    }
                    bits = bits | C_TORELEASE;

                    log_printf(15, "in use tagging for release seg=" XIDT " p->offset=" XOT " bits=%d\n", segment_id(p->seg), p->offset, bits);
                    tbx_atomic_set(p->bit_fields, bits);

                    pending_bytes += s->page_size;
                } else {  //** Not in use
                    if ((bits & (C_ISDIRTY|C_EMPTY)) == 0) {  //** Don't have to flush it just drop the page
                        cp->limbo_pages--;
                        log_printf(15, "FREEING page seg=" XIDT " p->offset=" XOT " bits=%d limbo=%d\n", segment_id(p->seg), p->offset, bits, cp->limbo_pages);
                        tbx_list_remove(s->pages, &(p->offset), p);  //** Have to do this here cause p->offset is the key var
                        if (p->data[0].ptr) free(p->data[0].ptr);
                        if (p->data[1].ptr) free(p->data[1].ptr);
                        lp = (page_lru_t *)p->priv;
                        free(lp);
                        freed_bytes += s->page_size;
                    } else {         //** Got to flush the page first but don't have to track it cause the flush will do the release
                        if (p->offset > -1) { //** Skip blank pages
                            if (min_off > p->offset) min_off = p->offset;
                            if (max_off < p->offset) max_off = p->offset;
                        }

                        bits = bits | C_TORELEASE;
                        tbx_atomic_set(p->bit_fields, bits);

                        pending_bytes += s->page_size;
                        if (p->offset > -1) {
                            log_printf(15, "FLUSHING page seg=" XIDT " p->offset=" XOT " bits=%d\n", segment_id(p->seg), p->offset, bits);
                        } else {
                            log_printf(15, "RELEASE trigger for empty page seg=" XIDT " p->offset=" XOT " bits=%d\n", segment_id(p->seg), p->offset, bits);
                        }
                    }
                }

                tbx_list_next(&sit, (tbx_list_key_t **)&poff, (tbx_list_data_t **)&p);
            }

            segment_unlock(ptable->seg);

            if (max_off>-1) {
                gop = cache_flush_range(ptable->seg, s->c->da, min_off, max_off + s->page_size - 1, s->c->timeout);
                opque_add(q, gop);
            }
        } else {  //** Segment has been deleted so drop everything cause it's already freeed
            tbx_stack_empty(ptable->stack, 0);
        }

        cache_lock(c);
        tbx_pch_release(cp->free_page_tables, &(ptable->pch));
        cache_unlock(c);

        tbx_list_next(&sit, (tbx_sl_key_t **)&pseg, (tbx_sl_data_t **)&ptable);
    }

    cache_lock(c);
    log_printf(15, "BEFORE waitall seg=" XIDT " bytes_to_free=" XOT " bytes_used=" XOT " freed_bytes=" XOT " pending_bytes=" XOT "\n",
               segment_id(page_seg), bytes_to_free, cp->bytes_used, freed_bytes, pending_bytes);
    cache_unlock(c);


    //** Wait for any tasks to complete
    opque_waitall(q);
    opque_free(q, OP_DESTROY);

    //** Had this when we came in
    cache_lock(c);

    log_printf(15, "AFTER waitall seg=" XIDT " bytes_used=" XOT "\n", segment_id(page_seg), cp->bytes_used);

    cp->bytes_used -= freed_bytes;  //** Update how much I directly freed

    log_printf(15, "AFTER used update seg=" XIDT " bytes_used=" XOT "\n", segment_id(page_seg), cp->bytes_used);

    //** Clean up
    tbx_sl_empty(table);
    tbx_pch_release(cp->free_pending_tables, &pch);

    log_printf(15, "total_bytes marked for removal =" XOT "\n", total_bytes);

    return(total_bytes);
}

//*************************************************************************
// _lru_force_free_mem - Frees page memory
//   Returns the number of bytes freed
//*************************************************************************

ex_off_t _lru_force_free_mem(cache_t *c, segment_t *page_seg, ex_off_t bytes_to_free, int check_waiters)
{
    cache_segment_t *s = (cache_segment_t *)page_seg->priv;
    cache_lru_t *cp = (cache_lru_t *)c->fn.priv;
    ex_off_t freed_bytes, bytes_left;
    int top, finished;
    tbx_pch_t pch;
    cache_cond_t *cache_cond;

    //** I'm holding this coming in but don't need it cause I can touch all segs
    segment_unlock(page_seg);

    top = 0;
    bytes_left = bytes_to_free;
    freed_bytes = _lru_attempt_free_mem(c, page_seg, bytes_left);
    finished = 0;

    while ((freed_bytes < bytes_to_free) && (finished == 0)) {  //** Keep trying to mark space as free until I get enough
        if (top == 0) {
            top = 1;
            pch = tbx_pch_reserve(s->c->cond_coop);
            cache_cond = (cache_cond_t *)tbx_pch_data(&pch);
            cache_cond->count = 0;

            tbx_stack_move_to_bottom(cp->pending_free_tasks);
            tbx_stack_insert_below(cp->pending_free_tasks, cache_cond);  //** Add myself to the bottom
        } else {
            tbx_stack_push(cp->pending_free_tasks, cache_cond);  //** I go on the top
        }

        log_printf(15, "not enough space so waiting cache_cond=%p freed_bytes=" XOT " bytes_to_free=" XOT "\n", cache_cond, freed_bytes, bytes_to_free);
        //** Now wait until it's my turn
        apr_thread_cond_wait(cache_cond->cond, c->lock);

        bytes_left -= freed_bytes;
        freed_bytes = _lru_attempt_free_mem(c, page_seg, bytes_left);
        finished = 1;
    }

    //** Now check if we can handle some waiters
    if (check_waiters == 1) _lru_process_waiters(c);

    cache_unlock(c);  //** Reacquire the lock in the proper order
    segment_lock(page_seg);  //** Reacquire the lock cause I had it coming in
    cache_lock(c);

    if (top == 1) tbx_pch_release(s->c->cond_coop, &pch);

    freed_bytes = bytes_to_free - bytes_left;
//NEW  freed_bytes = bytes_left - freed_bytes;

    return(freed_bytes);
}

//*************************************************************************
// _lru_wait_for_page - Waits for space to free up
//*************************************************************************

void _lru_wait_for_page(cache_t *c, segment_t *seg, int ontop)
{
    cache_lru_t *cp = (cache_lru_t *)c->fn.priv;
    cache_segment_t *s = (cache_segment_t *)seg->priv;
    lru_page_wait_t pw;
    tbx_pch_t pch;
    cache_cond_t *cc;
    ex_off_t bytes_free, bytes_needed, n;
    int check_waiters_first;

    check_waiters_first = (ontop == 0) ? 1 : 0;
    pch = tbx_pch_reserve(c->cond_coop);
    cc = (cache_cond_t *)tbx_pch_data(&pch);
    pw.cond = cc->cond;
    pw.bytes_needed = s->page_size;

    bytes_free = _lru_max_bytes(c) - cp->bytes_used;
    while (s->page_size > bytes_free) {
        //** Attempt to free pages
        bytes_needed = s->page_size - bytes_free;
        n = _lru_force_free_mem(c, seg, bytes_needed, check_waiters_first);

        if (n > 0) { //** Didn't make it so wait
            if (ontop == 0) {
                tbx_stack_move_to_bottom(cp->waiting_stack);
                tbx_stack_insert_below(cp->waiting_stack, &pw);
            } else {
                tbx_stack_push(cp->waiting_stack, &pw);
            }

            segment_unlock(seg);  //** Unlock the segment to prevent deadlocks

            apr_thread_cond_wait(pw.cond, c->lock);  //** Wait for the space to become available

            //** Have to reaquire both locks in the correct order
            cache_unlock(c);
            segment_lock(seg);
            cache_lock(c);

            ontop = 1;  //** 2nd time we are always placed on the top of the stack
            check_waiters_first = 0;  //** And don't check on waiters
        }

        bytes_free = _lru_max_bytes(c) - cp->bytes_used;
    }

    tbx_pch_release(c->cond_coop, &pch);

    return;
}

//*************************************************************************
// lru_create_empty_page - Creates an empty page for use
//*************************************************************************

cache_page_t *lru_create_empty_page(cache_t *c, segment_t *seg, int doblock)
{
    cache_lru_t *cp = (cache_lru_t *)c->fn.priv;
    cache_segment_t *s = (cache_segment_t *)seg->priv;
    ex_off_t max_bytes, bytes_to_free;
    cache_page_t *p = NULL;
    int qend;

    cache_lock(c);

    qend = 0;
    do {
        max_bytes = _lru_max_bytes(c);
        bytes_to_free = s->page_size + cp->bytes_used - max_bytes;
        log_printf(15, "lru_create_empty_page: max_bytes=" XOT " used=" XOT " bytes_to_free=" XOT " doblock=%d\n", max_bytes, cp->bytes_used, bytes_to_free, doblock);
        if (bytes_to_free > 0) {
            bytes_to_free = _lru_free_mem(c, seg, bytes_to_free);
            if ((doblock==1) && (bytes_to_free>0)) _lru_wait_for_page(c, seg, qend);
            qend = 1;
        }
    } while ((doblock==1) && (bytes_to_free>0));

    if (bytes_to_free <= 0) p = _lru_new_page(c, seg);

    cache_unlock(c);

    return(p);
}

//*************************************************************************
//  lru_update - Dummy routine
//*************************************************************************

void lru_update(cache_t *c, segment_t *seg, int mode, ex_off_t lo, ex_off_t hi, void *miss)
{
    return;
}

//*************************************************************************
//  lru_miss_tag - Dummy routine
//*************************************************************************

void lru_miss_tag(cache_t *c, segment_t *seg, int mode, ex_off_t lo, ex_off_t hi, ex_off_t missing_offset, void **miss)
{
    return;
}

//*************************************************************************
//  lru_adding_segment - Dummy routine
//*************************************************************************

void lru_adding_segment(cache_t *c, segment_t *seg)
{
    return;
}

//*************************************************************************
//  lru_removing_segment - Dummy routine
//*************************************************************************

void lru_removing_segment(cache_t *c, segment_t *seg)
{
    return;
}

//*************************************************************************
// lru_cache_destroy - Destroys the cache structure.
//     NOTE: Data is not flushed!
//*************************************************************************

int lru_cache_destroy(cache_t *c)
{
    apr_status_t value;

    cache_lru_t *cp = (cache_lru_t *)c->fn.priv;

    //** Shutdown the dirty thread
    cache_lock(c);
    c->shutdown_request = 1;
    apr_thread_cond_signal(cp->dirty_trigger);
    cache_unlock(c);

    apr_thread_join(&value, cp->dirty_thread);  //** Wait for it to complete

    cache_base_destroy(c);

    tbx_free_stack(cp->stack, 0);
    tbx_free_stack(cp->waiting_stack, 0);
    tbx_free_stack(cp->pending_free_tasks, 0);

    tbx_pc_destroy(cp->free_pending_tables);
    tbx_pc_destroy(cp->free_page_tables);

    free(cp);
    free(c);

    return(0);
}


//*************************************************************************
// lru_cache_create - Creates an empty LRU cache structure
//*************************************************************************

cache_t *lru_cache_create(void *arg, data_attr_t *da, int timeout)
{
    cache_t *cache;
    cache_lru_t *c;

    tbx_type_malloc_clear(cache, cache_t, 1);
    tbx_type_malloc_clear(c, cache_lru_t, 1);
    cache->fn.priv = c;

    cache_base_create(cache, da, timeout);

    cache->shutdown_request = 0;
    c->stack = tbx_stack_new();
    c->waiting_stack = tbx_stack_new();
    c->pending_free_tasks = tbx_stack_new();
    c->max_bytes = 100*1024*1024;
    c->bytes_used = 0;
    c->dirty_fraction = 0.1;
    cache->n_ppages = 0;
    cache->max_fetch_fraction = 0.1;
    cache->max_fetch_size = cache->max_fetch_fraction * c->max_bytes;
    cache->write_temp_overflow_used = 0;
    cache->write_temp_overflow_fraction = 0.01;
    cache->write_temp_overflow_size = cache->write_temp_overflow_fraction * c->max_bytes;

    c->dirty_bytes_trigger = c->dirty_fraction * c->max_bytes;
    c->dirty_max_wait = apr_time_make(1, 0);
    c->flush_in_progress = 0;
    c->limbo_pages = 0;
    c->free_pending_tables = tbx_pc_new("free_pending_tables", 50, sizeof(tbx_list_t *), cache->mpool, free_pending_table_new, free_pending_table_free);
    c->free_page_tables = tbx_pc_new("free_page_tables", 50, sizeof(page_table_t), cache->mpool, free_page_tables_new, free_page_tables_free);

    cache->fn.create_empty_page = lru_create_empty_page;
    cache->fn.adjust_dirty = lru_adjust_dirty;
    cache->fn.destroy_pages = lru_pages_destroy;
    cache->fn.cache_update = lru_update;
    cache->fn.cache_miss_tag = lru_miss_tag;
    cache->fn.s_page_access = lru_page_access;
    cache->fn.s_pages_release = lru_pages_release;
    cache->fn.destroy = lru_cache_destroy;
    cache->fn.adding_segment = lru_adding_segment;
    cache->fn.removing_segment = lru_removing_segment;
    cache->fn.get_handle = cache_base_handle;

    apr_thread_cond_create(&(c->dirty_trigger), cache->mpool);
    tbx_thread_create_assert(&(c->dirty_thread), NULL, lru_dirty_thread, (void *)cache, cache->mpool);

    return(cache);
}


//*************************************************************************
// lru_cache_load -Creates and configures an LRU cache structure
//*************************************************************************

cache_t *lru_cache_load(void *arg, tbx_inip_file_t *fd, char *grp, data_attr_t *da, int timeout)
{
    cache_t *c;
    cache_lru_t *cp;
    int dt;

    if (grp == NULL) grp = "cache-lru";

    //** Create the default structure
    c = lru_cache_create(arg, da, timeout);
    cp = (cache_lru_t *)c->fn.priv;

    cache_lock(c);
    cp->max_bytes = tbx_inip_integer_get(fd, grp, "max_bytes", cp->max_bytes);
    cp->dirty_fraction = tbx_inip_double_get(fd, grp, "dirty_fraction", cp->dirty_fraction);
    cp->dirty_bytes_trigger = cp->dirty_fraction * cp->max_bytes;
    c->default_page_size = tbx_inip_integer_get(fd, grp, "default_page_size", c->default_page_size);
    dt = tbx_inip_integer_get(fd, grp, "dirty_max_wait", apr_time_sec(cp->dirty_max_wait));
    cp->dirty_max_wait = apr_time_make(dt, 0);
    c->max_fetch_fraction = tbx_inip_double_get(fd, grp, "max_fetch_fraction", c->max_fetch_fraction);
    c->max_fetch_size = c->max_fetch_fraction * cp->max_bytes;
    c->write_temp_overflow_fraction = tbx_inip_double_get(fd, grp, "write_temp_overflow_fraction", c->write_temp_overflow_fraction);
    c->write_temp_overflow_size = c->write_temp_overflow_fraction * cp->max_bytes;
    c->n_ppages = tbx_inip_integer_get(fd, grp, "ppages", c->n_ppages);

    log_printf(0, "COP size=" XOT "\n", c->write_temp_overflow_size);

    cache_unlock(c);

    return(c);
}
