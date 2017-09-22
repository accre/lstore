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

#define _log_module_index 161

#include <apr_errno.h>
#include <apr_pools.h>
#include <apr_thread_cond.h>
#include <apr_thread_mutex.h>
#include <apr_time.h>
#include <assert.h>
#include <gop/gop.h>
#include <gop/hp.h>
#include <gop/opque.h>
#include <gop/tp.h>
#include <gop/types.h>
#include <inttypes.h>
#include <limits.h>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <tbx/append_printf.h>
#include <tbx/assert_result.h>
#include <tbx/atomic_counter.h>
#include <tbx/iniparse.h>
#include <tbx/list.h>
#include <tbx/log.h>
#include <tbx/pigeon_coop.h>
#include <tbx/skiplist.h>
#include <tbx/stack.h>
#include <tbx/string_token.h>
#include <tbx/transfer_buffer.h>
#include <tbx/type_malloc.h>
#include <unistd.h>

#include "cache.h"
#include "ds.h"
#include "ex3.h"
#include "ex3/compare.h"
#include "ex3/header.h"
#include "ex3/system.h"
#include "segment/cache.h"
#include "service_manager.h"

#define XOT_MAX (LONG_MAX-2)

const lio_segment_vtable_t lio_cacheseg_vtable;
typedef struct {
    lio_segment_t *seg;
    data_attr_t *da;
    ex_off_t new_size;
    int timeout;
} lio_cache_truncate_op_t;

typedef struct {
    lio_segment_t *seg;
    tbx_tbuf_t *buf;
    data_attr_t *da;
    lio_segment_rw_hints_t *rw_hints;
    ex_off_t   boff;
    ex_tbx_iovec_t *iov;
    ex_tbx_iovec_t iov_single;
    int        rw_mode;
    int        n_iov;
    int skip_ppages;
    int timeout;
int dummy;
} cache_rw_op_t;

typedef struct {
    lio_segment_t *seg;
    ex_off_t lo;
    ex_off_t hi;
    int rw_mode;
    int force_wait;
    lio_page_handle_t *page;
    int *n_pages;
    lio_segment_rw_hints_t *rw_hints;
} cache_advise_op_t;

typedef struct {
    gop_op_generic_t *gop;
    tbx_iovec_t *iov;
    lio_page_handle_t *page;
    ex_tbx_iovec_t ex_iov;
    ex_off_t nbytes;
    tbx_tbuf_t buf;
    int n_iov;
    int myid;
} cache_rw_tbx_iovec_t;

typedef struct {
    lio_segment_t *sseg;
    lio_segment_t *dseg;
    gop_op_generic_t *gop;
} cache_clone_t;

tbx_atomic_int_t _cache_count = 0;
tbx_atomic_int_t _flush_count = 0;

gop_op_status_t cache_rw_func(void *arg, int id);
int _cache_ppages_flush(lio_segment_t *seg, data_attr_t *da);

//*************************************************************
// cache_cond_new - Creates a new shelf of cond variables
//*************************************************************

void *cache_cond_new(void *arg, int size)
{
    lio_cache_cond_t *shelf;
    apr_pool_t **pool_ptr;
    int i;

    i = sizeof(lio_cache_cond_t)*size + sizeof(apr_pool_t *);
    shelf = malloc(i);
    FATAL_UNLESS(shelf != NULL);
    memset(shelf, 0, i);

    pool_ptr = (apr_pool_t **)&(shelf[size]);
    assert_result(apr_pool_create(pool_ptr, NULL), APR_SUCCESS);

    log_printf(15, "cache_cond_new: making new shelf of size %d\n", size);
    for (i=0; i<size; i++) {
        apr_thread_cond_create(&(shelf[i].cond), *pool_ptr);
    }

    return((void *)shelf);
}

//*************************************************************
// cache_cond_free - Destroys a new shelf of cond variables
//*************************************************************

void cache_cond_free(void *arg, int size, void *data)
{
    apr_pool_t **pool_ptr;
    lio_cache_cond_t *shelf = (lio_cache_cond_t *)data;

    log_printf(15, "cache_cond_free: destroying shelf of size %d\n", size);

    pool_ptr = (apr_pool_t **)&(shelf[size]);

    //** All the data is in the memory pool
    apr_pool_destroy(*pool_ptr);

    free(shelf);
    return;
}

//*******************************************************************************
//  cache_new_range - Makes a new cache range object
//*******************************************************************************

lio_cache_range_t *cache_new_range(ex_off_t lo, ex_off_t hi, ex_off_t boff, int iov_index)
{
    lio_cache_range_t *r;

    tbx_type_malloc(r, lio_cache_range_t, 1);

    r->lo = lo;
    r->hi = hi;
    r->boff = boff;
    r->iov_index = iov_index;

    return(r);
}

//*******************************************************************************
//  flush_wait - Waits for pending flushes to complete
//*******************************************************************************

void flush_wait(lio_segment_t *seg, ex_off_t *my_flush)
{
    lio_cache_lio_segment_t *s = (lio_cache_lio_segment_t *)seg->priv;
    int finished;
    ex_off_t *check;

    segment_lock(seg);

    do {
        finished = 1;
        tbx_stack_move_to_bottom(s->flush_stack);
        while ((check = (ex_off_t *)tbx_stack_get_current_data(s->flush_stack)) != NULL) {
            if (check[2] < my_flush[2]) {
                if ((check[0] <= my_flush[0]) && (check[1] >= my_flush[0])) {
                    finished = 0;
                    break;
                } else if ((check[0] > my_flush[0]) && (check[0] <= my_flush[1])) {
                    finished = 0;
                    break;
                }
            }
            tbx_stack_move_up(s->flush_stack);
        }
        if (finished == 0) apr_thread_cond_wait(s->flush_cond, seg->lock);
    } while (finished == 0);

    segment_unlock(seg);

    return;
}

//*******************************************************************************
// full_page_overlap - Returns 1 if the range fully overlaps with the given page
//     and 0 otherwise
//*******************************************************************************

int full_page_overlap(ex_off_t poff, ex_off_t psize, ex_off_t lo, ex_off_t hi)
{
    ex_off_t phi;

    phi = poff + psize - 1;
    if ((lo <= poff) && (phi <= hi)) {
        log_printf(15, "FULL_PAGE prange=(" XOT ", " XOT ") (lo,hi)=(" XOT ", " XOT ")\n", poff, phi, lo, hi);
        return(1);
    }

    log_printf(15, "PARTIAL_PAGE prange=(" XOT ", " XOT ") (lo,hi)=(" XOT ", " XOT ")\n", poff, phi, lo, hi);

    return(0);
}

//*******************************************************************************
//  _cache_drain_writes - Waits for all writes to complete.
//
//  NOTE: Assumes segment is locked and the C_TORELEASE flag is set
//*******************************************************************************

void _cache_drain_writes(lio_segment_t *seg, lio_cache_page_t *p)
{
    lio_cache_lio_segment_t *s = (lio_cache_lio_segment_t *)seg->priv;
    lio_cache_cond_t *cache_cond;

    log_printf(15, "seg=" XIDT " START p->offset=" XOT " cr=%d cw=%d cf=%d bit_fields=%d\n", segment_id(seg),p->offset,
               p->access_pending[CACHE_READ], p->access_pending[CACHE_WRITE], p->access_pending[CACHE_FLUSH], p->bit_fields);

    cache_cond = (lio_cache_cond_t *)tbx_pch_data(&(p->cond_pch));
    if (cache_cond == NULL) {
        p->cond_pch = tbx_pch_reserve(s->c->cond_coop);
        cache_cond = (lio_cache_cond_t *)tbx_pch_data(&(p->cond_pch));
        cache_cond->count = 0;
    }

    cache_cond->count++;
    while ((p->access_pending[CACHE_WRITE] > 0) || ((p->bit_fields & C_EMPTY) > 0)) {
        apr_thread_cond_wait(cache_cond->cond, s->c->lock);
    }

    log_printf(15, "seg=" XIDT " END p->offset=" XOT " cr=%d cw=%d cf=%d bit_fields=%d\n", segment_id(seg),p->offset,
               p->access_pending[CACHE_READ], p->access_pending[CACHE_WRITE], p->access_pending[CACHE_FLUSH], p->bit_fields);

    cache_cond->count--;
    if (cache_cond->count <= 0) tbx_pch_release(s->c->cond_coop, &(p->cond_pch));

}

//*******************************************************************************
//  _cache_wait_for_page - Waits for a page to become accessible.
//
//  NOTE: Assumes segment is locked and the appropriate access_pending is set
//*******************************************************************************

void _cache_wait_for_page(lio_segment_t *seg, int rw_mode, lio_cache_page_t *p)
{
    lio_cache_lio_segment_t *s = (lio_cache_lio_segment_t *)seg->priv;
    lio_cache_cond_t *cache_cond;

    cache_cond = (lio_cache_cond_t *)tbx_pch_data(&(p->cond_pch));
    if (cache_cond == NULL) {
        p->cond_pch = tbx_pch_reserve(s->c->cond_coop);
        cache_cond = (lio_cache_cond_t *)tbx_pch_data(&(p->cond_pch));
        cache_cond->count = 0;
    }

    log_printf(15, "seg=" XIDT " START rw_mode=%d p->offset=" XOT " cr=%d cw=%d cf=%d bit_fields=%d\n", segment_id(seg), rw_mode, p->offset,
               p->access_pending[CACHE_READ], p->access_pending[CACHE_WRITE], p->access_pending[CACHE_FLUSH], p->bit_fields);

    cache_cond->count++;
    if (rw_mode == CACHE_WRITE) {
        while ((p->access_pending[CACHE_FLUSH] > 0) || ((p->bit_fields & C_EMPTY) > 0)) {
            apr_thread_cond_wait(cache_cond->cond, s->c->lock);
        }
    } else {
        while ((p->bit_fields & C_EMPTY) > 0) {
            apr_thread_cond_wait(cache_cond->cond, s->c->lock);
        }
    }

    log_printf(15, "seg=" XIDT " END rw_mode=%d p->offset=" XOT " cr=%d cw=%d cf=%d bit_fields=%d\n", segment_id(seg), rw_mode, p->offset,
               p->access_pending[CACHE_READ], p->access_pending[CACHE_WRITE], p->access_pending[CACHE_FLUSH], p->bit_fields);

    cache_cond->count--;
    if (cache_cond->count <= 0) tbx_pch_release(s->c->cond_coop, &(p->cond_pch));
}


//*******************************************************************************
// s_cache_page_init - Initializes a cache page for use and addes it to the segment page list
//*******************************************************************************

void s_cache_page_init(lio_segment_t *seg, lio_cache_page_t *p, ex_off_t poff)
{
    lio_cache_lio_segment_t *s = (lio_cache_lio_segment_t *)seg->priv;

    log_printf(15, "s_cache_page_init: seg=" XIDT " p->offset=" XOT " start->offset=" XOT "\n", segment_id(seg), poff, p->offset);
    p->seg = seg;
    p->offset = poff;
    p->used_count = 0;;

    p->bit_fields = C_EMPTY;

    tbx_list_insert(s->pages, &(p->offset), p);

    log_printf(15, "seg=" XIDT " init p->offset=" XOT " cr=%d cw=%d cf=%d bit_fields=%d\n", segment_id(seg),p->offset,
               p->access_pending[CACHE_READ], p->access_pending[CACHE_WRITE], p->access_pending[CACHE_FLUSH], p->bit_fields);

}

//*******************************************************************************
//  cache_rw_pages - Reads or Writes pages on the given segment.  Optionally releases the pages
//*******************************************************************************

int cache_rw_pages(lio_segment_t *seg, lio_segment_rw_hints_t *rw_hints, lio_page_handle_t *plist, int pl_size, int rw_mode, int do_release)
{
    lio_cache_lio_segment_t *s = (lio_cache_lio_segment_t *)seg->priv;
    lio_page_handle_t *ph;
    cache_rw_tbx_iovec_t *cio;
    gop_opque_t *q;
    gop_op_generic_t *gop;
    lio_cache_cond_t *cache_cond;
    tbx_iovec_t iovec[pl_size];
    lio_page_handle_t blank_pages[pl_size];
    lio_cache_counters_t cc;
    int error_count, blank_count;
    int myid, n, i, j, pli, contig_start;
    ex_off_t off, last_page, contig_last;

    log_printf(15, "START pl_size=%d\n", pl_size);

    if (pl_size == 0) return(0);

    memset(&cc, 0, sizeof(cc));  //** Reset the counters

    error_count = 0;
    blank_count = 0;
    last_page = -1;

    //** Figure out the contiguous blocks
    q = gop_opque_new();
    myid = -1;
    pli = 0;
    while (pli<pl_size) {
        if (plist[pli].data->ptr != NULL) {
            break;    //** Kick out if not NULL
        }
        log_printf(15, "skipping NULL page p->offset=" XOT "\n", plist[pli].p->offset); //** Skip error pages
        blank_pages[blank_count] = plist[pli];
        blank_count++;
        pli++;
    }
    contig_start = pli;
    if (pli < pl_size) off = plist[pli].p->offset;
    while (pli<pl_size) {
        ph = &(plist[pli]);
        if ((ph->p->offset != off) || (ph->data == NULL)) {  //** Continuity break so bundle up the ops into a single command
            myid++;
            n = pli - contig_start;
            tbx_type_malloc(cio, cache_rw_tbx_iovec_t, 1);
            cio->n_iov = n;
            cio->myid = myid;
            cio->nbytes = s->page_size * n;
            cio->page = &(plist[contig_start]);
            cio->iov = &(iovec[contig_start]);

            log_printf(15, "cache_rw_pages: rw_mode=%d pli=%d contig_start=%d n=%d start_offset=" XOT "\n", rw_mode, pli, contig_start, n, plist[contig_start].p->offset);

            for (i=0; i<n; i++) {
                cio->iov[i].iov_base = plist[contig_start+i].data->ptr;
                cio->iov[i].iov_len = s->page_size;
                log_printf(15, "cache_rw_pages: rw_mode=%d i=%d offset=" XOT "\n", rw_mode, i, plist[contig_start+i].p->offset);
            }

            tbx_tbuf_vec(&(cio->buf), cio->nbytes, cio->n_iov, cio->iov);
            ex_iovec_single(&(cio->ex_iov), plist[contig_start].p->offset, cio->nbytes);
            if (rw_mode == CACHE_READ) {
                cc.read_count++;
                cc.read_bytes += cio->nbytes;
                cio->gop = segment_read(s->child_seg, s->c->da, rw_hints, 1, &(cio->ex_iov), &(cio->buf), 0, s->c->timeout);
            } else {
                cc.write_count++;
                cc.write_bytes += cio->nbytes;
                cio->gop = segment_write(s->child_seg, s->c->da, rw_hints, 1, &(cio->ex_iov), &(cio->buf), 0, s->c->timeout);
            }
            log_printf(2, "rw_mode=%d gid=%d offset=" XOT " len=" XOT "\n", rw_mode, gop_id(cio->gop), plist[contig_start].p->offset, cio->nbytes);
            tbx_log_flush();

            gop_set_myid(cio->gop, myid);
            gop_set_private(cio->gop, (void *)cio);
            gop_opque_add(q, cio->gop);

            //** Skip error pages
            while (pli<pl_size) {
                if (plist[pli].data->ptr != NULL) {
                    break;    //** Kick out if not NULL
                }
                log_printf(15, "skipping NULL page p->offset=" XOT "\n", plist[pli].p->offset); //** Skip error pages
                blank_pages[blank_count] = plist[pli];
                blank_count++;
                pli++;
            }
            contig_start = pli;
            if (pli <pl_size)  {
                ph = &(plist[pli]);
                pli++; //** Start checking with the next page
            }
        } else {
            pli++;
        }

        off = ph->p->offset + s->page_size;
    }


    //** Handle the last chunk if needed

    n = pl_size - contig_start;
    if (n > 0) {
        myid++;
        tbx_type_malloc(cio, cache_rw_tbx_iovec_t, 1);
        cio->n_iov = n;
        cio->myid = myid;
        cio->nbytes = s->page_size * n;
        cio->iov = &(iovec[contig_start]);
        cio->page = &(plist[contig_start]);
        log_printf(15, "cache_rw_pages: end rw_mode=%d pli=%d contig_start=%d n=%d start_offset=" XOT " iov[0]=%p\n", rw_mode, pli, contig_start, n, plist[contig_start].p->offset, cio->iov);

        for (i=0; i<n; i++) {
            cio->iov[i].iov_base = plist[contig_start+i].data->ptr;
            cio->iov[i].iov_len = s->page_size;
            log_printf(15, "cache_rw_pages: end rw_mode=%d i=%d offset=" XOT "\n", rw_mode, i, plist[contig_start+i].p->offset);
        }

        tbx_tbuf_vec(&(cio->buf), cio->nbytes, cio->n_iov, cio->iov);
        ex_iovec_single(&(cio->ex_iov), plist[contig_start].p->offset, cio->nbytes);  //** Last page is the starting point
        if (rw_mode == CACHE_READ) {
            cc.read_count++;
            cc.read_bytes += cio->nbytes;
            cio->gop = segment_read(s->child_seg, s->c->da, rw_hints, 1, &(cio->ex_iov), &(cio->buf), 0, s->c->timeout);
        } else {
            cc.write_count++;
            cc.write_bytes += cio->nbytes;
            cio->gop = segment_write(s->child_seg, s->c->da, rw_hints, 1, &(cio->ex_iov), &(cio->buf), 0, s->c->timeout);
        }
        log_printf(2, "end rw_mode=%d gid=%d offset=" XOT " len=" XOT "\n", rw_mode, gop_id(cio->gop), plist[contig_start].p->offset, cio->nbytes);
        log_printf(15, "end rw_mode=%d myid=%d gid=%d\n", rw_mode, myid, gop_id(cio->gop));
        tbx_log_flush();

        gop_set_myid(cio->gop, myid);
        gop_set_private(cio->gop, (void *)cio);

        gop_opque_add(q, cio->gop);
    }

    //** Dump the blank pages
    if (blank_count > 0) {
        log_printf(15, "Dumping blank pages blank_count=%d\n", blank_count);
        cache_lock(s->c);
        for (i=0; i<blank_count; i++) {
            ph = &(blank_pages[i]);
            if ((ph->p->bit_fields & C_EMPTY) > 0) {
                ph->p->bit_fields = ph->p->bit_fields ^ C_EMPTY;
            }
            cache_cond = (lio_cache_cond_t *)tbx_pch_data(&(ph->p->cond_pch));
            if (cache_cond != NULL) {  //** Someone is listening so wake them up
                apr_thread_cond_broadcast(cache_cond->cond);
            }
        }
        cache_unlock(s->c);

        if (do_release == 1) cache_release_pages(blank_count, blank_pages, rw_mode);
    }

    //** Process tasks as they complete
    n = gop_opque_task_count(q);
    log_printf(15, "cache_rw_pages: total tasks=%d\n", n);
    tbx_log_flush();

    for (i=0; i<n; i++) {
        gop = opque_waitany(q);
        myid= gop_get_myid(gop);
        log_printf(15, "cache_rw_pages: myid=%d gid=%d completed\n", myid, gop_id(gop));
        tbx_log_flush();

        cio = gop_get_private(gop);
        if (gop_completed_successfully(gop) != OP_STATE_SUCCESS) {
            log_printf(15, "cache_rw_pages: myid=%d gid=%d completed with errors!\n", myid, gop_id(gop));
            tbx_log_flush();

            if (rw_mode == CACHE_READ) {
                for (j=0; j<cio->n_iov; j++) {
                    log_printf(15, "error with read nullifying data p->offset=" XOT "\n", cio->page[j].p->offset);
                    free(cio->page[j].data->ptr);  //** Errors are signified by data=NULL;
                    error_count++;
                    cio->page[j].data->ptr = NULL;
                }
            }
        }

        contig_last = cio->page[cio->n_iov-1].p->offset;
        if (last_page < contig_last) last_page = contig_last;  //** Keep track of the largest page


        cache_lock(s->c);
        if ((rw_mode != CACHE_READ) && (last_page > s->child_last_page)) s->child_last_page = last_page;
        for (j=0; j<cio->n_iov; j++) {
            if ((cio->page[j].p->bit_fields & C_EMPTY) > 0) {
                cio->page[j].p->bit_fields ^= C_EMPTY;
            }
            ph = &(cio->page[j]);
            cache_cond = (lio_cache_cond_t *)tbx_pch_data(&(ph->p->cond_pch));
            if (cache_cond != NULL) {  //** Someone is listening so wake them up
                apr_thread_cond_broadcast(cache_cond->cond);
            }
        }
        cache_unlock(s->c);

        if (do_release == 1) cache_release_pages(cio->n_iov, cio->page, rw_mode);

        gop_free(gop, OP_DESTROY);
        free(cio);
    }

    //** And final clean up
    gop_opque_free(q, OP_DESTROY);

    log_printf(15, "END error_count=%d blank_count=%d rw_mode=%d\n", error_count, blank_count, rw_mode);
    return(error_count);
}

//*******************************************************************************
// cache_page_force_get - Waits until the requested page is loaded
//*******************************************************************************

lio_cache_page_t  *cache_page_force_get(lio_segment_t *seg, lio_segment_rw_hints_t *rw_hints, int rw_mode, ex_off_t poff, ex_off_t lo, ex_off_t hi)
{
    lio_cache_lio_segment_t *s = (lio_cache_lio_segment_t *)seg->priv;
    lio_cache_page_t *p, *p2;
    lio_page_handle_t ph;
    ex_off_t off_row;

    off_row = poff / s->page_size;
    off_row = off_row * s->page_size;

    cache_lock(s->c); //** Now get the lock

    p = tbx_list_search(s->pages, (tbx_sl_key_t *)(&off_row));
    log_printf(15, "cache_page_force_get: seg=" XIDT " offset=" XOT " p=%p count=%d\n", segment_id(seg), poff, p, tbx_sl_key_count(s->pages));
    tbx_log_flush();
    if (p == NULL) {  //** New page so may need to load it
        log_printf(15, "seg=" XIDT " offset=" XOT ". Not there so create it. count=%d\n", segment_id(seg), poff, tbx_sl_key_count(s->pages));
        tbx_log_flush();
        p = s->c->fn.create_empty_page(s->c, seg, 1);  //** Get the empty page
        p->seg = seg;

        //** During the page creation we may have released and reacquired the lock letting another thread insert the page
        p2 = tbx_list_search(s->pages, (tbx_sl_key_t *)(&off_row));
        if (p2 == NULL) {    //** Not inserted so I do it
            s_cache_page_init(seg, p, off_row);  //** Add the page
            p->access_pending[rw_mode]++;  //** and mark it for my access mode

            log_printf(15, "seg=" XIDT " rw_mode=%d offset=" XOT ". child_last_page=" XOT "\n", segment_id(seg), rw_mode, p->offset, s->child_last_page);

            if (rw_mode == CACHE_READ) {
                if (s->child_last_page >= p->offset) {  //** Data exists on disk so get it
                    log_printf(15, "seg=" XIDT " CACHE_RW_PAGES rw_mode=%d offset=" XOT ". child_last_page=" XOT "\n", segment_id(seg), rw_mode, p->offset, s->child_last_page);
                    ph.p = p;
                    ph.data = p->curr_data;
                    p->curr_data->usage_count++;
                    cache_unlock(s->c);  //** Now prep it
                    cache_rw_pages(seg, rw_hints, &ph, 1, CACHE_READ, 0);
                    cache_lock(s->c);
                    ph.data->usage_count--;
                } else {   //** No data on disk yet and if not in memory then it's all zero's so flag it as such
                    p->bit_fields = 0;
                }
            } else if (rw_mode == CACHE_WRITE) {
                if (full_page_overlap(p->offset, s->page_size, lo, hi) == 0) { //** Determine if I need to load the page
                    if (s->child_last_page >= p->offset) {
                        log_printf(15, "seg=" XIDT " CACHE_RW_PAGES rw_mode=%d offset=" XOT ". child_last_page=" XOT "\n", segment_id(seg), rw_mode, p->offset, s->child_last_page);
                        ph.p = p;
                        ph.data = p->curr_data;
                        p->curr_data->usage_count++;
                        cache_unlock(s->c);  //** Now prep it
                        cache_rw_pages(seg, rw_hints, &ph, 1, CACHE_READ, 0);
                        cache_lock(s->c);
                        ph.data->usage_count--;
                    }
                }
            }
        } else {   //** Somebody else beat me to it so wait until the data is available
            s->c->fn.destroy_pages(s->c, &p, 1, 0);  //** Destroy my page
            p = p2;
            log_printf(15, "cache_page_force_get: seg=" XIDT " offset=" XOT " rw_mode=%d. Already exists so wait for it to become accessible\n", segment_id(seg), poff, rw_mode);

            p->access_pending[CACHE_READ]++;  //** Use a read to hold the page
            _cache_wait_for_page(seg, rw_mode, p);
            p->access_pending[rw_mode]++;
            p->access_pending[CACHE_READ]--;
        }

        cache_unlock(s->c); //** Now release  the lock

    } else {  //** Page already exists so wait for it to be filled if needed
        log_printf(15, "cache_page_force_get: seg=" XIDT " offset=" XOT " rw_mode=%d. Already exists so wait for it to become free\n", segment_id(seg), poff, rw_mode);
        p->access_pending[CACHE_READ]++;  //** Use a read to hold the page
        _cache_wait_for_page(seg, rw_mode, p);
        p->access_pending[rw_mode]++;
        p->access_pending[CACHE_READ]--;

        cache_unlock(s->c);
    }


    if (p!= NULL) {
        log_printf(15, "PAGE_GET seg=" XIDT " get p->offset=" XOT " cr=%d cw=%d cf=%d bit_fields=%d usage=%d index=%d\n", segment_id(seg), p->offset,
                   p->access_pending[CACHE_READ], p->access_pending[CACHE_WRITE], p->access_pending[CACHE_FLUSH], p->bit_fields, p->curr_data->usage_count, p->current_index);
    }

    return(p);
}


//*******************************************************************************
// cache_advise_fn - Performs the cache_advise function
//*******************************************************************************

gop_op_status_t cache_advise_fn(void *arg, int id)
{
    cache_advise_op_t *ca = (cache_advise_op_t *)arg;
    lio_segment_t *seg = ca->seg;
    lio_cache_lio_segment_t *s = (lio_cache_lio_segment_t *)seg->priv;
    ex_off_t lo_row, hi_row, *poff, coff, poff2, io_size;
    lio_cache_page_t *p, *p2, *np;
    tbx_sl_iter_t it;
    int err, max_pages;

    //** Only works for READ ops
    if (ca->rw_mode != CACHE_READ) {
        *ca->n_pages = 0;
        return(gop_success_status);
    }

    //** Map the range to the page boundaries
    io_size = ca->hi - ca->lo + 1;
    lo_row = ca->lo / s->page_size;
    lo_row = lo_row * s->page_size;
    hi_row = ca->hi / s->page_size;
    hi_row = hi_row * s->page_size;

    //** Figure out if any pages need to be loaded

    log_printf(5, "START seg=" XIDT " lo=" XOT " hi=" XOT "\n", segment_id(seg), ca->lo, ca->hi);

    max_pages = *ca->n_pages;
    *ca->n_pages = 0;

    cache_lock(s->c);
    _cache_ppages_flush(seg, s->c->da); //** Flush any partial pages first  //QWERT

    //** Generate the page list to load
    coff = lo_row;
    it = tbx_sl_iter_search(s->pages, &lo_row, 0);
    for (coff = lo_row; coff <= hi_row; coff += s->page_size) {
        //** Make sure the next page matches coff
        tbx_sl_next(&it, (tbx_sl_key_t **)&poff, (tbx_sl_data_t **)&p);
        err = 0;
        if (p == NULL) {  //** End of range and no pages
            log_printf(15, "seg=" XIDT " coff=" XOT "p->offset=NULL err=1\n", segment_id(seg), coff);
            tbx_log_flush();
            err = 1;
        } else if (p->offset != coff) {  //** Missing page
            err = 1;
            log_printf(15, "seg=" XIDT " coff=" XOT "p->offset=" XOT " err=1\n", segment_id(seg), coff, p->offset);
            tbx_log_flush();
        } else {
            log_printf(15, "seg=" XIDT " coff=" XOT "p->offset=" XOT " err=0\n", segment_id(seg), coff, p->offset);
            tbx_log_flush();
        }

        //** If needed add the empty page
        if (err == 1) {
            log_printf(15, "seg=" XIDT " attempting to create page coff=" XOT "\n", segment_id(seg), coff);
            tbx_log_flush();
            np = s->c->fn.create_empty_page(s->c, seg, 0);  //** Get the empty page
            if ((np == NULL) && (ca->force_wait == 1) && (*ca->n_pages == 0)) {  //**may need to force a page to be created
                np = s->c->fn.create_empty_page(s->c, seg, ca->force_wait);  //** Get the empty page
            }
            log_printf(15, "seg=" XIDT " after attempt to create page coff=" XOT " new_page=%p\n", segment_id(seg), coff, np);
            tbx_log_flush();
            if (np != NULL) { //** This was an opportunistic request so it could be denied
                //** During the page creation we may have released and reacquired the lock letting another thread insert the page
                p2 = tbx_list_search(s->pages, (tbx_sl_key_t *)(&coff));
                if (p2 == NULL) {    //** Not inserted so I do it
                    s_cache_page_init(seg, np, coff);
                    np->access_pending[ca->rw_mode]++;
                    ca->page[*ca->n_pages].p = np;
                    ca->page[*ca->n_pages].data = np->curr_data;
                    np->curr_data->usage_count++;
                    s->c->fn.s_page_access(s->c, np, ca->rw_mode, io_size);  //** Update page access information

                    (*ca->n_pages)++;
                    if (*ca->n_pages >= max_pages) break;
                } else {   //** Somebody else beat me to it so skip it
                    log_printf(5, "seg=" XIDT " duplicate page for coff=" XOT "\n", segment_id(seg), coff);
                    tbx_log_flush();
                    s->c->fn.destroy_pages(s->c, &np, 1, 0);  //** Destroy my page
                }
            } else {
                log_printf(15, "seg=" XIDT " cant find the space for coff=" XOT " so stopping scan\n", segment_id(seg), coff);
                tbx_log_flush();
                break;
            }

            //** Tried to add the page and lost/reacuired the lock so reposition the iterator
            poff2 = coff + s->page_size;
            it = tbx_sl_iter_search(s->pages, &poff2, 0);
        } else {
            break;  //** Hit a valid page.
        }
    }

    cache_unlock(s->c);

    if (*ca->n_pages > 0) {
        //** Got some pages to fetch. Make sure the child segment is big enough.  If not flush
        if (segment_size(s->child_seg) < segment_size(seg)) {
            gop_sync_exec(cache_flush_range_gop(seg, s->c->da, 0, -1, s->c->timeout));
        }
        cache_rw_pages(seg, ca->rw_hints, ca->page, *(ca->n_pages), ca->rw_mode, 0);
    }

    log_printf(5, "END seg=" XIDT " lo=" XOT " hi=" XOT " n_pages=%d\n", segment_id(seg), ca->lo, ca->hi, *ca->n_pages);

    return(gop_success_status);
}

//*******************************************************************************
// cache_advise - Inform the cache system about the immediate R/W intent
//*******************************************************************************

void cache_advise(lio_segment_t *seg, lio_segment_rw_hints_t *rw_hints, int rw_mode, ex_off_t lo, ex_off_t hi, lio_page_handle_t *page, int *n_pages, int force_wait)
{
    lio_cache_lio_segment_t *s = (lio_cache_lio_segment_t *)seg->priv;
    ex_off_t lo_row, hi_row, nbytes, *poff;
    lio_cache_page_t *p;
    tbx_sl_iter_t it;
    cache_advise_op_t ca;
    int err;

    //** Map the rage to the page boundaries
    lo_row = lo / s->page_size;
    nbytes = lo_row;
    lo_row = lo_row * s->page_size;
    hi_row = hi / s->page_size;
    nbytes = hi_row - nbytes + 1;
    hi_row = hi_row * s->page_size;
    nbytes = nbytes * s->page_size;

    ex_off_t len = hi - lo + 1;
    log_printf(15, "START seg=" XIDT " lo=" XOT " hi=" XOT " lo_row=" XOT " hi_row=" XOT " nbytes=" XOT " hi-lo-1=" XOT "\n", segment_id(seg), lo, hi, lo_row, hi_row, nbytes, len);

    //** Figure out if any pages need to be loaded
    cache_lock(s->c);
    it = tbx_sl_iter_search(s->pages, &lo_row, 0);
    err = tbx_sl_next(&it, (tbx_sl_key_t **)&poff, (tbx_sl_data_t **)&p);
    while ((p != NULL) && (err == 0)) {
        log_printf(15, "CHECKING seg=" XIDT " p->offset=" XOT " nleft=" XOT "\n", segment_id(seg), p->offset, nbytes);
        if (p->offset <= hi_row) {
            nbytes -= s->page_size;
            log_printf(15, "IN loop seg=" XIDT " p->offset=" XOT " nleft=" XOT "\n", segment_id(seg), p->offset, nbytes);

            err = tbx_sl_next(&it, (tbx_sl_key_t **)&poff, (tbx_sl_data_t **)&p);
        } else {
            err = 1;
        }
    }
    cache_unlock(s->c);

    log_printf(15, "AFTER loop seg=" XIDT " nleft=" XOT "\n", segment_id(seg), nbytes);

    //** If none just return.  Otherwise trigger page fetches (and/or flushes)
    if (nbytes > 0) {
        ca.seg = seg;
        ca.lo = lo;
        ca.hi = hi;
        ca.force_wait = force_wait;
        ca.rw_mode = rw_mode;
        ca.page = page;
        ca.n_pages = n_pages;
        ca.rw_hints = rw_hints;
        cache_advise_fn((void *)&ca, tbx_atomic_thread_id);
    } else {
        *n_pages = 0;
    }
}

//*******************************************************************************
//  cache_page_drop - Permanately removes pages from cache within the given range
//     Pages are not flushed before removal!  This is mainly used for a truncate
//     or semenget close operation
//*******************************************************************************

int cache_page_drop(lio_segment_t *seg, ex_off_t lo, ex_off_t hi)
{
    lio_cache_lio_segment_t *s = (lio_cache_lio_segment_t *)seg->priv;
    ex_off_t lo_row, hi_row, *poff, coff;
//  ex_off_t my_flush[3];
    tbx_sl_iter_t it;
    lio_cache_page_t *p;
    lio_cache_page_t *page[CACHE_MAX_PAGES_RETURNED];
    int do_again, count, n;

    //** Map the rage to the page boundaries
    lo_row = lo / s->page_size;
    lo_row = lo_row * s->page_size;
    hi_row = hi / s->page_size;
    hi_row = hi_row * s->page_size;

    //** Need to tweak the lo_row to account for a lo being inside the page
    if (lo != lo_row) lo_row++;
    if (lo_row > hi_row) {
        log_printf(5, "seg=" XIDT " Nothing to do exiting.... lo=" XOT " hi=" XOT " lo_row=" XOT " hi_row=" XOT "\n", segment_id(seg), lo, hi, lo_row, hi_row);
        return(0);
    }
    log_printf(5, "START seg=" XIDT " lo=" XOT " hi=" XOT "\n", segment_id(seg), lo, hi);

    do {
        do_again = 0;
        n = 0;

        cache_lock(s->c);
        it = tbx_sl_iter_search(s->pages, &lo_row, 0);
        tbx_sl_next(&it, (tbx_sl_key_t **)&poff, (tbx_sl_data_t **)&p);
        coff = (p == NULL) ? hi+1 : p->offset;
        log_printf(15, "seg=" XIDT " loop start coff=" XOT "\n", segment_id(seg), coff);

        while (coff < hi) {
            count = p->access_pending[CACHE_READ] + p->access_pending[CACHE_WRITE] + p->access_pending[CACHE_FLUSH];

            log_printf(15, "PAGE_GET seg=" XIDT " get p->offset=" XOT " cr=%d cw=%d cf=%d bit_fields=%d usage=%d index=%d\n", segment_id(seg), p->offset,
                       p->access_pending[CACHE_READ], p->access_pending[CACHE_WRITE], p->access_pending[CACHE_FLUSH], p->bit_fields, p->curr_data->usage_count, p->current_index);

            if (count > 0) {
                do_again = 1;
            } else {
                page[n] = p;
                log_printf(15, "seg=" XIDT " adding p[%d]->offset=" XOT " n=%d\n", segment_id(seg), n, page[n]->offset, n);
                n++;
                if (n == CACHE_MAX_PAGES_RETURNED) {
                    log_printf(15, "1. seg=" XIDT " p[0]->offset=" XOT " n=%d\n", segment_id(seg), page[0]->offset, n);
                    s->c->fn.destroy_pages(s->c, page, n, 1);
                    it = tbx_sl_iter_search(s->pages, &lo_row, 0);
                    n=0;
                }
            }

            tbx_sl_next(&it, (tbx_sl_key_t **)&poff, (tbx_sl_data_t **)&p);
            coff = (p == NULL) ? hi+1 : p->offset;
            log_printf(15, "seg=" XIDT " loop bottom coff=" XOT " p=%p\n", segment_id(seg), coff, p);
        }

        log_printf(15, "outer loop seg=" XIDT " n=%d\n", segment_id(seg), n);
        if (n>0) {
            s->c->fn.destroy_pages(s->c, page, n, 1);
        }
        cache_unlock(s->c);

        if (do_again != 0) {
            usleep(10000);  //** Do a simple sleep
        }
    } while (do_again != 0);

    log_printf(5, "END seg=" XIDT " lo=" XOT " hi=" XOT "\n", segment_id(seg), lo, hi);

    return(0);
}


//*******************************************************************************
//  lio_segment_cache_pages_drop - Permanately removes pages from cache within the given range
//     Pages are not flushed before removal!  This is mainly used for a truncate
//     or semenget close operation
//
//  NOTE:  This is designed to be called by other apps whereas the "cache_drop_page"
//     rotuine is deisgned to be used by segment_cache routines only
//*******************************************************************************

int lio_segment_cache_pages_drop(lio_segment_t *seg, ex_off_t lo, ex_off_t hi)
{
    if (strcmp(seg->header.type, SEGMENT_TYPE_CACHE) != 0) return(0);

    return(cache_page_drop(seg, lo, hi));
}

//*******************************************************************************
//  cache_dirty_pages_get - Retrieves dirty pages from cache over the given range
//*******************************************************************************

int cache_dirty_pages_get(lio_segment_t *seg, int mode, ex_off_t lo, ex_off_t hi, ex_off_t *hi_got, lio_page_handle_t *page, int *n_pages)
{
    lio_cache_lio_segment_t *s = (lio_cache_lio_segment_t *)seg->priv;
    ex_off_t lo_row, *poff, n, old_hi;
    tbx_sl_iter_t it;
    lio_cache_page_t *p;
    int err, skip_mode, can_get;
    lio_cache_cond_t *cache_cond;

    //** Map the rage to the page boundaries
    lo_row = lo / s->page_size;
    lo_row = lo_row * s->page_size;

    log_printf(15, "START: seg=" XIDT " mode=%d lo=" XOT " hi=" XOT " lo_row=" XOT "\n", segment_id(seg), mode, lo, hi, lo_row);

    cache_lock(s->c);

    //** Get the 1st point and figure out the if we are skipping or getting pages
    //** If I can acquire a lock on the 1st block we retreive pages otherwise
    //** we are in skipping mode
    skip_mode = 0;
    it = tbx_sl_iter_search(s->pages, &lo_row, 0);
    err = tbx_sl_next(&it, (tbx_sl_key_t **)&poff, (tbx_sl_data_t **)&p);
    if (p != NULL) {
        if (p->offset > hi) err = 1;
        log_printf(15, "seg=" XIDT " p->offset=" XOT " bits=%d cf=%d\n", segment_id(seg), p->offset, p->bit_fields, p->access_pending[CACHE_FLUSH]);
        while (((p->bit_fields & C_ISDIRTY) == 0) || (p->access_pending[CACHE_FLUSH] > 0)) {
            tbx_sl_next(&it, (tbx_sl_key_t **)&poff, (tbx_sl_data_t **)&p);
            if (p != NULL) {
                log_printf(15, "seg=" XIDT " checking p->offset=" XOT " bits=%d cf=%d\n", segment_id(seg), p->offset, p->bit_fields, p->access_pending[CACHE_FLUSH]);
                if (p->offset > hi) {
                    p = NULL;
                    break;
                }
            } else {
                break;
            }
        }

        log_printf(15, "seg=" XIDT " after initial loop p=%p err=%d\n", segment_id(seg), p, err);

        if (p != NULL) {
            log_printf(15, "seg=" XIDT " checking mode=%d p->offset=" XOT " cw=%d bits=%d\n", segment_id(seg), mode, p->offset, p->access_pending[CACHE_WRITE], p->bit_fields);

            if ((mode == CACHE_DOBLOCK) && (p->access_pending[CACHE_WRITE] > 0)) {  //** Wait until I can acquire a lock
                cache_cond = (lio_cache_cond_t *)tbx_pch_data(&(p->cond_pch));
                if (cache_cond == NULL) {
                    p->cond_pch = tbx_pch_reserve(s->c->cond_coop);
                    cache_cond = (lio_cache_cond_t *)tbx_pch_data(&(p->cond_pch));
                    cache_cond->count = 0;
                }
                p->access_pending[CACHE_FLUSH]++;
                cache_cond->count++;
                while ((p->access_pending[CACHE_WRITE] > 0) || ((p->bit_fields & C_EMPTY) > 0)) {
                    apr_thread_cond_wait(cache_cond->cond, s->c->lock);
                }
                p->access_pending[CACHE_FLUSH]--;
                cache_cond->count--;
                if (cache_cond->count <= 0) tbx_pch_release(s->c->cond_coop, &(p->cond_pch));

                //** Need to reset iterator due to potential changes while waiting
                it = tbx_sl_iter_search(s->pages, &(p->offset), 0);
                tbx_sl_next(&it, (tbx_sl_key_t **)&poff, (tbx_sl_data_t **)&p);
            } else {
                skip_mode = (p->access_pending[CACHE_WRITE] == 0) ? 0 : 1;
            }
        }
    }

    if (p != NULL) {
        log_printf(15, "seg=" XIDT " mode=%d lo=" XOT " hi=" XOT " skip_mode=%d cw=%d\n", segment_id(seg), mode, lo, hi, skip_mode, p->access_pending[CACHE_WRITE]);
    } else {
        log_printf(15, "seg=" XIDT " mode=%d lo=" XOT " hi=" XOT " skip_mode=%d p=NULL\n", segment_id(seg), mode, lo, hi, skip_mode);
    }

    *hi_got = lo;
    n = 0;
    err = 0;
    while ((err == 0) && (p != NULL)) {
        can_get = (p->access_pending[CACHE_WRITE] == 0) ? 1 : 0;

        err = 0;
        if (skip_mode == 0) {
            if (can_get == 0) err = 1;
        } else {  //** Skipping mode so looking for a block I *could* get
            if (can_get == 1) err = 1;
        }

        log_printf(15, "1. seg=" XIDT " mode=%d lo=" XOT " hi=" XOT " hi_got=" XOT " skip_mode=%d err=%d p->offset=" XOT " can_get=%d cw=%d\n", segment_id(seg), mode, lo, hi, *hi_got, skip_mode, err, p->offset, can_get, p->access_pending[CACHE_WRITE]);

        if (err == 0) {
            old_hi = *hi_got;
            *hi_got = p->offset + s->page_size - 1;
            if (p->offset > hi) {
                err = 1;
                *hi_got = old_hi;
            }

            log_printf(15, "2. seg=" XIDT " mode=%d lo=" XOT " hi=" XOT " hi_got= " XOT " skip_mode=%d err=%d p->offset=" XOT "\n", segment_id(seg), mode, lo, hi, *hi_got, skip_mode, err, p->offset);

            if ((n < *n_pages) && (p->offset < *hi_got)) {
                if (skip_mode == 0) {
                    s->c->fn.s_page_access(s->c, p, CACHE_FLUSH, 0);  //** Update page access information
                    p->access_pending[CACHE_FLUSH]++;
                    log_printf(15, "PAGE_GET seg=" XIDT " p->offset=" XOT " usage=%d index=%d\n", segment_id(seg), p->offset, p->curr_data->usage_count, p->current_index);

                    page[n].p = p;
                    page[n].data = p->curr_data;
                    p->curr_data->usage_count++;
                    n++;
                    if (n >= *n_pages) err = 1;
                }

                tbx_sl_next(&it, (tbx_sl_key_t **)&poff, (tbx_sl_data_t **)&p);
                if ((p != NULL) && (err == 0)) {
                    old_hi = p->offset;
                    log_printf(15, "2a. seg=" XIDT " p->offset=" XOT " old_hi=" XOT " bits=%d fcount=%d\n", segment_id(seg), p->offset, old_hi, p->bit_fields, p->access_pending[CACHE_FLUSH]);

                    while ((((p->bit_fields & C_ISDIRTY) == 0) || (p->access_pending[CACHE_FLUSH] > 0)) && (err == 0)) {
                        err = tbx_sl_next(&it, (tbx_sl_key_t **)&poff, (tbx_sl_data_t **)&p);
                        if (p != NULL) {
                            old_hi = p->offset;
                            log_printf(15, "3. seg=" XIDT " lo=" XOT " hi=" XOT " err=%d p->offset=" XOT " bits=%d cf=%d err=%d\n", segment_id(seg), lo, hi, err, p->offset, p->bit_fields, p->access_pending[CACHE_FLUSH], err);
                            if (p->offset > hi) {
                                err = 1;
                            }
                        } else {
                            err = 1;
                            old_hi = old_hi + s->page_size;
                            break;  //** Kick out
                        }
                    }

                    *hi_got = old_hi - 1;
                }
            } else {
                err = 1;
            }
        }
    }

    cache_unlock(s->c);

    *n_pages = n;  //** Store the number of pages found

    if (n == 0) *hi_got = hi;

    if (*hi_got > hi) *hi_got = hi;

    log_printf(15, "END: seg=" XIDT " mode=%d lo=" XOT " hi=" XOT " hi_got=" XOT " skip_mode=%d n=%" PRId64 "\n", segment_id(seg), mode, lo, hi, *hi_got, skip_mode, n);

    return(skip_mode);
}

//*******************************************************************************
// _cache_add_page_to_list - Adds a page to the R/W list for processing
//*******************************************************************************

void _cache_add_page_to_list(lio_cache_t *c, lio_cache_page_t *p, lio_page_handle_t *ph, tbx_iovec_t *iov, int mode, int io_size, int page_size)
{
    p->access_pending[mode]++;
    p->used_count++;
    p->curr_data->usage_count++;
    c->fn.s_page_access(c, p, mode, io_size);  //** Update page access information

    //** Add the page
    ph->p = p;
    ph->data = p->curr_data;
    iov->iov_base = p->curr_data->ptr;
    iov->iov_len = page_size;
}

//*******************************************************************************
//  cache_read_pages_get - Retrieves pages from cache for READING over the given range
//*******************************************************************************

int cache_read_pages_get(lio_segment_t *seg, lio_segment_rw_hints_t *rw_hints, int mode, ex_off_t lo, ex_off_t hi, ex_off_t *hi_got, lio_page_handle_t *page, tbx_iovec_t *iov, int *n_pages, tbx_tbuf_t *buf, ex_off_t bpos_start, void **cache_missed, ex_off_t master_size)
{
    lio_cache_lio_segment_t *s = (lio_cache_lio_segment_t *)seg->priv;
    ex_off_t lo_row, hi_row, *poff, n, old_hi;
    tbx_sl_iter_t it;
    lio_cache_page_t *p;
    int err, i, skip_mode, can_get, max_pages;

    //** Map the rage to the page boundaries
    lo_row = lo / s->page_size;
    lo_row = lo_row * s->page_size;
    hi_row = hi / s->page_size;
    hi_row = hi_row * s->page_size;

    max_pages = *n_pages;
    *n_pages = 0;
    log_printf(15, "START seg=" XIDT " mode=%d lo=" XOT " hi=" XOT " lo_row=" XOT " hi_row=" XOT "\n", segment_id(seg), mode, lo, hi, lo_row, hi_row);
    cache_lock(s->c);

    //** Get the 1st point and figure out the if we are skipping or getting pages
    //** If I can acquire a lock on the 1st block we retreive pages otherwise
    //** we are in skipping mode
    skip_mode = 0;
    it = tbx_sl_iter_search(s->pages, &lo_row, 0);
    err = tbx_sl_next(&it, (tbx_sl_key_t **)&poff, (tbx_sl_data_t **)&p);

    if (p != NULL) {
        log_printf(15, "seg=" XIDT " mode=%d lo=" XOT " hi=" XOT " p->offset=" XOT "\n", segment_id(seg), mode, lo, hi, p->offset);

        if (*poff != lo_row) {  //** Should find an exact match otherwise it's a hole
            s->c->fn.cache_miss_tag(s->c, seg, CACHE_READ, lo_row, hi_row, lo_row, cache_missed);
            if ((*poff <= hi_row) && (mode != CACHE_DOBLOCK)) {
                *hi_got = *poff - 1;
                *n_pages = 0;
                cache_unlock(s->c);
                return(1);
            } else {
                p = NULL;
            }
        } else {
            log_printf(15, "seg=" XIDT " mode=%d lo=" XOT " hi=" XOT " p->offset=" XOT " cf=%d bits=%d\n", segment_id(seg), mode, lo, hi, p->offset, p->access_pending[CACHE_FLUSH], p->bit_fields);

            if ((p->bit_fields & (C_EMPTY|C_TORELEASE)) > 0) {  //** Always skip if empty or being released
                skip_mode = 1;
            }

            if ((mode == CACHE_DOBLOCK) && (skip_mode == 1)) { //** Got to wait until I can acquire a lock
                p->access_pending[CACHE_READ]++;
                _cache_wait_for_page(seg, CACHE_READ, p);
                p->access_pending[CACHE_READ]--;
                skip_mode = 0;

                //** Need to reset iterator due to potential changes while waiting
                it = tbx_sl_iter_search(s->pages, &(p->offset), 0);
                err = tbx_sl_next(&it, (tbx_sl_key_t **)&poff, (tbx_sl_data_t **)&p);
            }
        }
    }

    log_printf(15, "seg=" XIDT " mode=%d lo=" XOT " hi=" XOT " skip_mode=%d\n", segment_id(seg), mode, lo, hi, skip_mode);

    if (p == NULL) err = 2;  //** Nothing valid so trigger it to be loaded

    *hi_got = lo;
    n = 0;
    while ((err == 0) && (p != NULL)) {
        can_get = 1;
        if ((p->bit_fields & (C_EMPTY|C_TORELEASE)) > 0) {  //** If empty can't access it
            can_get = 0;
        }

        err = 0;
        if (skip_mode == 0) {
            if (can_get == 0) err = 1;
        } else {  //** Skipping mode so looking for a block I *could* get
            if (can_get == 1) err = 1;
        }

        if (err == 0) {
            old_hi = *hi_got;
            *hi_got = p->offset + s->page_size - 1;
            if (p->offset > hi) {
                err = 1;
                *hi_got = old_hi;
            }

            if (err == 0) {
                if (skip_mode == 0) {
                    _cache_add_page_to_list(s->c, p, &page[n], &iov[n], CACHE_READ, master_size, s->page_size);
                    n++;
                }

                err = tbx_sl_next(&it, (tbx_sl_key_t **)&poff, (tbx_sl_data_t **)&p);

                if (p != NULL) {
                    if (*hi_got != (p->offset - 1)) {  //** Check for a hole
                        err = 2;
                        s->c->fn.cache_miss_tag(s->c, seg, CACHE_READ, lo_row, hi_row, *hi_got + 1, cache_missed);
                    }
                    log_printf(15, "seg=" XIDT " checking next page->offset=" XOT " hi_got=" XOT " hole_check=%d\n", segment_id(seg), p->offset, *hi_got, err);
                } else {
                    log_printf(15, "seg=" XIDT " No more pages\n", segment_id(seg));
                    err = 1;
                }
            } else {
                err = 1;
            }
        }
    }

    *n_pages = n;

    cache_unlock(s->c);

    if ((n == 0) && (mode == CACHE_DOBLOCK)) { //** Force the first page to be loaded
        *n_pages = max_pages;
        log_printf(15, "seg=" XIDT " calling cache_advise lo=" XOT " hi=" XOT "\n", segment_id(seg), lo_row, hi_row);
        cache_lock(s->c);
        s->c->fn.cache_miss_tag(s->c, seg, CACHE_READ, lo_row, hi_row, lo_row, cache_missed);
        cache_unlock(s->c);

        cache_advise(seg, rw_hints, CACHE_READ, lo_row, hi_row, page, n_pages, 0);
        log_printf(15, "seg=" XIDT " cache_advise lo=" XOT " hi=" XOT " n_pages=%d\n", segment_id(seg), lo_row, hi_row, *n_pages);
        if (*n_pages > 0) {
            for (i=0; i < *n_pages; i++) {
                iov[i].iov_base = page[i].data->ptr;
                iov[i].iov_len = s->page_size;
            }
            *hi_got = page[*n_pages-1].p->offset + s->page_size - 1;
        } else {
            p = cache_page_force_get(seg, rw_hints, CACHE_READ, lo_row, lo, hi);  //** This routine does it's own seg locking
            if (p != NULL) {
                *n_pages = 1;
                log_printf(15, "PAGE_GET seg=" XIDT " forcing page load lo_row=" XOT "\n", segment_id(seg), lo_row);
                cache_lock(s->c);
                page[0].p = p;
                page[0].data = p->curr_data;
                p->curr_data->usage_count++;
                cache_unlock(s->c);
                iov[0].iov_base = page[0].data->ptr;
                iov[0].iov_len = s->page_size;
                *hi_got = lo_row + s->page_size - 1;
                skip_mode = 0;
            }
        }
    }

    if ((*n_pages == 0) && (n == 0)) skip_mode = 1;
    log_printf(15, "END seg=" XIDT " mode=%d lo=" XOT " hi=" XOT " hi_got=" XOT " skip_mode=%d n_pages=%d\n", segment_id(seg), mode, lo, hi, *hi_got, skip_mode, *n_pages);
    tbx_log_flush();

    return(skip_mode);
}

//*******************************************************************************
//  cache_write_pages_get - Retrieves pages from cache over the given range for WRITING
//*******************************************************************************

int cache_write_pages_get(lio_segment_t *seg, lio_segment_rw_hints_t *rw_hints, int mode, ex_off_t lo, ex_off_t hi, ex_off_t *hi_got, lio_page_handle_t *page, tbx_iovec_t *iov, int *n_pages, tbx_tbuf_t *buf, ex_off_t bpos_start, void **cache_missed, ex_off_t master_size)
{
    lio_cache_lio_segment_t *s = (lio_cache_lio_segment_t *)seg->priv;
    ex_off_t lo_row, hi_row, *poff, old_hi, coff, pstart, page_off;
    tbx_sl_iter_t it;
    lio_page_handle_t pload[2];
    lio_cache_page_t *p, *np;
    int pload_index[2], i;
    int err, skip_mode, can_get, pload_count, max_pages;

    int flush_skip = 0;

    max_pages = *n_pages;
    *n_pages = 0;

    //** Map the rage to the page boundaries
    lo_row = lo / s->page_size;
    lo_row = lo_row * s->page_size;
    hi_row = hi / s->page_size;
    hi_row = hi_row * s->page_size;

    *hi_got = lo;

    log_printf(15, "START seg=" XIDT " mode=%d lo=" XOT " hi=" XOT " lo_row=" XOT " hi_row=" XOT "\n", segment_id(seg), mode, lo, hi, lo_row, hi_row);
    cache_lock(s->c);

    //** Get the 1st point and figure out the if we are skipping or getting pages
    //** If I can acquire a lock on the 1st block we retreive pages otherwise
    //** we are in skipping mode
    skip_mode = 0;
    it = tbx_sl_iter_search(s->pages, &lo_row, 0);
    tbx_sl_next(&it, (tbx_sl_key_t **)&poff, (tbx_sl_data_t **)&p);

    pload_count = 0;
    page_off = -1;

    pstart = hi;
    err = 0;
    if (p == NULL) {
        err = 1;
    } else if (*poff != lo_row) { //** Should find an exact match otherwise it's a hole
        log_printf(15, "seg=" XIDT " initial page p->offset=" XOT "\n", segment_id(seg), *poff);
        tbx_log_flush();

        pstart = *poff;
        page_off = *poff;
        err = 1;
    }


    if (err == 1) { //** Missing the starting point so see if we can make some blank pages
        err = 0;
        if (pstart > hi) pstart = hi;
        coff = lo_row;
        if (p) p->access_pending[CACHE_READ]++;  //** Preserve the page from deletion

        np = s->c->fn.create_empty_page(s->c, seg, 0);  //** Get the empty page  if possible
        while ((np != NULL) && (coff < pstart) && (*n_pages < max_pages)) {
            s_cache_page_init(seg, np, coff);
            if (full_page_overlap(coff, s->page_size, lo, hi) == 0) {
                if (s->child_last_page >= coff) {  //** Only load the page if not a write beyond the current EOF
                    log_printf(15, "seg=" XIDT " adding page for reading p->offset=" XOT " current child_last_page=" XOT "\n", segment_id(seg), np->offset, s->child_last_page);
                    pload[pload_count].p = np;
                    pload[pload_count].data = np->curr_data;
                    pload_index[pload_count] = *n_pages;
                    pload_count++;
                }
            }

            _cache_add_page_to_list(s->c, np, &page[*n_pages], &iov[*n_pages], CACHE_WRITE, master_size, s->page_size);
            (*n_pages)++;

            *hi_got = coff + s->page_size - 1;

            log_printf(15, "seg=" XIDT " adding page[%d]->offset=" XOT "\n", segment_id(seg), *n_pages-1, np->offset);
            log_printf(15, "PAGE_GET seg=" XIDT " get np->offset=" XOT " n=%d cr=%d cw=%d cf=%d bit_fields=%d np=%p usage=%d index=%d\n", segment_id(seg), np->offset, *n_pages-1,
                       np->access_pending[CACHE_READ], np->access_pending[CACHE_WRITE], np->access_pending[CACHE_FLUSH], np->bit_fields, np, np->curr_data->usage_count, np->current_index);

            coff += s->page_size;
            if ((coff < pstart) && (*n_pages < max_pages)) np = s->c->fn.create_empty_page(s->c, seg, 0);  //** Get the next empty page  if possible

            log_printf(15, " pstart=" XOT " coff=" XOT "\n", pstart, coff);
        }

        if (p) p->access_pending[CACHE_READ]--;  //** Release it

        if (coff < pstart) { //** Didn't make it up to the 1st loaded page
            err = 1;
            s->c->fn.cache_miss_tag(s->c, seg, CACHE_WRITE, lo_row, hi_row, coff, cache_missed);
        }
    } else {  //** The 1st page exists so see if I can get it
        log_printf(15, "seg=" XIDT "mode=%d lo=" XOT " hi=" XOT " p->offset=" XOT " cf=%d bits=%d\n", segment_id(seg), mode, lo, hi, p->offset, p->access_pending[CACHE_FLUSH], p->bit_fields);

        if (((p->bit_fields & (C_EMPTY|C_TORELEASE)) > 0) || (p->access_pending[CACHE_READ] > 0)) {  //** Always skip if empty or it's being read
            skip_mode = 1;
        } else {
            if (p->access_pending[CACHE_FLUSH] > 0) {  //Got a flush op in progress so see if we can do a copy-on-write
                skip_mode = 1;
                flush_skip = 1;
                if (p->access_pending[CACHE_WRITE] == 0) {
                    if ((s->c->write_temp_overflow_used+s->page_size) < s->c->write_temp_overflow_size) {
                        i = (p->current_index+1) % 2;
                        if (p->data[i].ptr == NULL) {  //** We can use the COW space
                            skip_mode = 0;
                            flush_skip = 0;
                        } else {
                            flush_skip = 2;
                        }
                    }
                }
            }

            if ((mode == CACHE_DOBLOCK) && (skip_mode == 1)) { //** Got to wait until I can acquire a lock
                log_printf(15, "seg=" XIDT " waiting for p->offset=" XOT " lo=" XOT " hi=" XOT " skip_mode=%d\n", segment_id(seg), p->offset, lo, hi, skip_mode);
                p->access_pending[CACHE_READ]++;  //** Use a read to hold the page
                _cache_wait_for_page(seg, CACHE_WRITE, p);
                p->access_pending[CACHE_READ]--;

                skip_mode = 0;

                //** Need to reset iterator due to potential changes while waiting
                it = tbx_sl_iter_search(s->pages, &(p->offset), 0);
                err = tbx_sl_next(&it, (tbx_sl_key_t **)&poff, (tbx_sl_data_t **)&p);
            }
        }
    }

    log_printf(15, "seg=" XIDT " mode=%d lo=" XOT " hi=" XOT " skip_mode=%d\n", segment_id(seg), mode, lo, hi, skip_mode);

    while ((err == 0) && (p != NULL) && (*n_pages < max_pages)) {
        can_get = 1;
        if (((p->bit_fields & (C_EMPTY|C_TORELEASE)) > 0) || (p->access_pending[CACHE_READ] > 0)) {  //** If empty can't access it yet
            can_get = 0;
        } else if (p->access_pending[CACHE_FLUSH] > 0) {  //** Doing a flush and a write so block
            can_get = 0;
            flush_skip = 3;
            if (skip_mode == 0) {  //** See if we can do a COW to access it
                if (p->access_pending[CACHE_WRITE] == 0) {
                    if ((s->c->write_temp_overflow_used+s->page_size) < s->c->write_temp_overflow_size) {
                        i = (p->current_index+1) % 2;
                        if (p->data[i].ptr == NULL) {  //** We can use the COW space
                            s->c->write_temp_overflow_used += s->page_size;
                            tbx_type_malloc(p->data[i].ptr, char, s->page_size);
                            memcpy(p->data[i].ptr, p->data[p->current_index].ptr, s->page_size);
                            p->current_index = i;
                            p->curr_data = &(p->data[i]);
                            can_get = 1;
                        } else {
                            flush_skip = 4;
                        }
                    }
                }
            }
        }

        err = 0;
        if (skip_mode == 0) {
            if (can_get == 0) err = 1;
        } else {  //** Skipping mode so looking for a block I *could* get
            if (can_get == 1) err = 1;
        }

        if (err == 0) {
            old_hi = *hi_got;
            *hi_got = p->offset + s->page_size - 1;
            if (p->offset > hi) {
                err = 1;
                *hi_got = old_hi;
            }

            if (err == 0) {
                if (skip_mode == 0) {
                    _cache_add_page_to_list(s->c, p, &page[*n_pages], &iov[*n_pages], CACHE_WRITE, master_size, s->page_size);
                    (*n_pages)++;

                    log_printf(15, "seg=" XIDT " adding page[%d]->offset=" XOT "\n", segment_id(seg), *n_pages-1, p->offset);
                    log_printf(15, "PAGE_GET seg=" XIDT " get p->offset=" XOT " n=%d cr=%d cw=%d cf=%d bit_fields=%d usage=%d index=%d\n", segment_id(seg), p->offset, *n_pages-1,
                               p->access_pending[CACHE_READ], p->access_pending[CACHE_WRITE], p->access_pending[CACHE_FLUSH], p->bit_fields, p->curr_data->usage_count, p->current_index);
                }

                err = tbx_sl_next(&it, (tbx_sl_key_t **)&poff, (tbx_sl_data_t **)&p);

                if (p != NULL) {
                    if ((*hi_got != (p->offset - 1)) && (skip_mode == 0)) { //** Got a hole so see if we can fill it with blank pages
                        page_off = p->offset;
                        coff = *hi_got + 1;
                        pstart = p->offset;
                        if (pstart > hi) pstart = hi;
                        np = NULL;
                        p->access_pending[CACHE_READ]++;  //** Preserve the page from deletion

                        log_printf(15, "seg=" XIDT " before blank loop coff=" XOT " pstart=" XOT "\n", segment_id(seg), coff, pstart);
                        if (coff < pstart) np = s->c->fn.create_empty_page(s->c, seg, 0);  //** Get the empty page  if possible
                        while ((np != NULL) && (coff < pstart) && (*n_pages < max_pages)) {
                            if (np != NULL) {
                                s_cache_page_init(seg, np, coff);
                                if (full_page_overlap(coff, s->page_size, lo, hi) == 0) {
                                    if (s->child_last_page >= coff) {  //** Only load the page if not a write beyond the current EOF
                                        log_printf(15, "seg=" XIDT " adding page for reading p->offset=" XOT " current child_last_page=" XOT "\n", segment_id(seg), np->offset, s->child_last_page);
                                        pload[pload_count].p = np;
                                        pload[pload_count].data = np->curr_data;
                                        pload_index[pload_count] = *n_pages;
                                        pload_count++;
                                    }
                                }

                                _cache_add_page_to_list(s->c, np, &page[*n_pages], &iov[*n_pages], CACHE_WRITE, master_size, s->page_size);
                                (*n_pages)++;        

                                *hi_got = coff + s->page_size - 1;

                                log_printf(15, "seg=" XIDT " adding page[%d]->offset=" XOT "\n", segment_id(seg), *n_pages-1, np->offset);
                                log_printf(15, "PAGE_GET seg=" XIDT " get np->offset=" XOT " n=%d cr=%d cw=%d cf=%d bit_fields=%d np=%p usage=%d index=%d\n", segment_id(seg), np->offset, *n_pages-1,
                                           np->access_pending[CACHE_READ], np->access_pending[CACHE_WRITE], np->access_pending[CACHE_FLUSH], np->bit_fields, np, np->curr_data->usage_count, np->current_index);

                                coff += s->page_size;

                                if ((coff < pstart) && (*n_pages < max_pages)) np = s->c->fn.create_empty_page(s->c, seg, 0);  //** Get the empty page  if possible
                            }
                            log_printf(15, "pstart=" XOT " coff=" XOT "\n", pstart, coff);
                        }

                        //** Reset the iterator cause the page could have been removed in the interim
                        it = tbx_sl_iter_search(s->pages, &page_off, 0);
                        err = tbx_sl_next(&it, (tbx_sl_key_t **)&poff, (tbx_sl_data_t **)&p);

                        p->access_pending[CACHE_READ]--;  //** Release it

                        if (coff < pstart) { //** Didn't make it up to the 1st loaded page
                            err = 1;
                            s->c->fn.cache_miss_tag(s->c, seg, CACHE_WRITE, lo_row, hi_row, coff, cache_missed);
                        }
                    }
                    if (p != NULL) log_printf(15, "cache_page_get: seg=" XIDT " checking next page->offset=" XOT " hi_got=" XOT " hole_check=%d\n", segment_id(seg), p->offset, *hi_got, err);
                } else {
                    log_printf(15, "seg=" XIDT " No more pages\n", segment_id(seg));
                }
            } else {
                err = 1;
            }
        }
    }


    cache_unlock(s->c);

    //** Check if there are missing pages, if so force the loading if needed
    if ((*n_pages == 0) && (mode == CACHE_DOBLOCK)) {
        log_printf(15, "PAGE_GET seg=" XIDT " forcing page load lo_row=" XOT "\n", segment_id(seg), lo_row);
        p = cache_page_force_get(seg, rw_hints, CACHE_WRITE, lo_row, lo, hi);  //** This routine does it's own seg locking
        if (p != NULL) {
            cache_lock(s->c);
            _cache_add_page_to_list(s->c, p, &page[*n_pages], &iov[*n_pages], CACHE_WRITE, master_size, s->page_size);
            p->access_pending[CACHE_WRITE]--;  //** Both force_get and add_page_to_list update this so adjust for double counting
            cache_unlock(s->c);

            (*n_pages)++;        

            *hi_got = lo_row + s->page_size - 1;
            skip_mode = 0;
        }
    } else  if (pload_count > 0) { //** If needed load some pages before returning
        err = cache_rw_pages(seg, rw_hints, pload, pload_count, CACHE_READ, 0);
        if (err > 0) { //** Handle any errors that may have occurred
            for (i=0; i<pload_count; i++) {
                if (pload[i].data->ptr == NULL)  {
                    iov[pload_index[i]].iov_base = NULL;
                    log_printf(15, "blanking p->offset=" XOT " i=%d iov_index=%d\n", pload[i].p->offset, i, pload_index[i]);
                }
            }
        }
    }

    if (*n_pages == 0) skip_mode = 1;

    log_printf(1, "END seg=" XIDT " mode=%d lo=" XOT " hi=" XOT " hi_got=" XOT " skip_mode=%d n_pages=%d\n", segment_id(seg), mode, lo, hi, *hi_got, skip_mode, *n_pages);
    if (flush_skip == 1) {
        log_printf(5, "END seg=" XIDT " mode=%d lo=" XOT " hi=" XOT " hi_got=" XOT " skip_mode=%d n_pages=%d flush_skip=%d\n", segment_id(seg), mode, lo, hi, *hi_got, skip_mode, *n_pages, flush_skip);
        tbx_log_flush();
    }

    return(skip_mode);
}


//*******************************************************************************
//  cache_release_pages - Releases a collection of cache pages
//    NOTE:  ALL PAGES MUST BE FROM THE SAME SEGMENT
//*******************************************************************************

int cache_release_pages(int n_pages, lio_page_handle_t *page_list, int rw_mode)
{
    lio_segment_t *seg = page_list[0].p->seg;
    lio_cache_lio_segment_t *s = (lio_cache_lio_segment_t *)seg->priv;
    lio_cache_page_t *page;
    lio_cache_cond_t *cache_cond;
    int count, i, cow_hit;
    ex_off_t min_off, max_off;
    gop_op_generic_t *gop;

    cache_lock(s->c);
    segment_lock(seg);

    min_off = s->total_size;
    max_off = -1;

    for (i=0; i<n_pages; i++) {
        page = page_list[i].p;

        page->access_pending[rw_mode]--;
        page_list[i].data->usage_count--;

        log_printf(15, "seg=" XIDT " initial rw_mode=%d p->offset=" XOT " cr=%d cw=%d cf=%d bit_fields=%d usage=%d index=%d\n", segment_id(seg), rw_mode, page->offset,
                   page->access_pending[CACHE_READ], page->access_pending[CACHE_WRITE], page->access_pending[CACHE_FLUSH], page->bit_fields, page_list[i].data->usage_count, page->current_index);

        cow_hit = 0;
        if (page_list[i].data != page->curr_data) {
            cow_hit = 1;
            if (page_list[i].data->usage_count <= 0) {  //** Clean up a COW
                free(page_list[i].data->ptr);
                page_list[i].data->ptr = NULL;
                s->c->write_temp_overflow_used -= s->page_size;
                log_printf(15, "seg=" XIDT " p->offset=" XOT " COP cleanup used=" XOT " rw_mode=%d usage=%d\n", segment_id(seg), page->offset, s->c->write_temp_overflow_used, rw_mode, page_list[i].data->usage_count);
                tbx_log_flush();
            }
        }

        if (rw_mode == CACHE_WRITE) {  //** Write release
            if (page->bit_fields & C_EMPTY) page->bit_fields ^= C_EMPTY;
            if ((page->bit_fields & C_ISDIRTY) == 0) {
                s->c->fn.adjust_dirty(s->c, s->page_size);
                page->bit_fields |= C_ISDIRTY;
            }
        } else if (rw_mode == CACHE_FLUSH) {  //** Flush release so tweak dirty page info
            if (cow_hit == 0) {
                s->c->fn.adjust_dirty(s->c, -s->page_size);
                page->bit_fields ^= C_ISDIRTY;
            }
        }

        log_printf(15, "seg=" XIDT " released rw_mode=%d p->offset=" XOT " cr=%d cw=%d cf=%d bit_fields=%d\n", segment_id(seg), rw_mode, page->offset,
                   page->access_pending[CACHE_READ], page->access_pending[CACHE_WRITE], page->access_pending[CACHE_FLUSH], page->bit_fields);

        cache_cond = (lio_cache_cond_t *)tbx_pch_data(&(page->cond_pch));
        if (cache_cond != NULL) {  //** Someone is listening so wake them up
            apr_thread_cond_broadcast(cache_cond->cond);
        } else {
            if ((page->bit_fields & C_TORELEASE) > 0) {
                count = page->access_pending[CACHE_READ] + page->access_pending[CACHE_WRITE] + page->access_pending[CACHE_FLUSH];
                if (count == 0) {
                    //** page->data is an array so the 2nd bool is always false.
                    //** leaving the old code as a comment: if (((page->bit_fields & C_ISDIRTY) == 0) || (page->data == NULL)) {
                    if ((page->bit_fields & C_ISDIRTY) == 0) {  //** Not dirty so release it
                        s->c->fn.s_pages_release(s->c, &page, 1); //** No one else is listening so release the page
                    } else {  //** Should be manually flushed so force one
                        if (min_off > page->offset) min_off = page->offset;
                        if (max_off < page->offset) max_off = page->offset;
                    }
                }
            }
        }
    }

    segment_unlock(seg);
    cache_unlock(s->c);

    if (max_off > -1) {  //** Got to flush some pages
        log_printf(5, "Looks like we need to do a manual flush.  min_off=" XOT " max_off=" XOT "\n", min_off, max_off);
        gop = cache_flush_range_gop(seg, s->c->da, min_off, max_off+s->page_size-1, s->c->timeout);
        gop_set_auto_destroy(gop, 1);
        gop_start_execution(gop);
    }

    return(0);
}

//*******************************************************************************
// _cache_ppages_range_print - Prints the PP range list
//*******************************************************************************

void _cache_ppages_range_print(int ll, lio_cache_partial_page_t *pp)
{
    int i;
    ex_off_t *rng, *crng;
    char *curr;
    tbx_stack_ele_t *cptr;

    if (tbx_log_level() < ll) return;

    log_printf(ll, "page_start=" XOT " page_end=" XOT " n_ranges=%d full=%d\n", pp->page_start, pp->page_end, tbx_stack_count(pp->range_stack), pp->flags);

    crng = tbx_stack_get_current_data(pp->range_stack);
    cptr = tbx_stack_get_current_ptr(pp->range_stack);
    tbx_stack_move_to_top(pp->range_stack);
    i=0;
    while ((rng = tbx_stack_get_current_data(pp->range_stack)) != NULL) {
        curr = (rng == crng) ? "CURR" : "";
        log_printf(ll, "  i=%d " XOT " - " XOT " %s\n", i, rng[0], rng[1], curr);
        tbx_stack_move_down(pp->range_stack);
        i++;
    }

    tbx_stack_move_to_ptr(pp->range_stack, cptr);
}

//*******************************************************************************
//  _cache_ppages_range_collapse - Collapses the pp ranges.  Starts processing
//    from the current range and iterates if needed.
//
//    NOTE: Assumes the cache is locked!
//*******************************************************************************

int _cache_ppages_range_collapse(lio_cache_partial_page_t *pp)
{
    ex_off_t *rng, *trng, hi1;
    int more;

    trng = tbx_stack_get_current_data(pp->range_stack);  //** This is the range just expanded
    hi1 = trng[1]+1;

    tbx_stack_move_down(pp->range_stack);
    more = 1;
    while (((rng = tbx_stack_get_current_data(pp->range_stack)) != NULL) && (more == 1)) {
        if (hi1 >= rng[0]) { //** Got an overlap so collapse
            if (rng[1] > trng[1]) {
                trng[1] = rng[1];
                more = 0;  //** Kick out this is the last range
            }
            tbx_stack_delete_current(pp->range_stack, 0, 1);
        } else {
            more = 0;
        }
    }

    log_printf(5, "n_ranges=%d\n", tbx_stack_count(pp->range_stack));

    //** Check if we have a full page
    if (tbx_stack_count(pp->range_stack) == 1) {
        tbx_stack_move_to_top(pp->range_stack);
        rng = tbx_stack_get_current_data(pp->range_stack);
        log_printf(5, "lo=" XOT " gi=" XOT "\n", rng[0], rng[1]);

        if ((rng[0] == 0) && (rng[1] == (pp->page_end - pp->page_start))) {
            pp->flags = 1;
        }
    }

    return(pp->flags);
}
//*******************************************************************************
// _cache_ppages_range_merge - Merges user write range w/ existing ranges
//     Returns 1 if the page is completely covered or 0 otherwise.
//
//    NOTE: Assumes the cache is locked!
//*******************************************************************************

int _cache_ppages_range_merge(lio_segment_t *seg, lio_cache_partial_page_t *pp, ex_off_t lo, ex_off_t hi)
{
    lio_cache_lio_segment_t *s = (lio_cache_lio_segment_t *)seg->priv;
    ex_off_t *rng, *prng, trng[2];
    int full;

    log_printf(5, "seg=" XIDT " START plo=" XOT " phi=" XOT "\n", segment_id(seg), lo, hi);
    _cache_ppages_range_print(5, pp);

    //** If an empty stack can handle it quickly
    if (tbx_stack_count(pp->range_stack) == 0) {
        if ((lo == 0) && (hi == s->page_size-1)) { //** See if a full page
            pp->flags = 1;
            return(1);
        }

        tbx_type_malloc(rng, ex_off_t, 2);
        rng[0] = lo;
        rng[1] = hi;
        tbx_stack_push(pp->range_stack, rng);
        log_printf(5, "seg=" XIDT " END stack_size=%d\n", segment_id(seg), tbx_stack_count(pp->range_stack));

        return(0);
    }


    //** Find the insertion point
    tbx_stack_move_to_top(pp->range_stack);
    prng = NULL;
    while ((rng = tbx_stack_get_current_data(pp->range_stack)) != NULL) {
        if (lo <= rng[0]) break;  //** Got it
        prng = rng;
        tbx_stack_move_down(pp->range_stack);
    }

    full = 0;

    if (rng != NULL) {
        log_printf(5, "seg=" XIDT " After insertion point rlo=" XOT " rhi=" XOT "\n", segment_id(seg), rng[0], rng[1]);
    } else {
        log_printf(5, "seg=" XIDT " After insertion point rng=NULL\n", segment_id(seg));
    }


    if (prng != NULL) {
        log_printf(5, "seg=" XIDT " After insertion point prlo=" XOT " prhi=" XOT "\n", segment_id(seg), prng[0], prng[1]);
    } else {
        log_printf(5, "seg=" XIDT " After insertion point prng=NULL\n", segment_id(seg));
    }

    if (prng == NULL) {  //** Fudge to get proper logic
        trng[0] = 12345;
        trng[1] = lo - 10;
        prng = trng;
    }

    if (lo <= prng[1]+1) { //** Expand prev range
        log_printf(5, "seg=" XIDT " checking if can collapse prhi=" XOT " hi=" XOT "\n", segment_id(seg), prng[1], hi);
        if (prng[1] < hi) {
            prng[1] = hi;  //** Extend the range
            if (rng != NULL) {  //** Move back before collapsing.  Otherwise we're at the end and we've already extended the range
                log_printf(5, "seg=" XIDT " collapsing prlo=" XOT " prhi=" XOT "\n", segment_id(seg), prng[0], prng[1]);
                tbx_stack_move_up(pp->range_stack);
                full = _cache_ppages_range_collapse(pp);
            } else if (tbx_stack_count(pp->range_stack) == 1) {   //** Check if we have a full page
                if ((prng[0] == 0) && (prng[1] == (pp->page_end - pp->page_start))) {
                    pp->flags = 1;
                }
            }
        }
    } else if (rng != NULL) {  //** Check if overlap on curr range
        if (rng[0] <= hi+1) {  //** Got an overlap
            rng[0] = lo;
            if (rng[1] < hi) {  //** Expanding on the hi side so need to check for collapse
                rng[1] = hi;
                full = _cache_ppages_range_collapse(pp);
            }
        } else {  //** No overlap.  This is a new range to insert
            tbx_type_malloc(rng, ex_off_t, 2);
            rng[0] = lo;
            rng[1] = hi;
            tbx_stack_insert_above(pp->range_stack, rng);
        }
    } else {  //** Adding to the end
        tbx_type_malloc(rng, ex_off_t, 2);
        rng[0] = lo;
        rng[1] = hi;
        tbx_stack_move_to_bottom(pp->range_stack);
        tbx_stack_insert_below(pp->range_stack, rng);
    }

    log_printf(5, "seg=" XIDT " Final table plo=" XOT " phi=" XOT "\n", segment_id(seg), lo, hi);
    _cache_ppages_range_print(5, pp);

    return(full);
}

//*******************************************************************************
//  _cache_ppages_ wait_for_flushes_to_complete - Waits for the parital pages to
//   complete being flushed.
//*******************************************************************************

void _cache_ppages_wait_for_flush_to_complete(lio_cache_lio_segment_t *s)
{
    if (s->ppages_flushing != 0) {
        log_printf(5, "Waiting for flush to complete\n");
        do {
            apr_thread_cond_wait(s->ppages_cond, s->c->lock);
        } while (s->ppages_flushing != 0);
        log_printf(5, "Flush completed\n");
    }
}


//*******************************************************************************
// _cache_ppages_flush_list - Flushes a list partial pages
//     NOTE:  Cache should be locked on entry
//*******************************************************************************

int _cache_ppages_flush_list(lio_segment_t *seg, data_attr_t *da, tbx_stack_t *pp_list)
{
    lio_cache_lio_segment_t *s = (lio_cache_lio_segment_t *)seg->priv;
    lio_cache_partial_page_t *pp;
    cache_rw_op_t cop;
    ex_tbx_iovec_t *ex_iov;
    tbx_iovec_t *iov;
    tbx_tbuf_t tbuf;
    ex_off_t *rng, r[2];
    int n_ranges, slot;
    ex_off_t nbytes, len;
    gop_op_status_t status;

    if (tbx_stack_count(pp_list) == 0) return(0);

    if (s->ppages_flushing != 0) _cache_ppages_wait_for_flush_to_complete(s);   //** Flushing ppages so wait until finished

    s->ppages_flushing = 1;  //** Let everyone know I'm flushing now

    log_printf(5, "Flushing ppages seg=" XIDT " tbx_stack_count(pp_list)=%d  ppages_unused=%d\n", segment_id(seg), tbx_stack_count(pp_list), tbx_stack_count(s->ppages_unused));

    //** Cycle through the pages makng the write map for each page
    n_ranges = 0;
    tbx_stack_move_to_top(pp_list);
    while ((pp = tbx_stack_get_current_data(pp_list)) != NULL) {
        log_printf(5, "START ppoff=" XOT " RSTACK=%p size=%d flags=%d\n", pp->page_start, pp->range_stack, tbx_stack_count(pp->range_stack), pp->flags);
        tbx_log_flush();

        n_ranges += (pp->flags == 1) ? 1 : tbx_stack_count(pp->range_stack);
        tbx_stack_move_down(pp_list);
        log_printf(5, "END ppoff=" XOT " RSTACK=%p size=%d full=%d n_ranges=%d\n", pp->page_start, pp->range_stack, tbx_stack_count(pp->range_stack), pp->flags, n_ranges);
        tbx_log_flush();
    }

    //** Fill in the RW op struct
    tbx_type_malloc_clear(ex_iov, ex_tbx_iovec_t, n_ranges);
    tbx_type_malloc_clear(iov, tbx_iovec_t, n_ranges);
    cop.seg = seg;
    cop.da = da;
    cop.n_iov = n_ranges;
    cop.iov = ex_iov;
    cop.rw_mode = CACHE_WRITE;
    cop.boff = 0;
    cop.buf = &tbuf;
    cop.skip_ppages = 1;
    cop.rw_hints = NULL;

    nbytes = 0;
    slot = 0;
    tbx_stack_move_to_top(pp_list);
    while ((pp = tbx_stack_get_current_data(pp_list)) != NULL) {
        if (pp->flags == 1) {
            iov[slot].iov_base = pp->data;
            iov[slot].iov_len = s->page_size;
            ex_iov[slot].offset = pp->page_start;
            ex_iov[slot].len = s->page_size;
            nbytes += s->page_size;
            r[1] = ex_iov[slot].offset + s->page_size - 1;
            log_printf(5, "seg=" XIDT " pp_start=" XOT " slot=%d off=" XOT " end=" XOT " len=" XOT "\n", segment_id(seg),pp->page_start, slot, ex_iov[slot].offset, r[1], ex_iov[slot].len);
            slot++;
        } else {
            while ((rng = (ex_off_t *)tbx_stack_pop(pp->range_stack)) != NULL) {
                len = rng[1] - rng[0] + 1;
                iov[slot].iov_base = &(pp->data[rng[0]]);
                iov[slot].iov_len = len;
                ex_iov[slot].offset = pp->page_start + rng[0];
                ex_iov[slot].len = len;
                nbytes += len;
                r[1] = ex_iov[slot].offset + len - 1;
                log_printf(5, "seg=" XIDT " pp_start=" XOT " slot=%d off=" XOT " end=" XOT " len=" XOT "\n", segment_id(seg), pp->page_start, slot, ex_iov[slot].offset, r[1], ex_iov[slot].len);
                slot++;
                free(rng);
            }
        }

        pp->flags = 0;
        tbx_stack_empty(pp->range_stack, 1);
        tbx_stack_push(s->ppages_unused, pp);
        tbx_sl_remove(s->partial_pages, &(pp->page_start), pp);

        tbx_stack_move_down(pp_list);
    }

    //** finish the tbuf setup
    tbx_tbuf_vec(&tbuf, nbytes, n_ranges, iov);

    //** Do the flush
    log_printf(5, "Performing flush now\n");

    cache_unlock(s->c);

    status = cache_rw_func(&cop, 0);

    //** Notify everyone it's done
    cache_lock(s->c);  //** I had this on the way in

    //** Update the ppage_max
    rng = tbx_sl_key_last(s->partial_pages);
    if (rng == NULL) {    //** No ppages left
        s->ppage_max = -1;
    } else {  //** Need to find the check the last partial page to determine the max offset
        s->ppage_max = *rng;  //** This is our backup value in case of an error.  It's soley an attempt to recover gracefully.
        pp = tbx_list_search(s->partial_pages, (tbx_sl_key_t *)rng);
        if (pp == NULL) { //** This shouldn't happen so print some diagnostic info and do our best to recover.
            log_printf(0, "ERROR: sid=" XIDT " lost partial page!  Looking for pp->page_start=" XOT "\n", segment_id(seg), *rng);
            fprintf(stderr, "ERROR: sid=" XIDT " lost partial page!  Looking for pp->page_start=" XOT "\n", segment_id(seg), *rng);
        } else {
            tbx_stack_move_to_bottom(pp->range_stack);
            rng = tbx_stack_get_current_data(pp->range_stack);
            if (rng != NULL) {
                s->ppage_max = pp->page_start + rng[1];
            }
        }
    }

    s->ppages_flushing = 0;  //** This is protected by the segment lock
    log_printf(5, "Flush completed pp_max=" XOT "\n", s->ppage_max);
    apr_thread_cond_broadcast(s->ppages_cond);

    free(ex_iov);
    free(iov);
    return((status.op_status == OP_STATE_SUCCESS) ? 0 : 1);
}

//*******************************************************************************
// _cache_ppages_flush - Flushes the partial pages
//     NOTE:  Cache should be locked on entry
//*******************************************************************************

int _cache_ppages_flush(lio_segment_t *seg, data_attr_t *da)
{
    lio_cache_lio_segment_t *s = (lio_cache_lio_segment_t *)seg->priv;
    lio_cache_partial_page_t *pp;
    tbx_stack_t pp_list;
    ex_off_t ppoff;
    int err;
    tbx_sl_iter_t it;

    if (tbx_stack_count(s->ppages_unused) == s->n_ppages) return(0);

    if (s->ppages_flushing != 0) _cache_ppages_wait_for_flush_to_complete(s);   //** Flushing ppages so wait until finished

    log_printf(5, "Flushing ppages seg=" XIDT " ppages_used=%d\n", segment_id(seg), s->ppages_used);

    //** Cycle through the pages makng the write map for each page
    tbx_stack_init(&pp_list);
    it = tbx_sl_iter_search(s->partial_pages, NULL, 0);
    while (tbx_sl_next(&it, (tbx_sl_key_t **)&ppoff, (tbx_sl_data_t **)&pp) == 0) {
        log_printf(5, "ppoff=" XOT " RSTACK=%p size=%d flags=%d\n", pp->page_start, pp->range_stack, tbx_stack_count(pp->range_stack), pp->flags);
        tbx_log_flush();
        tbx_stack_insert_below(&pp_list, pp);
    }

    err = _cache_ppages_flush_list(seg, da, &pp_list);

    tbx_stack_empty(&pp_list, 0);

    return(err);
}

//*******************************************************************************
// cache_ppages_handle - Process partail page requests storing them in interim
//     staging area
//
//     tb_err is used to return the number of bad bytes in the tbuf during the copy.
//          It's treated an accumulator and should be initialized by the calling program.
//*******************************************************************************

int cache_ppages_handle(lio_segment_t *seg, data_attr_t *da, int rw_mode, ex_off_t *lo, ex_off_t *hi, ex_off_t *len, ex_off_t *bpos, tbx_tbuf_t *tbuf, int *tb_err)
{
    lio_cache_lio_segment_t *s = (lio_cache_lio_segment_t *)seg->priv;
    lio_cache_partial_page_t *pp;
    ex_off_t lo_page, hi_page, n_pages, *ppoff, poff, boff, nbytes, pend, nhandled, plo, phi;
    ex_off_t lo_new, hi_new, bpos_new;
    ex_off_t *rng;
    tbx_stack_t pp_flush;
    tbx_tbuf_t pptbuf;
    tbx_sl_iter_t it;
    int do_flush, err, lo_mapped, hi_mapped;

    log_printf(5, "START lo=" XOT " hi=" XOT " bpos=" XOT "\n", *lo, *hi, *bpos);
    tbx_log_flush();

    cache_lock(s->c);
    if (s->n_ppages == 0) {
        cache_unlock(s->c);
        return(0);
    }

    if (s->ppages_flushing != 0)  _cache_ppages_wait_for_flush_to_complete(s);   //** Wait for any flushes to complete

    lo_page = *lo / s->page_size;
    n_pages = lo_page;
    lo_page = lo_page * s->page_size;
    hi_page = *hi / s->page_size;
    n_pages = hi_page - n_pages + 1;
    hi_page = hi_page * s->page_size;

    log_printf(5, "lo=" XOT " hi=" XOT " lo_page=" XOT " hi_page=" XOT " n_pages=%" PRId64 " \n", *lo, *hi, lo_page, hi_page, n_pages);

    //** If we made it here the end pages at least don't exist
    //** See if we map to existing pages and update as needed
    do_flush = 0;
    nhandled = 0;
    lo_mapped = 0;
    hi_mapped = 0;
    lo_new = *lo;
    hi_new = *hi;
    bpos_new = *bpos;

    it = tbx_sl_iter_search(s->partial_pages, &lo_page, 0);
    while (tbx_sl_next(&it, (tbx_sl_key_t **)&ppoff, (tbx_sl_data_t **)&pp) == 0) {
        log_printf(5, "LOOP seg=" XIDT " rw_mode=%d ppage pstart=" XOT " pend=" XOT "\n", segment_id(seg), rw_mode, pp->page_start, pp->page_end);

        if (*ppoff > *hi) break;  //** Out of bounds so kick out

        //** Interior whole page check  (always copy the data to make sure we have a full page before flushing)
        if ((n_pages > 2) && (lo_page < pp->page_start) && (pp->page_start < hi_page)) {
            if (rw_mode == CACHE_WRITE) {
                poff = 0;
                boff = *bpos + pp->page_start - *lo;
                nbytes = s->page_size;
                tbx_tbuf_single(&pptbuf, s->page_size, pp->data);
                *tb_err += tbx_tbuf_copy(tbuf, boff, &pptbuf, poff, nbytes, 1);
                pp->flags = 1; //** Full page
                nhandled++;
            } else {  //** Got a read so flush the page
                if (do_flush == 0) tbx_stack_init(&pp_flush);
                tbx_stack_insert_below(&pp_flush, pp);
                do_flush++;
            }

            log_printf(5, "INTERIOR FULL rw_mode=%d seg=" XIDT " using ppages pstart=" XOT " pend=" XOT "\n", rw_mode, segment_id(seg), pp->page_start, pp->page_end);
        }

        //** Move the hi end down
        if ((hi_page == pp->page_start) && (lo_page != hi_page)) {
            if (rw_mode == CACHE_WRITE) {
                poff = 0;
                boff = *bpos + pp->page_start - *lo;
                nbytes = *hi - pp->page_start + 1;
                tbx_tbuf_single(&pptbuf, s->page_size, pp->data);
                *tb_err += tbx_tbuf_copy(tbuf, boff, &pptbuf, poff, nbytes, 1);
                hi_new = pp->page_start - 1;

                _cache_ppages_range_merge(seg, pp, 0, nbytes - 1);

                pend = pp->page_start + nbytes - 1;
                if (pend > s->ppage_max) s->ppage_max = pend;
                nhandled++;
                hi_mapped = 1;
                log_printf(5, "HI_MAPPED INSERT seg=" XIDT " using pstart=" XOT " pend=" XOT " rlo=%d rhi=" XOT "\n", segment_id(seg), pp->page_start, pp->page_end, 0, nbytes-1);
            } else {   //** Got a read hit so check if the 1st range completely overlaps otherwise flush the page
                tbx_stack_move_to_top(pp->range_stack);
                rng = tbx_stack_get_current_data(pp->range_stack);
                poff = *hi - pp->page_start;
                if ((rng[0] == 0) && (rng[1] >= poff)) { //** 1st range overlaps so handle it
                    poff = 0;
                    boff = *bpos + pp->page_start - *lo;
                    nbytes = *hi - pp->page_start + 1;
                    tbx_tbuf_single(&pptbuf, s->page_size, pp->data);
                    *tb_err += tbx_tbuf_copy(&pptbuf, poff, tbuf, boff, nbytes, 1);
                    hi_new = pp->page_start - 1;
                    hi_mapped = 1;
                    nhandled++;
                    log_printf(5, "HI_MAPPED READ seg=" XIDT " using pstart=" XOT " pend=" XOT " rlo=%d rhi=" XOT "\n", segment_id(seg), pp->page_start, pp->page_end, 0, nbytes-1);
                } else { //** No luck so have to flush the page
                    if (do_flush == 0) tbx_stack_init(&pp_flush);
                    tbx_stack_insert_below(&pp_flush, pp);
                    do_flush++;
                }
            }
        }

        //** Move the lo end partial page (also handles if lo_page=hi_page)
        if (lo_page == pp->page_start) {
            if (rw_mode == CACHE_WRITE) {
                poff = *lo - pp->page_start;
                boff = *bpos;
                if (*hi > pp->page_end) {
                    pend = s->page_size - 1;
                } else {
                    hi_mapped = 1;
                    pend = *hi - pp->page_start;
                }
                nbytes = pend - poff + 1;
                tbx_tbuf_single(&pptbuf, s->page_size, pp->data);
                *tb_err += tbx_tbuf_copy(tbuf, boff, &pptbuf, poff, nbytes, 1);
                lo_new = *lo + nbytes;
                bpos_new = *bpos + nbytes;

                _cache_ppages_range_merge(seg, pp, poff, pend);

                log_printf(5, "LO_MAPPED INSERT seg=" XIDT " using pstart=" XOT " pend=" XOT " rlo=" XOT " rhi=" XOT "\n", segment_id(seg), pp->page_start, pp->page_end, poff, pend);
                pend = pp->page_start + pend;
                if (pend > s->ppage_max) s->ppage_max = pend;
                nhandled++;
                lo_mapped = 1;
            } else { //** Got a READ op so check if the last maps or middle if lo/hi pages are the same
                if ( lo_page == hi_page) {
                    plo = *lo - pp->page_start;
                    phi = *hi - pp->page_start;
                    tbx_stack_move_to_top(pp->range_stack);
                    while ((rng = tbx_stack_get_current_data(pp->range_stack)) != NULL) {
                        if ((rng[0] <= plo) && (rng[1] >= plo)) { //** Found the overlapping range
                            if (rng[1] >= phi) { //** we're good so map it
                                poff = plo;
                                boff = *bpos;
                                nbytes = phi - plo + 1;
                                tbx_tbuf_single(&pptbuf, s->page_size, pp->data);
                                *tb_err += tbx_tbuf_copy(&pptbuf, poff, tbuf, boff, nbytes, 1);
                                lo_mapped = hi_mapped = 1;
                                lo_new = *lo + nbytes;
                                bpos_new = *bpos + nbytes;
                                nhandled++;

                                log_printf(5, "LO_MAPPED READ seg=" XIDT " using pstart=" XOT " pend=" XOT " rlo=" XOT " rhi=" XOT "\n", segment_id(seg), pp->page_start, pp->page_end, rng[0], rng[1]);
                                break;  //** Kick out
                            } else {
                                break;  //** No good so kick out
                            }
                        }

                        tbx_stack_move_down(pp->range_stack);
                    }

                    log_printf(5, "LO_MAPPED READ seg=" XIDT " using pstart=" XOT " pend=" XOT " lo_mapped=hi_mapped=%d\n", segment_id(seg), pp->page_start, pp->page_end, lo_mapped);

                    if (lo_mapped != 1)  {  //** No luck so got to read it
                        if (do_flush == 0) tbx_stack_init(&pp_flush);
                        tbx_stack_insert_below(&pp_flush, pp);
                        do_flush++;
                    }
                } else {  //** The lo/hi mapped pages are different so just have to check the last range
                    tbx_stack_move_to_bottom(pp->range_stack);
                    rng = tbx_stack_get_current_data(pp->range_stack);
                    plo = *lo - pp->page_start;
                    if ((rng[0] <= plo) && (rng[1] == s->page_size-1)) {  //** Got a match
                        poff = plo;
                        boff = *bpos;
                        nbytes = s->page_size - plo;
                        tbx_tbuf_single(&pptbuf, s->page_size, pp->data);
                        *tb_err += tbx_tbuf_copy(&pptbuf, poff, tbuf, boff, nbytes, 1);
                        lo_mapped = 1;
                        lo_new = *lo + nbytes;
                        bpos_new = *bpos + nbytes;

                        nhandled++;

                        log_printf(5, "LO_MAPPED READ seg=" XIDT " using pstart=" XOT " pend=" XOT " rlo=" XOT " rhi=" XOT "\n", segment_id(seg), pp->page_start, pp->page_end, rng[0], rng[1]);

                    } else {
                        if (do_flush == 0) tbx_stack_init(&pp_flush);
                        tbx_stack_insert_below(&pp_flush, pp);
                        do_flush++;
                    }
                }
            }
        }

        if (pp->flags == 1) { // ** Got a full page so flush it
            if (do_flush == 0) tbx_stack_init(&pp_flush);

            do_flush++;
            tbx_stack_insert_below(&pp_flush, pp);
        }
    }

    if (do_flush > 0) {
        _cache_ppages_flush_list(seg, da, &pp_flush);
        tbx_stack_empty(&pp_flush, 0);
        do_flush = 0;
    }

    //** Completed overlap to existing pages check so
    //** Check if we have full coverage on a write.  If so kick out.
    //** For reads this is all we can do.
    if ((nhandled == n_pages) || (rw_mode == CACHE_READ)) {
        cache_unlock(s->c);
        *lo = lo_new;
        *hi = hi_new;
        *bpos = bpos_new;
        log_printf(5, "END lo=" XOT " hi=" XOT " bpos=" XOT " nhandled=%" PRId64 " n_pages=%" PRId64 "\n", *lo, *hi, *bpos, nhandled, n_pages);
        tbx_log_flush();
        return((n_pages == nhandled) ? 1 : 0);
    }

    //------------------------------------------------------------------
    //** If we made it here we are dealing with a ppage write.  We only
    //** care about checking the ppages on the ends.  Whole pages are
    //** Ignored and handle by the normal code.
    //------------------------------------------------------------------

    //** See if we have enough free ppages to store the ends. If not flush
    if (tbx_stack_count(s->ppages_unused) < (2 - lo_mapped - hi_mapped)) {
        log_printf(5, "Triggering a flush\n");

        err = _cache_ppages_flush(seg, da);
        if (err != 0) {
            cache_unlock(s->c);
            return(err);
        }

        //** During the flush we lost the lock and so the pages could have been loaded
        //** in either cache or ppages.  So we're just going to call ourself again
        *lo = lo_new;
        *hi = hi_new;
        *bpos = bpos_new;
        log_printf(5, "RECURSE lo=" XOT " hi=" XOT " bpos=" XOT "\n", *lo, *hi, *bpos);
        tbx_log_flush();
        cache_unlock(s->c);
        return(cache_ppages_handle(seg, da, rw_mode, lo, hi, len, bpos, tbuf, tb_err));
    }

    //** NOTE if we have whole pages don't store
    if (lo_mapped == 0) { // ** Map the lo end
        pp = tbx_stack_pop(s->ppages_unused);
        pp->page_start = lo_page;
        pp->page_end = lo_page + s->page_size -1;

        poff = *lo - pp->page_start;
        boff = *bpos;
        if (*hi > pp->page_end) {
            pend = s->page_size - 1;
        } else {
            hi_mapped = 1;
            pend = *hi - pp->page_start;
        }
        nbytes = pend - poff + 1;
        tbx_tbuf_single(&pptbuf, s->page_size, pp->data);
        *tb_err += tbx_tbuf_copy(tbuf, boff, &pptbuf, poff, nbytes, 1);
        lo_new = *lo + nbytes;
        bpos_new = *bpos + nbytes;

        tbx_list_insert(s->partial_pages, &(pp->page_start), pp);

        _cache_ppages_range_merge(seg, pp, poff, pend);

        log_printf(5, "LO_MAPPED ADDED seg=" XIDT " using ppage pstart=" XOT " pend=" XOT " rlo=" XOT " rhi=" XOT " n_ppages=%d\n", segment_id(seg), pp->page_start, pp->page_end, poff, pend, tbx_sl_key_count(s->partial_pages));

        pend = pp->page_start + pend;
        if (pend > s->ppage_max) s->ppage_max = pend;
        nhandled++;
        if (pp->flags == 1) {
            if (do_flush == 0) tbx_stack_init(&pp_flush);
            tbx_stack_insert_below(&pp_flush, pp);
            do_flush++;
        }
    }

    if (hi_mapped == 0) { // ** Do the same for the hi end
        pp = tbx_stack_pop(s->ppages_unused);
        pp->page_start = hi_page;
        pp->page_end = hi_page + s->page_size -1;

        poff = 0;
        boff = *bpos + pp->page_start - *lo;
        nbytes = *hi - pp->page_start + 1;
        tbx_tbuf_single(&pptbuf, s->page_size, pp->data);
        *tb_err += tbx_tbuf_copy(tbuf, boff, &pptbuf, poff, nbytes, 1);
        hi_new = pp->page_start - 1;

        tbx_list_insert(s->partial_pages, &(pp->page_start), pp);

        _cache_ppages_range_merge(seg, pp, 0, nbytes - 1);

        pend = pp->page_start + nbytes - 1;
        if (pend > s->ppage_max) s->ppage_max = pend;
        nhandled++;

        if (pp->flags == 1) {
            if (do_flush == 0) tbx_stack_init(&pp_flush);
            tbx_stack_insert_below(&pp_flush, pp);
            do_flush++;
        }

        log_printf(5, "HI_MAPPED ADDED seg=" XIDT " using ppage pstart=" XOT " pend=" XOT " rlo=%d rhi=" XOT "\n", segment_id(seg), pp->page_start, pp->page_end, 0, nbytes-1);
    }


    if (do_flush > 0) {  //** Do a flush if not completely covered
        log_printf(1, "Triggering a flush do_flush=%d nhandled=%" PRId64 " n_pages=%" PRId64 "\n", do_flush, nhandled, n_pages);

        _cache_ppages_flush_list(seg, da, &pp_flush);
        tbx_stack_empty(&pp_flush, 0);
    }

    cache_unlock(s->c);

    *lo = lo_new;
    *hi = hi_new;
    *bpos = bpos_new;
    log_printf(5, "END lo=" XOT " hi=" XOT " bpos=" XOT "\n", *lo, *hi, *bpos);
    tbx_log_flush();
    return((n_pages == nhandled) ? 1 : 0);
}

//*******************************************************************************
// cache_rw_func - Function for reading/writing to cache
//*******************************************************************************

gop_op_status_t cache_rw_func(void *arg, int id)
{
    cache_rw_op_t *cop = (cache_rw_op_t *)arg;
    lio_segment_t *seg = cop->seg;
    lio_cache_lio_segment_t *s = (lio_cache_lio_segment_t *)seg->priv;
    lio_page_handle_t page[CACHE_MAX_PAGES_RETURNED];
    tbx_iovec_t iov[CACHE_MAX_PAGES_RETURNED];
    int status, n_pages;
    tbx_stack_t stack;
    lio_cache_range_t *curr, *r;
    int progress, tb_err, rerr, first_time;
    int mode, i, j, top_cnt, bottom_cnt;
    gop_op_status_t err;
    ex_off_t bpos2, bpos, poff, len, mylen, lo, hi, ngot, pstart, plen;
    ex_off_t hi_got, new_size, blen;
    ex_off_t total_bytes, hit_bytes;
    tbx_tbuf_t tb;
    void *cache_missed_table[100];
    void **cache_missed;
    apr_time_t hit_time, miss_time;

    tb_err = 0;
    err = gop_success_status;
    if (cop->n_iov == 0) return(err);  //** Nothing to do so kick out

    tbx_stack_init(&stack);

    //** Push the initial ranges onto the work queue
    bpos = cop->boff;
    bpos2 = bpos;
    new_size = 0;
    rerr = 0;
    mylen = 0;
    total_bytes = 0;
    for (i=0; i< cop->n_iov; i++) {
        if (cop->iov[i].len <= 0) rerr = 1;
        lo = cop->iov[i].offset;
        len = cop->iov[i].len;
        hi = lo + len - 1;
        total_bytes += len;
        bpos2 = bpos;
        bpos += len;
        j = (cop->skip_ppages == 0) ? cache_ppages_handle(seg, cop->da, cop->rw_mode, &lo, &hi, &len, &bpos2, cop->buf, &tb_err) : 0;
        if (j == 0) { //** Check if the ppages slurped it up
            if (new_size < hi) new_size = hi;
            r = cache_new_range(lo, hi, bpos2, i);
            mylen += len;
            tbx_stack_push(&stack, r);
        } else if (j < 0) {
            rerr = -1;
        }
        log_printf(15, "gid=%d START i=%d lo=" XOT " hi=" XOT " new_size=" XOT " rw_mode=%d rerr=%d\n", id, i, lo, hi, new_size, cop->rw_mode, rerr);
    }

    if (tbx_stack_count(&stack) == 0) { //** Handled via ppages
        log_printf(15, "seg=" XIDT " Nothing to do. Handled by the ppage code.  rerr=%d\n", segment_id(cop->seg), rerr);
        return((rerr == 0) ? gop_success_status : gop_failure_status);
    }

//   hit_bytes = bpos - cop->boff;
//   hit_bytes = total_bytes - hit_bytes;
    if (new_size > 0) new_size++;

    ngot = bpos - cop->boff;

    log_printf(15, "seg=" XIDT " new_size=" XOT " child_size=" XOT "\n", segment_id(cop->seg),new_size, segment_size(cop->seg));
    //** Check for some input range errors
    if (((new_size > segment_size(cop->seg)) && (cop->rw_mode == CACHE_READ)) || (rerr != 0)) {
        log_printf(1, "ERROR  Read beyond EOF, bad range, or ppage_flush error!  rw_mode=%d rerr=%d new_size=" XOT " ssize=" XOT "\n", cop->rw_mode, rerr, new_size, segment_size(cop->seg));
        while ((r = tbx_stack_pop(&stack)) != NULL) {
            free(r);
        }
        return(gop_failure_status);
    }

    //** Make space the cache miss info
    if (cop->n_iov > 100) {
        tbx_type_malloc_clear(cache_missed, void *, cop->n_iov);
    } else {
        memset(cache_missed_table, 0, sizeof(cache_missed_table));
        cache_missed = cache_missed_table;
    }

    miss_time = 0;
    hit_bytes = 0;
    first_time = 1;
    mode = CACHE_NONBLOCK;
    top_cnt = cop->n_iov;
    bottom_cnt = 0;
    progress = 0;
    status = -1;
    hit_time = apr_time_now();
    while ((curr=(lio_cache_range_t *)tbx_stack_pop(&stack)) != NULL) {
        n_pages = CACHE_MAX_PAGES_RETURNED;
//mode = CACHE_DOBLOCK;

        log_printf(15, "processing range: lo=" XOT " hi=" XOT " progress=%d mode=%d\n", curr->lo, curr->hi, progress, mode);

        if (cop->rw_mode == CACHE_READ) {
            status = cache_read_pages_get(seg, cop->rw_hints, mode, curr->lo, curr->hi, &hi_got, page, iov, &n_pages, cop->buf, curr->boff, &(cache_missed[curr->iov_index]), cop->iov[curr->iov_index].len);
        } else if (cop->rw_mode == CACHE_WRITE) {
            status = cache_write_pages_get(seg, cop->rw_hints, mode, curr->lo, curr->hi, &hi_got, page, iov, &n_pages, cop->buf, curr->boff, &(cache_missed[curr->iov_index]), cop->iov[curr->iov_index].len);
        } else {
            log_printf(0, "ERROR invalid rw_mode!!!!!! rw_mode=%d\n", cop->rw_mode);
            err = gop_error_status;
        }

        if (hi_got > curr->hi) hi_got = curr->hi;  //** Returned value is based on page size so may need to truncate

        log_printf(15, "processing range: lo=" XOT " hi=" XOT " hi_got=" XOT " rw_mode=%d mode=%d skip_mode=%d n_pages=%d\n", curr->lo, curr->hi, hi_got, cop->rw_mode, mode, status, n_pages);

        if (status == 0) {  //** Got some data to process
            progress = 1;  //** Flag that progress was made

            if (n_pages > 0) {  //** Had to wait or fetch pages so we handle them
                pstart = page[0].p->offset;  //** Get the current starting offset

                //** Set the page transfer buffer size
                plen = hi_got - pstart + 1;
                tbx_tbuf_vec(&tb, plen, n_pages, iov);

                //** Determine the buffer / to page offset
                if (curr->lo >= pstart) {
                    poff = curr->lo - pstart;
                    bpos = curr->boff;
                } else {
                    poff = 0;
                    bpos = curr->boff + pstart - curr->lo;
                }

                //** and how much data to move
                len = plen - poff;
                blen = curr->hi - curr->lo + 1;
                if (blen > len) {
                    blen = len;
                }

                log_printf(15, "lo=" XOT " hi=" XOT " rw_mode=%d pstart=" XOT " poff=" XOT " bpos=" XOT " len=" XOT "\n",
                           curr->lo, curr->hi, cop->rw_mode, pstart, poff, bpos, blen);

                if (cop->rw_mode == CACHE_WRITE) {
                    tb_err += tbx_tbuf_copy(cop->buf, bpos, &tb, poff, blen, 1);
                    segment_lock(seg);  //** Tweak the size if needed
                    if (curr->hi > s->total_size) {
                        log_printf(0, "seg=" XIDT " total_size=" XOT " curr->hi=" XOT "\n", segment_id(cop->seg), s->total_size, curr->hi);
                        s->total_size = curr->hi + 1;
                    }
                    segment_unlock(seg);
                } else {
                    tb_err += tbx_tbuf_copy(&tb, poff, cop->buf, bpos, blen, 1);
                }

                log_printf(15, " tb_err=%d\n", tb_err);

                //** Release the pages
                len = s->page_size;
                cache_release_pages(n_pages, page, cop->rw_mode);
            } else if (first_time == 1) {
                hit_bytes += hi_got - curr->lo;  //** TRack the cahe hits
            }

            //** Add the top 1/2 of the old range back on the top of the stack if needed
            if (hi_got < curr->hi) {
                top_cnt++;
                tbx_stack_push(&stack, cache_new_range(hi_got+1, curr->hi, curr->boff + hi_got+1 - curr->lo, curr->iov_index));
            }
        } else {  //** Empty range so push it and the extra range on the bottom of the stack to retry later
            if (hi_got == curr->lo) { //** Got nothing
                bottom_cnt++;
                tbx_stack_move_to_bottom(&stack); //** Skipped range goes on the bottom of the stack
                log_printf(15, "got nothing inserting on bottom range lo=" XOT " hi=" XOT "\n", curr->lo, curr->hi);
                tbx_stack_insert_below(&stack, cache_new_range(curr->lo, curr->hi, curr->boff, curr->iov_index));
            } else {
                bottom_cnt++;
                tbx_stack_move_to_bottom(&stack); //** Skipped range goes on the bottom of the stack
                log_printf(15, "inserting on bottom range lo=" XOT " hi=" XOT "\n", curr->lo, hi_got);
                tbx_stack_insert_below(&stack, cache_new_range(curr->lo, hi_got, curr->boff, curr->iov_index));

                if (hi_got < curr->hi) {  //** The upper 1/2 has data so handle it 1st
                    log_printf(15, "inserting on top range lo=" XOT " hi=" XOT "\n", curr->lo, hi_got);
                    top_cnt++;
                    tbx_stack_push(&stack, cache_new_range(hi_got+1, curr->hi, curr->boff + hi_got+1 - curr->lo, curr->iov_index));  //** and the rest of the range on the top
                }
            }

        }

        //** If getting ready to cycle through again check if we need to switch modes
        top_cnt--;
        if (top_cnt <= 0) {
            log_printf(15, "completed cycle through list top=%d bottom=%d progress=%d\n", top_cnt, bottom_cnt, progress);
            if (first_time == 1) miss_time = apr_time_now();
            first_time = 0;
            top_cnt = tbx_stack_count(&stack);
            bottom_cnt = 0;
            if (progress == 0) mode = CACHE_DOBLOCK;
            progress = 0;
        }

        log_printf(15, "bottom lo=" XOT " hi=" XOT " progress=%d mode=%d top=%d bottom=%d\n", curr->lo, curr->hi, progress, mode, top_cnt, bottom_cnt);
        tbx_log_flush();
        free(curr);
    }

    hit_time = miss_time - hit_time;
    miss_time = apr_time_now() - miss_time;

    //** Let the caching aglorithm now of the 1st missed pages
    for (i=0; i < cop->n_iov; i++) {
        if (cache_missed[i] != NULL) {
            hi = cop->iov[i].offset + cop->iov[i].len - 1;
            s->c->fn.cache_update(s->c, seg, cop->rw_mode, cop->iov[i].offset, hi, cache_missed[i]);
        }
    }
    if (cache_missed != cache_missed_table) free(cache_missed);

    //** Update the counters
    segment_lock(seg);
    if (cop->rw_mode == CACHE_READ) {
        s->stats.user.read_count++;
        s->stats.user.read_bytes += ngot;
    } else {
        s->stats.user.write_count++;
        s->stats.user.write_bytes += ngot;

        //** Update the size if needed
        if ((s->total_size < new_size) && (cop->rw_mode == CACHE_WRITE)) s->total_size = new_size;

        if ((s->total_size < new_size) && (cop->rw_mode == CACHE_WRITE)) {
            log_printf(0, "seg=" XIDT " total_size=" XOT " new_size=" XOT "\n", segment_id(cop->seg), s->total_size, new_size);
            s->total_size = new_size;
        }
    }
    s->stats.hit_bytes += hit_bytes;
    s->stats.miss_bytes += total_bytes - hit_bytes;
    s->stats.hit_time += hit_time;
    s->stats.miss_time += miss_time;

    log_printf(15, "END size=" XOT " tb_err=%d\n", s->total_size, tb_err);

    segment_unlock(seg);

    if (tb_err > 0) {  //** We got some tbuf erros which mean hte underlying cache pages were bad
       err.op_status = OP_STATE_FAILURE;
    }

    return(err);
}


//***********************************************************************
// cache_read - Read from cache
//***********************************************************************

gop_op_generic_t *cache_read(lio_segment_t *seg, data_attr_t *da, lio_segment_rw_hints_t *rw_hints, int n_iov, ex_tbx_iovec_t *iov, tbx_tbuf_t *buffer, ex_off_t boff, int timeout)
{
    cache_rw_op_t *cop;
    lio_cache_lio_segment_t *s = (lio_cache_lio_segment_t *)seg->priv;

    tbx_type_malloc_clear(cop, cache_rw_op_t, 1);
    cop->seg = seg;
    cop->da = da;
    cop->rw_hints = rw_hints;
    cop->n_iov = n_iov;
    cop->iov = iov;
    cop->rw_mode = CACHE_READ;
    cop->boff = boff;
    cop->buf = buffer;

    return(gop_tp_op_new(s->tpc_unlimited, s->qname, cache_rw_func, (void *)cop, free, 1));
}


//***********************************************************************
// cache_write - Write to cache
//***********************************************************************

gop_op_generic_t *cache_write(lio_segment_t *seg, data_attr_t *da, lio_segment_rw_hints_t *rw_hints, int n_iov, ex_tbx_iovec_t *iov, tbx_tbuf_t *buffer, ex_off_t boff, int timeout)
{
    cache_rw_op_t *cop;
    lio_cache_lio_segment_t *s = (lio_cache_lio_segment_t *)seg->priv;

    tbx_type_malloc_clear(cop, cache_rw_op_t, 1);
    cop->seg = seg;
    cop->da = da;
    cop->rw_hints = rw_hints;
    cop->n_iov = n_iov;
    cop->iov = iov;
    cop->rw_mode = CACHE_WRITE;
    cop->boff = boff;
    cop->buf = buffer;

    return(gop_tp_op_new(s->tpc_unlimited, s->qname, cache_rw_func, (void *)cop, free, 1));
}


//*******************************************************************************
// cache_flush_range_gop - Flushes the given segment's byte range to disk
//*******************************************************************************

gop_op_status_t cache_flush_range_gop_func(void *arg, int id)
{
    cache_rw_op_t *cop = (cache_rw_op_t *)arg;
    lio_cache_lio_segment_t *s = (lio_cache_lio_segment_t *)cop->seg->priv;
    lio_page_handle_t page[CACHE_MAX_PAGES_RETURNED];
    ex_id_t sid;
    int status, n_pages, max_pages, total_pages;
    ex_off_t flush_id[3];
    tbx_stack_t stack;
    lio_cache_range_t *curr, *r;
    int progress;
    int mode, err;
    ex_off_t lo, hi, hi_got;
    double dt;
    apr_time_t now;


    err = OP_STATE_SUCCESS;

    tbx_stack_init(&stack);

    now = apr_time_now();
    log_printf(15, "COP seg=" XIDT " offset=" XOT " len=" XOT " size=" XOT "\n", segment_id(cop->seg), cop->iov_single.offset, cop->iov_single.len, segment_size(cop->seg));
    tbx_log_flush();

    total_pages = 0;
    lo = cop->iov_single.offset;
    if (cop->iov_single.len == -1) cop->iov_single.len = segment_size(cop->seg);  //** if len == -1 flush the whole file
    hi = lo + cop->iov_single.len - 1;

    if (hi == -1) {  //** segment_size == 0 so nothing to do
        goto finished;
    }

    //** Push myself on the flush stack
    segment_lock(cop->seg);
    flush_id[0] = lo;
    flush_id[1] = hi;
    flush_id[2] = tbx_atomic_inc(_flush_count);
    tbx_stack_push(s->flush_stack, flush_id);
    segment_unlock(cop->seg);

    max_pages = CACHE_MAX_PAGES_RETURNED;

    log_printf(5, "START seg=" XIDT " lo=" XOT " hi=" XOT " flush_id=" XOT "\n", segment_id(cop->seg), lo, hi, flush_id[2]);
    r = cache_new_range(lo, hi, 0, 0);
    tbx_stack_push(&stack, r);

    mode = CACHE_NONBLOCK;
    progress = 0;
    while ((curr=(lio_cache_range_t *)tbx_stack_pop(&stack)) != NULL) {
        log_printf(5, "cache_flush_range_gop_func: processing range: lo=" XOT " hi=" XOT " mode=%d\n", curr->lo, curr->hi, mode);
        n_pages = max_pages;
//mode = CACHE_DOBLOCK;  //**QWERTY
        status = cache_dirty_pages_get(cop->seg, mode, curr->lo, curr->hi, &hi_got, page, &n_pages);
        log_printf(15, "seg=" XIDT " processing range: lo=" XOT " hi=" XOT " hi_got=" XOT " mode=%d skip_mode=%d n_pages=%d\n", segment_id(cop->seg), curr->lo, curr->hi, hi_got, mode, status, n_pages);
        tbx_log_flush();

        if (status == 0) {  //** Got some data to process
            progress = 1;  //** Flag that progress was made
            total_pages += n_pages;
            err = (n_pages > 0) ? cache_rw_pages(cop->seg, cop->rw_hints, page, n_pages, CACHE_FLUSH, 1) : 0;
            err = (err == 0) ? OP_STATE_SUCCESS : OP_STATE_FAILURE;
            if (curr->hi > hi_got) tbx_stack_push(&stack, cache_new_range(hi_got+1, curr->hi, 0, 0));  //** and the rest of the range on the top
        } else if ( status == 1) {  //** Empty range so push it and the extra range on the stackon the stack to retry later
            if (hi_got == curr->lo) { //** Got nothing
                tbx_stack_move_to_bottom(&stack); //** Skipped range goes on the bottom of the stack
                log_printf(5, "got nothing inserting on bottom range lo=" XOT " hi=" XOT "\n", curr->lo, curr->hi);
                tbx_stack_insert_below(&stack, cache_new_range(curr->lo, curr->hi, 0, 0));
            } else {
                tbx_stack_move_to_bottom(&stack); //** Skipped range goes on the bottom of the stack
                log_printf(5, "inserting on bottom range lo=" XOT " hi=" XOT "\n", curr->lo, hi_got);
                tbx_stack_insert_below(&stack, cache_new_range(curr->lo, hi_got, 0, 0));

                if (hi_got < curr->hi) {  //** The upper 1/2 has data so handle it 1st
                    log_printf(5, "inserting on top range lo=" XOT " hi=" XOT "\n", curr->lo, hi_got);
                    tbx_stack_push(&stack, cache_new_range(hi_got+1, curr->hi, 0, 0));  //** and the rest of the range on the top
                }
            }
        } else {
            err = OP_STATE_FAILURE;
        }

        //** If getting ready to cycle through again check if we need to switch modes
        if (hi_got == hi) {
            if (progress == 0) mode = CACHE_DOBLOCK;
            progress = 0;
        }

        free(curr);
    }

    //** Wait for underlying flushed to complete
    flush_wait(cop->seg, flush_id);

    segment_lock(cop->seg);
    //** Remove myself from the stack
    tbx_stack_move_to_top(s->flush_stack);
    while ((ex_off_t *)tbx_stack_get_current_data(s->flush_stack) != flush_id) {
        tbx_stack_move_down(s->flush_stack);
    }
    tbx_stack_delete_current(s->flush_stack, 0, 0);
    log_printf(5, "END seg=" XIDT " lo=" XOT " hi=" XOT " flush_id=" XOT " AFTER WAIT\n", segment_id(cop->seg), lo, hi, flush_id[2]);

    //** Notify anyone else
    apr_thread_cond_broadcast(s->flush_cond);

    //** Now wait for any overlapping flushes that chould have started during my run to complete as well
    progress = flush_id[2];
    flush_id[2] = tbx_atomic_get(_flush_count) + 1;
    segment_unlock(cop->seg);

    flush_wait(cop->seg, flush_id);
    flush_id[2] = progress;

    //** Update that I'm finished
finished:
    sid = segment_id(cop->seg);
    segment_lock(cop->seg);
    s->flushing_count--;
    segment_unlock(cop->seg);

    dt = apr_time_now() - now;
    dt /= APR_USEC_PER_SEC;
    log_printf(15, "END seg=" XIDT " lo=" XOT " hi=" XOT " flush_id=" XOT " total_pages=%d status=%d dt=%lf\n", sid, lo, hi, flush_id[2], total_pages, err, dt);
    return((err == OP_STATE_SUCCESS) ? gop_success_status : gop_failure_status);
}

//***********************************************************************
// cache_flush_range_gop - Flush dirty pages to disk
//***********************************************************************

gop_op_generic_t *cache_flush_range_gop(lio_segment_t *seg, data_attr_t *da, ex_off_t lo, ex_off_t hi, int timeout)
{
    cache_rw_op_t *cop;
    lio_cache_lio_segment_t *s = (lio_cache_lio_segment_t *)seg->priv;

    tbx_type_malloc(cop, cache_rw_op_t, 1);
    cop->seg = seg;
    cop->da = da;
    cop->rw_hints = NULL;
    cop->iov = &(cop->iov_single);
    cop->iov_single.offset = lo;
    cop->iov_single.len = (hi == -1) ? -1 : hi - lo + 1;
    cop->rw_mode = CACHE_READ;
    cop->boff = 0;
    cop->buf = NULL;
    cop->timeout = timeout;


    segment_lock(seg);
    s->flushing_count++;
    segment_unlock(seg);

    return(gop_tp_op_new(s->tpc_unlimited, s->qname, cache_flush_range_gop_func, (void *)cop, free, 1));
}


//***********************************************************************
// segment_lio_cache_stats_get - Returns the cache stats for the segment
//***********************************************************************

lio_cache_stats_get_t segment_lio_cache_stats_get(lio_segment_t *seg)
{
    lio_cache_lio_segment_t *s = (lio_cache_lio_segment_t *)seg->priv;
    lio_cache_stats_get_t cs;

    segment_lock(seg);
    cs = s->stats;
    segment_unlock(seg);

    return(cs);
}

//***********************************************************************
// lio_cache_stats_get - Returns the overal cache stats
//   Returns the number of skipped segments due to locking
//***********************************************************************

int lio_cache_stats_get(lio_cache_t *c, lio_cache_stats_get_t *cs)
{
    lio_cache_lio_segment_t *s;
    tbx_list_iter_t it;
    lio_segment_t *seg2;
    ex_id_t *sid2;
    int i, n;

    cache_lock(c);

    *cs = c->stats;
    n = tbx_list_key_count(c->segments);
    it = tbx_list_iter_search(c->segments, NULL, 0);
    for (i=0; i<n; i++) {
        tbx_list_next(&it, (tbx_list_key_t **)&sid2, (tbx_list_data_t **)&seg2);

        if (seg2 != NULL) {
            if (apr_thread_mutex_trylock(seg2->lock) == APR_SUCCESS) {
                s = (lio_cache_lio_segment_t *)seg2->priv;
                cs->system.read_count += s->stats.system.read_count;
                cs->system.write_count += s->stats.system.write_count;
                cs->system.read_bytes += s->stats.system.read_bytes;
                cs->system.write_bytes += s->stats.system.write_bytes;

                cs->user.read_count += s->stats.user.read_count;
                cs->user.write_count += s->stats.user.write_count;
                cs->user.read_bytes += s->stats.user.read_bytes;
                cs->user.write_bytes += s->stats.user.write_bytes;

                cs->hit_time += s->stats.hit_time;
                cs->miss_time += s->stats.miss_time;
                cs->hit_bytes += s->stats.hit_bytes;
                cs->miss_bytes += s->stats.miss_bytes;
                cs->unused_bytes += s->stats.unused_bytes;
                segment_unlock(seg2);
            } else {
                n++;
            }
        }

    }

    cache_unlock(c);

    return(n);
}

//***********************************************************************
// lio_cache_stats_get_print - Prints the cache stats to a string
//***********************************************************************

int lio_cache_stats_get_print(lio_cache_stats_get_t *cs, char *buffer, int *used, int nmax)
{
    int n = 0;
    ex_off_t tsum1, tsum2, sum1, sum2;
    double d1, d2, d3, dt, drate;

    d1 = cs->system.read_bytes * 1.0 / (1024.0*1024.0*1024.0);
    n += tbx_append_printf(buffer, used, nmax, "System:: Read " XOT " bytes (%lf GiB) in " XOT " ops\n", cs->system.read_bytes, d1, cs->system.read_count);
    d1 = cs->system.write_bytes * 1.0 / (1024.0*1024.0*1024.0);
    n += tbx_append_printf(buffer, used, nmax, "System:: Write " XOT " bytes (%lf GiB) in " XOT " ops\n", cs->system.write_bytes, d1, cs->system.write_count);
    d1 = cs->user.read_bytes * 1.0 / (1024.0*1024.0*1024.0);
    n += tbx_append_printf(buffer, used, nmax, "User:: Read " XOT " bytes (%lf GiB) in " XOT " ops\n", cs->user.read_bytes, d1, cs->user.read_count);
    d1 = cs->user.write_bytes * 1.0 / (1024.0*1024.0*1024.0);
    n += tbx_append_printf(buffer, used, nmax, "User:: Write " XOT " bytes (%lf GiB) in " XOT " ops\n", cs->user.write_bytes, d1, cs->user.write_count);

    tsum1 = cs->system.read_bytes + cs->user.read_bytes;
    tsum2 = cs->system.read_count + cs->user.read_count;
    d1 = tsum1 * 1.0 / (1024.0*1024.0*1024.0);
    n += tbx_append_printf(buffer, used, nmax, "Total:: Read " XOT " bytes (%lf GiB) in " XOT " ops\n", tsum1, d1, tsum2);

    sum1 = cs->system.write_bytes + cs->user.write_bytes;
    sum2 = cs->system.write_count + cs->user.write_count;
    d1 = sum1 * 1.0 / (1024.0*1024.0*1024.0);
    n += tbx_append_printf(buffer, used, nmax, "Total:: Write " XOT " bytes (%lf GiB) in " XOT " ops\n", sum1, d1, sum2);

    d1 = cs->unused_bytes * 1.0 / (1024.0*1024.0*1024.0);
    n += tbx_append_printf(buffer, used, nmax, "Unused " XOT " bytes (%lf GiB)\n", cs->unused_bytes, d1);

    dt = cs->hit_time;
    dt = dt / (1.0*APR_USEC_PER_SEC);
    drate = cs->hit_bytes * 1.0 / (1024.0*1024.0*dt);
    d1 = cs->hit_bytes + cs->miss_bytes;
    d2 = (d1 > 0) ? (100.0*cs->hit_bytes) / d1 : 0;
    d3 = cs->hit_bytes * 1.0 / (1024.0*1024.0*1024.0);
    n += tbx_append_printf(buffer, used, nmax, "Hits: " XOT " bytes (%lf GiB) (%lf%% total) (%lf sec %lf MB/s)\n", cs->hit_bytes, d3, d2, dt, drate);

    dt = cs->miss_time;
    dt = dt / (1.0*APR_USEC_PER_SEC);
    drate = cs->miss_bytes * 1.0 / (1024.0*1024.0*dt);
    d2 = (d1 > 0) ? (100.0*cs->miss_bytes) / d1 : 0;
    d3 = cs->miss_bytes * 1.0 / (1024.0*1024.0*1024.0);
    n += tbx_append_printf(buffer, used, nmax, "Misses: " XOT " bytes (%lf GiB) (%lf%% total) (%lf sec %lf MB/s)\n", cs->miss_bytes, d3, d2, dt, drate);

    d3 = cs->dirty_bytes * 1.0 / (1024.0*1024.0*1024.0);
    n += tbx_append_printf(buffer, used, nmax, "Dirty: " XOT " bytes (%lf GiB)\n", cs->dirty_bytes, d3);

    return(n);
}

//***********************************************************************
// segcache_inspect - Issues integrity checks for the underlying segments
//***********************************************************************

gop_op_generic_t *segcache_inspect(lio_segment_t *seg, data_attr_t *da, tbx_log_fd_t *fd, int mode, ex_off_t bufsize, lio_inspect_args_t *args, int timeout)
{
    lio_cache_lio_segment_t *s = (lio_cache_lio_segment_t *)seg->priv;
    lio_ex3_inspect_command_t cmd = mode & INSPECT_COMMAND_BITS;

    if ((cmd != INSPECT_SOFT_ERRORS) && (cmd != INSPECT_HARD_ERRORS) && (cmd != INSPECT_WRITE_ERRORS)) {
        info_printf(fd, 1, XIDT ": Cache segment maps to child " XIDT "\n", segment_id(seg), segment_id(s->child_seg));
        info_printf(fd, 1, XIDT ": segment information: cache used_size=" XIDT "\n", segment_id(seg), segment_size(seg));

        //** Check the file size first
        if (segment_size(s->child_seg) < segment_size(seg)) {
            info_printf(fd, 1, XIDT ": ERROR Cache segment size(" XOT ") > child segment size(" XOT ")!\n", segment_id(seg), segment_size(seg), segment_size(s->child_seg));
            return(gop_dummy(gop_failure_status));
        }
    }

    return(segment_inspect(s->child_seg, da, fd, mode, bufsize, args, timeout));
}

//*******************************************************************************
// lio_cache_truncate_func - Function for truncating cache pages and actual segment
//*******************************************************************************

gop_op_status_t seglio_cache_truncate_func(void *arg, int id)
{
    lio_cache_truncate_op_t *cop = (lio_cache_truncate_op_t *)arg;
    lio_cache_lio_segment_t *s = (lio_cache_lio_segment_t *)cop->seg->priv;
    ex_off_t old_size;
    gop_op_generic_t *gop;
    int err1, err2;
    gop_op_status_t status;

    //** Adjust the size
    cache_lock(s->c);
    _cache_ppages_flush(cop->seg, cop->da); //** Flush any partial pages first

    segment_lock(cop->seg);
    old_size = s->total_size;
    s->total_size = cop->new_size;
    segment_unlock(cop->seg);
    cache_unlock(s->c);

    //** If shrinking the file need to destroy excess cache pages
    if (cop->new_size < old_size) {
        log_printf(5, "seg=" XIDT " dropping extra pages. inprogress=%d old=" XOT " new=" XOT "\n", segment_id(cop->seg), s->cache_check_in_progress, old_size, cop->new_size);
        //** Got to check if a dirty thread is trying to do an empty flush or a prefetch is running
        cache_lock(s->c);
        while (s->cache_check_in_progress != 0) {
            cache_unlock(s->c);
            log_printf(5, "seg=" XIDT " waiting for dirty flush/prefetch to complete. inprogress=%d\n", segment_id(cop->seg), s->cache_check_in_progress);
            usleep(10000);
            cache_lock(s->c);
        }
        cache_unlock(s->c);

        log_printf(5, "dropping extra pages. NOW\n");
        cache_page_drop(cop->seg, cop->new_size, XOT_MAX);
        log_printf(5, "dropping extra pages. FINISHED\n");
    }

    //** Do a cache flush
    gop = segment_flush(cop->seg, cop->da, 0, cop->new_size, cop->timeout);
    err1 = gop_waitall(gop);
    gop_free(gop, OP_DESTROY);

    //** Perform the truncate on the underlying segment
    gop = lio_segment_truncate(s->child_seg, cop->da, cop->new_size, cop->timeout);
    err2 = gop_waitall(gop);

    cache_lock(s->c);
    old_size = segment_size(s->child_seg);
    if (old_size > 0) {
        s->child_last_page = (old_size-1) / s->page_size;
        s->child_last_page *= s->page_size;
    } else {
        s->child_last_page = -1;
    }
    cache_unlock(s->c);

    gop_free(gop, OP_DESTROY);

    status = ((err1 == OP_STATE_SUCCESS) && (err2 == OP_STATE_SUCCESS)) ? gop_success_status : gop_failure_status;
    return(status);
}

//***********************************************************************
// seglio_cache_truncate - Truncates the underlying segment and flushes
//     cache as needed.
//***********************************************************************

gop_op_generic_t *seglio_cache_truncate(lio_segment_t *seg, data_attr_t *da, ex_off_t new_size, int timeout)
{
    lio_cache_lio_segment_t *s = (lio_cache_lio_segment_t *)seg->priv;
    lio_cache_truncate_op_t *cop;
    gop_op_generic_t *gop;


    if (new_size < 0) { //** Got a reserve call so just pass the call to the child to handle
        log_printf(5, "reserving space=" XOT "\n", new_size);
        return(lio_segment_truncate(s->child_seg, da, new_size, timeout));
    }

    tbx_type_malloc(cop, lio_cache_truncate_op_t, 1);
    cop->seg = seg;
    cop->da = da;
    cop->new_size = new_size;
    cop->timeout = timeout;

    gop = gop_tp_op_new(s->tpc_unlimited, NULL, seglio_cache_truncate_func, (void *)cop, free, 1);


    return(gop);
}

//*******************************************************************************
// segcache_clone_func - Does the clone function
//*******************************************************************************

gop_op_status_t segcache_clone_func(void *arg, int id)
{
    cache_clone_t *cop = (cache_clone_t *)arg;
    gop_op_status_t status;

    status = (gop_waitall(cop->gop) == OP_STATE_SUCCESS) ? gop_success_status : gop_failure_status;
    gop_free(cop->gop, OP_DESTROY);

    return(status);
}

//***********************************************************************
// segcache_signature - Generates the segment signature
//***********************************************************************

int segcache_signature(lio_segment_t *seg, char *buffer, int *used, int bufsize)
{
    lio_cache_lio_segment_t *s = (lio_cache_lio_segment_t *)seg->priv;

    tbx_append_printf(buffer, used, bufsize, "cache()\n");

    return(segment_signature(s->child_seg, buffer, used, bufsize));
}

//***********************************************************************
// segcache_clone - Clones a segment
//***********************************************************************

gop_op_generic_t *segcache_clone(lio_segment_t *seg, data_attr_t *da, lio_segment_t **clone_seg, int mode, void *arg, int timeout)
{
    lio_segment_t *clone;
    lio_cache_lio_segment_t *ss, *sd;
    cache_clone_t *cop;
    int use_existing = (*clone_seg != NULL) ? 1 : 0;

    ss = (lio_cache_lio_segment_t *)seg->priv;

    //** Sanity check the child first
    if (segment_size(ss->child_seg) < segment_size(seg)) {
        log_printf(0, XIDT ": ERROR Cache segment size(" XOT ") > child segment size(" XOT ")!\n", segment_id(seg), segment_size(seg), segment_size(ss->child_seg));
        return(gop_dummy(gop_failure_status));
    }

    //** Make the base segment
    if (use_existing == 0) *clone_seg = segment_cache_create(seg->ess);
    clone = *clone_seg;
    sd = (lio_cache_lio_segment_t *)clone->priv;

    //** Copy the header
    if ((seg->header.name != NULL) && (use_existing == 0)) clone->header.name = strdup(seg->header.name);

    //** Basic size info
    sd->total_size = ss->total_size;
    sd->page_size = ss->page_size;

    tbx_type_malloc(cop, cache_clone_t, 1);
    cop->sseg = seg;
    cop->dseg = clone;
    cop->gop = segment_clone(ss->child_seg, da, &(sd->child_seg), mode, arg, timeout);

    log_printf(5, "child_clone gid=%d\n", gop_id(cop->gop));
    return(gop_tp_op_new(ss->tpc_unlimited, NULL, segcache_clone_func, (void *)cop, free, 1));
}


//***********************************************************************
// seglin_size - Returns the segment size.
//***********************************************************************

ex_off_t segcache_size(lio_segment_t *seg)
{
    lio_cache_lio_segment_t *s = (lio_cache_lio_segment_t *)seg->priv;
    ex_off_t size;

    if (s->c) cache_lock(s->c);
    segment_lock(seg);
    size = (s->total_size > (s->ppage_max+1)) ? s->total_size : s->ppage_max + 1;
    log_printf(5, "seg=" XIDT " total_size=" XOT " ppage_max=" XOT " size=" XOT "\n", segment_id(seg), s->total_size, s->ppage_max, size);
    segment_unlock(seg);
    if (s->c) cache_unlock(s->c);
    return(size);
}

//***********************************************************************
// seglin_block_size - Returns the segment block size.
//***********************************************************************

ex_off_t segcache_block_size(lio_segment_t *seg)
{
    return(1);
}

//***********************************************************************
// segcache_remove - DECrements the ref counts for the segment which could
//     result in the data being removed.
//***********************************************************************

gop_op_generic_t *segcache_remove(lio_segment_t *seg, data_attr_t *da, int timeout)
{
    lio_cache_lio_segment_t *s = (lio_cache_lio_segment_t *)seg->priv;

    cache_page_drop(seg, 0, s->total_size + 1);
    return(segment_remove(s->child_seg, da, timeout));
}

//***********************************************************************
// segcache_serialize_text -Convert the segment to a text based format
//***********************************************************************

int segcache_serialize_text(lio_segment_t *seg, lio_exnode_exchange_t *exp)
{
    lio_cache_lio_segment_t *s = (lio_cache_lio_segment_t *)seg->priv;
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
    tbx_append_printf(segbuf, &sused, bufsize, "type=%s\n", SEGMENT_TYPE_CACHE);
    // FIXME
    //tbx_append_printf(segbuf, &sused, bufsize, "ref_count=%d\n", seg->ref_count);

    //** Basic size info
    tbx_append_printf(segbuf, &sused, bufsize, "used_size=" XOT "\n", s->total_size);

    //** And the child segment link
    tbx_append_printf(segbuf, &sused, bufsize, "segment=" XIDT "\n", segment_id(s->child_seg));

    //** Serialize the child as well
    child_exp = lio_exnode_exchange_create(EX_TEXT);
    segment_serialize(s->child_seg, child_exp);

    //** And merge everything together
    exnode_exchange_append(exp, child_exp);
    lio_exnode_exchange_destroy(child_exp);
    exnode_exchange_append_text(exp, segbuf);

    return(0);
}


//***********************************************************************
// segcache_serialize_proto -Convert the segment to a protocol buffer
//***********************************************************************

int segcache_serialize_proto(lio_segment_t *seg, lio_exnode_exchange_t *exp)
{
    return(-1);
}

//***********************************************************************
// segcache_serialize -Convert the segment to a more portable format
//***********************************************************************

int segcache_serialize(lio_segment_t *seg, lio_exnode_exchange_t *exp)
{
    if (exp->type == EX_TEXT) {
        return(segcache_serialize_text(seg, exp));
    } else if (exp->type == EX_PROTOCOL_BUFFERS) {
        return(segcache_serialize_proto(seg, exp));
    }

    return(-1);
}

//***********************************************************************
// segcache_deserialize_text -Read the text based segment
//***********************************************************************

int segcache_deserialize_text(lio_segment_t *seg, ex_id_t myid, lio_exnode_exchange_t *exp)
{
    lio_cache_lio_segment_t *s = (lio_cache_lio_segment_t *)seg->priv;
    int bufsize=1024;
    char seggrp[bufsize];
    char qname[512];
    tbx_inip_file_t *fd;
    ex_off_t n, child_size;
    ex_id_t id;
    int i;

    //** Parse the ini text
    fd = exp->text.fd;

    //** Make the segment section name
    snprintf(seggrp, bufsize, "segment-" XIDT, myid);

    //** Basic size info
    s->total_size = tbx_inip_get_integer(fd, seggrp, "used_size", -1);

    //** Load the child
    id = tbx_inip_get_integer(fd, seggrp, "segment", 0);
    if (id == 0) {
        log_printf(0, "ERROR missing child segment tag initial sid=" XIDT " myid=" XIDT "\n",segment_id(seg), myid);
        tbx_log_flush();
        return (-1);
    }

    s->child_seg = load_segment(seg->ess, id, exp);
    if (s->child_seg == NULL) {
        log_printf(0, "ERROR child_seg = NULL initial sid=" XIDT " myid=" XIDT " cid=" XIDT "\n",segment_id(seg), myid, id);
        tbx_log_flush();
        return(-2);
    }

    //** Remove my random ID from the segments table
    if (s->c) {
        cache_lock(s->c);
        log_printf(5, "CSEG-I Removing seg=" XIDT " nsegs=%d myid=" XIDT "\n", segment_id(seg), tbx_list_key_count(s->c->segments), myid);
        tbx_log_flush();
        tbx_list_remove(s->c->segments, &(segment_id(seg)), seg);
        s->c->fn.removing_segment(s->c, seg);
        cache_unlock(s->c);
    }

    //** Get the segment header info
    seg->header.id = myid;
    if (s->qname != NULL) free(s->qname);
    snprintf(qname, sizeof(qname), XIDT HP_HOSTPORT_SEPARATOR "1" HP_HOSTPORT_SEPARATOR "0" HP_HOSTPORT_SEPARATOR "0", seg->header.id);
    s->qname = strdup(qname);

    seg->header.type = SEGMENT_TYPE_CACHE;
    seg->header.name = tbx_inip_get_string(fd, seggrp, "name", "");

    //** Tweak the page size
    s->page_size = segment_block_size(s->child_seg);
    if (s->c != NULL) {
        if (s->page_size < s->c->default_page_size) {
            n = s->c->default_page_size / s->page_size;
            if ((s->c->default_page_size % s->page_size) > 0) n++;
            s->page_size = n * s->page_size;
        }
    }

    //** If total_size is -1 or child is smaller use the size from child
    child_size = segment_size(s->child_seg);

    //** Determine the child segment size so we don't have to call it
    //** on R/W and risk getting blocked due to child grow operations
    if (child_size > 0) {
        s->child_last_page = (child_size-1) / s->page_size;
        s->child_last_page *= s->page_size;
    } else {
        s->child_last_page = -1;  //** No pages
    }
    log_printf(5, "seg=" XIDT " Initial child_last_page=" XOT " child_size=" XOT " page_size=" XOT "\n", segment_id(seg), s->child_last_page, child_size, s->page_size);

    //** Make the partial pages table
    s->n_ppages = (s->c != NULL) ? s->c->n_ppages : 0;
    s->ppage_max = -1;
    if (s->n_ppages > 0) {
        tbx_type_malloc_clear(s->ppage, lio_cache_partial_page_t, s->n_ppages);
        tbx_type_malloc_clear(s->ppages_buffer, char, s->n_ppages*s->page_size);
        for (i=0; i<s->n_ppages; i++) {
            s->ppage[i].data = &(s->ppages_buffer[i*s->page_size]);
            s->ppage[i].range_stack = tbx_stack_new();
            tbx_stack_push(s->ppages_unused, &(s->ppage[i]));
        }
    }

    //** and reinsert myself with the new ID
    if (s->c != NULL) {
        cache_lock(s->c);
        log_printf(5, "CSEG Inserting seg=" XIDT " nsegs=%d\n", segment_id(seg), tbx_list_key_count(s->c->segments));
        tbx_log_flush();
        tbx_list_insert(s->c->segments, &(segment_id(seg)), seg);
        s->c->fn.adding_segment(s->c, seg);
        cache_unlock(s->c);
    }

    n = (s->c == NULL) ? 0 : s->c->default_page_size;
    log_printf(15, "segcache_deserialize_text: seg=" XIDT " page_size=" XOT " default=" XOT "\n", segment_id(seg), s->page_size, n);
    return(0);
}

//***********************************************************************
// segcache_deserialize_proto - Read the prot formatted segment
//***********************************************************************

int segcache_deserialize_proto(lio_segment_t *seg, ex_id_t id, lio_exnode_exchange_t *exp)
{
    return(-1);
}

//***********************************************************************
// segcache_deserialize -Convert from the portable to internal format
//***********************************************************************

int segcache_deserialize(lio_segment_t *seg, ex_id_t id, lio_exnode_exchange_t *exp)
{
    if (exp->type == EX_TEXT) {
        return(segcache_deserialize_text(seg, id, exp));
    } else if (exp->type == EX_PROTOCOL_BUFFERS) {
        return(segcache_deserialize_proto(seg, id, exp));
    }

    return(-1);
}

//***********************************************************************
// segcache_destroy - Destroys the cache segment
//***********************************************************************

void segcache_destroy(tbx_ref_t *ref)
{
    tbx_obj_t *obj = container_of(ref, tbx_obj_t, refcount);
    lio_segment_t *seg = container_of(obj, lio_segment_t, obj);
    lio_cache_lio_segment_t *s = (lio_cache_lio_segment_t *)seg->priv;
    gop_op_generic_t *gop;
    int i;

    //** Check if it's still in use
    log_printf(2, "segcache_destroy: seg->id=" XIDT " sptr=%p\n", segment_id(seg), seg);

    CACHE_PRINT;

    //** If s->c == NULL then we are just cloning the structure or serial/deserializing an exnode
    //** There should be no data loaded
    if (s->c != NULL) {
        //** Flush any ppages
        cache_lock(s->c);
        if (s->c != NULL) _cache_ppages_flush(seg, s->c->da);
        cache_unlock(s->c);

        //** Flush everything to backing store
        gop = segment_flush(seg, s->c->da, 0, s->total_size+1, s->c->timeout);
        gop_waitall(gop);
        gop_free(gop, OP_DESTROY);

        //** Remove it from the cache manager
        cache_lock(s->c);
        log_printf(5, "CSEG Removing seg=" XIDT " nsegs=%d\n", segment_id(seg), tbx_list_key_count(s->c->segments));
        tbx_log_flush();
        tbx_list_remove(s->c->segments, &(segment_id(seg)), seg);
        cache_unlock(s->c);

        //** Got to check if a dirty thread is trying to do an empty flush
        cache_lock(s->c);
        while (s->cache_check_in_progress != 0) {
            cache_unlock(s->c);
            log_printf(5, "seg=" XIDT " waiting for dirty flush/prefetch to complete\n", segment_id(seg));
            usleep(10000);
            cache_lock(s->c);
        }
        cache_unlock(s->c);

        //** And make sure all the flushing tasks are complete
        segment_lock(seg);
        while (s->flushing_count != 0) {
            log_printf(5, "seg=" XIDT " waiting for a flush to complete flushing_count=%d\n", segment_id(seg), s->flushing_count);
            segment_unlock(seg);
            usleep(10000);
            segment_lock(seg);
        }
        segment_unlock(seg);

        //** and drop the cache pages
        cache_page_drop(seg, 0, XOT_MAX);


        cache_lock(s->c);
        s->c->fn.removing_segment(s->c, seg);  //** Do the final remove
        cache_unlock(s->c);
    }

    //** Drop the flush args
    apr_thread_cond_destroy(s->flush_cond);
    tbx_stack_free(s->flush_stack, 0);

    CACHE_PRINT;

    log_printf(5, "seg=" XIDT " Starting segment destruction\n", segment_id(seg));

    //** Clean up the list
    tbx_list_destroy(s->pages);
    tbx_list_destroy(s->partial_pages);

    //** Destroy the child segment as well
    if (s->child_seg != NULL) {
        tbx_obj_put(&s->child_seg->obj);
    }

    //** and finally the misc stuff
    if (s->n_ppages > 0) {
        for (i=0; i<s->n_ppages; i++) {
            tbx_stack_free(s->ppage[i].range_stack, 1);
        }
        free(s->ppages_buffer);
        free(s->ppage);
    }

    tbx_stack_free(s->ppages_unused, 0);

    apr_thread_mutex_destroy(seg->lock);
    apr_thread_cond_destroy(seg->cond);
    apr_thread_cond_destroy(s->ppages_cond);
    apr_pool_destroy(seg->mpool);

    free(s->qname);
    free(s);

    ex_header_release(&(seg->header));

    free(seg);
}

//***********************************************************************
// segment_cache_create - Creates a cache segment
//***********************************************************************

lio_segment_t *segment_cache_create(void *arg)
{
    lio_service_manager_t *es = (lio_service_manager_t *)arg;
    lio_cache_lio_segment_t *s;
    lio_segment_t *seg;
    char qname[512];

    //** Make the space
    tbx_type_malloc_clear(seg, lio_segment_t, 1);
    tbx_type_malloc_clear(s, lio_cache_lio_segment_t, 1);
    tbx_obj_init(&seg->obj, (tbx_vtable_t *) &lio_cacheseg_vtable);
    assert_result(apr_pool_create(&(seg->mpool), NULL), APR_SUCCESS);
    apr_thread_mutex_create(&(seg->lock), APR_THREAD_MUTEX_DEFAULT, seg->mpool);
    apr_thread_cond_create(&(seg->cond), seg->mpool);
    apr_thread_cond_create(&(s->flush_cond), seg->mpool);
    apr_thread_cond_create(&(s->ppages_cond), seg->mpool);

    s->flush_stack = tbx_stack_new();
    s->tpc_unlimited = lio_lookup_service(es, ESS_RUNNING, ESS_TPC_CACHE);
    FATAL_UNLESS(s->tpc_unlimited != NULL);

    s->pages = tbx_list_create(0, &skiplist_compare_ex_off, NULL, NULL, NULL);

    s->ppages_unused = tbx_stack_new();
    s->partial_pages = tbx_list_create(0, &skiplist_compare_ex_off, NULL, NULL, NULL);

    s->c = lio_lookup_service(es, ESS_RUNNING, ESS_CACHE);
    if (s->c != NULL) s->c = cache_get_handle(s->c);
    s->page_size = 64*1024;
    s->n_ppages = 0;

    log_printf(2, "CACHE-PTR seg=" XIDT " s->c=%p\n", segment_id(seg), s->c);

    generate_ex_id(&(seg->header.id));
    seg->header.type = SEGMENT_TYPE_CACHE;

    snprintf(qname, sizeof(qname), XIDT HP_HOSTPORT_SEPARATOR "1" HP_HOSTPORT_SEPARATOR "0" HP_HOSTPORT_SEPARATOR "0", seg->header.id);
    s->qname = strdup(qname);

    seg->ess = es;
    seg->priv = s;
    if (s->c != NULL) { //** If no cache backend skip this  only used for temporary deseril/serial
        cache_lock(s->c);
        CACHE_PRINT;
        log_printf(5, "CSEG-I Inserting seg=" XIDT " nsegs=%d\n", segment_id(seg), tbx_list_key_count(s->c->segments));
        tbx_log_flush();
        tbx_list_insert(s->c->segments, &(segment_id(seg)), seg);
        s->c->fn.adding_segment(s->c, seg);
        CACHE_PRINT;
        cache_unlock(s->c);
    }

    return(seg);
}

//***********************************************************************
// segment_cache_load - Loads a cache segment from ini/ex3
//***********************************************************************

lio_segment_t *segment_cache_load(void *arg, ex_id_t id, lio_exnode_exchange_t *ex)
{
    lio_segment_t *seg = segment_cache_create(arg);
    if (segment_deserialize(seg, id, ex) != 0) {
        seg = NULL;
    }
    return(seg);
}

const lio_segment_vtable_t lio_cacheseg_vtable = {
        .base.name = "segment_cache",
        .base.free_fn = segcache_destroy,
        .read = cache_read,
        .write = cache_write,
        .inspect = segcache_inspect,
        .truncate = seglio_cache_truncate,
        .remove = segcache_remove,
        .flush = cache_flush_range_gop,
        .clone = segcache_clone,
        .signature = segcache_signature,
        .size = segcache_size,
        .block_size = segcache_block_size,
        .serialize = segcache_serialize,
        .deserialize = segcache_deserialize,
};

