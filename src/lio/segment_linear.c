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
// Routines for managing a linear segment
//***********************************************************************

#define _log_module_index 163

#include "ex3_abstract.h"
#include "ex3_system.h"
#include "interval_skiplist.h"
#include "ex3_compare.h"
#include "log.h"
#include "string_token.h"
#include "segment_linear.h"
#include "iniparse.h"
#include "random.h"
#include "append_printf.h"
#include "type_malloc.h"

typedef struct {
  data_block_t *data;    //** Data block
  ex_off_t cap_offset;  //** Starting location to use data in the cap
  ex_off_t seg_offset;  //** Offset withing the segment
  ex_off_t seg_end;     //** Ending location to use
  ex_off_t len;         //** Length
} seglin_slot_t;

typedef struct {
  opque_t *q;
  segment_t *seg;
  data_probe_t **probe;
  seglin_slot_t **block;
} seglin_check_t;

typedef struct {
  segment_t *seg;
  data_attr_t *da;
  ex_off_t new_size;
  int timeout;
} seglin_truncate_t;

typedef struct {
  segment_t *seg;
  data_attr_t *da;
  segment_rw_hints_t *rw_hints;
  ex_iovec_t  *iov;
  ex_off_t    boff;
  tbuffer_t  *buffer;
  int         n_iov;
  int timeout;
} seglin_rw_t;

typedef struct {
  segment_t *sseg;
  segment_t *dseg;
  data_attr_t *da;
  int mode;
  int timeout;
  int trunc;
} seglin_clone_t;

typedef struct {
  ex_off_t used_size;
  ex_off_t total_size;
  ex_off_t max_block_size;
  ex_off_t excess_block_size;
  rs_query_t *rsq;
  thread_pool_context_t *tpc;
  int n_rid_default;
  int hard_errors;
  interval_skiplist_t *isl;
  resource_service_fn_t *rs;
  data_service_fn_t *ds;
} seglin_priv_t;


//***********************************************************************
// seglin_grow - Expands a linear segment
//***********************************************************************

op_status_t _sl_grow(segment_t *seg, data_attr_t *da, ex_off_t new_size_arg, int timeout)
{
  int i, err;
  ex_off_t off, dsize;
  seglin_priv_t *s = (seglin_priv_t *)seg->priv;
  seglin_slot_t *b, *bexpand;
  interval_skiplist_iter_t it;
  op_generic_t *gop1, *gop2;
  opque_t *q;
  int n_blocks;
  ex_off_t lo, hi, bex_end, bex_len, bstart, new_size;
  rs_request_t *req_list;
  data_cap_set_t **cap_list;
  seglin_slot_t **block;
  tbuffer_t tbuf;
  op_status_t status;
  char c[1];
  
  new_size = new_size_arg;
  if (new_size_arg < 0) {  //** Got a space reservation call
     new_size = - new_size_arg;
     if (new_size < s->total_size) return(op_success_status);  //** Already have that much space reserved
  }
  
  //** Make the space
  lo = s->total_size;
  n_blocks = (new_size - s->total_size) / s->max_block_size + 1;
  type_malloc_clear(req_list, rs_request_t, n_blocks);
  type_malloc_clear(cap_list, data_cap_set_t *, n_blocks);
  type_malloc_clear(block, seglin_slot_t *, n_blocks);

log_printf(15, "_sl_grow: sid=" XIDT " currused=" XOT " currmax=" XOT " newmax=" XOT "\n", segment_id(seg), s->used_size, s->total_size, new_size);

  gop1 = NULL; gop2 = NULL; bexpand = NULL;

  //** Find the last block and see if it needs expanding
  bex_end = bex_len = 0;
  if (s->total_size > 0) {
     lo = s->total_size-1;
     hi = s->total_size;
     it = iter_search_interval_skiplist(s->isl, (skiplist_key_t *)&lo, (skiplist_key_t *)&hi);
     b = (seglin_slot_t *)next_interval_skiplist(&it);
     if (b->len < s->max_block_size) {
        dsize = new_size - b->seg_offset;
        if (dsize > s->max_block_size) dsize = s->max_block_size;
        gop1 = ds_truncate(b->data->ds, da, ds_get_cap(b->data->ds, b->data->cap, DS_CAP_MANAGE), dsize, timeout);
log_printf(15, "_sl_grow: sid=" XIDT " gid=%d growing existing block seg_off=" XOT " currlen=" XOT " newlen=" XOT "\n", segment_id(seg), gop_id(gop1), b->seg_offset, b->len, dsize);
        bexpand = b;  //** Keep track of it for later when updating
        bex_len = dsize;
        bex_end = b->seg_offset + dsize - 1;
        lo = bex_end + 1;
     } else {
        lo = b->seg_end + 1;
     }
  }

  //** Create the additional caps and commands
  n_blocks = 0;
  for (off=lo; off<new_size; off = off + s->max_block_size) {
     type_malloc_clear(b, seglin_slot_t, 1);
     b->data = data_block_create(s->ds);
     b->cap_offset = 0;
     b->seg_offset = off;
     b->len = s->max_block_size;
     dsize = off + s->max_block_size;
     if (dsize > new_size) b->len = new_size - off;
     b->data->size = b->len;
     b->data->max_size = b->len;
     b->seg_end = b->seg_offset + b->len -1;

log_printf(15, "_sl_grow: sid=" XIDT " off=" XOT " b->len=" XOT "\n", segment_id(seg), off, b->len);

     block[n_blocks] = b;
     req_list[n_blocks].rid_index = n_blocks%s->n_rid_default;
     req_list[n_blocks].size = b->len;
     cap_list[n_blocks] = b->data->cap;
     n_blocks++;
  }

log_printf(15, "_sl_grow: sid=" XIDT " n_blocks=%d max_block_size=" XOT "\n", segment_id(seg), n_blocks, s->max_block_size);
  //** Generate the query
  if (n_blocks > 0) {
     i = (s->n_rid_default > n_blocks) ? n_blocks : s->n_rid_default;
     gop2 = rs_data_request(s->rs, da, s->rsq, cap_list, req_list, n_blocks, NULL, 0, i, 0, timeout);
  }

log_printf(15, "_sl_grow: sid=" XIDT " before exec gop2=%p\n", segment_id(seg), gop2);
flush_log();

  //** Ececute it(them)
  if (gop1 == NULL) {
     err = gop_waitall(gop2);
     gop_free(gop2, OP_DESTROY);
  } else {
    if (gop2 == NULL) {
       err = gop_waitall(gop1); 
       gop_free(gop1, OP_DESTROY);
    } else {
       q = new_opque();
       opque_add(q, gop1);
       opque_add(q, gop2);
       err = opque_waitall(q);
       opque_free(q, OP_DESTROY);
    }
  }
log_printf(15, "_sl_grow: sid=" XIDT " after exec err=%d\n", segment_id(seg), err);

  //** and update the table
  if (err == OP_STATE_SUCCESS) {
     q = new_opque();

     c[0] = 0;
     tbuffer_single(&tbuf, 1, c);

     if (bexpand != NULL) { //** Update the expanded block
        err = remove_interval_skiplist(s->isl, (skiplist_key_t *)&(bexpand->seg_offset), (skiplist_key_t *)&(bexpand->seg_end), (skiplist_data_t *)bexpand);
        bexpand->len = bex_len;
        bexpand->seg_end = bex_end;
        bexpand->data->size = bex_len;
        bexpand->data->max_size = bex_len;
        err = insert_interval_skiplist(s->isl, (skiplist_key_t *)&(bexpand->seg_offset), (skiplist_key_t *)&(bexpand->seg_end), (skiplist_data_t *)bexpand);

        bstart = bexpand->cap_offset + bexpand->data->max_size - 1;
        gop1 = ds_write(bexpand->data->ds, da, ds_get_cap(bexpand->data->ds, bexpand->data->cap, DS_CAP_WRITE), bstart, &tbuf, 0, 1, timeout);
        opque_add(q, gop1);
     }

     for (i=0; i<n_blocks; i++) {  //** Add the new blocks
        b = block[i];
        b->data->rid_key = strdup(req_list[i].rid_key);
        atomic_inc(b->data->ref_count);
        err = insert_interval_skiplist(s->isl, (skiplist_key_t *)&(b->seg_offset), (skiplist_key_t *)&(b->seg_end), (skiplist_data_t *)b);

        bstart = b->cap_offset + b->data->max_size - 1;
        gop1 = ds_write(b->data->ds, da, ds_get_cap(b->data->ds, b->data->cap, DS_CAP_WRITE), bstart, &tbuf, 0, 1, timeout);
        opque_add(q, gop1);
      }

      err = opque_waitall(q);

      if (opque_tasks_failed(q) != 0) {
         log_printf(15, "ERROR with end of buffer write\n");
      }

      opque_free(q, OP_DESTROY);

      s->total_size = new_size;
  } else {
    for (i=0; i<n_blocks; i++) {
       b = block[i];
       atomic_dec(b->data->ref_count);
       data_block_destroy(b->data);
       free(b);
    }
  }

log_printf(15, "_sl_grow: sid=" XIDT " END used=" XOT " max=" XOT "\n", segment_id(seg), s->used_size, s->total_size);

  //** And cleanup
  free(block);
  free(req_list);
  free(cap_list);

  status = (err == OP_STATE_SUCCESS) ? op_success_status : op_failure_status;  

  return(status);
}

//***********************************************************************
// seglin_shrink - Shrinks a linear segment
//***********************************************************************

op_status_t _sl_shrink(segment_t *seg, data_attr_t *da, ex_off_t new_size, int timeout)
{
  seglin_priv_t *s = (seglin_priv_t *)seg->priv;
  op_generic_t *gop;
  interval_skiplist_iter_t it;
  seglin_slot_t *b;
  opque_t *q = NULL;
  ex_off_t lo, hi, dsize;
  Stack_t *stack;
  seglin_slot_t *start_b;
  op_status_t status;
  int i, err1;

  stack = new_stack();

  lo = new_size;
  hi = s->total_size;
  log_printf(15, "_sl_shrink: sid=" XIDT " total_size=" XOT " new_size=" XOT "\n", segment_id(seg), hi, lo);


  it = iter_search_interval_skiplist(s->isl, (skiplist_key_t *)&lo, (skiplist_key_t *)&hi);
  b = (seglin_slot_t *)next_interval_skiplist(&it);

  //** The 1st block maybe a partial removal
  dsize = new_size - b->seg_offset;
  if (dsize == 0) {  //** Full removal
     log_printf(15, "_sl_shrink: sid=" XIDT " removing bid=" XIDT " seg_off=" XOT "\n", segment_id(seg), data_block_id(b->data), b->seg_offset);
     gop = ds_remove(b->data->ds, da, ds_get_cap(b->data->ds, b->data->cap, DS_CAP_MANAGE), timeout);
     push(stack, (void *)b);
     start_b = NULL;
  } else {
     log_printf(15, "_sl_shrink: sid=" XIDT " shrinking bid=" XIDT " seg_off=" XOT " to=" XOT "\n", segment_id(seg), data_block_id(b->data), b->seg_offset, dsize);
     gop = ds_truncate(b->data->ds, da, ds_get_cap(b->data->ds, b->data->cap, DS_CAP_MANAGE), dsize, timeout);
     start_b = b;
  }

  //** Set up for the rest of the blocks
  b = (seglin_slot_t *)next_interval_skiplist(&it);
  if (b != NULL) {
     q = new_opque();
     opque_add(q, gop);
  }
  while (b != NULL) {
    log_printf(15, "_sl_shrink: sid=" XIDT " removing bid=" XIDT " seg_off=" XOT "\n", segment_id(seg), data_block_id(b->data), b->seg_offset);
    push(stack, (void *)b);
    gop = ds_remove(b->data->ds, da, ds_get_cap(b->data->ds, b->data->cap, DS_CAP_MANAGE), timeout);
    opque_add(q, gop);

    b = (seglin_slot_t *)next_interval_skiplist(&it);
  }

  //** Do the removal
  if (q != NULL) gop = opque_get_gop(q);
  err1 = gop_waitall(gop);
  gop_free(gop, OP_DESTROY);

  //** And now clean up
  while ((b = (seglin_slot_t *)pop(stack)) != NULL) {
     i = remove_interval_skiplist(s->isl, &(b->seg_offset), &(b->seg_end), b);
     log_printf(15, "_sl_shrink: sid=" XIDT " removing from interval bid=" XIDT " seg_off=" XOT " remove_isl=%d\n", segment_id(seg), data_block_id(b->data), b->seg_offset, i);
     atomic_dec(b->data->ref_count);
     data_block_destroy(b->data);
     free(b);
  }

  free_stack(stack, 0);

  //** If needed tweak the initial block
  if (start_b != NULL) {
     b = start_b;
     remove_interval_skiplist(s->isl, &(b->seg_offset), &(b->seg_end), b);
     b->seg_end = b->seg_offset + dsize - 1;
     b->len = dsize;
     insert_interval_skiplist(s->isl, (skiplist_key_t *)&(b->seg_offset), (skiplist_key_t *)&(b->seg_end), (skiplist_data_t *)b);
  }

  //** Update the size
  s->total_size = new_size;
  s->used_size = new_size;

  status = (err1 == OP_STATE_SUCCESS) ? op_success_status : op_failure_status;
  return(status);
}

//***********************************************************************
// _sl_truncate - Performs the truncate
//***********************************************************************

op_status_t _sl_truncate(segment_t *seg, data_attr_t *da, ex_off_t new_size, int timeout)
{
  seglin_priv_t *s = (seglin_priv_t *)seg->priv;
  op_status_t err = op_success_status;

  if (new_size < 0) {  //** Reserve space
     err = _sl_grow(seg, da, new_size, timeout); 
  } else if (s->total_size > new_size) {
     err = _sl_shrink(seg, da, new_size, timeout); 
  } else if (s->total_size < new_size) {
     err = _sl_grow(seg, da, new_size, timeout); 
  }

  return(err);
}

//***********************************************************************
//  seglin_truncate_func - Does the actual segment truncat operations
//***********************************************************************

op_status_t seglin_truncate_func(void *arg, int id)
{
  seglin_truncate_t *st = (seglin_truncate_t *)arg;
  op_status_t err;

  segment_lock(st->seg);
  err = _sl_truncate(st->seg, st->da, st->new_size, st->timeout);
  segment_unlock(st->seg);

  return(err);
}

//***********************************************************************
// segment_linear_truncate - Expands or contracts a linear segment
//***********************************************************************

op_generic_t *seglin_truncate(segment_t *seg, data_attr_t *da, ex_off_t new_size, int timeout)
{
  seglin_priv_t *s = (seglin_priv_t *)seg->priv;
  
  seglin_truncate_t *st;

  type_malloc_clear(st, seglin_truncate_t, 1);

  st->seg = seg;
  st->new_size = new_size;
  st->timeout = timeout;
  st->da = da;

  return(new_thread_pool_op(s->tpc, NULL, seglin_truncate_func, (void *)st, free, 1));  
}

//***********************************************************************
// seglin_read - Read from a linear segment 
//***********************************************************************

op_status_t seglin_read_func(void *arg, int id)
{
  seglin_rw_t *sr = (seglin_rw_t *)arg;
  seglin_priv_t *s = (seglin_priv_t *)sr->seg->priv;
  op_generic_t *gop;
  opque_t *q;
  seglin_slot_t *b;
  interval_skiplist_iter_t it;
  ex_off_t lo, hi, start, end, blen, bpos;
  op_status_t err;
  int i;

  segment_lock(sr->seg);

  gop = NULL;
  q = NULL;
  bpos = sr->boff;

  for (i=0; i<sr->n_iov; i++) {
    lo = sr->iov[i].offset;
    hi = lo + sr->iov[i].len - 1;
    it = iter_search_interval_skiplist(s->isl, (skiplist_key_t *)&lo, (skiplist_key_t *)&hi);
    b = (seglin_slot_t *)next_interval_skiplist(&it);

log_printf(15, "seglin_read_op: START sid=" XIDT " lo=" XOT " hi=" XOT " b=%p used_size=" XOT " total_size=" XOT "\n", segment_id(sr->seg), lo, hi, b, s->used_size, s->total_size);

    while (b != NULL) {
      start = (lo <= b->seg_offset) ? 0 : (lo - b->seg_offset);
      end = (hi >= b->seg_end) ? b->len-1 : (hi - b->seg_offset);
      blen = end - start + 1;
      start = start + b->cap_offset;

log_printf(15, "seglin_read: sid=" XIDT " bid=" XIDT " soff=" XOT " bpos=" XOT " blen=" XOT "\n", segment_id(sr->seg), 
     data_block_id(b->data), start, bpos, blen); flush_log();

      gop = ds_read(b->data->ds, sr->da, ds_get_cap(b->data->ds, b->data->cap, DS_CAP_READ), start, sr->buffer, bpos, blen, sr->timeout);

      bpos = bpos + blen;

      b = (seglin_slot_t *)next_interval_skiplist(&it);
      if ((b != NULL) && (q== NULL)) q = new_opque();
      if (q != NULL) opque_add(q, gop);
    }

if (gop == NULL) {
log_printf(0, " seg=" XIDT " read beyond EOF  curr_used_size=" XOT " curr_total_size=" XOT " requested lo=" XOT " hi=" XOT "\n", 
   segment_id(sr->seg), s->used_size, s->total_size, lo, hi);
}
  }
  segment_unlock(sr->seg);

  if (q == NULL) {
     if (gop == NULL) {
        log_printf(0, "ERROR Nothing to do so return an error\n");
        gop = gop_dummy(op_failure_status);
     }
  } else {
    gop = opque_get_gop(q);
  }

  i = gop_waitall(gop);

  gop_free(gop, OP_DESTROY);

  if (i == 0) {
     err = op_success_status;
  } else {
     err = op_success_status;
     segment_lock(sr->seg);
     s->hard_errors++;
     segment_unlock(sr->seg);
  }

  return(err);
}

//***********************************************************************
// seglin_read - Read from a linear segment 
//***********************************************************************

op_generic_t *seglin_read(segment_t *seg, data_attr_t *da, segment_rw_hints_t *rw_hints, int n_iov, ex_iovec_t *iov, tbuffer_t *buffer, ex_off_t boff, int timeout)
{
  seglin_priv_t *s = (seglin_priv_t *)seg->priv;
  seglin_rw_t *sw;
  op_generic_t *gop;
 

  type_malloc(sw, seglin_rw_t, 1);
  sw->seg = seg;
  sw->da = da;
  sw->rw_hints = rw_hints;
  sw->n_iov = n_iov;
  sw->iov = iov;
  sw->boff = boff;
  sw->buffer = buffer;
  sw->timeout = timeout;
  gop = new_thread_pool_op(s->tpc, NULL, seglin_read_func, (void *)sw, free, 1);

  return(gop);
}

//***********************************************************************
// seglin_write_op - Writes to a linear segment 
//***********************************************************************

op_generic_t *seglin_write_op(segment_t *seg, data_attr_t *da, segment_rw_hints_t *rw_hints, int n_iov, ex_iovec_t *iov, tbuffer_t *buffer, ex_off_t boff, int timeout)
{
  seglin_priv_t *s = (seglin_priv_t *)seg->priv;
  op_generic_t *gop;
  opque_t *q;
  seglin_slot_t *b;
  interval_skiplist_iter_t it;
  ex_off_t lo, hi, start, end, blen, bpos;
  int i;

  segment_lock(seg);

  gop = NULL;
  q = NULL;
  bpos = boff;

  for (i=0; i<n_iov; i++) {
    lo = iov[i].offset;
    hi = lo + iov[i].len - 1;
    it = iter_search_interval_skiplist(s->isl, (skiplist_key_t *)&lo, (skiplist_key_t *)&hi);
    b = (seglin_slot_t *)next_interval_skiplist(&it);
log_printf(15, "seglin_write_op: START sid=" XIDT " i=%d n_iov=%d lo=" XOT " hi=" XOT " b=%p\n", segment_id(seg), i, n_iov, lo, hi, b);

    while (b != NULL) {
      start = (lo <= b->seg_offset) ? 0 : (lo - b->seg_offset);
      end = (hi >= b->seg_end) ? b->len-1 : (hi - b->seg_offset);
      blen = end - start + 1;
      start = start + b->cap_offset;

log_printf(15, "seglin_write_op: sid=" XIDT " bid=" XIDT " soff=" XOT " bpos=" XOT " blen=" XOT " seg_off=" XOT " seg_len=" XOT " seg_end=" XOT "\n", segment_id(seg), 
     data_block_id(b->data), start, bpos, blen, b->seg_offset, b->len, b->seg_end); flush_log();

      gop = ds_write(b->data->ds, da, ds_get_cap(b->data->ds, b->data->cap, DS_CAP_WRITE), start, buffer, bpos, blen, timeout);

      bpos = bpos + blen;

      b = (seglin_slot_t *)next_interval_skiplist(&it);
      if ((b != NULL) && (q== NULL)) q = new_opque();
      if (q != NULL) opque_add(q, gop);
    }
log_printf(15, "seglin_write_op: END sid=" XIDT " i=%d\n", segment_id(seg), i);

  }
  segment_unlock(seg);
  
  if (q == NULL) {
     if (gop == NULL) {
        log_printf(0, "ERROR Nothing to do\n");
        gop = gop_dummy(op_failure_status);
     }
  } else {
    gop = opque_get_gop(q);
  }
  return(gop);  
}


//***********************************************************************
//  seglin_write_func - Expands and writes to the segment
//***********************************************************************

op_status_t seglin_write_func(void *arg, int id)
{
  seglin_rw_t *sw = (seglin_rw_t *)arg;
  seglin_priv_t *s = (seglin_priv_t *)sw->seg->priv;
  int i, err;
  op_status_t status;
  ex_off_t new_size;
  ex_off_t pos, maxpos;

  //** Grow the segment 1st
log_printf(15, "seglin_write_func: sid=" XIDT " n_iov=%d off[0]=" XOT " len[0]=" XOT " max_size=" XOT " used_size=" XOT "\n",
     segment_id(sw->seg), sw->n_iov, sw->iov[0].offset, sw->iov[0].len, s->total_size, s->used_size);

  //** Find the max extent;
  maxpos = 0;
  pos = 0;
  for (i=0; i<sw->n_iov; i++) {
    pos = sw->iov[i].offset + sw->iov[i].len - 1;
    if (pos > maxpos) maxpos = pos;
//log_printf(15, "i=%d off=" XOT " len=" XOT " pos=" XOT " maxpos=" XOT "\n", i, sw->iov[i].offset, sw->iov[i].len, pos, maxpos);
  }


  if (maxpos >= s->total_size) { //** Need to grow it first
     segment_lock(sw->seg);
     new_size = maxpos + s->excess_block_size;  
     if (s->total_size < new_size) {  //** Check again within the lock
log_printf(15, " seg=" XIDT " GROWING  curr_used_size=" XOT " curr_total_size=" XOT " new_size=" XOT " requested maxpos=" XOT "\n", 
   segment_id(sw->seg), s->used_size, s->total_size, new_size, maxpos);
        status = _sl_truncate(sw->seg, sw->da, new_size, sw->timeout);
        err = status.op_status;
     }
     segment_unlock(sw->seg);
  }

  //** Now do the actual write
log_printf(15, "seglin_write_func: Before exec\n");
  err = gop_sync_exec(seglin_write_op(sw->seg, sw->da, sw->rw_hints, sw->n_iov, sw->iov, sw->buffer, sw->boff, sw->timeout));
log_printf(15, "seglin_write_func: After exec err=%d\n", err);

  segment_lock(sw->seg);
log_printf(15, "seglin_write_func: oldused=" XOT " maxpos=" XOT "\n", s->used_size, maxpos);

  if (s->used_size <= maxpos) s->used_size = maxpos+1; 

  if (err == OP_STATE_SUCCESS) {
     status = op_success_status;
  } else {
     status = op_failure_status;
     s->hard_errors++;
  }

  segment_unlock(sw->seg);

  return(status);
}

//***********************************************************************
// seglin_write - Performs a segment write operation
//***********************************************************************

op_generic_t *seglin_write(segment_t *seg, data_attr_t *da, segment_rw_hints_t *rw_hints, int n_iov, ex_iovec_t *iov, tbuffer_t *buffer, ex_off_t boff, int timeout)
{
  seglin_priv_t *s = (seglin_priv_t *)seg->priv;
  seglin_rw_t *sw;
  op_generic_t *gop;
 

  type_malloc(sw, seglin_rw_t, 1);
  sw->seg = seg;
  sw->da = da;
  sw->rw_hints = rw_hints;
  sw->n_iov = n_iov;
  sw->iov = iov;
  sw->boff = boff;
  sw->buffer = buffer;
  sw->timeout = timeout;
  gop = new_thread_pool_op(s->tpc, NULL, seglin_write_func, (void *)sw, free, 1);

  return(gop);
}

//***********************************************************************
// _seglin_probe_cb - Validates all the segment probes
//***********************************************************************

void _seglin_probe_cb(void *arg, int state)
{
  seglin_check_t *sp = (seglin_check_t *)arg;
  segment_t *seg = sp->seg;
  seglin_priv_t *s = (seglin_priv_t *)seg->priv;
  seglin_slot_t *b = NULL;
  data_probe_t *p;
  ds_int_t psize, seg_size;
  int i;

  if (state == OP_STATE_SUCCESS) {
    opque_set_status(sp->q, op_success_status);        //** Default to success
    for (i=0; i<interval_skiplist_count(s->isl); i++) {
       b = sp->block[i];
       p = sp->probe[i];

       //** Verify the max_size >= cap_offset+len
       ds_get_probe(b->data->ds, p, DS_PROBE_MAX_SIZE, &psize, sizeof(psize));
       seg_size = b->cap_offset + b->len;
       if (psize < seg_size) {
          log_printf(10, "_seglin_probe_cb: allocation too small! i=%d\n", i);
          opque_set_status(sp->q, op_failure_status);
       }
    }
  } else {
    opque_set_status(sp->q, op_failure_status);
  }

  //*** Clean up ***
  for (i=0; i<interval_skiplist_count(s->isl); i++) {
    ds_probe_destroy(b->data->ds, sp->probe[i]);
  }
  free(sp->probe);
  free(sp->block);
  free(sp);
  //** NOTE: the CB is freed in the normal opque_free call **

  return;

}

//***********************************************************************
// seglin_remove - DECrements the ref counts for the segment which could
//     result in the data being removed.
//***********************************************************************

op_generic_t *seglin_remove(segment_t *seg, data_attr_t *da, int timeout)
{
  seglin_priv_t *s = (seglin_priv_t *)seg->priv;
  op_generic_t *gop;
  opque_t *q;
  seglin_slot_t *b;
  interval_skiplist_iter_t it;
  int i, n;

  q = new_opque();

  segment_lock(seg);
  n = interval_skiplist_count(s->isl);
  it = iter_search_interval_skiplist(s->isl, (skiplist_key_t *)NULL, (skiplist_key_t *)NULL);
  for (i=0; i<n; i++) {
     b = (seglin_slot_t *)next_interval_skiplist(&it);
     gop = ds_remove(b->data->ds, da, ds_get_cap(b->data->ds, b->data->cap, DS_CAP_MANAGE), timeout);
     opque_add(q, gop);
  }
  segment_unlock(seg);

  return(opque_get_gop(q));
}

//***********************************************************************
// seglin_inspect_func - Checks that all the segments are available and they are the right size
//***********************************************************************

op_generic_t *seglin_inspect_op(segment_t *seg, data_attr_t *da, info_fd_t *fd, int mode, ex_off_t buffer_size, inspect_args_t *args, int timeout)
{
  seglin_priv_t *s = (seglin_priv_t *)seg->priv;
  op_generic_t *gop;
  opque_t *q;
  seglin_slot_t *b;
  interval_skiplist_iter_t it;
  callback_t *cb;
  seglin_check_t *sp;
  int i;

  //** Make and assemble the cb
  type_malloc_clear(cb, callback_t, 1);
  type_malloc_clear(sp, seglin_check_t, 1);
  type_malloc_clear(sp->block, seglin_slot_t *, interval_skiplist_count(s->isl));
  type_malloc_clear(sp->probe, data_probe_t *, interval_skiplist_count(s->isl));

  q = new_opque();
  callback_set(cb, _seglin_probe_cb, sp);
  opque_callback_append(q, cb);

  sp->q = q;
  sp->seg = seg;

  it = iter_search_interval_skiplist(s->isl, (skiplist_key_t *)NULL, (skiplist_key_t *)NULL);
  i = 0;
  for (b = (seglin_slot_t *)next_interval_skiplist(&it); b != NULL; b = (seglin_slot_t *)next_interval_skiplist(&it)) {
    sp->block[i] = b;
    sp->probe[i] = ds_probe_create(b->data->ds);

    gop = ds_probe(b->data->ds, da, ds_get_cap(b->data->ds, b->data->cap, DS_CAP_MANAGE), sp->probe[i], timeout);

    opque_add(q, gop);
    i++;
  }


  return(opque_get_gop(q));
}

//***********************************************************************
//  seglin_truncate_func - Does the actual segment truncat operations
//***********************************************************************

op_generic_t *seglin_inspect(segment_t *seg, data_attr_t *da, info_fd_t *fd, int mode, ex_off_t bufsize, inspect_args_t *args, int timeout)
{
  seglin_priv_t *s = (seglin_priv_t *)seg->priv;
  op_generic_t *gop;
  op_status_t err;

  gop = NULL;

  switch (mode) {
    case (INSPECT_QUICK_CHECK):
    case (INSPECT_SCAN_CHECK):
    case (INSPECT_FULL_CHECK):
    case (INSPECT_QUICK_REPAIR):
    case (INSPECT_SCAN_REPAIR):
    case (INSPECT_FULL_REPAIR):
    case (INSPECT_MIGRATE):
//        gop = seglin_inspect_op(seg, da, fd, mode, bufsize, args, timeout);
          gop = gop_dummy(op_failure_status);  //** Not implemented
        break;
    case (INSPECT_SOFT_ERRORS):
    case (INSPECT_HARD_ERRORS):
        err.error_code = s->hard_errors;
        err.op_status = (err.error_code == 0) ? OP_STATE_SUCCESS : OP_STATE_FAILURE;
        gop = gop_dummy(err);
        break;
  }

  return(gop);
}

//***********************************************************************
// seglin_flush - Flushes a segment
//***********************************************************************

op_generic_t *seglin_flush(segment_t *seg, data_attr_t *da, ex_off_t lo, ex_off_t hi, int timeout)
{
  return(gop_dummy(op_success_status));
}

//***********************************************************************
// segline_clone_func - Clone data from the segment
//***********************************************************************

op_status_t seglin_clone_func(void *arg, int id)
{
  seglin_clone_t *slc = (seglin_clone_t *)arg;
  seglin_priv_t *ss = (seglin_priv_t *)slc->sseg->priv;
  seglin_priv_t *sd = (seglin_priv_t *)slc->dseg->priv;
  interval_skiplist_iter_t it;
  seglin_slot_t *bd, *bs;
  int n_blocks, dir, i;
  rs_request_t *req_list;
  data_cap_set_t **cap_list;
  seglin_slot_t **block;
  opque_t *q;
  op_generic_t *gop = NULL;
  op_status_t status;

  //** SEe if we are using an old seg.  If so we need to trunc it first
  if (slc->trunc == 1) {
     gop_sync_exec(segment_truncate(slc->dseg, slc->da, 0, slc->timeout));
  }

  //** Determine the number of intervals
  n_blocks = interval_skiplist_count(ss->isl);

  //** and make the space to store things
  type_malloc_clear(req_list, rs_request_t, n_blocks);
  type_malloc_clear(cap_list, data_cap_set_t *, n_blocks);
  type_malloc_clear(block, seglin_slot_t *, 2*n_blocks);

  //** Allocate the space
  it = iter_search_interval_skiplist(ss->isl, (skiplist_key_t *)NULL, (skiplist_key_t *)NULL);
  i = 0;
  while ((bs = (seglin_slot_t *)next_interval_skiplist(&it)) != NULL) {
     type_malloc_clear(bd, seglin_slot_t, 1);
     bd->data = data_block_create(sd->ds);
     bd->cap_offset = 0;
     bd->seg_offset = bs->seg_offset;
     bd->len = bs->len;
     bd->data->size = bs->len;
     bd->data->max_size = bs->len;
     bd->seg_end = bs->seg_end;

     block[i] = bd;
     block[i+n_blocks] = bs;
     req_list[i].rid_index = n_blocks%sd->n_rid_default;
     req_list[i].size = bd->len;
     cap_list[i] = bd->data->cap;
     i++;
  }

  //** Generate the query
  if (n_blocks > 0) {
     i = (sd->n_rid_default > n_blocks) ? n_blocks : sd->n_rid_default;
     gop = rs_data_request(sd->rs, slc->da, sd->rsq, cap_list, req_list, n_blocks, NULL, 0, i, 0, slc->timeout);
  }

  //** Wait for block creation to complete
  gop_waitall(gop);
  if (gop_completed_successfully(gop) != OP_STATE_SUCCESS) {  //** Error so clean up and return
       //** And cleanup
     free(block);
     free(req_list);
     free(cap_list);
     gop_free(gop, OP_DESTROY);
     return(op_failure_status);
  }

  gop_free(gop, OP_DESTROY);

  //** Add them to the segment  and generate the copy list
  q = new_opque();
  dir = ((slc->mode & DS_PULL) > 0) ? DS_PULL : DS_PUSH;

  for (i=0; i<n_blocks; i++) {
     bd = block[i];
     bs = block[i+n_blocks];
     atomic_inc(bd->data->ref_count);
     insert_interval_skiplist(sd->isl, (skiplist_key_t *)&(bd->seg_offset), (skiplist_key_t *)&(bd->seg_end), (skiplist_data_t *)bd);

     gop = ds_copy(bd->data->ds, slc->da, dir, NS_TYPE_SOCK, "",
              ds_get_cap(bs->data->ds, bs->data->cap, DS_CAP_READ), bs->cap_offset,
              ds_get_cap(bd->data->ds, bd->data->cap, DS_CAP_WRITE), bd->cap_offset,
              bs->len, slc->timeout);
     opque_add(q, gop);
  }

  //** Wait for the copying to finish
  opque_waitall(q);
  status = (opque_tasks_failed(q) == 0) ? op_success_status : op_failure_status;

  free(block);
  free(req_list);
  free(cap_list);

  opque_free(q, OP_DESTROY);

  sd->total_size = ss->total_size;
  sd->used_size = ss->used_size;

  return(status);
}


//***********************************************************************
// segline_clone - Clones a segment
//***********************************************************************

op_generic_t *seglin_clone(segment_t *seg, data_attr_t *da, segment_t **clone_seg, int mode, void *attr, int timeout)
{
  segment_t *clone;
  seglin_priv_t *ss = (seglin_priv_t *)seg->priv;
  seglin_priv_t *sd;
  op_generic_t *gop;
  seglin_clone_t *slc;
  int use_existing = (*clone_seg != NULL) ? 1 : 0;

    //** Make the base segment
  if (use_existing == 0) *clone_seg = segment_linear_create(seg->ess);
  clone = *clone_seg;
  sd = (seglin_priv_t *)clone->priv;

  //** Copy the header
  if ((seg->header.name != NULL) && (use_existing == 0)) clone->header.name = strdup(seg->header.name);

  //** Copy the default rs queury
  if (use_existing == 0) {
     if (attr == NULL) {
        sd->rsq = rs_query_dup(sd->rs, ss->rsq);
     } else {
        sd->rsq = rs_query_parse(sd->rs, (char *)attr);
     }
     sd->n_rid_default = ss->n_rid_default;
  }

  //** Basic size info
  sd->max_block_size = ss->max_block_size;
  sd->excess_block_size = ss->excess_block_size;

  //** Now copy the data if needed
   if (mode == CLONE_STRUCTURE) {
      if (use_existing == 1) {
         gop = segment_truncate(clone, da, 0, timeout);
      } else {
         gop = gop_dummy(op_success_status);
      }
  } else {
    type_malloc(slc, seglin_clone_t, 1);
    slc->sseg = seg;
    slc->dseg = clone;
    slc->da = da;
    slc->mode = mode;
    slc->timeout = timeout;
    slc->trunc = use_existing;
    gop = new_thread_pool_op(sd->tpc, NULL, seglin_clone_func, (void *)slc, free, 1);
  }

  return(gop);
}

//***********************************************************************
// seglin_size - Returns the segment size.
//***********************************************************************

ex_off_t seglin_size(segment_t *seg)
{
  seglin_priv_t *s = (seglin_priv_t *)seg->priv;
  ex_off_t size;

  segment_lock(seg);
  size = s->used_size;
  segment_unlock(seg);

  return(size);
}

//***********************************************************************
// seglin_block_size - Returns the segment block size.
//***********************************************************************

ex_off_t seglin_block_size(segment_t *seg)
{
//  seglin_priv_t *s = (seglin_priv_t *)seg->priv;

  return(1);
}

//***********************************************************************
// seglin_serialize_text -Convert the segment to a text based format
//***********************************************************************

int seglin_serialize_text(segment_t *seg, exnode_exchange_t *exp)
{
  seglin_priv_t *s = (seglin_priv_t *)seg->priv;
  int bufsize=10*1024;
  char segbuf[bufsize];
  char *ext, *etext;
  int sused;
  seglin_slot_t *b;
  exnode_exchange_t *cap_exp;
  interval_skiplist_iter_t it;

  segbuf[0] = 0;
  cap_exp = exnode_exchange_create(EX_TEXT);

  sused = 0;

  //** Store the segment header
  append_printf(segbuf, &sused, bufsize, "[segment-" XIDT "]\n", seg->header.id);
  if ((seg->header.name != NULL) && (strcmp(seg->header.name, "") != 0)) {
     etext = escape_text("=", '\\', seg->header.name);
     append_printf(segbuf, &sused, bufsize, "name=%s\n", etext);  free(etext);
  }
  append_printf(segbuf, &sused, bufsize, "type=%s\n", SEGMENT_TYPE_LINEAR);
  append_printf(segbuf, &sused, bufsize, "ref_count=%d\n", seg->ref_count);

  //** default resource query
  if (s->rsq != NULL) {
     ext = rs_query_print(s->rs, s->rsq);
     etext = escape_text("=", '\\', ext);
     append_printf(segbuf, &sused, bufsize, "query_default=%s\n", etext);  free(etext); free(ext);
  }
  append_printf(segbuf, &sused, bufsize, "n_rid_default=%d\n", s->n_rid_default);

  //** Basic size info
  append_printf(segbuf, &sused, bufsize, "max_block_size=" XOT "\n", s->max_block_size);
  append_printf(segbuf, &sused, bufsize, "excess_block_size=" XOT "\n", s->excess_block_size);
  append_printf(segbuf, &sused, bufsize, "max_size=" XOT "\n", s->total_size);
  append_printf(segbuf, &sused, bufsize, "used_size=" XOT "\n", s->used_size);

  //** Cycle through the blocks storing both the segment block information and also the cap blocks
  it = iter_search_interval_skiplist(s->isl, (skiplist_key_t *)NULL, (skiplist_key_t *)NULL);
  while ((b = (seglin_slot_t *)next_interval_skiplist(&it)) != NULL) {
     //** Add the cap
     data_block_serialize(b->data, cap_exp);

     //** Add the segment block
     append_printf(segbuf, &sused, bufsize, "block=" XIDT ":" XOT ":" XOT ":" XOT ":" XOT "\n",
          b->data->id, b->seg_offset, b->cap_offset, b->seg_end, b->len);
  }


  //** Merge everything together and return it
  exnode_exchange_append(exp, cap_exp);
  exnode_exchange_destroy(cap_exp);
  exnode_exchange_append_text(exp, segbuf);

  return(0);
}

//***********************************************************************
// seglin_serialize_proto -Convert the segment to a protocol buffer
//***********************************************************************

int seglin_serialize_proto(segment_t *seg, exnode_exchange_t *exp)
{
//  seglin_priv_t *s = (seglin_priv_t *)seg->priv;

  return(-1);
}

//***********************************************************************
// seglin_serialize -Convert the segment to a more portable format
//***********************************************************************

int seglin_serialize(segment_t *seg, exnode_exchange_t *exp)
{
  if (exp->type == EX_TEXT) {
     return(seglin_serialize_text(seg, exp));
  } else if (exp->type == EX_PROTOCOL_BUFFERS) {
     return(seglin_serialize_proto(seg, exp));
  }
 
  return(-1);
}

//***********************************************************************
// seglin_signature - Generates the segment signature
//***********************************************************************

int seglin_signature(segment_t *seg, char *buffer, int *used, int bufsize)
{
  seglin_priv_t *s = (seglin_priv_t *)seg->priv;

  append_printf(buffer, used, bufsize, "linear(n_rid_default=%d)\n", s->n_rid_default);

  return(0);
}

//***********************************************************************
// seglin_deserialize_text -Read the text based segment
//***********************************************************************

int seglin_deserialize_text(segment_t *seg, ex_id_t id, exnode_exchange_t *exp)
{
  seglin_priv_t *s = (seglin_priv_t *)seg->priv;
  int bufsize=1024;
  char seggrp[bufsize];
  char *text, *etext, *token, *bstate, *key, *value;
  int fin, fail;
  seglin_slot_t *b;
  inip_file_t *fd;
  inip_group_t *g;
  inip_element_t *ele;


  fail = 0;

  //** Parse the ini text
  fd = exp->text.fd;

  //** Make the segment section name
  snprintf(seggrp, bufsize, "segment-" XIDT, id);

  //** Get the segment header info
  seg->header.id = id;
  seg->header.type = SEGMENT_TYPE_LINEAR;
  seg->header.name = inip_get_string(fd, seggrp, "name", "");

  //** default resource query
  etext = inip_get_string(fd, seggrp, "query_default", "");
  text = unescape_text('\\', etext);
  s->rsq = rs_query_parse(s->rs, text);
  free(text); free(etext);
  s->n_rid_default = inip_get_integer(fd, seggrp, "n_rid_default", 2);

  //** Basic size info
  s->max_block_size = inip_get_integer(fd, seggrp, "max_block_size", 10*1024*1024);
  s->excess_block_size = inip_get_integer(fd, seggrp, "excess_block_size", s->max_block_size/4);
  s->total_size = inip_get_integer(fd, seggrp, "max_size", 0);
  s->used_size = inip_get_integer(fd, seggrp, "used_size", 0);

  //** Cycle through the blocks storing both the segment block information and also the cap blocks
  g = inip_find_group(fd, seggrp);
  ele = inip_first_element(g);
  while (ele != NULL) {
     key = inip_get_element_key(ele);
     if (strcmp(key, "block") == 0) {
        type_malloc_clear(b, seglin_slot_t, 1);

        //** Parse the segment line
        value = inip_get_element_value(ele);
        token = strdup(value);
        sscanf(escape_string_token(token, ":", '\\', 0, &bstate, &fin), XIDT, &id);
        sscanf(escape_string_token(NULL, ":", '\\', 0, &bstate, &fin), XOT, &(b->seg_offset));
        sscanf(escape_string_token(NULL, ":", '\\', 0, &bstate, &fin), XOT, &(b->cap_offset));
        sscanf(escape_string_token(NULL, ":", '\\', 0, &bstate, &fin), XOT, &(b->seg_end));
        sscanf(escape_string_token(NULL, ":", '\\', 0, &bstate, &fin), XOT, &(b->len));
        free(token);

        //** Find the cooresponding cap
        b->data = data_block_deserialize(seg->ess, id, exp);
        if (b->data == NULL) {
           log_printf(0, "Missing data block!  block id=" XIDT " seg=" XIDT "\n", id, segment_id(seg));
           free(b);
           fail = 1;
        } else {
           atomic_inc(b->data->ref_count);

           //** Finally add it to the ISL
           insert_interval_skiplist(s->isl, (skiplist_key_t *)&(b->seg_offset), (skiplist_key_t *)&(b->seg_end), (skiplist_data_t *)b);
        }
     }

     ele = inip_next_element(ele);
  }

  return(fail);
}

//***********************************************************************
// seglin_deserialize_proto - Read the prot formatted segment
//***********************************************************************

int seglin_deserialize_proto(segment_t *seg, ex_id_t id, exnode_exchange_t *exp)
{
  return(-1);
}

//***********************************************************************
// seglin_deserialize -Convert from the portable to internal format
//***********************************************************************

int seglin_deserialize(segment_t *seg, ex_id_t id, exnode_exchange_t *exp)
{
  if (exp->type == EX_TEXT) {
     return(seglin_deserialize_text(seg, id, exp));
  } else if (exp->type == EX_PROTOCOL_BUFFERS) {
     return(seglin_deserialize_proto(seg, id, exp));
  }
 
  return(-1);
}


//***********************************************************************
// seglin_destroy - Destroys a linear segment struct (not the data)
//***********************************************************************

void seglin_destroy(segment_t *seg)
{
  int i, n;
  interval_skiplist_iter_t it;
  seglin_slot_t **b_list;
  seglin_priv_t *s = (seglin_priv_t *)seg->priv;

  //** Check if it's still in use
log_printf(15, "seglin_destroy: seg->id=" XIDT " ref_count=%d\n", segment_id(seg), seg->ref_count);

  if (seg->ref_count > 0) return;

  n = interval_skiplist_count(s->isl);
  type_malloc_clear(b_list, seglin_slot_t *, n);
  it = iter_search_interval_skiplist(s->isl, (skiplist_key_t *)NULL, (skiplist_key_t *)NULL);
  for (i=0; i<n; i++) {
     b_list[i] = (seglin_slot_t *)next_interval_skiplist(&it);
  }
  destroy_interval_skiplist(s->isl);

  for (i=0; i<n; i++) {
     atomic_dec(b_list[i]->data->ref_count);
     data_block_destroy(b_list[i]->data);
     free(b_list[i]);
  }
  free(b_list);

  if (s->rsq != NULL) rs_query_destroy(s->rs, s->rsq);
  free(s);

  ex_header_release(&(seg->header));

  apr_thread_mutex_destroy(seg->lock);
  apr_thread_cond_destroy(seg->cond);
  apr_pool_destroy(seg->mpool);

  free(seg);
}


//***********************************************************************
// segment_linear_make - Creates a linear segment
//***********************************************************************

op_generic_t *segment_linear_make(segment_t *seg, data_attr_t *da, rs_query_t *rsq, int n_rid, ex_off_t block_size, ex_off_t total_size, int timeout)
{
  seglin_priv_t *s = (seglin_priv_t *)seg->priv;

  s->n_rid_default = n_rid;
  s->max_block_size = block_size;
  s->excess_block_size = block_size / 4;
  if (s->rsq != NULL) rs_query_destroy(s->rs, s->rsq);
  s->rsq = rs_query_dup(s->rs, rsq);

  return(seglin_truncate(seg, da, total_size, timeout));
}

//***********************************************************************
// segment_linear_create - Creates a linear segment
//***********************************************************************

segment_t *segment_linear_create(void *arg)
{
  service_manager_t *ess = (service_manager_t *)arg;
  seglin_priv_t *s;
  segment_t *seg;

  //** Make the space
  type_malloc_clear(seg, segment_t, 1);
  type_malloc_clear(s, seglin_priv_t, 1);

  s->isl = create_interval_skiplist(&skiplist_compare_ex_off, NULL, NULL, NULL);
  seg->priv = s;
  s->total_size = 0;
  s->used_size = 0;
  s->max_block_size = 10*1024*1024;;
  s->excess_block_size = 1*1024*1024;
  s->n_rid_default = 1;
  s->rsq = NULL;

  generate_ex_id(&(seg->header.id));
  atomic_set(seg->ref_count, 0);
  seg->header.type = SEGMENT_TYPE_LINEAR;

  assert(apr_pool_create(&(seg->mpool), NULL) == APR_SUCCESS);
  apr_thread_mutex_create(&(seg->lock), APR_THREAD_MUTEX_DEFAULT, seg->mpool);
  apr_thread_cond_create(&(seg->cond), seg->mpool);

  seg->ess = ess;
  s->rs = lookup_service(ess, ESS_RUNNING, ESS_RS);
  s->ds = lookup_service(ess, ESS_RUNNING, ESS_DS);
  s->tpc = lookup_service(ess, ESS_RUNNING, ESS_TPC_UNLIMITED);

  seg->fn.read = seglin_read;
  seg->fn.write = seglin_write;
  seg->fn.inspect = seglin_inspect;
  seg->fn.truncate = seglin_truncate;
  seg->fn.remove = seglin_remove;
  seg->fn.flush = seglin_flush;
  seg->fn.clone = seglin_clone;
  seg->fn.signature = seglin_signature;
  seg->fn.size = seglin_size;
  seg->fn.block_size = seglin_block_size;
  seg->fn.serialize = seglin_serialize;
  seg->fn.deserialize = seglin_deserialize;
  seg->fn.destroy = seglin_destroy;

  return(seg);
}

//***********************************************************************
// segment_linear_load - Loads a linear segment from ini/ex3
//***********************************************************************

segment_t *segment_linear_load(void *arg, ex_id_t id, exnode_exchange_t *ex)
{
  segment_t *seg = segment_linear_create(arg);
  if (segment_deserialize(seg, id, ex) != 0) {
     segment_destroy(seg);
     seg = NULL;
  }
  return(seg);
}
