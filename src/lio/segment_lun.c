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

#define _log_module_index 177

#include "ex3_abstract.h"
#include "ex3_system.h"
#include "interval_skiplist.h"
#include "ex3_compare.h"
#include "log.h"
#include "string_token.h"
#include "segment_lun.h"
#include "iniparse.h"
#include "random.h"
#include "append_printf.h"
#include "type_malloc.h"
#include "rs_query_base.h"
#include "segment_lun_priv.h"

typedef struct {
  data_block_t *data;    //** Data block
  ex_off_t cap_offset;  //** Starting location to use data in the cap
} seglun_block_t;

typedef struct {
  seglun_block_t *block;  //** Data blocks  making up the row
  ex_off_t seg_offset;  //** Offset withing the segment
  ex_off_t seg_end;     //** Ending location to use
  ex_off_t block_len;   //** Length of each block
  ex_off_t row_len;     //** Total length of row. (block_len*n_devices)
  int rwop_index;
} seglun_row_t;

typedef struct {
  opque_t *q;
  segment_t *seg;
  data_probe_t **probe;
  seglun_row_t **block;
} seglun_check_t;

typedef struct {
  segment_t *seg;
  data_attr_t *da;
  ex_off_t new_size;
  int timeout;
} seglun_truncate_t;

typedef struct {
  segment_t *seg;
  data_attr_t *da;
  info_fd_t *fd;
  rs_query_t *query;
  ex_off_t bufsize;
  int inspect_mode;
  int timeout;
} seglun_inspect_t;

typedef struct {
  segment_t *seg;
  data_attr_t *da;
  ex_iovec_t  *iov;
  ex_off_t    boff;
  tbuffer_t  *buffer;
  int         n_iov;
  int         rw_mode;
  int timeout;
} seglun_rw_t;

typedef struct {
  segment_t *sseg;
  segment_t *dseg;
  data_attr_t *da;
  int mode;
  int timeout;
  int trunc;
} seglun_clone_t;

typedef struct {
  op_generic_t *gop;
  tbuffer_t buffer;
  ex_iovec_t *ex_iov;
  iovec_t *iov;
  int n_ex;
  int c_ex;
  int n_iov;
  int c_iov;
//  ex_off_t offset;
  ex_off_t len;
} lun_rw_row_t;

//***********************************************************************
// _slun_perform_remap - Does a cap remap
//   **NOTE: Assumes the segment is locked
//***********************************************************************

void _slun_perform_remap(segment_t *seg)
{
  seglun_priv_t *s = (seglun_priv_t *)seg->priv;
  interval_skiplist_iter_t it;
  seglun_row_t *b;
  int i;

  log_printf(5, "START\n");
  it = iter_search_interval_skiplist(s->isl, (skiplist_key_t *)NULL, (skiplist_key_t *)NULL);
  while ((b = (seglun_row_t *)next_interval_skiplist(&it)) != NULL) {
    for (i=0; i<s->n_devices; i++) {
       rs_translate_cap_set(s->rs, b->block[i].data->rid_key, b->block[i].data->cap);
       log_printf(15, "i=%d rcap=%s\n", i, ds_get_cap(s->ds, b->block[i].data->cap, DS_CAP_READ));
    }
  }
  log_printf(5, "END\n");

  return;
}


//***********************************************************************
// slun_row_placement_check - Checks the placement of each allocation
//***********************************************************************

int slun_row_placement_check(segment_t *seg, data_attr_t *da, seglun_row_t *b, int *block_status, int n_devices, int soft_error_fail, rs_query_t *query, int timeout)
{
  seglun_priv_t *s = (seglun_priv_t *)seg->priv;
  int i, nbad;
  rs_hints_t hints_list[n_devices];
  char *migrate;
  op_generic_t *gop;

  //** Make the fixed list table
  for (i=0; i<n_devices; i++) {
    hints_list[i].fixed_rid_key = b->block[i].data->rid_key;
    hints_list[i].status = RS_ERROR_OK;
    hints_list[i].local_rsq = NULL;
    migrate = data_block_get_attr(b->block[i].data, "migrate");
    if (migrate != NULL) {
       hints_list[i].local_rsq = rs_query_parse(s->rs, migrate);
    }
  }

  //** Now call the query check
  gop = rs_data_request(s->rs, NULL, query, NULL, NULL, 0, hints_list, n_devices, n_devices, timeout);
  gop_waitall(gop);
  gop_free(gop, OP_DESTROY);

  nbad = 0;
  for (i=0; i<n_devices; i++) {
     if (hints_list[i].status != RS_ERROR_OK) {
        if (hints_list[i].status == RS_ERROR_FIXED_NOT_FOUND) {
            if (soft_error_fail > 0) nbad++;
        } else {
           nbad++;
        }
        block_status[i] = hints_list[i].status;
     }

     if (hints_list[i].local_rsq != NULL) { rs_query_destroy(s->rs, hints_list[i].local_rsq); }
  }

  return(nbad);
}

//***********************************************************************
// slun_row_placement_fix - Moves the allocations to satisfy the placement
//     constraints
//***********************************************************************

int slun_row_placement_fix(segment_t *seg, data_attr_t *da, seglun_row_t *b, int *block_status, int n_devices, rs_query_t *query, int timeout)
{
  seglun_priv_t *s = (seglun_priv_t *)seg->priv;
  int i, j, k, nbad, ngood, loop, cleanup_index;
  int missing[n_devices], m, todo;
  char *cleanup_key[5*n_devices];
  rs_request_t req[n_devices];
  rs_query_t *rsq;
  rs_hints_t hints_list[n_devices];
  data_block_t *db[n_devices], *dbs, *dbd, *dbold[n_devices];
  data_cap_set_t *cap[n_devices];

  char *migrate;
  op_generic_t *gop;
  opque_t *q;

  rsq = rs_query_dup(s->rs, query);

  cleanup_index = 0;
  loop = 0;
  do {
     q = new_opque();

     //** Make the fixed list mapping table
     memset(db, 0, sizeof(db));
     nbad = n_devices-1; ngood = 0;
     m = 0;
     for (i=0; i<n_devices; i++) {
        if (block_status[i] == 0) {
           j = ngood;
           hints_list[ngood].fixed_rid_key = b->block[i].data->rid_key;
           hints_list[ngood].status = RS_ERROR_OK;
           hints_list[ngood].local_rsq = NULL;
           ngood++;
        } else {
           j = nbad;
           hints_list[nbad].local_rsq = NULL;
           hints_list[nbad].fixed_rid_key = NULL;
           hints_list[nbad].status = RS_ERROR_OK;
           req[m].rid_index = nbad;
           req[m].size = b->block_len;
           db[m] = data_block_create(s->ds);
           cap[m] = db[m]->cap;
           missing[m] = i;
           nbad--;
           m++;
        }

       if (hints_list[j].local_rsq != NULL) { rs_query_destroy(s->rs, hints_list[j].local_rsq); }

        migrate = data_block_get_attr(b->block[i].data, "migrate");
        if (migrate != NULL) {
           hints_list[j].local_rsq = rs_query_parse(s->rs, migrate);
        }
//log_printf(0, "i=%d ngood=%d nbad=%d m=%d\n", i, ngood, nbad, m);
     }


     gop = rs_data_request(s->rs, da, rsq, cap, req, m, hints_list, ngood, n_devices, timeout);
     gop_waitall(gop);
     gop_free(gop, OP_DESTROY);

     //** Process the results
     opque_start_execution(q);
     todo = 0;
     for (j=0; j<m; j++) {
        i = missing[j];
//log_printf(0, "missing[%d]=%d rid_key=%s\n", j, missing[j], req[j].rid_key);
        if (ds_get_cap(db[j]->ds, db[j]->cap, DS_CAP_READ) != NULL) {
           db[j]->rid_key = req[j].rid_key;
           req[j].rid_key = NULL;  //** Cleanup

           //** Make the copy operation
           gop = ds_copy(b->block[i].data->ds, da, DS_PUSH, NS_TYPE_SOCK, "",
                       ds_get_cap(b->block[i].data->ds, b->block[i].data->cap, DS_CAP_READ), b->block[i].cap_offset,
                       ds_get_cap(db[j]->ds, db[j]->cap, DS_CAP_WRITE), 0,
                       b->block_len, timeout);
           gop_set_myid(gop, j);
           opque_add(q, gop);
        } else {  //** Make sure we exclude the RID key on the next round due to the failure
//           log_printf(15, "Excluding rid_key=%s on next round\n", req[j].rid_key);
           data_block_destroy(db[j]);

           if (req[j].rid_key != NULL) {
              log_printf(15, "Excluding rid_key=%s on next round\n", req[j].rid_key);
              cleanup_key[cleanup_index] = req[j].rid_key;
              req[j].rid_key = NULL;
              rs_query_add(s->rs, &rsq, RSQ_BASE_OP_KV, "rid_key", RSQ_BASE_KV_EXACT, cleanup_key[cleanup_index], RSQ_BASE_KV_EXACT);
              cleanup_index++;
              rs_query_add(s->rs, &rsq, RSQ_BASE_OP_NOT, NULL, 0, NULL, 0);
              rs_query_add(s->rs, &rsq, RSQ_BASE_OP_AND, NULL, 0, NULL, 0);
//char *qstr = rs_query_print(s->rs, rsq);
//log_printf(0, "rsq=%s\n", qstr);
//free(qstr);
           }
        }
log_printf(15, "after rs query block_status[%d]=%d block_len=" XOT "\n", i, block_status[i], b->block_len);
     }

log_printf(15, "q size=%d\n",opque_task_count(q));

     //** Wait for the copies to complete
     opque_waitall(q);
     k = 0;
     while ((gop = gop_get_next_finished(opque_get_gop(q))) != NULL) {
        j = gop_get_myid(gop);
log_printf(15, "index=%d\n", j);
        if (j >= 0) {  //** Skip any remove ops
           i = missing[j];
log_printf(15, "missing[%d]=%d status=%d\n", j,i, gop_completed_successfully(gop));
           if (gop_completed_successfully(gop) == OP_STATE_SUCCESS) {  //** Update the block
              dbs = b->block[i].data;
              dbd = db[j];

              dbd->size = dbs->size;
              dbd->max_size = dbs->max_size;
              atomic_inc(dbd->ref_count);
              dbd->attr_stack = dbs->attr_stack;
              dbs->attr_stack = NULL;

              b->block[i].data = dbd;
              b->block[i].cap_offset = 0;
              block_status[i] = 0;

              dbold[k] = dbs; k++;

              gop_free(gop, OP_DESTROY);

              //** Remove the old data
              gop = ds_remove(dbs->ds, da, ds_get_cap(dbs->ds, dbs->cap, DS_CAP_MANAGE), timeout);
              opque_add(q, gop);
           } else {  //** Copy failed so remove the destintation
              gop_free(gop, OP_DESTROY);
              gop = ds_remove(db[j]->ds, da, ds_get_cap(db[j]->ds, db[j]->cap, DS_CAP_MANAGE), timeout);
              gop_set_myid(gop, -1);
              dbold[k] = db[j]; k++;
              opque_add(q, gop);
           }
        } else {
           gop_free(gop, OP_DESTROY);
        }
     }

     opque_waitall(q);  //** Wait for the removal to complete.  Don't care if there are errors we can still continue
     opque_free(q, OP_DESTROY);

     //** Clean up
     for (i=0; i<k; i++) {
//log_printf(0, "dbold[%d]=%d\n", i, dbold[i]->ref_count);
        atomic_dec(dbold[i]->ref_count); data_block_destroy(dbold[i]);
     }

     todo= 0;
     for (i=0; i<n_devices; i++) if (block_status[i] != 0) todo++;

     loop++;
  } while ((loop < 5) && (todo > 0));

  for (i=0; i<cleanup_index; i++) free(cleanup_key[i]);

  for (i=0; i<n_devices; i++) {
     if (hints_list[i].local_rsq != NULL) { rs_query_destroy(s->rs, hints_list[i].local_rsq); }
  }
  rs_query_destroy(s->rs, rsq);

//log_printf(0, "todo=%d\n", todo);

  return(todo);
}


//***********************************************************************
// slun_row_size_check - Checks the size of eack block in the row.
//***********************************************************************

int slun_row_size_check(segment_t *seg, data_attr_t *da, seglun_row_t *b, int *block_status, int n_devices, int force_repair, int timeout)
{
  int i, n_size, n_missing;
  data_probe_t *probe[n_devices];
  opque_t *q;
  op_generic_t *gop;
  ex_off_t psize, seg_size;

  q = new_opque();

  for (i=0; i<n_devices; i++) {
     probe[i] = ds_probe_create(b->block[i].data->ds);

     gop = ds_probe(b->block[i].data->ds, da, ds_get_cap(b->block[i].data->ds, b->block[i].data->cap, DS_CAP_MANAGE), probe[i], timeout);
     gop_set_myid(gop, i);

     opque_add(q, gop);
  }

  opque_waitall(q);
  opque_free(q, OP_DESTROY);

  q = new_opque();
  n_missing = 0;  n_size = 0;
  for (i=0; i<n_devices; i++) {
     //** Verify the max_size >= cap_offset+len
     ds_get_probe(b->block[i].data->ds, probe[i], DS_PROBE_MAX_SIZE, &psize, sizeof(psize));
     seg_size = b->block[i].cap_offset + b->block_len;
     log_printf(10, "seg=" XIDT " seg_offset=" XOT " i=%d rcap=%s  size=" XOT " should be block_len=" XOT "\n", segment_id(seg),
            b->seg_offset, i, ds_get_cap(b->block[i].data->ds, b->block[i].data->cap, DS_CAP_READ), psize, b->block_len);
     if (psize < seg_size) {
        if (psize == 0) {  //** Can't access the allocation
           block_status[i] = 1;
           n_missing++;
        } else {   //** Size is screwed up
           gop = ds_truncate(b->block[i].data->ds, da, ds_get_cap(b->block[i].data->ds, b->block[i].data->cap, DS_CAP_MANAGE), seg_size, timeout);
           gop_set_myid(gop, i);
           opque_add(q, gop);
           block_status[i] = 2;
           n_size++;
        }
     }
  }

  if (n_size > 0) {
     while ((gop = opque_waitany(q)) != NULL) {
        if (gop_completed_successfully(gop) == OP_STATE_SUCCESS) {
           i = gop_get_myid(gop);
           block_status[i] = 0;
           b->block[i].data->max_size = b->block_len;

           n_size--;
        }
        gop_free(gop, OP_DESTROY);
     }
  }

  opque_free(q, OP_DESTROY);

  for (i=0; i<n_devices; i++) ds_probe_destroy(b->block[i].data->ds, probe[i]);

  return(n_size+n_missing);
}

//***********************************************************************
// slun_row_pad_fix - Pads the blocks=2 (size tweaked) to the full size
//***********************************************************************

int slun_row_pad_fix(segment_t *seg, data_attr_t *da, seglun_row_t *b, int *block_status, int n_devices, int timeout)
{
  int i, err;
  ex_off_t bstart;
  op_generic_t *gop;
  opque_t *q;
  tbuffer_t tbuf;
  char c;

  q = new_opque();

  c = 0;
  tbuffer_single(&tbuf, 1, &c);
  err = 0;
  for (i=0; i < n_devices; i++) {
     log_printf(10, "seg=" XIDT " seg_offset=" XOT " i=%d\n", segment_id(seg), b->seg_offset, i);
     if (block_status[i] == 2) {
        bstart = b->block[i].cap_offset + b->block[i].data->max_size - 1;
     log_printf(10, "seg=" XIDT " seg_offset=" XOT " i=%d rcap=%s  padding byte=" XOT "\n", segment_id(seg),
            b->seg_offset, i, ds_get_cap(b->block[i].data->ds, b->block[i].data->cap, DS_CAP_READ), bstart);

        gop = ds_write(b->block[i].data->ds, da, ds_get_cap(b->block[i].data->ds, b->block[i].data->cap, DS_CAP_WRITE), bstart, &tbuf, 0, 1, timeout);
        gop_set_myid(gop, i);
        opque_add(q, gop);
        err++;

     }
  }

  while ((gop = opque_waitany(q)) != NULL) {
     i = gop_get_myid(gop);
     if (gop_completed_successfully(gop) == OP_STATE_SUCCESS) {
        block_status[i] = 0;
        b->block[i].data->size = b->block[i].data->max_size;
        err--;
     } else {
        block_status[i] = 3;
     }
     gop_free(gop, OP_DESTROY);
  }

  opque_free(q, OP_DESTROY);

  return(err);
}

//***********************************************************************
// slun_row_replace_fix - Replaces the missing or bad allocation in the row
//***********************************************************************

int slun_row_replace_fix(segment_t *seg, data_attr_t *da, seglun_row_t *b, int *block_status, int n_devices, rs_query_t *rsq_base, int timeout)
{
  seglun_priv_t *s = (seglun_priv_t *)seg->priv;
  rs_request_t req_list[n_devices];
  data_cap_set_t *cap_list[n_devices];
  char *cleanup_key[5*n_devices];
  op_status_t status;
  opque_t *q;
  rs_query_t *rsq;
  op_generic_t *gop;
  Stack_t *attr_stack;
  int i, j, loop, err, m, ngood, nbad, cleanup_index;
  int missing[n_devices];
  rs_hints_t hints_list[n_devices];
  char *migrate;

  //** Dup the base query
  rsq = rs_query_dup(s->rs, rsq_base);

  memset(hints_list, 0, sizeof(hints_list));

  cleanup_index = 0;
  loop = 0;
  do {
    err = 0;
    q = new_opque();

log_printf(15, "loop=%d ------------------------------\n", loop);

    //** Make the fixed list mapping table
    nbad = n_devices-1; ngood = 0;
    m = 0;
    for (i=0; i<n_devices; i++) {
       if (block_status[i] == 0) {
          j = ngood;
          hints_list[ngood].fixed_rid_key = b->block[i].data->rid_key;
          hints_list[ngood].status = RS_ERROR_OK;
          ngood++;
       } else {
          j = nbad;
          hints_list[nbad].fixed_rid_key = NULL;
          hints_list[nbad].status = RS_ERROR_OK;

          req_list[m].rid_index = nbad;
          req_list[m].size = b->block_len;

          //** Make a new block and copy the old data
//log_printf(0, "block[%d].data=%p\n", i, b->block[i].data);
          attr_stack = NULL;
          if (b->block[i].data != NULL) {
//log_printf(0, "old b.data->id=" XIDT "\n", b->block[i].data->id);
             attr_stack = b->block[i].data->attr_stack;
             b->block[i].data->attr_stack = NULL;
             atomic_set(b->block[i].data->ref_count, 0);
             data_block_destroy(b->block[i].data);
             b->block[i].data = data_block_create(s->ds);
          } else {
             b->block[i].data = data_block_create(s->ds);
          }
//log_printf(0, "new b.data->id=" XIDT "\n", b->block[i].data->id);
          cap_list[m] = b->block[i].data->cap;
          b->block[i].data->rid_key = NULL;
//          b->block[i].data->cap = cap_list[m];
          b->block[i].data->attr_stack = attr_stack;
          b->block[i].data->max_size = b->block_len;
          b->block[i].data->size = b->block_len;
          missing[m] = i;
          m++;
          nbad--;
       }

       if (hints_list[j].local_rsq != NULL) { rs_query_destroy(s->rs, hints_list[j].local_rsq); }

       migrate = data_block_get_attr(b->block[i].data, "migrate");
       if (migrate != NULL) {
//log_printf(0, "i=%d migrate[" XIDT "]=%s\n", i, b->block[i].data->id, migrate);
           hints_list[j].local_rsq = rs_query_parse(s->rs, migrate);
       } else {
           hints_list[j].local_rsq = NULL;
       }
//log_printf(0, "i=%d ngood=%d nbad=%d\n", i, ngood, nbad);
    }

    //** Execute the Query
    gop = rs_data_request(s->rs, da, rsq, cap_list, req_list, m, hints_list, ngood, n_devices, timeout);
    err = gop_waitall(gop);

    //** Check if we have enough RIDS
    if (err != OP_STATE_SUCCESS) {
       status = gop_get_status(gop);
       if (status.error_code == RS_ERROR_NOT_ENOUGH_RIDS) { //** No use looping
          log_printf(1, "seg=" XIDT " ERROR not enough RIDS!\n", segment_id(seg));
          err = m;
          loop = 10;  //** Kick us out of the loop
          goto oops;
       }
    }

    //** Process the results
    err = 0;
    for (j=0; j<m; j++) {
       i = missing[j];
log_printf(15, "missing[%d]=%d req.op_status=%d\n", j, missing[j], gop_completed_successfully(req_list[j].gop));
       if (ds_get_cap(b->block[i].data->ds, b->block[i].data->cap, DS_CAP_READ) != NULL) {
          block_status[i] = 2;  //** Mark the block for padding
          b->block[i].data->rid_key = req_list[j].rid_key;
          atomic_inc(b->block[i].data->ref_count);
          req_list[j].rid_key = NULL; //** Cleanup
          err++;
       } else {  //** Make sure we exclude the RID key on the next round due to the failure
          if (req_list[j].rid_key != NULL) {
             log_printf(15, "Excluding rid_key=%s on next round\n", req_list[j].rid_key);
             cleanup_key[cleanup_index] = req_list[j].rid_key;
             req_list[j].rid_key = NULL;  //** Don't want to accidentally free it below
             rs_query_add(s->rs, &rsq, RSQ_BASE_OP_KV, "rid_key", RSQ_BASE_KV_EXACT, cleanup_key[cleanup_index], RSQ_BASE_KV_EXACT);
             cleanup_index++;
             rs_query_add(s->rs, &rsq, RSQ_BASE_OP_NOT, NULL, 0, NULL, 0);
             rs_query_add(s->rs, &rsq, RSQ_BASE_OP_AND, NULL, 0, NULL, 0);
          }
       }

log_printf(15, "after rs query block_status[%d]=%d block_len=" XOT "\n", i, block_status[i], b->block_len);
    }

    //**Pad the good ones
    err = m - err + slun_row_pad_fix(seg, da, b, block_status, n_devices, timeout);

log_printf(15, "after row_pad_fix.  m=%d err=%d\n", m, err);

oops:
    gop_free(gop, OP_DESTROY);
    opque_free(q, OP_DESTROY);
    loop++;
  } while ((loop < 5) && (err > 0));

  //** Clean up
  rs_query_destroy(s->rs, rsq);
  for (i=0; i<n_devices; i++) {
     if (hints_list[i].local_rsq != NULL) { rs_query_destroy(s->rs, hints_list[i].local_rsq); }
  }
  for (i=0; i<cleanup_index; i++) free(cleanup_key[i]);

  return(err);
}

//***********************************************************************
// seglun_grow - Expands a linear segment
//***********************************************************************

op_status_t _seglun_grow(segment_t *seg, data_attr_t *da, ex_off_t new_size, int timeout)
{
  int i, n, err, cnt;
  ex_off_t off, dsize, old_len;
  seglun_priv_t *s = (seglun_priv_t *)seg->priv;
  seglun_row_t *b;
  seglun_block_t *block;
  interval_skiplist_iter_t it;
  ex_off_t lo, hi, berr;
  op_status_t status;
  int block_status[s->n_devices];
  apr_time_t now;
  double gsecs, tsecs;


  now = apr_time_now();

  //** Round the size to the nearest stripe size
  berr = 0;
  err = 0;
  lo = s->total_size;
  dsize = new_size / s->stripe_size;
  dsize = dsize * s->stripe_size;
  if ((new_size % s->stripe_size) > 0) dsize += s->stripe_size;
  new_size = dsize;

  //** Make the space to store the new  rows

log_printf(1, "sid=" XIDT " currused=" XOT " currmax=" XOT " newmax=" XOT "\n", segment_id(seg), s->used_size, s->total_size, new_size);

  //** Find the last row and see if it needs expanding
  if ((s->total_size > 0) && (s->grow_break == 0)) {
     lo = s->total_size-1;
     hi = s->total_size;
     it = iter_search_interval_skiplist(s->isl, (skiplist_key_t *)&lo, (skiplist_key_t *)&hi);
     b = (seglun_row_t *)next_interval_skiplist(&it);
     if (b->row_len < s->max_row_size) {
        dsize = new_size - b->seg_offset;
        dsize /= s->n_devices;
        dsize += (dsize % s->chunk_size);  //** Round up to the nearest chunk
        if (dsize > s->max_block_size) dsize = s->max_block_size;

log_printf(15, "sid=" XIDT " increasing existing row seg_offset=" XOT " curr seg_end=" XOT " newmax=" XOT "\n", segment_id(seg), b->seg_offset, b->seg_end, new_size);

        remove_interval_skiplist(s->isl, (skiplist_key_t *)&(b->seg_offset), (skiplist_key_t *)&(b->seg_end), (skiplist_data_t *)b);

        old_len = b->block_len;
        b->block_len = dsize;
        b->row_len = dsize * s->n_devices;
        b->seg_end = b->seg_offset + b->row_len - 1;
        for (i=0; i<s->n_devices; i++) block_status[i] = 0;
        slun_row_size_check(seg, da, b, block_status, s->n_devices, 1, timeout);

        //** Check if we had an error on the size
        n = 0;
        for (i=0; i<s->n_devices; i++) if (block_status[i]==2) n++;
        if (n > 0) {  //** Error growing the allocations so just leave them with the bad size but truncate the block to the old size
           b->block_len = old_len;
           b->row_len = old_len * s->n_devices;
           b->seg_end = b->seg_offset + b->row_len - 1;
           for (i=0; i<s->n_devices; i++) b->block[i].data->max_size = old_len;
        } else {
           for (i=0; i<s->n_devices; i++) block_status[i] = 2;  //** Mark them for padding since they were enlarged
           berr = slun_row_pad_fix(seg, da, b, block_status, s->n_devices, timeout);
        }

        insert_interval_skiplist(s->isl, (skiplist_key_t *)&(b->seg_offset), (skiplist_key_t *)&(b->seg_end), (skiplist_data_t *)b);

log_printf(15, "sid=" XIDT " enlarged row seg_offset=" XOT " seg_end=" XOT " row_len=" XOT "\n", segment_id(seg), b->seg_offset, b->seg_end, b->row_len);

        lo = b->seg_end + 1;
     } else {
log_printf(15, "sid=" XIDT " row maxed out seg_offset=" XOT " curr seg_end=" XOT " row_len=" XOT "\n", segment_id(seg), b->seg_offset, b->seg_end, b->row_len);
        lo = b->seg_end + 1;
     }
  }

  //** Create the additional caps and commands
  err = 0;
  for (off=lo; off<new_size; off = off + s->max_row_size) {
     type_malloc_clear(b, seglun_row_t, 1);
     type_malloc_clear(block, seglun_block_t, s->n_devices);
     b->block = block;
     b->rwop_index = -1;
     b->seg_offset = off;

     dsize = off + s->max_row_size;
     if (dsize > new_size) {
        dsize = new_size - off;
     } else {
        dsize = s->max_row_size;
     }
     b->block_len = dsize / s->n_devices;
     b->row_len = b->block_len * s->n_devices;
     b->seg_end = b->seg_offset + b->row_len - 1;

     for (i=0; i< s->n_devices;  i++) {
        b->block[i].data = NULL;  //** Marked as bad so not used
        b->block[i].cap_offset = 0;
     }

     //** Flag them all as missing so they can be replaced
     for (i=0; i < s->n_devices; i++) block_status[i] = 1;
     err = err + slun_row_replace_fix(seg, da, b, block_status, s->n_devices, s->rsq, timeout);

     if (err == 0) {
        insert_interval_skiplist(s->isl, (skiplist_key_t *)&(b->seg_offset), (skiplist_key_t *)&(b->seg_end), (skiplist_data_t *)b);
     } else {  //** Got an error so clean up and kick out
       for (i=0; i<s->n_devices; i++) {
          if (block[i].data != NULL) {
             cnt = atomic_get(block[i].data->ref_count);
             if ( cnt > 0) atomic_dec(block[i].data->ref_count);
             data_block_destroy(block[i].data);
          }
       }
       free(block);
       free(b);
       goto oops;
     }

log_printf(15, "sid=" XIDT " off=" XOT " b->row_len=" XOT " err=%d\n", segment_id(seg), b->seg_offset, b->row_len, err);

  }

oops:
  now = apr_time_now() - now;
  s->grow_time += now;
  s->grow_count++;

gsecs = (double)now/APR_USEC_PER_SEC;
tsecs = (double)s->grow_time/APR_USEC_PER_SEC;
log_printf(1, "sid=" XIDT " END used=" XOT " old max=" XOT " newmax=" XOT " err=%d berr=%d dt=%lf dt_total=%lf grow_count=%d\n", segment_id(seg), s->used_size, s->total_size, new_size, err, berr, gsecs, tsecs, s->grow_count);

  if ((err+berr) == 0) {
    s->total_size = new_size;
    status = op_success_status;
  } else {
    status =  op_failure_status;
  }


  return(status);
}

//***********************************************************************
// seglun_shrink - Shrinks a linear segment
//***********************************************************************

op_status_t _seglun_shrink(segment_t *seg, data_attr_t *da, ex_off_t new_size, int timeout)
{
  seglun_priv_t *s = (seglun_priv_t *)seg->priv;
  op_generic_t *gop;
  interval_skiplist_iter_t it;
  seglun_row_t *b;
  opque_t *q = NULL;
  ex_off_t lo, hi, dsize, bstart_size, bstart_block_size;
  Stack_t *stack;
  seglun_row_t *start_b;
  op_status_t status;
  int i, err;

  stack = new_stack();
  q = new_opque();

  //** Round the size to the nearest stripe size
  dsize = new_size / s->stripe_size;
  dsize = dsize * s->stripe_size;
  if ((new_size % s->stripe_size) > 0) dsize += s->stripe_size;
  new_size = dsize;
  bstart_block_size = -1;

  lo = new_size;
  hi = s->total_size;
  log_printf(1, "_sl_shrink: sid=" XIDT " total_size=" XOT " new_size=" XOT "\n", segment_id(seg), hi, lo);


  it = iter_search_interval_skiplist(s->isl, (skiplist_key_t *)&lo, (skiplist_key_t *)&hi);
  b = (seglun_row_t *)next_interval_skiplist(&it);

  //** The 1st row maybe a partial removal
  dsize = new_size - b->seg_offset;
  bstart_size = dsize;
  if (dsize == 0) {  //** Full removal
     log_printf(15, "_sl_shrink: sid=" XIDT " removing seg_off=" XOT "\n", segment_id(seg), b->seg_offset);
     for (i=0; i < s->n_devices; i++) {
        gop = ds_remove(b->block[i].data->ds, da, ds_get_cap(b->block[i].data->ds, b->block[i].data->cap, DS_CAP_MANAGE), timeout);
        opque_add(q, gop);
     }
     push(stack, (void *)b);
     start_b = NULL;
  } else {
     log_printf(15, "_sl_shrink: sid=" XIDT " shrinking  seg_off=" XOT " to=" XOT "\n", segment_id(seg), b->seg_offset, dsize);
     bstart_block_size = dsize / s->n_devices;
     for (i=0; i < s->n_devices; i++) {
        gop = ds_truncate(b->block[i].data->ds, da, ds_get_cap(b->block[i].data->ds, b->block[i].data->cap, DS_CAP_MANAGE), bstart_block_size, timeout);
        opque_add(q, gop);
     }
     start_b = b;
  }

  //** Set up for the rest of the blocks
  b = (seglun_row_t *)next_interval_skiplist(&it);
  while (b != NULL) {
    log_printf(15, "_sl_shrink: sid=" XIDT " removing seg_off=" XOT "\n", segment_id(seg), b->seg_offset);
    push(stack, (void *)b);
    for (i=0; i < s->n_devices; i++) {
       gop = ds_remove(b->block[i].data->ds, da, ds_get_cap(b->block[i].data->ds, b->block[i].data->cap, DS_CAP_MANAGE), timeout);
       opque_add(q, gop);
    }

    b = (seglun_row_t *)next_interval_skiplist(&it);
  }

  //** Do the removal
  err = opque_waitall(q);
  opque_free(q, OP_DESTROY);

  //** And now clean up
  while ((b = (seglun_row_t *)pop(stack)) != NULL) {
     i = remove_interval_skiplist(s->isl, &(b->seg_offset), &(b->seg_end), b);
     log_printf(15, "_sl_shrink: sid=" XIDT " removing from interval seg_off=" XOT " remove_isl=%d\n", segment_id(seg), b->seg_offset, i);
     for (i=0; i < s->n_devices; i++) {
        atomic_dec(b->block[i].data->ref_count);
        data_block_destroy(b->block[i].data);
     }
     free(b->block);
     free(b);
  }

  free_stack(stack, 0);

  //** If needed tweak the initial block
  if (start_b != NULL) {
     b = start_b;
     remove_interval_skiplist(s->isl, &(b->seg_offset), &(b->seg_end), b);
     b->seg_end = b->seg_offset + bstart_size - 1;
     b->block_len = bstart_block_size;
     b->row_len = bstart_size;
     insert_interval_skiplist(s->isl, (skiplist_key_t *)&(b->seg_offset), (skiplist_key_t *)&(b->seg_end), (skiplist_data_t *)b);
  }

  //** Update the size
  s->total_size = new_size;
  s->used_size = new_size;

  status = (err == OP_STATE_SUCCESS) ? op_success_status : op_failure_status;
  return(status);
}

//***********************************************************************
// _slun_truncate - Performs the truncate
//***********************************************************************

op_status_t _slun_truncate(segment_t *seg, data_attr_t *da, ex_off_t new_size, int timeout)
{
  seglun_priv_t *s = (seglun_priv_t *)seg->priv;
  op_status_t err = op_success_status;

  if (s->total_size > new_size) {
     err = _seglun_shrink(seg, da, new_size, timeout);
  } else if (s->total_size < new_size) {
     err = _seglun_grow(seg, da, new_size, timeout);
  }

  return(err);
}

//***********************************************************************
//  seglun_truncate_func - Does the actual segment truncate operations
//***********************************************************************

op_status_t seglun_truncate_func(void *arg, int id)
{
  seglun_truncate_t *st = (seglun_truncate_t *)arg;
  op_status_t err;

  segment_lock(st->seg);
  err = _slun_truncate(st->seg, st->da, st->new_size, st->timeout);
  segment_unlock(st->seg);

  return(err);
}

//***********************************************************************
// seglun_truncate - Expands or contracts a linear segment
//***********************************************************************

op_generic_t *seglun_truncate(segment_t *seg, data_attr_t *da, ex_off_t new_size, int timeout)
{
  seglun_priv_t *s = (seglun_priv_t *)seg->priv;

  seglun_truncate_t *st;

  type_malloc_clear(st, seglun_truncate_t, 1);

  st->seg = seg;
  st->new_size = new_size;
  st->timeout = timeout;
  st->da = da;

  return(new_thread_pool_op(s->tpc, NULL, seglun_truncate_func, (void *)st, free, 1));
}

//***********************************************************************
// lun_row_decompose - Decomposes the R/W request (start->start+blen) on the
//    row into separate requests for each block in the row.
//    NOTE: start is relative to start of the row and not the file!
//***********************************************************************

void lun_row_decompose(segment_t *seg, lun_rw_row_t *rw_buf, seglun_row_t *b, ex_off_t start, tbuffer_t *buffer, ex_off_t bpos, ex_off_t rwlen)
{
  seglun_priv_t *s = (seglun_priv_t *)seg->priv;
  lun_rw_row_t *rwb;
  int i, j, k, n_stripes, start_stripe, end_stripe;
  ex_off_t lo, hi, nleft, pos, chunk_off, chunk_end, stripe_off, begin, end, nbytes;
  int err, dev, ss, stripe_shift;
  ex_off_t offset[s->n_devices], len[s->n_devices];
  tbuffer_var_t tbv;
//ex_off_t dummy;

  lo = start;  hi = lo + rwlen - 1;
  start_stripe = lo / s->stripe_size;
  end_stripe = hi / s->stripe_size;
  n_stripes = end_stripe - start_stripe + 1;

log_printf(15, "lo=" XOT " hi= " XOT " len=" XOT "\n", lo, hi, rwlen);
log_printf(15, "start_stripe=" XOT " end_stripe=" XOT " n_stripes=" XOT "\n", start_stripe, end_stripe, n_stripes);

  tbuffer_var_init(&tbv);

  for (i=0; i < s->n_devices; i++) {
     offset[i] = -1;
     len[i] = 0;

     //** Make the initial space
     k = rw_buf[i].c_iov - rw_buf[i].n_iov;
     if (k < n_stripes) {
        rw_buf[i].c_iov += n_stripes - k + 1;
        if (rw_buf[i].iov == NULL) {
           type_malloc(rw_buf[i].iov, iovec_t, rw_buf[i].c_iov);
        } else {
           type_realloc(rw_buf[i].iov, iovec_t, rw_buf[i].c_iov);
        }
     }
  }

  ss = start_stripe;
  stripe_shift = ss*s->n_shift;
  stripe_off = ss * s->stripe_size;
  while (stripe_off <= hi) {
//log_printf(15, "ss=%d stripe_off=" XOT " stripe_shift=%d\n", ss, stripe_off, stripe_shift);
     for (i=0; i< s->n_devices; i++) {
        dev = (i+stripe_shift) % s->n_devices;
        chunk_off = stripe_off + dev * s->chunk_size;
        chunk_end = chunk_off + s->chunk_size - 1;
        rwb = &(rw_buf[i]);
//log_printf(15, " i=%d dev=%d chunk_off=" XOT " chunk_end=" XOT "\n", i, dev, chunk_off, chunk_end);

        if ((chunk_end >= lo) && (chunk_off <= hi)) {
           begin = (chunk_off < lo) ? lo - chunk_off: 0;
           end = (chunk_end > hi) ? hi - chunk_off : s->chunk_size - 1;
           nbytes = end - begin + 1;

           if (offset[i] == -1) { //** 1st time it's used so set the offset
              offset[i] = ss * s->chunk_size + begin;
           }
           len[i] += nbytes;

           pos = bpos + chunk_off + begin - lo;
//log_printf(15, "begin=" XOT " end=" XOT " nbytes=" XOT " bpos=" XOT "\n", begin, end, nbytes, pos);

           nleft = nbytes;
           tbv.nbytes = nleft;
           err = TBUFFER_OK;
           while ((nleft > 0) && (err == TBUFFER_OK)) {
              err = tbuffer_next(buffer, pos, &tbv);
              k = rwb->n_iov + tbv.n_iov;
              if (k >= rwb->c_iov) { rwb->c_iov = 2*k; type_realloc(rwb->iov, iovec_t, rwb->c_iov); }
              for (k=0; k<tbv.n_iov; k++) {
                 rwb->iov[rwb->n_iov + k] = tbv.buffer[k];
//dummy = iov[i][c_iov[i]+k].iov_len;
//log_printf(15, "iov[%d][%d] -- iov_len=" XOT " iov_base=%p\n", i, c_iov[i]+k, dummy, iov[i][c_iov[i]+k].iov_base);
              }
              rwb->n_iov += tbv.n_iov;

              nleft -= tbv.nbytes;

              pos += tbv.nbytes;
              tbv.nbytes = nleft;
           }
        }
     }

     stripe_off += s->stripe_size;
     stripe_shift += s->n_shift;
     ss++;
  }

  for (i=0; i < s->n_devices; i++) {
//log_printf(15, "i=%d off=" XOT " len=" XOT " n_iov=%d\n", i, rw_buf[i].offset, rw_buf[i].len, c_iov[i]);
     if (offset[i] >= 0) {
        j = rw_buf[i].n_ex;
        if (rw_buf[i].n_ex == rw_buf[i].c_ex) {
           k = 2 * (j+1);
           rw_buf[i].c_ex = k;
           if (rw_buf[i].n_ex == 0) {
              type_malloc(rw_buf[i].ex_iov, ex_iovec_t, k);
           } else {
              type_realloc(rw_buf[i].ex_iov, ex_iovec_t, k);
           }
        }
        rw_buf[i].ex_iov[j].offset = offset[i];
        rw_buf[i].ex_iov[j].len = len[i];
        rw_buf[i].len += len[i];
        rw_buf[i].n_ex++;
     }
  }

}

//*************************************************************************
// seglun_compare_buffers_print - FInds the 1st index where the buffers differ
//*************************************************************************

int seglun_compare_buffers_print(char *b1, char *b2, int len)
{
  int i, k, mode, last, ok, err;
  ex_off_t start, end;
  char cbuf[51];

  mode = (b1[0] == b2[0]) ? 0 : 1;
  start = 0;
  last = len - 1;

  err = 0;
  log_printf(0, "Printing comparision breakdown -- Single byte matches are suppressed (len=%d)\n", len);
  for (i=0; i<len; i++) {
    if (b1[i] != b2[i]) err = 1;

    if (mode == 0) {  //** Matching range
      if ((b1[i] != b2[i]) || (last == i)) {
         end = i-1;
         if (b1[i] == b2[i]) end = i;
         k = end - start + 1;
         log_printf(0, "  MATCH : %d -> %d (%d bytes)\n", start, end, k);

         start = i;
         mode = 1;
      }
    } else {
      if ((b1[i] == b2[i]) || (last == i)) {
         ok = 0;  //** Suppress single byte matches
         if (last != i) {
            if (b1[i+1] == b2[i+1]) ok = 1;
         }
         if ((ok == 1) || (last == i)) {
            end = i-1;
            k = end - start + 1;
            log_printf(0, "  DIFFER: %d -> %d (%d bytes)\n", start, end, k);

            if (k>50) k = 50;
            memcpy(cbuf, &(b1[start]), k);  cbuf[k] = 0;
            log_printf(0, "   b1=%s\n", cbuf);
            memcpy(cbuf, &(b2[start]), k);  cbuf[k] = 0;
            log_printf(0, "   b2=%s\n", cbuf);

            start = i;
            mode = 0;
         }
      }
    }
  }

  return(err);
}

//***********************************************************************
//  seglun_row_decompose_test - Tests the lun_row_decopose routine
//***********************************************************************

int seglun_row_decompose_test()
{
  int max_dev = 100;  //** Can test with up to 100 devices
  int bufsize = 1024*1024;
  segment_t *seg;
  seglun_priv_t *s;
  lun_rw_row_t rw_buf[max_dev];
  iovec_t *iov_ref[max_dev], *iovbuf;
  tbuffer_t tbuf, tbuf_ref[max_dev];
  ex_off_t boff;
  seglun_row_t *b;  //**Fake row
  seglun_block_t *block;
  char *ref_buf, *buf;
  char *base = "ABCDEFGHIJKLOMOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  int i, j, k, r_shift, nbase, ndev, niov, nrows, len, offset, cerr, roff, coff;
  int n_devs, niov_run, n_tests;

  //** test configs
  n_devs = 20;
  niov_run = 10;
  n_tests = 10;


  nbase = strlen(base);
  log_printf(0, "strlen(base)=%d\n", nbase);

  cerr = 0;

  //** Make an empty segment for testing.
  //** The only variables used by lun_row_decompose are s->{n_devices,stripe_size,chunk_size}.
  seg = segment_lun_create((void *)exnode_service_set);
  s = (seglun_priv_t *)seg->priv;

  //** Make the fake row
  type_malloc_clear(b, seglun_row_t, 1);
  type_malloc_clear(block, seglun_block_t, max_dev);
  b->block = block;
  b->seg_offset = 0;
  for (i=0; i<max_dev; i++) {
     block[i].data = data_block_create(s->ds);
     block[i].cap_offset = 0;
     block[i].data->size = bufsize;     //** Set them to a not to exceed value so
     block[i].data->max_size = bufsize; //** I don't have to muck with them as I change params
  }

  //** Make the test buffers
  type_malloc(ref_buf, char, bufsize);
  type_malloc_clear(buf, char, bufsize);
  for (i=0; i<bufsize; i++) ref_buf[i] = base[i%nbase];


  //** Now do the tests
  for (ndev=1; ndev <= n_devs; ndev++) {  //** Number of devices
     log_printf(0, "ndev=%d----------------------------------------------------------------------\n", ndev);

     s->n_devices = ndev;
     s->chunk_size = 16*1024;
//s->chunk_size = 16000;
     s->stripe_size = s->n_devices * s->chunk_size;
     s->n_shift = 1;

     //** Make the reference tbufs
     nrows = bufsize / s->stripe_size;
     if ((bufsize % s->stripe_size) > 0) nrows++;

     log_printf(0, "ndev=%d  chunk_size=%d stripe_size=%d   nrows=%d----------------------------------------------\n", ndev, s->chunk_size, s->stripe_size, nrows);

     for (i=0; i < s->n_devices; i++) {
        type_malloc(iov_ref[i], iovec_t, nrows);
        tbuffer_vec(&(tbuf_ref[i]), bufsize, nrows, iov_ref[i]);
     }

     for (j=0; j < nrows; j++) {
        r_shift = j*s->n_shift;
        roff = j * s->stripe_size;
        for (i=0; i < s->n_devices; i++) {
            k = (r_shift+i) % s->n_devices;
            coff = k * s->chunk_size;
            boff = roff + coff;
            iov_ref[i][j].iov_base = &(ref_buf[boff]);
            iov_ref[i][j].iov_len = s->chunk_size;
        }
     }

     for (niov=1; niov <= niov_run; niov++) {  //** Number of iovec blocks
     log_printf(0, "ndev=%d  niov=%d----------------------------------------------------------------------\n", ndev, niov);

         //** Make the destination buf
         type_malloc(iovbuf, iovec_t, niov);

         for (j=0; j < n_tests; j++) {  //** Random tests

            //** Init the dest buf for the test
            len = random_int(0, bufsize-1);
            offset = random_int(0,bufsize-len-1);
//len = 30000;
//offset = 8000;
            k = len / niov;
            for (i=0; i<niov; i++) {
                iovbuf[i].iov_base = &(buf[i*k]);
                iovbuf[i].iov_len = k;
            }
            iovbuf[niov-1].iov_len = len - (niov-1)*k;
            tbuffer_vec(&tbuf, len, niov, iovbuf);

             log_printf(0, "ndev=%d  niov=%d j=%d  len=%d off=%d k=%d\n", ndev, niov, j, len, offset, k); flush_log();

            //** Do the test
            memset(buf, 0, bufsize);
            lun_row_decompose(seg, rw_buf, b, offset, &tbuf, 0, len);

            for (i=0; i < s->n_devices; i++) {
               if (rw_buf[i].n_ex > 0) {
                  tbuffer_copy(&(tbuf_ref[i]), rw_buf[i].ex_iov[0].offset, &(rw_buf[i].buffer), 0, rw_buf[i].ex_iov[0].len);
               }
            }

            //** and check the result
            cerr += seglun_compare_buffers_print(&(ref_buf[offset]), buf, len);
         }

         free(iovbuf);
     }

     for (i=0; i < s->n_devices; i++) {
        free(iov_ref[i]);
     }
  }

  ///** Clean up
  free(ref_buf);
  free(buf);
  for (i=0; i<max_dev; i++) {
     data_block_destroy(block[i].data);
  }
  free(block);
  free(b);
  segment_destroy(seg);

  log_printf(0, " Total error count=%d\n", cerr);
  if (cerr == 0) log_printf(0, "PASSED!\n");

  return(cerr);
}

//***********************************************************************
// seglun_rw_op - Reads/Writes to a LUN segment
//***********************************************************************

op_status_t seglun_rw_op(segment_t *seg, data_attr_t *da, int n_iov, ex_iovec_t *iov, tbuffer_t *buffer, ex_off_t boff, int rw_mode, int timeout)
{
  seglun_priv_t *s = (seglun_priv_t *)seg->priv;
  op_status_t status;
  opque_t *q;
  seglun_row_t *b, **bused;
  interval_skiplist_iter_t it;
  ex_off_t lo, hi, start, end, blen, bpos;
  int i, j, maxerr, nerr, slot, n_bslots;
  int *bcount;
  Stack_t *stack;
  lun_rw_row_t *rw_buf, *rwb_table;
  double dt;
  apr_time_t tstart, tstart2;
  op_generic_t *gop;

  tstart = apr_time_now();

  segment_lock(seg);

  //** Check if we need to translate the caps
  if (s->map_version != s->notify.map_version) {
     while (s->inprogress_count > 0) {
         apr_thread_cond_wait(seg->cond, seg->lock);
     }

     //** Do the remap unless someoue beat us to it.
     if (s->map_version != s->notify.map_version) {
        s->map_version = s->notify.map_version;
        _slun_perform_remap(seg);
     }
  }

  s->inprogress_count++;  //** Flag that we are doing an I/O op

  type_malloc(bused, seglun_row_t *, interval_skiplist_count(s->isl));
  type_malloc(bcount, int, s->n_devices * interval_skiplist_count(s->isl));
  type_malloc(rwb_table, lun_rw_row_t, s->n_devices * interval_skiplist_count(s->isl));

  q = new_opque();
  stack = new_stack();
  bpos = boff;

log_printf(15, "START sid=" XIDT " n_iov=%d rw_mode=%d intervals=%d\n", segment_id(seg), n_iov, rw_mode, interval_skiplist_count(s->isl));

  n_bslots = 0;
  for (slot=0; slot<n_iov; slot++) {
    lo = iov[slot].offset;

    hi = lo + iov[slot].len - 1;
    it = iter_search_interval_skiplist(s->isl, (skiplist_key_t *)&lo, (skiplist_key_t *)&hi);
    b = (seglun_row_t *)next_interval_skiplist(&it);
log_printf(15, "FOR sid=" XIDT " slot=%d n_iov=%d lo=" XOT " hi=" XOT " b=%p\n", segment_id(seg), slot, n_iov, lo, hi, b);

    while (b != NULL) {
      start = (lo <= b->seg_offset) ? 0 : (lo - b->seg_offset);
      end = (hi >= b->seg_end) ? b->row_len-1 : (hi - b->seg_offset);
      blen = end - start + 1;

log_printf(15, "sid=" XIDT " soff=" XOT " bpos=" XOT " blen=" XOT " seg_off=" XOT " seg_len=" XOT " seg_end=" XOT " rwop_index=%d\n", segment_id(seg),
     start, bpos, blen, b->seg_offset, b->row_len, b->seg_end, b->rwop_index); flush_log();

      if (b->rwop_index < 0) {
         bused[n_bslots] = b;
         b->rwop_index = n_bslots; n_bslots++;
         j = b->rwop_index * s->n_devices;
         memset(&(rwb_table[j]), 0, sizeof(lun_rw_row_t)*s->n_devices);
      }

log_printf(15, "rwop_index=%d\n", b->rwop_index);

      rw_buf = &(rwb_table[b->rwop_index*s->n_devices]);
      lun_row_decompose(seg, rw_buf, b, start, buffer, bpos, blen);

      bpos = bpos + blen;

      b = (seglun_row_t *)next_interval_skiplist(&it);
    }
log_printf(15, "bottom sid=" XIDT " slot=%d\n", segment_id(seg), slot);

  }

log_printf(15, " n_bslots=%d\n", n_bslots);

  //** Assemble the sub tasks and start executing them
  for (slot=0; slot < n_bslots; slot++) {
     b = bused[slot];
     b->rwop_index = -1;
     j = slot * s->n_devices;

     if (rw_mode== 0) {
        for (i=0; i < s->n_devices; i++) {
            if (rwb_table[j + i].n_ex > 0) {
               tbuffer_vec(&(rwb_table[j + i].buffer), rwb_table[j + i].len, rwb_table[j+i].n_iov, rwb_table[j+i].iov);
               if (rwb_table[j+i].n_iov == 1) {
                  rwb_table[j + i].gop = ds_read(b->block[i].data->ds, da, ds_get_cap(b->block[i].data->ds, b->block[i].data->cap, DS_CAP_READ),
                                          rwb_table[j+i].ex_iov[0].offset, &(rwb_table[j+i].buffer), 0, rwb_table[j+i].len, timeout);
               } else {
                  rwb_table[j + i].gop = ds_readv(b->block[i].data->ds, da, ds_get_cap(b->block[i].data->ds, b->block[i].data->cap, DS_CAP_READ),
                                          rwb_table[j + i].n_ex, rwb_table[j+i].ex_iov, &(rwb_table[j+i].buffer), 0, rwb_table[j+i].len, timeout);
               }
               opque_add(q, rwb_table[j+i].gop);
               gop_set_myid(rwb_table[j+i].gop, i*10000 + slot);
            }
         }
     } else {
         for (i=0; i < s->n_devices; i++) {
            if (rwb_table[i].n_ex > 0) {
//FORCE ERROR -- Force a failure on all writes to the first allocation for testing purposes
//if (i==0) {
//  rwb_table[i].len = 0;
//} else {
               tbuffer_vec(&(rwb_table[j + i].buffer), rwb_table[j + i].len, rwb_table[j+i].n_iov, rwb_table[j+i].iov);
               if (rwb_table[j+i].n_iov == 1) {
                  rwb_table[j + i].gop = ds_write(b->block[i].data->ds, da, ds_get_cap(b->block[i].data->ds, b->block[i].data->cap, DS_CAP_WRITE),
                                          rwb_table[j+i].ex_iov[0].offset, &(rwb_table[j+i].buffer), 0, rwb_table[j+i].len, timeout);
               } else {
                  rwb_table[j + i].gop = ds_writev(b->block[i].data->ds, da, ds_get_cap(b->block[i].data->ds, b->block[i].data->cap, DS_CAP_WRITE),
                                       rwb_table[j + i].n_ex, rwb_table[j+i].ex_iov, &(rwb_table[j+i].buffer), 0, rwb_table[j+i].len, timeout);
               }
               opque_add(q, rwb_table[j+i].gop);
               gop_set_myid(rwb_table[j+i].gop, i*10000 + slot);
//}
            }
         }
     }
  }

  segment_unlock(seg);

  if (opque_task_count(q) == 0) {
     log_printf(0, "ERROR Nothing to do\n");
     status = op_failure_status;
  } else {
     tstart2 = apr_time_now();
//     opque_waitall(q);
     while ((gop = opque_waitany(q)) != NULL) {
        dt = apr_time_now() - tstart2;
        dt /= (APR_USEC_PER_SEC*1.0);
        log_printf(15, "device=%d time: %lf\n", gop_get_myid(gop), dt);
     }
     dt = apr_time_now() - tstart2;
     dt /= (APR_USEC_PER_SEC*1.0);
     log_printf(15, "IBP time: %lf\n", dt);

     maxerr = 0;
     for (slot = 0; slot < n_bslots; slot++) {
        nerr = 0;
        j = slot * s->n_devices;
        for (i=0; i < s->n_devices; i++) {
           if (rwb_table[j+i].n_ex > 0) {
              if (gop_completed_successfully(rwb_table[j+i].gop) != OP_STATE_SUCCESS) {  //** Error
                 nerr++;  //** Increment the error count
              }

              free(rwb_table[j+i].ex_iov);
log_printf(15, "end stage i=%d gid=%d gop_completed_successfully=%d nerr=%d\n", i, gop_id(rwb_table[j+i].gop), gop_completed_successfully(rwb_table[j+i].gop), nerr);
           }

           if (rwb_table[j+i].iov != NULL) free(rwb_table[j+i].iov);
           gop_free(rwb_table[j+i].gop, OP_DESTROY);
        }

        if (nerr > maxerr) maxerr = nerr;
//        free(rwb_table);
     }

log_printf(15, "END stage maxerr=%d\n", maxerr);

     if (maxerr == 0) {
log_printf(15, "success\n");
        status = op_success_status;
     } else {
log_printf(15, "failure maxerr=%d\n", maxerr);
        status.op_status = OP_STATE_FAILURE;
        status.error_code = maxerr;
     }
  }

  //** Update the inprogress count
  segment_lock(seg);
  s->inprogress_count--;
  if (s->inprogress_count == 0) apr_thread_cond_broadcast(seg->cond);
  segment_unlock(seg);

  free(rwb_table);
  free(bcount);
  free(bused);
  free_stack(stack, 0);
  opque_free(q, OP_DESTROY);

  dt = apr_time_now() - tstart;
  dt /= (APR_USEC_PER_SEC*1.0);
  log_printf(15, "Total time: %lf\n", dt);

  return(status);
}


//***********************************************************************
//  seglun_rw_func - Performs a bounds check (growing the file if needed for writes)
//     and then calls the actual R/W operation.
//***********************************************************************

op_status_t seglun_rw_func(void *arg, int id)
{
  seglun_rw_t *sw = (seglun_rw_t *)arg;
  seglun_priv_t *s = (seglun_priv_t *)sw->seg->priv;
  int i;
  op_status_t status;
  ex_off_t new_size;
  ex_off_t pos, maxpos;

log_printf(15, "sid=" XIDT " n_iov=%d off[0]=" XOT " len[0]=" XOT " max_size=" XOT " used_size=" XOT "\n",
     segment_id(sw->seg), sw->n_iov, sw->iov[0].offset, sw->iov[0].len, s->total_size, s->used_size);

//  err = OP_STATE_SUCCESS;

  //** Find the max extent;
  maxpos = 0;
  pos = 0;
  for (i=0; i<sw->n_iov; i++) {
    pos = sw->iov[i].offset + sw->iov[i].len - 1;
    if (pos > maxpos) maxpos = pos;
//log_printf(15, "i=%d off=" XOT " len=" XOT " pos=" XOT " maxpos=" XOT "\n", i, sw->iov[i].offset, sw->iov[i].len, pos, maxpos);
  }


  segment_lock(sw->seg);
  if (maxpos >= s->total_size) { //** Need to grow it first
     if (sw->rw_mode == 1) { //** Write op so grow the file
        new_size = maxpos + s->n_devices * s->excess_block_size;
        if (s->total_size < new_size) {  //** Check again within the lock
log_printf(15, " seg=" XIDT " GROWING  curr_used_size=" XOT " curr_total_size=" XOT " new_size=" XOT " requested maxpos=" XOT "\n",
      segment_id(sw->seg), s->used_size, s->total_size, new_size, maxpos);
           status = _slun_truncate(sw->seg, sw->da, new_size, sw->timeout);
log_printf(15, " seg=" XIDT " GROWING  err=%d\n",segment_id(sw->seg), status.op_status);
           if (status.op_status != OP_STATE_SUCCESS) {
              segment_unlock(sw->seg);
              status.op_status = OP_STATE_FAILURE;  status.error_code = s->n_devices;
              return(status);
           }
        }
     } else {  //** Got a bad offset so fail the whole thing
log_printf(15, "ERROR seg=" XIDT " READ beyond EOF!  cur_size=" XOT " requested maxpos=" XOT "\n", segment_id(sw->seg), s->total_size, maxpos);
        segment_unlock(sw->seg);
        status.op_status = OP_STATE_FAILURE;  status.error_code = s->n_devices;
        return(status);
     }
  }
  segment_unlock(sw->seg);

  //** Now do the actual R/W operation
log_printf(15, "Before exec\n");
  status = seglun_rw_op(sw->seg, sw->da, sw->n_iov, sw->iov, sw->buffer, sw->boff, sw->rw_mode, sw->timeout);
//  if (status.op_status != OP_STATE_SUCCESS) err = OP_STATE_FAILURE;
log_printf(15, "After exec err=%d\n", status.op_status);

  segment_lock(sw->seg);
log_printf(15, "oldused=" XOT " maxpos=" XOT "\n", s->used_size, maxpos);

  if ((sw->rw_mode == 1) && (s->used_size <= maxpos)) s->used_size = maxpos+1;

  if (status.op_status != OP_STATE_SUCCESS) {
     s->hard_errors++;
  }

  segment_unlock(sw->seg);

  return(status);
}

//***********************************************************************
// seglun_write - Performs a segment write operation
//***********************************************************************

op_generic_t *seglun_write(segment_t *seg, data_attr_t *da, int n_iov, ex_iovec_t *iov, tbuffer_t *buffer, ex_off_t boff, int timeout)
{
  seglun_priv_t *s = (seglun_priv_t *)seg->priv;
  seglun_rw_t *sw;
  op_generic_t *gop;

  type_malloc(sw, seglun_rw_t, 1);
  sw->seg = seg;
  sw->da = da;
  sw->n_iov = n_iov;
  sw->iov = iov;
  sw->boff = boff;
  sw->buffer = buffer;
  sw->timeout = timeout;
  sw->rw_mode = 1;
  gop = new_thread_pool_op(s->tpc, NULL, seglun_rw_func, (void *)sw, free, 1);

  return(gop);
}

//***********************************************************************
// seglun_read - Read from a linear segment
//***********************************************************************

op_generic_t *seglun_read(segment_t *seg, data_attr_t *da, int n_iov, ex_iovec_t *iov, tbuffer_t *buffer, ex_off_t boff, int timeout)
{
  seglun_priv_t *s = (seglun_priv_t *)seg->priv;
  seglun_rw_t *sw;
  op_generic_t *gop;

  type_malloc(sw, seglun_rw_t, 1);
  sw->seg = seg;
  sw->da = da;
  sw->n_iov = n_iov;
  sw->iov = iov;
  sw->boff = boff;
  sw->buffer = buffer;
  sw->timeout = timeout;
  sw->rw_mode = 0;
  gop = new_thread_pool_op(s->tpc, NULL, seglun_rw_func, (void *)sw, free, 1);

  return(gop);
}


//***********************************************************************
// seglun_remove - DECrements the ref counts for the segment which could
//     result in the data being removed.
//***********************************************************************

op_generic_t *seglun_remove(segment_t *seg, data_attr_t *da, int timeout)
{
  seglun_priv_t *s = (seglun_priv_t *)seg->priv;
  op_generic_t *gop;
  opque_t *q;
  seglun_row_t *b;
  interval_skiplist_iter_t it;
  int i, j, n;

  q = new_opque();

  segment_lock(seg);
  n = interval_skiplist_count(s->isl);
  it = iter_search_interval_skiplist(s->isl, (skiplist_key_t *)NULL, (skiplist_key_t *)NULL);
  for (i=0; i<n; i++) {
     b = (seglun_row_t *)next_interval_skiplist(&it);
     for (j=0; j < s->n_devices; j++) {
        gop = ds_remove(b->block[j].data->ds, da, ds_get_cap(b->block[j].data->ds, b->block[j].data->cap, DS_CAP_MANAGE), timeout);
        opque_add(q, gop);
     }
  }
  segment_unlock(seg);

  log_printf(15, "seg=" XIDT " qid=%d ntasks=%d\n", segment_id(seg), gop_id(opque_get_gop(q)), opque_task_count(q));
  if (n == 0) {
     opque_free(q, OP_DESTROY);
     return(gop_dummy(op_success_status));
  }
  return(opque_get_gop(q));
}

//***********************************************************************
// seglun_migrate_func - Attempts to migrate any flagged allocations
//***********************************************************************

op_status_t seglun_migrate_func(void *arg, int id)
{
  seglun_inspect_t *si = (seglun_inspect_t *)arg;
  seglun_priv_t *s = (seglun_priv_t *)si->seg->priv;
  seglun_row_t *b;
  int bufsize = 10*1024;
  char info[bufsize];
  int used;
  int block_status[s->n_devices];
  int nattempted, nmigrated, err, i;
  int soft_error_fail;

  op_status_t status = op_success_status;
  interval_skiplist_iter_t it;

  soft_error_fail = (si->inspect_mode & INSPECT_SOFT_ERROR_FAIL);

  segment_lock(si->seg);

  info_printf(si->fd, 1, XIDT ": segment information: n_devices=%d n_shift=%d chunk_size=" XOT "  used_size=" XOT " total_size=" XOT " mode=%d\n", segment_id(si->seg), s->n_devices, s->n_shift, s->chunk_size, s->used_size, s->total_size, si->inspect_mode);

  nattempted = 0; nmigrated = 0;
  it = iter_search_interval_skiplist(s->isl, (skiplist_key_t *)NULL, (skiplist_key_t *)NULL);
  for (b = (seglun_row_t *)next_interval_skiplist(&it); b != NULL; b = (seglun_row_t *)next_interval_skiplist(&it)) {
    for (i=0; i < s->n_devices; i++) block_status[i] = 0;

    info_printf(si->fd, 1, XIDT ": Checking row (" XOT ", " XOT ", " XOT ")\n", segment_id(si->seg), b->seg_offset, b->seg_end, b->row_len);

    for (i=0; i < s->n_devices; i++) block_status[i] = 0;
    err = slun_row_placement_check(si->seg, si->da, b, block_status, s->n_devices, soft_error_fail, s->rsq, si->timeout);
    used = 0;
    append_printf(info, &used, bufsize, XIDT ":     slun_row_placement_check:", segment_id(si->seg));
    for (i=0; i < s->n_devices; i++) append_printf(info, &used, bufsize, " %d", block_status[i]);
    info_printf(si->fd, 1, "%s\n", info);
    if (err > 0) {
       i = slun_row_placement_fix(si->seg, si->da, b, block_status, s->n_devices, s->rsq, si->timeout);
       nmigrated +=  err - i;
       nattempted += err;

       used =0;
       append_printf(info, &used, bufsize, XIDT ":     slun_row_placement_fix:", segment_id(si->seg));
       for (i=0; i < s->n_devices; i++) append_printf(info, &used, bufsize, " %d", block_status[i]);
       info_printf(si->fd, 1, "%s\n", info);
    } else {
       nattempted = nattempted + err;
    }

  }

  segment_unlock(si->seg);

  if (nattempted != nmigrated) {
     info_printf(si->fd, 1, XIDT ": status: FAILURE (%d needed migrating, %d migrated)\n", segment_id(si->seg), nattempted, nmigrated);
     status = op_failure_status;
  } else {
     info_printf(si->fd, 1, XIDT ": status: SUCCESS (%d needed migrating, %d migrated)\n", segment_id(si->seg), nattempted, nmigrated);
  }

  return(status);
}

//***********************************************************************
// seglun_inspect_func - Checks that all the segments are available and they are the right size
//     and corrects them if requested
//***********************************************************************

op_status_t seglun_inspect_func(void *arg, int id)
{
  seglun_inspect_t *si = (seglun_inspect_t *)arg;
  seglun_priv_t *s = (seglun_priv_t *)si->seg->priv;
  seglun_row_t *b;
  rs_query_t *query;
  op_status_t status;
  interval_skiplist_iter_t it;
  int bufsize = 10*1024;
  char info[bufsize];
  int used, soft_error_fail, force_reconstruct, nforce;
  int block_status[s->n_devices];
  int i, err, option, force_repair, max_lost, total_lost, total_repaired, total_migrate, nmigrated, nlost, nrepaired;

  status = op_success_status;
  max_lost = 0;
  total_lost = 0;
  total_repaired = 0;
  total_migrate = 0;
  nmigrated = 0;
  nlost = 0;

  option = si->inspect_mode & INSPECT_COMMAND_BITS;
  soft_error_fail = (si->inspect_mode & INSPECT_SOFT_ERROR_FAIL);
  force_reconstruct = (si->inspect_mode & INSPECT_FORCE_RECONSTRUCTION);
  force_repair = 0;
  if ((option == INSPECT_QUICK_REPAIR) || (option == INSPECT_SCAN_REPAIR) || (option == INSPECT_FULL_REPAIR)) force_repair = si->inspect_mode & INSPECT_FORCE_REPAIR;

  segment_lock(si->seg);

  //** Form the query to use
  query = rs_query_dup(s->rs, s->rsq);
  if (si->query != NULL) {  //** Local query needs to be added
     rs_query_append(s->rs, query, si->query);
     rs_query_add(s->rs, &query, RSQ_BASE_OP_AND, NULL, 0, NULL, 0);
  }

info_printf(si->fd, 1, "local_query=%p\n", si->query);
  info_printf(si->fd, 1, XIDT ": segment information: n_devices=%d n_shift=%d chunk_size=" XOT "  used_size=" XOT " total_size=" XOT " mode=%d\n", segment_id(si->seg), s->n_devices, s->n_shift, s->chunk_size, s->used_size, s->total_size, si->inspect_mode);

  it = iter_search_interval_skiplist(s->isl, (skiplist_key_t *)NULL, (skiplist_key_t *)NULL);
  for (b = (seglun_row_t *)next_interval_skiplist(&it); b != NULL; b = (seglun_row_t *)next_interval_skiplist(&it)) {
    for (i=0; i < s->n_devices; i++) block_status[i] = 0;

    info_printf(si->fd, 1, XIDT ": Checking row (" XOT ", " XOT ", " XOT ")\n", segment_id(si->seg), b->seg_offset, b->seg_end, b->row_len);
    nlost = slun_row_size_check(si->seg, si->da, b, block_status, s->n_devices, force_repair, si->timeout);
    used = 0;
    append_printf(info, &used, bufsize, XIDT ":     slun_row_size_check:", segment_id(si->seg));
    for (i=0; i < s->n_devices; i++) append_printf(info, &used, bufsize, " %d", block_status[i]);
    info_printf(si->fd, 1, "%s\n", info);

    if (max_lost < nlost) max_lost = nlost;

    nrepaired = 0;
    if ((force_repair > 0) && (nlost > 0)) {
       info_printf(si->fd, 1, XIDT ":     Attempting to pad the row\n", segment_id(si->seg));
       err = slun_row_pad_fix(si->seg, si->da, b, block_status, s->n_devices, si->timeout);

       used = 0;
       append_printf(info, &used, bufsize, XIDT ":     slun_row_pad_fix:", segment_id(si->seg));
       for (i=0; i < s->n_devices; i++) append_printf(info, &used, bufsize, " %d", block_status[i]);
       info_printf(si->fd, 1, "%s\n", info);

       if (max_lost < err) max_lost = err;

       info_printf(si->fd, 1, XIDT ":     Attempting to replace missing row allocations\n", segment_id(si->seg));
       i = 0;  //** Iteratively try and repair the row
       do {
         err = slun_row_replace_fix(si->seg, si->da, b, block_status, s->n_devices, query, si->timeout);
         used = 0;
         append_printf(info, &used, bufsize, XIDT ":     slun_row_replace_fix:", segment_id(si->seg));
         for (i=0; i < s->n_devices; i++) append_printf(info, &used, bufsize, " %d", block_status[i]);
         info_printf(si->fd, 1, "%s\n", info);

         i++;
       } while ((err > 0) && (i<5));

       nrepaired = nlost - err;
    }

    err = 0;
    for (i=0; i < s->n_devices; i++) if (block_status[i] != 0) err++;
    if (err != 0) goto fail;

log_printf(0, "BEFORE_PLACEMENT_CHECK\n");
    err = slun_row_placement_check(si->seg, si->da, b, block_status, s->n_devices, soft_error_fail, query, si->timeout);
    for (i=0; i < s->n_devices; i++) append_printf(info, &used, bufsize, " %d", block_status[i]);

    total_migrate += err;
    used = 0;
    append_printf(info, &used, bufsize, XIDT ":     slun_row_placement_check:", segment_id(si->seg));
    for (i=0; i < s->n_devices; i++) append_printf(info, &used, bufsize, " %d", block_status[i]);
    info_printf(si->fd, 1, "%s\n", info);
log_printf(0, "AFTER_PLACEMENT_CHECK\n");
    if ((err > 0) && ((option == INSPECT_QUICK_REPAIR) || (option == INSPECT_SCAN_REPAIR) || (option == INSPECT_FULL_REPAIR))) {
       if (force_reconstruct == 0) {
          i = slun_row_placement_fix(si->seg, si->da, b, block_status, s->n_devices, query, si->timeout);
          nmigrated += err - i;
          used = 0;
          append_printf(info, &used, bufsize, XIDT ":     slun_row_placement_fix:", segment_id(si->seg));
          for (i=0; i < s->n_devices; i++) append_printf(info, &used, bufsize, " %d", block_status[i]);
          info_printf(si->fd, 1, "%s\n", info);
       } else if (force_repair > 0) {  //** Don't want to use depot-depot copies so instead make a blank allocation and let the higher level handle things
          i = 0;  //** Iteratively try and repair the row
          nforce = err;
          if (max_lost < err) max_lost = err;
          do {
            err = slun_row_replace_fix(si->seg, si->da, b, block_status, s->n_devices, query, si->timeout);
            used = 0;
            append_printf(info, &used, bufsize, XIDT ":     slun_row_replace_fix:", segment_id(si->seg));
            for (i=0; i < s->n_devices; i++) append_printf(info, &used, bufsize, " %d", block_status[i]);
            info_printf(si->fd, 1, "%s\n", info);
            i++;
          } while ((err > 0) && (i<5));

          nmigrated += nforce - err;
       }

    }

fail:
    total_lost += nlost;
    total_repaired += nrepaired;
  }

  segment_unlock(si->seg);

  i = total_lost - total_repaired + total_migrate - nmigrated;
  if (i != 0) {
     info_printf(si->fd, 1, XIDT ": status: FAILURE (%d max dev/row lost, %d lost, %d repaired, %d need(s) moving, %d moved)\n", segment_id(si->seg), max_lost, total_lost, total_repaired, total_migrate, nmigrated);
     status = op_failure_status;
  } else {
     info_printf(si->fd, 1, XIDT ": status: SUCCESS (%d max dev/row lost, %d lost, %d repaired, %d need(s) moving, %d moved)\n", segment_id(si->seg), max_lost, total_lost, total_repaired, total_migrate, nmigrated);
  }
//  free(si);

  rs_query_destroy(s->rs, query);

  status.error_code = max_lost;
  return(status);
}

//***********************************************************************
//  seglun_inspect_func - Does the actual segment inspection operations
//***********************************************************************

op_generic_t *seglun_inspect(segment_t *seg, data_attr_t *da, info_fd_t *fd, int mode, ex_off_t bufsize, rs_query_t *query, int timeout)
{
  seglun_priv_t *s = (seglun_priv_t *)seg->priv;
  op_generic_t *gop;
  op_status_t err;
  seglun_inspect_t *si;
  int option;

  gop = NULL;
  option = mode & INSPECT_COMMAND_BITS;

//log_printf(0, "mode=%d option=%d\n", mode, option); flush_log();
//printf("mode=%d option=%d\n", mode, option); fflush(stdout);

  switch (option) {
    case (INSPECT_QUICK_CHECK):
    case (INSPECT_SCAN_CHECK):
    case (INSPECT_FULL_CHECK):
    case (INSPECT_QUICK_REPAIR):
    case (INSPECT_SCAN_REPAIR):
    case (INSPECT_FULL_REPAIR):
        type_malloc(si, seglun_inspect_t, 1);
        si->seg = seg;
        si->da = da;
        si->fd = fd;
        si->inspect_mode = mode;
        si->bufsize = bufsize;
        si->timeout = timeout;
        si->query = query;
        gop = new_thread_pool_op(s->tpc, NULL, seglun_inspect_func, (void *)si, free, 1);
        break;
    case (INSPECT_MIGRATE):
//log_printf(0, "INSPECT_MIGRATE\n");
        type_malloc(si, seglun_inspect_t, 1);
        si->seg = seg;
        si->da = da;
        si->fd = fd;
        si->inspect_mode = mode;
        si->bufsize = bufsize;
        si->timeout = timeout;
        si->query = query;
        gop = new_thread_pool_op(s->tpc, NULL, seglun_migrate_func, (void *)si, free, 1);
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
// seglun_flush - Flushes a segment
//***********************************************************************

op_generic_t *seglun_flush(segment_t *seg, data_attr_t *da, ex_off_t lo, ex_off_t hi, int timeout)
{
  return(gop_dummy(op_success_status));
}

//***********************************************************************
// seglun_clone_func - Clone data from the segment
//***********************************************************************

op_status_t seglun_clone_func(void *arg, int id)
{
  seglun_clone_t *slc = (seglun_clone_t *)arg;
  seglun_priv_t *ss = (seglun_priv_t *)slc->sseg->priv;
  seglun_priv_t *sd = (seglun_priv_t *)slc->dseg->priv;
  interval_skiplist_iter_t its, itd;
  seglun_row_t *bd, *bs;
  ex_off_t row_size;
  int err, dir, i;
  opque_t *q;
  op_generic_t *gop = NULL;
  op_status_t status;

  //** SEe if we are using an old seg.  If so we need to trunc it first
  if (slc->trunc == 1) {
     gop_sync_exec(segment_truncate(slc->dseg, slc->da, 0, slc->timeout));
  }

  if (ss->total_size == 0) return(op_success_status);  //** No data to clone

  //** Grow the file size but keep the same breaks as the original
  its = iter_search_interval_skiplist(ss->isl, (skiplist_key_t *)NULL, (skiplist_key_t *)NULL);
  row_size = -1;
  bs = NULL;
  i = 0;
  while ((bs = (seglun_row_t *)next_interval_skiplist(&its)) != NULL) {
     row_size = bs->row_len;
     if (bs->row_len != ss->max_row_size) {
        //** grow the destination to the same size as the source
        log_printf(15, "dseg=" XIDT " Growing dest to " XOT "\n", segment_id(slc->dseg), bs->seg_end+1);
        err = gop_sync_exec(segment_truncate(slc->dseg, slc->da, bs->seg_end+1, slc->timeout));
        if (err != OP_STATE_SUCCESS) {
           log_printf(15, "Error growing destination! dseg=" XIDT "\n", segment_id(slc->dseg));
           sd->grow_break = 0; //** Undo the break flag
           return(op_failure_status);
        }
        sd->used_size = ss->used_size;
        sd->grow_break = 1; //** Flag a break for the next grow operation
        row_size = -1;
     }
  }

  //** Do the final grow if needed
  if (row_size != -1) {
     log_printf(15, "dseg=" XIDT " Growing dest to " XOT "\n", segment_id(slc->dseg), bs->seg_end+1);
     err = gop_sync_exec(segment_truncate(slc->dseg, slc->da, bs->seg_end+1, slc->timeout));
     if (err != OP_STATE_SUCCESS) {
        log_printf(15, "Error growing destination! dseg=" XIDT "\n", segment_id(slc->dseg));
        sd->grow_break = 0; //** Undo the break flag
        return(op_failure_status);
     }
     sd->used_size = ss->used_size;
  }

  sd->grow_break = 0; //** Fished growing so undo the break flag



  //** Generate the copy list
  q = new_opque();
  opque_start_execution(q);
  dir = ((slc->mode & DS_PULL) > 0) ? DS_PULL : DS_PUSH;

  its = iter_search_interval_skiplist(ss->isl, (skiplist_key_t *)NULL, (skiplist_key_t *)NULL);
  itd = iter_search_interval_skiplist(sd->isl, (skiplist_key_t *)NULL, (skiplist_key_t *)NULL);
  while ((bs = (seglun_row_t *)next_interval_skiplist(&its)) != NULL) {
     bd = (seglun_row_t *)next_interval_skiplist(&itd);
     for (i=0; i < ss->n_devices; i++) {
        gop = ds_copy(bd->block[i].data->ds, slc->da, dir, NS_TYPE_SOCK, "",
                 ds_get_cap(bs->block[i].data->ds, bs->block[i].data->cap, DS_CAP_READ), bs->block[i].cap_offset,
                 ds_get_cap(bd->block[i].data->ds, bd->block[i].data->cap, DS_CAP_WRITE), bd->block[i].cap_offset,
                 bs->block_len, slc->timeout);
        opque_add(q, gop);
     }
  }

  //** Wait for the copying to finish
  opque_waitall(q);
  status = (opque_tasks_failed(q) == 0) ? op_success_status : op_failure_status;

  opque_free(q, OP_DESTROY);

  return(status);
}


//***********************************************************************
// seglun_clone - Clones a segment
//***********************************************************************

op_generic_t *seglun_clone(segment_t *seg, data_attr_t *da, segment_t **clone_seg, int mode, void *attr, int timeout)
{
  segment_t *clone;
  seglun_priv_t *ss = (seglun_priv_t *)seg->priv;
  seglun_priv_t *sd;
  op_generic_t *gop;
  seglun_clone_t *slc;
  int use_existing = (*clone_seg != NULL) ? 1 : 0;

    //** Make the base segment
//log_printf(0, " before clone create\n");
  if (use_existing == 0) *clone_seg = segment_lun_create(seg->ess);
//log_printf(0, " after clone create\n");
  clone = *clone_seg;
  sd = (seglun_priv_t *)clone->priv;

log_printf(15, "use_existing=%d sseg=" XIDT " dseg=" XIDT "\n", use_existing, segment_id(seg), segment_id(clone));

  //** Copy the private constants
  sd->max_block_size = ss->max_block_size;
  sd->excess_block_size = ss->excess_block_size;
  sd->max_row_size = ss->max_row_size;
  sd->chunk_size = ss->chunk_size;
  sd->stripe_size = ss->stripe_size;
  sd->n_devices = ss->n_devices;
  sd->n_shift = ss->n_shift;

  //** Copy the header
  if ((seg->header.name != NULL) && (use_existing == 0)) clone->header.name = strdup(seg->header.name);

  //** Copy the default rs query
  if (use_existing == 0) {
     if (attr == NULL) {
        sd->rsq = rs_query_dup(sd->rs, ss->rsq);
     } else {
        sd->rsq = rs_query_parse(sd->rs, (char *)attr);
     }
  }

  //** Now copy the data if needed
   if (mode == CLONE_STRUCTURE) {
    if (use_existing == 1) {
       gop = segment_truncate(clone, da, 0, timeout);
    } else {
       gop = gop_dummy(op_success_status);
    }
  } else {
    type_malloc(slc, seglun_clone_t, 1);
    slc->sseg = seg;
    slc->dseg = clone;
    slc->da = da;
    slc->mode = mode;
    slc->timeout = timeout;
    slc->trunc = use_existing;
    gop = new_thread_pool_op(sd->tpc, NULL, seglun_clone_func, (void *)slc, free, 1);
  }

  return(gop);
}

//***********************************************************************
// seglun_size - Returns the segment size.
//***********************************************************************

ex_off_t seglun_size(segment_t *seg)
{
  seglun_priv_t *s = (seglun_priv_t *)seg->priv;
  ex_off_t size;

  segment_lock(seg);
  size = s->used_size;
  segment_unlock(seg);

  return(size);
}

//***********************************************************************
// seglun_block_size - Returns the segment block size.
//***********************************************************************

ex_off_t seglun_block_size(segment_t *seg)
{
  seglun_priv_t *s = (seglun_priv_t *)seg->priv;

  return(s->stripe_size);
}

//***********************************************************************
// seglun_signature - Generates the segment signature
//***********************************************************************

int seglun_signature(segment_t *seg, char *buffer, int *used, int bufsize)
{
  seglun_priv_t *s = (seglun_priv_t *)seg->priv;

  append_printf(buffer, used, bufsize, "lun(\n");
  append_printf(buffer, used, bufsize, "    n_devices=%d\n", s->n_devices);
  append_printf(buffer, used, bufsize, "    n_shift=%d\n", s->n_shift);
  append_printf(buffer, used, bufsize, "    chunk_size=" XOT "\n", s->chunk_size);
  append_printf(buffer, used, bufsize, ")\n");

  return(0);
}

//***********************************************************************
// seglun_serialize_text -Convert the segment to a text based format
//***********************************************************************

int seglun_serialize_text(segment_t *seg, exnode_exchange_t *exp)
{
  seglun_priv_t *s = (seglun_priv_t *)seg->priv;
  int bufsize=10*1024;
  char segbuf[bufsize];
  char *ext, *etext;
  int sused, i;
  seglun_row_t *b;
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
  append_printf(segbuf, &sused, bufsize, "type=%s\n", SEGMENT_TYPE_LUN);
  append_printf(segbuf, &sused, bufsize, "ref_count=%d\n", seg->ref_count);

  //** default resource query
  if (s->rsq != NULL) {
     ext = rs_query_print(s->rs, s->rsq);
     etext = escape_text("=", '\\', ext);
     append_printf(segbuf, &sused, bufsize, "query_default=%s\n", etext);  free(etext); free(ext);
  }

  append_printf(segbuf, &sused, bufsize, "n_devices=%d\n", s->n_devices);
  append_printf(segbuf, &sused, bufsize, "n_shift=%d\n", s->n_shift);

  //** Basic size info
  append_printf(segbuf, &sused, bufsize, "max_block_size=" XOT "\n", s->max_block_size);
  append_printf(segbuf, &sused, bufsize, "excess_block_size=" XOT "\n", s->excess_block_size);
  append_printf(segbuf, &sused, bufsize, "max_size=" XOT "\n", s->total_size);
  append_printf(segbuf, &sused, bufsize, "used_size=" XOT "\n", s->used_size);
  append_printf(segbuf, &sused, bufsize, "chunk_size=" XOT "\n", s->chunk_size);

  //** Cycle through the blocks storing both the segment block information and also the cap blocks
  it = iter_search_interval_skiplist(s->isl, (skiplist_key_t *)NULL, (skiplist_key_t *)NULL);
  while ((b = (seglun_row_t *)next_interval_skiplist(&it)) != NULL) {

//log_printf(0, "seg=" XIDT " block seg_off=" XOT " end=" XOT " row_len=" XOT "\n", segment_id(seg), b->seg_offset, b->seg_end, b->row_len);

     //** Add the segment stripe information
     append_printf(segbuf, &sused, bufsize, "row=" XOT ":" XOT ":" XOT, b->seg_offset, b->seg_end, b->row_len);
     for (i=0; i < s->n_devices; i++) {
        data_block_serialize(b->block[i].data, cap_exp); //** Add the cap
//log_printf(0, "seg=" XIDT "        dev=%d bid=" XIDT " cap_offset=" XOT "\n", segment_id(seg), i, b->block[i].data->id, b->block[i].cap_offset);
        append_printf(segbuf, &sused, bufsize, ":" XIDT ":" XOT, b->block[i].data->id, b->block[i].cap_offset);
     }
     append_printf(segbuf, &sused, bufsize, "\n");
  }


  //** Merge everything together and return it
  exnode_exchange_append(exp, cap_exp);
  exnode_exchange_destroy(cap_exp);

  exnode_exchange_append_text(exp, segbuf);

  return(0);
}

//***********************************************************************
// seglun_serialize_proto -Convert the segment to a protocol buffer
//***********************************************************************

int seglun_serialize_proto(segment_t *seg, exnode_exchange_t *exp)
{
//  seglun_priv_t *s = (seglun_priv_t *)seg->priv;

  return(-1);
}

//***********************************************************************
// seglun_serialize -Convert the segment to a more portable format
//***********************************************************************

int seglun_serialize(segment_t *seg, exnode_exchange_t *exp)
{
  if (exp->type == EX_TEXT) {
     return(seglun_serialize_text(seg, exp));
  } else if (exp->type == EX_PROTOCOL_BUFFERS) {
     return(seglun_serialize_proto(seg, exp));
  }

  return(-1);
}

//***********************************************************************
// seglun_deserialize_text -Read the text based segment
//***********************************************************************

int seglun_deserialize_text(segment_t *seg, ex_id_t id, exnode_exchange_t *exp)
{
  seglun_priv_t *s = (seglun_priv_t *)seg->priv;
  int bufsize=1024;
  char seggrp[bufsize];
  char *text, *etext, *token, *bstate, *key, *value;
  int fin, i, fail;
  seglun_row_t *b;
  seglun_block_t *block;
  inip_file_t *fd;
  inip_group_t *g;
  inip_element_t *ele;

  //** Parse the ini text
  fd = exp->text.fd;

  fail = 0;  //** Default to no failure

  //** Make the segment section name
  snprintf(seggrp, bufsize, "segment-" XIDT, id);

  //** Get the segment header info
  seg->header.id = id;
  seg->header.type = SEGMENT_TYPE_LUN;
  seg->header.name = inip_get_string(fd, seggrp, "name", "");

  //** default resource query
  etext = inip_get_string(fd, seggrp, "query_default", "");
  text = unescape_text('\\', etext);
  s->rsq = rs_query_parse(s->rs, text);
  free(text); free(etext);

  s->n_devices = inip_get_integer(fd, seggrp, "n_devices", 2);

  //** Basic size info
  s->max_block_size = inip_get_integer(fd, seggrp, "max_block_size", 10*1024*1024);
  s->excess_block_size = inip_get_integer(fd, seggrp, "excess_block_size", s->max_block_size/4);
  s->total_size = inip_get_integer(fd, seggrp, "max_size", 0);
  s->used_size = inip_get_integer(fd, seggrp, "used_size", 0);
  if (s->used_size > s->total_size) s->used_size = s->total_size;  //** Sanity check the size
  s->chunk_size = inip_get_integer(fd, seggrp, "chunk_size", 16*1024);
  s->n_shift = inip_get_integer(fd, seggrp, "n_shift", 1);

  //** Make sure the mac block size is a mulitple of the chunk size
  s->max_block_size = (s->max_block_size / s->chunk_size);
  s->max_block_size = s->max_block_size * s->chunk_size;
  s->max_row_size = s->max_block_size * s->n_devices;
  s->stripe_size = s->n_devices * s->chunk_size;

  //** Cycle through the blocks storing both the segment block information and also the cap blocks
  g = inip_find_group(fd, seggrp);
  ele = inip_first_element(g);
  while (ele != NULL) {
     key = inip_get_element_key(ele);
     if (strcmp(key, "row") == 0) {
        type_malloc_clear(b, seglun_row_t, 1);
        type_malloc_clear(block, seglun_block_t, s->n_devices);
        b->block = block;
        b->rwop_index = -1;

        //** Parse the segment line
        value = inip_get_element_value(ele);
        token = strdup(value);
        sscanf(escape_string_token(token, ":", '\\', 0, &bstate, &fin), XOT, &(b->seg_offset));
        sscanf(escape_string_token(NULL, ":", '\\', 0, &bstate, &fin), XOT, &(b->seg_end));
        sscanf(escape_string_token(NULL, ":", '\\', 0, &bstate, &fin), XOT, &(b->row_len));
        b->block_len = b->row_len / s->n_devices;

        for (i=0; i< s->n_devices; i++) {
           sscanf(escape_string_token(NULL, ":", '\\', 0, &bstate, &fin), XIDT, &id);
           sscanf(escape_string_token(NULL, ":", '\\', 0, &bstate, &fin), XOT, &(block[i].cap_offset));

           //** Find the cooresponding cap
           block[i].data = data_block_deserialize(seg->ess, id, exp);
           if (block[i].data == NULL) {
              log_printf(0, "Missing data block!  block id=" XIDT " seg=" XIDT "\n", id, segment_id(seg));
              fail = 1;
           } else {
              atomic_inc(block[i].data->ref_count);
           }

        }
        free(token);

       //** Finally add it to the ISL
       insert_interval_skiplist(s->isl, (skiplist_key_t *)&(b->seg_offset), (skiplist_key_t *)&(b->seg_end), (skiplist_data_t *)b);
     }

     ele = inip_next_element(ele);
  }

  return(fail);
}

//***********************************************************************
// seglun_deserialize_proto - Read the prot formatted segment
//***********************************************************************

int seglun_deserialize_proto(segment_t *seg, ex_id_t id, exnode_exchange_t *exp)
{
  return(-1);
}

//***********************************************************************
// seglun_deserialize -Convert from the portable to internal format
//***********************************************************************

int seglun_deserialize(segment_t *seg, ex_id_t id, exnode_exchange_t *exp)
{
  if (exp->type == EX_TEXT) {
     return(seglun_deserialize_text(seg, id, exp));
  } else if (exp->type == EX_PROTOCOL_BUFFERS) {
     return(seglun_deserialize_proto(seg, id, exp));
  }

  return(-1);
}


//***********************************************************************
// seglun_destroy - Destroys a linear segment struct (not the data)
//***********************************************************************

void seglun_destroy(segment_t *seg)
{
  int i, j, n;
  interval_skiplist_iter_t it;
  seglun_row_t **b_list;
  seglun_priv_t *s = (seglun_priv_t *)seg->priv;

  //** Check if it's still in use
log_printf(15, "seglun_destroy: seg->id=" XIDT " ref_count=%d\n", segment_id(seg), seg->ref_count);

  if (seg->ref_count > 0) return;

  //** Disable notification about mapping changes
  rs_unregister_mapping_updates(s->rs, &(s->notify));

  n = interval_skiplist_count(s->isl);
  type_malloc_clear(b_list, seglun_row_t *, n);
  it = iter_search_interval_skiplist(s->isl, (skiplist_key_t *)NULL, (skiplist_key_t *)NULL);
  for (i=0; i<n; i++) {
     b_list[i] = (seglun_row_t *)next_interval_skiplist(&it);
  }
  destroy_interval_skiplist(s->isl);

  for (i=0; i<n; i++) {
     for (j=0; j<s->n_devices; j++) {
       if (b_list[i]->block[j].data != NULL) {
          atomic_dec(b_list[i]->block[j].data->ref_count);
          data_block_destroy(b_list[i]->block[j].data);
       }
     }
     free(b_list[i]->block);
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
// segment_linear_create - Creates a linear segment
//***********************************************************************

segment_t *segment_lun_create(void *arg)
{
  service_manager_t *es = (service_manager_t *)arg;
  seglun_priv_t *s;
  segment_t *seg;

//log_printf(15, "ESS es=%p\n", es);
//log_printf(15, "ESS es->rs=%p\n", es->rs);

//log_printf(15, "creating new segment\n");
  //** Make the space
  type_malloc_clear(seg, segment_t, 1);
  type_malloc_clear(s, seglun_priv_t, 1);

  s->isl = create_interval_skiplist(&skiplist_compare_ex_off, NULL, NULL, NULL);
  seg->priv = s;
  s->grow_break = 0;
  s->total_size = 0;
  s->used_size = 0;
  s->chunk_size = 16*1024;
  s->max_block_size = 10*1024*1024;;
  s->excess_block_size = 1*1024*1024;
  s->max_row_size = s->max_block_size;
  s->n_devices = 1;
  s->stripe_size = s->n_devices * s->chunk_size;
  s->n_shift = 1;
  s->rsq = NULL;
  s->grow_time = 0;
  s->grow_count = 0;

  generate_ex_id(&(seg->header.id));
  atomic_set(seg->ref_count, 0);
  seg->header.type = SEGMENT_TYPE_LUN;

  assert(apr_pool_create(&(seg->mpool), NULL) == APR_SUCCESS);
  apr_thread_mutex_create(&(seg->lock), APR_THREAD_MUTEX_DEFAULT, seg->mpool);
  apr_thread_cond_create(&(seg->cond), seg->mpool);

  seg->ess = es;
  s->tpc = lookup_service(es, ESS_RUNNING, ESS_TPC_UNLIMITED);
  s->rs = lookup_service(es, ESS_RUNNING, ESS_RS);
  s->ds = lookup_service(es, ESS_RUNNING, ESS_DS);

  //** Set up rempa notifications
  s->notify.lock = seg->lock;
  s->notify.map_version = -1;  //** This should trigger a remap on the first R/W op.
  rs_register_mapping_updates(s->rs, &(s->notify));

  seg->fn.read = seglun_read;
  seg->fn.write = seglun_write;
  seg->fn.inspect = seglun_inspect;
  seg->fn.truncate = seglun_truncate;
  seg->fn.remove = seglun_remove;
  seg->fn.flush = seglun_flush;
  seg->fn.clone = seglun_clone;
  seg->fn.signature = seglun_signature;
  seg->fn.size = seglun_size;
  seg->fn.block_size = seglun_block_size;
  seg->fn.serialize = seglun_serialize;
  seg->fn.deserialize = seglun_deserialize;
  seg->fn.destroy = seglun_destroy;

  return(seg);
}

//***********************************************************************
// segment_linear_load - Loads a linear segment from ini/ex3
//***********************************************************************

segment_t *segment_lun_load(void *arg, ex_id_t id, exnode_exchange_t *ex)
{
  segment_t *seg = segment_lun_create(arg);
  if (segment_deserialize(seg, id, ex) != 0) {
     segment_destroy(seg);
     seg = NULL;
  }
  return(seg);
}
