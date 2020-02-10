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
// Routines for managing a Logical UNit segment driver which mimics a
// traditional SAN LUN device
//***********************************************************************

#define _log_module_index 177

#include <apr_errno.h>
#include <apr_hash.h>
#include <apr_pools.h>
#include <apr_thread_cond.h>
#include <apr_thread_mutex.h>
#include <apr_time.h>
#include <gop/gop.h>
#include <gop/opque.h>
#include <gop/tp.h>
#include <gop/types.h>
#include <lio/segment.h>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <tbx/append_printf.h>
#include <tbx/assert_result.h>
#include <tbx/atomic_counter.h>
#include <tbx/fmttypes.h>
#include <tbx/iniparse.h>
#include <tbx/interval_skiplist.h>
#include <tbx/log.h>
#include <tbx/network.h>
#include <tbx/random.h>
#include <tbx/range_stack.h>
#include <tbx/skiplist.h>
#include <tbx/stack.h>
#include <tbx/string_token.h>
#include <tbx/transfer_buffer.h>
#include <tbx/type_malloc.h>

#include "blacklist.h"
#include "data_block.h"
#include "ds.h"
#include "ex3.h"
#include "ex3/compare.h"
#include "ex3/header.h"
#include "ex3/system.h"
#include "rs.h"
#include "rs/query_base.h"
#include "segment/lun.h"
#include "service_manager.h"

// Forward declaration
const lio_segment_vtable_t lio_seglun_vtable;

typedef struct {
    lio_data_block_t *data;    //** Data block
    ex_off_t cap_offset;   //** Starting location to use data in the cap
    int read_err_count;    //** Read errors
    int write_err_count;   //** Write errors
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
    gop_opque_t *q;
    lio_segment_t *seg;
    data_probe_t **probe;
    seglun_row_t **block;
} seglun_check_t;

typedef struct {
    lio_segment_t *seg;
    data_attr_t *da;
    ex_off_t new_size;
    int timeout;
} seglun_truncate_t;

typedef struct {
    lio_segment_t *seg;
    data_attr_t *da;
    tbx_log_fd_t *fd;
    lio_inspect_args_t *args;
    ex_off_t bufsize;
    int inspect_mode;
    int timeout;
} seglun_inspect_t;

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
} seglun_rw_t;

typedef struct {
    lio_segment_t *sseg;
    lio_segment_t *dseg;
    data_attr_t *da;
    ex_off_t max_transfer;
    int mode;
    int timeout;
    int trunc;
} seglun_clone_t;

typedef struct {
    gop_op_generic_t *gop;
    ex_tbx_iovec_t *ex_iov;
    tbx_iovec_t *iov;
    seglun_block_t *block;
    tbx_tbuf_t buffer;
    int n_ex;
    int c_ex;
    int n_iov;
    int c_iov;
    ex_off_t len;
} lun_rw_row_t;

//***********************************************************************
// _slun_perform_remap - Does a cap remap
//   **NOTE: Assumes the segment is locked
//***********************************************************************

void _slun_perform_remap(lio_segment_t *seg)
{
    lio_seglun_priv_t *s = (lio_seglun_priv_t *)seg->priv;
    tbx_isl_iter_t it;
    seglun_row_t *b;
    int i;

    log_printf(5, "START\n");
    it = tbx_isl_iter_search(s->isl, (tbx_sl_key_t *)NULL, (tbx_sl_key_t *)NULL);
    while ((b = (seglun_row_t *)tbx_isl_next(&it)) != NULL) {
        for (i=0; i<s->n_devices; i++) {
            rs_translate_cap_set(s->rs, b->block[i].data->rid_key, b->block[i].data->cap);
            log_printf(15, "i=%d rcap=%s\n", i, (char *)ds_get_cap(s->ds, b->block[i].data->cap, DS_CAP_READ));
        }
    }
    log_printf(5, "END\n");

    return;
}


//***********************************************************************
// slun_row_placement_check - Checks the placement of each allocation
//***********************************************************************

int slun_row_placement_check(lio_segment_t *seg, data_attr_t *da, seglun_row_t *b, int *block_status, int n_devices, int soft_error_fail, rs_query_t *query, lio_inspect_args_t *args, int timeout)
{
    lio_seglun_priv_t *s = (lio_seglun_priv_t *)seg->priv;
    int i, nbad;
    lio_rs_hints_t hints_list[n_devices];
    char *migrate;
    gop_op_generic_t *gop;
    apr_hash_t *rid_changes;
    lio_rid_inspect_tweak_t *rid;
    apr_thread_mutex_t *rid_lock;

    rid_changes = args->rid_changes;
    rid_lock = args->rid_lock;

    //** Make the fixed list table
    for (i=0; i<n_devices; i++) {
        hints_list[i].fixed_rid_key = b->block[i].data->rid_key;
        hints_list[i].status = RS_ERROR_OK;
        hints_list[i].local_rsq = NULL;
        hints_list[i].pick_from = NULL;
        migrate = data_block_get_attr(b->block[i].data, "migrate");
        if (migrate != NULL) {
            hints_list[i].local_rsq = rs_query_parse(s->rs, migrate);
        }
    }

    //** Now call the query check
    gop = rs_data_request(s->rs, NULL, query, NULL, NULL, 0, hints_list, n_devices, n_devices, 0, timeout);
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
        } else if (rid_changes) { //** See if the allocation can be shuffled
            if (rid_lock != NULL) apr_thread_mutex_lock(rid_lock);
            rid = apr_hash_get(rid_changes, b->block[i].data->rid_key, APR_HASH_KEY_STRING);
            if (rid != NULL) {
                if ((rid->rid->state != 0) || (rid->rid->delta >= 0)) {
                    rid = NULL;
                }
                if (rid != NULL) {  //** See about shuffling the data
                    nbad++;
                    block_status[i] = -103;
                }
            }
            if (rid_lock != NULL) apr_thread_mutex_unlock(rid_lock);

        }

        if (hints_list[i].local_rsq != NULL) {
            rs_query_destroy(s->rs, hints_list[i].local_rsq);
        }
    }

    return(nbad);
}

//***********************************************************************
// slun_row_placement_fix - Moves the allocations to satisfy the placement
//     constraints
//***********************************************************************

int slun_row_placement_fix(lio_segment_t *seg, data_attr_t *da, seglun_row_t *b, int *block_status, int n_devices, lio_inspect_args_t *args, int timeout)
{
    lio_seglun_priv_t *s = (lio_seglun_priv_t *)seg->priv;
    int i, j, k, nbad, ngood, loop, cleanup_index;
    int missing[n_devices], m, todo;
    char *cleanup_key[5*n_devices];
    lio_rs_request_t req[n_devices];
    lio_rid_inspect_tweak_t *rid_pending[n_devices];
    rs_query_t *rsq;
    apr_hash_t *rid_changes;
    lio_rid_inspect_tweak_t *rid;
    apr_thread_mutex_t *rid_lock;

    lio_rs_hints_t hints_list[n_devices];
    lio_data_block_t *db[n_devices], *dbs, *dbd, *dbold[n_devices];
    data_cap_set_t *cap[n_devices];

    char *migrate;
    gop_op_generic_t *gop;
    gop_opque_t *q;

    rid_changes = args->rid_changes;
    rid_lock = args->rid_lock;
    rsq = rs_query_dup(s->rs, args->query);

    cleanup_index = 0;
    loop = 0;
    do {
        q = gop_opque_new();

        //** Make the fixed list mapping table
        memset(db, 0, sizeof(db));
        nbad = n_devices-1;
        ngood = 0;
        m = 0;
        if (rid_lock != NULL) apr_thread_mutex_lock(rid_lock);
        for (i=0; i<n_devices; i++) {
            rid = NULL;
            if (rid_changes != NULL) {
                rid = apr_hash_get(rid_changes, b->block[i].data->rid_key, APR_HASH_KEY_STRING);
                if (rid != NULL) {
                    if ((rid->rid->state != 0) || (rid->rid->delta >= 0)) {
                        rid = NULL;
                    }
                }
            }
            rid_pending[i] = rid;

            if ((block_status[i] == 0) && (rid == NULL)) {
                j = ngood;
                hints_list[ngood].fixed_rid_key = b->block[i].data->rid_key;
                hints_list[ngood].status = RS_ERROR_OK;
                hints_list[ngood].local_rsq = NULL;
                hints_list[ngood].pick_from = NULL;
                ngood++;
            } else {
                j = nbad;
                hints_list[nbad].local_rsq = NULL;
                hints_list[nbad].fixed_rid_key = NULL;
                hints_list[nbad].status = RS_ERROR_OK;
                hints_list[nbad].pick_from = NULL;
                if (rid != NULL) {
                    hints_list[nbad].pick_from = rid->pick_pool;
                    rid->rid->delta += b->block_len;
                    rid->rid->state = ((llabs(rid->rid->delta) <= rid->rid->tolerance) || (rid->rid->tolerance == 0)) ? 1 : 0;
                    log_printf(5, "i=%d rid_key=%s, pick_pool_count=%d\n", i, b->block[i].data->rid_key, apr_hash_count(rid->pick_pool));
                }
                req[m].rid_index = nbad;
                req[m].size = b->block_len;
                db[m] = data_block_create(s->ds);
                cap[m] = db[m]->cap;
                missing[m] = i;
                nbad--;
                m++;
            }

            if (hints_list[j].local_rsq != NULL) {
                rs_query_destroy(s->rs, hints_list[j].local_rsq);
            }

            migrate = data_block_get_attr(b->block[i].data, "migrate");
            if (migrate != NULL) {
                hints_list[j].local_rsq = rs_query_parse(s->rs, migrate);
            }
//log_printf(0, "i=%d ngood=%d nbad=%d m=%d\n", i, ngood, nbad, m);
        }

        // 3=ignore fixed and it's ok to return a partial list
        gop = rs_data_request(s->rs, da, rsq, cap, req, m, hints_list, ngood, n_devices, 3, timeout);

        if (rid_lock != NULL) apr_thread_mutex_unlock(rid_lock);  //** The data request will use the rid_changes table in constructing the ops

        gop_waitall(gop);
        gop_free(gop, OP_DESTROY);

        //** Process the results
        opque_start_execution(q);
        for (j=0; j<m; j++) {
            i = missing[j];
            if (ds_get_cap(db[j]->ds, db[j]->cap, DS_CAP_READ) != NULL) {
                db[j]->rid_key = req[j].rid_key;
                req[j].rid_key = NULL;  //** Cleanup

                //** Make the copy operation
                gop = ds_copy(b->block[i].data->ds, da, DS_PUSH, NS_TYPE_SOCK, "",
                              ds_get_cap(b->block[i].data->ds, b->block[i].data->cap, DS_CAP_READ), b->block[i].cap_offset,
                              ds_get_cap(db[j]->ds, db[j]->cap, DS_CAP_WRITE), 0,
                              b->block_len, timeout);
                gop_set_myid(gop, j);
                gop_opque_add(q, gop);
            } else {  //** Make sure we exclude the RID key on the next round due to the failure
                data_block_destroy(db[j]);

                if (req[j].rid_key != NULL) {
                    log_printf(15, "Excluding rid_key=%s on next round\n", req[j].rid_key);
                    cleanup_key[cleanup_index] = req[j].rid_key;
                    req[j].rid_key = NULL;
                    rs_query_add(s->rs, &rsq, RSQ_BASE_OP_KV, "rid_key", RSQ_BASE_KV_EXACT, cleanup_key[cleanup_index], RSQ_BASE_KV_EXACT);
                    cleanup_index++;
                    rs_query_add(s->rs, &rsq, RSQ_BASE_OP_NOT, NULL, 0, NULL, 0);
                    rs_query_add(s->rs, &rsq, RSQ_BASE_OP_AND, NULL, 0, NULL, 0);
                } else if (block_status[i] == -103) {  //** Can't move the allocation so unflag it
                    if (rid_pending[i] != NULL) {
                        apr_thread_mutex_lock(rid_lock);
                        rid_pending[i]->rid->delta -= b->block_len;  //** This is the original allocation
                        rid_pending[i]->rid->state = ((llabs(rid_pending[i]->rid->delta) <= rid_pending[i]->rid->tolerance) || (rid_pending[i]->rid->tolerance == 0)) ? 1 : 0;
                        apr_thread_mutex_unlock(rid_lock);
                    }
                }
            }
            log_printf(15, "after rs query block_status[%d]=%d block_len=" XOT "\n", i, block_status[i], b->block_len);
        }

        log_printf(15, "q size=%d\n",gop_opque_task_count(q));

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
                    tbx_atomic_inc(dbd->ref_count);
                    dbd->attr_stack = dbs->attr_stack;
                    dbs->attr_stack = NULL;

                    data_block_auto_warm(dbd);  //** Add it to be auto-warmed

                    b->block[i].data = dbd;
                    b->block[i].cap_offset = 0;
                    block_status[i] = 0;

                    gop_free(gop, OP_DESTROY);

                    if (args->qs) { //** Remove the old data on complete success
                        gop = ds_remove(dbs->ds, da, ds_get_cap(dbs->ds, dbs->cap, DS_CAP_MANAGE), timeout);
                        gop_opque_add(args->qs, gop);  //** This gets placed on the success queue so we can roll it back if needed
                    } else {       //** Remove the just created allocation on failure
                        gop = ds_remove(dbd->ds, da, ds_get_cap(dbd->ds, dbd->cap, DS_CAP_MANAGE), timeout);
                        gop_opque_add(args->qf, gop);  //** This gets placed on the failed queue so we can roll it back if needed
                    }
                    if (s->db_cleanup == NULL) s->db_cleanup = tbx_stack_new();
                    tbx_stack_push(s->db_cleanup, dbs);  //** Dump the data block here cause the cap is needed for the gop.  We'll cleanup up on destroy()
                } else {  //** Copy failed so remove the destintation
                    gop_free(gop, OP_DESTROY);
                    gop = ds_remove(db[j]->ds, da, ds_get_cap(db[j]->ds, db[j]->cap, DS_CAP_MANAGE), timeout);
                    gop_set_myid(gop, -1);
                    dbold[k] = db[j];
                    k++;
                    gop_opque_add(q, gop);

                    if (rid_pending[i] != NULL) { //** Cleanup RID changes
                        apr_thread_mutex_lock(rid_lock);
                        rid_pending[i]->rid->delta -= b->block_len;  //** This is the original allocation
                        rid_pending[i]->rid->state = ((llabs(rid_pending[i]->rid->delta) <= rid_pending[i]->rid->tolerance) || (rid_pending[i]->rid->tolerance == 0)) ? 1 : 0;

                        //** and this is the destination
                        rid = apr_hash_get(rid_changes, db[j]->rid_key, APR_HASH_KEY_STRING);
                        if (rid != NULL) {
                            rid->rid->delta += b->block_len;
                            rid->rid->state = ((llabs(rid->rid->delta) <= rid->rid->tolerance) || (rid->rid->tolerance == 0)) ? 1 : 0;
                        }
                        apr_thread_mutex_unlock(rid_lock);
                    }
                }
            } else {
                gop_free(gop, OP_DESTROY);
            }
        }

        opque_waitall(q);  //** Wait for the removal to complete.  Don't care if there are errors we can still continue
        gop_opque_free(q, OP_DESTROY);

        //** Clean up
        for (i=0; i<k; i++) {
            data_block_destroy(dbold[i]);
        }

        todo= 0;
        for (i=0; i<n_devices; i++) if (block_status[i] != 0) todo++;
        loop++;
    } while ((loop < 5) && (todo > 0));

    for (i=0; i<cleanup_index; i++) free(cleanup_key[i]);

    for (i=0; i<n_devices; i++) {
        if (hints_list[i].local_rsq != NULL) {
            rs_query_destroy(s->rs, hints_list[i].local_rsq);
        }
    }
    rs_query_destroy(s->rs, rsq);

    return(todo);
}

//#########################################
//  DEBUG PURPOSES ONLY TO SIMULATE A FAILURE
//#########################################
//int dbg_trigger = 0;

//***********************************************************************
// slun_row_size_check - Checks the size of eack block in the row.
//***********************************************************************

int slun_row_size_check(lio_segment_t *seg, data_attr_t *da, seglun_row_t *b, int *block_status, apr_time_t *dt, int n_devices, int force_repair, int timeout)
{
    int i, n_size, n_missing;
    data_probe_t *probe[n_devices];
    gop_opque_t *q;
    gop_op_generic_t *gop;
    ex_off_t psize, seg_size;
    apr_time_t start_time;
    q = gop_opque_new();

    start_time = apr_time_now();
    for (i=0; i<n_devices; i++) {
        probe[i] = ds_probe_create(b->block[i].data->ds);

        gop = ds_probe(b->block[i].data->ds, da, ds_get_cap(b->block[i].data->ds, b->block[i].data->cap, DS_CAP_MANAGE), probe[i], timeout);
        gop_set_myid(gop, i);

        gop_opque_add(q, gop);
    }

    // ** Collect the timing information for hte probe if requested
    if (dt != NULL) {
        for (i=0; i<n_devices; i++) {
            gop = opque_waitany(q);
            dt[gop_get_myid(gop)] = apr_time_now() - start_time;
            gop_free(gop, OP_DESTROY);
        }
    } else {
        opque_waitall(q);
    }
    gop_opque_free(q, OP_DESTROY);

    q = gop_opque_new();
    n_missing = 0;
    n_size = 0;
    for (i=0; i<n_devices; i++) {
        //** Verify the max_size >= cap_offset+len
        ds_get_probe(b->block[i].data->ds, probe[i], DS_PROBE_MAX_SIZE, &psize, sizeof(psize));
        seg_size = b->block[i].cap_offset + b->block_len;
        log_printf(10, "seg=" XIDT " seg_offset=" XOT " i=%d rcap=%s  size=" XOT " should be block_len=" XOT "\n", segment_id(seg),
                   b->seg_offset, i, (char *)ds_get_cap(b->block[i].data->ds, b->block[i].data->cap, DS_CAP_READ), psize, b->block_len);
        if (psize < seg_size) {
            if (psize == 0) {  //** Can't access the allocation
                block_status[i] = 1;
                n_missing++;
            } else {   //** Size is screwed up
                gop = ds_truncate(b->block[i].data->ds, da, ds_get_cap(b->block[i].data->ds, b->block[i].data->cap, DS_CAP_MANAGE), seg_size, timeout);
                gop_set_myid(gop, i);
                gop_opque_add(q, gop);
                block_status[i] = 2;
                n_size++;
            }
        }
    }

    if (n_size > 0) {
        while ((gop = opque_waitany(q)) != NULL) {
            i = gop_get_myid(gop);
            if (gop_completed_successfully(gop) == OP_STATE_SUCCESS) {
                b->block[i].data->max_size = b->block_len;  //** We don't clear the block_status[i].  Any errors are trapped in the slun_row_pad_fix() call

                n_size--;
            } else {
                block_status[i] = -2;  //** Failed on the truncate so flag it
                log_printf(5, "truncate failed for i=%d\n", i);
            }
            gop_free(gop, OP_DESTROY);
        }
    }

    gop_opque_free(q, OP_DESTROY);

    for (i=0; i<n_devices; i++) ds_probe_destroy(b->block[i].data->ds, probe[i]);

    return(n_size+n_missing);
}

//***********************************************************************
// slun_row_pad_fix - Pads the blocks=2 (size tweaked) to the full size
//***********************************************************************

int slun_row_pad_fix(lio_segment_t *seg, data_attr_t *da, seglun_row_t *b, int *block_status, int n_devices, int timeout)
{
    int i, err;
    ex_off_t bstart;
    gop_op_generic_t *gop;
    gop_opque_t *q;
    tbx_tbuf_t tbuf;
    char c;

    q = gop_opque_new();

    c = 0;
    tbx_tbuf_single(&tbuf, 1, &c);
    err = 0;
    for (i=0; i < n_devices; i++) {
        log_printf(10, "seg=" XIDT " seg_offset=" XOT " i=%d block_status=%d\n", segment_id(seg), b->seg_offset, i, block_status[i]);
        if (block_status[i] == 2) {
            bstart = b->block[i].cap_offset + b->block[i].data->max_size - 1;
            log_printf(10, "seg=" XIDT " seg_offset=" XOT " i=%d rcap=%s  padding byte=" XOT "\n", segment_id(seg),
                       b->seg_offset, i, (char *)ds_get_cap(b->block[i].data->ds, b->block[i].data->cap, DS_CAP_READ), bstart);

            gop = ds_write(b->block[i].data->ds, da, ds_get_cap(b->block[i].data->ds, b->block[i].data->cap, DS_CAP_WRITE), bstart, &tbuf, 0, 1, timeout);
            gop_set_myid(gop, i);
            gop_opque_add(q, gop);
            err++;
        } else if (block_status[i] == -2) {  //** Failed on the grow
            err++;
            block_status[i] = 3;
            log_printf(5, "truncate failed. resetting block_status[%d]=%d\n", i, block_status[i]);
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
        log_printf(5, "gop complete. block_status[%d]=%d\n", i, block_status[i]);
        gop_free(gop, OP_DESTROY);
    }

    gop_opque_free(q, OP_DESTROY);

    return(err);
}

//***********************************************************************
// slun_row_replace_fix - Replaces the missing or bad allocation in the row
//***********************************************************************

int slun_row_replace_fix(lio_segment_t *seg, data_attr_t *da, seglun_row_t *b, int *block_status, int n_devices, lio_inspect_args_t *args, int timeout)
{
    lio_seglun_priv_t *s = (lio_seglun_priv_t *)seg->priv;
    lio_rs_request_t req_list[n_devices];
    data_cap_set_t *cap_list[n_devices];
    char *key;
    tbx_stack_t *cleanup_stack;
    gop_op_status_t status;
    rs_query_t *rsq;
    gop_op_generic_t *gop, *g;
    int i, j, loop, err, m, ngood, nbad, kick_out;
    int missing[n_devices];
    lio_rs_hints_t hints_list[n_devices];
    char *migrate;
    lio_data_block_t *db;
    lio_data_block_t *db_orig[n_devices];
    lio_data_block_t *db_working[n_devices];

    //** Dup the base query
    rsq = rs_query_dup(s->rs, args->query);

    memset(hints_list, 0, sizeof(hints_list));
    memset(db_working, 0, n_devices * sizeof(lio_data_block_t *));

    loop = 0;
    kick_out = 10000;
    cleanup_stack = NULL;
    do {
        log_printf(15, "loop=%d ------------------------------\n", loop);

        //** Copy the original data blocks over
        for (i=0; i<n_devices; i++) db_orig[i] = b->block[i].data;

        //** Make the fixed list mapping table
        memset(req_list, 0, sizeof(lio_rs_request_t)*n_devices);
        nbad = n_devices-1;
        ngood = 0;
        m = 0;
        for (i=0; i<n_devices; i++) {
            if (block_status[i] == 0) {
                j = ngood;
                hints_list[ngood].fixed_rid_key = b->block[i].data->rid_key;
                hints_list[ngood].status = RS_ERROR_OK;
                migrate = data_block_get_attr(b->block[i].data, "migrate");
                ngood++;
            } else {   //** Make sure we haven't already replaced it
                j = nbad;
                hints_list[nbad].fixed_rid_key = NULL;
                hints_list[nbad].status = RS_ERROR_OK;

                req_list[m].rid_index = nbad;
                req_list[m].size = b->block_len;

                //** check if we need to make a working data block
                if (db_working[i] == NULL) {
                    db = data_block_create(s->ds);
                    db_working[i] = db;
                    db->attr_stack = (db_orig[i]) ? db_orig[i]->attr_stack : NULL;
                    db->rid_key = NULL;
                    db->max_size = b->block_len;
                    db->size = b->block_len;
                }

                cap_list[m] = db_working[i]->cap;
                migrate = data_block_get_attr(db_working[i], "migrate");

                missing[m] = i;
                m++;
                nbad--;
            }

            if (hints_list[j].local_rsq != NULL) {
                rs_query_destroy(s->rs, hints_list[j].local_rsq);
            }

            if (migrate != NULL) {
                hints_list[j].local_rsq = rs_query_parse(s->rs, migrate);
            } else {
                hints_list[j].local_rsq = NULL;
            }
        }

        //** Execute the Query
        gop = rs_data_request(s->rs, da, rsq, cap_list, req_list, m, hints_list, ngood, n_devices, 1, timeout);
        err = gop_waitall(gop);

        //** Check if we have enough RIDS
        if (err != OP_STATE_SUCCESS) {
            status = gop_get_status(gop);
            if (status.error_code == RS_ERROR_NOT_ENOUGH_RIDS) { //** No use looping
                log_printf(1, "seg=" XIDT " ERROR not enough RIDS!\n", segment_id(seg));
                err = m;
                loop = kick_out + 10;  //** Kick us out of the loop
                for (j=0; j<m; j++) {
                    if (req_list[j].rid_key) free(req_list[j].rid_key);
                }
                goto oops;
            } else if (status.error_code == RS_ERROR_EMPTY_STACK) { //** No use looping
                log_printf(1, "seg=" XIDT " ERROR RS query is BAD!\n", segment_id(seg));
                err = m;
                loop = kick_out + 10;  //** Kick us out of the loop
                for (j=0; j<m; j++) {
                    if (req_list[j].rid_key) free(req_list[j].rid_key);
                }
                goto oops;
            }
        }

        //** Process the results
        err = 0;
        for (j=0; j<m; j++) {
            i = missing[j];
            log_printf(15, "missing[%d]=%d req.op_status=%d\n", j, missing[j], (req_list[j].gop) ? gop_completed_successfully(req_list[j].gop) : -123);
            db = db_working[i];
            if (ds_get_cap(db->ds, db->cap, DS_CAP_READ) != NULL) {
                block_status[i] = 2;  //** Mark the block for padding
                data_block_auto_warm(db);  //** Add it to be auto-warmed
                b->block[i].data = db;
                db->rid_key = req_list[j].rid_key;
                tbx_atomic_inc(db->ref_count);
                b->block[i].read_err_count = 0;
                b->block[i].write_err_count = 0;
                req_list[j].rid_key = NULL; //** Cleanup
                db_working[i] = NULL;

                //** Make the cleanup operations
                if (db_orig[i]) {
                    db_orig[i]->attr_stack = NULL;  //** This is now used by the new allocation
                    if (args->qs) {
                        g = ds_remove(s->ds, da, ds_get_cap(db_orig[i]->ds, db_orig[i]->cap, DS_CAP_MANAGE), timeout);
                        gop_opque_add(args->qs, g);  //** This gets placed on the success queue so we can roll it back if needed
                    }
                    if (s->db_cleanup == NULL) s->db_cleanup = tbx_stack_new();
                    tbx_stack_push(s->db_cleanup, db_orig[i]);   //** Dump the data block here cause the cap is needed for the gop.  We'll cleanup up on destroy()
                }
                err++;
            } else {  //** Make sure we exclude the RID key on the next round due to the failure
                if (req_list[j].rid_key != NULL) {
                    log_printf(15, "Excluding rid_key=%s on next round\n", req_list[j].rid_key);
                    if (cleanup_stack == NULL) cleanup_stack = tbx_stack_new();
                    key = req_list[j].rid_key;
                    tbx_stack_push(cleanup_stack, key);
                    req_list[j].rid_key = NULL;  //** Don't want to accidentally free it below
                    rs_query_add(s->rs, &rsq, RSQ_BASE_OP_KV, "rid_key", RSQ_BASE_KV_EXACT, key, RSQ_BASE_KV_EXACT);
                    rs_query_add(s->rs, &rsq, RSQ_BASE_OP_NOT, NULL, 0, NULL, 0);
                    rs_query_add(s->rs, &rsq, RSQ_BASE_OP_AND, NULL, 0, NULL, 0);
                }
            }

            log_printf(15, "after rs query block_status[%d]=%d block_len=" XOT "\n", i, block_status[i], b->block_len);
        }

        //**Pad the good ones
        err = m - err + slun_row_pad_fix(seg, da, b, block_status, n_devices, timeout);

        log_printf(15, "after row_pad_fix.  m=%d err=%d loop=%d\n", m, err, loop);

oops:
        gop_free(gop, OP_DESTROY);
        loop++;
    } while ((loop < kick_out) && (err > 0));

    //** Clean up
    rs_query_destroy(s->rs, rsq);
    for (i=0; i<n_devices; i++) {
        if (hints_list[i].local_rsq != NULL) {
            rs_query_destroy(s->rs, hints_list[i].local_rsq);
        }
        if (db_working[i] != NULL) {
            db_working[i]->attr_stack = NULL;  //** This is still used by yhe original data block
            if (s->db_cleanup == NULL) s->db_cleanup = tbx_stack_new();
            tbx_stack_push(s->db_cleanup, db_working[i]);   //** Dump the unused data block for destruction
        }
    }
    if (cleanup_stack != NULL) tbx_stack_free(cleanup_stack, 1);

    return(err);
}

//***********************************************************************
// seglun_grow - Expands a linear segment
//***********************************************************************

gop_op_status_t _seglun_grow(lio_segment_t *seg, data_attr_t *da, ex_off_t new_size_arg, int timeout)
{
    int i, err, cnt;
    ex_off_t off, dsize, old_len;
    lio_seglun_priv_t *s = (lio_seglun_priv_t *)seg->priv;
    seglun_row_t *b;
    seglun_block_t *block;
    tbx_isl_iter_t it;
    ex_off_t lo, hi, berr, new_size;
    gop_op_status_t status;
    int block_status[s->n_devices];
    apr_time_t now;
    double gsecs, tsecs;
    lio_inspect_args_t args;

    new_size = new_size_arg;
    if (new_size < 0) { //** Reserve space call
        new_size = - new_size_arg;
        log_printf(5, "reserving space: current=" XOT " new=" XOT "\n", s->total_size, new_size);
        if (new_size < s->total_size) return(gop_success_status);  //** Already have enough space reserved
    }

    memset(&args, 0, sizeof(args));
    args.query = s->rsq;

    now = apr_time_now();

    //** Round the size to the nearest stripe size
    berr = 0;
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
        it = tbx_isl_iter_search(s->isl, (tbx_sl_key_t *)&lo, (tbx_sl_key_t *)&hi);
        b = (seglun_row_t *)tbx_isl_next(&it);
        if (b->row_len < s->max_row_size) {
            dsize = new_size - b->seg_offset;
            dsize /= s->n_devices;
            dsize += (dsize % s->chunk_size);  //** Round up to the nearest chunk
            if (dsize > s->max_block_size) dsize = s->max_block_size;

            log_printf(15, "sid=" XIDT " increasing existing row seg_offset=" XOT " curr seg_end=" XOT " newmax=" XOT "\n", segment_id(seg), b->seg_offset, b->seg_end, new_size);

            tbx_isl_remove(s->isl, (tbx_sl_key_t *)&(b->seg_offset), (tbx_sl_key_t *)&(b->seg_end), (tbx_sl_data_t *)b);

            old_len = b->block_len;
            b->block_len = dsize;
            b->row_len = dsize * s->n_devices;
            b->seg_end = b->seg_offset + b->row_len - 1;
            for (i=0; i<s->n_devices; i++) block_status[i] = 0;
            slun_row_size_check(seg, da, b, block_status, NULL, s->n_devices, 1, timeout);

            //** Check if we had an error on the size
            berr = 0;
            for (i=0; i<s->n_devices; i++) {
                if (block_status[i]==2) {  //** Tweaked an allocation's size
                    berr = slun_row_pad_fix(seg, da, b, block_status, s->n_devices, timeout);
                    break;  //** Kick out
                }
            }

            if (berr > 0) { //** Error growing the allocations so just leave them with the bad size but truncate the block to the old size
                b->block_len = old_len;
                b->row_len = old_len * s->n_devices;
                b->seg_end = b->seg_offset + b->row_len - 1;
                for (i=0; i<s->n_devices; i++) {
                    b->block[i].data->max_size = old_len;
                    b->block[i].data->size = old_len;
                }
            }

            tbx_isl_insert(s->isl, (tbx_sl_key_t *)&(b->seg_offset), (tbx_sl_key_t *)&(b->seg_end), (tbx_sl_data_t *)b);

            log_printf(15, "sid=" XIDT " enlarged row seg_offset=" XOT " seg_end=" XOT " row_len=" XOT " berr=" XOT "\n", segment_id(seg), b->seg_offset, b->seg_end, b->row_len, berr);

            lo = b->seg_end + 1;
        } else {
            log_printf(15, "sid=" XIDT " row maxed out seg_offset=" XOT " curr seg_end=" XOT " row_len=" XOT "\n", segment_id(seg), b->seg_offset, b->seg_end, b->row_len);
            lo = b->seg_end + 1;
        }
    }

    //** Create the additional caps and commands
    err = 0;
    for (off=lo; off<new_size; off = off + s->max_row_size) {
        tbx_type_malloc_clear(b, seglun_row_t, 1);
        tbx_type_malloc_clear(block, seglun_block_t, s->n_devices);
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
        err = err + slun_row_replace_fix(seg, da, b, block_status, s->n_devices, &args, timeout);

        if (err == 0) {
            tbx_isl_insert(s->isl, (tbx_sl_key_t *)&(b->seg_offset), (tbx_sl_key_t *)&(b->seg_end), (tbx_sl_data_t *)b);
        } else {  //** Got an error so clean up and kick out
            for (i=0; i<s->n_devices; i++) {
                if (block[i].data != NULL) {
                    cnt = tbx_atomic_get(block[i].data->ref_count);
                    if ( cnt > 0) tbx_atomic_dec(block[i].data->ref_count);
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
    log_printf(1, "sid=" XIDT " END used=" XOT " old max=" XOT " newmax=" XOT " err=%d berr=" XOT " dt=%lf dt_total=%lf grow_count=%d\n", segment_id(seg), s->used_size, s->total_size, new_size, err, berr, gsecs, tsecs, s->grow_count);

    if (err == 0) {
        s->total_size = new_size;
        if (new_size_arg > -1) s->used_size = new_size;  //** Only update the used size for a non-reserve space call
        status = gop_success_status;
    } else {
        status =  gop_failure_status;
    }

    return(status);
}

//***********************************************************************
// seglun_shrink - Shrinks a linear segment
//***********************************************************************

gop_op_status_t _seglun_shrink(lio_segment_t *seg, data_attr_t *da, ex_off_t new_size, int timeout)
{
    lio_seglun_priv_t *s = (lio_seglun_priv_t *)seg->priv;
    gop_op_generic_t *gop;
    tbx_isl_iter_t it;
    seglun_row_t *b;
    gop_opque_t *q = NULL;
    ex_off_t lo, hi, dsize, bstart_size, bstart_block_size, new_used;
    tbx_stack_t *stack;
    seglun_row_t *start_b;
    gop_op_status_t status;
    int i, err, cnt;

    //** Round the size to the nearest stripe size
    new_used = new_size;
    dsize = new_size / s->stripe_size;
    dsize = dsize * s->stripe_size;
    if ((new_size % s->stripe_size) > 0) dsize += s->stripe_size;
    new_size = dsize;
    bstart_block_size = -1;

    lo = new_size;
    hi = s->total_size;
    log_printf(1, "_sl_shrink: sid=" XIDT " total_size=" XOT " new_size=" XOT "\n", segment_id(seg), hi, lo);


    it = tbx_isl_iter_search(s->isl, (tbx_sl_key_t *)&lo, (tbx_sl_key_t *)&hi);
    b = (seglun_row_t *)tbx_isl_next(&it);
    if ( b == NULL) {  //** Nothing to do
        err = OP_STATE_SUCCESS;
        goto finished;
    }

    stack = tbx_stack_new();
    q = gop_opque_new();

    //** The 1st row maybe a partial removal
    dsize = new_size - b->seg_offset;
    bstart_size = dsize;
    if (dsize == 0) {  //** Full removal
        log_printf(15, "_sl_shrink: sid=" XIDT " removing seg_off=" XOT "\n", segment_id(seg), b->seg_offset);
        for (i=0; i < s->n_devices; i++) {
            gop = ds_remove(b->block[i].data->ds, da, ds_get_cap(b->block[i].data->ds, b->block[i].data->cap, DS_CAP_MANAGE), timeout);
            gop_opque_add(q, gop);
        }
        tbx_stack_push(stack, (void *)b);
        start_b = NULL;
    } else {
        log_printf(15, "_sl_shrink: sid=" XIDT " shrinking  seg_off=" XOT " to=" XOT "\n", segment_id(seg), b->seg_offset, dsize);
        bstart_block_size = dsize / s->n_devices;
        for (i=0; i < s->n_devices; i++) {
            gop = ds_truncate(b->block[i].data->ds, da, ds_get_cap(b->block[i].data->ds, b->block[i].data->cap, DS_CAP_MANAGE), bstart_block_size, timeout);
            gop_opque_add(q, gop);
        }
        start_b = b;
    }

    //** Set up for the rest of the blocks
    b = (seglun_row_t *)tbx_isl_next(&it);
    while (b != NULL) {
        log_printf(15, "_sl_shrink: sid=" XIDT " removing seg_off=" XOT "\n", segment_id(seg), b->seg_offset);
        tbx_stack_push(stack, (void *)b);
        for (i=0; i < s->n_devices; i++) {
            gop = ds_remove(b->block[i].data->ds, da, ds_get_cap(b->block[i].data->ds, b->block[i].data->cap, DS_CAP_MANAGE), timeout);
            gop_opque_add(q, gop);
        }

        b = (seglun_row_t *)tbx_isl_next(&it);
    }

    //** Do the removal
    err = opque_waitall(q);
    gop_opque_free(q, OP_DESTROY);

    //** And now clean up
    while ((b = (seglun_row_t *)tbx_stack_pop(stack)) != NULL) {
        i = tbx_isl_remove(s->isl, &(b->seg_offset), &(b->seg_end), b);
        log_printf(15, "_sl_shrink: sid=" XIDT " removing from interval seg_off=" XOT " remove_isl=%d\n", segment_id(seg), b->seg_offset, i);
        for (i=0; i < s->n_devices; i++) {
            cnt = tbx_atomic_get(b->block[i].data->ref_count);
            if (cnt > 0) tbx_atomic_dec(b->block[i].data->ref_count);
            data_block_destroy(b->block[i].data);
        }
        free(b->block);
        free(b);
    }

    tbx_stack_free(stack, 0);

    //** If needed tweak the initial block
    if (start_b != NULL) {
        b = start_b;
        tbx_isl_remove(s->isl, &(b->seg_offset), &(b->seg_end), b);
        b->seg_end = b->seg_offset + bstart_size - 1;
        b->block_len = bstart_block_size;
        b->row_len = bstart_size;

        for (i=0; i<s->n_devices; i++) {
            b->block[i].data->max_size = b->block_len;
            b->block[i].data->size = b->block_len;
        }

        tbx_isl_insert(s->isl, (tbx_sl_key_t *)&(b->seg_offset), (tbx_sl_key_t *)&(b->seg_end), (tbx_sl_data_t *)b);
    }

finished:
    //** Update the size
    s->total_size = new_size;
    s->used_size = new_used;

    if (err == OP_STATE_SUCCESS) {
        status = gop_success_status;
    } else if (new_size == 0) {   //** If new size is 0 then we can ignore any failed removals
        status = gop_success_status;
    } else {
        status = gop_failure_status;
    }
    return(status);
}

//***********************************************************************
// _slun_truncate - Performs the truncate
//***********************************************************************

gop_op_status_t _slun_truncate(lio_segment_t *seg, data_attr_t *da, ex_off_t new_size, int timeout)
{
    lio_seglun_priv_t *s = (lio_seglun_priv_t *)seg->priv;
    gop_op_status_t err = gop_success_status;

    if (new_size < 0) { //Reserve space call
        err = _seglun_grow(seg, da, new_size, timeout);
    } else if (s->total_size > new_size) {
        err = _seglun_shrink(seg, da, new_size, timeout);
    } else if (s->total_size < new_size) {
        err = _seglun_grow(seg, da, new_size, timeout);
    }

    return(err);
}

//***********************************************************************
//  seglun_truncate_func - Does the actual segment truncate operations
//***********************************************************************

gop_op_status_t seglun_truncate_func(void *arg, int id)
{
    seglun_truncate_t *st = (seglun_truncate_t *)arg;
    gop_op_status_t err;

    segment_lock(st->seg);
    err = _slun_truncate(st->seg, st->da, st->new_size, st->timeout);
    segment_unlock(st->seg);

    return(err);
}

//***********************************************************************
// seglun_truncate - Expands or contracts a linear segment
//***********************************************************************

gop_op_generic_t *seglun_truncate(lio_segment_t *seg, data_attr_t *da, ex_off_t new_size, int timeout)
{
    lio_seglun_priv_t *s = (lio_seglun_priv_t *)seg->priv;

    seglun_truncate_t *st;

    tbx_type_malloc_clear(st, seglun_truncate_t, 1);

    st->seg = seg;
    st->new_size = new_size;
    st->timeout = timeout;
    st->da = da;

    return(gop_tp_op_new(s->tpc, NULL, seglun_truncate_func, (void *)st, free, 1));
}

//***********************************************************************
// lun_row_decompose - Decomposes the R/W request (start->start+blen) on the
//    row into separate requests for each block in the row.
//    NOTE: start is relative to start of the row and not the file!
//***********************************************************************

void lun_row_decompose(lio_segment_t *seg, lun_rw_row_t *rw_buf, seglun_row_t *b, ex_off_t start, tbx_tbuf_t *buffer, ex_off_t bpos, ex_off_t rwlen)
{
    lio_seglun_priv_t *s = (lio_seglun_priv_t *)seg->priv;
    lun_rw_row_t *rwb;
    int i, j, k, n_stripes, start_stripe, end_stripe;
    ex_off_t lo, hi, nleft, pos, chunk_off, chunk_end, stripe_off, begin, end, nbytes;
    int err, dev, ss, stripe_shift;
    ex_off_t offset[s->n_devices], len[s->n_devices];
    tbx_tbuf_var_t tbv;

    lo = start;
    hi = lo + rwlen - 1;
    start_stripe = lo / s->stripe_size;
    end_stripe = hi / s->stripe_size;
    n_stripes = end_stripe - start_stripe + 1;

    log_printf(15, "lo=" XOT " hi= " XOT " len=" XOT "\n", lo, hi, rwlen);
    log_printf(15, "start_stripe=%d end_stripe=%d n_stripes=%d\n", start_stripe, end_stripe, n_stripes);

    tbx_tbuf_var_init(&tbv);

    for (i=0; i < s->n_devices; i++) {
        offset[i] = -1;
        len[i] = 0;

        //** Make the initial space
        k = rw_buf[i].c_iov - rw_buf[i].n_iov;
        if (k < n_stripes) {
            rw_buf[i].c_iov += n_stripes - k + 1;
            if (rw_buf[i].iov == NULL) {
                tbx_type_malloc(rw_buf[i].iov, tbx_iovec_t, rw_buf[i].c_iov);
            } else {
                tbx_type_realloc(rw_buf[i].iov, tbx_iovec_t, rw_buf[i].c_iov);
            }
        }
    }

    ss = start_stripe;
    stripe_shift = ss*s->n_shift;
    stripe_off = ss * s->stripe_size;
    while (stripe_off <= hi) {
        for (i=0; i< s->n_devices; i++) {
            dev = (i+stripe_shift) % s->n_devices;
            chunk_off = stripe_off + dev * s->chunk_size;
            chunk_end = chunk_off + s->chunk_size - 1;
            rwb = &(rw_buf[i]);

            if ((chunk_end >= lo) && (chunk_off <= hi)) {
                begin = (chunk_off < lo) ? lo - chunk_off: 0;
                end = (chunk_end > hi) ? hi - chunk_off : s->chunk_size - 1;
                nbytes = end - begin + 1;

                if (offset[i] == -1) { //** 1st time it's used so set the offset
                    offset[i] = ss * s->chunk_size + begin;
                }
                len[i] += nbytes;

                pos = bpos + chunk_off + begin - lo;

                nleft = nbytes;
                tbv.nbytes = nleft;
                err = TBUFFER_OK;
                while ((nleft > 0) && (err == TBUFFER_OK)) {
                    err = tbx_tbuf_next(buffer, pos, &tbv);
                    k = rwb->n_iov + tbv.n_iov;
                    if (k >= rwb->c_iov) {
                        rwb->c_iov = 2*k;
                        tbx_type_realloc(rwb->iov, tbx_iovec_t, rwb->c_iov);
                    }
                    for (k=0; k<tbv.n_iov; k++) {
                        rwb->iov[rwb->n_iov + k] = tbv.buffer[k];
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
        if (offset[i] >= 0) {
            j = rw_buf[i].n_ex;
            if (rw_buf[i].n_ex == rw_buf[i].c_ex) {
                k = 2 * (j+1);
                rw_buf[i].c_ex = k;
                if (rw_buf[i].n_ex == 0) {
                    tbx_type_malloc(rw_buf[i].ex_iov, ex_tbx_iovec_t, k);
                } else {
                    tbx_type_realloc(rw_buf[i].ex_iov, ex_tbx_iovec_t, k);
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
                log_printf(0, "  MATCH : " XOT " -> " XOT " (%d bytes)\n", start, end, k);

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
                    log_printf(0, "  DIFFER: " XOT " -> " XOT " (%d bytes)\n", start, end, k);

                    if (k>50) k = 50;
                    memcpy(cbuf, &(b1[start]), k);
                    cbuf[k] = 0;
                    log_printf(0, "   b1=%s\n", cbuf);
                    memcpy(cbuf, &(b2[start]), k);
                    cbuf[k] = 0;
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
    lio_segment_t *seg;
    lio_seglun_priv_t *s;
    lun_rw_row_t rw_buf[max_dev];
    tbx_iovec_t *iov_ref[max_dev], *iovbuf;
    tbx_tbuf_t tbuf, tbuf_ref[max_dev];
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
    seg = segment_lun_create((void *)lio_exnode_service_set);
    s = (lio_seglun_priv_t *)seg->priv;

    //** Make the fake row
    tbx_type_malloc_clear(b, seglun_row_t, 1);
    tbx_type_malloc_clear(block, seglun_block_t, max_dev);
    b->block = block;
    b->seg_offset = 0;
    for (i=0; i<max_dev; i++) {
        block[i].data = data_block_create(s->ds);
        block[i].cap_offset = 0;
        block[i].data->size = bufsize;     //** Set them to a not to exceed value so
        block[i].data->max_size = bufsize; //** I don't have to muck with them as I change params
    }

    //** Make the test buffers
    tbx_type_malloc(ref_buf, char, bufsize);
    tbx_type_malloc_clear(buf, char, bufsize);
    for (i=0; i<bufsize; i++) ref_buf[i] = base[i%nbase];


    //** Now do the tests
    for (ndev=1; ndev <= n_devs; ndev++) {  //** Number of devices
        log_printf(0, "ndev=%d----------------------------------------------------------------------\n", ndev);

        s->n_devices = ndev;
        s->chunk_size = 16*1024;
        s->stripe_size = s->n_devices * s->chunk_size;
        s->n_shift = 1;

        //** Make the reference tbufs
        nrows = bufsize / s->stripe_size;
        if ((bufsize % s->stripe_size) > 0) nrows++;

        log_printf(0, "ndev=%d  chunk_size=" XOT " stripe_size=" XOT "   nrows=%d----------------------------------------------\n", ndev, s->chunk_size, s->stripe_size, nrows);

        for (i=0; i < s->n_devices; i++) {
            tbx_type_malloc(iov_ref[i], tbx_iovec_t, nrows);
            tbx_tbuf_vec(&(tbuf_ref[i]), bufsize, nrows, iov_ref[i]);
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
            tbx_type_malloc(iovbuf, tbx_iovec_t, niov);

            for (j=0; j < n_tests; j++) {  //** Random tests

                //** Init the dest buf for the test
                len = tbx_random_get_int64(0, bufsize-1);
                offset = tbx_random_get_int64(0,bufsize-len-1);
                k = len / niov;
                for (i=0; i<niov; i++) {
                    iovbuf[i].iov_base = &(buf[i*k]);
                    iovbuf[i].iov_len = k;
                }
                iovbuf[niov-1].iov_len = len - (niov-1)*k;
                tbx_tbuf_vec(&tbuf, len, niov, iovbuf);

                log_printf(0, "ndev=%d  niov=%d j=%d  len=%d off=%d k=%d\n", ndev, niov, j, len, offset, k);
                tbx_log_flush();

                //** Do the test
                memset(buf, 0, bufsize);
                lun_row_decompose(seg, rw_buf, b, offset, &tbuf, 0, len);

                for (i=0; i < s->n_devices; i++) {
                    if (rw_buf[i].n_ex > 0) {
                        tbx_tbuf_copy(&(tbuf_ref[i]), rw_buf[i].ex_iov[0].offset, &(rw_buf[i].buffer), 0, rw_buf[i].ex_iov[0].len, 1);
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
    tbx_obj_put(&seg->obj);

    log_printf(0, " Total error count=%d\n", cerr);
    if (cerr == 0) log_printf(0, "PASSED!\n");

    return(cerr);
}

//***********************************************************************
// seglun_rw_op - Reads/Writes to a LUN segment
//***********************************************************************

gop_op_status_t seglun_rw_op(lio_segment_t *seg, data_attr_t *da, lio_segment_rw_hints_t *rw_hints, int n_iov, ex_tbx_iovec_t *iov, tbx_tbuf_t *buffer, ex_off_t boff, int rw_mode, int timeout)
{
    lio_seglun_priv_t *s = (lio_seglun_priv_t *)seg->priv;
    lio_blacklist_t *bl = s->bl;
    gop_op_status_t status;
    gop_op_status_t blacklist_status = {OP_STATE_FAILURE, -1234};
    gop_opque_t *q;

    //** For small I/O we use the stack
    int isl_size = 100;
    seglun_row_t *bused_ptr[isl_size];
    lun_rw_row_t rwb_table_ptr[isl_size];
    seglun_row_t **bused = bused_ptr;
    lun_rw_row_t *rwb_table = rwb_table_ptr;

    seglun_row_t *b;
    tbx_isl_iter_t it;
    ex_off_t lo, hi, start, end, blen, bpos;
    int i, j, maxerr, nerr, slot, n_bslots, bl_count, dev, bl_rid;
    tbx_stack_t *stack;
    lun_rw_row_t *rw_buf;
    double dt;
    apr_time_t exec_time;
    apr_time_t tstart, tstart2;
    gop_op_generic_t *gop;

    tstart = apr_time_now();

    log_printf(5, "bl=%p rw_hints=%p\n", bl, rw_hints);
    //** Check if we can use blacklisting
    if (rw_hints == NULL) {
        bl = NULL;
    } else {
        log_printf(5, "max_blacklist=%d\n", rw_hints->lun_max_blacklist);
        if (rw_hints->lun_max_blacklist <= 0) bl = NULL;
    }

    segment_lock(seg);

    //** Check if we need to translate the caps.  We exec the "if" rarely
    apr_thread_mutex_lock(s->notify.lock);
    if (s->map_version != s->notify.map_version) {
        apr_thread_mutex_unlock(s->notify.lock); //** DOn;t need this while waiting for ops to complete

        while (s->inprogress_count > 0) {  //** Wait until all the current ops complete
            apr_thread_cond_wait(seg->cond, seg->lock);
            log_printf(5, "sid=" XIDT " inprogress_count=%d\n", segment_id(seg), s->inprogress_count);
        }

        //** Do the remap unless someoue beat us to it while waiting
        apr_thread_mutex_lock(s->notify.lock);  //** Reacquire it
        if (s->map_version != s->notify.map_version) {
            s->map_version = s->notify.map_version;
            _slun_perform_remap(seg);
        }
    }
    apr_thread_mutex_unlock(s->notify.lock);

    s->inprogress_count++;  //** Flag that we are doing an I/O op

    q = gop_opque_new();
    stack = tbx_stack_new();
    bpos = boff;

    log_printf(15, "START sid=" XIDT " n_iov=%d rw_mode=%d intervals=%d\n", segment_id(seg), n_iov, rw_mode, tbx_isl_count(s->isl));

    n_bslots = 0;
    for (slot=0; slot<n_iov; slot++) {
        lo = iov[slot].offset;

        hi = lo + iov[slot].len - 1;
        it = tbx_isl_iter_search(s->isl, (tbx_sl_key_t *)&lo, (tbx_sl_key_t *)&hi);
        b = (seglun_row_t *)tbx_isl_next(&it);
        log_printf(15, "FOR sid=" XIDT " slot=%d n_iov=%d lo=" XOT " hi=" XOT " len=" XOT " b=%p\n", segment_id(seg), slot, n_iov, lo, hi, iov[slot].len, b);

        while (b != NULL) {
            start = (lo <= b->seg_offset) ? 0 : (lo - b->seg_offset);
            end = (hi >= b->seg_end) ? b->row_len-1 : (hi - b->seg_offset);
            blen = end - start + 1;

            log_printf(15, "sid=" XIDT " soff=" XOT " bpos=" XOT " blen=" XOT " seg_off=" XOT " seg_len=" XOT " seg_end=" XOT " rwop_index=%d\n", segment_id(seg),
                       start, bpos, blen, b->seg_offset, b->row_len, b->seg_end, b->rwop_index);
            tbx_log_flush();

            if (b->rwop_index < 0) {
                bused[n_bslots] = b;
                b->rwop_index = n_bslots;
                n_bslots++;
                j = b->rwop_index * s->n_devices;
                if (n_bslots >= isl_size) { //** Need to grow the tables
                    log_printf(5, "REALLOCATING tables. isl_size=%d n_bslot=%d\n", isl_size, n_bslots);
                    isl_size = 1.5*isl_size + 10;
                    if (bused == bused_ptr) {
                        tbx_type_malloc(rwb_table, lun_rw_row_t, s->n_devices * isl_size);
                        tbx_type_malloc(bused, seglun_row_t *, isl_size);
                        memcpy(rwb_table, rwb_table_ptr, sizeof(rwb_table_ptr));
                        memcpy(bused, bused_ptr, sizeof(bused_ptr));
                    } else {
                        tbx_type_realloc(rwb_table, lun_rw_row_t, s->n_devices * isl_size);
                        tbx_type_realloc(bused, seglun_row_t *, isl_size);
                    }
                }
                memset(&(rwb_table[j]), 0, sizeof(lun_rw_row_t)*s->n_devices);
            }

            log_printf(15, "rwop_index=%d\n", b->rwop_index);

            rw_buf = &(rwb_table[b->rwop_index*s->n_devices]);
            lun_row_decompose(seg, rw_buf, b, start, buffer, bpos, blen);

            bpos = bpos + blen;

            b = (seglun_row_t *)tbx_isl_next(&it);
        }
        log_printf(15, "bottom sid=" XIDT " slot=%d\n", segment_id(seg), slot);

    }

    log_printf(15, " n_bslots=%d\n", n_bslots);

    //** Acquire the blacklist lock if using it
    if (bl) apr_thread_mutex_lock(bl->lock);

    //** Assemble the sub tasks and start executing them
    for (slot=0; slot < n_bslots; slot++) {
        b = bused[slot];
        bl_count = 0;
        b->rwop_index = -1;
        j = slot * s->n_devices;

        for (i=0; i < s->n_devices; i++) {
            bl_rid = 0;

            if (rwb_table[j + i].n_ex > 0) {
                //** Check on blacklisting the RID
                if (bl != NULL) {
                    bl_rid = blacklist_check(bl, b->block[i].data->rid_key, 0);
                    if (bl_rid == 1) {
                        if (bl_count >= rw_hints->lun_max_blacklist) {  //** Already blacklisted enough RIDS
                            bl_rid = 0;
                        } else {
                            bl_count++;  //** Blacklisting it
                        }
                    }
                }

                //** Form the op
                tbx_tbuf_vec(&(rwb_table[j + i].buffer), rwb_table[j + i].len, rwb_table[j+i].n_iov, rwb_table[j+i].iov);
                if (rw_mode== 0) {
                    if (rwb_table[j+i].n_iov == 1) {
                        gop = (bl_rid == 0) ? ds_read(b->block[i].data->ds, da, ds_get_cap(b->block[i].data->ds, b->block[i].data->cap, DS_CAP_READ),
                                                         rwb_table[j+i].ex_iov[0].offset, &(rwb_table[j+i].buffer), 0, rwb_table[j+i].len, timeout) :
                              gop_dummy(blacklist_status);
                    } else {
                        gop = (bl_rid == 0) ? ds_readv(b->block[i].data->ds, da, ds_get_cap(b->block[i].data->ds, b->block[i].data->cap, DS_CAP_READ),
                                                          rwb_table[j + i].n_ex, rwb_table[j+i].ex_iov, &(rwb_table[j+i].buffer), 0, rwb_table[j+i].len, timeout) :
                              gop_dummy(blacklist_status);
                    }
                } else {
                    if (rwb_table[j+i].n_iov == 1) {
                        gop = (bl_rid == 0) ? ds_write(b->block[i].data->ds, da, ds_get_cap(b->block[i].data->ds, b->block[i].data->cap, DS_CAP_WRITE),
                                                          rwb_table[j+i].ex_iov[0].offset, &(rwb_table[j+i].buffer), 0, rwb_table[j+i].len, timeout) :
                              gop_dummy(blacklist_status);
                    } else {
                        gop = (bl_rid == 0) ? ds_writev(b->block[i].data->ds, da, ds_get_cap(b->block[i].data->ds, b->block[i].data->cap, DS_CAP_WRITE),
                                                           rwb_table[j + i].n_ex, rwb_table[j+i].ex_iov, &(rwb_table[j+i].buffer), 0, rwb_table[j+i].len, timeout) :
                              gop_dummy(blacklist_status);
                    }
                }

                rwb_table[j+i].gop = gop;
                rwb_table[j+i].block = &(b->block[i]);
                gop_opque_add(q, rwb_table[j+i].gop);
                gop_set_myid(rwb_table[j+i].gop, j+i);
                gop_set_private(gop, b->block[i].data->rid_key);
            }
        }
    }

    if (bl) apr_thread_mutex_unlock(bl->lock);

    segment_unlock(seg);

    if (gop_opque_task_count(q) == 0) {
        log_printf(0, "ERROR Nothing to do\n");
        status = gop_failure_status;
    } else {
        tstart2 = apr_time_now();
        gop_op_status_t dt_status;
        int bad_count = 0;
        while ((gop = opque_waitany(q)) != NULL) {
            dt = apr_time_now() - tstart2;
            dt /= (APR_USEC_PER_SEC*1.0);
            dt_status = gop_get_status(gop);
            if (dt_status.op_status != OP_STATE_SUCCESS) bad_count++;
            dev = gop_get_myid(gop) % s->n_devices;
            log_printf(1, "device=%d slot=%d time: %lf op_status=%d error_code=%d gid=%d\n", dev, gop_get_myid(gop), dt, dt_status.op_status, dt_status.error_code, gop_id(gop));
            log_printf(5, "bl=%p\n", bl);
            //** Check if we need to do any blacklisting
            if ((dt_status.error_code != -1234) && (bl != NULL)) { //** Skip the blacklisted ops
                exec_time = gop_time_exec(gop);
                log_printf(5, "exec_time=" TT " min_time=" TT "\n", exec_time, bl->min_io_time);
                if (exec_time > bl->min_io_time) { //** Make sure the exec time was long enough
                    dt = rwb_table[gop_get_myid(gop)].len;
                    dt /= exec_time;
                    log_printf(5, "dt=%lf min_bw=" XOT "\n", dt, bl->min_bandwidth);
                    if (dt < bl->min_bandwidth) { // ** Blacklist it
                        blacklist_add(bl, rwb_table[gop_get_myid(gop)].block->data->rid_key, 0, 1);
                    }
                }
            }
        }
        dt = apr_time_now() - tstart2;
        dt /= (APR_USEC_PER_SEC*1.0);
        log_printf(1, "IBP time: %lf errors=%d\n", dt, bad_count);

        maxerr = 0;
        for (slot = 0; slot < n_bslots; slot++) {
            nerr = 0;
            j = slot * s->n_devices;
            for (i=0; i < s->n_devices; i++) {
                if (rwb_table[j+i].n_ex > 0) {
                    if (gop_completed_successfully(rwb_table[j+i].gop) != OP_STATE_SUCCESS) {  //** Error
                        nerr++;  //** Increment the error count
                        if (rw_mode == 0) {
                            tbx_tbuf_memset(&(rwb_table[j+i].buffer), 0, 0, rwb_table[j+i].len); //** Blank the data on READs
                            rwb_table[j+i].block->read_err_count++;
                        } else {
                            rwb_table[j+i].block->write_err_count++;
                        }
                    }

                    free(rwb_table[j+i].ex_iov);
                    log_printf(15, "end stage i=%d gid=%d gop_completed_successfully=%d nerr=%d\n", i, gop_id(rwb_table[j+i].gop), gop_completed_successfully(rwb_table[j+i].gop), nerr);
                }

                if (rwb_table[j+i].iov != NULL) free(rwb_table[j+i].iov);
                if (rwb_table[j+i].gop != NULL) gop_free(rwb_table[j+i].gop, OP_DESTROY);
            }

            if (nerr > maxerr) maxerr = nerr;
        }

        log_printf(15, "END stage maxerr=%d\n", maxerr);

        if (maxerr == 0) {
            log_printf(15, "success\n");
            status = gop_success_status;
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

    if (bused != bused_ptr) {
        free(rwb_table);
        free(bused);
    }
    tbx_stack_free(stack, 0);
    gop_opque_free(q, OP_DESTROY);

    dt = apr_time_now() - tstart;
    dt /= (APR_USEC_PER_SEC*1.0);
    log_printf(15, "Total time: %lf\n", dt);

    return(status);
}


//***********************************************************************
//  seglun_rw_func - Performs a bounds check (growing the file if needed for writes)
//     and then calls the actual R/W operation.
//***********************************************************************

gop_op_status_t seglun_rw_func(void *arg, int id)
{
    seglun_rw_t *sw = (seglun_rw_t *)arg;
    lio_seglun_priv_t *s = (lio_seglun_priv_t *)sw->seg->priv;
    int i;
    gop_op_status_t status;
    char *label;
    ex_off_t new_size;
    ex_off_t pos, maxpos, t1, t2, t3;
    apr_time_t now;
    double dt;

    //** Find the max extent;
    maxpos = 0;
    for (i=0; i<sw->n_iov; i++) {
        pos = sw->iov[i].offset + sw->iov[i].len - 1;
        if (pos > maxpos) maxpos = pos;
    }


    segment_lock(sw->seg);
    log_printf(2, "sid=" XIDT " n_iov=%d off[0]=" XOT " len[0]=" XOT " max_size=" XOT " used_size=" XOT "\n",
               segment_id(sw->seg), sw->n_iov, sw->iov[0].offset, sw->iov[0].len, s->total_size, s->used_size);

    if (maxpos >= s->total_size) { //** Need to grow it first
        if (sw->rw_mode == 1) { //** Write op so grow the file
            new_size = maxpos + s->n_devices * s->excess_block_size;
            if (s->total_size < new_size) {  //** Check again within the lock
                log_printf(3, " seg=" XIDT " GROWING  curr_used_size=" XOT " curr_total_size=" XOT " new_size=" XOT " requested maxpos=" XOT "\n",
                           segment_id(sw->seg), s->used_size, s->total_size, new_size, maxpos);
                status = _slun_truncate(sw->seg, sw->da, -new_size, sw->timeout);  //** This ia grow op so (-) new_size
                log_printf(3, " seg=" XIDT " GROWING  err=%d\n",segment_id(sw->seg), status.op_status);
                if (status.op_status != OP_STATE_SUCCESS) {
                    segment_unlock(sw->seg);
                    status.op_status = OP_STATE_FAILURE;
                    status.error_code = s->n_devices;
                    return(status);
                }
            }
        } else {  //** Got a bad offset so fail the whole thing
            log_printf(15, "ERROR seg=" XIDT " READ beyond EOF!  cur_size=" XOT " requested maxpos=" XOT "\n", segment_id(sw->seg), s->total_size, maxpos);
            segment_unlock(sw->seg);
            status.op_status = OP_STATE_FAILURE;
            status.error_code = s->n_devices;
            return(status);
        }
    }
    segment_unlock(sw->seg);

    if (tbx_log_level() > 0) {  //** Add some logging
        label = (sw->rw_mode == 1) ? "LUN_WRITE" : "LUN_READ";
        for (i=0; i<sw->n_iov; i++) {
            t1 = sw->iov[i].offset;
            t2 = t1 + sw->iov[i].len - 1;
            t3 = sw->iov[i].len;
            log_printf(1, "%s:START " XOT " " XOT " " XOT "\n", label, t1, t2, t3);
        }
    }

    //** Now do the actual R/W operation
    log_printf(15, "Before exec\n");
    now = apr_time_now();
    status = seglun_rw_op(sw->seg, sw->da, sw->rw_hints, sw->n_iov, sw->iov, sw->buffer, sw->boff, sw->rw_mode, sw->timeout);
    now = apr_time_now() - now;
    log_printf(15, "After exec err=%d\n", status.op_status);

    segment_lock(sw->seg);
    log_printf(15, "oldused=" XOT " maxpos=" XOT "\n", s->used_size, maxpos);


    if (tbx_log_level() > 0) {  //** Add some logging
        dt = (double) now / APR_USEC_PER_SEC;

        label = (sw->rw_mode == 1) ? "LUN_WRITE" : "LUN_READ";
        for (i=0; i<sw->n_iov; i++) {
            t1 = sw->iov[i].offset;
            t2 = t1 + sw->iov[i].len - 1;
            t3 = sw->iov[i].len;
            log_printf(1, "%s:END " XOT " : " XOT " " XOT " " XOT " %lf\n", label, t2, t1, t2, t3, dt);
        }
    }
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

gop_op_generic_t *seglun_write(lio_segment_t *seg, data_attr_t *da, lio_segment_rw_hints_t *rw_hints, int n_iov, ex_tbx_iovec_t *iov, tbx_tbuf_t *buffer, ex_off_t boff, int timeout)
{
    lio_seglun_priv_t *s = (lio_seglun_priv_t *)seg->priv;
    seglun_rw_t *sw;
    gop_op_generic_t *gop;

    tbx_type_malloc(sw, seglun_rw_t, 1);
    sw->seg = seg;
    sw->da = da;
    sw->rw_hints = rw_hints;
    sw->n_iov = n_iov;
    sw->iov = iov;
    sw->boff = boff;
    sw->buffer = buffer;
    sw->timeout = timeout;
    sw->rw_mode = 1;
    gop = gop_tp_op_new(s->tpc, NULL, seglun_rw_func, (void *)sw, free, 1);

    return(gop);
}

//***********************************************************************
// seglun_read - Read from a linear segment
//***********************************************************************

gop_op_generic_t *seglun_read(lio_segment_t *seg, data_attr_t *da, lio_segment_rw_hints_t *rw_hints, int n_iov, ex_tbx_iovec_t *iov, tbx_tbuf_t *buffer, ex_off_t boff, int timeout)
{
    lio_seglun_priv_t *s = (lio_seglun_priv_t *)seg->priv;
    seglun_rw_t *sw;
    gop_op_generic_t *gop;

    tbx_type_malloc(sw, seglun_rw_t, 1);
    sw->seg = seg;
    sw->da = da;
    sw->rw_hints = rw_hints;
    sw->n_iov = n_iov;
    sw->iov = iov;
    sw->boff = boff;
    sw->buffer = buffer;
    sw->timeout = timeout;
    sw->rw_mode = 0;
    gop = gop_tp_op_new(s->tpc, NULL, seglun_rw_func, (void *)sw, free, 1);

    return(gop);
}


//***********************************************************************
// seglun_remove - DECrements the ref counts for the segment which could
//     result in the data being removed.
//***********************************************************************

gop_op_generic_t *seglun_remove(lio_segment_t *seg, data_attr_t *da, int timeout)
{
    lio_seglun_priv_t *s = (lio_seglun_priv_t *)seg->priv;
    gop_op_generic_t *gop;
    gop_opque_t *q;
    seglun_row_t *b;
    tbx_isl_iter_t it;
    int i, j, n;

    q = gop_opque_new();

    segment_lock(seg);
    n = tbx_isl_count(s->isl);
    it = tbx_isl_iter_search(s->isl, (tbx_sl_key_t *)NULL, (tbx_sl_key_t *)NULL);
    for (i=0; i<n; i++) {
        b = (seglun_row_t *)tbx_isl_next(&it);
        for (j=0; j < s->n_devices; j++) {
            gop = ds_remove(b->block[j].data->ds, da, ds_get_cap(b->block[j].data->ds, b->block[j].data->cap, DS_CAP_MANAGE), timeout);
            gop_opque_add(q, gop);
        }
    }
    segment_unlock(seg);

    log_printf(15, "seg=" XIDT " qid=%d ntasks=%d\n", segment_id(seg), gop_id(opque_get_gop(q)), gop_opque_task_count(q));
    if (n == 0) {
        gop_opque_free(q, OP_DESTROY);
        return(gop_dummy(gop_success_status));
    }
    return(opque_get_gop(q));
}

//***********************************************************************
// seglun_migrate_func - Attempts to migrate any flagged allocations
//***********************************************************************

gop_op_status_t seglun_migrate_func(void *arg, int id)
{
    seglun_inspect_t *si = (seglun_inspect_t *)arg;
    lio_seglun_priv_t *s = (lio_seglun_priv_t *)si->seg->priv;
    seglun_row_t *b;
    int bufsize = 10*1024;
    char info[bufsize];
    ex_off_t sstripe, estripe;
    int used;
    int block_status[s->n_devices], block_copy[s->n_devices];
    int nattempted, nmigrated, err, i;
    int soft_error_fail;

    gop_op_status_t status = gop_success_status;
    tbx_isl_iter_t it;

    soft_error_fail = (si->inspect_mode & INSPECT_SOFT_ERROR_FAIL);

    segment_lock(si->seg);

    info_printf(si->fd, 1, XIDT ": segment information: n_devices=%d n_shift=%d chunk_size=" XOT "  used_size=" XOT " total_size=" XOT " mode=%d\n", segment_id(si->seg), s->n_devices, s->n_shift, s->chunk_size, s->used_size, s->total_size, si->inspect_mode);

    nattempted = 0;
    nmigrated = 0;
    it = tbx_isl_iter_search(s->isl, (tbx_sl_key_t *)NULL, (tbx_sl_key_t *)NULL);
    for (b = (seglun_row_t *)tbx_isl_next(&it); b != NULL; b = (seglun_row_t *)tbx_isl_next(&it)) {
        for (i=0; i < s->n_devices; i++) block_status[i] = 0;

        sstripe = b->seg_offset / s->stripe_size;
        estripe = b->seg_end / s->stripe_size;
        info_printf(si->fd, 1, XIDT ": Checking row: (" XOT ", " XOT ", " XOT ")   Stripe: (" XOT ", " XOT ")\n", segment_id(si->seg), b->seg_offset, b->seg_end, b->row_len, sstripe, estripe);

        for (i=0; i < s->n_devices; i++) {
            info_printf(si->fd, 3, XIDT ":     dev=%i rcap=%s\n", segment_id(si->seg), i, (char *)ds_get_cap(s->ds, b->block[i].data->cap, DS_CAP_READ));
        }

        for (i=0; i < s->n_devices; i++) block_status[i] = 0;
        err = slun_row_placement_check(si->seg, si->da, b, block_status, s->n_devices, soft_error_fail, si->args->query, si->args, si->timeout);
        used = 0;
        tbx_append_printf(info, &used, bufsize, XIDT ":     slun_row_placement_check:", segment_id(si->seg));
        for (i=0; i < s->n_devices; i++) tbx_append_printf(info, &used, bufsize, " %d", block_status[i]);
        info_printf(si->fd, 1, "%s\n", info);
        if ((err > 0) || (si->args->rid_changes != NULL)) {
            memcpy(block_copy, block_status, sizeof(int)*s->n_devices);

            i = slun_row_placement_fix(si->seg, si->da, b, block_status, s->n_devices, s->rsq, si->timeout);
            nmigrated +=  err - i;
            nattempted += err;

            for (i=0; i < s->n_devices; i++) {
                if (block_copy[i] != 0) {
                    if (block_status[i] == 0) {
                        info_printf(si->fd, 2, XIDT ":     dev=%i moved to rcap=%s\n", segment_id(si->seg), i, (char *)ds_get_cap(s->ds, b->block[i].data->cap, DS_CAP_READ));
                    } else if (block_status[i] == -103) { //** Can't opportunistically move the allocation so unflagg it
                        log_printf(0, "OPPORTUNISTIC mv failed i=%d\n", i);
                        block_status[i] = 0;
                        nattempted--;
                        nmigrated--;  //** Adjust the totals
                    }
                }
            }

            used =0;
            tbx_append_printf(info, &used, bufsize, XIDT ":     slun_row_placement_fix:", segment_id(si->seg));
            for (i=0; i < s->n_devices; i++) tbx_append_printf(info, &used, bufsize, " %d", block_status[i]);
            info_printf(si->fd, 1, "%s\n", info);
        } else {
            nattempted = nattempted + err;
        }

    }

    segment_unlock(si->seg);

    if (nattempted != nmigrated) {
        info_printf(si->fd, 1, XIDT ": status: FAILURE (%d needed migrating, %d migrated)\n", segment_id(si->seg), nattempted, nmigrated);
        status = gop_failure_status;
    } else {
        info_printf(si->fd, 1, XIDT ": status: SUCCESS (%d needed migrating, %d migrated)\n", segment_id(si->seg), nattempted, nmigrated);
    }

    return(status);
}

//***********************************************************************
// seglun_inspect_func - Checks that all the segments are available and they are the right size
//     and corrects them if requested
//***********************************************************************

gop_op_status_t seglun_inspect_func(void *arg, int id)
{
    seglun_inspect_t *si = (seglun_inspect_t *)arg;
    lio_seglun_priv_t *s = (lio_seglun_priv_t *)si->seg->priv;
    seglun_row_t *b;
    rs_query_t *query;
    gop_op_status_t status;
    tbx_isl_iter_t it;
    int bufsize = 10*1024;
    char info[bufsize];
    ex_off_t sstripe, estripe;
    int used, soft_error_fail, force_reconstruct, nforce;
    int block_status[s->n_devices], block_copy[s->n_devices], block_tmp[s->n_devices];
    int i, j, err, option, force_repair, max_lost, total_lost, total_repaired, total_migrate, nmigrated, nlost, nrepaired, drow;
    lio_inspect_args_t args;
    lio_inspect_args_t args_blank;
    apr_time_t dt[s->n_devices];
    char pp[128];

    args = *(si->args);
    args_blank = args;
    args_blank.rid_changes = NULL;
    args_blank.rid_lock = NULL;

    status = gop_success_status;
    max_lost = 0;
    total_lost = 0;
    total_repaired = 0;
    total_migrate = 0;
    nmigrated = 0;

    option = si->inspect_mode & INSPECT_COMMAND_BITS;
    soft_error_fail = (si->inspect_mode & INSPECT_SOFT_ERROR_FAIL);
    force_reconstruct = (si->inspect_mode & INSPECT_FORCE_RECONSTRUCTION);
    force_repair = 0;
    if ((option == INSPECT_QUICK_REPAIR) || (option == INSPECT_SCAN_REPAIR) || (option == INSPECT_FULL_REPAIR)) force_repair = si->inspect_mode & INSPECT_FORCE_REPAIR;

    segment_lock(si->seg);

    //** Form the query to use
    query = rs_query_dup(s->rs, s->rsq);
    if (si->args != NULL) {
        if (si->args->query != NULL) {  //** Local query needs to be added
            rs_query_append(s->rs, query, si->args->query);
            rs_query_add(s->rs, &query, RSQ_BASE_OP_AND, NULL, 0, NULL, 0);
        }
    }
    args.query = query;

    info_printf(si->fd, 1, XIDT ": segment information: n_devices=%d n_shift=%d chunk_size=" XOT "  used_size=" XOT " total_size=" XOT " mode=%d\n", segment_id(si->seg), s->n_devices, s->n_shift, s->chunk_size, s->used_size, s->total_size, si->inspect_mode);

    si->args->n_dev_rows = tbx_isl_range_count(s->isl, (tbx_sl_key_t *)NULL, (tbx_sl_key_t *)NULL);
    drow = -1;
    it = tbx_isl_iter_search(s->isl, (tbx_sl_key_t *)NULL, (tbx_sl_key_t *)NULL);
    for (b = (seglun_row_t *)tbx_isl_next(&it); b != NULL; b = (seglun_row_t *)tbx_isl_next(&it)) {
        drow++;
        for (i=0; i < s->n_devices; i++) block_status[i] = 0;

        sstripe = b->seg_offset / s->stripe_size;
        estripe = b->seg_end / s->stripe_size;
        info_printf(si->fd, 1, XIDT ": Checking row: (" XOT ", " XOT ", " XOT ")   Stripe: (" XOT ", " XOT ")\n", segment_id(si->seg), b->seg_offset, b->seg_end, b->row_len, sstripe, estripe);

        for (i=0; i < s->n_devices; i++) {
            info_printf(si->fd, 3, XIDT ":     dev=%i rcap=%s\n", segment_id(si->seg), i, (char *)ds_get_cap(s->ds, b->block[i].data->cap, DS_CAP_READ));
        }

        nlost = slun_row_size_check(si->seg, si->da, b, block_status, dt, s->n_devices, force_repair, si->timeout);
        used = 0;
        tbx_append_printf(info, &used, bufsize, XIDT ":     slun_row_size_check:", segment_id(si->seg));
        for (i=0; i < s->n_devices; i++) {
            if ((b->block[i].read_err_count > 0) && ((si->inspect_mode & INSPECT_FIX_READ_ERROR) > 0)) {
                if (block_status[i] == 0) nlost++;
                block_status[i] += 4;
            }
            if ((b->block[i].write_err_count > 0) && ((si->inspect_mode & INSPECT_FIX_WRITE_ERROR) > 0)) {
                if (block_status[i] == 0) nlost++;
                block_status[i] += 8;
            }
            tbx_append_printf(info, &used, bufsize, " %d", block_status[i]);
        }

        //** Add the timing info
        tbx_append_printf(info, &used, bufsize, "  [");
        for (i=0; i < s->n_devices; i++) {
            tbx_append_printf(info, &used, bufsize, " %s", tbx_stk_pretty_print_double_with_scale(1000, (double)dt[i], pp));
        }
        info_printf(si->fd, 1, "%s (us)]\n", info);

        if (max_lost < nlost) max_lost = nlost;
        si->args->dev_row_replaced[drow] += nlost;
        log_printf(5, "row=%d nlost=%d dev_row_replaced=%d\n", drow, nlost, si->args->dev_row_replaced[drow]);

        nrepaired = 0;
        if ((force_repair > 0) && (nlost > 0)) {
            info_printf(si->fd, 1, XIDT ":     Attempting to pad the row\n", segment_id(si->seg));
            err = slun_row_pad_fix(si->seg, si->da, b, block_status, s->n_devices, si->timeout);

            used = 0;
            tbx_append_printf(info, &used, bufsize, XIDT ":     slun_row_pad_fix:", segment_id(si->seg));
            for (i=0; i < s->n_devices; i++) tbx_append_printf(info, &used, bufsize, " %d", block_status[i]);
            info_printf(si->fd, 1, "%s\n", info);

            for (i=0; i < s->n_devices; i++) {
                if (block_status[i] != 0) info_printf(si->fd, 5, XIDT ":     dev=%i replacing rcap=%s\n", segment_id(si->seg), i, (char *)ds_get_cap(s->ds, b->block[i].data->cap, DS_CAP_READ));
            }

            if (max_lost < err) max_lost = err;

            info_printf(si->fd, 1, XIDT ":     Attempting to replace missing row allocations (%d total allocs replaced or replacing for row)\n", segment_id(si->seg), si->args->dev_row_replaced[drow]);
            j = 0;  //** Iteratively try and repair the row
            do {
                memcpy(block_copy, block_status, sizeof(int)*s->n_devices);

                err = slun_row_replace_fix(si->seg, si->da, b, block_status, s->n_devices, &args, si->timeout);

                for (i=0; i < s->n_devices; i++) {
                    if ((block_copy[i] != 0) && (block_status[i] == 0)) info_printf(si->fd, 2, XIDT ":     dev=%i replaced with rcap=%s\n", segment_id(si->seg), i, (char *)ds_get_cap(s->ds, b->block[i].data->cap, DS_CAP_READ));
                }

                used = 0;
                tbx_append_printf(info, &used, bufsize, XIDT ":     slun_row_replace_fix:", segment_id(si->seg));
                for (i=0; i < s->n_devices; i++) tbx_append_printf(info, &used, bufsize, " %d", block_status[i]);
                info_printf(si->fd, 1, "%s\n", info);

                j++;
            } while ((err > 0) && (j<5));

            //** Add the range as repaired
            tbx_range_stack_merge2(&(si->args->bad_ranges), b->seg_offset, b->seg_end);

            nrepaired = nlost - err;
        }

        err = 0;
        for (i=0; i < s->n_devices; i++) if (block_status[i] != 0) err++;
        if (err != 0) goto fail;

        log_printf(0, "BEFORE_PLACEMENT_CHECK\n");
        err = slun_row_placement_check(si->seg, si->da, b, block_status, s->n_devices, soft_error_fail, query, &args, si->timeout);
        for (i=0; i < s->n_devices; i++) tbx_append_printf(info, &used, bufsize, " %d", block_status[i]);

        total_migrate += err;
        used = 0;
        tbx_append_printf(info, &used, bufsize, XIDT ":     slun_row_placement_check:", segment_id(si->seg));
        for (i=0; i < s->n_devices; i++) tbx_append_printf(info, &used, bufsize, " %d", block_status[i]);
        info_printf(si->fd, 1, "%s\n", info);
        log_printf(0, "AFTER_PLACEMENT_CHECK\n");
        if ((err > 0) && ((option == INSPECT_QUICK_REPAIR) || (option == INSPECT_SCAN_REPAIR) || (option == INSPECT_FULL_REPAIR))) {
            if (force_reconstruct == 0) {
                memcpy(block_copy, block_status, sizeof(int)*s->n_devices);
                i = slun_row_placement_fix(si->seg, si->da, b, block_status, s->n_devices, &args, si->timeout);
                nmigrated += err - i;
                memcpy(block_tmp, block_status, sizeof(int)*s->n_devices);

                j = 0;
                for (i=0; i < s->n_devices; i++) {
                    if (block_copy[i] != 0) {
                        if (block_status[i] == 0) {
                            info_printf(si->fd, 2, XIDT ":     dev=%i moved to rcap=%s\n", segment_id(si->seg), i, (char *)ds_get_cap(s->ds, b->block[i].data->cap, DS_CAP_READ));
                        } else if (block_status[i] == -103) { //** Can't opportunistically move the allocation so unflagg it but I need to check for collisions later
                            log_printf(0, "OPPORTUNISTIC mv failed i=%d\n", i);
                            block_status[i] = 0;
                            total_migrate--; //** Adjust the totals
                            j++;
                        }
                    }
                }

                //** Opportunistic shuffle failed so check for placement collisions
                if (j > 0) {
                    err = slun_row_placement_check(si->seg, si->da, b, block_tmp, s->n_devices, soft_error_fail, query, &args_blank, si->timeout);
                    if (err > 0) {
                        log_printf(1, "ERROR: opportunistic overlap.  err=%d\n", err);
                        info_printf(si->fd, 1, XIDT ": ERROR: opportunistic overlap.  err=%d\n", segment_id(si->seg), err);
                        memcpy(block_status, block_tmp, sizeof(int)*s->n_devices);
                        total_migrate += err;
                    }
                }

                used = 0;
                tbx_append_printf(info, &used, bufsize, XIDT ":     slun_row_placement_fix:", segment_id(si->seg));
                for (i=0; i < s->n_devices; i++) tbx_append_printf(info, &used, bufsize, " %d", block_status[i]);
                info_printf(si->fd, 1, "%s\n", info);
            } else if (force_repair > 0) {  //** Don't want to use depot-depot copies so instead make a blank allocation and let the higher level handle things
                j = 0;  //** Iteratively try and repair the row
                nforce = err;
                if (max_lost < err) max_lost = err;
                do {
                    memcpy(block_copy, block_status, sizeof(int)*s->n_devices);
                    err = slun_row_replace_fix(si->seg, si->da, b, block_status, s->n_devices, &args, si->timeout);

                    for (i=0; i < s->n_devices; i++) {
                        if ((block_copy[i] != 0) && (block_status[i] == 0)) info_printf(si->fd, 2, XIDT ":     dev=%i replaced rcap=%s\n", segment_id(si->seg), i, (char *)ds_get_cap(s->ds, b->block[i].data->cap, DS_CAP_READ));
                    }
                    used = 0;
                    tbx_append_printf(info, &used, bufsize, XIDT ":     slun_row_replace_fix:", segment_id(si->seg));
                    for (i=0; i < s->n_devices; i++) tbx_append_printf(info, &used, bufsize, " %d", block_status[i]);
                    info_printf(si->fd, 1, "%s\n", info);
                    j++;
                } while ((err > 0) && (j<5));

                //** Add the range as repaired
                tbx_range_stack_merge2(&(si->args->bad_ranges), b->seg_offset, b->seg_end);

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
        status = gop_failure_status;
    } else {
        info_printf(si->fd, 1, XIDT ": status: SUCCESS (%d max dev/row lost, %d lost, %d repaired, %d need(s) moving, %d moved)\n", segment_id(si->seg), max_lost, total_lost, total_repaired, total_migrate, nmigrated);
    }

    rs_query_destroy(s->rs, query);

    status.error_code = max_lost;  //** error_code < 0 means a placement error
    status.error_code |= (max_lost == 0) ? 0 : INSPECT_RESULT_HARD_ERROR;
    status.error_code |= (total_migrate == nmigrated) ? 0 : INSPECT_RESULT_MIGRATE_ERROR;
    return(status);
}

//***********************************************************************
//  seglun_inspect_func - Does the actual segment inspection operations
//***********************************************************************

gop_op_generic_t *seglun_inspect(lio_segment_t *seg, data_attr_t *da, tbx_log_fd_t *fd, int mode, ex_off_t bufsize, lio_inspect_args_t *args, int timeout)
{
    lio_seglun_priv_t *s = (lio_seglun_priv_t *)seg->priv;
    tbx_isl_iter_t it;
    seglun_row_t *b;
    gop_op_generic_t *gop;
    gop_op_status_t err;
    seglun_inspect_t *si;
    lio_ex3_inspect_command_t option;
    int i;

    gop = NULL;
    option = mode & INSPECT_COMMAND_BITS;

    switch (option) {
    case (INSPECT_QUICK_CHECK):
    case (INSPECT_SCAN_CHECK):
    case (INSPECT_FULL_CHECK):
    case (INSPECT_QUICK_REPAIR):
    case (INSPECT_SCAN_REPAIR):
    case (INSPECT_FULL_REPAIR):
        tbx_type_malloc(si, seglun_inspect_t, 1);
        si->seg = seg;
        si->da = da;
        si->fd = fd;
        si->inspect_mode = mode;
        si->bufsize = bufsize;
        si->timeout = timeout;
        si->args = args;
        gop = gop_tp_op_new(s->tpc, NULL, seglun_inspect_func, (void *)si, free, 1);
        break;
    case (INSPECT_MIGRATE):
        tbx_type_malloc(si, seglun_inspect_t, 1);
        si->seg = seg;
        si->da = da;
        si->fd = fd;
        si->inspect_mode = mode;
        si->bufsize = bufsize;
        si->timeout = timeout;
        si->args = args;
        gop = gop_tp_op_new(s->tpc, NULL, seglun_migrate_func, (void *)si, free, 1);
        break;
    case (INSPECT_SOFT_ERRORS):
    case (INSPECT_HARD_ERRORS):
        segment_lock(seg);
        err.error_code = s->hard_errors;
        segment_unlock(seg);
        err.op_status = (err.error_code == 0) ? OP_STATE_SUCCESS : OP_STATE_FAILURE;
        gop = gop_dummy(err);
        break;
    case (INSPECT_WRITE_ERRORS):
        segment_lock(seg);
        //** Cycle through the blocks counting the write errors
        it = tbx_isl_iter_search(s->isl, (tbx_sl_key_t *)NULL, (tbx_sl_key_t *)NULL);
        err.error_code = 0;
        while ((b = (seglun_row_t *)tbx_isl_next(&it)) != NULL) {
            for (i=0; i < s->n_devices; i++) {
                err.error_code += b->block[i].write_err_count;
            }
        }
        segment_unlock(seg);

        err.op_status = (err.error_code == 0) ? OP_STATE_SUCCESS : OP_STATE_FAILURE;
        gop = gop_dummy(err);
        break;
    case (INSPECT_NO_CHECK):
        break;
    }

    return(gop);
}

//***********************************************************************
// seglun_flush - Flushes a segment
//***********************************************************************

gop_op_generic_t *seglun_flush(lio_segment_t *seg, data_attr_t *da, ex_off_t lo, ex_off_t hi, int timeout)
{
    return(gop_dummy(gop_success_status));
}

//***********************************************************************
// seglun_clone_func - Clone data from the segment
//***********************************************************************

gop_op_status_t seglun_clone_func(void *arg, int id)
{
    seglun_clone_t *slc = (seglun_clone_t *)arg;
    lio_seglun_priv_t *ss = (lio_seglun_priv_t *)slc->sseg->priv;
    lio_seglun_priv_t *sd = (lio_seglun_priv_t *)slc->dseg->priv;
    tbx_isl_iter_t its, itd;
    seglun_row_t *bd, *bs;
    ex_off_t row_size, max_gops, n_gops, offset, d_offset, len, end;
    int err, dir, i, j, k, *max_index, n_rows, n;
    tbx_stack_t **gop_stack;
    gop_opque_t *q;
    gop_op_generic_t *gop = NULL;
    gop_op_generic_t *gop_next;
    gop_op_status_t status;

    //** See if we are using an old seg.  If so we need to trunc it first
    if (slc->trunc == 1) {
        gop_sync_exec(lio_segment_truncate(slc->dseg, slc->da, 0, slc->timeout));
    }

    if (ss->total_size == 0) return(gop_success_status);  //** No data to clone


    segment_lock(slc->sseg);

    //** Determine how many elements and reserve the space for it.
    n_rows = tbx_isl_count(ss->isl);
    tbx_type_malloc_clear(max_index, int, n_rows*ss->n_devices);

    //** Grow the file size but keep the same breaks as the original
    its = tbx_isl_iter_search(ss->isl, (tbx_sl_key_t *)NULL, (tbx_sl_key_t *)NULL);
    row_size = -1;
    bs = NULL;
    i = 0;
    max_gops = 0;
    while ((bs = (seglun_row_t *)tbx_isl_next(&its)) != NULL) {
        row_size = bs->row_len;
        if (bs->row_len != ss->max_row_size) {
            //** grow the destination to the same size as the source
            log_printf(15, "dseg=" XIDT " Growing dest to " XOT "\n", segment_id(slc->dseg), bs->seg_end+1);
            err = gop_sync_exec(lio_segment_truncate(slc->dseg, slc->da, bs->seg_end+1, slc->timeout));
            if (err != OP_STATE_SUCCESS) {
                log_printf(15, "Error growing destination! dseg=" XIDT "\n", segment_id(slc->dseg));
                sd->grow_break = 0; //** Undo the break flag
                free(max_index);
                segment_unlock(slc->sseg);
                return(gop_failure_status);
            }
            sd->used_size = ss->used_size;
            sd->grow_break = 1; //** Flag a break for the next grow operation
            row_size = -1;
        }

        n_gops = bs->block_len / slc->max_transfer;
        if ((bs->block_len % slc->max_transfer) > 0) n_gops++;
        for (j=0; j<ss->n_devices; j++) max_index[i+j] = n_gops;
        if (n_gops > max_gops) max_gops = n_gops;
        i++;
    }

    //** Do the final grow if needed
    if (row_size != -1) {
        if (bs) {
            log_printf(15, "dseg=" XIDT " Growing dest to " XOT "\n", segment_id(slc->dseg), bs->seg_end+1);
            err = gop_sync_exec(lio_segment_truncate(slc->dseg, slc->da, bs->seg_end+1, slc->timeout));
        } else {
            err = OP_STATE_FAILURE;
            log_printf(15, "dseg=" XIDT " Growing dest to " XOT "\n", segment_id(slc->dseg), bs->seg_end+1);
        }
        if (err != OP_STATE_SUCCESS) {
            log_printf(15, "Error growing destination! dseg=" XIDT "\n", segment_id(slc->dseg));
            sd->grow_break = 0; //** Undo the break flag
            free(max_index);
            segment_unlock(slc->sseg);
            return(gop_failure_status);
        }
        sd->used_size = ss->used_size;
    }

    sd->grow_break = 0; //** Finished growing so undo the break flag

    tbx_type_malloc_clear(gop_stack, tbx_stack_t *, n_rows*ss->n_devices);
    for (i=0; i<n_rows*ss->n_devices; i++) gop_stack[i] = tbx_stack_new();

    //** Generate the copy list
    q = gop_opque_new();
    opque_start_execution(q);
    dir = ((slc->mode & DS_PULL) > 0) ? DS_PULL : DS_PUSH;

    its = tbx_isl_iter_search(ss->isl, (tbx_sl_key_t *)NULL, (tbx_sl_key_t *)NULL);
    itd = tbx_isl_iter_search(sd->isl, (tbx_sl_key_t *)NULL, (tbx_sl_key_t *)NULL);
    j = 0;
    n = 0;
    while ((bs = (seglun_row_t *)tbx_isl_next(&its)) != NULL) {
        bd = (seglun_row_t *)tbx_isl_next(&itd);

        for (i=0; i < ss->n_devices; i++) {
            len = slc->max_transfer;
            d_offset = bd->block[i].cap_offset;
            offset = bs->block[i].cap_offset;
            end = offset + bs->block_len;
            k = 0;
            do {
                if ((offset+len) >= end) {
                    len = end - offset;
                    k = -1;
                }

                gop = ds_copy(bd->block[i].data->ds, slc->da, dir, NS_TYPE_SOCK, "",
                              ds_get_cap(bs->block[i].data->ds, bs->block[i].data->cap, DS_CAP_READ), offset,
                              ds_get_cap(bd->block[i].data->ds, bd->block[i].data->cap, DS_CAP_WRITE), d_offset,
                              len, slc->timeout);
                gop_set_private(gop, gop_stack[j]);
                n++;

                if (k<1) {  //** Start executing the 1st couple for each allocation
                    gop_opque_add(q, gop);
                } else {    //** The rest we place on a stack
                    tbx_stack_move_to_bottom(gop_stack[j]);
                    tbx_stack_insert_below(gop_stack[j], gop);
                }

                d_offset += len;
                offset += len;
                k++;
            } while (k > 0);

            j++;
        }
    }

    segment_unlock(slc->sseg);

    log_printf(5, "Total number of tasks: %d\n", n);

    //** Loop through adding tasks as needed
    for (i=0; i<n; i++) {
        gop = opque_waitany(q);
        gop_next = tbx_stack_pop((tbx_stack_t *)gop_get_private(gop));
        if (gop_next != NULL) gop_opque_add(q, gop_next);
        gop_free(gop, OP_DESTROY);
    }

    //** Wait for the copying to finish
    opque_waitall(q);
    status = (gop_opque_tasks_failed(q) == 0) ? gop_success_status : gop_failure_status;

    gop_opque_free(q, OP_DESTROY);
    free(max_index);
    for (i=0; i<n_rows*ss->n_devices; i++) tbx_stack_free(gop_stack[i], 0);
    free(gop_stack);
    return(status);
}


//***********************************************************************
// seglun_clone - Clones a segment
//***********************************************************************

gop_op_generic_t *seglun_clone(lio_segment_t *seg, data_attr_t *da, lio_segment_t **clone_seg, int mode, void *attr, int timeout)
{
    lio_segment_t *clone;
    lio_seglun_priv_t *ss = (lio_seglun_priv_t *)seg->priv;
    lio_seglun_priv_t *sd;
    gop_op_generic_t *gop;
    seglun_clone_t *slc;
    int use_existing = (*clone_seg != NULL) ? 1 : 0;

    //** Make the base segment
    if (use_existing == 0) *clone_seg = segment_lun_create(seg->ess);
    clone = *clone_seg;
    sd = (lio_seglun_priv_t *)clone->priv;

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
            gop = lio_segment_truncate(clone, da, 0, timeout);
        } else {
            gop = gop_dummy(gop_success_status);
        }
    } else {
        tbx_type_malloc(slc, seglun_clone_t, 1);
        slc->sseg = seg;
        slc->dseg = clone;
        slc->da = da;
        slc->mode = mode;
        slc->timeout = timeout;
        slc->trunc = use_existing;
        slc->max_transfer = 20*1024*1024;
        gop = gop_tp_op_new(sd->tpc, NULL, seglun_clone_func, (void *)slc, free, 1);
    }

    return(gop);
}

//***********************************************************************
// seglun_size - Returns the segment size.
//***********************************************************************

ex_off_t seglun_size(lio_segment_t *seg)
{
    lio_seglun_priv_t *s = (lio_seglun_priv_t *)seg->priv;
    ex_off_t size;

    segment_lock(seg);
    size = s->used_size;
    segment_unlock(seg);

    return(size);
}

//***********************************************************************
// seglun_block_size - Returns the segment block size.
//***********************************************************************

ex_off_t seglun_block_size(lio_segment_t *seg, int btype)
{
    lio_seglun_priv_t *s = (lio_seglun_priv_t *)seg->priv;

    if (btype == LIO_SEGMENT_BLOCK_NATURAL) return(s->stripe_size);

    return(1);
}

//***********************************************************************
// seglun_signature - Generates the segment signature
//***********************************************************************

int seglun_signature(lio_segment_t *seg, char *buffer, int *used, int bufsize)
{
    lio_seglun_priv_t *s = (lio_seglun_priv_t *)seg->priv;

    tbx_append_printf(buffer, used, bufsize, "lun(\n");
    tbx_append_printf(buffer, used, bufsize, "    n_devices=%d\n", s->n_devices);
    tbx_append_printf(buffer, used, bufsize, "    n_shift=%d\n", s->n_shift);
    tbx_append_printf(buffer, used, bufsize, "    chunk_size=" XOT "\n", s->chunk_size);
    tbx_append_printf(buffer, used, bufsize, ")\n");

    return(0);
}

//***********************************************************************
// seglun_serialize_text_try - Convert the segment to a text based format
//***********************************************************************

int seglun_serialize_text_try(lio_segment_t *seg, char *segbuf, int bufsize, lio_exnode_exchange_t *cap_exp)
{
    lio_seglun_priv_t *s = (lio_seglun_priv_t *)seg->priv;
    char *ext, *etext;
    int sused, i, err;
    seglun_row_t *b;
    tbx_isl_iter_t it;


    sused = 0;
    segbuf[0] = 0;

    //** Store the segment header
    tbx_append_printf(segbuf, &sused, bufsize, "[segment-" XIDT "]\n", seg->header.id);
    if ((seg->header.name != NULL) && (strcmp(seg->header.name, "") != 0)) {
        etext = tbx_stk_escape_text("=", '\\', seg->header.name);
        tbx_append_printf(segbuf, &sused, bufsize, "name=%s\n", etext);
        free(etext);
    }
    tbx_append_printf(segbuf, &sused, bufsize, "type=%s\n", SEGMENT_TYPE_LUN);

    //** default resource query
    if (s->rsq != NULL) {
        ext = rs_query_print(s->rs, s->rsq);
        etext = tbx_stk_escape_text("=", '\\', ext);
        tbx_append_printf(segbuf, &sused, bufsize, "query_default=%s\n", etext);
        free(etext);
        free(ext);
    }

    tbx_append_printf(segbuf, &sused, bufsize, "n_devices=%d\n", s->n_devices);
    tbx_append_printf(segbuf, &sused, bufsize, "n_shift=%d\n", s->n_shift);

    //** Basic size info
    tbx_append_printf(segbuf, &sused, bufsize, "max_block_size=" XOT "\n", s->max_block_size);
    tbx_append_printf(segbuf, &sused, bufsize, "excess_block_size=" XOT "\n", s->excess_block_size);
    tbx_append_printf(segbuf, &sused, bufsize, "max_size=" XOT "\n", s->total_size);
    tbx_append_printf(segbuf, &sused, bufsize, "used_size=" XOT "\n", s->used_size);
    err = tbx_append_printf(segbuf, &sused, bufsize, "chunk_size=" XOT "\n", s->chunk_size);

    //** Cycle through the blocks storing both the segment block information and also the cap blocks
    it = tbx_isl_iter_search(s->isl, (tbx_sl_key_t *)NULL, (tbx_sl_key_t *)NULL);
    while ((b = (seglun_row_t *)tbx_isl_next(&it)) != NULL) {
        //** Add the segment stripe information
        tbx_append_printf(segbuf, &sused, bufsize, "row=" XOT ":" XOT ":" XOT, b->seg_offset, b->seg_end, b->row_len);
        for (i=0; i < s->n_devices; i++) {
            data_block_serialize(b->block[i].data, cap_exp); //** Add the cap
            tbx_append_printf(segbuf, &sused, bufsize, ":" XIDT ":" XOT, b->block[i].data->id, b->block[i].cap_offset);
        }
        err = tbx_append_printf(segbuf, &sused, bufsize, "\n");
        if (err == -1) break;  //** Kick out on the first error
    }

    return(err);
}

//***********************************************************************
// seglun_serialize_text -Convert the segment to a text based format
//***********************************************************************

int seglun_serialize_text(lio_segment_t *seg, lio_exnode_exchange_t *exp)
{
    int bufsize=100*1024;
    char staticbuf[bufsize];
    char *segbuf = staticbuf;
    lio_exnode_exchange_t *cap_exp;
    int err;

    do {
        cap_exp = lio_exnode_exchange_create(EX_TEXT);
        err = seglun_serialize_text_try(seg, segbuf, bufsize, cap_exp);
        if (err == -1) { //** Need to grow the buffer
            if (staticbuf != segbuf) free(segbuf);
            lio_exnode_exchange_destroy(cap_exp);

            bufsize = 2*bufsize;
            tbx_type_malloc(segbuf, char, bufsize);
            log_printf(1, "Growing buffer bufsize=%d\n", bufsize);
        }
    } while (err == -1);

    //** Merge everything together and return it
    exnode_exchange_append(exp, cap_exp);
    lio_exnode_exchange_destroy(cap_exp);

    exnode_exchange_append_text(exp, segbuf);

    if (staticbuf != segbuf) free(segbuf);

    return(0);
}

//***********************************************************************
// seglun_serialize_proto -Convert the segment to a protocol buffer
//***********************************************************************

int seglun_serialize_proto(lio_segment_t *seg, lio_exnode_exchange_t *exp)
{
    return(-1);
}

//***********************************************************************
// seglun_serialize -Convert the segment to a more portable format
//***********************************************************************

int seglun_serialize(lio_segment_t *seg, lio_exnode_exchange_t *exp)
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

int seglun_deserialize_text(lio_segment_t *seg, ex_id_t id, lio_exnode_exchange_t *exp)
{
    lio_seglun_priv_t *s = (lio_seglun_priv_t *)seg->priv;
    int bufsize=1024;
    char seggrp[bufsize];
    char *text, *etext, *token, *bstate, *key, *value;
    int fin, i, fail;
    seglun_row_t *b;
    seglun_block_t *block;
    tbx_inip_file_t *fd;
    tbx_inip_group_t *g;
    tbx_inip_element_t *ele;

    //** Parse the ini text
    fd = exp->text.fd;

    fail = 0;  //** Default to no failure

    //** Make the segment section name
    snprintf(seggrp, bufsize, "segment-" XIDT, id);

    //** Get the segment header info
    seg->header.id = id;
    seg->header.type = SEGMENT_TYPE_LUN;
    seg->header.name = tbx_inip_get_string(fd, seggrp, "name", "");

    //** default resource query
    etext = tbx_inip_get_string(fd, seggrp, "query_default", "");
    text = tbx_stk_unescape_text('\\', etext);
    s->rsq = rs_query_parse(s->rs, text);
    free(text);
    free(etext);

    s->n_devices = tbx_inip_get_integer(fd, seggrp, "n_devices", 2);

    //** Basic size info
    s->max_block_size = tbx_inip_get_integer(fd, seggrp, "max_block_size", 10*1024*1024);
    s->excess_block_size = tbx_inip_get_integer(fd, seggrp, "excess_block_size", s->max_block_size/4);
    s->total_size = tbx_inip_get_integer(fd, seggrp, "max_size", 0);
    s->used_size = tbx_inip_get_integer(fd, seggrp, "used_size", 0);
    if (s->used_size > s->total_size) s->used_size = s->total_size;  //** Sanity check the size
    s->chunk_size = tbx_inip_get_integer(fd, seggrp, "chunk_size", 16*1024);
    s->n_shift = tbx_inip_get_integer(fd, seggrp, "n_shift", 1);

    //** Make sure the mac block size is a mulitple of the chunk size
    s->max_block_size = (s->max_block_size / s->chunk_size);
    s->max_block_size = s->max_block_size * s->chunk_size;
    s->max_row_size = s->max_block_size * s->n_devices;
    s->stripe_size = s->n_devices * s->chunk_size;

    //** Cycle through the blocks storing both the segment block information and also the cap blocks
    g = tbx_inip_group_find(fd, seggrp);
    ele = tbx_inip_ele_first(g);
    while (ele != NULL) {
        key = tbx_inip_ele_get_key(ele);
        if (strcmp(key, "row") == 0) {
            tbx_type_malloc_clear(b, seglun_row_t, 1);
            tbx_type_malloc_clear(block, seglun_block_t, s->n_devices);
            b->block = block;
            b->rwop_index = -1;

            //** Parse the segment line
            value = tbx_inip_ele_get_value(ele);
            token = strdup(value);
            sscanf(tbx_stk_escape_string_token(token, ":", '\\', 0, &bstate, &fin), XOT, &(b->seg_offset));
            sscanf(tbx_stk_escape_string_token(NULL, ":", '\\', 0, &bstate, &fin), XOT, &(b->seg_end));
            sscanf(tbx_stk_escape_string_token(NULL, ":", '\\', 0, &bstate, &fin), XOT, &(b->row_len));
            b->block_len = b->row_len / s->n_devices;

            for (i=0; i< s->n_devices; i++) {
                sscanf(tbx_stk_escape_string_token(NULL, ":", '\\', 0, &bstate, &fin), XIDT, &id);
                sscanf(tbx_stk_escape_string_token(NULL, ":", '\\', 0, &bstate, &fin), XOT, &(block[i].cap_offset));

                //** Find the cooresponding cap
                block[i].data = data_block_deserialize(seg->ess, id, exp);
                if (block[i].data == NULL) {
                    log_printf(0, "Missing data block!  block id=" XIDT " seg=" XIDT "\n", id, segment_id(seg));
                    fail = 1;
                } else {
                    tbx_atomic_inc(block[i].data->ref_count);
                }

            }
            free(token);

            //** Finally add it to the ISL
            tbx_isl_insert(s->isl, (tbx_sl_key_t *)&(b->seg_offset), (tbx_sl_key_t *)&(b->seg_end), (tbx_sl_data_t *)b);
        }

        ele = tbx_inip_ele_next(ele);
    }

    return(fail);
}

//***********************************************************************
// seglun_deserialize_proto - Read the prot formatted segment
//***********************************************************************

int seglun_deserialize_proto(lio_segment_t *seg, ex_id_t id, lio_exnode_exchange_t *exp)
{
    return(-1);
}

//***********************************************************************
// seglun_deserialize -Convert from the portable to internal format
//***********************************************************************

int seglun_deserialize(lio_segment_t *seg, ex_id_t id, lio_exnode_exchange_t *exp)
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

void seglun_destroy(tbx_ref_t *ref)
{
    tbx_obj_t *obj = container_of(ref, tbx_obj_t, refcount);
    lio_segment_t *seg = container_of(obj, lio_segment_t, obj);
    int i, j, n, cnt;
    tbx_isl_iter_t it;
    seglun_row_t **b_list;
    lio_data_block_t *db;
    lio_seglun_priv_t *s = (lio_seglun_priv_t *)seg->priv;

    //** Check if it's still in use
    log_printf(15, "seglun_destroy: seg->id=" XIDT "\n", segment_id(seg));

    //** Disable notification about mapping changes
    rs_unregister_mapping_updates(s->rs, &(s->notify));
    apr_thread_mutex_destroy(s->notify.lock);
    apr_thread_cond_destroy(s->notify.cond);

    n = tbx_isl_count(s->isl);
    tbx_type_malloc_clear(b_list, seglun_row_t *, n);
    it = tbx_isl_iter_search(s->isl, (tbx_sl_key_t *)NULL, (tbx_sl_key_t *)NULL);
    for (i=0; i<n; i++) {
        b_list[i] = (seglun_row_t *)tbx_isl_next(&it);
    }
    tbx_isl_del(s->isl);

    for (i=0; i<n; i++) {
        for (j=0; j<s->n_devices; j++) {
            if (b_list[i]->block[j].data != NULL) {
                cnt = tbx_atomic_get(b_list[i]->block[j].data->ref_count);
                if (cnt > 0) tbx_atomic_dec(b_list[i]->block[j].data->ref_count);
                data_block_destroy(b_list[i]->block[j].data);
            }
        }
        free(b_list[i]->block);
        free(b_list[i]);
    }
    free(b_list);

    if (s->db_cleanup != NULL) {
        while ((db = tbx_stack_pop(s->db_cleanup)) != NULL) {
            cnt = tbx_atomic_get(db->ref_count);
            if (cnt > 0) tbx_atomic_dec(db->ref_count);
            data_block_destroy(db);
        }

        tbx_stack_free(s->db_cleanup, 0);
    }

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

lio_segment_t *segment_lun_create(void *arg)
{
    lio_service_manager_t *es = (lio_service_manager_t *)arg;
    lio_seglun_priv_t *s;
    lio_segment_t *seg;

    //** Make the space
    tbx_type_malloc_clear(seg, lio_segment_t, 1);
    tbx_type_malloc_clear(s, lio_seglun_priv_t, 1);

    s->isl = tbx_isl_new(&skiplist_compare_ex_off, NULL, NULL, NULL);
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
    tbx_obj_init(&seg->obj, (tbx_vtable_t *) &lio_seglun_vtable);
    seg->header.type = SEGMENT_TYPE_LUN;

    assert_result(apr_pool_create(&(seg->mpool), NULL), APR_SUCCESS);
    apr_thread_mutex_create(&(seg->lock), APR_THREAD_MUTEX_DEFAULT, seg->mpool);
    apr_thread_cond_create(&(seg->cond), seg->mpool);

    seg->ess = es;
    s->tpc = lio_lookup_service(es, ESS_RUNNING, ESS_TPC_UNLIMITED);
    s->rs = lio_lookup_service(es, ESS_RUNNING, ESS_RS);
    s->ds = lio_lookup_service(es, ESS_RUNNING, ESS_DS);
    s->bl = lio_lookup_service(es, ESS_RUNNING, "blacklist");

    //** Set up remap notifications
    apr_thread_mutex_create(&(s->notify.lock), APR_THREAD_MUTEX_DEFAULT, seg->mpool);
    apr_thread_cond_create(&(s->notify.cond), seg->mpool);
    s->notify.map_version = -1;  //** This should trigger a remap on the first R/W op.
    rs_register_mapping_updates(s->rs, &(s->notify));

    return(seg);
}

//***********************************************************************
// segment_linear_load - Loads a linear segment from ini/ex3
//***********************************************************************

lio_segment_t *segment_lun_load(void *arg, ex_id_t id, lio_exnode_exchange_t *ex)
{
    lio_segment_t *seg = segment_lun_create(arg);
    if (segment_deserialize(seg, id, ex) != 0) {
        seg = NULL;
    }
    return(seg);
}

const lio_segment_vtable_t lio_seglun_vtable = {
    .base.name = "seglun_vtable",
    .base.free_fn = seglun_destroy,
    .read = seglun_read,
    .write = seglun_write,
    .inspect = seglun_inspect,
    .truncate = seglun_truncate,
    .remove = seglun_remove,
    .flush = seglun_flush,
    .clone = seglun_clone,
    .signature = seglun_signature,
    .size = seglun_size,
    .block_size = seglun_block_size,
    .serialize = seglun_serialize,
    .deserialize = seglun_deserialize,
};

