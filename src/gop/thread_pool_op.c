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

#define _log_module_index 123

#include <apr_pools.h>
#include <apr_thread_mutex.h>
#include <apr_thread_proc.h>
#include <stdio.h>
#include <string.h>
#include <tbx/assert_result.h>
#include <tbx/atomic_counter.h>
#include <tbx/log.h>
#include <tbx/network.h>
#include <tbx/stack.h>
#include <tbx/type_malloc.h>

#include "gop.h"
#include "gop/tp.h"
#include "gop/types.h"

#define TP_MAX_DEPTH 100

typedef struct {  //** This is used to track the thread local versions of some globals to minimize locking
    int concurrent_max;                 //** My concurrent max
    int depth_concurrent[TP_MAX_DEPTH]; //** My copy of the concurrency/depth table
} thread_local_stats_t;

extern int _tp_context_count;
extern apr_thread_mutex_t *_tp_lock;
extern apr_pool_t *_tp_pool;
extern int _tp_stats;

static int _tp_concurrent_max;
static int _tp_depth_concurrent_max[TP_MAX_DEPTH];
static tbx_atomic_unit32_t _tp_depth_concurrent[TP_MAX_DEPTH];
static tbx_atomic_unit32_t _tp_concurrent;
static tbx_atomic_unit32_t _tp_depth_total[TP_MAX_DEPTH];

apr_threadkey_t *thread_local_stats_key = NULL;
apr_threadkey_t *thread_local_depth_key = NULL;

void _tp_submit_op(void *arg, gop_op_generic_t *gop);
void _tp_op_free(gop_op_generic_t *gop, int mode);

//*************************************************************************
// thread_pool_stats_print - Dumps the stats to the local log file
//*************************************************************************

void thread_pool_stats_print()
{
    int i, total;

    log_printf(0, "--------Thread Pool Stats----------\n");
    log_printf(0, "Max Concurrency: %d\n", _tp_concurrent_max);
    log_printf(0, "Level  Concurrent     Total\n");
    for (i=0; i<TP_MAX_DEPTH; i++) {
        total = tbx_atomic_get(_tp_depth_total[i]);
        log_printf(0, " %2d    %10d  %10d\n", i, _tp_depth_concurrent_max[i], total);
    }
}

//*************************************************************************
// thread_pool_stats_make - Makes the global thread pools stats
//*************************************************************************

void thread_pool_stats_make()
{
    int i;

    _tp_concurrent_max = 0;
    for (i=0; i<TP_MAX_DEPTH; i++) {
        _tp_depth_concurrent_max[i] = 0;
        tbx_atomic_set(_tp_depth_total[i], 0);
        tbx_atomic_set(_tp_depth_concurrent[i], 0);
    }
}

//*************************************************************************
// _thread_local_stats_ptr - Returns the pointer to the unique thread local stats
//*************************************************************************

thread_local_stats_t *_thread_local_stats_ptr()
{
    thread_local_stats_t *my = NULL;

    apr_threadkey_private_get((void *)&my, thread_local_stats_key);
    if (my == NULL ) {
        tbx_type_malloc_clear(my, thread_local_stats_t, 1);
        apr_thread_mutex_lock(_tp_lock);
        my->concurrent_max = _tp_concurrent_max;
        memcpy(my->depth_concurrent, _tp_depth_concurrent_max, sizeof(_tp_depth_concurrent_max));  //** Set to the current global
        apr_thread_mutex_unlock(_tp_lock);
        apr_threadkey_private_set(my, thread_local_stats_key);
    }

    return(my);
}


//*************************************************************************
// _thread_local_depth_ptr - Returns the pointer to the unique thread local depth
//*************************************************************************

int *_thread_local_depth_ptr()
{
    int *my_depth = NULL;

    apr_threadkey_private_get((void *)&my_depth, thread_local_depth_key);
    if (my_depth == NULL ) {
        tbx_type_malloc_clear(my_depth, int, 1);
        *my_depth = 0;
        apr_threadkey_private_set(my_depth, thread_local_depth_key);
    }

    return(my_depth);
}

//*************************************************************

gop_op_status_t tp_command(gop_op_generic_t *gop, tbx_ns_t *ns)
{
    return(gop_success_status);
}

//*************************************************************
// _tpc_overflow_next - Returns the next task for execution in
//     the overflow pool or NULL if none are available
//
//  NOTE: _tp_lock should be held on entry!!!
//*************************************************************

gop_op_generic_t *_tpc_overflow_next(gop_thread_pool_context_t *tpc)
{
    gop_op_generic_t *gop = NULL;
    gop_thread_pool_op_t *op;
    int i, dmax, slot;

    //** Determine the currently running max depth
    dmax = -1;
    slot = -1;
    if ((int)tbx_atomic_get(tpc->n_running) >= tpc->max_concurrency) { //** Don't care about a slot if less than the max concurrency
        for (i=0; i<tpc->recursion_depth; i++) {
            if (tpc->overflow_running_depth[i] > dmax) dmax = tpc->overflow_running_depth[i];
            if (tpc->overflow_running_depth[i] == -1) slot = i;
        }
    }

    //** Not look for a viable gop
    for (i=tpc->recursion_depth-1; i>dmax; i--) {
        gop = tbx_stack_pop(tpc->reserve_stack[i]);
        if (gop) {
            tbx_atomic_dec(tpc->n_overflow);
            op = gop_get_tp(gop);
            op->overflow_slot = slot;
            if (slot > -1) tpc->overflow_running_depth[slot] = i;
            break;
        }
    }

if (gop) {
    log_printf(0, "dmax=%d reserve=%d slot=%d gid=%d n_running=%d max=%d\n", dmax, i, slot, gop_id(gop), tbx_atomic_get(tpc->n_running), tpc->max_concurrency);
} else {
    log_printf(0, "dmax=%d reserve=%d slot=%d gop=%p n_running=%d max=%d\n", dmax, i, slot, gop, tbx_atomic_get(tpc->n_running), tpc->max_concurrency);
}
    return(gop);
}

//*************************************************************
void thread_pool_exec_fn(void *arg, gop_op_generic_t *gop)
{
    gop_thread_pool_op_t *op = gop_get_tp(gop);
    gop_thread_pool_context_t *tpc = op->tpc;
    gop_op_status_t status;
    thread_local_stats_t *my;
    int *my_depth;
    int tid;
    int concurrent, start_depth;

    tid = tbx_atomic_thread_id;

    op = gop_get_tp(gop);
    my_depth = _thread_local_depth_ptr();
    start_depth = *my_depth;  //** Store the old depth for later

    if (tid != op->parent_tid) *my_depth = op->depth;   //** Set the depth

    if (_tp_stats > 0) {
        //** Set everything to the GOP depth and inc if not running in the parent thread
        if (tid != op->parent_tid) {
            //** Check if we set a new high for max concurrency
            my = _thread_local_stats_ptr();
            concurrent = tbx_atomic_inc(_tp_concurrent) + 1;
            if (concurrent > my->concurrent_max) {
                my->concurrent_max = concurrent;
                apr_thread_mutex_lock(_tp_lock);  //** We passed the local check so now check the global
                if (concurrent > _tp_concurrent_max) _tp_concurrent_max = concurrent;
                apr_thread_mutex_unlock(_tp_lock);
            }

            tbx_atomic_inc(_tp_depth_total[*my_depth]);

            //** Check if we may have set a new concurrency limit for the depth
            concurrent = tbx_atomic_inc(_tp_depth_concurrent[*my_depth]) + 1;
            if (concurrent > my->depth_concurrent[*my_depth]) {
                apr_thread_mutex_lock(_tp_lock);  //** We passed the local check so now check the global table
                if (concurrent > _tp_depth_concurrent_max[*my_depth]) {
                    _tp_depth_concurrent_max[*my_depth] = concurrent;
                    my->depth_concurrent[*my_depth] = _tp_depth_concurrent_max[*my_depth];  //** Update to global
                }
                apr_thread_mutex_unlock(_tp_lock);
            }
        }
    }

    log_printf(4, "tp_recv: Start!!! gid=%d tid=%d op->depth=%d op->overflow_slot=%d n_overflow=%d\n", gop_id(gop), tid, op->depth, op->overflow_slot, tbx_atomic_get(tpc->n_overflow));
    tbx_atomic_inc(tpc->n_started);

    status = op->fn(op->arg, gop_id(gop));
    if (_tp_stats > 0) {
        if (tid != op->parent_tid) {
            tbx_atomic_dec(_tp_depth_concurrent[*(_thread_local_depth_ptr())]);
            tbx_atomic_dec(_tp_concurrent);
        }
    }

    log_printf(4, "tp_recv: end!!! gid=%d ptid=%d status=%d op->depth=%d op->overflow_slot=%d n_overflow=%d\n", gop_id(gop), op->parent_tid, status.op_status, op->depth, op->overflow_slot, tbx_atomic_get(tpc->n_overflow));

    if (op->overflow_slot != -1) { //** Need to clean up our overflow slot
        apr_thread_mutex_lock(_tp_lock);
        tpc->overflow_running_depth[op->overflow_slot] = -1;
        apr_thread_mutex_unlock(_tp_lock);
    }

    tbx_atomic_inc(tpc->n_completed);
    if (op->via_submit == 1) tbx_atomic_dec(tpc->n_running);  //** Update the concurrency if started by calling the proper submit fn

    gop_mark_completed(gop, status);

    //** Restore the original depth in case this is a sync_exec fn chain
    *my_depth = start_depth;

    //** Check if we need to get something from the overflow que
    if (tbx_atomic_get(tpc->n_overflow) > 0) {
        apr_thread_mutex_lock(_tp_lock);
        gop = _tpc_overflow_next(tpc);

        if (gop) {
            op = gop_get_tp(gop);
            if (op->overflow_slot != -1) {   //** Check if we need to undo our overflow slot since this submit may fail
                op->tpc->overflow_running_depth[op->overflow_slot] = -1;
            }
        }
        apr_thread_mutex_unlock(_tp_lock);

        if (gop) _tp_submit_op(NULL, gop); //** If we got one just loop around and process it
    }
}

//*************************************************************
// init_tp_op - Does the 1-time initialization for the op
//*************************************************************

void init_tp_op(gop_thread_pool_context_t *tpc, gop_thread_pool_op_t *op)
{
    gop_op_generic_t *gop;

    //** Clear it
    tbx_type_memclear(op, gop_thread_pool_op_t, 1);

    op->depth =  *(_thread_local_depth_ptr()) + 1; //** Store my recursion depth
    op->overflow_slot = -1;
    op->parent_tid = tbx_atomic_thread_id;   //** Also store the parent TID so we can adjust the depth if needed

    //** Now munge the pointers
    gop = &(op->gop);
    gop_init(gop);
    gop->op = &(op->dop);
    gop->op->priv = op;
    gop->type = Q_TYPE_OPERATION;
    op->tpc = tpc;
    op->dop.priv = op;
    op->dop.pc = tpc->pc; //**IS this needed?????
    gop->base.free = _tp_op_free;
    gop->free_ptr = op;
    gop->base.pc = tpc->pc;
}


//*************************************************************
// set_thread_pool_op - Sets a thread pool op
//*************************************************************

int set_thread_pool_op(gop_thread_pool_op_t *op, gop_thread_pool_context_t *tpc, char *que, gop_op_status_t (*fn)(void *arg, int id), void *arg, void (*my_op_free)(void *arg), int workload)
{
    op->fn = fn;
    op->arg = arg;
    op->my_op_free = my_op_free;

    return(0);
}

//*************************************************************
// gop_tp_op_new - Allocates space for a new op
//*************************************************************

gop_op_generic_t *gop_tp_op_new(gop_thread_pool_context_t *tpc, char *que, gop_op_status_t (*fn)(void *arg, int id), void *arg, void (*my_op_free)(void *arg), int workload)
{
    gop_thread_pool_op_t *op;

    //** Make the struct and clear it
    tbx_type_malloc(op, gop_thread_pool_op_t, 1);

    tbx_atomic_inc(tpc->n_ops);

    init_tp_op(tpc, op);

    set_thread_pool_op(op, tpc, que, fn, arg, my_op_free, workload);

    return(tp_get_gop(op));
}


