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

#define _log_module_index 123

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "assert_result.h"
#include "thread_pool.h"
#include "fmttypes.h"
#include "network.h"
#include "log.h"
#include "type_malloc.h"
#include "append_printf.h"
#include "atomic_counter.h"

#define TP_MAX_DEPTH 20

extern int _tp_context_count;
extern apr_thread_mutex_t *_tp_lock;
extern apr_pool_t *_tp_pool;
extern int _tp_id;
extern int _tp_stats;

static int _tp_concurrent_max;
static int _tp_depth_concurrent_max[TP_MAX_DEPTH];
static atomic_int_t _tp_depth_concurrent[TP_MAX_DEPTH];
static atomic_int_t _tp_concurrent;
static atomic_int_t _tp_depth_total[TP_MAX_DEPTH];

apr_threadkey_t *thread_depth_key;
apr_threadkey_t *thread_depth_table_key;
apr_threadkey_t *thread_concurrent_key;

void _tp_op_free(op_generic_t *gop, int mode);

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
        total = atomic_get(_tp_depth_total[i]);
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
        atomic_set(_tp_depth_total[i], 0);
        atomic_set(_tp_depth_concurrent[i], 0);
    }
}

//*************************************************************************
// _thread_concurrent_ptr - Returns the pointer to the unique thread concurrency
//*************************************************************************

int *_thread_concurrent_ptr()
{
    int *ptr = NULL;

    apr_threadkey_private_get((void *)&ptr, thread_concurrent_key);
    if (ptr == NULL ) {
        type_malloc(ptr, int, 1);
        apr_threadkey_private_set(ptr, thread_concurrent_key);
        apr_thread_mutex_lock(_tp_lock);
        *ptr = _tp_concurrent_max;
        apr_thread_mutex_unlock(_tp_lock);
    }

    return(ptr);
}

//*************************************************************************
// _thread_depth_ptr - Returns the pointer to the unique thread depth
//*************************************************************************

int *_thread_depth_ptr()
{
    int *ptr = NULL;

    apr_threadkey_private_get((void *)&ptr, thread_depth_key);
    if (ptr == NULL ) {
        type_malloc(ptr, int, 1);
        apr_threadkey_private_set(ptr, thread_depth_key);
        *ptr = 0;
    }

    return(ptr);
}

//*************************************************************************
// _thread_depth_table_ptr - Returns the local pointer to the unique thread depth table
//*************************************************************************

int *_thread_depth_table_ptr()
{
    int *ptr = NULL;

    apr_threadkey_private_get((void *)&ptr, thread_depth_table_key);
    if (ptr == NULL ) {
        type_malloc(ptr, int, TP_MAX_DEPTH);
        apr_thread_mutex_lock(_tp_lock);
        memcpy(ptr, _tp_depth_concurrent_max, sizeof(_tp_depth_concurrent_max));  //** Set to the current global
        apr_thread_mutex_unlock(_tp_lock);
        apr_threadkey_private_set(ptr, thread_depth_table_key);
    }

    return(ptr);
}


//*************************************************************

op_status_t tp_command(op_generic_t *gop, NetStream_t *ns)
{
    return(op_success_status);
}

//*************************************************************

void  *thread_pool_exec_fn(apr_thread_t *th, void *arg)
{
    op_generic_t *gop = (op_generic_t *)arg;
    thread_pool_op_t *op = gop_get_tp(gop);
    op_status_t status;
    int tid;
    int my_depth = -1;
    int concurrent;

    tid = atomic_thread_id;
    if (_tp_stats == 1) {
        //** Check if we set a new high for max concurrency
        concurrent = atomic_inc(_tp_concurrent) + 1;
        if (concurrent > *(_thread_concurrent_ptr())) {
            apr_thread_mutex_lock(_tp_lock);  //** We passed the local check so now check the global table
            if (concurrent > _tp_concurrent_max) {
                _tp_concurrent_max = concurrent;
                *(_thread_concurrent_ptr()) = concurrent;
            }
            apr_thread_mutex_unlock(_tp_lock);
        }

        my_depth = *_thread_depth_ptr();
        (*_thread_depth_ptr()) = op->depth;

        atomic_inc(_tp_depth_total[my_depth]);

        //** Check if we may have set a new concurrency limit for the depth
        concurrent = atomic_inc(_tp_depth_concurrent[my_depth]) + 1;
        if (concurrent > (_thread_depth_table_ptr())[my_depth]) {
            apr_thread_mutex_lock(_tp_lock);  //** We passed the local check so now check the global table
            concurrent = atomic_get(_tp_depth_concurrent[my_depth]);
            if (concurrent > _tp_depth_concurrent_max[my_depth]) {
                _tp_depth_concurrent_max[my_depth] = concurrent;
                (_thread_depth_table_ptr())[my_depth] = _tp_depth_concurrent_max[my_depth];  //** Update to global
            }
            apr_thread_mutex_unlock(_tp_lock);
        }
    }

    log_printf(4, "tp_recv: Start!!! gid=%d tid=%d depth=%d\n", gop_id(gop), tid, my_depth);
    atomic_inc(op->tpc->n_started);

    status = op->fn(op->arg, gop_id(gop));
    if (_tp_stats == 1) {
        (*_thread_depth_ptr()) = my_depth;
        atomic_dec(_tp_depth_concurrent[my_depth]);
        atomic_dec(_tp_concurrent);
    }

    log_printf(4, "tp_recv: end!!! gid=%d tid=%d status=%d\n", gop_id(gop), tid, status.op_status);
//log_printf(15, "gid=%d user_priv=%p\n", gop_id(gop), gop_get_private(gop));

    atomic_inc(op->tpc->n_completed);
    gop_mark_completed(gop, status);

    return(NULL);
}

//*************************************************************
// init_tp_op - Does the 1-time initialization for the op
//*************************************************************

void init_tp_op(thread_pool_context_t *tpc, thread_pool_op_t *op)
{
    op_generic_t *gop;

    //** Clear it
    type_memclear(op, thread_pool_op_t, 1);

    op->depth = (_tp_stats == 0) ? -1 : *_thread_depth_ptr() + 1; //** Store my recursion depth

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

int set_thread_pool_op(thread_pool_op_t *op, thread_pool_context_t *tpc, char *que, op_status_t (*fn)(void *arg, int id), void *arg, void (*my_op_free)(void *arg), int workload)
{
    op->fn = fn;
    op->arg = arg;
    op->my_op_free = my_op_free;

    return(0);
}

//*************************************************************
// new_thread_pool_op - Allocates space for a new op
//*************************************************************

op_generic_t *new_thread_pool_op(thread_pool_context_t *tpc, char *que, op_status_t (*fn)(void *arg, int id), void *arg, void (*my_op_free)(void *arg), int workload)
{
    thread_pool_op_t *op;

    //** Make the struct and clear it
    type_malloc(op, thread_pool_op_t, 1);

    atomic_inc(tpc->n_ops);

    init_tp_op(tpc, op);

    set_thread_pool_op(op, tpc, que, fn, arg, my_op_free, workload);

    return(tp_get_gop(op));
}


