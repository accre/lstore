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

#define _log_module_index 122

#include <apr_env.h>
#include <apr_errno.h>
#include <apr_pools.h>
#include <apr_thread_mutex.h>
#include <apr_thread_proc.h>
#include <apr_time.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/assert_result.h>
#include <tbx/atomic_counter.h>
#include <tbx/log.h>
#include <tbx/network.h>
#include <tbx/siginfo.h>
#include <tbx/stack.h>
#include <tbx/thread_pool.h>
#include <tbx/type_malloc.h>

#include "gop/gop.h"
#include "gop/hp.h"
#include "gop/opque.h"
#include "gop/portal.h"
#include "gop/tp.h"
#include "gop/types.h"
#include "thread_pool.h"

void *_tp_dup_connect_context(void *connect_context);
void _tp_destroy_connect_context(void *connect_context);
int _tp_connect(tbx_ns_t *ns, void *connect_context, char *host, int port, tbx_ns_timeout_t timeout);
void _tp_close_connection(tbx_ns_t *ns);
gop_op_generic_t *_tpc_overflow_next(gop_thread_pool_context_t *tpc);

void _tp_op_free(gop_op_generic_t *op, int mode);
void _tp_submit_op(void *arg, gop_op_generic_t *op);

static gop_portal_fn_t _tp_base_portal = {
    .dup_connect_context = _tp_dup_connect_context,
    .destroy_connect_context = _tp_destroy_connect_context,
    .connect = _tp_connect,
    .close_connection = _tp_close_connection,
    .sort_tasks = gop_default_sort_ops,
    .submit = _tp_submit_op,
    .sync_exec = thread_pool_exec_fn
};

void thread_pool_stats_make();
void thread_pool_stats_print();

tbx_atomic_unit32_t _tp_context_count = 0;
apr_thread_mutex_t *_tp_lock = NULL;
apr_pool_t *_tp_pool = NULL;
int _tp_stats = 0;

extern apr_threadkey_t *thread_local_stats_key;
extern apr_threadkey_t *thread_local_depth_key;


//************************************************************************
//  tp_siginfo_handler - Prints info about the threadpool
//************************************************************************

void tp_siginfo_handler(void *arg, FILE *fd)
{
    gop_thread_pool_context_t *tpc = (gop_thread_pool_context_t *)arg;
//    char ppbuf1[100], ppbuf2[100];

    apr_thread_mutex_lock(_tp_lock);
    fprintf(fd, "Thread pool info (%s)-------------------------------------------\n", tpc->name);
    fprintf(fd, "    Ops -- Submitted: %d  Completed: %d  Running: %d\n", tbx_atomic_get(tpc->n_submitted), tbx_atomic_get(tpc->n_completed), tbx_atomic_get(tpc->n_running));
    fprintf(fd, "    Threads -- Current: %lu  Busy: %lu  Idle: %lu  Max Concurrent: %lu   Pool Min: %d  Pool Max: %d  Max Recursion: %d\n",
        tbx_thread_pool_threads_count(tpc->tp), tbx_thread_pool_busy_count(tpc->tp), tbx_thread_pool_idle_count(tpc->tp),
        tbx_thread_pool_threads_high_count(tpc->tp), tpc->min_threads, tpc->max_threads, tpc->recursion_depth);

    if (_tp_stats > 0) {
        fprintf(fd, "\n");
        thread_pool_stats_print(fd);
    }
    fprintf(fd, "\n");
    apr_thread_mutex_unlock(_tp_lock);

    return;
}


//***************************************************************************

void _thread_pool_destructor(void *ptr)
{
    free(ptr);
}

//**********************************************************
// thread_pool_stats_init - Initializes the thread pool stats routines
//     Should only be called once.
//**********************************************************

void thread_pool_stats_init()
{
    int i;
    char *eval;

    //** Check if we are enabling stat collection
    eval = NULL;
    apr_env_get(&eval, "GOP_TP_STATS", _tp_pool);
    if (eval != NULL) {
        i = atol(eval);
        if (i > 0) {
            _tp_stats = i;

            if (thread_local_stats_key == NULL) {
                apr_threadkey_private_create(&thread_local_stats_key,_thread_pool_destructor, _tp_pool);
                thread_pool_stats_make();
            }
        }
    }
}

//*************************************************************

gop_portal_fn_t default_tp_imp()
{
    return(_tp_base_portal);
}


//*************************************************************

void _tp_op_free(gop_op_generic_t *gop, int mode)
{
    gop_thread_pool_op_t *top = gop_get_tp(gop);
    int id = gop_id(gop);

    log_printf(15, "_tp_op_free: mode=%d gid=%d gop=%p\n", mode, gop_id(gop), gop);
    tbx_log_flush();

    if (top->my_op_free != NULL) top->my_op_free(top->arg);

    gop_generic_free(gop, OP_FINALIZE);  //** I free the actual op

    if (top->dop.cmd.hostport) free(top->dop.cmd.hostport);

    if (mode == OP_DESTROY) free(gop->free_ptr);
    log_printf(15, "_tp_op_free: gid=%d END\n", id);
    tbx_log_flush();

}

//*************************************************************

void _tp_submit_op(void *arg, gop_op_generic_t *gop)
{
    gop_thread_pool_op_t *op = gop_get_tp(gop);
    apr_status_t aerr;
    int running;

    log_printf(15, "_tp_submit_op: gid=%d\n", gop_id(gop));

    tbx_atomic_inc(op->tpc->n_submitted);
    op->via_submit = 1;
    running = tbx_atomic_inc(op->tpc->n_running) + 1;

    if (running > op->tpc->max_concurrency) {
        apr_thread_mutex_lock(_tp_lock);
        tbx_atomic_inc(op->tpc->n_overflow);
        if (op->depth >= op->tpc->recursion_depth) {  //** Check if we hit the max recursion
            log_printf(0, "GOP has a recursion depth >= max specified in the TP!!!! gop depth=%d  TPC max=%d\n", op->depth, op->tpc->recursion_depth);
            tbx_stack_push(op->tpc->reserve_stack[op->tpc->recursion_depth-1], gop);  //** Need to do the push and overflow check
        } else {
            tbx_stack_push(op->tpc->reserve_stack[op->depth], gop);  //** Need to do the push and overflow check
        }
        gop = _tpc_overflow_next(op->tpc);             //** along with the submit or rollback atomically

        if (gop) {
            op = gop_get_tp(gop);
            aerr = tbx_thread_pool_push(op->tpc->tp,(void *(*)(apr_thread_t *, void *))thread_pool_exec_fn, gop, TBX_THREAD_TASK_PRIORITY_NORMAL, NULL);
        } else {
            tbx_atomic_dec(op->tpc->n_running);  //** We didn't actually submit anything
            if (op->overflow_slot != -1) {   //** Check if we need to undo our overflow slot
                op->tpc->overflow_running_depth[op->overflow_slot] = -1;
            }

            aerr = APR_SUCCESS;
        }
        apr_thread_mutex_unlock(_tp_lock);
    } else {
        aerr = tbx_thread_pool_push(op->tpc->tp, (void *(*)(apr_thread_t *, void *))thread_pool_exec_fn, gop, TBX_THREAD_TASK_PRIORITY_NORMAL, NULL);
    }

    if (aerr != APR_SUCCESS) {
        log_printf(0, "ERROR submiting task!  aerr=%d gid=%d\n", aerr, gop_id(gop));
    }
}

//********************************************************************

void *_tp_dup_connect_context(void *connect_context)
{
    return(NULL);
}

//********************************************************************

void _tp_destroy_connect_context(void *connect_context)
{
    return;
}

//**********************************************************

int _tp_connect(tbx_ns_t *ns, void *connect_context, char *host, int port, tbx_ns_timeout_t timeout)
{
    tbx_ns_setid(ns, tbx_ns_generate_id());
    return(0);
}


//**********************************************************

void _tp_close_connection(tbx_ns_t *ns)
{
    return;
}

//*************************************************************
// thread_pool_direct - Bypasses the _tp_exec GOP wrapper
//     and directly submits the task to the APR thread pool
//*************************************************************

int thread_pool_direct(gop_thread_pool_context_t *tpc, apr_thread_start_t fn, void *arg)
{
    int err = tbx_thread_pool_push(tpc->tp, fn, arg, TBX_THREAD_TASK_PRIORITY_NORMAL, NULL);

    tbx_atomic_inc(tpc->n_direct);

    log_printf(10, "tpd=%d\n", tbx_atomic_get(tpc->n_direct));
    if (err != APR_SUCCESS) {
        log_printf(0, "ERROR submiting task!  err=%d\n", err);
    }

    return((err == APR_SUCCESS) ? 0 : 1);
}

//**********************************************************
// default_thread_pool_config - Sets the default thread pool config options
//**********************************************************

void default_thread_pool_config(gop_thread_pool_context_t *tpc)
{
    tpc->min_idle = 1; //** default to close after 1 sec
    tpc->min_threads = 1;
    tpc->max_threads = 4;

    tpc->name = NULL;
}


//**********************************************************
//  gop_tp_context_create - Creates a TP context
//**********************************************************

gop_thread_pool_context_t *gop_tp_context_create(char *tp_name, int min_threads, int max_threads, int max_recursion_depth)
{
//  char buffer[1024];
    gop_thread_pool_context_t *tpc;
    apr_interval_time_t dt;
    int i;

    log_printf(15, "count=%d\n", _tp_context_count);

    tbx_type_malloc_clear(tpc, gop_thread_pool_context_t, 1);

    if (tbx_atomic_inc(_tp_context_count) == 0) {
        apr_pool_create(&_tp_pool, NULL);
        apr_thread_mutex_create(&_tp_lock, APR_THREAD_MUTEX_DEFAULT, _tp_pool);
        thread_pool_stats_init();
        apr_threadkey_private_create(&thread_local_depth_key,_thread_pool_destructor, _tp_pool);
    }

    tpc->pc = gop_hp_context_create(&_tp_base_portal);  //** Really just used for the submit

    default_thread_pool_config(tpc);
    if (min_threads > 0) tpc->min_threads = min_threads;
    if (max_threads > 0) tpc->max_threads = max_threads + 1;  //** Add one for the recursion depth starting offset being 1
    tpc->recursion_depth = max_recursion_depth + 1;  //** The min recusion normally starts at 1 so just slap an extra level and we don't care about 0|1 starting location
    tpc->max_concurrency = tpc->max_threads - tpc->recursion_depth;
    if (tpc->max_concurrency <= 0) {
        tpc->max_threads += 5 - tpc->max_concurrency;  //** MAke sure we have at least 5 threads for work
        tpc->max_concurrency = tpc->max_threads - tpc->recursion_depth;
        log_printf(0, "Specified max threads and recursion depth don't work. Adjusting max_threads=%d\n", tpc->max_threads);
    }

    dt = tpc->min_idle * 1000000;
    assert_result(tbx_thread_pool_create(&(tpc->tp), tpc->min_threads, tpc->max_threads, _tp_pool), APR_SUCCESS);
    tbx_thread_pool_idle_wait_set(tpc->tp, dt);
    tbx_thread_pool_threshold_set(tpc->tp, 0);

    tpc->name = (tp_name == NULL) ? NULL : strdup(tp_name);
    tbx_atomic_set(tpc->n_ops, 0);
    tbx_atomic_set(tpc->n_completed, 0);
    tbx_atomic_set(tpc->n_started, 0);
    tbx_atomic_set(tpc->n_submitted, 0);
    tbx_atomic_set(tpc->n_running, 0);

    tbx_type_malloc(tpc->overflow_running_depth, int, tpc->recursion_depth);
    tbx_type_malloc(tpc->reserve_stack, tbx_stack_t *, tpc->recursion_depth);
    for (i=0; i<tpc->recursion_depth; i++) {
        tpc->overflow_running_depth[i] = -1;
        tpc->reserve_stack[i] = tbx_stack_new();
    }

    tbx_siginfo_handler_add(tp_siginfo_handler, tpc);

    return(tpc);
}


//**********************************************************
//  gop_tp_context_destroy - Shuts down the Thread pool system
//**********************************************************

void gop_tp_context_destroy(gop_thread_pool_context_t *tpc)
{
    int i;
    log_printf(15, "gop_tp_context_destroy: Shutting down! count=%d\n", _tp_context_count);

    log_printf(15, "tpc->name=%s  high=%zu idle=%zu\n", tpc->name, tbx_thread_pool_threads_high_count(tpc->tp),  tbx_thread_pool_threads_idle_timeout_count(tpc->tp));

    tbx_siginfo_handler_remove(tp_siginfo_handler, tpc);
    gop_hp_context_destroy(tpc->pc);

    tbx_thread_pool_destroy(tpc->tp);

    if (tbx_atomic_dec(_tp_context_count) == 0) {
        if (_tp_stats > 0) thread_pool_stats_print(stderr);
        apr_thread_mutex_destroy(_tp_lock);
        apr_pool_destroy(_tp_pool);
    }

    if (tpc->name != NULL) free(tpc->name);

    for (i=0; i<tpc->recursion_depth; i++) {
        tbx_stack_free(tpc->reserve_stack[i], 0);
    }
    free(tpc->reserve_stack);
    free(tpc->overflow_running_depth);

    free(tpc);
}

