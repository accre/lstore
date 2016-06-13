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


//*************************************************************
// opque.h - Header defining I/O structs and operations for
//     collections of oplists
//*************************************************************

#ifndef __OPQUE_H_
#define __OPQUE_H_

#include "gop/gop_visibility.h"
#include <apr_thread_mutex.h>
#include <apr_thread_cond.h>
#include <apr_hash.h>
#include <tbx/atomic_counter.h>
#include <tbx/network.h>
#include <tbx/stack.h>
#include "callback.h"
#include <tbx/pigeon_coop.h>
#include "gop.h"
#include "types.h"

#ifdef __cplusplus
extern "C" {
#endif

// Types
struct que_data_t {
    tbx_stack_t *list;         //** List of tasks
    tbx_stack_t *finished;     //** lists that have completed and not yet processed
    tbx_stack_t *failed;       //** All lists that fail are also placed here
    int nleft;             //** Number of lists left to be processed
    int nsubmitted;        //** Nunmber of submitted tasks (doesn't count sub q's)
    int finished_submission; //** No more tasks will be submitted so it's safe to free the data when finished
    callback_t failure_cb;   //** Only used if a task fails
    opque_t *opque;
};

struct opque_t {
    op_generic_t op;
    que_data_t   qd;
};

// Functions
void opque_set_failure_mode(opque_t *q, int value);
int opque_get_failure_mode(opque_t *q);
op_status_t opque_completion_status(opque_t *q);
void opque_set_arg(opque_t *q, void *arg);
void *opque_get_arg(opque_t *q);
GOP_API opque_t *gop_opque_new();
void init_opque(opque_t *que);
GOP_API void gop_init_opque_system();
GOP_API void gop_shutdown();
GOP_API void gop_opque_free(opque_t *que, int mode);
GOP_API int gop_opque_add(opque_t *que, op_generic_t *gop);
int internal_gop_opque_add(opque_t *que, op_generic_t *gop, int dolock);
GOP_API void gop_default_sort_ops(void *arg, opque_t *que);

// Preprocessor Macros
#define lock_opque(q)   log_printf(15, "lock_opque: qid=%d\n", (q)->opque->op.base.id); apr_thread_mutex_lock((q)->opque->op.base.ctl->lock)
#define unlock_opque(q) log_printf(15, "unlock_opque: qid=%d\n", (q)->opque->op.base.id); apr_thread_mutex_unlock((q)->opque->op.base.ctl->lock)
#define opque_get_gop(q) &((q)->op)
#define opque_failure_gop_cb_set(q, fn, priv) gop_cb_set(&(q->failure_cb), fn, priv)
#define opque_callback_append(q, cb) gop_callback_append(opque_get_gop(q), (cb))
#define opque_get_next_finished(q) gop_get_next_finished(opque_get_gop(q))
#define opque_get_next_failed(q) gop_get_next_failed(opque_get_gop(q))
#define opque_tasks_failed(q) gop_tasks_failed(opque_get_gop(q))
#define opque_tasks_finished(q) gop_tasks_finished(opque_get_gop(q))
#define opque_tasks_left(q) gop_tasks_left(opque_get_gop(q))
#define opque_task_count(q) q->qd.nsubmitted
#define opque_waitall(q) gop_waitall(opque_get_gop(q))
#define opque_waitany(q) gop_waitany(opque_get_gop(q))
#define opque_start_execution(q) gop_start_execution(opque_get_gop(q))
#define opque_finished_submission(q) gop_finished_submission(opque_get_gop(q))

#define opque_set_status(q, val) gop_set_status(opque_get_gop(q), val)
#define opque_get_status(q) gop_get_status(opque_get_gop(q))
#define opque_completed_successfully(q) gop_completed_successfully(q)


#ifdef __cplusplus
}
#endif


#endif

