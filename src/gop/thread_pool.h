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
// Generic thread pool implementation designed to woth with opque
//*************************************************************

#include "gop/gop_visibility.h"
#include "opque.h"
#include "host_portal.h"
#include "atomic_counter.h"
#include <apr_thread_pool.h>

#ifndef __THREAD_POOL_H_
#define __THREAD_POOL_H_

#ifdef __cplusplus
extern "C" {
#endif

#define TP_E_ERROR              OP_STATE_FAILURE
#define TP_E_OK                 OP_STATE_SUCCESS
#define TP_E_NOP               -1
#define TP_E_IGNORE            -2

typedef struct {
    char *name;
    portal_context_t *pc;
    apr_thread_pool_t *tp;
    tbx_stack_t **reserve_stack;
    int *overflow_running_depth;
    tbx_atomic_unit32_t n_overflow;
    tbx_atomic_unit32_t n_ops;
    tbx_atomic_unit32_t n_completed;
    tbx_atomic_unit32_t n_started;
    tbx_atomic_unit32_t n_submitted;
    tbx_atomic_unit32_t n_direct;
    tbx_atomic_unit32_t n_running;
    int min_idle;
    int min_threads;
    int max_threads;
    int recursion_depth;
    int max_concurrency;
} thread_pool_context_t;

typedef struct {
    thread_pool_context_t *tpc;
    op_generic_t gop;
    op_data_t dop;
    op_status_t (*fn)(void *priv, int id);
    void (*my_op_free)(void *arg);
    void *arg;
    int depth;
    int parent_tid;
    int via_submit;
    int overflow_slot;
} thread_pool_op_t;

#define tp_get_gop(top) &((top)->gop)
#define gop_get_tp(gop) (gop)->op->priv
//#define tp_gop_id(top) ((thread_pool_op_t *)((gop)->op->priv))->id

int thread_pool_direct(thread_pool_context_t *tpc, apr_thread_start_t fn, void *arg);

int set_thread_pool_op(thread_pool_op_t *op, thread_pool_context_t *tpc, char *que, op_status_t (*fn)(void *arg, int id), void *arg, void (*my_op_free)(void *arg), int workload);
GOP_API op_generic_t *new_thread_pool_op(thread_pool_context_t *tpc, char *que, op_status_t (*fn)(void *arg, int id), void *arg, void (*my_op_free)(void *arg), int workload);

GOP_API thread_pool_context_t *thread_pool_create_context(char *tp_name, int min_threads, int max_threads, int max_recursion);
GOP_API void thread_pool_destroy_context(thread_pool_context_t *tpc);

void  *thread_pool_exec_fn(apr_thread_t *th, void *data);

#ifdef __cplusplus
}
#endif


#endif

