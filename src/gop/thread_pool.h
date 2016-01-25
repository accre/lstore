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

//*************************************************************
// Generic thread pool implementation designed to woth with opque
//*************************************************************

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
    Stack_t **reserve_stack;
    int *overflow_running_depth;
    atomic_int_t n_overflow;
    atomic_int_t n_ops;
    atomic_int_t n_completed;
    atomic_int_t n_started;
    atomic_int_t n_submitted;
    atomic_int_t n_direct;
    atomic_int_t n_running;
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
    int overflow_slot;
} thread_pool_op_t;

#define tp_get_gop(top) &((top)->gop)
#define gop_get_tp(gop) (gop)->op->priv
//#define tp_gop_id(top) ((thread_pool_op_t *)((gop)->op->priv))->id

int thread_pool_direct(thread_pool_context_t *tpc, apr_thread_start_t fn, void *arg);

int set_thread_pool_op(thread_pool_op_t *op, thread_pool_context_t *tpc, char *que, op_status_t (*fn)(void *arg, int id), void *arg, void (*my_op_free)(void *arg), int workload);
op_generic_t *new_thread_pool_op(thread_pool_context_t *tpc, char *que, op_status_t (*fn)(void *arg, int id), void *arg, void (*my_op_free)(void *arg), int workload);

thread_pool_context_t *thread_pool_create_context(char *tp_name, int min_threads, int max_threads);
void thread_pool_destroy_context(thread_pool_context_t *tpc);

void  *thread_pool_exec_fn(apr_thread_t *th, void *data);

#ifdef __cplusplus
}
#endif


#endif

