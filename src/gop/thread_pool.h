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

#include <tbx/atomic_counter.h>
#include <tbx/thread_pool.h>

#include "gop.h"
#include "gop/visibility.h"
#include "gop/tp.h"

#ifndef __THREAD_POOL_H_
#define __THREAD_POOL_H_

#ifdef __cplusplus
extern "C" {
#endif

//#define tp_gop_id(top) ((gop_thread_pool_op_t *)((gop)->op->priv))->id

int thread_pool_direct(gop_thread_pool_context_t *tpc, apr_thread_start_t fn, void *arg);

int set_thread_pool_op(gop_thread_pool_op_t *op, gop_thread_pool_context_t *tpc, char *que, gop_op_status_t (*fn)(void *arg, int id), void *arg, void (*my_op_free)(void *arg), int workload);


void thread_pool_exec_fn(void *arg, gop_op_generic_t *op);

#ifdef __cplusplus
}
#endif


#endif

