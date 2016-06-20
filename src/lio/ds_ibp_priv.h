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
// IBP based data service
//***********************************************************************

#include <apr_hash.h>
#include <apr_thread_cond.h>
#include <apr_thread_proc.h>
#include <ibp/ibp.h>

#include "ds_ibp.h"

#ifndef _DS_IBP_PRIV_H_
#define _DS_IBP_PRIV_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef struct ds_ibp_alloc_op_t ds_ibp_alloc_op_t;
struct ds_ibp_alloc_op_t {
    ibp_depot_t depot;
};

typedef struct ds_ibp_truncate_op_t ds_ibp_truncate_op_t;
struct ds_ibp_truncate_op_t {
    int state;
    int timeout;
    ibp_off_t new_size;
    ibp_capstatus_t probe;
    callback_t *cb;
    data_service_fn_t *dsf;
    ds_ibp_attr_t *attr;
    opque_t *q;
    ibp_cap_t *mcap;
};

typedef struct ds_ibp_op_t ds_ibp_op_t;
struct ds_ibp_op_t {
    void *sf_ptr;
    ds_ibp_attr_t *attr;
    op_generic_t *gop;
    void (*free)(op_generic_t *d, int mode);
    void *free_ptr;
    union {
        ds_ibp_alloc_op_t alloc;
        ds_ibp_truncate_op_t truncate;
    } ops;
};

typedef struct ds_ibp_priv_t ds_ibp_priv_t;
struct ds_ibp_priv_t {
    ds_ibp_attr_t attr_default;
    ibp_context_t *ic;

    //** These are all for the warmer
    apr_pool_t *pool;
    apr_hash_t *warm_table;
    apr_thread_mutex_t *lock;
    apr_thread_cond_t *cond;
    apr_thread_t *thread;
    int warm_interval;
    int warm_duration;
    int warm_stop;
};

#ifdef __cplusplus
}
#endif

#endif
