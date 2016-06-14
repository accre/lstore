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
// Simple resource managment implementation
//***********************************************************************

#include <tbx/list.h>
#include "data_service_abstract.h"
#include <gop/opque.h>
#include "service_manager.h"

#ifndef _RS_SIMPLE_PRIV_H_
#define _RS_SIMPLE_PRIV_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef struct rss_rid_entry_t rss_rid_entry_t;
struct rss_rid_entry_t {
    char *rid_key;
    char *ds_key;
    tbx_list_t *attr;
    int  status;
    int  slot;
    ex_off_t space_total;
    ex_off_t space_used;
    ex_off_t space_free;
};

typedef struct rss_check_entry_t rss_check_entry_t;
struct rss_check_entry_t {
    char *ds_key;
    char *rid_key;
    data_inquire_t *space;
    rss_rid_entry_t *re;
};

typedef struct rs_simple_priv_t rs_simple_priv_t;
struct rs_simple_priv_t {
    tbx_list_t *rid_table;
    rss_rid_entry_t **random_array;
    data_service_fn_t *ds;
    data_attr_t *da;
    apr_thread_mutex_t *lock;
    apr_thread_mutex_t *update_lock;
    apr_thread_cond_t *cond;
    apr_thread_t *check_thread;
    apr_pool_t *mpool;
    apr_hash_t *mapping_updates;
    apr_hash_t *rid_mapping;
    time_t modify_time;
    time_t current_check;
    char *fname;
    uint64_t min_free;
    int n_rids;
    int shutdown;
    int dynamic_mapping;
    int unique_rids;
    int check_interval;
    int check_timeout;
    int last_config_size;
};

#ifdef __cplusplus
}
#endif

#endif

