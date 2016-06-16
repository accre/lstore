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
// Remote client resource managment implementation
//***********************************************************************

#ifndef _RS_REMOTE_PRIV_H_
#define _RS_REMOTE_PRIV_H_

#include <gop/mq.h>
#include <gop/opque.h>
#include <tbx/list.h>

#include "data_service_abstract.h"
#include "service_manager.h"

#ifdef __cplusplus
extern "C" {
#endif

#define RSR_GET_RID_CONFIG_KEY      "rs_get_rid"
#define RSR_GET_RID_CONFIG_SIZE     10
#define RSR_GET_UPDATE_CONFIG_KEY   "rs_get_update"
#define RSR_GET_UPDATE_CONFIG_SIZE  13
#define RSR_ABORT_KEY               "abort"
#define RSR_ABORT_SIZE              5


typedef struct rs_remote_client_priv_t rs_remote_client_priv_t;
struct rs_remote_client_priv_t {
    data_service_fn_t *ds;
    data_attr_t *da;
    apr_thread_mutex_t *lock;
    apr_thread_cond_t *cond;
    apr_thread_t *check_thread;
    apr_pool_t *mpool;
    apr_hash_t *mapping_updates;
    time_t modify_time;
    time_t current_check;
    rs_mapping_notify_t version;
    uint64_t update_id;
    int shutdown;
    int dynamic_mapping;
    int unique_rids;
    int check_interval;
    resource_service_fn_t *rrs_test;  //** This is only used for testing by lauching the "remote" RS internally
    resource_service_fn_t *rs_child;
    mq_context_t *mqc;            //** Portal for connecting to he remote RS server
    char *host_remote_rs;               //** Address of the Remote RS server
    char *child_target_file;      //** File child is looking at for changes
};


typedef struct rs_remote_server_priv_t rs_remote_server_priv_t;
struct rs_remote_server_priv_t {
    int shutdown;
    apr_thread_mutex_t *lock;
    apr_thread_cond_t *cond;
    apr_thread_t *monitor_thread;
    apr_pool_t *mpool;
    apr_hash_t *mapping_updates;
    apr_time_t wakeup_time;
    resource_service_fn_t *rs_child;
    rs_mapping_notify_t my_map_version;
    rs_mapping_notify_t notify_map_version;
    mq_context_t *mqc;            //** Portal to use.  The server creates this itself
    mq_portal_t *server_portal;
    char *hostname;
    tbx_stack_t *pending;
};

#ifdef __cplusplus
}
#endif

#endif

