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
// Remote resource managment implementation
//***********************************************************************

#ifndef _RS_REMOTE_H_
#define _RS_REMOTE_H_

#include <gop/mq.h>
#include <gop/opque.h>
#include <tbx/list.h>

#include "ds.h"
#include "rs.h"
#include "service_manager.h"

#ifdef __cplusplus
extern "C" {
#endif

#define RS_TYPE_REMOTE_CLIENT "remote_client"
#define RS_TYPE_REMOTE_SERVER "remote_server"

lio_resource_service_fn_t *rs_remote_client_create(void *arg, tbx_inip_file_t *fd, char *section);
lio_resource_service_fn_t *rs_remote_server_create(void *arg, tbx_inip_file_t *fd, char *section);

#define RSR_GET_RID_CONFIG_KEY      "rs_get_rid"
#define RSR_GET_RID_CONFIG_SIZE     10
#define RSR_GET_UPDATE_CONFIG_KEY   "rs_get_update"
#define RSR_GET_UPDATE_CONFIG_SIZE  13
#define RSR_ABORT_KEY               "abort"
#define RSR_ABORT_SIZE              5


struct lio_rs_remote_client_priv_t {
    char *rrs_test_section;
    char *section;
    char *local_child_section;
    lio_data_service_fn_t *ds;
    data_attr_t *da;
    apr_thread_mutex_t *lock;
    apr_thread_cond_t *cond;
    apr_thread_t *check_thread;
    apr_pool_t *mpool;
    apr_hash_t *mapping_updates;
    time_t modify_time;
    time_t current_check;
    lio_rs_mapping_notify_t version;
    uint64_t update_id;
    int shutdown;
    int dynamic_mapping;
    int unique_rids;
    int check_interval;
    int delete_target;
    lio_resource_service_fn_t *rrs_test;  //** This is only used for testing by lauching the "remote" RS internally
    lio_resource_service_fn_t *rs_child;
    gop_mq_context_t *mqc;            //** Portal for connecting to he remote RS server
    char *host_remote_rs;               //** Address of the Remote RS server
    char *child_target_file;      //** File child is looking at for changes
};


struct lio_rs_remote_server_priv_t {
    int shutdown;
    char *section;
    char *rs_local_section;
    apr_thread_mutex_t *lock;
    apr_thread_cond_t *cond;
    apr_thread_t *monitor_thread;
    apr_pool_t *mpool;
    apr_hash_t *mapping_updates;
    apr_time_t wakeup_time;
    lio_resource_service_fn_t *rs_child;
    lio_rs_mapping_notify_t my_map_version;
    lio_rs_mapping_notify_t notify_map_version;
    gop_mq_context_t *mqc;            //** Portal to use.  The server creates this itself
    gop_mq_portal_t *server_portal;
    char *hostname;
    tbx_stack_t *pending;
};

#ifdef __cplusplus
}
#endif

#endif
