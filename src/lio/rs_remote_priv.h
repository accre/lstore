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

//***********************************************************************
// Remote client resource managment implementation
//***********************************************************************

#include "list.h"
#include "data_service_abstract.h"
#include "opque.h"
#include "service_manager.h"
#include "mq_portal.h"

#ifndef _RS_REMOTE_PRIV_H_
#define _RS_REMOTE_PRIV_H_

#ifdef __cplusplus
extern "C" {
#endif

#define RSR_GET_RID_CONFIG_KEY      "rs_get_rid"
#define RSR_GET_RID_CONFIG_SIZE     10
#define RSR_GET_UPDATE_CONFIG_KEY   "rs_get_update"
#define RSR_GET_UPDATE_CONFIG_SIZE  13
#define RSR_ABORT_KEY               "abort"
#define RSR_ABORT_SIZE              5


typedef struct {
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
} rs_remote_client_priv_t;


typedef struct {
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
    Stack_t *pending;
} rs_remote_server_priv_t;


#ifdef __cplusplus
}
#endif

#endif

