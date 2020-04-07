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
// OS Remote Server/Client header file
//***********************************************************************

#ifndef _OS_REMOTE_H_
#define _OS_REMOTE_H_

#include <gop/mq_ongoing.h>
#include <gop/mq.h>

#include "authn.h"
#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

#define OS_TYPE_REMOTE_SERVER "os_remote_server"
#define OS_TYPE_REMOTE_CLIENT "os_remote_client"

lio_object_service_fn_t *object_service_remote_server_create(lio_service_manager_t *ess, tbx_inip_file_t *fd, char *section);
lio_object_service_fn_t *object_service_remote_client_create(lio_service_manager_t *ess, tbx_inip_file_t *ifd, char *section);

//*** List the various OS commands
#define OSR_EXISTS_KEY             "os_exists"
#define OSR_EXISTS_SIZE            9
#define OSR_CREATE_OBJECT_KEY      "os_create_object"
#define OSR_CREATE_OBJECT_SIZE     16
#define OSR_REMOVE_OBJECT_KEY      "os_remove_object"
#define OSR_REMOVE_OBJECT_SIZE     16
#define OSR_REMOVE_REGEX_OBJECT_KEY  "os_remove_regex_object"
#define OSR_REMOVE_REGEX_OBJECT_SIZE 22
#define OSR_ABORT_REMOVE_REGEX_OBJECT_KEY  "os_abort_remove_regex_object"
#define OSR_ABORT_REMOVE_REGEX_OBJECT_SIZE 28
#define OSR_MOVE_OBJECT_KEY        "os_move_object"
#define OSR_MOVE_OBJECT_SIZE       14
#define OSR_SYMLINK_OBJECT_KEY     "os_symlink_object"
#define OSR_SYMLINK_OBJECT_SIZE    17
#define OSR_HARDLINK_OBJECT_KEY    "os_hardlink_object"
#define OSR_HARDLINK_OBJECT_SIZE   18
#define OSR_OPEN_OBJECT_KEY        "os_open_object"
#define OSR_OPEN_OBJECT_SIZE       14
#define OSR_ABORT_OPEN_OBJECT_KEY  "os_abort_open_object"
#define OSR_ABORT_OPEN_OBJECT_SIZE 20
#define OSR_CLOSE_OBJECT_KEY       "os_close_object"
#define OSR_CLOSE_OBJECT_SIZE      15
#define OSR_REGEX_SET_MULT_ATTR_KEY  "os_regex_set_mult_object"
#define OSR_REGEX_SET_MULT_ATTR_SIZE 24
#define OSR_ABORT_REGEX_SET_MULT_ATTR_KEY  "os_abort_regex_set_mult_object"
#define OSR_ABORT_REGEX_SET_MULT_ATTR_SIZE 30
#define OSR_GET_MULTIPLE_ATTR_KEY  "os_get_mult"
#define OSR_GET_MULTIPLE_ATTR_SIZE 11
#define OSR_SET_MULTIPLE_ATTR_KEY  "os_set_mult"
#define OSR_SET_MULTIPLE_ATTR_SIZE 11
#define OSR_COPY_MULTIPLE_ATTR_KEY  "os_copy_mult"
#define OSR_COPY_MULTIPLE_ATTR_SIZE 12
#define OSR_MOVE_MULTIPLE_ATTR_KEY  "os_move_mult"
#define OSR_MOVE_MULTIPLE_ATTR_SIZE 12
#define OSR_SYMLINK_MULTIPLE_ATTR_KEY  "os_symlink_mult"
#define OSR_SYMLINK_MULTIPLE_ATTR_SIZE 15
#define OSR_OBJECT_ITER_ALIST_KEY   "os_object_iter_alist"
#define OSR_OBJECT_ITER_ALIST_SIZE  20
#define OSR_OBJECT_ITER_AREGEX_KEY  "os_object_iter_aregex"
#define OSR_OBJECT_ITER_AREGEX_SIZE  21
#define OSR_ATTR_ITER_KEY           "os_attr_iter"
#define OSR_ATTR_ITER_SIZE          12
#define OSR_FSCK_ITER_KEY           "os_fsck_iter"
#define OSR_FSCK_ITER_SIZE          12
#define OSR_FSCK_OBJECT_KEY         "os_fsck_object"
#define OSR_FSCK_OBJECT_SIZE        14
#define OSR_SPIN_HB_KEY             "os_spin_hb"
#define OSR_SPIN_HB_SIZE            10

//** Types of ongoing objects stored
#define OSR_ONGOING_FD_TYPE    0
#define OSR_ONGOING_OBJECT_ITER 1
#define OSR_ONGOING_ATTR_ITER   2
#define OSR_ONGOING_FSCK_ITER   3

struct lio_osrs_priv_t {
    char *section;
    char *os_local_section;
    lio_object_service_fn_t *os_child;  //** Actual OS used
    apr_thread_mutex_t *lock;
    apr_thread_mutex_t *abort_lock;
    apr_thread_cond_t *cond;
    apr_pool_t *mpool;
    apr_hash_t *active_table;   //** Queryable active table
    tbx_stack_t *active_lru;        //** LRU sorted active table
    gop_mq_context_t *mqc;          //** Portal for connecting to he remote OS server
    gop_mq_ongoing_t *ongoing;      //** Ongoing open files or iterators
    apr_hash_t *abort;          //** Abort open handles
    apr_hash_t *spin;           //** Abort spin handles
    char *hostname;             //** Addres to bind to
    gop_mq_portal_t *server_portal;
    gop_thread_pool_context_t *tpc;
    int ongoing_interval;       //** Ongoing command check interval
    int shutdown;
    int max_stream;
    int max_active;             //** Max size of the active table.
    lio_authn_t *authn;
    lio_creds_t *dummy_creds;       //** Dummy creds. Should be replaced when proper AuthN/AuthZ is added
    char *fname_active;         //** Filename for logging ACTIVE operations.
    char *fname_activity;       //** Filename for logging create/remove/move operations.
};

struct lio_osrc_priv_t {
    char *section;
    char *temp_section;
    char *authn_section;
    lio_object_service_fn_t *os_temp;  //** Used only for initial debugging of the client/server
    lio_object_service_fn_t *os_remote;//** Used only for initial debugging of the client/server
    apr_thread_mutex_t *lock;
    apr_thread_cond_t *cond;
    gop_mq_context_t *mqc;             //** Portal for connecting to he remote OS server
    gop_mq_ongoing_t *ongoing;         //** Ongoing handle
    mq_msg_t *remote_host;         //** Address of the Remote OS server
    char *remote_host_string;      //** Stringified version of Remote OS server
    char *host_id;                 //** Used for forming the open handle id;
    int host_id_len;               //** Length of host id
    int spin_interval;             //** Spin heartbeat interval
    int spin_fail;                 //** Spin fail interval
    lio_os_authz_t *osaz;
    lio_authn_t *authn;
    apr_pool_t *mpool;
    apr_thread_t *heartbeat_thread;
    gop_thread_pool_context_t *tpc;
    int stream_timeout;
    int timeout;
    int heartbeat;
    int shutdown;
    int max_stream;
};



#ifdef __cplusplus
}
#endif

#endif
