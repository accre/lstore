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
// MQ ongoing task management header
//***********************************************************************

#include "gop/gop_visibility.h"
#include "gop.h"
#include "mq_portal.h"

#ifndef _MQ_ONGOING_H_
#define _MQ_ONGOING_H_

#ifdef __cplusplus
extern "C" {
#endif

#define ONGOING_KEY            "ongoing"
#define ONGOING_SIZE           7

#define ONGOING_SERVER 1
#define ONGOING_CLIENT 2

typedef op_generic_t *(mq_ongoing_fail_t)(void *arg, void *handle);

typedef struct {
    char *id;
    int id_len;
    int heartbeat;
    int in_progress;
    int count;
    apr_time_t next_check;
} ongoing_hb_t;

typedef struct {
    apr_hash_t *table;
    mq_msg_hash_t remote_host_hash;
    mq_msg_t *remote_host;
    int count;
} ongoing_table_t;


typedef struct {
    int type;
    int count;
    int auto_clean;
    void *handle;
    intptr_t key;
    mq_ongoing_fail_t *on_fail;
    void *on_fail_arg;
} mq_ongoing_object_t;

typedef struct {
    int heartbeat;
    apr_time_t next_check;
    char *id;
    int id_len;
    apr_hash_t *table;
    apr_pool_t *mpool;
} mq_ongoing_host_t;

typedef struct {
    apr_pool_t *mpool;
    apr_thread_mutex_t *lock;
    apr_thread_cond_t *cond;
    apr_hash_t *id_table;       //** Server table
    apr_hash_t *table;          //** Client table
    apr_thread_t *ongoing_server_thread;
    apr_thread_t *ongoing_heartbeat_thread;
    mq_context_t *mqc;
    mq_portal_t *server_portal;
    int check_interval;
    int shutdown;
    int send_divisor;
} mq_ongoing_t;

GOP_API void mq_ongoing_host_inc(mq_ongoing_t *on, mq_msg_t *remote_host, char *id, int id_len, int heartbeat);
GOP_API void mq_ongoing_host_dec(mq_ongoing_t *on, mq_msg_t *remote_host, char *id, int id_len);
void ongoing_heartbeat_shutdown();
void mq_ongoing_cb(void *arg, mq_task_t *task);
GOP_API mq_ongoing_object_t *mq_ongoing_add(mq_ongoing_t *mqon, int auto_clean, char *id, int id_len, void *handle, mq_ongoing_fail_t *on_fail, void *on_fail_arg);
GOP_API void *mq_ongoing_remove(mq_ongoing_t *mqon, char *id, int id_len, intptr_t key);
GOP_API void *mq_ongoing_get(mq_ongoing_t *mqon, char *id, int id_len, intptr_t key);
GOP_API void mq_ongoing_release(mq_ongoing_t *mqon, char *id, int id_len, intptr_t key);
GOP_API mq_ongoing_t *mq_ongoing_create(mq_context_t *mqc, mq_portal_t *server_portal, int check_interval, int mode);
GOP_API void mq_ongoing_destroy(mq_ongoing_t *mqon);

#ifdef __cplusplus
}
#endif

#endif

