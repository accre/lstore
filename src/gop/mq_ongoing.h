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

#ifndef _MQ_ONGOING_H_
#define _MQ_ONGOING_H_

#include <apr_hash.h>
#include <apr_pools.h>
#include <apr_thread_cond.h>
#include <apr_thread_mutex.h>
#include <apr_thread_proc.h>
#include <apr_time.h>

#include "gop.h"
#include "gop/visibility.h"
#include "gop/mq_ongoing.h"
#include "mq_portal.h"

#ifdef __cplusplus
extern "C" {
#endif

struct ongoing_hb_t {
    char *id;
    int id_len;
    int heartbeat;
    int in_progress;
    int count;
    apr_time_t next_check;
};

struct ongoing_table_t {
    apr_hash_t *table;
    mq_msg_hash_t remote_host_hash;
    mq_msg_t *remote_host;
    int count;
};
struct mq_ongoing_host_t {
    int heartbeat;
    apr_time_t next_check;
    char *id;
    int id_len;
    apr_hash_t *table;
    apr_pool_t *mpool;
};

struct mq_ongoing_t {
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
};

void ongoing_heartbeat_shutdown();
void mq_ongoing_cb(void *arg, mq_task_t *task);

#ifdef __cplusplus
}
#endif

#endif

