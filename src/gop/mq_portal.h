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
// *************************************************************
//  Generic MQ wrapper for GOP support
// *************************************************************

#ifndef __MQ_PORTAL_H_
#define __MQ_PORTAL_H_

#include <apr_hash.h>
#include <apr_pools.h>
#include <apr_thread_cond.h>
#include <apr_thread_mutex.h>
#include <apr_thread_proc.h>
#include <apr_time.h>
#include <tbx/assert_result.h>
#include <tbx/atomic_counter.h>
#include <tbx/iniparse.h>
#include <tbx/stack.h>
#include <tbx/thread_pool.h>
#include <unistd.h>

#include "gop/gop.h"
#include "gop/visibility.h"
#include "gop/mq.h"
#include "gop/portal.h"
#include "gop/tp.h"
#include "gop/types.h"
#include "host_portal.h"
#include "thread_pool.h"


#ifdef __cplusplus
extern "C" {
#endif


// Types
struct gop_mq_frame_t {
    int len;
    gop_mqf_msg_t auto_free;
    char *data;
    zmq_msg_t zmsg;
};

struct gop_mq_msg_hash_t {
    unsigned int full_hash;
    unsigned int even_hash;
};

struct gop_mq_command_t {
    gop_mq_exec_fn_t fn;
    void *cmd;
    int cmd_size;
    void *arg;
};

struct gop_mq_command_table_t {
    gop_mq_exec_fn_t fn_default;
    void *arg_default;
    apr_hash_t *table;
    apr_pool_t *mpool;
    apr_thread_mutex_t *lock;
};

struct gop_mq_heartbeat_entry_t {
    mq_msg_t *address;
    char *key;
    uint64_t lut_id;
    int key_size;
    int count;
    apr_time_t last_check;
};

struct gop_mq_task_monitor_t {
    gop_mq_task_t *task;
    gop_mq_heartbeat_entry_t *tracking;
    char *id;
    int id_size;
    apr_time_t last_check;
    apr_time_t timeout;
};

struct gop_mq_conn_t {  //** MQ connection container
    gop_mq_portal_t *pc;   //** Parent MQ portal
    char mq_uuid[255];     //** MQ UUID
    gop_mq_socket_t *sock; //** MQ connection socket
    apr_hash_t *waiting;  //** Tasks waiting for a response (key = task ID)
    apr_hash_t *heartbeat_dest;  //** List of unique destinations for heartbeats (key = tracking address)
    apr_hash_t *heartbeat_lut;  //** This is a table of valid heartbeat pointers
    apr_time_t check_start;  //** Last check time
    apr_thread_t *thread;     //** thread handle
    gop_mq_heartbeat_entry_t *hb_conn;  //** Immediate connection uplink
    uint64_t  n_ops;         //** Numbr of ops the connection has processed
    int cefd[2];             //** Private event FD for initial connection handshake
    gop_mq_command_stats_t stats;//** Command stats
    apr_pool_t *mpool;       //** MEmory pool for connection/thread. APR mpools aren't thread safe!!!!!!!
};

struct gop_mq_context_t {      //** Main MQ context
    int min_conn;              //** Min connections to MQ host
    int max_conn;              //** Max number of connections to MQ host
    int min_threads;           //** Min number of worker threads
    int max_threads;           //** Max number of worker threads
    int max_recursion;         //** Max recursion depth expected to eliminate GOP tree deadlocks
    int backlog_trigger;       //** Number of backlog ops to trigger a new connection
    int heartbeat_dt;          //** Heartbeat interval
    int heartbeat_failure;     //** Missing heartbeat DT for failure classification
    int socket_type;           //** NEW: Type of socket to use (TRACE_ROUTER or ROUND_ROBIN)
    double min_ops_per_sec;    //** Minimum ops/sec needed to keep a connection open.
    apr_thread_mutex_t *lock;  //** Context lock
    apr_pool_t *mpool;         //** Context memory pool
    tbx_atomic_unit32_t n_ops;        //** Operation count
    gop_thread_pool_context_t *tp; //** Worker thread pool
    apr_hash_t  *client_portals;      //** List of all client or outgoing portals
    apr_hash_t  *server_portals;  //** List of all the server or incoming portals
    gop_portal_fn_t pcfn;          //** Portal contect used to overide the submit op for the TP
    gop_mq_command_stats_t stats;//** Command stats
};


struct gop_mq_portal_t {   //** Container for managing connections to a single host
    char *host;       //** Host address
    int connect_mode; //** Connection mode connect vs bind
    int min_conn;     //** Min connections to MQ host
    int max_conn;     //** Max number of connections to MQ host
    int active_conn;  //** Active connection count
    int total_conn;   //** Active+closing connection count
    int backlog_trigger;       //** Number of backlog ops to trigger a new connection
    int heartbeat_dt;          //** Heartbeat interval
    int heartbeat_failure;     //** Missing heartbeat DT for failure classification
    int counter;               //** Connections counter
    int n_close;               //** Number of connections being requested to close
    int socket_type;           //** Socket type
    uint64_t n_ops;            //** Operation count
    double min_ops_per_sec;    //** Minimum ops/sec needed to keep a connection open.
    tbx_stack_t *tasks;            //** List of tasks
    tbx_stack_t *closed_conn;      //** List of closed connections that can be destroyed
    mq_pipe_t efd[2];
    apr_thread_mutex_t *lock;  //** Context lock
    apr_thread_cond_t *cond;   //** Shutdown complete cond
    gop_mq_command_table_t *command_table; //** Server command ops for execution
    void *implementation_arg; //** Implementation-specific pointer for general use. Round robin uses this as worker table
    apr_pool_t *mpool;         //** Context memory pool
    gop_thread_pool_context_t *tp; //** Worker thread pool to use
    gop_portal_fn_t pcfn;
    gop_mq_socket_context_t *ctx;  //** Socket context
    gop_mq_context_t *mqc;
    gop_mq_command_stats_t stats;//** Command stats
};

int mq_task_set(gop_mq_task_t *task, gop_mq_context_t *ctx, mq_msg_t *msg, gop_op_generic_t *gop,  void *arg, int dt);
void mq_task_destroy(gop_mq_task_t *task);

int mq_task_send(gop_mq_context_t *mqc, gop_mq_task_t *task);
gop_mq_socket_t *zero_create_socket(gop_mq_socket_context_t *ctx, int stype);
void zero_socket_context_destroy(gop_mq_socket_context_t *ctx);

#ifdef __cplusplus
}
#endif
#endif
