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
//  Generic MQ wrapper for GOP support
//*************************************************************

#ifndef __MQ_PORTAL_H_
#define __MQ_PORTAL_H_

#include <apr_hash.h>
#include <apr_pools.h>
#include <apr_thread_cond.h>
#include <apr_thread_mutex.h>
#include <apr_thread_pool.h>
#include <apr_thread_proc.h>
#include <apr_time.h>
#include <czmq.h>
#include <tbx/assert_result.h>
#include <tbx/atomic_counter.h>
#include <tbx/iniparse.h>
#include <tbx/stack.h>
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


// Preprocessor defines required for structs
#define MQS_PING_INDEX         0
#define MQS_PONG_INDEX         1
#define MQS_EXEC_INDEX         2
#define MQS_TRACKEXEC_INDEX    3
#define MQS_TRACKADDRESS_INDEX 4
#define MQS_RESPONSE_INDEX     5
#define MQS_HEARTBEAT_INDEX    6
#define MQS_UNKNOWN_INDEX      7
#define MQS_SIZE               8

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

struct gop_mq_socket_t {
    int type;
    void *arg;
    void (*destroy)(gop_mq_socket_context_t *ctx, gop_mq_socket_t  *socket);
    int (*bind)(gop_mq_socket_t *socket, const char *format, ...);
    int (*connect)(gop_mq_socket_t *socket, const char *format, ...);
    int (*disconnect)(gop_mq_socket_t *socket, const char *format, ...);  //** Need host since multiple simul endpoints are supported.
    void *(*poll_handle)(gop_mq_socket_t *socket);
    int (*monitor)(gop_mq_socket_t *socket, char *address, int events);
    int (*send)(gop_mq_socket_t *socket, mq_msg_t *msg, int flags);
    int (*recv)(gop_mq_socket_t *socket, mq_msg_t *msg, int flags);
};

struct gop_mq_socket_context_t {
    void *arg;
    gop_mq_socket_t *(*create_socket)(gop_mq_socket_context_t *ctx, int stype);
    void (*destroy)(gop_mq_socket_context_t *ctx);
};

struct gop_mq_command_t {
    gop_mq_exec_fn_t fn;
    void *cmd;
    int cmd_size;
    void *arg;
};

struct gop_gop_mq_command_table_t {
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

struct gop_mq_command_stats_t {
    int incoming[MQS_SIZE];
    int outgoing[MQS_SIZE];
};

struct gop_mq_conn_t {  //** MQ connection container
    gop_mq_portal_t *pc;   //** Parent MQ portal
    char *mq_uuid;     //** MQ UUID
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
    gop_gop_mq_command_table_t *command_table; //** Server command ops for execution
    void *implementation_arg; //** Implementation-specific pointer for general use. Round robin uses this as worker table
    apr_pool_t *mpool;         //** Context memory pool
    gop_thread_pool_context_t *tp; //** Worker thread pool to use
    gop_portal_fn_t pcfn;
    gop_mq_socket_context_t *ctx;  //** Socket context
    gop_mq_context_t *mqc;
    gop_mq_command_stats_t stats;//** Command stats
};

typedef zmq_pollitem_t mq_pollitem_t;

//***** MQ Frame constants

//****** Error states
#define MQ_E_ERROR      OP_STATE_FAILURE
#define MQ_E_OK         OP_STATE_SUCCESS
#define MQ_E_DESTROY    -1
#define MQ_E_NOP        -2
#define MQ_E_IGNORE     -3

//******** Polling states
#define MQ_POLLIN  ZMQ_POLLIN
#define MQ_POLLOUT ZMQ_POLLOUT
#define MQ_POLLERR ZMQ_POLLERR

//********  Connection modes
//******** Socket types
#define MQ_DEALER ZMQ_DEALER
#define MQ_PAIR   ZMQ_PAIR
#define MQ_ROUTER ZMQ_ROUTER
#define MQ_TRACE_ROUTER   1000
#define MQ_SIMPLE_ROUTER  1001

//******** Event types
//QWERT #define MQ_EVENT_CONNECTED ZMQ_EVENT_CONNECTED
//QWERT #define MQ_EVENT_CLOSED    ZMQ_EVENT_CLOSED
//QWERT #define MQ_EVENT_ALL       ZMQ_EVENT_ALL

//******** Send/Recv flags
#define MQ_DONTWAIT ZMQ_DONTWAIT



#define mq_poll(items, n, wait_ms) zmq_poll(items, n, wait_ms)
#define mq_socket_new(ctx, type) (ctx)->create_socket(ctx, type)
#define mq_socket_destroy(ctx, socket) (socket)->destroy(ctx, socket)
#define mq_socket_context_new()  zero_socket_context_new()
#define mq_socket_context_destroy(ctx)  (ctx)->destroy(ctx)
#define mq_socket_monitor(sock, port, event) (sock)->monitor(sock, port, event)
#define mq_connect(sock, ...) (sock)->connect(sock, __VA_ARGS__)
#define mq_bind(sock, ...) (sock)->bind(sock, __VA_ARGS__)
#define mq_disconnect(sock, ...) (sock)->disconnect(sock, __VA_ARGS__)
#define mq_send(sock, msg, flags)  (sock)->send(sock, msg, flags)
#define mq_recv(sock, msg, flags)  (sock)->recv(sock, msg, flags)
#define mq_poll_handle(sock)  (sock)->poll_handle(sock)

#define mq_pipe_create(ctx, pfd)  assert_result(pipe(pfd), 0)
#define mq_pipe_poll_store(pollfd, cfd, mode) (pollfd)->fd = cfd;  (pollfd)->events = mode
#define mq_pipe_destroy(ctx, pfd) if (pfd[0] != -1) { close(pfd[0]); close(pfd[1]); }
#define mq_pipe_read(fd, c) read(fd, c, 1)
#define mq_pipe_write(fd, c) write(fd, c, 1)

gop_mq_frame_t *mq_msg_prev(mq_msg_t *msg);
gop_mq_frame_t *mq_frame_dup(gop_mq_frame_t *f);
void mq_msg_tbx_stack_insert_above(mq_msg_t *msg, gop_mq_frame_t *f);
void mq_msg_tbx_stack_insert_below(mq_msg_t *msg, gop_mq_frame_t *f);
void mq_msg_push_frame(mq_msg_t *msg, gop_mq_frame_t *f);
gop_mq_msg_hash_t mq_msg_hash(mq_msg_t *msg);
void mq_msg_push_mem(mq_msg_t *msg, void *data, int len, gop_mqf_msg_t auto_free);
int mq_msg_total_size(mq_msg_t *msg);

mq_msg_t *mq_trackaddress_msg(char *host, mq_msg_t *raw_address, gop_mq_frame_t *fid, int dup_frames);

void mq_stats_add(gop_mq_command_stats_t *a, gop_mq_command_stats_t *b);
void mq_stats_print(int ll, char *tag, gop_mq_command_stats_t *a);
int mq_task_set(gop_mq_task_t *task, gop_mq_context_t *ctx, mq_msg_t *msg, gop_op_generic_t *gop,  void *arg, int dt);
void mq_task_destroy(gop_mq_task_t *task);

gop_mq_command_t *mq_command_new(void *cmd, int cmd_size, void *arg, gop_mq_exec_fn_t fn);
void mq_command_exec(gop_gop_mq_command_table_t *t, gop_mq_task_t *task, void *key, int klen);
void gop_mq_command_table_destroy(gop_gop_mq_command_table_t *t);
gop_gop_mq_command_table_t *gop_mq_command_table_new(void *arg, gop_mq_exec_fn_t fn_default);

int mq_task_send(gop_mq_context_t *mqc, gop_mq_task_t *task);
gop_mq_socket_t *zero_create_socket(gop_mq_socket_context_t *ctx, int stype);
void zero_socket_context_destroy(gop_mq_socket_context_t *ctx);
gop_mq_socket_context_t *zero_socket_context_new();

#ifdef __cplusplus
}
#endif
#endif
