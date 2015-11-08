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

//*************************************************************
//  Generic MQ wrapper for GOP support
//*************************************************************

#include "opque.h"
#include "host_portal.h"
#include "atomic_counter.h"
#include "thread_pool.h"
#include "iniparse.h"
#include <zmq.h>
#include <czmq.h>
#include <apr_thread_pool.h>
#include <apr_thread_proc.h>

#ifndef __MQ_PORTAL_H_
#define __MQ_PORTAL_H_

#ifdef __cplusplus
extern "C" {
#endif

//******* MQ Message Auto_Free modes
#define MQF_MSG_AUTO_FREE     0  //** Auto free data on destroy
#define MQF_MSG_KEEP_DATA     1  //** Skip free'ing of data on destroy.  App is responsible.
#define MQF_MSG_INTERNAL_FREE 2  //** The msg routines are responsible for free'ing the data. Used on mq_recv().
 
//***** MQ Frame constants
#define MQF_VERSION_KEY        "LMQv100"
#define MQF_VERSION_SIZE       7
#define MQF_PING_KEY           "\001"
#define MQF_PING_SIZE          1
#define MQF_PONG_KEY           "\002"
#define MQF_PONG_SIZE          1
#define MQF_EXEC_KEY           "\003"
#define MQF_EXEC_SIZE          1
#define MQF_TRACKEXEC_KEY      "\004"
#define MQF_TRACKEXEC_SIZE     1
#define MQF_TRACKADDRESS_KEY   "\005"
#define MQF_TRACKADDRESS_SIZE  1
#define MQF_RESPONSE_KEY       "\006"
#define MQF_RESPONSE_SIZE      1
 
#define MQS_PING_INDEX         0
#define MQS_PONG_INDEX         1
#define MQS_EXEC_INDEX         2
#define MQS_TRACKEXEC_INDEX    3
#define MQS_TRACKADDRESS_INDEX 4
#define MQS_RESPONSE_INDEX     5
#define MQS_HEARTBEAT_INDEX    6
#define MQS_UNKNOWN_INDEX      7
#define MQS_SIZE               8
 
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
#define MQ_CMODE_CLIENT  0     //** Normal outgoing connection
#define MQ_CMODE_SERVER  1     //** USed by servers for incoming connections
 
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
 
typedef zmq_pollitem_t mq_pollitem_t;
//QWERT typedef zmq_event_t mq_event_t;
 
typedef Stack_t mq_msg_t;
 
typedef struct {
int len;
int auto_free;
char *data;
zmq_msg_t zmsg;
} mq_frame_t;
 
typedef struct {
unsigned int full_hash;
unsigned int even_hash;
} mq_msg_hash_t;
 
#define mq_data_compare(A, sA, B, sB) (((sA) == (sB)) ? memcmp(A, B, sA) : 1)
 
#define mq_poll(items, n, wait_ms) zmq_poll(items, n, wait_ms)
#define mq_msg_pop(A) (mq_frame_t *)pop(A)
 
 
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
 
struct mq_portal_s;
typedef struct mq_portal_s mq_portal_t;
struct mq_context_s;
typedef struct mq_context_s mq_context_t;
 
struct mq_socket_context_s;
typedef struct mq_socket_context_s mq_socket_context_t;
 
struct mq_socket_s;
typedef struct mq_socket_s mq_socket_t;
 
struct mq_task_s;
typedef struct mq_task_s mq_task_t;
 
#ifdef MQ_PIPE_COMM
typedef int mq_pipe_t;       //** Event notification FD
#else
typedef mq_socket_t* mq_pipe_t;    //** ZMQ_PAIR sockets
#endif
 
struct mq_socket_s {
int type;
void *arg;
void (*destroy)(mq_socket_context_t *ctx, mq_socket_t  *socket);
int (*bind)(mq_socket_t *socket, const char *format, ...);
int (*connect)(mq_socket_t *socket, const char *format, ...);
int (*disconnect)(mq_socket_t *socket, const char *format, ...);  //** Need host since multiple simul endpoints are supported.
void *(*poll_handle)(mq_socket_t *socket);
int (*monitor)(mq_socket_t *socket, char *address, int events);
int (*send)(mq_socket_t *socket, mq_msg_t *msg, int flags);
int (*recv)(mq_socket_t *socket, mq_msg_t *msg, int flags);
};
 
struct mq_socket_context_s {
void *arg;
mq_socket_t *(*create_socket)(mq_socket_context_t *ctx, int stype);
void (*destroy)(mq_socket_context_t *ctx);
};
 
typedef void (mq_fn_exec_t)(void *arg, mq_task_t *task);
 
typedef struct {
mq_fn_exec_t *fn;
void *cmd;
int cmd_size;
void *arg;
} mq_command_t;
 
typedef struct {
mq_fn_exec_t *fn_default;
void *arg_default;
apr_hash_t *table;
apr_pool_t *mpool;
apr_thread_mutex_t *lock;
} mq_command_table_t;
 
struct mq_task_s {      //** Generic containter for MQ messages for both the server and GOP (or client). If the variable is not used it's value is NULL.
mq_msg_t *msg;          //** Actual message to send with address (Server+GOP)
mq_msg_t *response;     //** Response message (GOP)
op_generic_t *gop;      //** GOP corresponding to the task.  This could be NULL if a direct submission is used (GOP)
mq_context_t *ctx;      //** Portal context for sending responses. (Server+GOP)
void *arg;              //** Optional argument when calling mq_command_add() or new_mq_op() (server+GOP)
apr_time_t timeout;     //** Initially the DT in sec for the command to complete and converted to abs timeout when sent
void (*my_arg_free)(void *arg);  //** Function for cleaning up the GOP arg. (GOP)
int pass_through;       //** Flag to set when a task is only used to pass a message; no heartbeating necessary
};
 
typedef struct {
mq_msg_t *address;
char *key;
uint64_t lut_id;
int key_size;
int count;
apr_time_t last_check;
} mq_heartbeat_entry_t;
 
typedef struct {
mq_task_t *task;
mq_heartbeat_entry_t *tracking;
char *id;
int id_size;
apr_time_t last_check;
apr_time_t timeout;
} mq_task_monitor_t;
 
typedef struct {
int incoming[MQS_SIZE];
int outgoing[MQS_SIZE];
} mq_command_stats_t;
 
typedef struct {  //** MQ connection container
mq_portal_t *pc;   //** Parent MQ portal
char *mq_uuid;     //** MQ UUID
mq_socket_t *sock; //** MQ connection socket
apr_hash_t *waiting;  //** Tasks waiting for a response (key = task ID)
apr_hash_t *heartbeat_dest;  //** List of unique destinations for heartbeats (key = tracking address)
apr_hash_t *heartbeat_lut;  //** This is a table of valid heartbeat pointers
apr_time_t check_start;  //** Last check time
apr_thread_t *thread;     //** thread handle
mq_heartbeat_entry_t *hb_conn;  //** Immediate connection uplink
uint64_t  n_ops;         //** Numbr of ops the connection has processed
int cefd[2];             //** Private event FD for initial connection handshake
mq_command_stats_t stats;//** Command stats
apr_pool_t *mpool;       //** MEmory pool for connection/thread. APR mpools aren't thread safe!!!!!!!
} mq_conn_t;
 
struct mq_portal_s {   //** Container for managing connections to a single host
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
Stack_t *tasks;            //** List of tasks
Stack_t *closed_conn;      //** List of closed connections that can be destroyed
mq_pipe_t efd[2];
apr_thread_mutex_t *lock;  //** Context lock
apr_thread_cond_t *cond;   //** Shutdown complete cond
mq_command_table_t *command_table; //** Server command ops for execution
void *implementation_arg; //** Implementation-specific pointer for general use. Round robin uses this as worker table
apr_pool_t *mpool;         //** Context memory pool
thread_pool_context_t *tp; //** Worker thread pool to use
portal_fn_t pcfn;
mq_socket_context_t *ctx;  //** Socket context
mq_context_t *mqc;
mq_command_stats_t stats;//** Command stats
};
 
 
struct mq_context_s {      //** Main MQ context
int min_conn;              //** Min connections to MQ host
int max_conn;              //** Max number of connections to MQ host
int min_threads;           //** Min number of worker threads
int max_threads;           //** Max number of worker threads
int backlog_trigger;       //** Number of backlog ops to trigger a new connection
int heartbeat_dt;          //** Heartbeat interval
int heartbeat_failure;     //** Missing heartbeat DT for failure classification
int socket_type;           //** NEW: Type of socket to use (TRACE_ROUTER or ROUND_ROBIN)
double min_ops_per_sec;    //** Minimum ops/sec needed to keep a connection open.
apr_thread_mutex_t *lock;  //** Context lock
apr_pool_t *mpool;         //** Context memory pool
atomic_int_t n_ops;        //** Operation count
thread_pool_context_t *tp; //** Worker thread pool
apr_hash_t  *client_portals;      //** List of all client or outgoing portals
apr_hash_t  *server_portals;  //** List of all the server or incoming portals
portal_fn_t pcfn;          //** Portal contect used to overide the submit op for the TP
mq_command_stats_t stats;//** Command stats
};
 
 
//--------------------------------------------------------------
//  Code for performing MQ portal <-> client communication.
//  There are 2 methods for this.  The most efficient is to use
//  pipe() and do simple write/read/poll communications with both
//  normal file descriptors and network sockets.  But this only works
//  on systems that have BSD style sockets.  Which is all the *nix
//  variants.
//
//  On MS windows we instead use a ZMQ PAIR type socket.
//--------------------------------------------------------------
 
#ifdef MQ_PIPE_COMM
//  #define mq_pipe_create(ctx, pfd)  assert_result(pipe(pfd), 0); fcntl(pfd[0], F_SETFL, O_NONBLOCK)
#define mq_pipe_create(ctx, pfd)  assert_result(pipe(pfd), 0)
#define mq_pipe_poll_store(pollfd, cfd, mode) (pollfd)->fd = cfd;  (pollfd)->events = mode
#define mq_pipe_destroy(ctx, pfd) if (pfd[0] != -1) { close(pfd[0]); close(pfd[1]); }
#define mq_pipe_read(fd, c) read(fd, c, 1)
#define mq_pipe_write(fd, c) write(fd, c, 1)
 
#else
void mq_pipe_create(mq_socket_context_t *ctx, mq_socket_t **pfd);
void mq_pipe_poll_store(mq_pollitem_t *pfd, mq_socket_t *sock, int mode);
void mq_pipe_destroy(mq_socket_context_t *ctx, mq_socket_t **pfd);
int mq_pipe_read(mq_socket_t *sock, char *buf);
int mq_pipe_write(mq_socket_t *sock, char *buf);
#endif
 
typedef mq_context_t *(mq_create_t)(inip_file_t *ifd, char *section);
 
char *mq_id2str(char *id, int id_len, char *str, int str_len);
 
mq_msg_t *mq_msg_new();
int mq_get_frame(mq_frame_t *f, void **data, int *size);
char *mq_frame_strdup(mq_frame_t *f);
mq_frame_t *mq_msg_first(mq_msg_t *msg);
mq_frame_t *mq_msg_last(mq_msg_t *msg);
mq_frame_t *mq_msg_next(mq_msg_t *msg);
mq_frame_t *mq_msg_prev(mq_msg_t *msg);
mq_frame_t *mq_msg_current(mq_msg_t *msg);
mq_frame_t *mq_frame_dup(mq_frame_t *f);
mq_frame_t *mq_msg_pluck(mq_msg_t *msg, int move_up);
void mq_msg_insert_above(mq_msg_t *msg, mq_frame_t *f);
void mq_msg_insert_below(mq_msg_t *msg, mq_frame_t *f);
void mq_msg_push_frame(mq_msg_t *msg, mq_frame_t *f);
void mq_msg_append_frame(mq_msg_t *msg, mq_frame_t *f);
void mq_msg_append_msg(mq_msg_t *msg, mq_msg_t *extra, int mode);
mq_msg_hash_t mq_msg_hash(mq_msg_t *msg);
mq_frame_t *mq_frame_new(void *data, int len, int auto_free);
void mq_frame_set(mq_frame_t *f, void *data, int len, int auto_free);
void mq_frame_destroy(mq_frame_t *f);
void mq_msg_destroy(mq_msg_t *msg);
void mq_msg_push_mem(mq_msg_t *msg, void *data, int len, int auto_free);
void mq_msg_append_mem(mq_msg_t *msg, void *data, int len, int auto_free);
int mq_msg_total_size(mq_msg_t *msg);
 
mq_msg_t *mq_trackaddress_msg(char *host, mq_msg_t *raw_address, mq_frame_t *fid, int dup_frames);
void mq_apply_return_address_msg(mq_msg_t *msg, mq_msg_t *raw_address, int dup_frames);
 
void mq_stats_add(mq_command_stats_t *a, mq_command_stats_t *b);
void mq_stats_print(int ll, char *tag, mq_command_stats_t *a);
mq_task_t *mq_task_new(mq_context_t *ctx, mq_msg_t *msg, op_generic_t *gop, void *arg, int dt);
int mq_task_set(mq_task_t *task, mq_context_t *ctx, mq_msg_t *msg, op_generic_t *gop,  void *arg, int dt);
void mq_task_destroy(mq_task_t *task);
op_generic_t *new_mq_op(mq_context_t *ctx, mq_msg_t *msg, op_status_t (*fn_response)(void *arg, int id), void *arg, void (*my_arg_free)(void *arg), int dt);
 
mq_command_t *mq_command_new(void *cmd, int cmd_size, void *arg, mq_fn_exec_t *fn);
void mq_command_set(mq_command_table_t *table, void *cmd, int cmd_size, void *arg, mq_fn_exec_t *fn);
void mq_command_exec(mq_command_table_t *t, mq_task_t *task, void *key, int klen);
void mq_command_table_destroy(mq_command_table_t *t);
mq_command_table_t *mq_command_table_new(void *arg, mq_fn_exec_t *fn_default);
void mq_command_table_set_default(mq_command_table_t *table, void *arg, mq_fn_exec_t *fn);
 
int mq_task_send(mq_context_t *mqc, mq_task_t *task);
int mq_submit(mq_portal_t *p, mq_task_t *task);
int mq_portal_install(mq_context_t *mqc, mq_portal_t *p);
void mq_portal_remove(mq_context_t *mqc, mq_portal_t *p);
void mq_portal_destroy(mq_portal_t *p);
mq_portal_t *mq_portal_create(mq_context_t *mqc, char *host, int connect_mode);
mq_portal_t *mq_portal_lookup(mq_context_t *mqc, char *host, int connect_mode);
mq_command_table_t *mq_portal_command_table(mq_portal_t *portal);
mq_context_t *mq_create_context(inip_file_t *ifd, char *section);
void mq_destroy_context(mq_context_t *mqp);
mq_socket_t *zero_create_socket(mq_socket_context_t *ctx, int stype);
void zero_socket_context_destroy(mq_socket_context_t *ctx);
mq_socket_context_t *zero_socket_context_new();
 
#ifdef __cplusplus
}
#endif
 
 
#endif
 
 
