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
// opque.h - Header defining I/O structs and operations for
//     collections of oplists
//*************************************************************

#ifndef __OPQUE_H_
#define __OPQUE_H_

#include <apr_thread_mutex.h>
#include <apr_thread_cond.h>
#include <apr_hash.h>
#include "atomic_counter.h"
#include "network.h"
#include "stack.h"
#include "callback.h"
#include "pigeon_coop.h"

#ifdef __cplusplus
extern "C" {
#endif

struct opque_s;
typedef struct opque_s opque_t;

struct op_generic_s;
typedef struct op_generic_s op_generic_t;

#define OP_STATE_SUCCESS  10
#define OP_STATE_FAILURE  20
#define OP_STATE_RETRY    30
#define OP_STATE_DEAD     40
#define OP_STATE_TIMEOUT  50
#define OP_STATE_INVALID_HOST 60
#define OP_STATE_CANT_CONNECT 70
#define OP_STATE_ERROR    80

#define Q_TYPE_OPERATION 50
#define Q_TYPE_QUE       51

#define OP_FINALIZE      -10
#define OP_DESTROY       -20

#define OP_FM_FORCED     11
#define OP_FM_GET_END    22

#define OP_EXEC_QUEUE    100
#define OP_EXEC_DIRECT   101

typedef struct {
  apr_thread_mutex_t *lock;  //** shared lock
  apr_thread_cond_t *cond;   //** shared condition variable
  pigeon_coop_hole_t  pch;   //** Pigeon coop hole for the lock and cond
} gop_control_t;

typedef struct {       //** Generic opcode status
  int op_status;          //** Simplified operation status, OP_SUCCESS or OP_FAILURE
  int error_code;         //** Low level op error code
} op_status_t;

extern op_status_t op_success_status;
extern op_status_t op_failure_status;
extern op_status_t op_retry_status;
extern op_status_t op_dead_status;
extern op_status_t op_timeout_status;
extern op_status_t op_invalid_host_status;
extern op_status_t op_cant_connect_status;
extern op_status_t op_error_status;

typedef struct {   //** Command operation
   char *hostport; //** Depot hostname:port:type:...  Unique string for host/connect_context
   void *connect_context;   //** Private information needed to make a host connection
   int  cmp_size;  //** Used for ordering commands within the same host
   apr_time_t timeout;    //** Command timeout
   apr_time_t retry_wait; //** How long to wait in case of a dead socket, if 0 then retry immediately
   int64_t workload;   //** Workload for measuring channel usage
   int retry_count;//** Number of times retried
   op_status_t (*send_command)(op_generic_t *gop, NetStream_t *ns);  //**Send command routine
   op_status_t (*send_phase)(op_generic_t *gop, NetStream_t *ns);    //**Handle "sending" side of command
   op_status_t (*recv_phase)(op_generic_t *gop, NetStream_t *ns);    //**Handle "receiving" half of command
   int (*on_submit)(Stack_t *stack, Stack_ele_t *gop_ele);                      //** Executed during initial execution submission
   int (*before_exec)(op_generic_t *gop);                    //** Executed when popped off the globabl que
   int (*destroy_command)(op_generic_t *gop);                //**Destroys the data structure
   Stack_t  *coalesced_ops;                                  //** Stores any other coalesced ops
   atomic_int_t on_top;
   apr_time_t start_time;
   apr_time_t end_time;
} command_op_t;


typedef struct {  //** Hportal specific implementation
  void *(*dup_connect_context)(void *connect_context);  //** Duplicates a ccon
  void (*destroy_connect_context)(void *connect_context);
  int (*connect)(NetStream_t *ns, void *connect_context, char *host, int port, Net_timeout_t timeout);
  void (*close_connection)(NetStream_t *ns);
  void (*sort_tasks)(void *arg, opque_t *q);        //**optional
  void (*submit)(void *arg, op_generic_t *op);
} portal_fn_t;

typedef struct {             //** Handle for maintaining all the ecopy connections
  apr_thread_mutex_t *lock;
  apr_hash_t *table;         //** Table containing the depot_portal structs
  apr_pool_t *pool;          //** Memory pool for hash table
  int running_threads;       //** currently running # of connections
  int max_connections;       //** Max aggregate allowed number of threads
  int min_threads;           //** Max allowed number of threads/host
  int max_threads;           //** Max allowed number of threads/host
  int max_wait;              //** Max time to wait on a retry_dead_socket
  int64_t max_workload;      //** Max allowed workload before spawning another connection
  int compact_interval;      //** Interval between garbage collections calls
  int wait_stable_time;      //** time to wait before adding connections for unstable hosts
  int abort_conn_attempts;   //** If this many failed connection requests occur in a row we abort
  int check_connection_interval; //** Max time to wait for a thread to check for a close
  int min_idle;              //** Idle time before closing connection
  int max_retry;             //** Default max number of times to retry an op
  int count;                 //** Internal Counter 
  apr_time_t   next_check;       //** Time for next compact_dportal call
  Net_timeout_t dt;          //** Default wait time
  void *arg;
  portal_fn_t *fn;       //** Actual implementaion for application
} portal_context_t;

typedef struct {
   callback_t *cb;        //** Optional callback
   opque_t *parent_q;     //** Parent que attached to
   op_status_t status;    //** Command result
   int failure_mode;      //** Used via the callbacks to force a failure, even on success
   int retries;           //** Upon failure how many times we've retried
   int id;                //** Op's global id.  Can be changed by use but generally should use my_id
   int my_id;             //** User/Application settable id.  Defaults to id.
   int state;             //** Command state 0=submitted 1=completed
   int started_execution; //** If 1 the tasks have already been submitted for execution
   int execution_mode;    //** Execution mode OP_EXEC_QUEUE | OP_EXEC_DIRECT
   int auto_destroy;      //** If 1 then automatically call the free fn to destroy the object
   gop_control_t *ctl;    //** Lock and condition struct
   void *user_priv;           //** Optional user supplied handle
   void (*free)(op_generic_t *d, int mode);
   portal_context_t *pc;
} op_common_t;

typedef struct {
   Stack_t *list;         //** List of tasks
   Stack_t *finished;     //** lists that have completed and not yet processed
   Stack_t *failed;       //** All lists that fail are also placed here
   int nleft;             //** Number of lists left to be processed
   int nsubmitted;        //** Nunmber of submitted tasks (doesn't count sub q's)
   int finished_submission; //** No more tasks will be submitted so it's safe to free the data when finished
//   int success;             //** Only used if no failed tasks occur to determine success
   callback_t failure_cb;   //** Only used if a task fails
   opque_t *opque;
} que_data_t;


typedef struct {
  portal_context_t *pc;
  command_op_t cmd;
  void *priv;
} op_data_t;


struct op_generic_s {
  int type;
  void *free_ptr;
  op_common_t base;
  que_data_t   *q;
  op_data_t   *op;
};


struct opque_s {
  op_generic_t op;
  que_data_t   qd;
};


extern int _opque_counter;

#define _op_set_status(v, opstat, errcode) (v).op_status = opstat; (v).error_code = errcode

#define lock_opque(q)   log_printf(15, "lock_opque: qid=%d\n", (q)->opque->op.base.id); apr_thread_mutex_lock((q)->opque->op.base.ctl->lock)
#define unlock_opque(q) log_printf(15, "unlock_opque: qid=%d\n", (q)->opque->op.base.id); apr_thread_mutex_unlock((q)->opque->op.base.ctl->lock)
//#define lock_opque(q)   apr_thread_mutex_lock((q)->opque->op.base.ctl->lock)
//#define unlock_opque(q) apr_thread_mutex_unlock((q)->opque->op.base.ctl->lock)
#define lock_gop(gop)   log_printf(15, "lock_gop: gid=%d\n", (gop)->base.id); apr_thread_mutex_lock((gop)->base.ctl->lock)
#define unlock_gop(gop) log_printf(15, "unlock_gop: gid=%d\n", (gop)->base.id); apr_thread_mutex_unlock((gop)->base.ctl->lock)
//#define lock_gop(gop)   apr_thread_mutex_lock((gop)->base.ctl->lock)
//#define unlock_gop(gop) apr_thread_mutex_unlock((gop)->base.ctl->lock)
#define gop_id(gop) (gop)->base.id
#define gop_get_auto_destroy(gop) (gop)->base.auto_destroy
#define gop_set_auto_destroy(gop, val) (gop)->base.auto_destroy = val
#define gop_get_private(gop) (gop)->base.user_priv
#define gop_set_private(gop, newval) (gop)->base.user_priv = newval
#define gop_get_id(gop) (gop)->base.id
#define gop_set_id(gop, newval) (gop)->base.id = newval
#define gop_get_myid(gop) (gop)->base.my_id
#define gop_set_myid(gop, newval) (gop)->base.my_id = newval
#define gop_get_type(gop) (gop)->type
//#define opque_set_success_state(q, state) (q)->qd.success = state
#define opque_get_gop(q) &((q)->op)
#define opque_failure_callback_set(q, fn, priv) callback_set(&(q->failure_cb), fn, priv)
#define opque_callback_append(q, cb) gop_callback_append(opque_get_gop(q), (cb))
#define opque_get_next_finished(q) gop_get_next_finished(opque_get_gop(q))
#define opque_get_next_failed(q) gop_get_next_failed(opque_get_gop(q))
#define opque_tasks_failed(q) gop_tasks_failed(opque_get_gop(q))
#define opque_tasks_finished(q) gop_tasks_finished(opque_get_gop(q))
#define opque_tasks_left(q) gop_tasks_left(opque_get_gop(q))
#define opque_task_count(q) q->qd.nsubmitted
#define opque_waitall(q) gop_waitall(opque_get_gop(q))
#define opque_waitany(q) gop_waitany(opque_get_gop(q))
#define opque_start_execution(q) gop_start_execution(opque_get_gop(q))
#define opque_finished_submission(q) gop_finished_submission(opque_get_gop(q))

#define opque_set_status(q, val) gop_set_status(opque_get_gop(q), val)
#define opque_get_status(q) gop_get_status(opque_get_gop(q))
#define opque_completed_successfully(q) gop_completed_successfully(q)

#define gop_get_status(gop) (gop)->base.status
#define gop_set_status(gop, val) (gop)->base.status = val

//#define gop_get_result(gop) (gop)->base.op_result
//#define gop_set_result(gop, val) (gop)->base.result = val

void gop_simple_cb(void *v, int mode);
void opque_set_failure_mode(opque_t *q, int value);
int opque_get_failure_mode(opque_t *q);
op_status_t opque_completion_status(opque_t *q);
void opque_set_arg(opque_t *q, void *arg);
void *opque_get_arg(opque_t *q);
opque_t *new_opque();
void init_opque(opque_t *que);
void init_opque_system();
void destroy_opque_system();
void opque_free(opque_t *que, int mode);
int opque_add(opque_t *que, op_generic_t *gop);
int internal_opque_add(opque_t *que, op_generic_t *gop, int dolock);
void default_sort_ops(void *arg, opque_t *que);

op_generic_t *gop_dummy(op_status_t state);
void gop_free(op_generic_t *gop, int mode);
void gop_set_success_state(op_generic_t *g, op_status_t state);
void gop_callback_append(op_generic_t *q, callback_t *cb);
op_generic_t *gop_get_next_finished(op_generic_t *gop);
op_generic_t *gop_get_next_failed(op_generic_t *gop);
int gop_tasks_failed(op_generic_t *gop);
int gop_tasks_finished(op_generic_t *gop);
int gop_tasks_left(op_generic_t *gop);
int gop_waitall(op_generic_t *gop);
op_generic_t *gop_waitany(op_generic_t *gop);
void gop_start_execution(op_generic_t *gop);
void gop_finished_submission(op_generic_t *gop);
void gop_set_exec_mode(op_generic_t *g, int mode);

int gop_completed_successfully(op_generic_t *gop);

void gop_mark_completed(op_generic_t *gop, op_status_t status);
int gop_sync_exec(op_generic_t *gop);
void gop_reset(op_generic_t *gop);
void gop_init(op_generic_t *gop);
void gop_generic_free(op_generic_t *gop, int mode);
void gop_callback_append(op_generic_t *gop, callback_t *cb);

#ifdef __cplusplus
}
#endif


#endif

