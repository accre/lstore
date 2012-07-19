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

#ifndef __HOST_PORTAL_H_
#define __HOST_PORTAL_H_

#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <apr_hash.h>
#include <apr_pools.h>
#include <apr_time.h>
#include <apr_thread_proc.h>
#include <apr_thread_mutex.h>
#include <apr_thread_cond.h>
#include "fmttypes.h"
#include "network.h"
#include "opque.h"

#ifdef __cplusplus
extern "C" {
#endif

#define HP_COMPACT_TIME 10   //** How often to run the garbage collector
#define HP_HOSTPORT_SEPARATOR "|"


typedef struct {       //** Contains information about the depot including all connections
  char skey[512];         //** Search key used for lookups its "host:port:type:..." Same as for the op
  char host[512];         //** Hostname
  int port;               //** port
  int invalid_host;       //** Flag that this host is not resolvable
  int64_t workload;       //** Amount of work left in the feeder que
  int64_t executing_workload;   //** Amount of work in the executing queues
  int64_t cmds_processed; //** Number of commands processed
  int failed_conn_attempts;     //** Failed net_connects()
  int successful_conn_attempts; //** Successful net_connects()
  int abort_conn_attempts; //** IF this many failed connection requests occur in a row we abort
  int n_conn;             //** Number of current depot connections
  int stable_conn;        //** Last count of "stable" connections
  int max_conn;           //** Max allowed connections, normally global_config->max_threads
  int min_conn;           //** Max allowed connections, normally global_config->min_threads
  int sleeping_conn;      //** Connections currently sleeping due to a depot load error
  apr_time_t pause_until;     //** Forces the system to wait, if needed, before making new conn
  Stack_t *conn_list;     //** List of connections
  Stack_t *que;           //** Task que
  Stack_t *closed_que;    //** List of closed but not reaped connections
  Stack_t *direct_list;     //** List of dedicated dportal/dc for the traditional direct execution calls
  apr_thread_mutex_t *lock;  //** shared lock
  apr_thread_cond_t *cond;
  apr_pool_t *mpool;
  void *connect_context;   //** Private information needed to make a host connection
  portal_context_t *context;  //** Specific portal implementaion
} host_portal_t;

typedef struct {            //** Individual depot connection in conn_list
   int cmd_count;
   int curr_workload;
   int shutdown_request;
   int net_connect_status;
   int start_stable;
   apr_time_t last_used;          //** Time the last command completed
   NetStream_t *ns;           //** Socket
   Stack_t *pending_stack;    //** Local task que. An op  is mpoved from the parent que to here
   Stack_ele_t *my_pos;       //** My position int the dp conn list
   op_generic_t *curr_op;   //** Sending phase op that could have failed
   host_portal_t *hp;         //** Pointerto parent depot portal with the todo list
   apr_thread_mutex_t *lock;      //** shared lock
   apr_thread_cond_t *send_cond;
   apr_thread_cond_t *recv_cond;
   apr_thread_t *send_thread; //** Sending thread
   apr_thread_t *recv_thread; //** recving thread
   apr_pool_t   *mpool;       //** MEmory pool for
} host_connection_t;



extern Net_timeout_t global_dt;

//** Routines from hportal.c
#define hportal_trylock(hp)   apr_thread_mutex_trylock(hp->lock)
#define hportal_lock(hp)   apr_thread_mutex_lock(hp->lock)
#define hportal_unlock(hp) apr_thread_mutex_unlock(hp->lock)
#define hportal_signal(hp) apr_thread_cond_broadcast(hp->cond)

op_generic_t *_get_hportal_op(host_portal_t *hp);
void hportal_wait(host_portal_t *hp, int dt);
int get_hpc_thread_count(portal_context_t *hpc);
void modify_hpc_thread_count(portal_context_t *hpc, int n);
host_portal_t *create_hportal(portal_context_t *hpc, void *connect_context, char *hostport, int min_conn, int max_conn);
portal_context_t *create_hportal_context(portal_fn_t *hpi);
void destroy_hportal_context(portal_context_t *hpc);
void finalize_hportal_context(portal_context_t *hpc);
void shutdown_hportal(portal_context_t *hpc);
void compact_dportals(portal_context_t *hpc);
void change_all_hportal_conn(portal_context_t *hpc, int min_conn, int max_conn);
void _hp_fail_tasks(host_portal_t *hp, op_status_t err_code);
void check_hportal_connections(host_portal_t *hp);
int submit_hp_direct_op(portal_context_t *hpc, op_generic_t *op);
int submit_hportal(host_portal_t *dp, op_generic_t *op, int addtotop);
int submit_hp_que_op(portal_context_t *hpc, op_generic_t *op);

//** Routines for hconnection.c
#define trylock_hc(a) apr_thread_mutex_trylock(a->lock)
#define lock_hc(a) apr_thread_mutex_lock(a->lock)
#define unlock_hc(a) apr_thread_mutex_unlock(a->lock)
#define hc_send_signal(hc) apr_thread_cond_signal(hc->send_cond)
#define hc_recv_signal(hc) apr_thread_cond_signal(hc->recv_cond)

host_connection_t *new_host_connection();
void destroy_host_connection(host_connection_t *hc);
void close_hc(host_connection_t *dc);
int create_host_connection(host_portal_t *hp);

#ifdef __cplusplus
}
#endif

#endif

