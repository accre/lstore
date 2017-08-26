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

#ifndef __HOST_PORTAL_H_
#define __HOST_PORTAL_H_

#include <apr_hash.h>
#include <apr_pools.h>
#include <apr_thread_cond.h>
#include <apr_thread_mutex.h>
#include <apr_thread_proc.h>
#include <apr_time.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/atomic_counter.h>
#include <tbx/fmttypes.h>
#include <tbx/network.h>

#include "gop.h"
#include "gop/hp.h"
#include "gop/visibility.h"

#ifdef __cplusplus
extern "C" {
#endif

#define HP_COMPACT_TIME 10   //** How often to run the garbage collector

struct gop_host_portal_t {       //** Contains information about the depot including all connections
    char skey[512];         //** Search key used for lookups its "host:port:type:..." Same as for the op
    char host[512];         //** Hostname
    int oops_neg;           //** All the oops are just for tracking down a bug and should be removed once it's found and fixed.
    int oops_check;
    int oops_recv_start;
    int oops_recv_end;
    int oops_send_start;
    int oops_send_end;
    int oops_spawn;
    int oops_spawn_send_err;
    int oops_spawn_recv_err;
    int oops_spawn_retry;
    int port;               //** port
    int invalid_host;       //** Flag that this host is not resolvable
    int64_t workload;       //** Amount of work left in the feeder que
    int64_t executing_workload;   //** Amount of work in the executing queues
    int64_t cmds_processed; //** Number of commands processed
    int64_t n_coalesced;     //** Number of commands merged together
    int failed_conn_attempts;     //** Failed net_connects()
    int successful_conn_attempts; //** Successful net_connects()
    int abort_conn_attempts; //** IF this many failed connection requests occur in a row we abort
    int n_conn;             //** Number of current depot connections
    int stable_conn;        //** Last count of "stable" connections
    int max_conn;           //** Max allowed connections, normally global_config->max_threads
    int min_conn;           //** Max allowed connections, normally global_config->min_threads
    int sleeping_conn;      //** Connections currently sleeping due to a depot load error
    int closing_conn;       //** Connetions currently being closed
    tbx_atomic_unit32_t idle_conn; //** Number of idle connections
    apr_time_t pause_until;     //** Forces the system to wait, if needed, before making new conn
    apr_time_t dt_connect;  //** Max time to wait when initiating a connection
    tbx_stack_t *conn_list;     //** List of connections
    tbx_stack_t *que;           //** Task que
    tbx_stack_t *closed_que;    //** List of closed but not reaped connections
    tbx_stack_t *direct_list;     //** List of dedicated dportal/dc for the traditional direct execution calls
    apr_thread_mutex_t *lock;  //** shared lock
    apr_thread_cond_t *cond;
    apr_pool_t *mpool;
    void *connect_context;   //** Private information needed to make a host connection
    gop_portal_context_t *context;  //** Specific portal implementaion
};

struct gop_host_connection_t {            //** Individual depot connection in conn_list
    int recv_up;
    int cmd_count;
    int curr_workload;
    int shutdown_request;
    int net_connect_status;
    int start_stable;
    int send_down;
    int closing;
    apr_time_t last_used;          //** Time the last command completed
    tbx_ns_t *ns;           //** Socket
    tbx_stack_t *pending_stack;    //** Local task que. An op  is mpoved from the parent que to here
    tbx_stack_ele_t *my_pos;       //** My position int the dp conn list
    gop_op_generic_t *curr_op;   //** Sending phase op that could have failed
    gop_host_portal_t *hp;         //** Pointerto parent depot portal with the todo list
    apr_thread_mutex_t *lock;      //** shared lock
    apr_thread_cond_t *send_cond;
    apr_thread_cond_t *recv_cond;
    apr_thread_t *send_thread; //** Sending thread
    apr_thread_t *recv_thread; //** recving thread
    apr_pool_t   *mpool;       //** MEmory pool for
};


//** Routines from hportal.c
#define hportal_trylock(hp)   apr_thread_mutex_trylock(hp->lock)
#define hportal_lock(hp)   apr_thread_mutex_lock(hp->lock)
#define hportal_unlock(hp) apr_thread_mutex_unlock(hp->lock)
#define hportal_signal(hp) apr_thread_cond_broadcast(hp->cond)

void _reap_hportal(gop_host_portal_t *hp, int quick);
gop_op_generic_t *_get_hportal_op(gop_host_portal_t *hp);
void hportal_wait(gop_host_portal_t *hp, int dt);
int get_hpc_thread_count(gop_portal_context_t *hpc);
void modify_hpc_thread_count(gop_portal_context_t *hpc, int n);
gop_host_portal_t *create_hportal(gop_portal_context_t *hpc, void *connect_context, char *hostport, int min_conn, int max_conn, apr_time_t dt_connect);
void finalize_hportal_context(gop_portal_context_t *hpc);
void compact_dportals(gop_portal_context_t *hpc);
void _hp_fail_tasks(gop_host_portal_t *hp, gop_op_status_t err_code);
void check_hportal_connections(gop_host_portal_t *hp);

//** Routines for hconnection.c
#define trylock_hc(a) apr_thread_mutex_trylock(a->lock)
#define lock_hc(a) apr_thread_mutex_lock(a->lock)
#define unlock_hc(a) apr_thread_mutex_unlock(a->lock)
#define hc_send_signal(hc) apr_thread_cond_signal(hc->send_cond)
#define hc_recv_signal(hc) apr_thread_cond_signal(hc->recv_cond)

gop_host_connection_t *new_host_connection();
void destroy_host_connection(gop_host_connection_t *hc);
void close_hc(gop_host_connection_t *dc, int quick);
int create_host_connection(gop_host_portal_t *hp);

#ifdef __cplusplus
}
#endif

#endif


