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

#ifndef ACCRE_PORTAL_H_INCLUDED
#define ACCRE_PORTAL_H_INCLUDED


#include <apr_hash.h>
#include <apr_thread_cond.h>
#include <apr_thread_mutex.h>
#include <apr_thread_proc.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/apr_wrapper.h>
#include <tbx/assert_result.h>
#include <tbx/atomic_counter.h>
#include <tbx/log.h>
#include <tbx/network.h>
#include <tbx/pigeon_coop.h>
#include <tbx/stack.h>
#include <tbx/type_malloc.h>
#include "types.h"
#include "gop.h"

#ifdef __cplusplus
extern "C" {
#endif
// Types
// Forward declarations
struct portal_fn_t {  //** Hportal specific implementation
    void *(*dup_connect_context)(void *connect_context);  //** Duplicates a ccon
    void (*destroy_connect_context)(void *connect_context);
    int (*connect)(tbx_ns_t *ns, void *connect_context, char *host, int port, tbx_ns_timeout_t timeout);
    void (*close_connection)(tbx_ns_t *ns);
    void (*sort_tasks)(void *arg, opque_t *q);        //** optional
    void (*submit)(void *arg, op_generic_t *op);
    void (*sync_exec)(void *arg, op_generic_t *op);   //** optional
};

struct portal_context_t {             //** Handle for maintaining all the ecopy connections
    apr_thread_mutex_t *lock;
    apr_hash_t *table;         //** Table containing the depot_portal structs
    apr_pool_t *pool;          //** Memory pool for hash table
    apr_time_t min_idle;       //** Idle time before closing connection
    tbx_atomic_unit32_t running_threads;       //** currently running # of connections
    int max_connections;       //** Max aggregate allowed number of threads
    int min_threads;           //** Max allowed number of threads/host
    int max_threads;           //** Max allowed number of threads/host
    apr_time_t dt_connect;     //** Max time to wait when making a connection to a host
    int max_wait;              //** Max time to wait on a retry_dead_socket
    int64_t max_workload;      //** Max allowed workload before spawning another connection
    int compact_interval;      //** Interval between garbage collections calls
    int wait_stable_time;      //** time to wait before adding connections for unstable hosts
    int abort_conn_attempts;   //** If this many failed connection requests occur in a row we abort
    int check_connection_interval; //** Max time to wait for a thread to check for a close
    int max_retry;             //** Default max number of times to retry an op
    int count;                 //** Internal Counter
    apr_time_t   next_check;       //** Time for next compact_dportal call
    tbx_ns_timeout_t dt;          //** Default wait time
    void *arg;
    portal_fn_t *fn;       //** Actual implementaion for application
};


#ifdef __cplusplus
}
#endif


#endif
