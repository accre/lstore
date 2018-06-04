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

#define _log_module_index 125

#include <apr_hash.h>
#include <apr_time.h>
#include <apr_thread_proc.h>
#include <apr_thread_mutex.h>
#include <apr_thread_cond.h>
#include <gop/gop.h>
#include <gop/opque.h>
#include <gop/portal.h>
#include <tbx/apr_wrapper.h>
#include <tbx/atomic_counter.h>
#include <tbx/fmttypes.h>
#include <tbx/network.h>
#include <tbx/que.h>
#include <tbx/siginfo.h>
#include <tbx/stack.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>

typedef struct hconn_t hconn_t;
typedef struct hportal_t hportal_t;

//** HPC operations
#define HPC_CMD_GOP       0    //** Normal GOP command to process
#define HPC_CMD_SHUTDOWN  1    //** Shutdown command
#define HPC_CMD_STATS     2    //** Dump the stats

typedef struct {
    int cmd;
    gop_op_generic_t *gop;
} hpc_cmd_t;

//** Responses from a connection
#define CONN_EMPTY         0   //** NULL op
#define CONN_CLOSE         1   //** Close connection request (sent to conn)
#define CONN_CLOSE_REQUEST 2   //** Connection is requesting to close (from conn)
#define CONN_CLOSED        3   //** Connection is closed (from conn)
#define CONN_READY         4   //** Connection is ready for commands (from conn)
#define CONN_GOP_SUBMIT    5   //** GOP task (to conn)
#define CONN_GOP_RETRY     6   //** GOP should be retried (from conn)
#define CONN_GOP_DONE      7   //** GOP has been processed (from conn)

typedef struct {
    int cmd;
    hconn_t *hc;
    void *ptr;
    int64_t gop_workload;
    apr_time_t gop_dt;
    gop_op_status_t status;
} conn_cmd_t;

typedef struct gop_portal_context_t gop_portal_context_t;

struct hconn_t {     //** Host connection structure
    hportal_t *hp;          //** Host portal
    tbx_ns_t *ns;           //** Network connection
    tbx_que_t *incoming;    //** Incoming communication que
    tbx_que_t *internal;    //** Internal communication que for messages between send->recv threads in a conn
    tbx_que_t *outgoing;    //** Result communication que
    tbx_stack_ele_t *ele;   //** My position in the conn_list
    apr_thread_t *send_thread; //** Connection sending thread
    apr_thread_t *recv_thread; //** Connection receiving thread
    int64_t cmds_processed; //** Number of commands processed
    int64_t workload;       //** Current task workload
    int64_t ntasks;         //** Current # of tasks
    int64_t gop_last_workload; //** Last processed GOP workload
    apr_time_t gop_last_dt;    //** Processing time for last GOP
    int state;              //** Connection state 0=startup, 1=ready, 2=closing
};

struct hportal_t {   //** Host portal container
    char *skey;          //** Search key used for lookups its "host:port:type:..." Same as for the op
    char *host;          //** Hostname
    apr_time_t pause_until;  //** Wait until this time before updating stable connections
    int64_t n_coalesced;     //** Number of commands merged together
    int64_t cmds_processed;  //** Number of commands processed
    int64_t cmds_submitted;  //** # of tasks submitted
    int64_t workload_pending;//** Amount of work on the pending que
    int64_t workload_executing;//** Amount of work being processed by the connections
    int64_t avg_workload;      //** Running average command workload
    int64_t avg_dt;            //** Running average time;
    int port;                  //** Port to connect to
    apr_time_t dead;           //** Dead host if non-zero
    int limbo_conn;            //** Number of connections spawned but not yet connected.
    int pending_conn;          //** This is the number of outstanding connections we are waiting to close from ANY HP
    int stable_conn;           //** Number of stable connections
    tbx_stack_t *conn_list;  //** List of connections
    tbx_stack_t *pending;    //** List of pending commands
    void *connect_context;   //** Private information needed to make a host connection
    gop_portal_context_t *hpc;  //** Specific portal implementaion
    double avg_bw;              //** Avg bandwidth as calculated by determine_bandwidths()
};

struct gop_portal_context_t {             //** Handle for maintaining all the ecopy connections
    char *name;                //** Identifier for logging
    apr_thread_t *main_thread; //** Main processing thread
    apr_hash_t *hp;            //** Table containing the hportal_t structs
    apr_pool_t *pool;          //** Memory pool for hash table
    apr_thread_mutex_t *todo_lock;  //** Lock for tracking work
    apr_thread_cond_t *todo_cond;   //** Corresponding condition
    tbx_que_t *gop_que;        //** Incoming operations que
    tbx_que_t *results_que;    //**  Results que from Hconn
    apr_time_t max_idle;       //** Idle time before closing connection
    apr_time_t wait_stable_time; //** Time to wait between stable connection checks
    double mix_latest_fraction;   //** Amount of the running average that comes from the new
    double min_bw_fraction;    //** The minimum bandwidth compared to the median depot to keep a connection alive
    int todo_waiting;          //** Flag that someone is waiting
    int running_conn;          //** currently running # of connections
    int max_total_conn;        //** Max aggregate allowed number of connections
    int min_conn;              //** Min allowed number of connections to a host
    int max_conn;              //** Max allowed number of connections to a host
    apr_time_t dt_connect;     //** Max time to wait when making a connection to a host
    apr_time_t dt_dead_timeout; //** How long to keep a connection as dead before trying again
    apr_time_t dt_dead_check;  //** Check interval between host health checks
    int64_t max_workload;      //** Max allowed workload before spawning another connection
    tbx_atomic_int_t dump_running; //** Dump stats running if = 1

    void *arg;
    gop_portal_fn_t *fn;       //** Actual implementaion for application
};

int process_results(gop_portal_context_t *hpc);
hconn_t *hconn_new(hportal_t *hp, tbx_que_t *outgoing, apr_pool_t *mpool);
void hconn_add(hportal_t *hp, hconn_t *hc);
void hconn_destroy(hconn_t *hc);
hportal_t *hp_create(gop_portal_context_t *hpc, char *id);
void hp_destroy(hportal_t *hp);

//***** NOTE: The hpc_notify/hpc_wait routines are NOT foolproof! Requiring  ******
//*****       them to be would require nesting locks and hurt performance.   ******
//*****       Instead the main loop has a short time timeout to compensate.  ******

//************************************************************************
// hpc_notify - Flags the HPC thead that there is something to do
//************************************************************************

void hpc_notify(gop_portal_context_t *hpc)
{
    apr_thread_mutex_lock(hpc->todo_lock);
log_printf(15, "hp=%s todo_waiting=%d\n", hpc->name, hpc->todo_waiting);
    if (hpc->todo_waiting == 0) {
        hpc->todo_waiting = 1;
        apr_thread_cond_signal(hpc->todo_cond);
    }
    apr_thread_mutex_unlock(hpc->todo_lock);
}

//************************************************************************
// hpc_wait - Pauses the main HPC thead until there is something to do
//    or it times out
//************************************************************************

void hpc_wait(gop_portal_context_t *hpc, apr_time_t dt)
{
log_printf(15, "hp=%s START\n", hpc->name);
    if (tbx_que_get(hpc->gop_que, NULL, 0) == 0) return;
    if (tbx_que_get(hpc->results_que, NULL, 0) == 0) return;

log_printf(15, "hp=%s WAITING\n", hpc->name);

    //** Nothing to do so wait
    apr_thread_mutex_lock(hpc->todo_lock);
    if (hpc->todo_waiting == 0) {
        apr_thread_cond_timedwait(hpc->todo_cond, hpc->todo_lock, dt);
    }
    hpc->todo_waiting = 0;
    apr_thread_mutex_unlock(hpc->todo_lock);
log_printf(15, "hp=%s END\n", hpc->name);

}

//************************************************************************
//  hportal_siginfo_handler - Prints the status of all the connections
//      This isn't the most elegant foolproof way to not gauarantee you
//      don't have multiple dumps running but based on our use case this
//      is for the most part foolproof.
//************************************************************************

void hportal_siginfo_handler(void *arg, FILE *fd)
{
    gop_portal_context_t *hpc = (gop_portal_context_t *)arg;
    hpc_cmd_t cmd;

    tbx_atomic_set(hpc->dump_running, 1);  //** Flag that it's running

    //** Send the command to dump
    cmd.cmd = HPC_CMD_STATS;
    cmd.gop = (gop_op_generic_t *)fd;
    tbx_que_put(hpc->gop_que, &cmd, TBX_QUE_BLOCK);
    hpc_notify(hpc);

    //** Wait for it to finish
    apr_sleep(apr_time_as_msec(10));
    while (tbx_atomic_get(hpc->dump_running) != 0) {
        apr_sleep(apr_time_as_msec(100));
    }
}

//************************************************************************
//  submit_shutdown - Tell all the connections to close
//************************************************************************

int submit_shutdown(gop_portal_context_t *hpc)
{
    hportal_t *hp;
    hconn_t *hc;
    apr_hash_index_t *hi;
    int n;
    apr_time_t dt;
    conn_cmd_t cmd;

    memset(&cmd, 0, sizeof(cmd));
    cmd.cmd = CONN_CLOSE;
    dt = apr_time_from_sec(10);
    n = 0;
    for (hi=apr_hash_first(hpc->pool, hpc->hp); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, NULL, (void **)&hp);
	    for (hc = tbx_stack_top_first(hp->conn_list); hc != NULL; hc = tbx_stack_next_down(hp->conn_list)) {
            if (hc->state == 1) {
                n++;
                hc->state = 2;
                tbx_que_put(hc->incoming, &cmd, dt);
            }
        }
    }
    return(n);
}


//************************************************************************
//  find_conn_to_close - Find a connection to close
//************************************************************************

hconn_t *find_conn_to_close(gop_portal_context_t *hpc)
{
    apr_hash_index_t *hi;
    hportal_t *hp;
    hconn_t *best_hc, *hc;
    int64_t best_workload;

    best_hc = NULL;
    best_workload = hpc->max_workload + 1;
    for (hi=apr_hash_first(hpc->pool, hpc->hp); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, NULL, (void **)&hp);
	    for (hc = tbx_stack_top_first(hp->conn_list); hc != NULL; hc = tbx_stack_next_down(hp->conn_list)) {
            if ((hc->state == 1) && (best_workload > hc->workload)) {
                best_workload = hc->workload;
                best_hc = hc;
            }
        }
    }

    return(best_hc);
}

//************************************************************************
//  hpc_close_connetions - Closes connection on the request of the given hp.
//************************************************************************

void hpc_close_connections(gop_portal_context_t *hpc, hportal_t *hp, int n_close)
{
    int i;
    hconn_t * hc;
    apr_time_t dt;
    conn_cmd_t cmd;

    memset(&cmd, 0, sizeof(cmd));
    cmd.cmd = CONN_CLOSE;
    cmd.ptr = hp;

    dt = apr_time_from_sec(10);
    for (i=0; i<n_close; i++) {
        hc = find_conn_to_close(hpc);
        hc->state = 1;

        tbx_que_put(hc->incoming, &cmd, dt);
    }
}

//************************************************************************
// wair_for_shutdown - Shuts down all the connections
//************************************************************************

void wait_for_shutdown(gop_portal_context_t *hpc, int n)
{
    int nclosed;

    nclosed = process_results(hpc);
    while (nclosed < n) {
        nclosed += process_results(hpc);
    }
}

// *************************************************************************
//  hp_compare - Sort comparison function
// *************************************************************************

static int hp_compare(const void *p1, const void *p2)
{
    hportal_t *hp1, *hp2;
    double bw1, bw2;

    hp1 = *(hportal_t **)p1;
    hp2 = *(hportal_t **)p2;

    bw1 = hp1->avg_dt;
    bw1 = (1.0*hp1->avg_workload * apr_time_from_sec(1)) / bw1;
    bw2 = hp2->avg_dt;
    bw2 = (1.0*hp2->avg_workload * apr_time_from_sec(1)) / bw2;


    if (bw1 < bw2) {
        return(-1);
    } else if (bw1 == bw2) {
        return(0);
    }

    return(1);
}

//************************************************************************
// determine_bandwidths - Determine the various HP bandwidths and optionally
//     return the min, max, and median HP's. The average BW is returned

//************************************************************************

double determine_bandwidths(gop_portal_context_t *hpc, hportal_t **hp_min, hportal_t **hp_median, hportal_t **hp_max)
{
    hportal_t **array;
    apr_hash_index_t *hi;
    hportal_t *hp;
    double avg_bw, total_avg_bw;
    int n, i;

    n = apr_hash_count(hpc->hp);
    if (n == 0) {
        if (hp_min) *hp_min = NULL;
        if (hp_median) *hp_median = NULL;
        if (hp_max) *hp_max = NULL;
        return(0.0);
    }

    tbx_type_malloc(array, hportal_t *, n);

    //** Fill the array
    total_avg_bw = 0;
    n = 0;
    for (hi=apr_hash_first(hpc->pool, hpc->hp); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, NULL, (void **)&hp);
        if (hp->dead != 0) continue;

        array[n] = hp;
        avg_bw = hp->avg_dt;
        avg_bw = (1.0*hp->avg_workload * apr_time_from_sec(1)) / avg_bw;
        hp->avg_bw = avg_bw;
        total_avg_bw += avg_bw;
        n++;
    }

    //** Sort it
    qsort(array, n, sizeof(hportal_t *), hp_compare);

    //** Return as needed
    i = n / 2;
    total_avg_bw = total_avg_bw / n;  //** Normalize the BW
    if (hp_min) *hp_min = array[0];
    if (hp_median) *hp_median = array[i];
    if (hp_max) *hp_max = array[n-1];

    free(array);  //** Clean up
    return(total_avg_bw);
}

//************************************************************************
// dump_stats - Dumps the stats
//************************************************************************

void dump_stats(gop_portal_context_t *hpc, gop_op_generic_t *gop)
{
    FILE *fd = (FILE *)gop;
    apr_hash_index_t *hi;
    hportal_t *hp;
    hportal_t *hp_range[3];
    char *label[3] = {"HP Min   ", "HP Median", "HP Max   "};
    hconn_t *hc;
    double avg_bw, total_avg_bw;
    apr_time_t total_avg_dt;
    int64_t total_avg_wl;
    char ppbuf1[100];
    char ppbuf2[100];
    char ppbuf3[100];
    char ppbuf4[100];
    int i, n_hp, n_hc;

    total_avg_bw = 0;
    total_avg_dt = 0;
    total_avg_wl = 0;
    n_hp = n_hc = 0;
    fprintf(fd, "Host Portal info (%s) -------------------------------------------\n", hpc->name);
    determine_bandwidths(hpc, &hp_range[0], &hp_range[1], &hp_range[2]);

    for (hi=apr_hash_first(hpc->pool, hpc->hp); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, NULL, (void **)&hp);

        fprintf(fd, "    Host: %s\n", hp->skey);
        fprintf(fd, "        Workload - pending: %s executing: %s,  Commands processed: " I64T "  Tasks Queued: %d  Merged Commands: " I64T "\n",
            tbx_stk_pretty_print_double_with_scale(1024, hp->workload_pending, ppbuf1),
            tbx_stk_pretty_print_double_with_scale(1024, hp->workload_executing, ppbuf2),
            hp->cmds_processed, tbx_stack_count(hp->pending), hp->n_coalesced);

        fprintf(fd, "        Conn: %d  Stable: %d  Pending: %d  Limbo: %d  Dead: %d  -- Avg  Workload: %s DT: %s Bandwidth: %s/s\n",
            tbx_stack_count(hp->conn_list), hp->stable_conn, hp->pending_conn, hp->limbo_conn, (hp->dead > 0) ? 1 : 0,
            tbx_stk_pretty_print_double_with_scale(1024, hp->avg_workload, ppbuf1), tbx_stk_pretty_print_time(hp->avg_dt, 0, ppbuf2),
            tbx_stk_pretty_print_double_with_scale(1024, hp->avg_bw, ppbuf3));

        n_hp++;
        total_avg_bw += hp->avg_bw;
        total_avg_dt += hp->avg_dt;
        total_avg_wl += hp->avg_workload;

        for (hc = tbx_stack_top_first(hp->conn_list), i=0; hc != NULL; hc = tbx_stack_next_down(hp->conn_list), i++) {
            n_hc++;
            fprintf(fd, "        %d --  Commands processed: " I64T "  Workload: %s  Pending: " I64T "  State: %d -- GOP: DT: %s  Workload: %s\n",
                 i, hc->cmds_processed, tbx_stk_pretty_print_double_with_scale(1024, hc->workload, ppbuf1), hc->ntasks, hc->state,
                 tbx_stk_pretty_print_time(hc->gop_last_dt, 0, ppbuf2), tbx_stk_pretty_print_double_with_scale(1024, hc->gop_last_workload, ppbuf3));
        }
    }

    if (n_hp != 0) {
        avg_bw = n_hp * total_avg_dt;
        avg_bw = (1.0*total_avg_wl * apr_time_from_sec(1)) / avg_bw;
        total_avg_wl = total_avg_wl / n_hp;
        total_avg_dt = total_avg_dt / n_hp;
        total_avg_bw = total_avg_bw / n_hp;
        fprintf(fd, "-------  HP Average ------- HP: %d  HC: %d DT: %s  Workload: %s  Bandwidth: %s/s  SUM(WL)/SUM(DT)/N_HP: %s/s\n",
            n_hp, n_hc, tbx_stk_pretty_print_time(total_avg_dt, 0, ppbuf1), tbx_stk_pretty_print_double_with_scale(1024, total_avg_wl, ppbuf2),
            tbx_stk_pretty_print_double_with_scale(1024, total_avg_bw, ppbuf3),
            tbx_stk_pretty_print_double_with_scale(1024, avg_bw, ppbuf4));

        for (i=0; i<3; i++) {
            hp = hp_range[i];
            fprintf(fd, "------- %s ------- Host: %s  DT: %s  Workload: %s  Bandwidth: %s/s\n",
                label[i], hp->skey, tbx_stk_pretty_print_time(hp->avg_dt, 0, ppbuf1), tbx_stk_pretty_print_double_with_scale(1024, hp->avg_workload, ppbuf2),
                tbx_stk_pretty_print_double_with_scale(1024, hp->avg_bw, ppbuf3));
        }
    }

    fprintf(fd, "\n");

    tbx_atomic_set(hpc->dump_running, 0);  //** Signal that we are finished
}

//************************************************************************
// route_gop - Routes the gop to the appropriate HP que
//************************************************************************

void route_gop(gop_portal_context_t *hpc, gop_op_generic_t *gop)
{
    gop_command_op_t *hop = &(gop->op->cmd);
    tbx_stack_ele_t *ele;
    hportal_t *hp;

    hp = apr_hash_get(hpc->hp, hop->hostport, APR_HASH_KEY_STRING);
    if (hp == NULL) {
        hp = hp_create(hpc, hop->hostport);
        apr_hash_set(hpc->hp, hp->skey, APR_HASH_KEY_STRING, (const void *)hp);
    }
    hp->workload_pending += hop->workload;
    tbx_stack_push(hp->pending, gop);

log_printf(15, "hp=%s gid=%d pending=%d workload=" I64T "\n", hp->skey, gop_id(gop), tbx_stack_count(hp->pending), hp->workload_pending);
    if (hop->on_submit) {
        ele = tbx_stack_get_current_ptr(hp->pending);
        hop->on_submit(hp->pending, ele);
    }

}

//************************************************************************
// fetch_tasks - Gets all the new incoming tasks and places them on
//    appropriate queues
//************************************************************************

int fetch_tasks(gop_portal_context_t *hpc)
{
    int finished;
    hpc_cmd_t cmd;

    finished = 0;
    while (tbx_que_get(hpc->gop_que, &cmd, 0) == 0) {
log_printf(15, "hpc=%s LOOP\n", hpc->name);

        switch (cmd.cmd) {
            case HPC_CMD_SHUTDOWN:
                finished = 1;
log_printf(15, "hpc=%s HPC_CMD_SHUTDOWN\n", hpc->name);
                break;
            case HPC_CMD_STATS:
                dump_stats(hpc, cmd.gop);
                break;
            default:
                route_gop(hpc, cmd.gop);
        }
    }

log_printf(15, "hpc=%s END finished=%d\n", hpc->name, finished);

    return(finished);
}

//************************************************************************
// check_hportal_connections - Adds connections if needed to the HP
//     based on the workload.
//************************************************************************

void check_hportal_connections(gop_portal_context_t *hpc, hportal_t *hp)
{
    int total_workload, i, n, nconn, hpconn, nshort;

    hpconn = tbx_stack_count(hp->conn_list);

    if (hp->dead > 0) return;  //** Nothing to do. Still in timeout

    //** Check if we need to add connections
    total_workload = hp->workload_pending + hp->workload_executing;
    nconn = hpconn + hp->pending_conn;
    n = (nconn > 0) ? total_workload / nconn : 1;
log_printf(20, "n=%d\n", n);
log_printf(20, "max_conn=%d\n", hpc->max_conn);

    if (n > hpc->max_conn) n = hpc->max_conn;
    n = (hpconn > n) ? 0 : n - hpconn;
    if ((n+hpconn) > hpc->max_total_conn) {
        nshort = n + hpconn - hpc->max_total_conn - hp->pending_conn;
        if (nshort > 0) {
            hpc_close_connections(hpc, hp, nshort);
            hp->pending_conn += nshort;
        }
        n = hpc->max_total_conn - hpc->running_conn;
    }

    nconn = n + hpconn + hp->pending_conn;
    if (nconn > hp->stable_conn) {
        if (apr_time_now() > hp->pause_until) {
            hp->stable_conn++;
            hp->pause_until = apr_time_now();
            if (hp->stable_conn > hpc->max_total_conn) {
                hp->stable_conn = hpc->max_total_conn;
                n = 0;
            } else if (hp->stable_conn == 0) {
                hp->stable_conn = 1;
                n = (hp->pause_until == 0) ? 1 : 0;
            } else {
                n = (hpconn < hpc->max_total_conn) ? 1 : 0;
                hp->pause_until = apr_time_now() + hpc->wait_stable_time;
            }
        } else {
            if (hpconn > 0) {
                n = 0;
            } else if (hp->pause_until == 0) {
                n = 1;
            }
        }
    }

    for (i=0; i<n; i++) {
        hconn_add(hp, hconn_new(hp, hpc->results_que, hpc->pool));
    }
}

//*************************************************************************
// hp_fail_all_tasks - Fails all the tasks for a depot.
//       Only used when a depot is dead
//       NOTE:  No locking is done!
//*************************************************************************

void hp_fail_all_tasks(hportal_t *hp, gop_op_status_t err_code)
{
    gop_op_generic_t *hsop;

    //** Use the hportal_get_op() To make sure we handle any coalescing
    while ((hsop = tbx_stack_pop(hp->pending)) != NULL) {
        gop_mark_completed(hsop, err_code);
    }
    hp->workload_pending = 0;

}

//*************************************************************************
//  hp_gop_retry - Either retries the GOP or fails it if needed
//*************************************************************************

void hp_gop_retry(hconn_t *hc, gop_op_generic_t *gop)
{
    conn_cmd_t cmd;

    gop->op->cmd.retry_count--;
    if (gop->op->cmd.retry_count <= 0) {  //** No more retries left so fail the gop
        cmd.gop_workload = gop->op->cmd.workload;
        cmd.gop_dt = 0;
        cmd.hc = hc;
        cmd.ptr = gop;
        cmd.cmd = CONN_GOP_DONE;
        gop_mark_completed(gop, gop_failure_status);
        tbx_que_put(hc->outgoing, &cmd, TBX_QUE_BLOCK);
        hpc_notify(hc->hp->hpc);
    } else {  //** Got a retry
        cmd.gop_workload = gop->op->cmd.workload;
        cmd.hc = hc;
        cmd.ptr = gop;
        cmd.cmd = CONN_GOP_RETRY;
        tbx_que_put(hc->outgoing, &cmd, TBX_QUE_BLOCK);
        hpc_notify(hc->hp->hpc);
    }
}

//************************************************************************
// hconn_least_busy - Returns the least busy connection
//************************************************************************

hconn_t *hconn_least_busy(gop_portal_context_t *hpc, hportal_t *hp, int ideal_conn)
{
    hconn_t *hc, *best_hc;
    int i, best_load;

    best_load = hpc->max_workload+1;
    best_hc = NULL;
log_printf(15, "START hp=%s\n", hp->skey);
    for (hc = tbx_stack_top_first(hp->conn_list), i=0; (hc != NULL) && (i<ideal_conn); hc = tbx_stack_next_down(hp->conn_list)) {
        if (hc->state == 1) {
            i++;
            if ((hc->workload < best_load) || (best_hc == NULL)) {
                best_load = hc->workload;
                best_hc = hc;
            }
        }
    }

log_printf(15, "END hp=%s c=%p\n", hp->skey, best_hc);

    return(best_hc);
}

//************************************************************************
// hp_submit_tasks - Submits the task to the hconn for execution
//************************************************************************

int hp_submit_tasks(gop_portal_context_t *hpc, hportal_t *hp)
{
    gop_op_generic_t *gop;
    hconn_t *c;
    conn_cmd_t cmd;
    int ideal_conn;
    int workload;

log_printf(15, "hp=%s pending=%d\n", hp->skey, tbx_stack_count(hp->pending));
    //** Make sure there is something to do
    if (tbx_stack_count(hp->pending) == 0) return(0);

    //** Adjust the connections if needed
    check_hportal_connections(hpc, hp);

    //** See if all the connections are dead
    if ((hp->dead > 0) && (tbx_stack_count(hp->conn_list) == hp->limbo_conn)) {
        hp_fail_all_tasks(hp, gop_failure_status);  //** and fail everything in the que
        return(0);
    }

    memset(&cmd, 0, sizeof(cmd));

    ideal_conn = (hp->workload_pending + hp->workload_executing) / hpc->max_workload;
    if (ideal_conn == 0) {
        ideal_conn = 1;
    } else if (ideal_conn > hpc->max_conn) {
        ideal_conn = hpc->max_conn;
    }

    //** submit the tasks
    c = hconn_least_busy(hpc, hp, ideal_conn);
log_printf(15, "hp=%s c=%p\n", hp->skey, c);
    if (c == NULL) return(tbx_stack_count(hp->pending));

    cmd.cmd = CONN_GOP_SUBMIT;
    tbx_stack_move_to_bottom(hp->pending);
    while ((gop = tbx_stack_get_current_data(hp->pending)) != NULL) {
        if (c->workload < hpc->max_workload) {
            //** Check if we need to to some command coalescing
            if (gop->op->cmd.before_exec != NULL) {
                hp->n_coalesced += gop->op->cmd.before_exec(gop);
            }

            //** See if we can push the gop onto the connections
log_printf(15, "incoming: hp=%s CONN_GOP_SUBMIT gid=%d\n", hp->skey, gop_id(gop));
            cmd.ptr = gop;
            workload = gop->op->cmd.workload;  //** Snag this because the gop could complete before we finish using it
            if (tbx_que_put(c->incoming, &cmd, apr_time_from_sec(1)) != 0) break;

            //** Managed to push the task so update counters
            c->workload += workload;
            c->ntasks++;
            hp->cmds_submitted++;
            hp->workload_pending -= workload;
            hp->workload_executing += workload;
            tbx_stack_delete_current(hp->pending, 1, 0);
        } else {
            c = hconn_least_busy(hpc, hp, ideal_conn);
            if (c) {
log_printf(15, "hpc=%s hp=%s workload=" I64T " max=" I64T "\n", hpc->name, hp->skey, c->workload, hpc->max_workload);

                if (c->workload > hpc->max_workload) break;
            } else {
log_printf(15, "hpc=%s hp=%s c=NULL\n", hpc->name, hp->skey);
                break;
            }
        }
    }

    return(tbx_stack_count(hp->pending));
}

//************************************************************************
//  handle_closed - Handles closing a connection
//************************************************************************

void handle_closed(gop_portal_context_t *hpc, hconn_t *conn, hportal_t *hp_requested_close)
{
    hportal_t *hp = conn->hp;

log_printf(15, "CONN_CLOSED hp=%s\n", hp->skey);
    if (hp_requested_close != NULL) hp_requested_close->pending_conn--;
    hpc->running_conn--;

    tbx_stack_move_to_ptr(hp->conn_list, conn->ele);
    tbx_stack_delete_current(hp->conn_list, 1, 0);

    if (tbx_stack_count(hp->conn_list) == 0) { //** Last connections so check if we enable dead mode
        if (conn->state == 0) hp->dead = 1;
    }

    hconn_destroy(conn);
}

//************************************************************************
// process_results - Processes the results from all he HP connections.
//    Returns the number of close connections processed.
//************************************************************************

int process_results(gop_portal_context_t *hpc)
{
    int nclosed;
    conn_cmd_t cmd;
    hportal_t *hp;

    nclosed = 0;
    while (!tbx_que_get(hpc->results_que, &cmd, 0)) {
        switch (cmd.cmd) {
            case CONN_READY:
log_printf(15, "CONN_READY hp=%s\n", cmd.hc->hp->skey);
                cmd.hc->state = 1;
                cmd.hc->hp->limbo_conn--;
                break;
            case CONN_CLOSE_REQUEST:
                if (cmd.hc->state == 1) {  //** 1st time so send an offical CLOSE
                    cmd.hc->state = 2;
                    cmd.cmd = CONN_CLOSE;
                    cmd.ptr = NULL;
log_printf(15, "Got a CONN_CLOSE_REQUEST from hp=%s.  Sending official CONN_CLOSE\n", cmd.hc->hp->skey);
                    tbx_que_put(cmd.hc->incoming, &cmd, apr_time_from_sec(100));
                }
                break;
            case CONN_CLOSED:
                hp = cmd.ptr;
                if (cmd.hc->state == 0) cmd.hc->hp->limbo_conn--;  //** Never connected
                handle_closed(hpc, cmd.hc, hp);
                nclosed++;
                break;
            case CONN_GOP_RETRY:
                cmd.hc->ntasks--;
                cmd.hc->workload -= cmd.hc->workload;
                cmd.hc->hp->workload_executing -= cmd.gop_workload;
                cmd.hc->hp->workload_pending += cmd.gop_workload;
                tbx_stack_move_to_bottom(cmd.hc->hp->pending);
                tbx_stack_insert_below(cmd.hc->hp->pending, cmd.ptr);
                break;
            case CONN_GOP_DONE:
                hp = cmd.hc->hp;
                cmd.hc->ntasks--;
                cmd.hc->workload -= cmd.gop_workload;
                cmd.hc->gop_last_workload = cmd.gop_workload;
                cmd.hc->gop_last_dt = cmd.gop_dt;
                hp->avg_workload = (hp->avg_workload != 0) ? hpc->mix_latest_fraction * cmd.gop_workload + (1-hpc->mix_latest_fraction)*hp->avg_workload : cmd.gop_workload;
                hp->avg_dt = (hp->avg_dt != 0) ? hpc->mix_latest_fraction * cmd.gop_dt + (1-hpc->mix_latest_fraction)*hp->avg_dt : cmd.gop_dt;
                cmd.hc->hp->workload_executing -= cmd.gop_workload;
                cmd.hc->hp->cmds_processed++;
                //cmd.hc->hp->dead = 0;
                break;
        }
    }
    return(nclosed);
}

//************************************************************************
// hpc_submit_tasks - Submits all the tasks fetched
//************************************************************************

int hpc_submit_tasks(gop_portal_context_t *hpc)
{
    int ntodo;
    apr_hash_index_t *hi;
    hportal_t *hp;

log_printf(15, "START hpc=%s\n", hpc->name);

    ntodo = 0;
    for (hi=apr_hash_first(hpc->pool, hpc->hp); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, NULL, (void **)&hp);
log_printf(15, "SUBMIT hpc=%s hp=%s\n", hpc->name, hp->skey);

        //** See if we clear the dead flag
        if (hp->dead > 0) {
            if (apr_time_now() > hp->dead) {
                log_printf(10, "CLEARING dead flag for hp=%s\n", hp->skey);
                hp->dead = 0;
                hp->avg_bw = 0;
                hp->avg_workload = 0;
                hp->avg_dt = 0;
            }
        }

        ntodo += hp_submit_tasks(hpc, hp);
    }

log_printf(15, "END hpc=%s todo=%d\n", hpc->name, ntodo);
    return(ntodo);
}

//************************************************************************
// depot_health_check - Checks the health of the depots and marks them as
//     dead if needed.
//************************************************************************

void depot_health_check(gop_portal_context_t *hpc)
{
    hportal_t *hp_min, *hp_median, *hp_max, *hp;
    apr_hash_index_t *hi;
    double min_bw;

    log_printf(15, "hpc=%s Checking depot health\n", hpc->name);

    determine_bandwidths(hpc, &hp_min, &hp_median, &hp_max);
    if (!hp_min) return;  //** No HP's to check

    min_bw = hp_median->avg_bw * hpc->min_bw_fraction;
    if (hp_min->avg_bw >= min_bw) return;

    //** Mark the HP's as dead
    for (hi=apr_hash_first(hpc->pool, hpc->hp); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, NULL, (void **)&hp);

        if ((hp->dead != 0) || (hp->avg_bw >= min_bw)) continue;  //** Nothing to do

        //** If we made it here we need to mark the HP as dead.
        hp->dead = apr_time_now() + hpc->dt_dead_timeout;
        log_printf(15, "hpc=%s Marking as DEAD hp=%s\n", hpc->name, hp->skey);
    }
}

//************************************************************************
//  hportal_thread - Main execution thread controlling GOP flow between
//      user space and actual connections
//************************************************************************

void *hportal_thread(apr_thread_t *th, void *arg)
{
    gop_portal_context_t *hpc = arg;
    apr_hash_index_t *hi;
    apr_time_t dt;
    void *val;
    hportal_t *hp;
    int finished, done, ntodo;
    apr_time_t dead_next_check;

log_printf(15, "START name=%s\n", hpc->name);

    dead_next_check = apr_time_now() + apr_time_from_sec(60);
    dt = apr_time_from_msec(100);
    finished = done = 0;
    do {
log_printf(15, "AAAAA hpc=%s dt_dead_check=" TT " next=" TT " now=" TT "\n", hpc->name, hpc->dt_dead_check, dead_next_check, apr_time_now());

        if (apr_time_now() > dead_next_check) {
            depot_health_check(hpc);
            dead_next_check = apr_time_now() + hpc->dt_dead_check;
log_printf(15, "hpc=%s dt_dead_check=" TT " next=" TT " now=" TT "\n", hpc->name, hpc->dt_dead_check, dead_next_check, apr_time_now());
        }

        hpc_wait(hpc, dt);
        process_results(hpc);  //** Always start up and clear out any backend tasks to make room for new submits
        if (!finished) finished = fetch_tasks(hpc);
        ntodo = hpc_submit_tasks(hpc);
        process_results(hpc);
        if ((finished == 1) && (ntodo == 0)) done = 1;
    } while (!done);

log_printf(15, "AFTER LOOP name=%s\n", hpc->name);

    ntodo = submit_shutdown(hpc);
    wait_for_shutdown(hpc, ntodo);

    //** Now destroy all the hportals
    for (hi=apr_hash_first(hpc->pool, hpc->hp); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, NULL, &val);
        hp = (hportal_t *)val;
        apr_hash_set(hpc->hp, hp->skey, APR_HASH_KEY_STRING, NULL);
        hp_destroy(hp);
    }

log_printf(15, "END name=%s\n", hpc->name);

    return(NULL);
}

//************************************************************************
// hc_send_thread - Sending Hconnection thread
//************************************************************************

void *hc_send_thread(apr_thread_t *th, void *data)
{
    hconn_t *hc = data;
    hportal_t *hp = hc->hp;
    gop_portal_context_t *hpc = hp->hpc;
    tbx_ns_t *ns = hc->ns;
    gop_op_generic_t *gop;
    gop_command_op_t *hop;
    conn_cmd_t cmd;
    apr_time_t dt, start_time, timeout;
    int err, nbytes;
    gop_op_status_t finished;
    apr_status_t dummy;

    //** Attempt to connect
    err = hpc->fn->connect(ns, hp->connect_context, hp->host, hp->port, hp->hpc->dt_connect);

    //** If failed then err out and exit
    if (err) {
        memset(&cmd, 0, sizeof(cmd));
        dt = apr_time_from_sec(10);
        cmd.cmd = CONN_CLOSED;
        cmd.hc = hc;
log_printf(15, "internal hp=%s SEND CONN_CLOSED\n", hp->skey);
        tbx_que_put(hc->internal, &cmd, dt);
        apr_thread_exit(th, 0);
    }

    //** If we made it here notify the recv thread
    cmd.cmd = CONN_READY;
log_printf(15, "internal hp=%s SEND CONN_READY\n", hp->skey);
    tbx_que_put(hc->internal, &cmd, TBX_QUE_BLOCK);

    //** Now enter the main loop
    finished = gop_success_status;
    start_time = apr_time_now();
    dt = 0;
    timeout = apr_time_from_sec(1);
    while (finished.op_status == OP_STATE_SUCCESS) {
        cmd.cmd = CONN_EMPTY;
        nbytes = tbx_que_get(hc->incoming, &cmd, timeout);
log_printf(15, "hp=%s get=%d cmd=%d\n", hp->skey, nbytes, cmd.cmd);
        if (nbytes != 0) {
log_printf(15, "hp=%s checking timeout\n", hp->skey);
            dt = apr_time_now() - start_time;
            if (dt > hpc->max_idle) {   //** Nothing to do so kick out
                finished.op_status = OP_STATE_FAILURE;
log_printf(15, "hp=%s kicking out\n", hp->skey);
            }
            continue;
        } else if (cmd.cmd == CONN_CLOSE) {
log_printf(15, "hp=%s CONN_CLOSE\n", hp->skey);
            finished = gop_failure_status;
            continue;
        }

        //** Got a task to process
        gop = cmd.ptr;
log_printf(15, "hp=%s gid=%d\n", hp->skey, gop_id(gop));
        hop = &(gop->op->cmd);
        hop->start_time = apr_time_now();  //** This is changed in the recv phase also
        hop->end_time = hop->start_time + hop->timeout;

        //** Send the command
        finished = (hop->send_command != NULL) ? hop->send_command(gop, ns) : gop_success_status;
log_printf(15, "hp=%s send_command=%d\n", hp->skey, finished.op_status);

        if (finished.op_status == OP_STATE_SUCCESS) {
            //** Perform the send phase
            finished = (hop->send_phase != NULL) ? hop->send_phase(gop, ns) : gop_success_status;
log_printf(15, "hp=%s send_phase=%d\n", hp->skey, finished.op_status);
        }

        cmd.status = finished;
        tbx_que_put(hc->internal, &cmd, TBX_QUE_BLOCK);
    }

log_printf(15, "hp=%s END OF LOOP cmd=%d finished=%d\n", hp->skey, cmd.cmd, finished.op_status);

    if (cmd.cmd != CONN_CLOSE) {
        //** Notify the hportal I want to exit
log_printf(15, "hp=%s sending CONN_CLOSE_REQUEST\n", hp->skey);
        memset(&cmd, 0, sizeof(cmd));
        cmd.cmd = CONN_CLOSE_REQUEST;
        cmd.hc = hc;
        tbx_que_put(hc->outgoing, &cmd, TBX_QUE_BLOCK);
        hpc_notify(hpc);

        //** Wait until I get an official close request
        //** In the meantime just dump all the requests bak on the que
        while (tbx_que_get(hc->incoming, &cmd, TBX_QUE_BLOCK) == 0) {
            if (cmd.cmd == CONN_CLOSE) break;
            hp_gop_retry(hc, cmd.ptr);
        }
    }

log_printf(15, "hp=%s Got a CONN_CLOSE\n", hp->skey);

    //** Notify the sending end
    cmd.cmd = CONN_CLOSE;
    tbx_que_put(hc->internal, &cmd, TBX_QUE_BLOCK);
    apr_thread_join(&dummy, hc->recv_thread);

    apr_thread_exit(th, 0);

    return(NULL);
}

//************************************************************************
// hc_recv_thread - Receiving Hconnection thread
//************************************************************************

void *hc_recv_thread(apr_thread_t *th, void *data)
{
    hconn_t *hc = data;
    hportal_t *hp = hc->hp;
    gop_portal_context_t *hpc = hp->hpc;
    tbx_ns_t *ns = hc->ns;
    gop_op_generic_t *gop;
    gop_command_op_t *hop;
    apr_time_t dt, start_time, timeout;
    int nbytes;
    conn_cmd_t cmd;
    gop_op_status_t status;

    //** Wait to make sure the send_thread connected
    tbx_que_get(hc->internal, &cmd, TBX_QUE_BLOCK);
    if (cmd.cmd != CONN_READY) {  //** IF we fail pass the error on and exit
        memset(&cmd, 0, sizeof(cmd));
        cmd.cmd = CONN_CLOSED;
        cmd.hc = hc;
log_printf(15, "hp=%s CONN_CLOSED\n", hc->hp->skey);
        tbx_que_put(hc->outgoing, &cmd, TBX_QUE_BLOCK);
        hpc_notify(hpc);
        apr_thread_exit(th, 0);
        return(NULL);
    }

    //** Notify the main loop we are ready
log_printf(15, "hp=%s CONN_READY\n", hc->hp->skey);
    cmd.cmd = CONN_READY;
    cmd.hc = hc;
    tbx_que_put(hc->outgoing, &cmd, TBX_QUE_BLOCK);
    hpc_notify(hpc);

    //** Now enter the main loop
    status = gop_success_status;
    start_time = apr_time_now();
    cmd.cmd = CONN_EMPTY;
    dt = 0;
    timeout = apr_time_from_sec(1);
    while (status.op_status == OP_STATE_SUCCESS) {
        cmd.cmd = CONN_EMPTY;
        nbytes = tbx_que_get(hc->internal, &cmd, timeout);
log_printf(15, "hp=%s get=%d cmd=%d\n", hc->hp->skey, nbytes, cmd.cmd);
        if (nbytes != 0) {
            dt = apr_time_now() - start_time;
            if (dt > hpc->max_idle) {   //** Nothing to do so kick out
                status.op_status = OP_STATE_FAILURE;
            }
            continue;
        } else if (cmd.cmd == CONN_CLOSE) {
            status = gop_failure_status;
            continue;
        } else if (cmd.status.op_status != OP_STATE_SUCCESS) {  //** Push the command back on the stack and kick out
            status = cmd.status;
            hp_gop_retry(hc, cmd.ptr);
            continue;
        }


        //** Got a task to process
        gop = cmd.ptr;
log_printf(15, "hp=%s gid=%d\n", hp->skey, gop_id(gop));
        hop = &(gop->op->cmd);
        status = (hop->recv_phase != NULL) ? hop->recv_phase(gop, ns) : status;
        hop->end_time = apr_time_now();
        cmd.status = status;
        cmd.gop_workload = hop->workload;
        cmd.gop_dt = hop->end_time - hop->start_time;
        cmd.cmd = CONN_GOP_DONE;
        cmd.hc = hc;
log_printf(15, "hp=%s gid=%d status=%d\n", hp->skey, gop_id(gop), status.op_status);
        gop_mark_completed(gop, status);

        tbx_que_put(hc->outgoing, &cmd, TBX_QUE_BLOCK);
        hpc_notify(hpc);
    }


    if (cmd.cmd != CONN_CLOSE) {
        //** Notify the hportal I want to exit
log_printf(15, "hp=%s sending a CONN_CLOSE_REQUEST\n", hp->skey);
        memset(&cmd, 0, sizeof(cmd));
        cmd.cmd = CONN_CLOSE_REQUEST;
        cmd.hc = hc;
        tbx_que_put(hc->outgoing, &cmd, TBX_QUE_BLOCK);
        hpc_notify(hpc);

        //** Wait until I get an official close request
        //** In the meantime just dump all the requests back on the que
        while (tbx_que_get(hc->internal, &cmd, TBX_QUE_BLOCK) == 0) {
            if (cmd.cmd == CONN_CLOSE) break;
            hp_gop_retry(hc, cmd.ptr);
        }
    }

log_printf(15, "hp=%s Got a CONN_CLOSE\n", hp->skey);

    //** Update the router
    cmd.cmd = CONN_CLOSED;
    cmd.hc = hc;
    tbx_que_put(hc->outgoing, &cmd, TBX_QUE_BLOCK);
    hpc_notify(hpc);

    apr_thread_exit(th, 0);
    return(NULL);
}

//************************************************************************
//  hconn_new - Creates a new Hportal connection
//************************************************************************

hconn_t *hconn_new(hportal_t *hp, tbx_que_t *outgoing, apr_pool_t *mpool)
{
    hconn_t *hc;
    int err;
    tbx_type_malloc_clear(hc, hconn_t, 1);

    hc->hp = hp;
    hp->limbo_conn++;
    hc->incoming = tbx_que_create(1000, sizeof(conn_cmd_t));
    hc->internal = tbx_que_create(10000, sizeof(conn_cmd_t));
    hc->ns = tbx_ns_new();

    hc->outgoing = hp->hpc->results_que;

    tbx_thread_create_warn(err, &(hc->send_thread), NULL, hc_send_thread, (void *)hc, mpool);
    tbx_thread_create_warn(err, &(hc->recv_thread), NULL, hc_recv_thread, (void *)hc, mpool);

    return(hc);
}

//************************************************************************
// hconn_add - Adds the new hconn to the connection list
//************************************************************************

void hconn_add(hportal_t *hp, hconn_t *hc)
{
    tbx_stack_push(hp->conn_list, hc);
    hc->ele = tbx_stack_get_current_ptr(hp->conn_list);
}

//************************************************************************
//  hconn_destroy - Destroys an Hportal connection
//************************************************************************

void hconn_destroy(hconn_t *hc)
{
    apr_status_t val;

    apr_thread_join(&val, hc->send_thread);
    tbx_que_destroy(hc->incoming);
    tbx_que_destroy(hc->internal);
    tbx_ns_destroy(hc->ns);

    free(hc);
}

//************************************************************************
// hp_create - Creates a Host portal
//************************************************************************

hportal_t *hp_create(gop_portal_context_t *hpc, char *hostport)
{
    hportal_t *hp;
    char *hp2 = strdup(hostport);
    char *bstate;
    int fin, n;

    tbx_type_malloc_clear(hp, hportal_t, 1);

    n = strlen(hp2);
    hp->host = tbx_stk_string_token(hp2, HP_HOSTPORT_SEPARATOR, &bstate, &fin);
    hp->host[n-1] = '\0';
    hp->port = atoi(bstate);
    log_printf(15, "hostport: %s host=%s port=%d\n", hostport, hp->host, hp->port);

    hp->skey = strdup(hostport);

    hp->conn_list = tbx_stack_new();
    hp->pending = tbx_stack_new();
    hp->workload_pending = 0;
    hp->hpc = hpc;

    return(hp);
}

//************************************************************************
//  hp_destroy - Destroys an Hportal
//************************************************************************

void hp_destroy(hportal_t *hp)
{
    apr_hash_set(hp->hpc->hp, hp->skey, APR_HASH_KEY_STRING, NULL);

    free(hp->skey);
    free(hp->host);
    tbx_stack_free(hp->conn_list, 0);
    tbx_stack_free(hp->pending, 0);
    free(hp);
}

//************************************************************************
// gop_portal_options_set - Sets the hportal options.  If the option
//   is 0 then it's not changed.
//
//   NOTE: This is not thread safe and so should only be called right
//         after the HPC is created and before any commands have been
//         submitted.
//************************************************************************

void gop_portal_context_options_set(gop_portal_context_t *hpc, int max_total_conn,
        apr_time_t max_idle, apr_time_t wait_stable, apr_time_t max_connect,
        int min_conn, int max_conn, int64_t max_workload)
{
    if (max_total_conn > 0) hpc->max_total_conn = max_total_conn;
    if (max_idle > 0) hpc->max_idle = max_idle;
    if (wait_stable > 0) hpc->wait_stable_time =  wait_stable;
    if (min_conn > 0) hpc->min_conn = min_conn;
    if (max_conn > 0) hpc->max_conn = max_conn;
    if (max_connect > 0) hpc->dt_connect = max_connect;
    if (max_workload > 0) hpc->max_workload = max_workload;
}

//************************************************************************
// gop_portal_options_get - Gets the hportal options.  Pass NULL to
//   variables to ignore returning.
//************************************************************************

void gop_portal_context_options_get(gop_portal_context_t *hpc, int *max_total_conn,
        apr_time_t *max_idle, apr_time_t *wait_stable, apr_time_t *max_connect,
        int *min_conn, int *max_conn, int64_t *max_workload)
{
    if (!max_total_conn) *max_total_conn = hpc->max_total_conn;
    if (!max_idle) *max_idle = hpc->max_idle;
    if (!wait_stable) *wait_stable = hpc->wait_stable_time;
    if (!min_conn) *min_conn = hpc->min_conn;
    if (!max_conn) *max_conn = hpc->max_conn;
    if (!max_connect) *max_connect = hpc->dt_connect;
    if (!max_workload) *max_workload = hpc->max_workload;
}


//************************************************************************
// gop_hp_fn_set - Sets the HPC's fn structure
//************************************************************************

void gop_hp_fn_set(gop_portal_context_t *hpc, gop_portal_fn_t *fn)
{
    hpc->fn = fn;
}

//************************************************************************
// gop_hp_fn_get - Returns the HPC's fn structure
//************************************************************************

gop_portal_fn_t *gop_hp_fn_get(gop_portal_context_t *hpc)
{
    return(hpc->fn);
}

//************************************************************************
// gop_op_sync_exec_enabled - Returns ifthe task can be run via sync_exec.
//     For internal Use in gop.c
//************************************************************************

int gop_op_sync_exec_enabled(gop_op_generic_t *gop)
{
    return(gop->base.pc->fn->sync_exec == NULL ? 0 : 1);
}

//************************************************************************
// gop_op_sync_exec - Executes a task.  For internal Use in gop.c
//************************************************************************

void gop_op_sync_exec(gop_op_generic_t *gop)
{
    gop->base.pc->fn->sync_exec(gop->base.pc->arg, gop);
}

//************************************************************************
// gop_op_submit - Submits a task.  For internal Use in gop.c
//************************************************************************

void gop_op_submit(gop_op_generic_t *gop)
{
    gop->base.pc->fn->submit(gop->base.pc->arg, gop);
}

//************************************************************************
// gop_hp_que_op_submit - Submits a task to the HPC for execution.
//    This is designed to be called from the implementation specific submit
//************************************************************************

int gop_hp_que_op_submit(gop_portal_context_t *hpc, gop_op_generic_t *op)
{
    hpc_cmd_t cmd;

    cmd.cmd = HPC_CMD_GOP;
    cmd.gop = op;

    return(tbx_que_put(hpc->gop_que, &cmd, TBX_QUE_BLOCK));
}

//************************************************************************
//  gop_hp_context_create - Creates a new hportal context structure for use
//************************************************************************

gop_portal_context_t *gop_hp_context_create(gop_portal_fn_t *imp, char *name)
{
    gop_portal_context_t *hpc;
    int err;

    tbx_type_malloc_clear(hpc, gop_portal_context_t, 1);

    assert_result(apr_pool_create(&(hpc->pool), NULL), APR_SUCCESS);
    hpc->hp = apr_hash_make(hpc->pool); FATAL_UNLESS(hpc->hp != NULL);
    apr_thread_mutex_create(&(hpc->todo_lock), APR_THREAD_MUTEX_DEFAULT, hpc->pool);
    apr_thread_cond_create(&(hpc->todo_cond), hpc->pool);
    hpc->fn = imp;
    hpc->name = (name) ? strdup(name) : strdup("MISSING");
    hpc->results_que = tbx_que_create(10000, sizeof(conn_cmd_t));
    hpc->gop_que = tbx_que_create(10000, sizeof(hpc_cmd_t));

    hpc->dt_dead_timeout = apr_time_from_sec(5*60);
    hpc->dt_dead_check = apr_time_from_sec(60);
    hpc->min_bw_fraction = 0.001;
    hpc->max_total_conn = 64;
    hpc->max_idle = apr_time_from_sec(30);
    hpc->wait_stable_time = apr_time_from_sec(60);
    hpc->min_conn = 1;
    hpc->max_conn = 4;
    hpc->dt_connect = apr_time_from_sec(10);
    hpc->max_workload = 10*1024*1024;
    hpc->mix_latest_fraction = 0.5;
    tbx_ns_timeout_set(&(hpc->dt_connect), 1, 0);

    tbx_thread_create_warn(err, &(hpc->main_thread), NULL, hportal_thread, (void *)hpc, hpc->pool);

    tbx_siginfo_handler_add(hportal_siginfo_handler, hpc);

    return(hpc);
}

//************************************************************************
// gop_hp_context_destroy - Destroys a hportal context structure
//************************************************************************

void gop_hp_context_destroy(gop_portal_context_t *hpc)
{
    apr_status_t val;
    hpc_cmd_t cmd;

    tbx_siginfo_handler_remove(hportal_siginfo_handler, hpc);

    //** Shutdown all the connections and the main thread
    cmd.cmd = HPC_CMD_SHUTDOWN;
    tbx_que_put(hpc->gop_que, &cmd, TBX_QUE_BLOCK);
    hpc_notify(hpc);
    apr_thread_join(&val, hpc->main_thread);

    //** Cleanup
    tbx_que_destroy(hpc->results_que);
    tbx_que_destroy(hpc->gop_que);
    if (hpc->name) free(hpc->name);
    apr_hash_clear(hpc->hp);
    apr_thread_mutex_destroy(hpc->todo_lock);
    apr_thread_cond_destroy(hpc->todo_cond);
    apr_pool_destroy(hpc->pool);

    free(hpc);

    return;
}

