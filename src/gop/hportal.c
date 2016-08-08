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

#include <apr_errno.h>
#include <apr_hash.h>
#include <apr_pools.h>
#include <apr_thread_cond.h>
#include <apr_thread_mutex.h>
#include <apr_thread_proc.h>
#include <apr_time.h>
#include <assert.h>
#include <gop/portal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/assert_result.h>
#include <tbx/atomic_counter.h>
#include <tbx/dns_cache.h>
#include <tbx/fmttypes.h>
#include <tbx/log.h>
#include <tbx/network.h>
#include <tbx/stack.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>
#include <time.h>
#include <unistd.h>

#include "gop.h"
#include "gop/hp.h"
#include "gop/types.h"
#include "host_portal.h"

//***************************************************************************
//  hportal_wait - Waits up to the specified time for the condition
//***************************************************************************

void hportal_wait(gop_host_portal_t *hp, int dt)
{
    apr_time_t t;

    if (dt < 0) return;   //** If negative time has run out so return

    tbx_ns_timeout_set(&t, dt, 0);
    apr_thread_cond_timedwait(hp->cond, hp->lock, t);
}


//***************************************************************************
// get_hpc_thread_count - Returns the current # of running threads
//***************************************************************************

int get_hpc_thread_count(gop_portal_context_t *hpc)
{
    int n;


    n = tbx_atomic_get(hpc->running_threads);
    return(n);
}

//***************************************************************************
// modify_hpc_thread_count - Modifies the total thread count
//***************************************************************************

void modify_hpc_thread_count(gop_portal_context_t *hpc, int n)
{

    if (n == -1) {
        tbx_atomic_dec(hpc->running_threads);
    } else if (n == 1) {
        tbx_atomic_inc(hpc->running_threads);
    } else {
       FATAL_UNLESS((n == 1) || (n== -1));
    }

}

//************************************************************************
//  create_hportal
//************************************************************************

gop_host_portal_t *create_hportal(gop_portal_context_t *hpc, void *connect_context, char *hostport, int min_conn, int max_conn, apr_time_t dt_connect)
{
    gop_host_portal_t *hp;

    log_printf(15, "create_hportal: hpc=%p\n", hpc);
    tbx_type_malloc_clear(hp, gop_host_portal_t, 1);
    assert_result(apr_pool_create(&(hp->mpool), NULL), APR_SUCCESS);

    char host[sizeof(hp->host)];
    int port;
    char *hp2 = strdup(hostport);
    char *bstate;
    int fin;

    host[0] = '\0';

    strncpy(host, tbx_stk_string_token(hp2, HP_HOSTPORT_SEPARATOR, &bstate, &fin), sizeof(host)-1);
    host[sizeof(host)-1] = '\0';
    port = atoi(bstate);
    free(hp2);
    log_printf(15, "create_hportal: hostport: %s host=%s port=%d min=%d max=%d dt=" TT "\n", hostport, host, port, min_conn, max_conn, dt_connect);

    strncpy(hp->host, host, sizeof(hp->host)-1);
    hp->host[sizeof(hp->host)-1] = '\0';

    //** Check if we can resolve the host's IP address
    char in_addr[DNS_ADDR_MAX];
    if (tbx_dnsc_lookup(host, in_addr, NULL) != 0) {
        log_printf(1, "create_hportal: Can\'t resolve host address: %s:%d\n", host, port);
        hp->invalid_host = 0;
    } else {
        hp->invalid_host = 0;
    }

    hp->port = port;
    snprintf(hp->skey, sizeof(hp->skey), "%s", hostport);
    hp->connect_context = hpc->fn->dup_connect_context(connect_context);

    hp->context = hpc;
    hp->min_conn = min_conn;
    hp->max_conn = max_conn;
    hp->dt_connect = dt_connect;
    hp->sleeping_conn = 0;
    hp->workload = 0;
    hp->executing_workload = 0;
    hp->cmds_processed = 0;
    hp->n_conn = 0;
    hp->conn_list = tbx_stack_new();
    hp->closed_que = tbx_stack_new();
    hp->que = tbx_stack_new();
    hp->direct_list = tbx_stack_new();
    hp->pause_until = 0;
    hp->stable_conn = max_conn;
    hp->closing_conn = 0;
    hp->failed_conn_attempts = 0;
    hp->successful_conn_attempts = 0;
    hp->abort_conn_attempts = hpc->abort_conn_attempts;

    apr_thread_mutex_create(&(hp->lock), APR_THREAD_MUTEX_DEFAULT, hp->mpool);
    apr_thread_cond_create(&(hp->cond), hp->mpool);

    return(hp);
}

//************************************************************************
// _reap_hportal - Frees the closed depot connections
//************************************************************************

void _reap_hportal(gop_host_portal_t *hp, int quick)
{
    gop_host_connection_t *hc;
    apr_status_t value;
    int count;

    tbx_stack_move_to_top(hp->closed_que);
    while ((hc = (gop_host_connection_t *)tbx_stack_get_current_data(hp->closed_que)) != NULL) {
        apr_thread_join(&value, hc->recv_thread);
        log_printf(5, "hp=%s ns=%d\n", hp->skey, tbx_ns_getid(hc->ns));
        for (count=0; ((quick == 0) || (count < 2)); count++) {
            lock_hc(hc);  //** Make sure that no one is running close_hc() while we're trying to close it
            if (hc->closing != 1) {  //** Ok to to remove it
                unlock_hc(hc);

                tbx_stack_delete_current(hp->closed_que, 0, 0);
                destroy_host_connection(hc);

                break;
            } else {  //** Got somone trying ot close it so wait a little bit
                unlock_hc(hc);
                apr_sleep(apr_time_from_msec(10));
            }
        }

        tbx_stack_move_down(hp->closed_que);
        if (tbx_stack_get_current_data(hp->closed_que) == NULL) tbx_stack_move_to_top(hp->closed_que);  //** Restart it needed
    }
}

//************************************************************************
// destroy_hportal - Destroys a Host_portal data struct
//************************************************************************

void destroy_hportal(gop_host_portal_t *hp)
{
    log_printf(5, "host=%s conn_list=%d closed=%d\n", hp->host, tbx_stack_count(hp->conn_list),tbx_stack_count(hp->closed_que));
    hportal_lock(hp);
    _reap_hportal(hp, 0);
    hportal_unlock(hp);

    tbx_stack_free(hp->conn_list, 1);
    tbx_stack_free(hp->que, 1);
    tbx_stack_free(hp->closed_que, 1);
    tbx_stack_free(hp->direct_list, 1);

    hp->context->fn->destroy_connect_context(hp->connect_context);

    apr_thread_mutex_destroy(hp->lock);
    apr_thread_cond_destroy(hp->cond);

    apr_pool_destroy(hp->mpool);
    log_printf(5, "destroy_hportal: Total commands processed: " I64T " (host:%s:%d)\n", hp->cmds_processed,
               hp->host, hp->port);
    free(hp);
}

//************************************************************************
// lookup_hportal - Looks up a depot/port in the current list
//************************************************************************

gop_host_portal_t *_lookup_hportal(gop_portal_context_t *hpc, char *hostport)
{
    gop_host_portal_t *hp;

    hp = (gop_host_portal_t *)(apr_hash_get(hpc->table, hostport, APR_HASH_KEY_STRING));

    return(hp);
}

//************************************************************************
//  gop_hp_context_create - Creates a new hportal context structure for use
//************************************************************************

gop_portal_context_t *gop_hp_context_create(gop_portal_fn_t *imp)
{
    gop_portal_context_t *hpc;


    hpc = (gop_portal_context_t *)malloc(sizeof(gop_portal_context_t));FATAL_UNLESS(hpc != NULL);
    memset(hpc, 0, sizeof(gop_portal_context_t));


    assert_result(apr_pool_create(&(hpc->pool), NULL), APR_SUCCESS);
    hpc->table = apr_hash_make(hpc->pool);FATAL_UNLESS(hpc->table != NULL);


    apr_thread_mutex_create(&(hpc->lock), APR_THREAD_MUTEX_DEFAULT, hpc->pool);

    hpc->fn = imp;
    hpc->next_check = time(NULL);
    hpc->count = 0;
    tbx_ns_timeout_set(&(hpc->dt), 1, 0);

    return(hpc);
}


//************************************************************************
// gop_hp_context_destroy - Destroys a hportal context structure
//************************************************************************

void gop_hp_context_destroy(gop_portal_context_t *hpc)
{
    apr_hash_index_t *hi;
    gop_host_portal_t *hp;
    void *val;

    for (hi=apr_hash_first(hpc->pool, hpc->table); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, NULL, &val);
        hp = (gop_host_portal_t *)val;
        apr_hash_set(hpc->table, hp->skey, APR_HASH_KEY_STRING, NULL);
        destroy_hportal(hp);
    }

    apr_thread_mutex_destroy(hpc->lock);

    apr_hash_clear(hpc->table);
    apr_pool_destroy(hpc->pool);

    free(hpc);

    return;
}

//************************************************************************
// shutdown_direct - shuts down the direct hportals
//************************************************************************

void shutdown_direct(gop_host_portal_t *hp)
{
    gop_host_portal_t *shp;
    gop_host_connection_t *hc;

    if (tbx_stack_count(hp->direct_list) == 0) return;

    tbx_stack_move_to_top(hp->direct_list);
    while ((shp = (gop_host_portal_t *)tbx_stack_pop(hp->direct_list)) != NULL) {
        hportal_lock(shp);
        _reap_hportal(shp, 0);  //** Clean up any closed connections

        if ((shp->n_conn == 0) && (tbx_stack_count(shp->que) == 0)) { //** if not used so remove it
            tbx_stack_delete_current(hp->direct_list, 0, 0);  //**Already closed
        } else {     //** Force it to close
            tbx_stack_free(shp->que, 1);  //** Empty the que so we don't respawn connections
            shp->que = tbx_stack_new();

            tbx_stack_move_to_top(shp->conn_list);
            hc = (gop_host_connection_t *)tbx_stack_get_current_data(shp->conn_list);

            hportal_unlock(shp);
            apr_thread_mutex_unlock(hp->context->lock);

            close_hc(hc, 0);

            apr_thread_mutex_lock(hp->context->lock);
            hportal_lock(shp);
        }

        hportal_unlock(shp);
        destroy_hportal(shp);

//     tbx_stack_move_to_top(hp->direct_list);
    }
}

//*************************************************************************
// gop_hp_shutdown - Shuts down the IBP sys system
//*************************************************************************

void gop_hp_shutdown(gop_portal_context_t *hpc)
{
    gop_host_portal_t *hp;
    gop_host_connection_t *hc;
    apr_hash_index_t *hi;
    void *val;

    log_printf(15, "gop_hp_shutdown: Shutting down the whole system\n");


    //** First tell everyone to shutdown
    for (hi=apr_hash_first(hpc->pool, hpc->table); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, NULL, &val);
        hp = (gop_host_portal_t *)val;
        hportal_lock(hp);

        log_printf(5, "before wait n_conn=%d tbx_stack_count(conn_list)=%d host=%s\n", hp->n_conn, tbx_stack_count(hp->conn_list), hp->skey);
        while (tbx_stack_count(hp->conn_list) != hp->n_conn) {
            hportal_unlock(hp);
            log_printf(5, "waiting for connections to finish starting.  host=%s closing_conn=%d n_conn=%d tbx_stack_count(conn_list)=%d\n", hp->skey, hp->closing_conn, hp->n_conn, tbx_stack_count(hp->conn_list));
            usleep(10000);
            hportal_lock(hp);
        }
        log_printf(5, "after wait n_conn=%d tbx_stack_count(conn_list)=%d\n", hp->n_conn, tbx_stack_count(hp->conn_list));

        tbx_stack_move_to_top(hp->conn_list);
        while ((hc = (gop_host_connection_t *)tbx_stack_get_current_data(hp->conn_list)) != NULL) {
            tbx_stack_free(hp->que, 1);  //** Empty the que so we don't respawn connections
            hp->que = tbx_stack_new();

            lock_hc(hc);
            hc->shutdown_request = 1;
            apr_thread_cond_signal(hc->recv_cond);
            unlock_hc(hc);

            tbx_stack_move_down(hp->conn_list);
        }

        hportal_unlock(hp);
    }


    //** Now go and clean up
    for (hi=apr_hash_first(hpc->pool, hpc->table); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, NULL, &val);
        hp = (gop_host_portal_t *)val;
        apr_hash_set(hpc->table, hp->skey, APR_HASH_KEY_STRING, NULL);  //** This removes the key

        log_printf(15, "gop_hp_shutdown: Shutting down host=%s\n", hp->skey);

        hportal_lock(hp);

        log_printf(5, "closing_conn=%d n_conn=%d host=%s\n", hp->closing_conn, hp->n_conn, hp->host);
        _reap_hportal(hp, 0);  //** clean up any closed connections

        log_printf(5, "closing_conn=%d n_conn=%d\n", hp->closing_conn, hp->n_conn);
        while ((hp->closing_conn > 0) || (hp->n_conn > 0)) {
            log_printf(5, "waiting for connections to close.  host=%s closing_conn=%d n_conn=%d tbx_stack_count(conn_list)=%d\n", hp->skey, hp->closing_conn, hp->n_conn, tbx_stack_count(hp->conn_list));
            hportal_unlock(hp);
            usleep(10000);
            hportal_lock(hp);
        }

        shutdown_direct(hp);  //** Shutdown any direct connections

        tbx_stack_move_to_top(hp->conn_list);
        while ((hc = (gop_host_connection_t *)tbx_stack_get_current_data(hp->conn_list)) != NULL) {
            tbx_stack_free(hp->que, 1);  //** Empty the que so we don't respawn connections
            hp->que = tbx_stack_new();
            hportal_unlock(hp);
            apr_thread_mutex_unlock(hpc->lock);

            close_hc(hc, 0);

            apr_thread_mutex_lock(hpc->lock);
            hportal_lock(hp);

            tbx_stack_move_to_top(hp->conn_list);
        }

        hportal_unlock(hp);

        destroy_hportal(hp);
    }


    return;
}

//************************************************************************
// compact_hportal_direct - Compacts the direct hportals if needed
//************************************************************************

void compact_hportal_direct(gop_host_portal_t *hp)
{
    gop_host_portal_t *shp;

    if (tbx_stack_count(hp->direct_list) == 0) return;

    tbx_stack_move_to_top(hp->direct_list);
    while ((shp = (gop_host_portal_t *)tbx_stack_get_current_data(hp->direct_list)) != NULL) {

        hportal_lock(shp);
        _reap_hportal(shp, 1);  //** Clean up any closed connections

        if ((shp->n_conn == 0) && (shp->closing_conn == 0) && (tbx_stack_count(shp->que) == 0) && (tbx_stack_count(shp->closed_que) == 0)) { //** if not used so remove it
            tbx_stack_delete_current(hp->direct_list, 0, 0);
            hportal_unlock(shp);
            destroy_hportal(shp);
        } else {
            hportal_unlock(shp);
            tbx_stack_move_down(hp->direct_list);
        }
    }


}

//************************************************************************
// compact_hportals - Removes any hportals that are no longer used
//************************************************************************

void compact_hportals(gop_portal_context_t *hpc)
{
    apr_hash_index_t *hi;
    gop_host_portal_t *hp;
    void *val;

    apr_thread_mutex_lock(hpc->lock);

    for (hi=apr_hash_first(hpc->pool, hpc->table); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, NULL, &val);
        hp = (gop_host_portal_t *)val;

        hportal_lock(hp);

        _reap_hportal(hp, 1);  //** Clean up any closed connections

        compact_hportal_direct(hp);

        if ((hp->n_conn == 0) && (hp->closing_conn == 0) && (tbx_stack_count(hp->que) == 0) &&
                (tbx_stack_count(hp->direct_list) == 0) && (tbx_stack_count(hp->closed_que) == 0)) { //** if not used so remove it
            if (tbx_stack_count(hp->conn_list) != 0) {
                log_printf(0, "ERROR! DANGER WILL ROBINSON! tbx_stack_count(hp->conn_list)=%d hp=%s\n", tbx_stack_count(hp->conn_list), hp->skey);
                tbx_log_flush();
               FATAL_UNLESS(tbx_stack_count(hp->conn_list) == 0);
            } else {
                hportal_unlock(hp);
                apr_hash_set(hpc->table, hp->skey, APR_HASH_KEY_STRING, NULL);  //** This removes the key
                destroy_hportal(hp);
            }
        } else {
            hportal_unlock(hp);
        }
    }

    apr_thread_mutex_unlock(hpc->lock);
}

//************************************************************************
// gop_change_all_hportal_conn - Changes all the hportals min/max connection count
//************************************************************************

void gop_change_all_hportal_conn(gop_portal_context_t *hpc, int min_conn, int max_conn, apr_time_t dt_connect)
{
    apr_hash_index_t *hi;
    gop_host_portal_t *hp;
    void *val;

    apr_thread_mutex_lock(hpc->lock);

    for (hi=apr_hash_first(hpc->pool, hpc->table); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, NULL, &val);
        hp = (gop_host_portal_t *)val;

        hportal_lock(hp);
        hp->min_conn = min_conn;
        hp->max_conn = max_conn;
        hp->stable_conn = max_conn;
        hp->dt_connect = dt_connect;
        hportal_unlock(hp);
    }

    apr_thread_mutex_unlock(hpc->lock);
}

//*************************************************************************
//  _add_hportal_op - Adds a task to a hportal que
//        NOTE:  No locking is performed
//*************************************************************************

void _add_hportal_op(gop_host_portal_t *hp, gop_op_generic_t *hsop, int addtotop, bool release_master)
{
    gop_command_op_t *hop = &(hsop->op->cmd);
    tbx_stack_ele_t *ele;

    hp->workload = hp->workload + hop->workload;

    if (addtotop == 1) {
        tbx_stack_push(hp->que, (void *)hsop);
    } else {
        tbx_stack_move_to_bottom(hp->que);
        tbx_stack_insert_below(hp->que, (void *)hsop);
    };

    //** Since we've now added the op to the hp que we can release the master lock if needed
    //** without fear of having the compact_hportals() coming in and destroying the hp
    if (release_master == 1) apr_thread_mutex_unlock(hp->context->lock);

    //** Check if we need a little pre-processing
    if (hop->on_submit != NULL) {
        ele = tbx_stack_get_current_ptr(hp->que);
        hop->on_submit(hp->que, ele);
    }

    hportal_signal(hp);  //** Send a signal for any tasks listening
}

//*************************************************************************
//  _get_hportal_op - Gets the next task for the depot.
//      NOTE:  No locking is done!
//*************************************************************************

gop_op_generic_t *_get_hportal_op(gop_host_portal_t *hp)
{
    log_printf(16, "_get_hportal_op: stack_size=%d\n", tbx_stack_count(hp->que));

    gop_op_generic_t *hsop;

    tbx_stack_move_to_top(hp->que);
    hsop = (gop_op_generic_t *)tbx_stack_get_current_data(hp->que);

    if (hsop != NULL) {
        gop_command_op_t *hop = &(hsop->op->cmd);

        //** Check if we need to to some command coalescing
        if (hop->before_exec != NULL) {
            hop->before_exec(hsop);
        }

        tbx_stack_pop(hp->que);  //** Actually pop it after the before_exec

        hp->workload = hp->workload - hop->workload;
    }
    return(hsop);
}

//*************************************************************************
// find_hc_to_close - Finds a connection to be close
//*************************************************************************

gop_host_connection_t *find_hc_to_close(gop_portal_context_t *hpc)
{
    apr_hash_index_t *hi;
    gop_host_portal_t *hp, *shp;
    gop_host_connection_t *hc, *best_hc;
    void *val;
    int best_workload, hold_lock;

    hc = NULL;
    best_hc = NULL;
    best_workload = -1;

    apr_thread_mutex_lock(hpc->lock);

    for (hi=apr_hash_first(hpc->pool, hpc->table); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, NULL, &val);
        hp = (gop_host_portal_t *)val;

        hportal_lock(hp);

        //** Scan the async connections
        tbx_stack_move_to_top(hp->conn_list);
        while ((hc = (gop_host_connection_t *)tbx_stack_get_current_data(hp->conn_list)) != NULL) {
            hold_lock = 0;
            lock_hc(hc);
            if (hc->closing == 0) {
                if (best_workload<0) best_workload = hc->curr_workload+1;
                if (hc->curr_workload < best_workload) {
                    if (best_hc != NULL) unlock_hc(best_hc);
                    hold_lock = 1;
                    best_workload = hc->curr_workload;
                    best_hc = hc;
                }
            }
            tbx_stack_move_down(hp->conn_list);
            if (hold_lock == 0) unlock_hc(hc);
        }

        //** Scan the direct connections
        tbx_stack_move_to_top(hp->direct_list);
        while ((shp = (gop_host_portal_t *)tbx_stack_get_current_data(hp->direct_list)) != NULL)  {
            hportal_lock(shp);
            if (tbx_stack_count(shp->conn_list) > 0) {
                tbx_stack_move_to_top(shp->conn_list);
                hc = (gop_host_connection_t *)tbx_stack_get_current_data(shp->conn_list);
                hold_lock = 0;
                lock_hc(hc);
                if (hc->closing == 0) {
                    if (best_workload<0) best_workload = hc->curr_workload+1;
                    if (hc->curr_workload < best_workload) {
                        if (best_hc != NULL) unlock_hc(best_hc);
                        hold_lock = 1;
                        best_workload = hc->curr_workload;
                        best_hc = hc;
                    }
                }
                if (hold_lock == 0) unlock_hc(hc);
            }
            hportal_unlock(shp);
            tbx_stack_move_down(hp->direct_list);
        }

        hportal_unlock(hp);
    }

    if (best_hc != NULL) {  //** Flag it as being closed so we ignore it next round and release the lock
        best_hc->closing = 1;
        unlock_hc(best_hc);
    }
    apr_thread_mutex_unlock(hpc->lock);


    return(best_hc);
}


//*************************************************************************
// spawn_new_connection - Creates a new hportal thread/connection
//*************************************************************************

int spawn_new_connection(gop_host_portal_t *hp)
{
    int n;

    hportal_lock(hp);
    hp->oops_spawn++;
    hportal_unlock(hp);

    n = get_hpc_thread_count(hp->context);
    if (n > hp->context->max_connections) {
        gop_host_connection_t *hc = find_hc_to_close(hp->context);
        if (hc != NULL) close_hc(hc, 1);
    }

    return(create_host_connection(hp));
}

//*************************************************************************
// _hp_fail_tasks - Fails all the tasks for a depot.
//       Only used when a depot is dead
//       NOTE:  No locking is done!
//*************************************************************************

void _hp_fail_tasks(gop_host_portal_t *hp, gop_op_status_t err_code)
{
    gop_op_generic_t *hsop;

    hp->workload = 0;

    //** Use the _get_hportal_op() To make sure we handle any coalescing
    while ((hsop = _get_hportal_op(hp)) != NULL) {
        hportal_unlock(hp);
        gop_mark_completed(hsop, err_code);
        hportal_lock(hp);
    }

}

//*************************************************************************
// check_hportal_connections - checks if the hportal has the appropriate
//     number of connections and if not spawns them
//*************************************************************************

void check_hportal_connections(gop_host_portal_t *hp)
{
    int i, j, total;
    int n_newconn = 0;
    int64_t curr_workload;

    hportal_lock(hp);

    curr_workload = hp->workload + hp->executing_workload;

    //** Now figure out how many new connections are needed, if any
    if (tbx_stack_count(hp->que) == 0) {
        n_newconn = 0;
    } else if (hp->n_conn < hp->min_conn) {
        n_newconn = hp->min_conn - hp->n_conn;
    } else {
        n_newconn = (curr_workload / hp->context->max_workload) - hp->n_conn;
        if (n_newconn < 0) n_newconn = 0;

        if ((hp->n_conn+n_newconn) > hp->max_conn) {
            n_newconn = hp->max_conn - hp->n_conn;
            if (n_newconn < 0) n_newconn = 0;
        }
    }

    i = n_newconn;

    if (hp->sleeping_conn > 0) n_newconn = 0;  //** IF sleeping don't spawn any more connections

    total = n_newconn + hp->n_conn;
    if (total > hp->stable_conn) {
        if (apr_time_now() > hp->pause_until) {
            hp->stable_conn++;
            hp->pause_until = apr_time_now();
            if (hp->stable_conn > hp->max_conn) {
                hp->stable_conn = hp->max_conn;
                n_newconn = 0;
            } else if (hp->stable_conn == 0) {
                hp->stable_conn = 1;
//            n_newconn = 1;
                n_newconn = (hp->pause_until == 0) ? 1 : 0;
            } else {
                n_newconn = (hp->n_conn < hp->max_conn) ? 1 : 0;
                hp->pause_until = apr_time_now() + apr_time_make(hp->context->wait_stable_time, 0);
            }
        } else {
            if (hp->n_conn > 0) {
                n_newconn = 0;
            } else if (hp->pause_until == 0) {
                n_newconn = 1;
            }
        }
    }

    //** Do a check for invalid or down host
    if (hp->invalid_host == 1) {
        if ((hp->n_conn == 0) && (tbx_stack_count(hp->que) > 0)) n_newconn = 1;   //** If no connections create one to sink the command
    }

    j = (hp->pause_until > apr_time_now()) ? 1 : 0;
    log_printf(6, "check_hportal_connections: host=%s n_conn=%d sleeping=%d workload=" I64T " curr_wl=" I64T " exec_wl=" I64T " start_new_conn=%d new_conn=%d stable=%d stack_size=%d pause_until=" TT " now=" TT " pause_until_blocked=%d\n",
               hp->skey, hp->n_conn, hp->sleeping_conn, hp->workload, curr_workload, hp->executing_workload, i, n_newconn, hp->stable_conn, tbx_stack_count(hp->que), hp->pause_until, apr_time_now(), j);

    //** Update the total # of connections after the operation
    //** n_conn is used instead of conn_list to prevent false positives on a dead depot
    hp->n_conn = hp->n_conn + n_newconn;
    hp->oops_check += n_newconn;
    if (n_newconn < 0) hp->oops_neg = -100000;
    hportal_unlock(hp);

    //** Spawn the new connections if needed **
    for (i=0; i<n_newconn; i++) {
        spawn_new_connection(hp);
    }
}

//*************************************************************************
// gop_hp_direct_submit - Creates an empty hportal, if needed, for a dedicated
//    directly executed command *and* submits the command for execution
//*************************************************************************

int gop_hp_direct_submit(gop_portal_context_t *hpc, gop_op_generic_t *op)
{
    int status;
    gop_host_portal_t *hp, *shp;
    gop_host_connection_t *hc;
    gop_command_op_t *hop = &(op->op->cmd);

    apr_thread_mutex_lock(hpc->lock);

    //** Check if we should do a garbage run **
    if (hpc->next_check < time(NULL)) {
        hpc->next_check = time(NULL) + hpc->compact_interval;

        apr_thread_mutex_unlock(hpc->lock);
        compact_hportals(hpc);
        apr_thread_mutex_lock(hpc->lock);
    }

    //** Find it in the list or make a new one
    hp = _lookup_hportal(hpc, hop->hostport);
    if (hp == NULL) {
        log_printf(15, "gop_hp_direct_submit: New host: %s\n", hop->hostport);
        hp = create_hportal(hpc, hop->connect_context, hop->hostport, 1, 1, apr_time_from_sec(1));
        if (hp == NULL) {
            log_printf(15, "gop_hp_direct_submit: create_hportal failed!\n");
            apr_thread_mutex_unlock(hpc->lock);
            return(-1);
        }
        apr_hash_set(hpc->table, hp->skey, APR_HASH_KEY_STRING, (const void *)hp);
    }

    apr_thread_mutex_unlock(hpc->lock);

    log_printf(15, "gop_hp_direct_submit: start opid=%d\n", op->base.id);

    //** Scan the direct list for a free connection
    hportal_lock(hp);
    tbx_stack_move_to_top(hp->direct_list);
    while ((shp = (gop_host_portal_t *)tbx_stack_get_current_data(hp->direct_list)) != NULL)  {
        if (hportal_trylock(shp) == 0) {
            log_printf(15, "gop_hp_direct_submit: opid=%d shp->wl=" I64T " stack_size=%d\n", op->base.id, shp->workload, tbx_stack_count(shp->que));

            if (tbx_stack_count(shp->que) == 0) {
                if (tbx_stack_count(shp->conn_list) > 0) {
                    tbx_stack_move_to_top(shp->conn_list);
                    hc = (gop_host_connection_t *)tbx_stack_get_current_data(shp->conn_list);
                    if (trylock_hc(hc) == 0) {
                        if ((tbx_stack_count(hc->pending_stack) == 0) && (hc->curr_workload == 0)) {
                            log_printf(15, "gop_hp_direct_submit(A): before submit ns=%d opid=%d wl=%d\n",tbx_ns_getid(hc->ns), op->base.id, hc->curr_workload);
                            unlock_hc(hc);
                            hportal_unlock(shp);
                            status = gop_hp_submit(shp, op, 1, 0);
                            log_printf(15, "gop_hp_direct_submit(A): after submit ns=%d opid=%d\n",tbx_ns_getid(hc->ns), op->base.id);
                            hportal_unlock(hp);
                            return(status);
                        }
                        unlock_hc(hc);
                    }
                } else {
                    hportal_unlock(shp);
                    log_printf(15, "gop_hp_direct_submit(B): opid=%d\n", op->base.id);
                    status = gop_hp_submit(shp, op, 1, 0);
                    hportal_unlock(hp);
                    return(status);
                }
            }

            hportal_unlock(shp);
        }

        tbx_stack_move_down(hp->direct_list);  //** Move to the next hp in the list
    }

    //** If I made it here I have to add a new hportal
    shp = create_hportal(hpc, hop->connect_context, hop->hostport, 1, 1, apr_time_from_sec(1));
    if (shp == NULL) {
        log_printf(15, "gop_hp_direct_submit: create_hportal failed!\n");
        hportal_unlock(hp);
        return(-1);
    }
    tbx_stack_push(hp->direct_list, (void *)shp);
    status = gop_hp_submit(shp, op, 1, 0);

    hportal_unlock(hp);

    return(status);
}

//*************************************************************************
// gop_hp_submit - places the op in the hportal's que and also
//     spawns any new connections if needed
//*************************************************************************

int gop_hp_submit(gop_host_portal_t *hp, gop_op_generic_t *op, bool addtotop, bool release_master)
{
    hportal_lock(hp);
    _add_hportal_op(hp, op, addtotop, release_master);  //** Add the task
    hportal_unlock(hp);

    //** Now figure out how many new connections are needed, if any
    check_hportal_connections(hp);

    return(0);
}

//*************************************************************************
// gop_hp_que_op_submit - submit an IBP task for execution via a que
//*************************************************************************

int gop_hp_que_op_submit(gop_portal_context_t *hpc, gop_op_generic_t *op)
{
    gop_command_op_t *hop = &(op->op->cmd);

    apr_thread_mutex_lock(hpc->lock);

    //** Check if we should do a garbage run **
    if (hpc->next_check < time(NULL)) {
        hpc->next_check = time(NULL) + hpc->compact_interval;

        apr_thread_mutex_unlock(hpc->lock);
        log_printf(15, "submit_hp_op: Calling compact_hportals\n");
        compact_hportals(hpc);
        apr_thread_mutex_lock(hpc->lock);
    }

    gop_host_portal_t *hp = _lookup_hportal(hpc, hop->hostport);
    if (hp == NULL) {
        log_printf(15, "gop_hp_que_op_submit: New host: %s\n", hop->hostport);
        hp = create_hportal(hpc, hop->connect_context, hop->hostport, hpc->min_threads, hpc->max_threads, hpc->dt_connect);
        if (hp == NULL) {
            log_printf(15, "gop_hp_que_op_submit: create_hportal failed!\n");
            return(1);
        }
        log_printf(15, "submit_op: New host.. hp->skey=%s\n", hp->skey);
        apr_hash_set(hpc->table, hp->skey, APR_HASH_KEY_STRING, (const void *)hp);
        gop_host_portal_t *hp2 = _lookup_hportal(hpc, hop->hostport);
        log_printf(15, "gop_hp_que_op_submit: after lookup hp2=%p\n", hp2);
    }

    //** This is done in the gop_hp_submit() since we have release_master=1
    //** This protects against accidental compaction removal
    //** apr_thread_mutex_unlock(hpc->lock);

    return(gop_hp_submit(hp, op, 0, 1));
}

