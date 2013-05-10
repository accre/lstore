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

#define _log_module_index 125

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdio.h>
#include <apr_thread_proc.h>
#include "dns_cache.h"
#include "host_portal.h"
#include "fmttypes.h"
#include "network.h"
#include "log.h"
#include "string_token.h"

//***************************************************************************
//  hportal_wait - Waits up to the specified time for the condition
//***************************************************************************

void hportal_wait(host_portal_t *hp, int dt)
{
   apr_time_t t;

   if (dt < 0) return;   //** If negative time has run out so return

   set_net_timeout(&t, dt, 0);
   apr_thread_cond_timedwait(hp->cond, hp->lock, t);
}


//***************************************************************************
// get_hpc_thread_count - Returns the current # of running threads
//***************************************************************************

int get_hpc_thread_count(portal_context_t *hpc)
{
  int n;

  apr_thread_mutex_lock(hpc->lock);
  n = hpc->running_threads;
  apr_thread_mutex_unlock(hpc->lock);

  return(n);
}

//***************************************************************************
// modify_hpc_thread_count - Modifies the total thread count
//***************************************************************************

void modify_hpc_thread_count(portal_context_t *hpc, int n)
{
  apr_thread_mutex_lock(hpc->lock);
  hpc->running_threads = hpc->running_threads + n;
  apr_thread_mutex_unlock(hpc->lock);

}

//************************************************************************
//  create_hportal
//************************************************************************

host_portal_t *create_hportal(portal_context_t *hpc, void *connect_context, char *hostport, int min_conn, int max_conn)
{
  host_portal_t *hp;

log_printf(15, "create_hportal: hpc=%p\n", hpc);
  assert((hp = (host_portal_t *)malloc(sizeof(host_portal_t))) != NULL);
  assert(apr_pool_create(&(hp->mpool), NULL) == APR_SUCCESS);

  char host[sizeof(hp->host)];
  int port;
  char *hp2 = strdup(hostport);
  char *bstate;
  int fin;

  host[0] = '\0'; port = 0;

  strncpy(host, string_token(hp2, HP_HOSTPORT_SEPARATOR, &bstate, &fin), sizeof(host)-1); host[sizeof(host)-1] = '\0';
  port = atoi(bstate);
  free(hp2);
  log_printf(15, "create_hportal: hostport: %s host=%s port=%d min=%d max=%d\n", hostport, host, port, min_conn, max_conn);

  strncpy(hp->host, host, sizeof(hp->host)-1);  hp->host[sizeof(hp->host)-1] = '\0';

  //** Check if we can resolve the host's IP address
  char in_addr[6];
  if (lookup_host(host, in_addr, NULL) != 0) {
     log_printf(1, "create_hportal: Can\'t resolve host address: %s:%d\n", host, port);
     hp->invalid_host = 0;
//     hp->invalid_host = 1;
  } else {
     hp->invalid_host = 0;
  }

  hp->port = port;
  snprintf(hp->skey, sizeof(hp->skey), "%s", hostport);
  hp->connect_context = hpc->fn->dup_connect_context(connect_context);

  hp->context = hpc;
  hp->min_conn = min_conn;
  hp->max_conn = max_conn;
  hp->sleeping_conn = 0;
  hp->workload = 0;
  hp->executing_workload = 0;
  hp->cmds_processed = 0;
  hp->n_conn = 0;
  hp->conn_list = new_stack();
  hp->closed_que = new_stack();
  hp->que = new_stack();
  hp->direct_list = new_stack();
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

void _reap_hportal(host_portal_t *hp)
{
   host_connection_t *hc;
   apr_status_t value;

   while ((hc = (host_connection_t *)pop(hp->closed_que)) != NULL) {
     apr_thread_join(&value, hc->recv_thread);
log_printf(5, "hp=%s\n", hp->skey);
     destroy_host_connection(hc);
   }
}

//************************************************************************
// destroy_hportal - Destroys a Host_portal data struct
//************************************************************************

void destroy_hportal(host_portal_t *hp)
{
  _reap_hportal(hp);

  free_stack(hp->conn_list, 1);
  free_stack(hp->que, 1);
  free_stack(hp->closed_que, 1);
  free_stack(hp->direct_list, 1);

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

host_portal_t *_lookup_hportal(portal_context_t *hpc, char *hostport)
{
  host_portal_t *hp;

//log_printf(1, "_lookup_hportal: hpc=%p hpc->table=%p\n", hpc, hpc->table);
  hp = (host_portal_t *)(apr_hash_get(hpc->table, hostport, APR_HASH_KEY_STRING));
//log_printf(1, "_lookup_hportal: hpc=%p hpc->table=%p hp=%p hostport=%s\n", hpc, hpc->table, hp, hostport);

  return(hp);
}

//************************************************************************
//  create_hportal_context - Creates a new hportal context structure for use
//************************************************************************

portal_context_t *create_hportal_context(portal_fn_t *imp)
{
  portal_context_t *hpc;

//log_printf(1, "create_hportal_context: start\n");

  assert((hpc = (portal_context_t *)malloc(sizeof(portal_context_t))) != NULL);
  memset(hpc, 0, sizeof(portal_context_t));


  assert(apr_pool_create(&(hpc->pool), NULL) == APR_SUCCESS);
  assert((hpc->table = apr_hash_make(hpc->pool)) != NULL);

//log_printf(15, "create_hportal_context: hpc=%p hpc->table=%p\n", hpc, hpc->table);

  apr_thread_mutex_create(&(hpc->lock), APR_THREAD_MUTEX_DEFAULT, hpc->pool);

  hpc->fn = imp;
  hpc->next_check = time(NULL);
  hpc->count = 0;
  set_net_timeout(&(hpc->dt), 1, 0);

  return(hpc);
}


//************************************************************************
// destroy_hportal_context - Destroys a hportal context structure
//************************************************************************

void destroy_hportal_context(portal_context_t *hpc)
{
  apr_hash_index_t *hi;
  host_portal_t *hp;
  void *val;

  for (hi=apr_hash_first(hpc->pool, hpc->table); hi != NULL; hi = apr_hash_next(hi)) {
     apr_hash_this(hi, NULL, NULL, &val); hp = (host_portal_t *)val;
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

void shutdown_direct(host_portal_t *hp)
{
  host_portal_t *shp;
  host_connection_t *hc;

  if (stack_size(hp->direct_list) == 0) return;

  move_to_top(hp->direct_list);
  while ((shp = (host_portal_t *)pop(hp->direct_list)) != NULL) {
     hportal_lock(shp);
     _reap_hportal(shp);  //** Clean up any closed connections

     if ((shp->n_conn == 0) && (stack_size(shp->que) == 0)) { //** if not used so remove it
        delete_current(hp->direct_list, 0, 0);  //**Already closed 
     } else {     //** Force it to close
        free_stack(shp->que, 1);  //** Empty the que so we don't respawn connections
        shp->que = new_stack();

        move_to_top(shp->conn_list);
        hc = (host_connection_t *)get_ele_data(shp->conn_list);

        hportal_unlock(shp);
        apr_thread_mutex_unlock(hp->context->lock);

        close_hc(hc);

        apr_thread_mutex_lock(hp->context->lock);
        hportal_lock(shp);
     }

     hportal_unlock(shp);
     destroy_hportal(shp);

//     move_to_top(hp->direct_list);
  }
}

//*************************************************************************
// shutdown_hportal - Shuts down the IBP sys system
//*************************************************************************

void shutdown_hportal(portal_context_t *hpc)
{
  host_portal_t *hp;
  host_connection_t *hc;
  apr_hash_index_t *hi;
  void *val;

  log_printf(15, "shutdown_hportal: Shutting down the whole system\n");

//IFFY  apr_thread_mutex_lock(hpc->lock);

  //** First tell everyone to shutdown
  for (hi=apr_hash_first(hpc->pool, hpc->table); hi != NULL; hi = apr_hash_next(hi)) {
     apr_hash_this(hi, NULL, NULL, &val); hp = (host_portal_t *)val;
     hportal_lock(hp);

log_printf(5, "before wait n_conn=%d stack_size(conn_list)=%d host=%s\n", hp->n_conn, stack_size(hp->conn_list), hp->skey);
     while (stack_size(hp->conn_list) != hp->n_conn) {
        hportal_unlock(hp);
        log_printf(5, "waiting for connections to finish starting.  host=%s closing_conn=%d n_conn=%d stack_size(conn_list)=%d\n", hp->skey, hp->closing_conn, hp->n_conn, stack_size(hp->conn_list));
        usleep(10000);
        hportal_lock(hp);
     }
log_printf(5, "after wait n_conn=%d stack_size(conn_list)=%d\n", hp->n_conn, stack_size(hp->conn_list));

     move_to_top(hp->conn_list);
     while ((hc = (host_connection_t *)get_ele_data(hp->conn_list)) != NULL) {
        free_stack(hp->que, 1);  //** Empty the que so we don't respawn connections
        hp->que = new_stack();
//        hportal_unlock(hp);

        lock_hc(hc);
        hc->shutdown_request = 1;
        apr_thread_cond_signal(hc->recv_cond);
        unlock_hc(hc);

//        hportal_lock(hp);
        move_down(hp->conn_list);
     }

     hportal_unlock(hp);
  }


  //** Now go and clean up
  for (hi=apr_hash_first(hpc->pool, hpc->table); hi != NULL; hi = apr_hash_next(hi)) {
     apr_hash_this(hi, NULL, NULL, &val); hp = (host_portal_t *)val;
     apr_hash_set(hpc->table, hp->skey, APR_HASH_KEY_STRING, NULL);  //** This removes the key

     log_printf(15, "shutdown_hportal: Shutting down host=%s\n", hp->skey);

     hportal_lock(hp);

log_printf(5, "closing_conn=%d n_conn=%d\n", hp->closing_conn, hp->n_conn);
     _reap_hportal(hp);  //** clean up any closed connections

log_printf(5, "closing_conn=%d n_conn=%d\n", hp->closing_conn, hp->n_conn);
     while ((hp->closing_conn > 0) || (hp->n_conn > 0)) {
        hportal_unlock(hp);
        log_printf(5, "waiting for connections to close.  host=%s closing_conn=%d n_conn=%d stack_size(conn_list)=%d\n", hp->skey, hp->closing_conn, hp->n_conn, stack_size(hp->conn_list));
        usleep(10000);
        hportal_lock(hp);
     }

     shutdown_direct(hp);  //** Shutdown any direct connections

     move_to_top(hp->conn_list);
     while ((hc = (host_connection_t *)get_ele_data(hp->conn_list)) != NULL) {
        free_stack(hp->que, 1);  //** Empty the que so we don't respawn connections
        hp->que = new_stack();
        hportal_unlock(hp);
        apr_thread_mutex_unlock(hpc->lock);

        close_hc(hc);

        apr_thread_mutex_lock(hpc->lock);
        hportal_lock(hp);

        move_to_top(hp->conn_list);
     }

     hportal_unlock(hp);

     destroy_hportal(hp);
  }

//IFFY  apr_thread_mutex_unlock(hpc->lock);

  return;
}

//************************************************************************
// compact_hportal_direct - Compacts the direct hportals if needed
//************************************************************************

void compact_hportal_direct(host_portal_t *hp)
{
  host_portal_t *shp;

  if (stack_size(hp->direct_list) == 0) return;

  move_to_top(hp->direct_list);
  while ((shp = (host_portal_t *)get_ele_data(hp->direct_list)) != NULL) {

     hportal_lock(shp);
     _reap_hportal(shp);  //** Clean up any closed connections

     if ((shp->n_conn == 0) && (shp->closing_conn == 0) && (stack_size(shp->que) == 0)) { //** if not used so remove it
        delete_current(hp->direct_list, 0, 0);
        hportal_unlock(shp);
        destroy_hportal(shp);
     } else {
       hportal_unlock(shp);
       move_down(hp->direct_list);
     }
  }


}

//************************************************************************
// compact_hportals - Removes any hportals that are no longer used
//************************************************************************

void compact_hportals(portal_context_t *hpc)
{
  apr_hash_index_t *hi;
  host_portal_t *hp;
  void *val;

  apr_thread_mutex_lock(hpc->lock);

  for (hi=apr_hash_first(hpc->pool, hpc->table); hi != NULL; hi = apr_hash_next(hi)) {
     apr_hash_this(hi, NULL, NULL, &val); hp = (host_portal_t *)val;

     hportal_lock(hp);

     _reap_hportal(hp);  //** Clean up any closed connections

     compact_hportal_direct(hp);

     if ((hp->n_conn == 0) && (hp->closing_conn == 0) && (stack_size(hp->que) == 0) && (stack_size(hp->direct_list) == 0)) { //** if not used so remove it
       hportal_unlock(hp);
       apr_hash_set(hpc->table, hp->skey, APR_HASH_KEY_STRING, NULL);  //** This removes the key
       destroy_hportal(hp);
     } else {
       hportal_unlock(hp);
     }
  }

  apr_thread_mutex_unlock(hpc->lock);
}

//************************************************************************
// change_all_hportal_conn - Changes all the hportals min/max connection count
//************************************************************************

void change_all_hportal_conn(portal_context_t *hpc, int min_conn, int max_conn)
{
  apr_hash_index_t *hi;
  host_portal_t *hp;
  void *val;

  apr_thread_mutex_lock(hpc->lock);

  for (hi=apr_hash_first(hpc->pool, hpc->table); hi != NULL; hi = apr_hash_next(hi)) {
     apr_hash_this(hi, NULL, NULL, &val); hp = (host_portal_t *)val;

     hportal_lock(hp);
//log_printf(0, "change_all_hportal_conn: hp=%s min=%d max=%d\n", hp->skey, min_conn, max_conn);
     hp->min_conn = min_conn;
     hp->max_conn = max_conn;
     hp->stable_conn = max_conn;
     hportal_unlock(hp);
  }

  apr_thread_mutex_unlock(hpc->lock);
}

//*************************************************************************
//  _add_hportal_op - Adds a task to a hportal que
//        NOTE:  No locking is performed
//*************************************************************************

void _add_hportal_op(host_portal_t *hp, op_generic_t *hsop, int addtotop)
{
  command_op_t *hop = &(hsop->op->cmd);
  Stack_ele_t *ele;

  hp->workload = hp->workload + hop->workload;

  if (addtotop == 1) {
    push(hp->que, (void *)hsop);
  } else {
    move_to_bottom(hp->que);
    insert_below(hp->que, (void *)hsop);
  };

  //** Check if we need a little pre-processing
  if (hop->on_submit != NULL) {
     ele = get_ptr(hp->que);
     hop->on_submit(hp->que, ele);
  }

  hportal_signal(hp);  //** Send a signal for any tasks listening
}

//*************************************************************************
//  _get_hportal_op - Gets the next task for the depot.
//      NOTE:  No locking is done!
//*************************************************************************

op_generic_t *_get_hportal_op(host_portal_t *hp)
{
  log_printf(16, "_get_hportal_op: stack_size=%d\n", stack_size(hp->que));

  op_generic_t *hsop;

  move_to_top(hp->que);
  hsop = (op_generic_t *)get_ele_data(hp->que);

  if (hsop != NULL) {
     command_op_t *hop = &(hsop->op->cmd);

     //** Check if we need to to some command coalescing
     if (hop->before_exec != NULL) {
        hop->before_exec(hsop);
     }

     pop(hp->que);  //** Actually pop it after the before_exec

     hp->workload = hp->workload - hop->workload;
  }
  return(hsop);
}

//*************************************************************************
// find_hc_to_close - Finds a connection to be close
//*************************************************************************

host_connection_t *find_hc_to_close(portal_context_t *hpc)
{
  apr_hash_index_t *hi;
  host_portal_t *hp, *shp;
  host_connection_t *hc, *best_hc, *best_direct;
  void *val;
  int best_workload;
  int oldest_direct_time;

  hc = NULL;
  best_hc = NULL;
  best_workload = -1;
  oldest_direct_time = apr_time_now() + apr_time_make(10,0);
  best_direct = NULL;

  apr_thread_mutex_lock(hpc->lock);

  for (hi=apr_hash_first(hpc->pool, hpc->table); hi != NULL; hi = apr_hash_next(hi)) {
     apr_hash_this(hi, NULL, NULL, &val); hp = (host_portal_t *)val;
//     apr_hash_set(hpc->table, hp->skey, APR_HASH_KEY_STRING, NULL);  //** This removes the key

     hportal_lock(hp);

     //** Scan the async connections
     move_to_top(hp->conn_list);
     while ((hc = (host_connection_t *)get_ele_data(hp->conn_list)) != NULL) {
        lock_hc(hc);
        if (best_workload<0) best_workload = hc->curr_workload+1;
        if (hc->curr_workload < best_workload) {
           best_workload = hc->curr_workload;
           best_hc = hc;
        }
        move_down(hp->conn_list);
        unlock_hc(hc);
     }

     //** Scan the direct connections
     move_to_top(hp->direct_list);
     while ((shp = (host_portal_t *)get_ele_data(hp->direct_list)) != NULL)  {
        hportal_lock(shp);
        if (stack_size(shp->conn_list) > 0) {
           move_to_top(shp->conn_list);
           hc = (host_connection_t *)get_ele_data(shp->conn_list);
           lock_hc(hc);
           if (oldest_direct_time > hc->last_used) {
              best_direct = hc;
              oldest_direct_time = hc->last_used;
           }
           unlock_hc(hc);
        }
        hportal_unlock(shp);
        move_down(hp->direct_list);
     }

     hportal_unlock(hp);
  }

  apr_thread_mutex_unlock(hpc->lock);

  hc = best_hc;
  if (best_direct != NULL) {
     if (best_workload > 0) hc = best_direct;
  }

  return(hc);
}


//*************************************************************************
// spawn_new_connection - Creates a new hportal thread/connection
//*************************************************************************

int spawn_new_connection(host_portal_t *hp)
{
  int n;

  n = get_hpc_thread_count(hp->context);
  if (n > hp->context->max_connections) {
       host_connection_t *hc = find_hc_to_close(hp->context);
       close_hc(hc);
  }

  return(create_host_connection(hp));
}

//*************************************************************************
// _hp_fail_tasks - Fails all the tasks for a depot.  
//       Only used when a depot is dead
//       NOTE:  No locking is done!
//*************************************************************************

void _hp_fail_tasks(host_portal_t *hp, op_status_t err_code)
{
  op_generic_t *hsop;

  hp->workload = 0;
  while ((hsop = (op_generic_t *)pop(hp->que)) != NULL) {
      hportal_unlock(hp);
      gop_mark_completed(hsop, err_code);
      hportal_lock(hp);
  }
}

//*************************************************************************
// check_hportal_connections - checks if the hportal has the appropriate
//     number of connections and if not spawns them
//*************************************************************************

void check_hportal_connections(host_portal_t *hp)
{
   int i, j, total;
   int n_newconn = 0;
   int64_t curr_workload;

   hportal_lock(hp);

   curr_workload = hp->workload + hp->executing_workload;

   //** Now figure out how many new connections are needed, if any
   if (stack_size(hp->que) == 0) {
      n_newconn = 0;
   } else if (hp->n_conn < hp->min_conn) {
       n_newconn = hp->min_conn - hp->n_conn;
   } else {
       n_newconn = (curr_workload / hp->context->max_workload) - hp->n_conn;
       if (n_newconn < 0) n_newconn = 0;

       if ((hp->n_conn+n_newconn) > hp->max_conn) {
          n_newconn = hp->max_conn - hp->n_conn;
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
            n_newconn = 1;
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
      if ((hp->n_conn == 0) && (stack_size(hp->que) > 0)) n_newconn = 1;   //** If no connections create one to sink the command
   }

   j = (hp->pause_until > apr_time_now()) ? 1 : 0;
   log_printf(6, "check_hportal_connections: host=%s n_conn=%d sleeping=%d workload=" I64T " curr_wl=" I64T " exec_wl=" I64T " start_new_conn=%d new_conn=%d stable=%d stack_size=%d pause_until=" TT " now=" TT " pause_until_blocked=%d\n", 
          hp->skey, hp->n_conn, hp->sleeping_conn, hp->workload, curr_workload, hp->executing_workload, i, n_newconn, hp->stable_conn, stack_size(hp->que), hp->pause_until, apr_time_now(), j);

   //** Update the total # of connections after the operation
   //** n_conn is used instead of conn_list to prevent false positives on a dead depot
   hp->n_conn = hp->n_conn + n_newconn;

   hportal_unlock(hp);

   //** Spawn the new connections if needed **
   for (i=0; i<n_newconn; i++) {
       spawn_new_connection(hp);
   }
}

//*************************************************************************
// submit_hp_direct_op - Creates an empty hportal, if needed, for a dedicated
//    directly executed command *and* submits the command for execution
//*************************************************************************

int submit_hp_direct_op(portal_context_t *hpc, op_generic_t *op)
{
   int status;
   host_portal_t *hp, *shp;
   host_connection_t *hc;
   command_op_t *hop = &(op->op->cmd);

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
      log_printf(15, "submit_hp_direct_op: New host: %s\n", hop->hostport);
      hp = create_hportal(hpc, hop->connect_context, hop->hostport, 1, 1);
      if (hp == NULL) {
          log_printf(15, "submit_hp_direct_op: create_hportal failed!\n");
          apr_thread_mutex_unlock(hpc->lock);
          return(-1);
      }
      apr_hash_set(hpc->table, hp->skey, APR_HASH_KEY_STRING, (const void *)hp);
   }

   apr_thread_mutex_unlock(hpc->lock);

   log_printf(15, "submit_hp_direct_op: start opid=%d\n", op->base.id);

   //** Scan the direct list for a free connection
   hportal_lock(hp);
   move_to_top(hp->direct_list);
   while ((shp = (host_portal_t *)get_ele_data(hp->direct_list)) != NULL)  {
      if (hportal_trylock(shp) == 0) {
         log_printf(15, "submit_hp_direct_op: opid=%d shp->wl=" I64T " stack_size=%d\n", op->base.id, shp->workload, stack_size(shp->que));

         if (stack_size(shp->que) == 0) {
            if (stack_size(shp->conn_list) > 0) {
               move_to_top(shp->conn_list);
               hc = (host_connection_t *)get_ele_data(shp->conn_list);
               if (trylock_hc(hc) == 0) {
                  if ((stack_size(hc->pending_stack) == 0) && (hc->curr_workload == 0)) {
                     log_printf(15, "submit_hp_direct_op(A): before submit ns=%d opid=%d wl=%d\n",ns_getid(hc->ns), op->base.id, hc->curr_workload);
                     unlock_hc(hc);
                     hportal_unlock(shp);
                     status = submit_hportal(shp, op, 1);
                     log_printf(15, "submit_hp_direct_op(A): after submit ns=%d opid=%d\n",ns_getid(hc->ns), op->base.id);
                     hportal_unlock(hp);
                     return(status);
                  }
                  unlock_hc(hc);
               }
            } else {
              hportal_unlock(shp);
              log_printf(15, "submit_hp_direct_op(B): opid=%d\n", op->base.id);
              status = submit_hportal(shp, op, 1);
              hportal_unlock(hp);
              return(status);
            }
         }

         hportal_unlock(shp);
      }

      move_down(hp->direct_list);  //** Move to the next hp in the list
   }

   //** If I made it here I have to add a new hportal
   shp = create_hportal(hpc, hop->connect_context, hop->hostport, 1, 1);
   if (shp == NULL) {
      log_printf(15, "submit_hp_direct_op: create_hportal failed!\n");
      hportal_unlock(hp);
      return(-1);
   }
   push(hp->direct_list, (void *)shp);
   status = submit_hportal(shp, op, 1);
   
   hportal_unlock(hp);

   return(status);
}

//*************************************************************************
// submit_hportal - places the op in the hportal's que and also
//     spawns any new connections if needed
//*************************************************************************

int submit_hportal(host_portal_t *hp, op_generic_t *op, int addtotop)
{
   hportal_lock(hp);
   _add_hportal_op(hp, op, addtotop);  //** Add the task
   hportal_unlock(hp);

   //** Now figure out how many new connections are needed, if any
   check_hportal_connections(hp);

   return(0);
}

//*************************************************************************
// submit_hp_que_op - submit an IBP task for execution via a que
//*************************************************************************

int submit_hp_que_op(portal_context_t *hpc, op_generic_t *op)
{
   command_op_t *hop = &(op->op->cmd);

   apr_thread_mutex_lock(hpc->lock);

   //** Check if we should do a garbage run **
   if (hpc->next_check < time(NULL)) {
       hpc->next_check = time(NULL) + hpc->compact_interval;

       apr_thread_mutex_unlock(hpc->lock);
       log_printf(15, "submit_hp_op: Calling compact_hportals\n");
       compact_hportals(hpc);
       apr_thread_mutex_lock(hpc->lock);
   }

//log_printf(1, "submit_hp_op: hpc=%p hpc->table=%p\n",hpc, hpc->table);
   host_portal_t *hp = _lookup_hportal(hpc, hop->hostport);
   if (hp == NULL) {
      log_printf(15, "submit_hp_que_op: New host: %s\n", hop->hostport);
      hp = create_hportal(hpc, hop->connect_context, hop->hostport, hpc->min_threads, hpc->max_threads);
      if (hp == NULL) {
          log_printf(15, "submit_hp_que_op: create_hportal failed!\n");
          return(1);
      }
      log_printf(15, "submit_op: New host.. hp->skey=%s\n", hp->skey);
      apr_hash_set(hpc->table, hp->skey, APR_HASH_KEY_STRING, (const void *)hp);
host_portal_t *hp2 = _lookup_hportal(hpc, hop->hostport);
log_printf(15, "submit_hp_que_op: after lookup hp2=%p\n", hp2);
   }

   apr_thread_mutex_unlock(hpc->lock);

   return(submit_hportal(hp, op, 0));
}

