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

#define _log_module_index 122

#include <assert.h>
#include <apr_pools.h>
#include <apr_thread_proc.h>
#include <apr_thread_pool.h>
#include "apr_wrapper.h"
#include "opque.h"
#include "thread_pool.h"
#include "network.h"
#include "log.h"
#include "type_malloc.h"
#include "hwinfo.h"

void  *thread_pool_exec_fn(apr_thread_t *th, void *arg);
void *_tp_dup_connect_context(void *connect_context);
void _tp_destroy_connect_context(void *connect_context);
int _tp_connect(NetStream_t *ns, void *connect_context, char *host, int port, Net_timeout_t timeout);
void _tp_close_connection(NetStream_t *ns);

void _tp_op_free(op_generic_t *op, int mode);
void _tp_submit_op(void *arg, op_generic_t *op);

static portal_fn_t _tp_base_portal = {
  .dup_connect_context = _tp_dup_connect_context,
  .destroy_connect_context = _tp_destroy_connect_context,
  .connect = _tp_connect,
  .close_connection = _tp_close_connection,
  .sort_tasks = default_sort_ops,
  .submit = _tp_submit_op,
  .sync_exec = thread_pool_exec_fn
};

atomic_int_t _tp_context_count = 0;
apr_thread_mutex_t *_tp_lock = NULL;
apr_pool_t *_tp_pool = NULL;
//int _tp_id = 0;

//*************************************************************

portal_fn_t default_tp_imp() { return(_tp_base_portal); }


//*************************************************************

void _tp_op_free(op_generic_t *gop, int mode)
{
  thread_pool_op_t *top = gop_get_tp(gop);
  int id = gop_id(gop);

  log_printf(15, "_tp_op_free: mode=%d gid=%d gop=%p\n", mode, gop_id(gop), gop); flush_log();

  if (top->my_op_free != NULL) top->my_op_free(top->arg);

  gop_generic_free(gop, OP_FINALIZE);  //** I free the actual op

  if (top->dop.cmd.hostport) free(top->dop.cmd.hostport);

  if (mode == OP_DESTROY) free(gop->free_ptr);
  log_printf(15, "_tp_op_free: gid=%d END\n", id); flush_log();

}

//*************************************************************

void _tp_submit_op(void *arg, op_generic_t *gop)
{
 thread_pool_op_t *op = gop_get_tp(gop);
 apr_status_t aerr;

 log_printf(15, "_tp_submit_op: gid=%d\n", gop_id(gop));

atomic_inc(op->tpc->n_submitted);

  aerr = apr_thread_pool_push(op->tpc->tp, thread_pool_exec_fn, gop, APR_THREAD_TASK_PRIORITY_NORMAL, NULL);

  if (aerr != APR_SUCCESS) {
    log_printf(0, "ERROR submiting task!  aerr=%d gid=%d\n", aerr, gop_id(gop));
  }
}

//********************************************************************

void *_tp_dup_connect_context(void *connect_context)
{
  return(NULL);
}

//********************************************************************

void _tp_destroy_connect_context(void *connect_context)
{
  return;
}

//**********************************************************

int _tp_connect(NetStream_t *ns, void *connect_context, char *host, int port, Net_timeout_t timeout)
{
  ns->id = ns_generate_id();
  return(0);
}


//**********************************************************

void _tp_close_connection(NetStream_t *ns)
{
  return;
}

//*************************************************************
// thread_pool_direct - Bypasses the _tp_exec GOP wrapper
//     and directly submits the task to the APR thread pool
//*************************************************************

int thread_pool_direct(thread_pool_context_t *tpc, apr_thread_start_t fn, void *arg)
{
  int err = apr_thread_pool_push(tpc->tp, fn, arg, APR_THREAD_TASK_PRIORITY_NORMAL, NULL);

  atomic_inc(tpc->n_direct);

log_printf(0, "tpd=%d\n", atomic_get(tpc->n_direct));
  if (err != APR_SUCCESS) {
    log_printf(0, "ERROR submiting task!  err=%d\n", err);
  }

  return((err == APR_SUCCESS) ? 0 : 1);
}

//**********************************************************
// default_thread_pool_config - Sets the default thread pool config options
//**********************************************************

void default_thread_pool_config(thread_pool_context_t *tpc)
{
  int sockets, cores, vcores;

  proc_info(&sockets, &cores, &vcores);

  tpc->min_idle = 1; //** default to close after 1 sec
  tpc->min_threads = 1;
  tpc->max_threads = cores;

//log_printf(15, "default_thread_pool_config: max_threads=%d\n", cores);
  tpc->name = NULL;
}


//**********************************************************
//  thread_pool_create_context - Creates a TP context
//**********************************************************

thread_pool_context_t *thread_pool_create_context(char *tp_name, int min_threads, int max_threads)
{
//  char buffer[1024];
  thread_pool_context_t *tpc;
  apr_interval_time_t dt;

  log_printf(15, "count=%d\n", _tp_context_count);

  type_malloc_clear(tpc, thread_pool_context_t, 1);

  assert(apr_wrapper_start() == APR_SUCCESS);

  if (atomic_inc(_tp_context_count) == 0) {
     apr_pool_create(&_tp_pool, NULL);
     apr_thread_mutex_create(&_tp_lock, APR_THREAD_MUTEX_DEFAULT, _tp_pool);
     init_opque_system();
  }

  tpc->pc = create_hportal_context(&_tp_base_portal);  //** Really just used for the submit

  default_thread_pool_config(tpc);
  if (min_threads > 0) tpc->min_threads = min_threads;
  if (max_threads > 0) tpc->max_threads = max_threads;

  dt = tpc->min_idle * 1000000;
  assert(apr_thread_pool_create(&(tpc->tp), tpc->min_threads, tpc->max_threads, _tp_pool) == APR_SUCCESS);
  apr_thread_pool_idle_wait_set(tpc->tp, dt);
  apr_thread_pool_threshold_set(tpc->tp, 0);

  tpc->name = (tp_name == NULL) ? NULL : strdup(tp_name);
  atomic_set(tpc->n_ops, 0);
atomic_set(tpc->n_completed, 0);
atomic_set(tpc->n_started, 0);
atomic_set(tpc->n_submitted, 0);

//  _tp_context_count++;

  return(tpc);
}


//**********************************************************
//  thread_pool_destroy_context - Shuts down the Thread pool system
//**********************************************************

void thread_pool_destroy_context(thread_pool_context_t *tpc)
{
  log_printf(15, "thread_pool_destroy_context: Shutting down! count=%d\n", _tp_context_count);

log_printf(15, "tpc->name=%s  high=%d idle=%d\n", tpc->name, apr_thread_pool_threads_high_count(tpc->tp),  apr_thread_pool_threads_idle_timeout_count(tpc->tp));
  destroy_hportal_context(tpc->pc);

  apr_thread_pool_destroy(tpc->tp);

  if (atomic_dec(_tp_context_count) == 0) {
     destroy_opque_system();
     apr_thread_mutex_destroy(_tp_lock);
     apr_pool_destroy(_tp_pool);
  }

  apr_wrapper_stop();

  if (tpc->name != NULL) free(tpc->name);

  free(tpc);
}

