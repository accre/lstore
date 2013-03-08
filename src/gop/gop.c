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

#define _log_module_index 124

#include <assert.h>
#include <apr_thread_mutex.h>
#include <apr_thread_cond.h>
#include <stdlib.h>
#include <string.h>
#include "type_malloc.h"
#include "log.h"
#include "opque.h"
#include "atomic_counter.h"

//** Defined in opque.c
void _opque_start_execution(opque_t *que);
void _opque_print_stack(Stack_t *stack);
extern pigeon_coop_t *_gop_control;

op_status_t op_success_status = {OP_STATE_SUCCESS, 0};
op_status_t op_failure_status = {OP_STATE_FAILURE, 0};
op_status_t op_retry_status = {OP_STATE_RETRY, 0};
op_status_t op_dead_status = {OP_STATE_DEAD, 0};
op_status_t op_timeout_status = {OP_STATE_TIMEOUT, 0};
op_status_t op_invalid_host_status = {OP_STATE_INVALID_HOST, 0};
op_status_t op_cant_connect_status = {OP_STATE_CANT_CONNECT, 0};
op_status_t op_error_status = {OP_STATE_ERROR, 0};


//***********************************************************************
//***********************************************************************

int gd_shutdown = 0;
apr_thread_t *gd_thread = NULL;
apr_pool_t *gd_pool = NULL;
apr_thread_mutex_t *gd_lock = NULL;
apr_thread_cond_t *gd_cond = NULL;
Stack_t *gd_stack = NULL;

void _gop_dummy_submit_op(void *arg, op_generic_t *op);

static portal_fn_t _gop_dummy_portal = {
  .dup_connect_context = NULL,
  .destroy_connect_context = NULL,
  .connect = NULL,
  .close_connection = NULL,
  .sort_tasks = NULL,
  .submit = _gop_dummy_submit_op
};

static portal_context_t _gop_dummy_pc = {
  .lock = NULL,
  .table = NULL,
  .pool = NULL,
  .running_threads = 1,
  .max_connections = 1,
  .min_threads = 1,
  .max_threads = 1,
  .max_wait = 1,
  .max_workload = 1,
  .compact_interval = 1,
  .wait_stable_time = 1,
  .abort_conn_attempts = 1,
  .check_connection_interval = 1,
  .min_idle = 1,
  .max_retry = 0,
  .count = 0,
  .next_check = 0,
  .dt = 0,
  .arg = NULL,
  .fn = &_gop_dummy_portal
};

//***********************************************************************
// gd_thread_func - gop_dummy execution thread.  Just calls the
//   gop_mark_completed() routine for the gops
//***********************************************************************

void *gd_thread_func(apr_thread_t *th, void *data)
{
  op_generic_t *gop;

  apr_thread_mutex_lock(gd_lock);
  while (gd_shutdown == 0) {
    //** Execute everything on the stack
    while ((gop = (op_generic_t *)pop(gd_stack)) != NULL) {
        log_printf(15, "DUMMY gid=%d status=%d\n", gop_id(gop), gop->base.status);
        apr_thread_mutex_unlock(gd_lock);
        gop_mark_completed(gop, gop->base.status);
        apr_thread_mutex_lock(gd_lock);
    }

    //** Wait for more work
    apr_thread_cond_wait(gd_cond, gd_lock);
  }
  apr_thread_mutex_unlock(gd_lock);

  return(NULL);
}


//***********************************************************************
// gop_dummy_init - Initializes the gop_dummy portal
//***********************************************************************

void gop_dummy_init()
{
  //** Make the variables
  assert(apr_pool_create(&gd_pool, NULL) == APR_SUCCESS);
  assert(apr_thread_mutex_create(&gd_lock, APR_THREAD_MUTEX_DEFAULT, gd_pool) == APR_SUCCESS);
  assert(apr_thread_cond_create(&gd_cond, gd_pool) == APR_SUCCESS);
  gd_stack = new_stack();

  //** and launch the thread
  apr_thread_create(&gd_thread, NULL, gd_thread_func, NULL, gd_pool);
}

//***********************************************************************
// gop_dummy_destroy - Destroys the gop_dummy portal
//***********************************************************************

void gop_dummy_destroy()
{
 apr_status_t tstat;

 //** Signal a shutdown
 apr_thread_mutex_lock(gd_lock);
 gd_shutdown = 1;
 apr_thread_cond_broadcast(gd_cond);
 apr_thread_mutex_unlock(gd_lock);

 //** Wait for the thread to complete
 apr_thread_join(&tstat, gd_thread);

  //** Clean up;
 free_stack(gd_stack, 0);
 apr_thread_mutex_destroy(gd_lock);
 apr_thread_cond_destroy(gd_cond);
 apr_pool_destroy(gd_pool);
}

//***********************************************************************
// _gop_dummy_submit - Dummy submit routine
//***********************************************************************

void _gop_dummy_submit_op(void *arg, op_generic_t *op)
{
  int dolock = 0;

log_printf(15, "gid=%d\n", gop_id(op));
//  if (op->base.cb != NULL) {  //** gop is on a q
     apr_thread_mutex_lock(gd_lock);
     push(gd_stack, op);
     apr_thread_cond_signal(gd_cond);
     apr_thread_mutex_unlock(gd_lock);
     return;
//  }

//*-------* This isn't executed below -----------

  if (apr_thread_mutex_trylock(op->base.ctl->lock) != APR_SUCCESS) dolock = 1;
  unlock_gop(op);

//log_printf(15, "dolock=%d gid=%d err=%d APR_SUCCESS=%d\n", dolock, gop_id(op), err, APR_SUCCESS);
  op->base.started_execution = 1;
  gop_mark_completed(op, op->base.status);

  if (dolock == 1) { lock_gop(op); } //** lock_gop is a macro so need the {}
  return;
}


//***********************************************************************
// dummy free operation
//***********************************************************************

void _gop_dummy_free(op_generic_t *gop, int mode)
{
  gop_generic_free(gop, mode);  //** I free the actual op

  if (mode == OP_DESTROY) free(gop);
}

//***********************************************************************
// gop_dummy - Creates a GOP dummy op with the appropriate state
//   OP_STATE_SUCCESS = Success
//   OP_STATE_FAILIURE = Failure
//***********************************************************************

op_generic_t *gop_dummy(op_status_t state)
{
  op_generic_t *gop;

  type_malloc_clear(gop, op_generic_t, 1);

log_printf(15, " state=%d\n", state); flush_log();

  gop_init(gop);
  gop->base.pc = &_gop_dummy_pc;
  gop->type = Q_TYPE_OPERATION;
//  gop->base.started_execution = 1;
  gop->base.free = _gop_dummy_free;
  gop->base.status = state;
//  gop_mark_completed(gop, state);

  return(gop);
}


//*************************************************************
//  gop_callback_append
//*************************************************************

void gop_callback_append(op_generic_t *gop, callback_t *cb)
{
  lock_gop(gop);
  callback_append(&(gop->base.cb), cb);
  unlock_gop(gop);
}

//*************************************************************
// gop_set_success_state - Sets the success state.  For internal
//   callback use only.  Locking isn't used.
//*************************************************************

void gop_set_success_state(op_generic_t *g, op_status_t state)
{
    g->base.status = state;
}

//*************************************************************
// _gop_completed_successfully - Returns if the task/que completed
//    successfully
//    INTERNAL command does no locking!!!!
//*************************************************************

int _gop_completed_successfully(op_generic_t *g)
{
  int status;

  if (gop_get_type(g) == Q_TYPE_QUE) {
     status = stack_size(g->q->failed);
     status = (status == 0) ? g->base.status.op_status : OP_STATE_FAILURE;
  } else {
    status = g->base.status.op_status;
  }

  return(status);
}

//*************************************************************
// gop_completed_successfully - Returns if the task/que completed
//    successfully
//*************************************************************

int gop_completed_successfully(op_generic_t *g)
{
  int status;

  lock_gop(g);
  status = _gop_completed_successfully(g);
  unlock_gop(g);

  return(status);
}

//*************************************************************
// gop_get_next_finished - Returns the next completed tasks
//*************************************************************

op_generic_t *gop_get_next_finished(op_generic_t *g)
{
  op_generic_t *gop;

  lock_gop(g);
  if (gop_get_type(g) == Q_TYPE_QUE) {
    gop = (op_generic_t *)pop(g->q->finished);
  } else {
    gop = NULL;
    if (g->base.failure_mode != OP_FM_GET_END) {
       g->base.failure_mode = OP_FM_GET_END;
       gop = g;
    }
  }
  unlock_gop(g);

  return(gop);
}

//*************************************************************
// gop_get_next_failed - returns a failed task list from the provided que
//      or NULL if none exist.
//*************************************************************

op_generic_t *gop_get_next_failed(op_generic_t *g)
{
  op_generic_t *gop = NULL;

  lock_gop(g);
  if (gop_get_type(g) == Q_TYPE_QUE) {
    gop = (op_generic_t *)pop(g->q->failed);
  } else {
    gop = NULL;
    if (g->base.failure_mode != OP_FM_GET_END) {
       g->base.failure_mode = OP_FM_GET_END;
       if (g->base.status.op_status != OP_STATE_SUCCESS) gop = g;
    }
  }
  unlock_gop(g);

  return(gop);
}

//*************************************************************
// gop_tasks_failed- Returns the # of errors left in the
//    failed task/que
//*************************************************************

int gop_tasks_failed(op_generic_t *g)
{
  int nf;

  lock_gop(g);
  if (gop_get_type(g) == Q_TYPE_QUE) {
    nf = stack_size(g->q->failed);
  } else {
    nf = (g->base.status.op_status == OP_STATE_SUCCESS) ? 0 : 1;
  }
  unlock_gop(g);

  return(nf);
}

//*************************************************************
// gop_tasks_finished- Returns the # of tasks finished
//*************************************************************

int gop_tasks_finished(op_generic_t *g)
{
  int nf;

  lock_gop(g);
  if (gop_get_type(g) == Q_TYPE_QUE) {
    nf = stack_size(g->q->finished);
  } else {
    nf = (g->base.state == 1) ? 1 : 0;
  }
  unlock_gop(g);

  return(nf);
}


//*************************************************************
// gop_tasks_left - Returns the number of tasks remaining
//*************************************************************

int gop_tasks_left(op_generic_t *g)
{
  int n;

  lock_gop(g);
  if (gop_get_type(g) == Q_TYPE_QUE) {
    n = g->q->nleft;
  } else {
    n = (g->base.state == 1) ? 0 : 1;
  }
  unlock_gop(g);

  return(n);
}

//*************************************************************
// _gop_start_execution - Submit tasks for execution (No locking)
//*************************************************************

void _gop_start_execution(op_generic_t *g)
{
  if (gop_get_type(g) == Q_TYPE_QUE) {
    _opque_start_execution(g->q->opque);
  } else if (g->base.started_execution == 0) {
    log_printf(15, "gid=%d started_execution=%d\n", gop_get_id(g), g->base.started_execution);
    g->base.started_execution = 1;
    g->base.pc->fn->submit(g->base.pc->arg, g);
  }
}

//*************************************************************
// gop_start_execution - Submit tasks for execution
//*************************************************************

void gop_start_execution(op_generic_t *g)
{
  lock_gop(g);
  _gop_start_execution(g);
  unlock_gop(g);
}

//*************************************************************
// gop_set_exec_mode - SEts the gop's execution mode
//*************************************************************

void gop_set_exec_mode(op_generic_t *g, int mode)
{
  if (gop_get_type(g) == Q_TYPE_OPERATION) {
     g->base.execution_mode = mode;
  }
}


//*************************************************************
// gop_finished_submission - Mark que to stop accepting
//     tasks.
//*************************************************************

void gop_finished_submission(op_generic_t *g)
{
  lock_gop(g);
  if (gop_get_type(g) == Q_TYPE_QUE) {
    g->q->finished_submission = 1;

    //** If nothing left to do trigger the condition in case anyone's waiting
    if (g->q->nleft == 0) { apr_thread_cond_broadcast(g->base.ctl->cond); }
  }
  unlock_gop(g);
}

//*************************************************************
// gop_wait - waits until the gop completes
//*************************************************************

int gop_wait(op_generic_t *gop)
{
 op_status_t status;
//log_printf(15, "gop_wait: START gid=%d state=%d\n", gop_id(gop), gop->base.state);

  lock_gop(gop);

//log_printf(15, "gop_wait: after lock gid=%d state=%d\n", gop_id(gop), gop->base.state);

  while (gop->base.state == 0) {
log_printf(15, "gop_wait: WHILE gid=%d state=%d\n", gop_id(gop), gop->base.state);
     apr_thread_cond_wait(gop->base.ctl->cond, gop->base.ctl->lock); //** Sleep until something completes
  }

  status = gop_get_status(gop);
//  state = _gop_completed_successfully(gop);
log_printf(15, "gop_wait: FINISHED gid=%d status=%d err=%d\n", gop_id(gop), status.op_status, status.error_code);

  unlock_gop(gop);

  return(status.op_status);
}

//*************************************************************
// gop_free - Frees an opque or a gop
//*************************************************************

void gop_free(op_generic_t *gop, int mode)
{
  int type;

log_printf(15, "gop_free: gid=%d tid=%d\n", gop_id(gop), atomic_thread_id);
  //** Get the status
  type = gop_get_type(gop);
  if (type == Q_TYPE_QUE) {
     opque_free(gop->q->opque, mode);
  } else {
     //** the gop is locked in gop_generic_free before freeing
     gop->base.free(gop, mode);
  }
}

//*************************************************************
// gop_set_auto_destroy - Sets the auto destroy mode.
//  NOTE:  If the gop has completed already the gop is destroyed
//*************************************************************

void gop_set_auto_destroy(op_generic_t *gop, int val)
{
  int state;

  lock_gop(gop);
  gop->base.auto_destroy = val;
  state = gop->base.state;
  unlock_gop(gop);

  //** Already completed go ahead and destroy it
  if (state == 1) gop_free(gop, OP_DESTROY);
}


//*************************************************************
// gop_waitany - waits until any given task completes and
//   returns the operation.
//*************************************************************

op_generic_t *gop_waitany(op_generic_t *g)
{
  op_generic_t *gop = g;

  lock_gop(g);
  _gop_start_execution(g);  //** Make sure things have been submitted

  if (gop_get_type(g) == Q_TYPE_QUE) {
log_printf(15, "gop_waitany: BEFORE WHILE qid=%d nleft=%d finished=%d\n", gop_id(g), g->q->nleft, stack_size(g->q->finished));
      while (((gop = (op_generic_t *)pop(g->q->finished)) == NULL) && (g->q->nleft > 0)) {
log_printf(15, "gop_waitany: WHILE qid=%d nleft=%d finished=%d gop=%p\n", gop_id(g), g->q->nleft, stack_size(g->q->finished), gop);
        apr_thread_cond_wait(g->base.ctl->cond, g->base.ctl->lock); //** Sleep until something completes
log_printf(15, "gop_waitany: after cond qid=%d nleft=%d finished=%d\n", gop_id(g), g->q->nleft, stack_size(g->q->finished));
     }
log_printf(15, "gop_waitany: AFTER qid=%d nleft=%d finished=%d gop=%p\n", gop_id(g), g->q->nleft, stack_size(g->q->finished), gop);

if (gop != NULL) log_printf(15, "POP finished qid=%d gid=%d\n", gop_id(g), gop_id(gop));
log_printf(15, "Printing qid=%d finished stack\n", gop_id(g));
_opque_print_stack(g->q->finished);
  } else {
log_printf(15, "gop_waitany: BEFORE (type=op) While gid=%d state=%d\n", gop_id(g), g->base.state); flush_log();

     while (g->base.state == 0) {
   log_printf(15, "gop_waitany: WHILE gid=%d state=%d\n", gop_id(g), g->base.state); flush_log();
        apr_thread_cond_wait(g->base.ctl->cond, g->base.ctl->lock); //** Sleep until something completes
     }
log_printf(15, "gop_waitany: AFTER (type=op) While gid=%d state=%d\n", gop_id(g), g->base.state); flush_log();
  }
  unlock_gop(g);

  return(gop);
}

//*************************************************************
// gop_waitall - waits until all the tasks are completed
//    It returns op_status if all the tasks completed without problems or
//    with the last error otherwise.
//*************************************************************

int gop_waitall(op_generic_t *g)
{
  int status;

log_printf(15, "START gid=%d type=%d\n", gop_id(g), gop_get_type(g));
  lock_gop(g);
  _gop_start_execution(g);  //** Make sure things have been submitted

  if (gop_get_type(g) == Q_TYPE_QUE) {
     while (g->q->nleft > 0) {
        apr_thread_cond_wait(g->base.ctl->cond, g->base.ctl->lock); //** Sleep until something completes
     }
  } else {
     while (g->base.state == 0) {
   log_printf(15, "gop_waitall: WHILE gid=%d state=%d\n", gop_id(g), g->base.state);
        apr_thread_cond_wait(g->base.ctl->cond, g->base.ctl->lock); //** Sleep until something completes
     }
  }

  status = _gop_completed_successfully(g);

log_printf(15, "END gid=%d type=%d\n", gop_id(g), gop_get_type(g));
  unlock_gop(g);

  return(status);
}


//*************************************************************
// gop_timed_waitany - waits until any given task completes and
//   returns the operation.
//*************************************************************

op_generic_t *gop_timed_waitany(op_generic_t *g, int dt)
{
  op_generic_t *gop = NULL;
  apr_interval_time_t adt = apr_time_from_sec(dt);
  int loop;

  lock_gop(g);
  _gop_start_execution(g);  //** Make sure things have been submitted

  loop = 0;
  if (gop_get_type(g) == Q_TYPE_QUE) {
      while (((gop = (op_generic_t *)pop(g->q->finished)) == NULL) && (g->q->nleft > 0) && (loop == 0)) {
        apr_thread_cond_timedwait(g->base.ctl->cond, g->base.ctl->lock, adt); //** Sleep until something completes
        loop++;
     }
  } else {
     while ((g->base.state == 0) && (loop == 0)) {
        apr_thread_cond_timedwait(g->base.ctl->cond, g->base.ctl->lock, adt); //** Sleep until something completes
        loop++;
     }

     if (g->base.state != 0) gop = g;
  }
  unlock_gop(g);

  return(gop);
}

//*************************************************************
// gop_timed_waitall - waits until all the tasks are completed
//    It returns op_status if all the tasks completed without problems or
//    with the last error otherwise.
//*************************************************************

int gop_timed_waitall(op_generic_t *g, int dt)
{
  int status;
  int loop;
  apr_interval_time_t adt = apr_time_from_sec(dt);

  lock_gop(g);
  _gop_start_execution(g);  //** Make sure things have been submitted

  loop = 0;
  if (gop_get_type(g) == Q_TYPE_QUE) {
     while ((g->q->nleft > 0) && (loop == 0)) {
        apr_thread_cond_timedwait(g->base.ctl->cond, g->base.ctl->lock, adt); //** Sleep until something completes
        loop++;
     }

     status = (g->q->nleft > 0) ? OP_STATE_RETRY : _gop_completed_successfully(g);
  } else {
     while ((g->base.state == 0) && (loop == 0)) {
        apr_thread_cond_timedwait(g->base.ctl->cond, g->base.ctl->lock, adt); //** Sleep until something completes
        loop++;
     }

     status = (g->base.state == 0) ? OP_STATE_RETRY : _gop_completed_successfully(g);
  }

  unlock_gop(g);

  return(status);
}


//*************************************************************
// single_gop_mark_completed - Marks a single operation as completed and 
//   triggers any callbacks if needed
//*************************************************************

void single_gop_mark_completed(op_generic_t *gop, op_status_t status)
{
  op_common_t *base = &(gop->base);
  int mode;

log_printf(15, "gop_mark_completed: START gid=%d status=%d\n", gop_id(gop), status);

  lock_gop(gop);
log_printf(15, "gop_mark_completed: after lock gid=%d\n", gop_id(gop));

  //** Store the status
  base->status = status;

  //** and trigger any callbacks

log_printf(15, "gop_mark_completed: before cb gid=%d op_status=%d\n", gop_id(gop), base->status.op_status);

  callback_execute(base->cb, base->status.op_status);

log_printf(15, "gop_mark_completed: after cb gid=%d op_success=%d\n", gop_id(gop), base->status.op_status);

  base->state = 1;

  //** Lastly trigger the signal. for anybody listening
  apr_thread_cond_broadcast(gop->base.ctl->cond);

log_printf(15, "gop_mark_completed: after brodcast gid=%d\n", gop_id(gop));

  mode = gop_get_auto_destroy(gop);  //** Get the auto destroy status w/in the lock

  unlock_gop(gop);

  //** Check if we do an auto cleanop
  if (mode == 1) gop_free(gop, OP_DESTROY);
}

//*************************************************************
// gop_mark_completed - Marks the operation as completed and 
//   triggers any callbacks if needed
//*************************************************************

void gop_mark_completed(op_generic_t *gop, op_status_t status)
{
  command_op_t *cop;
  op_generic_t *sgop;

  //** Process any slaved ops first
//  lock_gop(gop);
  if (gop->op != NULL) {
     cop = &(gop->op->cmd);
    if (cop->coalesced_ops != NULL) {
      while ((sgop = (op_generic_t *)pop(cop->coalesced_ops)) != NULL) {
        single_gop_mark_completed(sgop, status);
      }
    }
  }
//  unlock_gop(gop);

  //** And lastly the initial op that triggered the coalescing
  //** It's done last to do any coalescing cleanup.
  single_gop_mark_completed(gop, status);
}

//*************************************************************
// gop_sync_exec - Quick way to exec a command when you
//    are only concerned with success/failure
//*************************************************************

int gop_sync_exec(op_generic_t *gop)
{
  int err;

  log_printf(15, "waiting for gid=%d to complete\n", gop_id(gop)); 
  err = gop_waitall(gop);
  log_printf(15, "gid=%d completed with err=%d\n", gop_id(gop), err);
  gop_free(gop, OP_DESTROY);
  log_printf(15, "After gop destruction\n"); 

  return(err);
}

//*************************************************************
// gop_reset - Resets an already inited gop
//*************************************************************

void gop_reset(op_generic_t *gop)
{
  gop->base.id = atomic_global_counter();

  unlock_gop(gop);
  gop->base.cb = NULL;
  gop->base.state = 0;
  gop->base.status = op_failure_status;
  gop->base.started_execution = 0;
  gop->base.auto_destroy = 0;
} 


//*************************************************************
// gop_init - Initializes a generic op
//*************************************************************

void gop_init(op_generic_t *gop)
{
  pigeon_coop_hole_t pch;

  op_common_t *base = &(gop->base);

  type_memclear(gop, op_generic_t, 1);

  base->id = atomic_global_counter();

log_printf(15, "gop ptr=%p gid=%d\n", gop, gop_id(gop));

  //** Get the control struct
  pch = reserve_pigeon_coop_hole(_gop_control);
  gop->base.ctl = (gop_control_t *)pigeon_coop_hole_data(&pch);
  gop->base.ctl->pch = pch;
}    


//*************************************************************
// gop_generic_free - Frees the data generic internal op data
//*************************************************************

void gop_generic_free(op_generic_t *gop, int mode)
{
log_printf(20, "op_generic_free: before lock gid=%d\n", gop_get_id(gop));
  lock_gop(gop);  //** Make sure I own the lock just to be safe
log_printf(20, "op_generic_free: AFTER lock gid=%d\n", gop_get_id(gop));

  callback_destroy(gop->base.cb);  //** Free the callback chain as well

  unlock_gop(gop);  //** Make sure the lock is left in the proper state for reuse
  release_pigeon_coop_hole(_gop_control, &(gop->base.ctl->pch));
}








