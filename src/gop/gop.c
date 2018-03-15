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

#define _log_module_index 124

#include <apr_thread_cond.h>
#include <gop/portal.h>
#include <stdlib.h>
#include <tbx/atomic_counter.h>
#include <tbx/log.h>
#include <tbx/pigeon_coop.h>
#include <tbx/stack.h>
#include <tbx/type_malloc.h>

#include "callback.h"
#include "gop.h"
#include "gop/opque.h"
#include "gop/types.h"

//** Defined in opque.c
void _opque_start_execution(gop_opque_t *que);
void _opque_print_stack(tbx_stack_t *stack);
extern tbx_pc_t *_gop_control;

gop_op_status_t gop_success_status = {OP_STATE_SUCCESS, 0};
gop_op_status_t gop_failure_status = {OP_STATE_FAILURE, 0};
gop_op_status_t op_retry_status = {OP_STATE_RETRY, 0};
gop_op_status_t op_dead_status = {OP_STATE_DEAD, 0};
gop_op_status_t op_timeout_status = {OP_STATE_TIMEOUT, 0};
gop_op_status_t op_invalid_host_status = {OP_STATE_INVALID_HOST, 0};
gop_op_status_t op_cant_connect_status = {OP_STATE_FAILURE, OP_STATE_CANT_CONNECT};
gop_op_status_t gop_error_status = {OP_STATE_ERROR, 0};

//*************************************************************
//  gop_callback_append
//*************************************************************

void gop_callback_append(gop_op_generic_t *gop, gop_callback_t *cb)
{
    lock_gop(gop);
    callback_append(&(gop->base.cb), cb);
    unlock_gop(gop);
}

//*************************************************************
// gop_set_success_state - Sets the success state.  For internal
//   callback use only.  Locking isn't used.
//*************************************************************

void gop_set_success_state(gop_op_generic_t *g, gop_op_status_t state)
{
    g->base.status = state;
}

//*************************************************************
// _gop_completed_successfully - Returns if the task/que completed
//    successfully
//    INTERNAL command does no locking!!!!
//*************************************************************

int _gop_completed_successfully(gop_op_generic_t *g)
{
    int status;

    if (gop_get_type(g) == Q_TYPE_QUE) {
        status = tbx_stack_count(g->q->failed);
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

int gop_completed_successfully(gop_op_generic_t *g)
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

gop_op_generic_t *gop_get_next_finished(gop_op_generic_t *g)
{
    gop_op_generic_t *gop;

    lock_gop(g);
    if (gop_get_type(g) == Q_TYPE_QUE) {
        gop = (gop_op_generic_t *)tbx_stack_pop(g->q->finished);
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

gop_op_generic_t *gop_get_next_failed(gop_op_generic_t *g)
{
    gop_op_generic_t *gop = NULL;

    lock_gop(g);
    if (gop_get_type(g) == Q_TYPE_QUE) {
        gop = (gop_op_generic_t *)tbx_stack_pop(g->q->failed);
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

int gop_tasks_failed(gop_op_generic_t *g)
{
    int nf;

    lock_gop(g);
    if (gop_get_type(g) == Q_TYPE_QUE) {
        nf = tbx_stack_count(g->q->failed);
    } else {
        nf = (g->base.status.op_status == OP_STATE_SUCCESS) ? 0 : 1;
    }
    unlock_gop(g);

    return(nf);
}

//*************************************************************
// gop_tasks_finished- Returns the # of tasks finished
//*************************************************************

int gop_tasks_finished(gop_op_generic_t *g)
{
    int nf;

    lock_gop(g);
    if (gop_get_type(g) == Q_TYPE_QUE) {
        nf = tbx_stack_count(g->q->finished);
    } else {
        nf = (g->base.state == 1) ? 1 : 0;
    }
    unlock_gop(g);

    return(nf);
}


//*************************************************************
// gop_tasks_left - Returns the number of tasks remaining
//*************************************************************

int gop_tasks_left(gop_op_generic_t *g)
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

void _gop_start_execution(gop_op_generic_t *g)
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

void gop_start_execution(gop_op_generic_t *g)
{
    lock_gop(g);
    _gop_start_execution(g);
    unlock_gop(g);
}

//*************************************************************
// gop_finished_submission - Mark que to stop accepting
//     tasks.
//*************************************************************

void gop_finished_submission(gop_op_generic_t *g)
{
    lock_gop(g);
    if (gop_get_type(g) == Q_TYPE_QUE) {
        g->q->finished_submission = 1;

        //** If nothing left to do trigger the condition in case anyone's waiting
        if (g->q->nleft == 0) {
            apr_thread_cond_broadcast(g->base.ctl->cond);
        }
    }
    unlock_gop(g);
}

//*************************************************************
// gop_wait - waits until the gop completes
//*************************************************************

int gop_wait(gop_op_generic_t *gop)
{
    gop_op_status_t status;
    lock_gop(gop);
    while (gop->base.state == 0) {
        log_printf(15, "gop_wait: WHILE gid=%d state=%d\n", gop_id(gop), gop->base.state);
        apr_thread_cond_wait(gop->base.ctl->cond, gop->base.ctl->lock); //** Sleep until something completes
    }

    status = gop_get_status(gop);
    log_printf(15, "gop_wait: FINISHED gid=%d status=%d err=%d\n", gop_id(gop), status.op_status, status.error_code);

    unlock_gop(gop);

    return(status.op_status);
}

//*************************************************************
// gop_free - Frees an opque or a gop
//*************************************************************

void gop_free(gop_op_generic_t *gop, gop_op_free_mode_t mode)
{
    int type;

    log_printf(15, "gop_free: gid=%d tid=%d\n", gop_id(gop), tbx_atomic_thread_id);
    //** Get the status
    type = gop_get_type(gop);
    if (type == Q_TYPE_QUE) {
        gop_opque_free(gop->q->opque, mode);
    } else {
        //** the gop is locked in gop_generic_free before freeing
        gop->base.free(gop, mode);
    }
}

//*************************************************************
// gop_set_auto_destroy - Sets the auto destroy mode.
//  NOTE:  If the gop has completed already the gop is destroyed
//*************************************************************

void gop_set_auto_destroy(gop_op_generic_t *gop, int val)
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
// gop_will_block - Returns 1 if a gop_waitany will block
//*************************************************************

int gop_will_block(gop_op_generic_t *g)
{
    int status = 0;

    lock_gop(g);
    if (gop_get_type(g) == Q_TYPE_QUE) {
        if ((tbx_stack_count(g->q->finished) == 0) && (g->q->nleft > 0)) status = 1;
    } else {
        if (g->base.state == 0) status = 1;
    }
    unlock_gop(g);

    return(status);
}

//*************************************************************
// gop_waitany - waits until any given task completes and
//   returns the operation.
//*************************************************************

gop_op_generic_t *gop_waitany(gop_op_generic_t *g)
{
    gop_op_generic_t *gop = g;
    gop_callback_t *cb;

    lock_gop(g);

    if (gop_get_type(g) == Q_TYPE_QUE) {
        log_printf(15, "sync_exec_que_check gid=%d stack_size=%d started_exec=%d\n", gop_id(g), tbx_stack_count(g->q->opque->qd.list), g->base.started_execution);
        if ((tbx_stack_count(g->q->opque->qd.list) == 1) && (g->base.started_execution == 0)) {  //** See if we can directly exec
            g->base.started_execution = 1;
            cb = (gop_callback_t *)tbx_stack_pop(g->q->opque->qd.list);
            gop = (gop_op_generic_t *)cb->priv;
            log_printf(15, "sync_exec_que -- waiting for pgid=%d cgid=%d to complete\n", gop_id(g), gop_id(gop));
            unlock_gop(g);
            gop_waitany(gop);
            tbx_stack_pop(g->q->finished); //** Remove it from the finished list.
            return(gop);
        } else {
            _gop_start_execution(g);  //** Make sure things have been submitted
            while (((gop = (gop_op_generic_t *)tbx_stack_pop(g->q->finished)) == NULL) && (g->q->nleft > 0)) {
                apr_thread_cond_wait(g->base.ctl->cond, g->base.ctl->lock); //** Sleep until something completes
            }
        }

        if (gop != NULL) log_printf(15, "POP finished qid=%d gid=%d\n", gop_id(g), gop_id(gop));
    } else {
        log_printf(15, "gop_waitany: BEFORE (type=op) While gid=%d state=%d\n", gop_id(g), g->base.state);
        tbx_log_flush();
        if ((g->base.pc->fn->sync_exec != NULL) && (g->base.started_execution == 0)) {  //** See if we can directly exec
            unlock_gop(g);  //** Don't need this for a direct exec
            log_printf(15, "sync_exec -- waiting for gid=%d to complete\n", gop_id(g));
            g->base.pc->fn->sync_exec(g->base.pc, g);
            log_printf(15, "sync_exec -- gid=%d completed with err=%d\n", gop_id(g), g->base.state);
            return(g);
        } else {  //** Got to submit it normally
            unlock_gop(g);  //** It's a single task so no need to hold the lock.  Otherwise we can deadlock
            _gop_start_execution(g);  //** Make sure things have been submitted
            lock_gop(g);  //** but we do need it for detecting when we're finished.
            while (g->base.state == 0) {
                apr_thread_cond_wait(g->base.ctl->cond, g->base.ctl->lock); //** Sleep until something completes
            }
        }
        log_printf(15, "gop_waitany: AFTER (type=op) While gid=%d state=%d\n", gop_id(g), g->base.state);
        tbx_log_flush();
    }
    unlock_gop(g);

    return(gop);
}

//*************************************************************
// gop_waitall - waits until all the tasks are completed
//    It returns op_status if all the tasks completed without problems or
//    with the last error otherwise.
//*************************************************************

int gop_waitall(gop_op_generic_t *g)
{
    int status;
    gop_op_generic_t *g2;
    gop_callback_t *cb;

    log_printf(5, "START gid=%d type=%d\n", gop_id(g), gop_get_type(g));
    lock_gop(g);

    if (gop_get_type(g) == Q_TYPE_QUE) {
        log_printf(15, "sync_exec_que_check gid=%d stack_size=%d started_exec=%d\n", gop_id(g), tbx_stack_count(g->q->opque->qd.list), g->base.started_execution);

        if ((tbx_stack_count(g->q->opque->qd.list) == 1) && (g->base.started_execution == 0)) {  //** See if we can directly exec
            log_printf(15, "sync_exec_que -- waiting for gid=%d to complete\n", gop_id(g));
            cb = (gop_callback_t *)tbx_stack_pop(g->q->opque->qd.list);
            g2 = (gop_op_generic_t *)cb->priv;
            unlock_gop(g);  //** Don't need this for a direct exec
            status = gop_waitall(g2);
            log_printf(15, "sync_exec -- gid=%d completed with err=%d\n", gop_id(g), status);
            return(status);
        } else {  //** Got to submit it normally
            _gop_start_execution(g);  //** Make sure things have been submitted
            while (g->q->nleft > 0) {
                apr_thread_cond_wait(g->base.ctl->cond, g->base.ctl->lock); //** Sleep until something completes
            }
        }
    } else {     //** Got a single task
        if ((g->base.pc->fn->sync_exec != NULL) && (g->base.started_execution == 0)) {  //** See if we can directly exec
            unlock_gop(g);  //** Don't need this for a direct exec
            log_printf(15, "sync_exec -- waiting for gid=%d to complete\n", gop_id(g));
            g->base.pc->fn->sync_exec(g->base.pc, g);
            status = _gop_completed_successfully(g);
            log_printf(15, "sync_exec -- gid=%d completed with err=%d\n", gop_id(g), status);
            return(status);
        } else {  //** Got to submit it the normal way
            _gop_start_execution(g);  //** Make sure things have been submitted
            while (g->base.state == 0) {
                log_printf(15, "gop_waitall: WHILE gid=%d state=%d\n", gop_id(g), g->base.state);
                apr_thread_cond_wait(g->base.ctl->cond, g->base.ctl->lock); //** Sleep until something completes
            }
        }
    }

    status = _gop_completed_successfully(g);

    log_printf(15, "END gid=%d type=%d\n", gop_id(g), gop_get_type(g));
    unlock_gop(g);

    return(status);
}


//*************************************************************
// gop_waitany_timed - waits until any given task completes and
//   returns the operation.
//*************************************************************

gop_op_generic_t *gop_waitany_timed(gop_op_generic_t *g, int dt)
{
    gop_op_generic_t *gop = NULL;
    apr_interval_time_t adt = apr_time_from_sec(dt);
    int loop;

    lock_gop(g);
    _gop_start_execution(g);  //** Make sure things have been submitted

    loop = 0;
    if (gop_get_type(g) == Q_TYPE_QUE) {
        while (((gop = (gop_op_generic_t *)tbx_stack_pop(g->q->finished)) == NULL) && (g->q->nleft > 0) && (loop == 0)) {
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

int gop_timed_waitall(gop_op_generic_t *g, int dt)
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

void single_gop_mark_completed(gop_op_generic_t *gop, gop_op_status_t status)
{
    gop_op_common_t *base = &(gop->base);
    int mode;

    log_printf(15, "gop_mark_completed: START gid=%d status=%d\n", gop_id(gop), status.op_status);

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

void gop_mark_completed(gop_op_generic_t *gop, gop_op_status_t status)
{
    gop_command_op_t *cop;
    gop_op_generic_t *sgop;

    //** Process any slaved ops first
    if (gop->op != NULL) {
        cop = &(gop->op->cmd);
        if (cop->coalesced_ops != NULL) {
            while ((sgop = (gop_op_generic_t *)tbx_stack_pop(cop->coalesced_ops)) != NULL) {
                single_gop_mark_completed(sgop, status);
            }
        }
    }

    //** And lastly the initial op that triggered the coalescing
    //** It's done last to do any coalescing cleanup.
    single_gop_mark_completed(gop, status);
}

//*************************************************************
// gop_sync_exec - Quick way to exec a command when you
//    are only concerned with success/failure
//*************************************************************

int gop_sync_exec(gop_op_generic_t *gop)
{
    int err;

    if (gop->type == Q_TYPE_OPERATION) { //** Got an operation so see if we can directly exec it
        if (gop->base.pc->fn->sync_exec != NULL) {  //** Yup we can!
            log_printf(15, "sync_exec -- waiting for gid=%d to complete\n", gop_id(gop));
            gop->base.pc->fn->sync_exec(gop->base.pc, gop);
            err = _gop_completed_successfully(gop);
            log_printf(15, "sync_exec -- gid=%d completed with err=%d\n", gop_id(gop), err);
            gop_free(gop, OP_DESTROY);
            return(err);
        }
    }

    log_printf(15, "waiting for gid=%d to complete\n", gop_id(gop));
    err = gop_waitall(gop);
    log_printf(15, "gid=%d completed with err=%d\n", gop_id(gop), err);
    gop_free(gop, OP_DESTROY);
    log_printf(15, "After gop destruction\n");

    return(err);
}


//*************************************************************
// gop_sync_exec_status - Quick way to exec a command that just returns
//   the gop status
//*************************************************************

gop_op_status_t gop_sync_exec_status(gop_op_generic_t *gop)
{
    int err;
    gop_op_status_t status;

    if (gop->type == Q_TYPE_OPERATION) { //** Got an operation so see if we can directly exec it
        if (gop->base.pc->fn->sync_exec != NULL) {  //** Yup we can!
            log_printf(15, "sync_exec -- waiting for gid=%d to complete\n", gop_id(gop));
            gop->base.pc->fn->sync_exec(gop->base.pc, gop);
            status = gop->base.status;
            log_printf(15, "sync_exec -- gid=%d completed with err=%d\n", gop_id(gop), status.op_status);
            gop_free(gop, OP_DESTROY);
            return(status);
        }
    }

    log_printf(15, "waiting for gid=%d to complete\n", gop_id(gop));
    err = gop_waitall(gop);
    status = gop_get_status(gop);
    log_printf(15, "gid=%d completed with err=%d\n", gop_id(gop), err);
    gop_free(gop, OP_DESTROY);
    log_printf(15, "After gop destruction\n");

    return(status);
}

//*************************************************************
// gop_reset - Resets an already inited gop
//*************************************************************

void gop_reset(gop_op_generic_t *gop)
{
    gop->base.id = tbx_atomic_global_counter();

    unlock_gop(gop);
    gop->base.cb = NULL;
    gop->base.state = 0;
    gop->base.status = gop_failure_status;
    gop->base.started_execution = 0;
    gop->base.auto_destroy = 0;
}


//*************************************************************
// gop_init - Initializes a generic op
//*************************************************************

void gop_init(gop_op_generic_t *gop)
{
    tbx_pch_t pch;

    gop_op_common_t *base = &(gop->base);

    tbx_type_memclear(gop, gop_op_generic_t, 1);

    base->id = tbx_atomic_global_counter();

    log_printf(15, "gop ptr=%p gid=%d\n", gop, gop_id(gop));

    //** Get the control struct
    pch = tbx_pch_reserve(_gop_control);
    gop->base.ctl = (gop_control_t *)tbx_pch_data(&pch);
    gop->base.ctl->pch = pch;
}


//*************************************************************
// gop_generic_free - Frees the data generic internal op data
//*************************************************************

void gop_generic_free(gop_op_generic_t *gop, gop_op_free_mode_t mode)
{
    log_printf(20, "op_generic_free: before lock gid=%d\n", gop_get_id(gop));
    lock_gop(gop);  //** Make sure I own the lock just to be safe
    log_printf(20, "op_generic_free: AFTER lock gid=%d\n", gop_get_id(gop));

    callback_destroy(gop->base.cb);  //** Free the callback chain as well

    unlock_gop(gop);  //** Make sure the lock is left in the proper state for reuse
    tbx_pch_release(_gop_control, &(gop->base.ctl->pch));
}

//*************************************************************
// gop_time_exec - returns the execution time
//*************************************************************

apr_time_t gop_time_exec(gop_op_generic_t *gop)
{
    apr_time_t dt;

    lock_gop(gop);
    dt = gop->op->cmd.end_time - gop->op->cmd.start_time;
    unlock_gop(gop);

    return(dt);
}

//*************************************************************
// gop_time_start - returns the start time
//*************************************************************

apr_time_t gop_time_start(gop_op_generic_t *gop)
{
    apr_time_t start_time;

    lock_gop(gop);
    start_time = gop->op->cmd.start_time;
    unlock_gop(gop);

    return(start_time);
}

//*************************************************************
// gop_time_end - returns the end time
//*************************************************************

apr_time_t gop_time_end(gop_op_generic_t *gop)
{
    apr_time_t end_time;

    lock_gop(gop);
    end_time = gop->op->cmd.end_time;
    unlock_gop(gop);

    return(end_time);
}
