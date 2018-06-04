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

#define _log_module_index 128

#include <apr_errno.h>
#include <apr_pools.h>
#include <apr_thread_cond.h>
#include <apr_thread_mutex.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/assert_result.h>
#include <tbx/atomic_counter.h>
#include <tbx/fmttypes.h>
#include <tbx/log.h>
#include <tbx/pigeon_coop.h>
#include <tbx/stack.h>
#include <tbx/type_malloc.h>

#include "callback.h"
#include "gop.h"
#include "gop/portal.h"
#include "gop/types.h"
#include "hp.h"
#include "opque.h"

void gop_opque_free(gop_opque_t *q, int mode);
void gop_dummy_init();
void gop_dummy_destroy();

tbx_atomic_int_t _opque_counter = 0;
tbx_pc_t *_gop_control = NULL;

//*************************************************************
//  _opque_print_stack - Prints the list stack
//*************************************************************

void _opque_print_stack(tbx_stack_t *stack)
{
    gop_op_generic_t *gop;
    int i=0;

    if (tbx_log_level() <= 15) return;

    tbx_stack_move_to_top(stack);
    while ((gop = (gop_op_generic_t *)tbx_stack_get_current_data(stack)) != NULL) {
        log_printf(15, "    i=%d gid=%d type=%d\n", i, gop_id(gop), gop_get_type(gop));
        i++;
        tbx_stack_move_down(stack);
    }

    if (tbx_stack_count(stack) != i) log_printf(0, "Stack size mismatch! stack_size=%d i=%d\n", tbx_stack_count(stack), i);
}


//*************************************************************
// gop_control_new - Creates a new gop_control shelf set
//*************************************************************

void *gop_control_new(void *arg, int size)
{
    gop_control_t *shelf;
    apr_pool_t **pool_ptr;
    int i;

    i = sizeof(gop_control_t)*size + sizeof(apr_pool_t *);
    shelf = malloc(i);
    FATAL_UNLESS(shelf != NULL);
    memset(shelf, 0, i);

    pool_ptr = (apr_pool_t **)&(shelf[size]);
    assert_result(apr_pool_create(pool_ptr, NULL), APR_SUCCESS);

    for (i=0; i<size; i++) {
        assert_result(apr_thread_mutex_create(&(shelf[i].lock), APR_THREAD_MUTEX_DEFAULT,*pool_ptr), APR_SUCCESS);
        assert_result(apr_thread_cond_create(&(shelf[i].cond), *pool_ptr), APR_SUCCESS);
    }

    return((void *)shelf);
}

//*************************************************************
// gop_control_free - Destroys a gop_control set
//*************************************************************

void gop_control_free(void *arg, int size, void *data)
{
    apr_pool_t **pool_ptr;
    gop_control_t *shelf = (gop_control_t *)data;

    pool_ptr = (apr_pool_t **)&(shelf[size]);

    //** All the data is in the memory pool
    apr_pool_destroy(*pool_ptr);

    free(shelf);
    return;
}

//*************************************************************
//  gop_init_opque_system - Initializes the OpQue system
//*************************************************************

void gop_init_opque_system()
{
    log_printf(15, "gop_init_opque_system: counter=" AIT "\n", _opque_counter);
    if (tbx_atomic_inc(_opque_counter) == 0) {   //** Only init if needed
        _gop_control = tbx_pc_new("gop_control", 50, sizeof(gop_control_t), NULL, gop_control_new, gop_control_free);
        gop_dummy_init();
        tbx_atomic_startup();
    }
}

//*************************************************************
//  gop_shutdown - Initializes the OpQue system
//*************************************************************

void gop_shutdown()
{
    log_printf(15, "gop_shutdown: counter=" AIT "\n", _opque_counter);
    if (tbx_atomic_dec(_opque_counter) == 0) {   //** Only wipe if not used
        tbx_pc_destroy(_gop_control);
        gop_dummy_destroy();
        tbx_atomic_shutdown();

    }
}

//*************************************************************
// _opque_cb - Global callback for all opque's
//*************************************************************

void _opque_cb(void *v, int mode)
{
    int type, n;
    gop_op_status_t success;
    gop_op_generic_t *gop = (gop_op_generic_t *)v;
    gop_que_data_t *q = &(gop->base.parent_q->qd);

    log_printf(15, "_opque_cb: START qid=%d gid=%d\n", gop_id(&(q->opque->op)), gop_id(gop));

    //** Get the status (gop is already locked)
    type = gop_get_type(gop);
    if (type == Q_TYPE_QUE) {
        n = tbx_stack_count(gop->q->failed);
        log_printf(15, "_opque_cb: qid=%d gid=%d  tbx_stack_count(q->failed)=%d gop->status=%d\n", gop_id(&(q->opque->op)), gop_id(gop), n, gop->base.status.op_status);
        success = (n == 0) ? gop->base.status : gop_failure_status;
    } else {
        success = gop->base.status;
    }

    lock_opque(q);

    log_printf(15, "_opque_cb: qid=%d gid=%d success=%d gop_type(gop)=%d\n", gop_id(&(q->opque->op)), gop_id(gop), success.op_status, gop_get_type(gop));


    //** It always goes on the finished list
    tbx_stack_move_to_bottom(q->finished);
    tbx_stack_insert_below(q->finished, gop);

    log_printf(15, "PUSH finished gid=%d qid=%d\n", gop_id(gop), gop_id(&(q->opque->op)));
    log_printf(15, "Printing finished stack for qid=%d\n", gop_id(&(q->opque->op)));
    if (tbx_log_level() > 15) _opque_print_stack(q->finished);

    if (success.op_status == OP_STATE_FAILURE) tbx_stack_push(q->failed, gop); //** Push it on the failed list if needed

    q->nleft--;
    log_printf(15, "_opque_cb: qid=%d gid=%d nleft=%d tbx_stack_count(q->failed)=%d tbx_stack_count(q->finished)=%d\n", gop_id(&(q->opque->op)), gop_id(gop), q->nleft, tbx_stack_count(q->failed), tbx_stack_count(q->finished));
    tbx_log_flush();

    if (q->nleft <= 0) {  //** we're finished
        if (tbx_stack_count(q->failed) == 0) {
            q->opque->op.base.status = gop_success_status;
            callback_execute(q->opque->op.base.cb, OP_STATE_SUCCESS);

            //** Lastly trigger the signal. for anybody listening
            apr_thread_cond_broadcast(q->opque->op.base.ctl->cond);
        } else if (q->opque->op.base.retries == 0) {  //** How many times we're retried
            //** Trigger the callbacks
            q->opque->op.base.retries++;
            q->nleft = 0;
            q->opque->op.base.failure_mode = 0;
            callback_execute(&(q->failure_cb), OP_STATE_FAILURE);  //** Attempt to fix things

            if (q->opque->op.base.failure_mode == 0) {  //** No retry
                q->opque->op.base.status = gop_failure_status;
                callback_execute(q->opque->op.base.cb, OP_STATE_FAILURE);  //**Execute the other CBs
                apr_thread_cond_broadcast(q->opque->op.base.ctl->cond);  //** and fail for good
            }

            //** If retrying don't send the broadcast
            log_printf(15, "_opque_cb: RETRY END qid=%d gid=%d\n", gop_id(&(q->opque->op)), gop_id(gop));
            tbx_log_flush();
        } else {
            //** Finished with errors but trigger the signal for anybody listening
            apr_thread_cond_broadcast(q->opque->op.base.ctl->cond);
        }
    } else {
        //** Not finished but trigger the signal for anybody listening
        apr_thread_cond_broadcast(q->opque->op.base.ctl->cond);
    }

    log_printf(15, "_opque_cb: END qid=%d gid=%d\n", gop_id(&(q->opque->op)), gop_id(gop));
    tbx_log_flush();

    unlock_opque(q);
}

//*************************************************************
//  opque_[s|g]et_failure_mode
//*************************************************************

void opque_set_failure_mode(gop_opque_t *q, int value)
{
    q->op.base.failure_mode = value;
}
int opque_get_failure_mode(gop_opque_t *q)
{
    return(q->op.base.failure_mode);
}

//*************************************************************
// init_opque - Initializes a que list container
//*************************************************************

void init_opque(gop_opque_t *q)
{
    gop_que_data_t *que = &(q->qd);
    gop_op_generic_t *gop = &(q->op);

    tbx_type_memclear(q, gop_opque_t, 1);

    gop_init(gop);

    log_printf(15, "init_opque: qid=%d\n", gop_id(gop));

    //**Set up the pointers
    gop->q = que;
    gop->q->opque = q;

    q->op.type = Q_TYPE_QUE;

    que->list = tbx_stack_new();
    que->finished = tbx_stack_new();
    que->failed = tbx_stack_new();
    que->nleft = 0;
    que->nsubmitted = 0;
    gop->base.retries = 0;
    que->finished_submission = 0;
}

//*************************************************************
// gop_opque_new - Generates a new que container
//*************************************************************

gop_opque_t *gop_opque_new()
{
    gop_opque_t *q;

    tbx_type_malloc(q, gop_opque_t , 1);
    init_opque(q);

    return(q);
}

//*************************************************************
// free_finished_stack - Frees an opque
//*************************************************************

void free_finished_stack(tbx_stack_t *stack, int mode)
{
    gop_op_generic_t *gop;

    gop = (gop_op_generic_t *)tbx_stack_pop(stack);
    while (gop != NULL) {
        if (gop->type == Q_TYPE_QUE) {
            gop_opque_free(gop->q->opque, mode);
        } else {
            if (gop->base.free != NULL) gop->base.free(gop, mode);
        }

        gop = (gop_op_generic_t *)tbx_stack_pop(stack);
    }

    tbx_stack_free(stack, 0);
}

//*************************************************************
// free_list_stack - Frees an opque
//*************************************************************

void free_list_stack(tbx_stack_t *stack, int mode)
{
    gop_op_generic_t *gop;
    gop_callback_t *cb;

    cb = (gop_callback_t *)tbx_stack_pop(stack);
    while (cb != NULL) {
        gop = (gop_op_generic_t *)cb->priv;
        log_printf(15, "gid=%d\n", gop_id(gop));
        if (gop->type == Q_TYPE_QUE) {
            gop_opque_free(gop->q->opque, mode);
        } else {
            if (gop->base.free != NULL) gop->base.free(gop, mode);
        }

        cb = (gop_callback_t *)tbx_stack_pop(stack);
    }

    tbx_stack_free(stack, 0);
}

//*************************************************************
//  gop_opque_free - Frees the que container and optionally
//    the container itself allowing it to be reused based
//    on the mode
//*************************************************************

void gop_opque_free(gop_opque_t *opq, int mode)
{
    gop_que_data_t *q = &(opq->qd);

    log_printf(15, "qid=%d nfin=%d nlist=%d nfailed=%d\n", gop_id(&(opq->op)), tbx_stack_count(q->finished), tbx_stack_count(q->list), tbx_stack_count(q->failed));

    lock_opque(&(opq->qd));  //** Lock it to make sure Everything is finished and safe to free

    //** Free the stacks
    tbx_stack_free(q->failed, 0);
    free_finished_stack(q->finished, mode);
    free_list_stack(q->list, mode);

    unlock_opque(&(opq->qd));  //** Has to be unlocked for gop_generic_free to work cause it also locks it
    gop_generic_free(opque_get_gop(opq), mode);

    if (mode == OP_DESTROY) free(opq);
}

//*************************************************************
// internal_gop_opque_add - Adds a task or list to the que with
//   optional locking.  Designed for use withing a callback
//*************************************************************

int internal_gop_opque_add(gop_opque_t *que, gop_op_generic_t *gop, int dolock)
{
    int err = 0;
    gop_callback_t *cb;
    gop_que_data_t *q = &(que->qd);

    //** Create the callback **
    tbx_type_malloc(cb, gop_callback_t, 1);
    gop_cb_set(cb, _opque_cb, (void *)gop); //** Set the global callback for the list

    if (dolock != 0) lock_opque(q);

    log_printf(15, "gop_opque_add: qid=%d gid=%d\n", que->op.base.id, gop_get_id(gop));

    //** Add the list CB to the the op
    gop->base.parent_q = que;
    callback_append(&(gop->base.cb), cb);

    //**Add the op to the q
    q->nsubmitted++;
    q->nleft++;
    if (q->opque->op.base.started_execution == 0) {
        tbx_stack_move_to_bottom(q->list);
        tbx_stack_insert_below(q->list, (void *)cb);
    }

    if (dolock != 0) unlock_opque(q);

    if (q->opque->op.base.started_execution == 1) {
        if (gop->type == Q_TYPE_OPERATION) {
            log_printf(15, "gid=%d started_execution=%d\n", gop_get_id(gop), gop->base.started_execution);
            gop->base.started_execution = 1;
            gop_op_submit(gop);
        } else {  //** It's a queue
            opque_start_execution(gop->q->opque);
        }
    }

    return(err);
}

//*************************************************************
// gop_opque_add - Adds a task or list to the que
//*************************************************************

int gop_opque_add(gop_opque_t *que, gop_op_generic_t *gop)
{
    return(internal_gop_opque_add(que, gop, 1));
}

//*************************************************************
// opque_completion_status - Returns the que status
//*************************************************************

gop_op_status_t opque_completion_status(gop_opque_t *que)
{
    gop_que_data_t *q = &(que->qd);
    gop_op_status_t status;

    lock_opque(q);
    status = (tbx_stack_count(q->failed) == 0) ? q->opque->op.base.status : gop_failure_status;
    unlock_opque(q);

    return(status);
}

//*************************************************************
// _opque_start_execution - Routine for submitting ques for exec
//*************************************************************

void _opque_start_execution(gop_opque_t *que)
{
    int n, i;
    gop_callback_t *cb;
    gop_op_generic_t *gop;
    gop_que_data_t *q = &(que->qd);

    gop = opque_get_gop(que);
    if (gop->base.started_execution != 0) {
        return;
    }

    gop->base.started_execution = 1;

    n = tbx_stack_count(q->list);
    tbx_stack_move_to_top(q->list);
    for (i=0; i<n; i++) {
        cb = (gop_callback_t *)tbx_stack_pop(q->list);
        gop = (gop_op_generic_t *)cb->priv;
        if (gop->type == Q_TYPE_OPERATION) {
            log_printf(15, "qid=%d gid=%d\n",gop_id(opque_get_gop(que)), gop_get_id(gop));
            gop->base.started_execution = 1;
            gop_op_submit(gop);
        } else {  //** It's a queue
            log_printf(15, "qid=%d Q gid=%d\n",gop_id(opque_get_gop(que)), gop_get_id(gop));
            lock_opque(gop->q);
            _opque_start_execution(gop->q->opque);
            unlock_opque(gop->q);
        }
    }

//  unlock_opque(q);
}

//*************************************************************
// compare_ops - Compares the ops for sorting
//*************************************************************

int compare_ops(const void *arg1, const void *arg2)
{
    int cmp;
    gop_callback_t *cb;
    gop_op_generic_t *gop;
    gop_command_op_t *c1, *c2;

    cb = (gop_callback_t *)arg1;
    gop = (gop_op_generic_t *)cb->priv;
    c1 = &(gop->op->cmd);
    cb = (gop_callback_t *)arg2;
    gop = (gop_op_generic_t *)cb->priv;
    c2 = &(gop->op->cmd);

    cmp = strcmp(c1->hostport, c2->hostport);
    if (cmp == 0) {  //** Same depot so compare size
        if (c1->cmp_size > c2->cmp_size) {
            cmp = 1;
        } else if (c1->cmp_size < c2->cmp_size) {
            cmp = -1;
        }
    }

    return(cmp);
}


//*************************************************************
// gop_default_sort_ops - Default routine to sort ops.
//   Ops are sorted to group ops for the same host together in
//   descending amount of work.
//*************************************************************

void gop_default_sort_ops(void *arg, gop_opque_t *que)
{
    int i, n, count;
    gop_callback_t **array;
    gop_callback_t *cb;
    void *ptr;
    gop_op_generic_t *gop;
    gop_que_data_t *q = &(que->qd);
    tbx_stack_t *q_list = tbx_stack_new();
    n = tbx_stack_count(q->list);
    tbx_type_malloc(array, gop_callback_t *, n);

    //**Create the linear array used for qsort
    count = 0;
    for (i=0; i<n; i++) {
        cb = (gop_callback_t *)tbx_stack_pop(q->list);
        gop = (gop_op_generic_t *)cb->priv;
        if (gop->type == Q_TYPE_OPERATION) {
            array[count] = cb;
            count++;
        } else {
            tbx_stack_push(q_list, cb);
        }
    }

    //** Now sort the linear array **
    qsort((void *)array, count, sizeof(gop_callback_t *), compare_ops);

    //** Now place them back on the list **
    for (i=0; i<count; i++) {
        tbx_stack_push(q->list, (void *)array[i]);
    }

    //** with the que on the top.
    while ((ptr = tbx_stack_pop(q_list)) != NULL) {
        tbx_stack_push(q->list, ptr);
    }

    tbx_stack_free(q_list, 0);
    free(array);
}



