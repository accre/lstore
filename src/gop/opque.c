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

#define _log_module_index 128

#include <assert.h>
#include "assert_result.h"
#include <apr_thread_mutex.h>
#include <apr_thread_cond.h>
#include <stdlib.h>
#include <string.h>
#include "type_malloc.h"
#include "log.h"
#include "opque.h"
#include "atomic_counter.h"

void opque_free(opque_t *q, int mode);
void gop_dummy_init();
void gop_dummy_destroy();


atomic_int_t _opque_counter = 0;
apr_pool_t *_opque_pool = NULL;
pigeon_coop_t *_gop_control = NULL;

//*************************************************************
//  _opque_print_stack - Prints the list stack
//*************************************************************

void _opque_print_stack(Stack_t *stack)
{
    op_generic_t *gop;
    int i=0;

    if (log_level() <= 15) return;

    move_to_top(stack);
    while ((gop = (op_generic_t *)get_ele_data(stack)) != NULL) {
        log_printf(15, "    i=%d gid=%d type=%d\n", i, gop_id(gop), gop_get_type(gop));
        i++;
        move_down(stack);
    }

    if (stack_size(stack) != i) log_printf(0, "Stack size mismatch! stack_size=%d i=%d\n", stack_size(stack), i);
}


//*************************************************************
// gop_control_new - Creates a new gop_control shelf set
//*************************************************************

void *gop_control_new(void *arg, int size)
{
    gop_control_t *shelf;
    int i;

    type_malloc_clear(shelf, gop_control_t, size);

    for (i=0; i<size; i++) {
        assert_result(apr_thread_mutex_create(&(shelf[i].lock), APR_THREAD_MUTEX_DEFAULT,_opque_pool), APR_SUCCESS);
        assert_result(apr_thread_cond_create(&(shelf[i].cond), _opque_pool), APR_SUCCESS);
    }

    return((void *)shelf);
}

//*************************************************************
// gop_control_free - Destroys a gop_control set
//*************************************************************

void gop_control_free(void *arg, int size, void *data)
{
    gop_control_t *shelf = (gop_control_t *)data;
    int i;

    for (i=0; i<size; i++) {
        apr_thread_mutex_destroy(shelf[i].lock);
        apr_thread_cond_destroy(shelf[i].cond);
    }

    free(shelf);
    return;
}

//*************************************************************
//  init_opque_system - Initializes the OpQue system
//*************************************************************

void init_opque_system()
{
    log_printf(15, "init_opque_system: counter=%d\n", _opque_counter);
    if (atomic_inc(_opque_counter) == 0) {   //** Only init if needed
        assert_result(apr_pool_create(&_opque_pool, NULL), APR_SUCCESS);
        _gop_control = new_pigeon_coop("gop_control", 50, sizeof(gop_control_t), NULL, gop_control_new, gop_control_free);
        gop_dummy_init();
        atomic_init();
    }

}

//*************************************************************
//  destroy_opque_system - Initializes the OpQue system
//*************************************************************

void destroy_opque_system()
{
    log_printf(15, "destroy_opque_system: counter=%d\n", _opque_counter);
    if (atomic_dec(_opque_counter) == 0) {   //** Only wipe if not used
        destroy_pigeon_coop(_gop_control);
        apr_pool_destroy(_opque_pool);
        gop_dummy_destroy();
        atomic_destroy();

    }
}

//*************************************************************
// _opque_cb - Global callback for all opque's
//*************************************************************

void _opque_cb(void *v, int mode)
{
    int type, n;
    op_status_t success;
    op_generic_t *gop = (op_generic_t *)v;
    que_data_t *q = &(gop->base.parent_q->qd);

    log_printf(15, "_opque_cb: START qid=%d gid=%d\n", gop_id(&(q->opque->op)), gop_id(gop));

    //** Get the status (gop is already locked)
    type = gop_get_type(gop);
    if (type == Q_TYPE_QUE) {
        n = stack_size(gop->q->failed);
        log_printf(15, "_opque_cb: qid=%d gid=%d  stack_size(q->failed)=%d gop->status=%d\n", gop_id(&(q->opque->op)), gop_id(gop), n, gop->base.status.op_status);
        success = (n == 0) ? gop->base.status : op_failure_status;
    } else {
        success = gop->base.status;
    }

    lock_opque(q);

    log_printf(15, "_opque_cb: qid=%d gid=%d success=%d gop_type(gop)=%d\n", gop_id(&(q->opque->op)), gop_id(gop), success.op_status, gop_get_type(gop));


    //** It always goes on the finished list
    move_to_bottom(q->finished);
    insert_below(q->finished, gop);

    log_printf(15, "PUSH finished gid=%d qid=%d\n", gop_id(gop), gop_id(&(q->opque->op)));
    log_printf(15, "Printing finished stack for qid=%d\n", gop_id(&(q->opque->op)));
    if (log_level() > 15) _opque_print_stack(q->finished);

    if (success.op_status == OP_STATE_FAILURE) push(q->failed, gop); //** Push it on the failed list if needed

    q->nleft--;
    log_printf(15, "_opque_cb: qid=%d gid=%d nleft=%d stack_size(q->failed)=%d stack_size(q->finished)=%d\n", gop_id(&(q->opque->op)), gop_id(gop), q->nleft, stack_size(q->failed), stack_size(q->finished));
    flush_log();

    if (q->nleft <= 0) {  //** we're finished
        if (stack_size(q->failed) == 0) {
            q->opque->op.base.status = op_success_status;
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
                q->opque->op.base.status = op_failure_status;
                callback_execute(q->opque->op.base.cb, OP_STATE_FAILURE);  //**Execute the other CBs
                apr_thread_cond_broadcast(q->opque->op.base.ctl->cond);  //** and fail for good
            }

            //** If retrying don't send the broadcast
            log_printf(15, "_opque_cb: RETRY END qid=%d gid=%d\n", gop_id(&(q->opque->op)), gop_id(gop));
            flush_log();
        } else {
            //** Finished with errors but trigger the signal for anybody listening
            apr_thread_cond_broadcast(q->opque->op.base.ctl->cond);
        }
    } else {
        //** Not finished but trigger the signal for anybody listening
        apr_thread_cond_broadcast(q->opque->op.base.ctl->cond);
    }

    log_printf(15, "_opque_cb: END qid=%d gid=%d\n", gop_id(&(q->opque->op)), gop_id(gop));
    flush_log();

    unlock_opque(q);
}

//*************************************************************
//  opque_[s|g]et_failure_mode
//*************************************************************

void opque_set_failure_mode(opque_t *q, int value)
{
    q->op.base.failure_mode = value;
}
int opque_get_failure_mode(opque_t *q)
{
    return(q->op.base.failure_mode);
}

//*************************************************************
// init_opque - Initializes a que list container
//*************************************************************

void init_opque(opque_t *q)
{
    que_data_t *que = &(q->qd);
    op_generic_t *gop = &(q->op);

    type_memclear(q, opque_t, 1);

    gop_init(gop);

    log_printf(15, "init_opque: qid=%d\n", gop_id(gop));

    //**Set up the pointers
    gop->q = que;
    gop->q->opque = q;

    q->op.type = Q_TYPE_QUE;

    que->list = new_stack();
    que->finished = new_stack();
    que->failed = new_stack();
    que->nleft = 0;
    que->nsubmitted = 0;
    gop->base.retries = 0;
//  que->started_execution = 0;
    que->finished_submission = 0;
//  que->success = OP_STATE_FAILURE;
//  que->success = 12345;
}

//*************************************************************
// new_opque - Generates a new que container
//*************************************************************

opque_t *new_opque()
{
    opque_t *q;

    type_malloc(q, opque_t , 1);
    init_opque(q);

    return(q);
}

//*************************************************************
// free_finished_stack - Frees an opque
//*************************************************************

void free_finished_stack(Stack_t *stack, int mode)
{
    op_generic_t *gop;

    gop = (op_generic_t *)pop(stack);
    while (gop != NULL) {
//log_printf(15, "gid=%d\n", gop_id(gop));
        if (gop->type == Q_TYPE_QUE) {
//log_printf(15, "free_opque_stack: gop->type=QUE\n"); flush_log();
            opque_free(gop->q->opque, mode);
        } else {
//log_printf(15, "free_opque_stack: gop->type=OPER\n"); flush_log();
//DONE in op_generic_destroy        callback_destroy(gop->base.cb);  //** Free the callback chain as well
            if (gop->base.free != NULL) gop->base.free(gop, mode);
        }

        gop = (op_generic_t *)pop(stack);
    }

    free_stack(stack, 0);
}

//*************************************************************
// free_list_stack - Frees an opque
//*************************************************************

void free_list_stack(Stack_t *stack, int mode)
{
    op_generic_t *gop;
    callback_t *cb;

    cb = (callback_t *)pop(stack);
    while (cb != NULL) {
        gop = (op_generic_t *)cb->priv;
        log_printf(15, "gid=%d\n", gop_id(gop));
        if (gop->type == Q_TYPE_QUE) {
//log_printf(15, "free_opque_stack: gop->type=QUE\n"); flush_log();
            opque_free(gop->q->opque, mode);
        } else {
//log_printf(15, "free_opque_stack: gop->type=OPER\n"); flush_log();
//DONE in op_generic_destroy        callback_destroy(gop->base.cb);  //** Free the callback chain as well
            if (gop->base.free != NULL) gop->base.free(gop, mode);
        }

        cb = (callback_t *)pop(stack);
    }

    free_stack(stack, 0);
}

//*************************************************************
//  opque_free - Frees the que container and optionally
//    the container itself allowing it to be reused based
//    on the mode
//*************************************************************

void opque_free(opque_t *opq, int mode)
{
    que_data_t *q = &(opq->qd);

    log_printf(15, "qid=%d nfin=%d nlist=%d nfailed=%d\n", gop_id(&(opq->op)), stack_size(q->finished), stack_size(q->list), stack_size(q->failed));

    lock_opque(&(opq->qd));  //** Lock it to make sure Everything is finished and safe to free

    //** Free the stacks
    free_stack(q->failed, 0);
    free_finished_stack(q->finished, mode);
    free_list_stack(q->list, mode);

    unlock_opque(&(opq->qd));  //** Has to be unlocked for gop_generic_free to work cause it also locks it
    gop_generic_free(opque_get_gop(opq), mode);

    if (mode == OP_DESTROY) free(opq);
}

//*************************************************************
// internal_opque_add - Adds a task or list to the que with
//   optional locking.  Designed for use withing a callback
//*************************************************************

int internal_opque_add(opque_t *que, op_generic_t *gop, int dolock)
{
    int err = 0;
    callback_t *cb;
    que_data_t *q = &(que->qd);

    //** Create the callback **
    type_malloc(cb, callback_t, 1);
    callback_set(cb, _opque_cb, (void *)gop); //** Set the global callback for the list

    if (dolock != 0) lock_opque(q);

    log_printf(15, "opque_add: qid=%d gid=%d\n", que->op.base.id, gop_get_id(gop));

    //** Add the list CB to the the op
//  lock_gop(gop)
    gop->base.parent_q = que;
    callback_append(&(gop->base.cb), cb);
//  unlock_gop(gop)

    //**Add the op to the q
    q->nsubmitted++;
    q->nleft++;
    if (q->opque->op.base.started_execution == 0) {
        move_to_bottom(q->list);
        insert_below(q->list, (void *)cb);
    }

    if (dolock != 0) unlock_opque(q);

    if (q->opque->op.base.started_execution == 1) {
        if (gop->type == Q_TYPE_OPERATION) {
            log_printf(15, "gid=%d started_execution=%d\n", gop_get_id(gop), gop->base.started_execution);
            gop->base.started_execution = 1;
            gop->base.pc->fn->submit(gop->base.pc->arg, gop);
        } else {  //** It's a queue
            opque_start_execution(gop->q->opque);
        }
    }

    return(err);
}

//*************************************************************
// opque_add - Adds a task or list to the que
//*************************************************************

int opque_add(opque_t *que, op_generic_t *gop)
{
    return(internal_opque_add(que, gop, 1));
}

//*************************************************************
// opque_completion_status - Returns the que status
//*************************************************************

op_status_t opque_completion_status(opque_t *que)
{
    que_data_t *q = &(que->qd);
    op_status_t status;

    lock_opque(q);
    status = (stack_size(q->failed) == 0) ? q->opque->op.base.status : op_failure_status;
    unlock_opque(q);

    return(status);
}

//*************************************************************
// _opque_start_execution - Routine for submitting ques for exec
//*************************************************************

void _opque_start_execution(opque_t *que)
{
    int n, i;
    callback_t *cb;
    op_generic_t *gop;
    que_data_t *q = &(que->qd);

    gop = opque_get_gop(que);
    if (gop->base.started_execution != 0) {
        return;
    }

    gop->base.started_execution = 1;

    n = stack_size(q->list);
    move_to_top(q->list);
    for (i=0; i<n; i++) {
        cb = (callback_t *)pop(q->list);
        gop = (op_generic_t *)cb->priv;
        if (gop->type == Q_TYPE_OPERATION) {
            log_printf(15, "qid=%d gid=%d\n",gop_id(opque_get_gop(que)), gop_get_id(gop));
            gop->base.started_execution = 1;
            gop->base.pc->fn->submit(gop->base.pc->arg, gop);
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
    callback_t *cb;
    op_generic_t *gop;
    command_op_t *c1, *c2;

    cb = (callback_t *)arg1;
    gop = (op_generic_t *)cb->priv;
    c1 = &(gop->op->cmd);
    cb = (callback_t *)arg2;
    gop = (op_generic_t *)cb->priv;
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
// default_sort_ops - Default routine to sort ops.
//   Ops are sorted to group ops for the same host together in
//   descending amount of work.
//*************************************************************

void default_sort_ops(void *arg, opque_t *que)
{
    int i, n, count;
    callback_t **array;
    callback_t *cb;
    void *ptr;
    op_generic_t *gop;
    que_data_t *q = &(que->qd);
    Stack_t *q_list = new_stack();
    n = stack_size(q->list);
    type_malloc(array, callback_t *, n);

    //**Create the linear array used for qsort
    count = 0;
    for (i=0; i<n; i++) {
        cb = (callback_t *)pop(q->list);
        gop = (op_generic_t *)cb->priv;
        if (gop->type == Q_TYPE_OPERATION) {
            array[count] = cb;
            count++;
        } else {
            push(q_list, cb);
        }
    }

    //** Now sort the linear array **
    qsort((void *)array, count, sizeof(callback_t *), compare_ops);

    //** Now place them back on the list **
    for (i=0; i<count; i++) {
        push(q->list, (void *)array[i]);
//    log_printf(15, "sort_io_list: i=%d hostdepot=%s size=%d\n", i, array[i]->hop.hostport, array[i]->hop.cmp_size);
    }

    //** with the que on the top.
    while ((ptr = pop(q_list)) != NULL) {
        push(q->list, ptr);
    }

    free_stack(q_list, 0);
    free(array);
}



