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

#define _log_module_index 123

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "thread_pool.h"
#include "fmttypes.h"
#include "network.h"
#include "log.h"
#include "type_malloc.h"
#include "append_printf.h"
#include "atomic_counter.h"

extern int _tp_context_count;
extern apr_thread_mutex_t *_tp_lock;
extern apr_pool_t *_tp_pool;
extern int _tp_id;

void _tp_op_free(op_generic_t *gop, int mode);

//*************************************************************

op_status_t tp_command(op_generic_t *gop, NetStream_t *ns)
{
    return(op_success_status);
}

//*************************************************************

void  *thread_pool_exec_fn(apr_thread_t *th, void *arg)
{
    op_generic_t *gop = (op_generic_t *)arg;
    thread_pool_op_t *op = gop_get_tp(gop);
    op_status_t status;
    int tid;

    tid = atomic_thread_id;
    log_printf(4, "tp_recv: Start!!! gid=%d tid=%d\n", gop_id(gop), tid);
    atomic_inc(op->tpc->n_started);

    status = op->fn(op->arg, gop_id(gop));

    log_printf(4, "tp_recv: end!!! gid=%d tid=%d status=%d\n", gop_id(gop), tid, status.op_status);
//log_printf(15, "gid=%d user_priv=%p\n", gop_id(gop), gop_get_private(gop));

    atomic_inc(op->tpc->n_completed);
    gop_mark_completed(gop, status);

    return(NULL);
}

//*************************************************************
// init_tp_op - Does the 1-time initialization for the op
//*************************************************************

void init_tp_op(thread_pool_context_t *tpc, thread_pool_op_t *op)
{
    op_generic_t *gop;

    //** Clear it
    type_memclear(op, thread_pool_op_t, 1);

    //** Now munge the pointers
    gop = &(op->gop);
    gop_init(gop);
    gop->op = &(op->dop);
    gop->op->priv = op;
    gop->type = Q_TYPE_OPERATION;
    op->tpc = tpc;
    op->dop.priv = op;
    op->dop.pc = tpc->pc; //**IS this needed?????
    gop->base.free = _tp_op_free;
    gop->free_ptr = op;
    gop->base.pc = tpc->pc;
}


//*************************************************************
// set_thread_pool_op - Sets a thread pool op
//*************************************************************

int set_thread_pool_op(thread_pool_op_t *op, thread_pool_context_t *tpc, char *que, op_status_t (*fn)(void *arg, int id), void *arg, void (*my_op_free)(void *arg), int workload)
{
    op->fn = fn;
    op->arg = arg;
    op->my_op_free = my_op_free;

    return(0);
}

//*************************************************************
// new_thread_pool_op - Allocates space for a new op
//*************************************************************

op_generic_t *new_thread_pool_op(thread_pool_context_t *tpc, char *que, op_status_t (*fn)(void *arg, int id), void *arg, void (*my_op_free)(void *arg), int workload)
{
    thread_pool_op_t *op;

    //** Make the struct and clear it
    type_malloc(op, thread_pool_op_t, 1);

    atomic_inc(tpc->n_ops);

    init_tp_op(tpc, op);

    set_thread_pool_op(op, tpc, que, fn, arg, my_op_free, workload);

    return(tp_get_gop(op));
}


