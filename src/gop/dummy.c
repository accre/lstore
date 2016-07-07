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

#include <apr_errno.h>
#include <apr_pools.h>
#include <apr_thread_cond.h>
#include <apr_thread_mutex.h>
#include <apr_thread_proc.h>
#include <gop/portal.h>
#include <stdlib.h>
#include <tbx/apr_wrapper.h>
#include <tbx/assert_result.h>
#include <tbx/log.h>
#include <tbx/stack.h>
#include <tbx/type_malloc.h>

#include "gop.h"
#include "gop/types.h"

static void gop_dummy_submit_op(void *arg, gop_op_generic_t *op);

static gop_portal_fn_t gop_dummy_portal = {
    .dup_connect_context = NULL,
    .destroy_connect_context = NULL,
    .connect = NULL,
    .close_connection = NULL,
    .sort_tasks = NULL,
    .submit = gop_dummy_submit_op
};

static gop_portal_context_t gop_dummy_pc = {
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
    .fn = &gop_dummy_portal
};

int gd_shutdown = 0;
apr_thread_t *gd_thread = NULL;
apr_pool_t *gd_pool = NULL;
apr_thread_mutex_t *gd_lock = NULL;
apr_thread_cond_t *gd_cond = NULL;
tbx_stack_t *gd_stack = NULL;


//***********************************************************************
// gd_thread_func - gop_dummy execution thread.  Just calls the
//   gop_mark_completed() routine for the gops
//***********************************************************************

static void *gd_thread_func(apr_thread_t *th, void *data)
{
    gop_op_generic_t *gop;

    apr_thread_mutex_lock(gd_lock);
    while (gd_shutdown == 0) {
        //** Execute everything on the stack
        while ((gop = (gop_op_generic_t *)tbx_stack_pop(gd_stack)) != NULL) {
            log_printf(15, "DUMMY gid=%d status=%d\n", gop_id(gop), gop->base.status.op_status);
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
    assert_result(apr_pool_create(&gd_pool, NULL), APR_SUCCESS);
    assert_result(apr_thread_mutex_create(&gd_lock, APR_THREAD_MUTEX_DEFAULT, gd_pool), APR_SUCCESS);
    assert_result(apr_thread_cond_create(&gd_cond, gd_pool), APR_SUCCESS);
    gd_stack = tbx_stack_new();

    //** and launch the thread
    tbx_thread_create_assert(&gd_thread, NULL, gd_thread_func, NULL, gd_pool);
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
    tbx_stack_free(gd_stack, 0);
    apr_thread_mutex_destroy(gd_lock);
    apr_thread_cond_destroy(gd_cond);
    apr_pool_destroy(gd_pool);
}

//***********************************************************************
// _gop_dummy_submit - Dummy submit routine
//***********************************************************************

static void gop_dummy_submit_op(void *arg, gop_op_generic_t *op)
{
    log_printf(15, "gid=%d\n", gop_id(op));
    apr_thread_mutex_lock(gd_lock);
    tbx_stack_push(gd_stack, op);
    apr_thread_cond_signal(gd_cond);
    apr_thread_mutex_unlock(gd_lock);
    return;
}


//***********************************************************************
// dummy free operation
//***********************************************************************

static void gop_dummy_free(gop_op_generic_t *gop, int mode)
{
    gop_generic_free(gop, mode);  //** I free the actual op

    if (mode == OP_DESTROY) free(gop);
}

//***********************************************************************
// gop_dummy - Creates a GOP dummy op with the appropriate state
//   OP_STATE_SUCCESS = Success
//   OP_STATE_FAILIURE = Failure
//***********************************************************************

gop_op_generic_t *gop_dummy(gop_op_status_t state)
{
    gop_op_generic_t *gop;

    tbx_type_malloc_clear(gop, gop_op_generic_t, 1);

    log_printf(15, " state=%d\n", state.op_status);
    tbx_log_flush();

    gop_init(gop);
    gop->base.pc = &gop_dummy_pc;
    gop->type = Q_TYPE_OPERATION;
    gop->base.free = gop_dummy_free;
    gop->base.status = state;

    return(gop);
}
