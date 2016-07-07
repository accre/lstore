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

#define _log_module_index 135

#include <gop/opque.h>
#include <gop/types.h>
#include <ibp/protocol.h>
#include <stdlib.h>
#include <tbx/log.h>
#include <tbx/stack.h>


//=============================================================


//*************************************************************
// ibp_sync_execute - Handles the sync iovec operations
//*************************************************************

int ibp_sync_execute(gop_opque_t *q, int nthreads)
{
    tbx_stack_t *tasks;
    gop_op_generic_t *gop;

    log_printf(15, "ibp_sync_execute: Start! ncommands=%d\n", tbx_stack_count(q->qd.list));
    gop_default_sort_ops(NULL, q);

    q = gop_opque_new();
    opque_start_execution(q);

    tasks = q->qd.list;
    q->qd.list = tbx_stack_new();

    while ((  gop = (gop_op_generic_t *)tbx_stack_pop(tasks)) != NULL) {
        gop_opque_add(q, gop);
        if (gop_opque_tasks_left(q) >= nthreads) {
            opque_waitany(q);
        }

        tbx_stack_pop(tasks);
    }


    if (gop_opque_tasks_failed(q) == 0) {
        return(IBP_OK);
    } else {
        return(IBP_E_GENERIC);
    }


}

