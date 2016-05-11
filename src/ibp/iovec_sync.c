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

#include <stdlib.h>
#include <string.h>
#include <apr_pools.h>
#include <apr_thread_proc.h>
#include "ibp.h"
#include "ibp_misc.h"
#include "host_portal.h"
#include <tbx/log.h>

//=============================================================


//*************************************************************
// ibp_sync_execute - Handles the sync iovec operations
//*************************************************************

int ibp_sync_execute(opque_t *q, int nthreads)
{
    tbx_stack_t *tasks;
    op_generic_t *gop;

    log_printf(15, "ibp_sync_execute: Start! ncommands=%d\n", tbx_stack_size(q->qd.list));
    default_sort_ops(NULL, q);

    q = new_opque();
    opque_start_execution(q);

    tasks = q->qd.list;
    q->qd.list = tbx_stack_new();

    while ((  gop = (op_generic_t *)tbx_stack_pop(tasks)) != NULL) {
        opque_add(q, gop);
        if (opque_tasks_left(q) >= nthreads) {
            opque_waitany(q);
        }

        tbx_stack_pop(tasks);
    }


    if (opque_tasks_failed(q) == 0) {
        return(IBP_OK);
    } else {
        return(IBP_E_GENERIC);
    }


}

