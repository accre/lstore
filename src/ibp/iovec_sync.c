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

#define _log_module_index 135

#include <stdlib.h>
#include <string.h>
#include <apr_pools.h>
#include <apr_thread_proc.h>
#include "ibp.h"
#include "ibp_misc.h"
#include "host_portal.h"
#include "log.h"

//=============================================================


//*************************************************************
// ibp_sync_execute - Handles the sync iovec operations
//*************************************************************

int ibp_sync_execute(opque_t *q, int nthreads)
{
    Stack_t *tasks;
    op_generic_t *gop;

    log_printf(15, "ibp_sync_execute: Start! ncommands=%d\n", stack_size(q->qd.list));
    default_sort_ops(NULL, q);

    q = new_opque();
    opque_start_execution(q);

    tasks = q->qd.list;
    q->qd.list = new_stack();

    while ((  gop = (op_generic_t *)pop(tasks)) != NULL) {
        opque_add(q, gop);
        if (opque_tasks_left(q) >= nthreads) {
            opque_waitany(q);
        }

        pop(tasks);
    }


    if (opque_tasks_failed(q) == 0) {
        return(IBP_OK);
    } else {
        return(IBP_E_GENERIC);
    }


}

