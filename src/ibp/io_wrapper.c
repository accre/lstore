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

#define _log_module_index 136

#include "io_wrapper.h"
#include <gop/opque.h>
#include "ibp.h"
#include "iovec_sync.h"
#include <tbx/log.h>

int _sync_transfer = 0;
int _print_progress = 0;
int _nthreads = 1;

//*************************************************************************
// ibp_io_mode_set - Sets the IO mode
//*************************************************************************

void ibp_io_mode_set(int sync_transfer, int print_progress, int nthreads)
{
    _sync_transfer = sync_transfer;
    _print_progress = print_progress;
    _nthreads = nthreads;
}

//*************************************************************************
//  ibp_io_start - Simple wrapper for sync/async to start execution
//*************************************************************************

void ibp_io_start(opque_t *q)
{
    if (_sync_transfer == 0) opque_start_execution(q);
}

//*************************************************************************
//  ibp_io_waitall - Simple wrapper for sync/async waitall
//*************************************************************************

int ibp_io_waitall(opque_t *q)
{
    int ibp_err, err, nleft;
    op_generic_t *op;

    log_printf(15, "ibp_io_waitall: sync_transfer=%d\n", _sync_transfer);
    if (_sync_transfer == 1) {
        ibp_err = ibp_sync_execute(q, _nthreads);
        err = ( ibp_err == IBP_OK) ? 0 : 1;
    } else {
        if (_print_progress == 0) {
            err = (opque_waitall(q) == OP_STATE_SUCCESS) ? 0 : 1;
        } else {
            do {
                nleft = opque_tasks_left(q);
                printf("%d ", nleft);
                do {
                    op = opque_get_next_finished(q);
                    if (op != NULL) gop_free(op, OP_DESTROY);
                } while (op != NULL);

                op = opque_waitany(q);
                if (op != NULL) gop_free(op, OP_DESTROY);
            } while (nleft > 0);

            printf(" --\n");
            err = opque_tasks_failed(q);
        }

        log_printf(15, "ibp_io_waitall: err=%d nfailed=%d nleft=%d\n", err, opque_tasks_failed(q), opque_tasks_left(q));
    }

    return(err);
}

