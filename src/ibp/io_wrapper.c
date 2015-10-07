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

#define _log_module_index 136

#include "opque.h"
#include "ibp.h"
#include "iovec_sync.h"
#include "log.h"

int _sync_transfer = 0;
int _print_progress = 0;
int _nthreads = 1;

//*************************************************************************
// io_set_mode - Sets the IO mode
//*************************************************************************

void io_set_mode(int sync_transfer, int print_progress, int nthreads)
{
    _sync_transfer = sync_transfer;
    _print_progress = print_progress;
    _nthreads = nthreads;
}

//*************************************************************************
//  io_start - Simple wrapper for sync/async to start execution
//*************************************************************************

void io_start(opque_t *q)
{
    if (_sync_transfer == 0) opque_start_execution(q);
}

//*************************************************************************
//  io_waitall - Simple wrapper for sync/async waitall
//*************************************************************************

int io_waitall(opque_t *q)
{
    int ibp_err, err, nleft;
    op_generic_t *op;

    log_printf(15, "io_waitall: sync_transfer=%d\n", _sync_transfer);
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

        log_printf(15, "io_waitall: err=%d nfailed=%d nleft=%d\n", err, opque_tasks_failed(q), opque_tasks_left(q));
//flush_log();
    }

    return(err);
}

