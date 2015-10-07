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

#define _log_module_index 168

#include <assert.h>
#include "exnode.h"
#include "log.h"
#include "iniparse.h"
#include "type_malloc.h"
#include "thread_pool.h"
#include "trace.h"
#include "lio.h"


//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int bufsize = 1024*1024;
    char buffer[bufsize+1];
    tbuffer_t tbuf;
    int i, start_option, np, update_interval;
    char *trace_header;
    char *base_path;
    char *template_name = NULL;
    exnode_t *tex;
    exnode_exchange_t *template_exchange;
    ex_iovec_t *iov;
    op_generic_t *gop;
    trace_t *trace;
    trace_op_t *top;
    opque_t *q;
    apr_time_t start_time, end_time;

    double dt;

//printf("argc=%d\n", argc);
    if (argc < 2) {
        printf("\n");
        printf("trace_replay LIO_COMMON_OPTIONS [-np n_at_once] [-i update_interval] [-path base_path] -template template.ex3 -t header.trh \n");
        lio_print_options(stdout);
        printf("    -path base_path - Base LIO directory path\n");
        printf("    -np n_at_once   - Number of commands to execute in parallel (default is 1)\n");
        printf("    -t header.trh   - Trace header file\n");
        printf("\n");
        return(1);
    }

    lio_init(&argc, &argv);

    np = 1;
    update_interval = 1000;

    //*** Parse the args
    i=1;
    do {
        start_option = i;

        if (strcmp(argv[i], "-template") == 0) { //** The template exnode
            i++;
            template_name = argv[i];
            i++;
        } else if (strcmp(argv[i], "-path") == 0) { //** Base LIO path
            i++;
            base_path = argv[i];
            i++;
        } else if (strcmp(argv[i], "-t") == 0) { //** TRace config file
            i++;
            trace_header = argv[i];
            i++;
        } else if (strcmp(argv[i], "-np") == 0) { //** TRace config file
            i++;
            np = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-i") == 0) { //** TRace config file
            i++;
            update_interval = atoi(argv[i]);
            i++;
        }
    } while (start_option < i);

    assert(template_name == NULL);

    //** Load the template
    template_exchange = exnode_exchange_load_file(template_name);
    tex = exnode_create();
    exnode_deserialize(tex, template_exchange, lio_gc->ess);

    //** Load the trace
    trace = trace_load(exnode_service_set, tex, lio_gc->da, lio_gc->timeout, trace_header);
    type_malloc_clear(iov, ex_iovec_t, trace->n_files);

    q = new_opque();
    tbuffer_single(&tbuf, bufsize, buffer);
    start_time = apr_time_now();
    for (i=0; i<trace->n_ops; i++) {
        if ((i%update_interval) == 0) {
            log_printf(0, "trace_replay: Submitting task %d\n", i);
        }

        top = &(trace->ops[i]);
        ex_iovec_single(&(iov[top->fd]), top->offset, top->len);
        if (top->cmd == CMD_READ) {
            gop = segment_write(trace->files[top->fd].seg, lio_gc->da, NULL, 1, &(iov[top->fd]), &tbuf, 0, lio_gc->timeout);
        } else {
            gop = segment_write(trace->files[top->fd].seg, lio_gc->da, NULL, 1, &(iov[top->fd]), &tbuf, 0, lio_gc->timeout);
        }

        gop_set_id(gop, i);
        opque_add(q, gop);
        if (opque_tasks_left(q) >= np) {
            gop = opque_waitany(q);
            if (gop_completed_successfully(gop) != OP_STATE_SUCCESS) {
                log_printf(0, "trace_replay: Errow with command index=%d\n", gop_id(gop));
            }
            gop_free(gop, OP_DESTROY);
        }
    }

    log_printf(0, "trace_replay:: Completed task submission (n_ops=%d).  Waiting for taks to complete\n", trace->n_ops);
    opque_waitall(q);

    end_time = apr_time_now();

    dt = end_time - start_time;
    dt = dt / APR_USEC_PER_SEC;
    log_printf(0, "trace_replay:  Total processing time: %lf\n", dt);

    trace_destroy(trace);

    //** Shut everything down;
    exnode_destroy(tex);

    lio_shutdown();

    return(0);
}


