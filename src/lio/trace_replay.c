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

#define _log_module_index 168

#include <apr_time.h>
#include <assert.h>
#include <gop/gop.h>
#include <gop/opque.h>
#include <gop/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/log.h>
#include <tbx/transfer_buffer.h>
#include <tbx/type_malloc.h>

#include "ex3_abstract.h"
#include "ex3_system.h"
#include "ex3_types.h"
#include "lio_abstract.h"
#include "trace.h"


//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int bufsize = 1024*1024;
    char buffer[bufsize+1];
    tbx_tbuf_t tbuf;
    int i, start_option, np, update_interval;
    char *trace_header = NULL;
    char *template_name = NULL;
    exnode_t *tex;
    exnode_exchange_t *template_exchange;
    ex_tbx_iovec_t *iov;
    op_generic_t *gop;
    trace_t *trace;
    trace_op_t *top;
    opque_t *q;
    apr_time_t start_time, end_time;

    double dt;

    if (argc < 2) {
        printf("\n");
        printf("trace_replay LIO_COMMON_OPTIONS [-np n_at_once] [-i update_interval] -template template.ex3 -t header.trh \n");
        lio_print_options(stdout);
        printf("    -np n_at_once   - Number of commands to execute in parallel (default is 1)\n");
        printf("    -t header.trh   - Trace header file\n");
        printf("\n");
        return 1;
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
    } while (start_option - i < 0);

    // Not sure why there was an assert here... If someone didn't want to set
    // a template, they could've just removed the option. - AMM
    if (template_name == NULL) {
        fprintf(stderr, "Template_name wasn't null!\n");
        return 1;
    }

    //** Load the template
    template_exchange = lio_exnode_exchange_load_file(template_name);
    tex = lio_exnode_create();
    lio_exnode_deserialize(tex, template_exchange, lio_gc->ess);

    //** Load the trace
    trace = trace_load(lio_exnode_service_set, tex, lio_gc->da, lio_gc->timeout, trace_header);
    tbx_type_malloc_clear(iov, ex_tbx_iovec_t, trace->n_files);

    q = gop_opque_new();
    tbx_tbuf_single(&tbuf, bufsize, buffer);
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
        gop_opque_add(q, gop);
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
    lio_exnode_destroy(tex);

    lio_shutdown();

    return 0;
}
