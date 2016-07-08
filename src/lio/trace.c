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

#define _log_module_index 164

#include <assert.h>
#include <gop/opque.h>
#include <gop/types.h>
#include <lio/segment.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/assert_result.h>
#include <tbx/iniparse.h>
#include <tbx/log.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>

#include "trace.h"

//**********************************************************************
// trace_rw_dist_print - Prints the R/W distribution table
//**********************************************************************

void trace_rw_dist_print(lio_trace_stats_t *s, FILE *fd)
{
    int i;
    ex_off_t ntotal;
    double dr, dw, dt, drops, dwops, dops;

    dr = s->total_bytes[CMD_READ];
    dr = dr / (1024.0*1024.0);
    dw = s->total_bytes[CMD_WRITE];
    dw = dw / (1024.0*1024.0);
    dt = dr + dw;
    ntotal = s->total_bytes[CMD_READ] + s->total_bytes[CMD_WRITE];
    dops = ntotal;
    drops = s->total_ops[CMD_READ];
    dwops = s->total_ops[CMD_WRITE];

    fprintf(fd, "Total Bytes    -- Read: %8" XOTC " (%lf MB) Write: %8" XOTC " (%lf MB)  Combined: %8" XOTC " (%lf MB)\n",
            s->total_bytes[CMD_READ], dr, s->total_bytes[CMD_WRITE], dw, ntotal, dt);
    ntotal = s->total_ops[CMD_READ] + s->total_ops[CMD_WRITE];
    fprintf(fd, "Total Commands -- Read: %8" XOTC " Write: %8" XOTC " Combined: %8" XOTC "\n", s->total_ops[CMD_READ], s->total_ops[CMD_WRITE], ntotal);

    for (i=0; i<MAX_BIN; i++) {
        dr = (100.0*s->rw_dist[i][CMD_READ]) / drops;
        dw = (100.0*s->rw_dist[i][CMD_WRITE]) / dwops;
        ntotal = s->rw_dist[i][CMD_READ] + s->rw_dist[i][CMD_WRITE];
        dt = (100.0*ntotal)/dops;
        fprintf(fd, "2^%d  -- Read: %8" XOTC " (%lf%%) Write: %8" XOTC " (%lf%%)  Combined: %8" XOTC " (%lf%%)\n",
                i, s->rw_dist[i][CMD_READ], dr, s->rw_dist[i][CMD_WRITE], dw, ntotal, dt);
    }
}

//**********************************************************************
// trace_print_summary - Prints a summary of the trace
//**********************************************************************

void trace_print_summary(lio_trace_t *trace, FILE *fd)
{
    int i;

    fprintf(fd, "Trace Summary for header file %s and data file %s\n", trace->header, trace->data);
    fprintf(fd, "------------------------------------------------------------------------------------------------------------\n");
    fprintf(fd, "\n");
    fprintf(fd, "n_files: %d\n", trace->n_files);
    fprintf(fd, "n_ops: %d\n", trace->n_ops);
    fprintf(fd, "\n");

    fprintf(fd, "Total R/W Distribution\n");
    fprintf(fd, "---------------------------------------------\n");
    trace_rw_dist_print(&(trace->stats), fd);
    fprintf(fd, "\n");

    for (i=0; i<trace->n_files; i++) {
        fprintf(fd, "File %d R/W Distribution\n", i);
        fprintf(fd, "---------------------------------------------\n");
        trace_rw_dist_print(&(trace->files[i].stats), fd);
        fprintf(fd, "\n");
    }

    return;
}

//**********************************************************************
// trace_load - Loads a trace
//**********************************************************************

lio_trace_t *trace_load(lio_service_manager_t *exs, lio_exnode_t *tex, data_attr_t *da, int timeout, char *fname)
{
    tbx_inip_file_t *tfd;
    int n_files, n_ops, i, j, k, fin;
    char *trace_fname;
    FILE *fd;
    lio_trace_t *trace;
    lio_trace_op_t *op;
    lio_trace_file_t *file;
    char buffer[1024];
    char *bstate, *str;
    double log2, d;
//  char *template;
    lio_segment_t *tseg;

    tfd = tbx_inip_file_read(fname);

    n_files = tbx_inip_get_integer(tfd, "trace", "n_files", -1);
    n_ops = tbx_inip_get_integer(tfd, "trace", "n_ops", -1);
    trace_fname = tbx_inip_get_string(tfd, "trace", "trace", "");

   FATAL_UNLESS(n_files > 0);
   FATAL_UNLESS(n_ops > 0);
   FATAL_UNLESS(strlen(trace_fname) > 0);

    fd = fopen(trace_fname, "r");
    if (fd == NULL) {
        log_printf(0, "trace_load:  Cannot load data file: %s\n", trace_fname);
       FATAL_UNLESS(fd != NULL);
    }

    tbx_type_malloc_clear(trace, lio_trace_t, 1);
    tbx_type_malloc_clear(trace->ops, lio_trace_op_t, n_ops);
    tbx_type_malloc_clear(trace->files, lio_trace_file_t, n_files);

    trace->da = da;
    trace->header = fname;
    trace->data = trace_fname;
    trace->n_ops = n_ops;
    trace->n_files = n_files;

    //** Load the files
    tseg = lio_exnode_default_get(tex);
    if (tseg == NULL) {
        printf("No default segment!  Aborting!\n");
        abort();
    }

    for (i=0; i<n_files; i++) {
        file = &(trace->files[i]);

        file->seg = NULL;
        file->ex = lio_exnode_create();
        segment_clone(tseg, da, &(file->seg), CLONE_STRUCTURE, NULL, timeout);
        lio_view_insert(file->ex, file->seg);
    }

    //** and the commands
    d = 2;
    log2 = log(d);
    file = trace->files;
    for (i=0; i<n_ops; i++) {
        op = &(trace->ops[i]);
        fgets(buffer, 1024, fd);
        sscanf(tbx_stk_string_token(buffer, " ,", &bstate, &fin), "%d", &(op->fd));
        sscanf(tbx_stk_string_token(NULL, " ,", &bstate, &fin), XOT, &(op->offset));
        sscanf(tbx_stk_string_token(NULL, " ,", &bstate, &fin), XOT, &(op->len));
        str = tbx_stk_string_token(NULL, " ,", &bstate, &fin);
        if ((str[0] == 'R') || (str[0] == 'r')) {
            op->cmd = CMD_WRITE;
        }
        if ((str[0] == 'W') || (str[0] == 'w')) {
            op->cmd = CMD_READ;
        }

        //** Update the RW distribution table
        d = op->len;
        d = log(d)/log2;
        j = d;
        file[op->fd].stats.rw_dist[op->cmd][j]++;

        file[op->fd].stats.total_bytes[op->cmd] += op->len;
        file[op->fd].stats.total_ops[op->cmd] ++;
    }

    //** Make the summary
    for (i=0; i<n_files; i++) {
        for (j=0; j<2; j++) {
            trace->stats.total_bytes[j] += file[i].stats.total_bytes[j];
            trace->stats.total_ops[j] += file[i].stats.total_ops[j];

            for (k=0; k<MAX_BIN; k++) {
                trace->stats.rw_dist[j][k] += file[i].stats.rw_dist[j][k];
            }
        }
    }

    return(trace);
}

//**********************************************************************
// trace_destroy - Destroy a trace
//**********************************************************************

void trace_destroy(lio_trace_t *trace)
{
    int i;
    gop_op_generic_t *gop;
    gop_opque_t *q;

    q = gop_opque_new();
    for (i=0; i<trace->n_files; i++) {
        gop = lio_segment_truncate(trace->files[i].seg, trace->da, 0, 60);
        gop_opque_add(q, gop);
    }

    opque_waitall(q);

    for (i=0; i<trace->n_files; i++) {
        lio_exnode_destroy(trace->files[i].ex);
    }

    free(trace->files);
    free(trace->ops);
    free(trace);
}

