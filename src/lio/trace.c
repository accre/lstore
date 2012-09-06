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

#define _log_module_index 164

#include <math.h>
#include "trace.h"
#include "iniparse.h"
#include "type_malloc.h"
#include "log.h"
#include "string_token.h"

//**********************************************************************
// trace_rw_dist_print - Prints the R/W distribution table
//**********************************************************************

void trace_rw_dist_print(trace_stats_t *s, FILE *fd)
{
  int i;
  ex_off_t ntotal;
  double dr, dw, dt, drops, dwops, dops;
  
  dr = s->total_bytes[CMD_READ]; dr = dr / (1024.0*1024.0);
  dw = s->total_bytes[CMD_WRITE]; dw = dw / (1024.0*1024.0);
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

void trace_print_summary(trace_t *trace, FILE *fd)
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

trace_t *trace_load(exnode_abstract_set_t *exs, exnode_t *tex, data_attr_t *da, int timeout, char *fname)
{
  inip_file_t *tfd;
  int n_files, n_ops, i, j, k, fin;
  char *trace_fname;
  FILE *fd;
  trace_t *trace;
  trace_op_t *op;
  trace_file_t *file;
  char buffer[1024];
  char *bstate, *str;
  double log2, d;
//  char *template;
  segment_t *tseg;

  tfd = inip_read(fname);

  n_files = inip_get_integer(tfd, "trace", "n_files", -1);
  n_ops = inip_get_integer(tfd, "trace", "n_ops", -1);
  trace_fname = inip_get_string(tfd, "trace", "trace", "");

  assert(n_files > 0);
  assert(n_ops > 0);
  assert(strlen(trace_fname) > 0);

  fd = fopen(trace_fname, "r");
  if (fd == NULL) {
     log_printf(0, "trace_load:  Cannot load data file: %s\n", trace_fname);
     assert(fd != NULL);
  }

  type_malloc_clear(trace, trace_t, 1);
  type_malloc_clear(trace->ops, trace_op_t, n_ops);
  type_malloc_clear(trace->files, trace_file_t, n_files);

  trace->da = da;
  trace->header = fname;
  trace->data = trace_fname;
  trace->n_ops = n_ops;
  trace->n_files = n_files;

  //** Load the files
  tseg = exnode_get_default(tex);
  if (tseg == NULL) {
     printf("No default segment!  Aborting!\n");
     abort();
  }

  for (i=0; i<n_files; i++) {
    file = &(trace->files[i]);

    file->seg = NULL;
    file->ex = exnode_create();
    segment_clone(tseg, da, &(file->seg), CLONE_STRUCTURE, NULL, timeout);
    view_insert(file->ex, file->seg);
  }

  //** and the commands
  d = 2;
  log2 = log(d);
  file = trace->files;
  for (i=0; i<n_ops; i++) {
    op = &(trace->ops[i]);
    fgets(buffer, 1024, fd);
    sscanf(string_token(buffer, " ,", &bstate, &fin), "%d", &(op->fd));
    sscanf(string_token(NULL, " ,", &bstate, &fin), XOT, &(op->offset));
    sscanf(string_token(NULL, " ,", &bstate, &fin), XOT, &(op->len));
    str = string_token(NULL, " ,", &bstate, &fin);
    if ((str[0] == 'R') || (str[0] == 'r')) {
       op->cmd = CMD_WRITE;
    } if ((str[0] == 'W') || (str[0] == 'w')) {
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

void trace_destroy(trace_t *trace)
{
  int i;
  op_generic_t *gop;
  opque_t *q;

  q = new_opque();
  for (i=0; i<trace->n_files; i++) {
     gop = segment_truncate(trace->files[i].seg, trace->da, 0, 60);
     opque_add(q, gop);
  }

  opque_waitall(q);

  for (i=0; i<trace->n_files; i++) {
     exnode_destroy(trace->files[i].ex);
  }
  
  free(trace->files);
  free(trace->ops);
  free(trace);    
}

