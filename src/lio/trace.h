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

#include "exnode.h"
#include "object_service_abstract.h"

#ifndef _TRACE_H_
#define _TRACE_H_

#define CMD_READ  0
#define CMD_WRITE 1

#define MAX_BIN 32

typedef struct {
  ex_off_t offset;
  ex_off_t len;
  int fd;
  int cmd;
} trace_op_t;

typedef struct {
  ex_off_t total_bytes[2];
  ex_off_t total_ops[2];
  ex_off_t rw_dist[2][MAX_BIN];
} trace_stats_t;

typedef struct {
  exnode_t *ex;
  segment_t *seg;
//  os_fd_t *fd;
  int op_count;
  ex_off_t max_offset;
  ex_off_t max_len;
  int id;
  trace_stats_t stats;
} trace_file_t;

typedef struct {
  char *header;
  char *data;
  int n_files;
  int n_ops;
  trace_op_t *ops;
  trace_file_t *files;
  trace_stats_t stats;
  data_attr_t *da;
} trace_t;

trace_t *trace_load(exnode_abstract_set_t *exs, exnode_t *template, data_attr_t *da, int timeout, char *fname);
void trace_destroy(trace_t *trace);
void trace_print_summary(trace_t *trace, FILE *fd);

#endif
