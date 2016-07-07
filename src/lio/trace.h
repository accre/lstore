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

#ifndef _TRACE_H_
#define _TRACE_H_

#include <lio/trace.h>
#include <stdio.h>

#include "ds.h"
#include "ex3.h"
#include "ex3/types.h"
#include "os.h"
#include "service_manager.h"

#ifdef __cplusplus
extern "C" {
#endif

struct lio_trace_file_t;
struct lio_trace_op_t;
struct lio_trace_stats_t;
struct lio_trace_t;


#define CMD_READ  0
#define CMD_WRITE 1

#define MAX_BIN 32

struct lio_trace_op_t {
    ex_off_t offset;
    ex_off_t len;
    int fd;
    int cmd;
};

struct lio_trace_stats_t {
    ex_off_t total_bytes[2];
    ex_off_t total_ops[2];
    ex_off_t rw_dist[2][MAX_BIN];
};

struct lio_trace_file_t {
    lio_exnode_t *ex;
    lio_segment_t *seg;
    int op_count;
    ex_off_t max_offset;
    ex_off_t max_len;
    int id;
    lio_trace_stats_t stats;
};

struct lio_trace_t {
    char *header;
    char *data;
    int n_files;
    int n_ops;
    lio_trace_op_t *ops;
    lio_trace_file_t *files;
    lio_trace_stats_t stats;
    data_attr_t *da;
};

lio_trace_t *trace_load(lio_service_manager_t *exs, lio_exnode_t *template, data_attr_t *da, int timeout, char *fname);
void trace_destroy(lio_trace_t *trace);
void trace_print_summary(lio_trace_t *trace, FILE *fd);

#ifdef __cplusplus
}
#endif

#endif
