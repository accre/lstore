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

#ifndef _SEGMENT_LOG_H_
#define _SEGMENT_LOG_H_

#include <gop/opque.h>
#include <gop/types.h>
#include <lio/visibility.h>
#include <lio/segment.h>
#include <tbx/interval_skiplist.h>

#include "ds.h"
#include "ex3.h"
#include "ex3/types.h"
#include "service_manager.h"

#ifdef __cplusplus
extern "C" {
#endif

#define SEGMENT_TYPE_LOG "log"

lio_segment_t *segment_log_load(void *arg, ex_id_t id, lio_exnode_exchange_t *ex);
lio_segment_t *segment_log_create(void *arg);
lio_segment_t *slog_make(lio_service_manager_t *sm, lio_segment_t *table, lio_segment_t *data, lio_segment_t *base);  //** Makes a new log segment using

struct lio_slog_range_t {
    ex_off_t lo;
    ex_off_t hi;  //** On disk this is actually the length.  It's converted to an offset when loaded and a length when stored
    ex_off_t data_offset;
};

struct lio_seglog_priv_t {
    lio_segment_t *table_seg;
    lio_segment_t *data_seg;
    lio_segment_t *base_seg;
    lio_data_service_fn_t *ds;
    tbx_isl_t *mapping;
    gop_thread_pool_context_t *tpc;
    ex_off_t file_size;
    ex_off_t log_size;
    ex_off_t data_size;
    int soft_errors;
    int hard_errors;
};

#ifdef __cplusplus
}
#endif

#endif
