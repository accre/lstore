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

//***********************************************************************
// LUN segment support
//***********************************************************************

#ifndef _SEGMENT_LOG_PRIV_H_
#define _SEGMENT_LOG_PRIV_H_

#include "interval_skiplist.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    ex_off_t lo;
    ex_off_t hi;  //** On disk this is actually the length.  It's converted to an offset when loaded and a length when stored
    ex_off_t data_offset;
} slog_range_t;

typedef struct {
    segment_t *table_seg;
    segment_t *data_seg;
    segment_t *base_seg;
    data_service_fn_t *ds;
    tbx_isl_t *mapping;
    thread_pool_context_t *tpc;
    ex_off_t file_size;
    ex_off_t log_size;
    ex_off_t data_size;
    int soft_errors;
    int hard_errors;
} seglog_priv_t;

#ifdef __cplusplus
}
#endif

#endif

