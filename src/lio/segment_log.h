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
// Log structured segment support
//***********************************************************************
#include "lio/lio_visibility.h"
#include <gop/opque.h>

#ifndef _SEGMENT_LOG_H_
#define _SEGMENT_LOG_H_

#ifdef __cplusplus
extern "C" {
#endif

#define SEGMENT_TYPE_LOG "log"

segment_t *segment_log_load(void *arg, ex_id_t id, exnode_exchange_t *ex);
segment_t *segment_log_create(void *arg);
segment_t *slog_make(service_manager_t *sm, segment_t *table, segment_t *data, segment_t *base);  //** Makes a new log segment using

LIO_API op_generic_t *lio_slog_merge_with_base(segment_t *seg, data_attr_t *da, ex_off_t bufsize, char *buffer, int truncate_old_log, int timeout);  //** Merges the current log with the base

#ifdef __cplusplus
}
#endif

#endif

