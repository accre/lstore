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
#include "lio/segment_log.h"

#ifndef _SEGMENT_LOG_H_
#define _SEGMENT_LOG_H_

#include <gop/opque.h>
#include <gop/types.h>

#include "data_service_abstract.h"
#include "ex3_abstract.h"
#include "ex3_types.h"
#include "lio/lio_visibility.h"
#include "service_manager.h"

#ifdef __cplusplus
extern "C" {
#endif

#define SEGMENT_TYPE_LOG "log"

segment_t *segment_log_load(void *arg, ex_id_t id, exnode_exchange_t *ex);
segment_t *segment_log_create(void *arg);
segment_t *slog_make(service_manager_t *sm, segment_t *table, segment_t *data, segment_t *base);  //** Makes a new log segment using


#ifdef __cplusplus
}
#endif

#endif

