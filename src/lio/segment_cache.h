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
// Generic cache segment support
//***********************************************************************

#ifndef _SEGMENT_CACHE_H_
#define _SEGMENT_CACHE_H_

#include <lio/lio_visibility.h>
#include <lio/segment.h>

#include "cache.h"
#include "ex3.h"
#include "ex3_types.h"

#ifdef __cplusplus
extern "C" {
#endif

#define SEGMENT_TYPE_CACHE "cache"

int cache_page_drop(segment_t *seg, ex_off_t lo, ex_off_t hi);
segment_t *segment_cache_load(void *arg, ex_id_t id, exnode_exchange_t *ex);
segment_t *segment_cache_create(void *arg);

#ifdef __cplusplus
}
#endif

#endif
