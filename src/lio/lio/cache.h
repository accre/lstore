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

/** \file
* Autogenerated public API
*/

#ifndef ACCRE_LIO_CACHE_PRIV_H_INCLUDED
#define ACCRE_LIO_CACHE_PRIV_H_INCLUDED

#include <lio/ex3.h>
#include <lio/visibility.h>

#ifdef __cplusplus
extern "C" {
#endif

// Typedefs
typedef struct lio_cache_cond_t lio_cache_cond_t;
typedef struct lio_cache_counters_t lio_cache_counters_t;
typedef struct lio_cache_fn_t lio_cache_fn_t;
typedef struct lio_cache_page_t lio_cache_page_t;
typedef struct lio_cache_partial_page_t lio_cache_partial_page_t;
typedef struct lio_cache_range_t lio_cache_range_t;
typedef struct lio_cache_segment_t lio_cache_segment_t;
typedef struct lio_cache_t lio_cache_t;
typedef struct lio_data_page_t lio_data_page_t;
typedef struct lio_cache_stats_get_t lio_cache_stats_get_t;

// Functions
LIO_API lio_cache_stats_get_t get_lio_cache_stats_get(lio_cache_t *c);
LIO_API int lio_segment_cache_pages_drop(lio_segment_t *seg, ex_off_t lo, ex_off_t hi);

#ifdef __cplusplus
}
#endif

#endif /* ^ ACCRE_LIO_CACHE_PRIV_H_INCLUDED ^ */ 
