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

#pragma once
#ifndef ACCRE_INTERVAL_SKIPLIST_H_INCLUDED
#define ACCRE_INTERVAL_SKIPLIST_H_INCLUDED

#include <tbx/skiplist.h>
#include <tbx/visibility.h>

#ifdef __cplusplus
extern "C" {
#endif

// Types
typedef struct tbx_isl_data_t tbx_isl_data_t;

typedef struct tbx_isl_iter_t tbx_isl_iter_t;

typedef struct tbx_isl_node_t tbx_isl_node_t;

typedef struct tbx_isl_t tbx_isl_t;

typedef tbx_sl_key_t *(*tbx_isl_dup_fn_t)(tbx_sl_key_t *a);

typedef void (*tbx_isl_key_free_fn_t)(tbx_sl_key_t *a);

typedef void (*tbx_isl_data_free_fn_t)(tbx_sl_data_t *a);

// Functions
TBX_API int tbx_isl_count2(tbx_isl_t *isl, tbx_sl_key_t *lo, tbx_sl_key_t *hi);
TBX_API void tbx_isl_del(tbx_isl_t *isl);
TBX_API int tbx_isl_insert(tbx_isl_t *isl, tbx_sl_key_t *lo, tbx_sl_key_t *hi, tbx_sl_data_t *data);
TBX_API tbx_isl_iter_t tbx_isl_iter_search(tbx_isl_t *isl, tbx_sl_key_t *lo, tbx_sl_key_t *hi);
TBX_API tbx_sl_key_t *tbx_isl_key_first(tbx_isl_t *isl);
TBX_API tbx_sl_key_t *tbx_isl_key_last(tbx_isl_t *isl);
TBX_API tbx_isl_t *tbx_isl_new(tbx_sl_compare_t *compare,
        tbx_isl_dup_fn_t dup,
        tbx_isl_key_free_fn_t key_free,
        tbx_isl_data_free_fn_t data_free);
TBX_API tbx_isl_t *tbx_isl_new_full(int maxlevels, double p,
        tbx_sl_compare_t *compare,
        tbx_isl_dup_fn_t dup,
        tbx_isl_key_free_fn_t key_free,
        tbx_isl_data_free_fn_t data_free);
TBX_API tbx_sl_data_t *tbx_isl_next(tbx_isl_iter_t *it);
TBX_API int tbx_isl_remove(tbx_isl_t *isl, tbx_sl_key_t *lo, tbx_sl_key_t *hi, tbx_sl_data_t *data);

// Preprocessor macros
#define tbx_isl_count(a) (a)->n_intervals

// TEMPORARY
#if !defined toolbox_EXPORTS && defined LSTORE_HACK_EXPORT
    struct tbx_isl_iter_t {
        tbx_sl_key_t *lo;
        tbx_sl_key_t *hi;
        tbx_isl_t *isl;
        tbx_sl_node_t *sn;
        tbx_isl_node_t *isln;
        tbx_isl_data_t *ele;
        tbx_sl_node_t *ptr[SKIPLIST_MAX_LEVEL];
        int mode;
        int ptr_level;
        int finished;
    };
    struct tbx_isl_t {  //** Generic Interval Skip Lists container
        tbx_sl_t *sl;
        void (*data_free)(tbx_sl_data_t *a);
        int n_intervals;
    };

#endif


#ifdef __cplusplus
}
#endif

#endif
