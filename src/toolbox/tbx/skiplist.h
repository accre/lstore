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
#ifndef ACCRE_SKIPLIST_H_INCLUDED
#define ACCRE_SKIPLIST_H_INCLUDED

#include <apr_pools.h>
#include <apr_thread_mutex.h>
#include "tbx/toolbox_visibility.h"

#ifdef __cplusplus
extern "C" {
#endif

// Types
typedef struct tbx_sl_compare_t tbx_sl_compare_t;

typedef void tbx_sl_data_t;

typedef struct tbx_sl_ele_t tbx_sl_ele_t;

typedef void tbx_sl_key_t;

typedef struct tbx_sl_iter_t tbx_sl_iter_t;

typedef struct tbx_sl_node_t tbx_sl_node_t;

typedef struct tbx_sl_t tbx_sl_t;


// Functions
TBX_API tbx_sl_t *tbx_sl_create_full(int maxlevels, double p, int allow_dups,
                                        tbx_sl_compare_t *compare,
                                        tbx_sl_key_t *(*dup)(tbx_sl_key_t *a),
                                        void (*key_free)(tbx_sl_key_t *a),
                                        void (*data_free)(tbx_sl_data_t *a));

TBX_API void tbx_sl_destroy(tbx_sl_t *sl);

TBX_API void tbx_sl_empty(tbx_sl_t *sl);

TBX_API int tbx_sl_insert(tbx_sl_t *sl, tbx_sl_key_t *key, tbx_sl_data_t *data);

TBX_API tbx_sl_iter_t tbx_sl_iter_search_compare(tbx_sl_t *sl, tbx_sl_key_t *nkey, tbx_sl_compare_t *compare, int round_mode);

TBX_API int tbx_sl_next(tbx_sl_iter_t *it, tbx_sl_key_t **nkey, tbx_sl_data_t **ndata);

TBX_API int tbx_sl_remove(tbx_sl_t *sl, tbx_sl_key_t *key, tbx_sl_data_t *data);

TBX_API tbx_sl_data_t *tbx_sl_search_compare(tbx_sl_t *sl, tbx_sl_key_t *key, tbx_sl_compare_t *compare);

TBX_API tbx_sl_key_t *tbx_sl_first_key(tbx_sl_t *sl);

TBX_API int tbx_sl_key_count(tbx_sl_t *sl);

TBX_API tbx_sl_key_t *tbx_sl_last_key(tbx_sl_t *sl);

TBX_API void tbx_sl_strncmp_set(tbx_sl_compare_t *compare, int n);

TBX_API void tbx_sl_no_data_free(tbx_sl_data_t *data);

TBX_API void tbx_sl_no_key_free(tbx_sl_key_t *key);

TBX_API void tbx_sl_simple_free(tbx_sl_data_t *data);

TBX_API tbx_sl_key_t *tbx_sl_string_dup(tbx_sl_key_t *data);

TBX_API extern tbx_sl_compare_t tbx_sl_compare_int;

TBX_API extern tbx_sl_compare_t tbx_sl_compare_strcmp;

// Preprocessor macros
#define SKIPLIST_MAX_LEVEL 32
#define tbx_sl_iter_search(sl, nkey, round_mode) tbx_sl_iter_search_compare(sl, nkey, (sl)->compare, round_mode)
// TEMPORARY
#if !defined toolbox_EXPORTS && defined LSTORE_HACK_EXPORT
    struct tbx_sl_t {  //** Generic Skip Lists container
        int max_levels;         //** Max number of pointers/levels
        int current_max;        //** Current Max level
        int allow_dups;         //** Allow duplicate keys if 1
        int n_keys;             //** Number of unique keys
        int n_ele;              //** Number of elements
        double p;               //** Negative Binomial distribution fraction
        tbx_sl_node_t *head;  //** Node list
        tbx_sl_key_t *(*dup)(tbx_sl_key_t *a);  //** Duplicate key function
        void (*key_free)(tbx_sl_key_t *a);            //** Free'sa duped key
        void (*data_free)(tbx_sl_data_t *a);            //** Free'sa duped key
        tbx_sl_compare_t *compare;
        apr_thread_mutex_t *lock;
        apr_pool_t         *pool;
    };
    struct tbx_sl_iter_t {
        tbx_sl_t *sl;
        tbx_sl_compare_t *compare;  //** Element comparison function
        tbx_sl_ele_t *ele;
        tbx_sl_ele_t *curr;
        tbx_sl_ele_t *prev;
        tbx_sl_node_t *sn;
        tbx_sl_node_t *ptr[SKIPLIST_MAX_LEVEL];
    };
    struct tbx_sl_compare_t {
        int (*fn)(void *arg, tbx_sl_key_t *a, tbx_sl_key_t *b);  //** Element comparison function
        void *arg;
    };
#endif

#ifdef __cplusplus
}
#endif

#endif
