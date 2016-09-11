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
#ifndef ACCRE_LIST_H_INCLUDED
#define ACCRE_LIST_H_INCLUDED

#ifdef __cplusplus
extern "C" {
#endif

// Precompiler macros
#define tbx_list_create(allow_dups, cmp, dup_fn, key_free_fn, data_free_fn) \
    tbx_sl_new_full(20, 0.5, allow_dups, cmp, dup_fn, key_free_fn, data_free_fn)

#define tbx_list_destroy(sl) tbx_sl_del(sl)

#define tbx_list_first_key(a) tbx_sl_first_key(a)

#define tbx_list_insert(sl, key, data) tbx_sl_insert(sl, key, data)

#define tbx_list_iter_search(sl, nkey, round_mode) tbx_sl_iter_search_compare(sl, nkey, (sl)->compare, round_mode)

#define tbx_list_iter_search_compare(sl, nkey, compare, round_mode) tbx_sl_iter_search_compare(sl, nkey, compare, round_mode)

#define tbx_list_key_count(a) (a)->n_keys

#define tbx_list_next(it, nkey, ndata) tbx_sl_next(it, nkey, ndata)

#define tbx_list_no_data_free tbx_sl_free_no_data

#define tbx_list_no_key_free tbx_sl_free_no_key

#define tbx_list_remove(sl, key, data) tbx_sl_remove(sl, key, data)

#define tbx_list_search(sl, key) tbx_sl_search_compare(sl, key, (sl)->compare)

#define tbx_list_simple_free tbx_sl_free_simple

#define tbx_list_string_compare tbx_sl_compare_strcmp

#define tbx_list_string_dup tbx_sl_dup_string

#define tbx_list_strncmp_set(cmp, n) tbx_sl_set_strncmp(cmp, n)
// TEMPORARY
#include <tbx/skiplist.h>
typedef tbx_sl_key_t tbx_list_key_t;
typedef tbx_sl_t tbx_list_t;
typedef tbx_sl_iter_t tbx_list_iter_t;
typedef tbx_sl_data_t tbx_list_data_t;
typedef tbx_sl_compare_t tbx_list_compare_t;


#ifdef __cplusplus
}
#endif

#endif
