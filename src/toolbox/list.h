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
// Searchable list routines - Just a wrapper around the skiplist routines
//***********************************************************************

#include "tbx/toolbox_visibility.h"
#include "skiplist.h"

#ifndef _LIST_H_
#define _LIST_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef tbx_sl_key_t tbx_list_key_t;
typedef tbx_sl_data_t tbx_list_data_t;
typedef tbx_sl_compare_t tbx_list_compare_t;
typedef tbx_sl_t tbx_list_t;
typedef tbx_sl_iter_t tbx_list_iter_t;

#define list_strncmp_set(cmp, n) skiplist_strncmp_set(cmp, n)
#define list_string_descending_compare tbx_sl_compare_strcmp_descending
#define list_string_compare tbx_sl_compare_strcmp
#define list_compare_ptr skiplist_compare_ptr
#define list_compare_int tbx_sl_compare_int
#define list_string_dup sl_string_dup
#define list_no_key_free sl_no_key_free
#define list_no_data_free sl_no_data_free
#define list_simple_free sl_simple_free

#define list_lock(a) apr_thread_mutex_lock((a)->lock)
#define list_unlock(a) apr_thread_mutex_unlock((a)->lock)
#define list_key_count(a) (a)->n_keys
#define list_ele_count(a) (a)->n_ele

#define list_first_key(a) skiplist_first_key(a)
#define list_last_key(a) skiplist_last_key(a)

#define list_find_key_compare(sl, ptr, key, compare) find_key_compare(sl, ptr, key, compare)
#define list_find_key(sl, ptr, key) find_key_compare(sl, ptr, key, (sl)->compare)
#define list_search(sl, key) search_skiplist_compare(sl, key, (sl)->compare)
#define list_search_compare(sl, key, compare) search_skiplist_compare(sl, key, compare)
#define list_iter_search(sl, nkey, round_mode) iter_search_skiplist_compare(sl, nkey, (sl)->compare, round_mode)
#define list_iter_search_compare(sl, nkey, compare, round_mode) iter_search_skiplist_compare(sl, nkey, compare, round_mode)
#define list_next(it, nkey, ndata) next_skiplist(it, nkey, ndata)
#define list_iter_remove(it) iter_remove_skiplist(it)
#define list_create(allow_dups, cmp, dup_fn, key_free_fn, data_free_fn) \
    create_skiplist_full(20, 0.5, allow_dups, cmp, dup_fn, key_free_fn, data_free_fn)
#define list_destroy(sl) destroy_skiplist(sl)
#define list_empty(sl) empty_skiplist(sl)
#define list_insert(sl, key, data) insert_skiplist(sl, key, data)
#define list_remove(sl, key, data) remove_skiplist(sl, key, data)

#ifdef __cplusplus
}
#endif

#endif

