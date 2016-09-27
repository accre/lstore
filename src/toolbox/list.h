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

#ifndef _LIST_H_
#define _LIST_H_

#include <tbx/skiplist.h>
#include <tbx/list.h>

#ifdef __cplusplus
extern "C" {
#endif

#define list_string_descending_compare tbx_sl_compare_strcmp_descending
#define list_compare_ptr skiplist_compare_ptr
#define list_compare_int tbx_sl_compare_int
#define tbx_list_no_key_free tbx_sl_free_no_key

#define list_lock(a) apr_thread_mutex_lock((a)->lock)
#define list_unlock(a) apr_thread_mutex_unlock((a)->lock)
#define list_ele_count(a) (a)->n_ele

#define list_last_key(a) tbx_sl_key_last(a)

#define list_find_key_compare(sl, ptr, key, compare) find_key_compare(sl, ptr, key, compare)
#define list_find_key(sl, ptr, key) find_key_compare(sl, ptr, key, (sl)->compare)
#define list_search_compare(sl, key, compare) tbx_sl_search_compare(sl, key, compare)
#define list_iter_remove(it) iter_tbx_sl_remove(it)
#define list_empty(sl) tbx_sl_empty(sl)

#ifdef __cplusplus
}
#endif

#endif

