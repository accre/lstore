/*
Advanced Computing Center for Research and Education Proprietary License
Version 1.0 (April 2006)

Copyright (c) 2006, Advanced Computing Center for Research and Education,
 Vanderbilt University, All rights reserved.

This Work is the sole and exclusive property of the Advanced Computing Center
for Research and Education department at Vanderbilt University.  No right to
disclose or otherwise disseminate any of the information contained herein is
granted by virtue of your possession of this software except in accordance with
the terms and conditions of a separate License Agreement entered into with
Vanderbilt University.

THE AUTHOR OR COPYRIGHT HOLDERS PROVIDES THE "WORK" ON AN "AS IS" BASIS,
WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, TITLE, FITNESS FOR A PARTICULAR
PURPOSE, AND NON-INFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Vanderbilt University
Advanced Computing Center for Research and Education
230 Appleton Place
Nashville, TN 37203
http://www.accre.vanderbilt.edu
*/

//***********************************************************************
// Searchable list routines - Just a wrapper around the skiplist routines
//***********************************************************************

#include "skiplist.h"

#ifndef _LIST_H_
#define _LIST_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef skiplist_key_t list_key_t;
typedef skiplist_data_t list_data_t;
typedef skiplist_compare_t list_compare_t;
typedef skiplist_t list_t;
typedef skiplist_iter_t list_iter_t;

#define list_strncmp_set(cmp, n) skiplist_strncmp_set(cmp, n)
#define list_string_descending_compare skiplist_compare_strcmp_descending
#define list_string_compare skiplist_compare_strcmp
#define list_compare_ptr skiplist_compare_ptr
#define list_compare_int skiplist_compare_int
#define list_string_dup sl_string_dup
#define list_no_key_free sl_no_key_free
#define list_no_data_free sl_no_data_free
#define list_simple_free sl_simple_free
#define list_dup_ptr sl_ptr_dup

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

