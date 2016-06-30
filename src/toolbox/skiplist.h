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

//*************************************************************************
//*************************************************************************

#ifndef __SKIPLIST_H_
#define __SKIPLIST_H_

#include <apr_pools.h>
#include <apr_thread_mutex.h>

#include "tbx/skiplist.h"

#ifdef __cplusplus
extern "C" {
#endif


struct tbx_sl_ele_t {
    tbx_sl_data_t *data;
    tbx_sl_ele_t *next;
};

struct tbx_sl_node_t {
    unsigned int level;                //** level of node
    tbx_sl_key_t *key;      //** Node value
    tbx_sl_node_t **next;   //** Pointers to the next node for each level
    tbx_sl_ele_t ele;       //** Pointer to the data list
};

struct tbx_sl_compare_t {
    int (*fn)(void *arg, tbx_sl_key_t *a, tbx_sl_key_t *b);  //** Element comparison function
    void *arg;
};

struct tbx_sl_t {  //** Generic Skip Lists container
    unsigned int max_levels;         //** Max number of pointers/levels
    unsigned int current_max;        //** Current Max level
    bool allow_dups;         //** Allow duplicate keys if 1
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


#define skiplist_lock(a) apr_thread_mutex_lock((a)->lock)
#define skiplist_unlock(a) apr_thread_mutex_unlock((a)->lock)
//#define tbx_sl_key_count(a) (a)->n_keys
//#define skiplist_ele_count(a) (a)->n_ele


#define find_key(sl, ptr, key, fixed_cmp_result) find_key_compare(sl, ptr, key, (sl)->compare, fixed_cmp_result)
#define search_skiplist(sl, key) tbx_sl_search_compare(sl, key, (sl)->compare)

tbx_sl_key_t *sl_passthru_dup(tbx_sl_key_t *key);

extern tbx_sl_compare_t tbx_sl_compare_strcmp_descending;
extern tbx_sl_compare_t skiplist_compare_ptr;


tbx_sl_t *create_skiplist(bool allow_dups,
                            tbx_sl_compare_t *compare,
                            tbx_sl_key_t *(*dup)(tbx_sl_key_t *a),
                            void (*key_free)(tbx_sl_key_t *a),
                            void (*data_free)(tbx_sl_data_t *a));
int skiplist_element_count(tbx_sl_t *sl);

int iter_tbx_sl_remove(tbx_sl_iter_t *it);

int find_key_compare(tbx_sl_t *sl, tbx_sl_node_t **ptr, tbx_sl_key_t *key, tbx_sl_compare_t *compare, int fixed_cmp);
tbx_sl_node_t *pos_tbx_sl_insert(tbx_sl_t *sl, tbx_sl_node_t **ptr, tbx_sl_key_t *key, tbx_sl_data_t *data);

#ifdef __cplusplus
}
#endif

#endif


