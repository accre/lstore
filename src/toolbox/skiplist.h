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

//*************************************************************************
//*************************************************************************

#ifndef __SKIPLIST_H_
#define __SKIPLIST_H_

#include <apr_thread_mutex.h>

#ifdef __cplusplus
extern "C" {
#endif

#define SKIPLIST_MAX_LEVEL 32

typedef void skiplist_data_t;
typedef void skiplist_key_t;

struct skiplist_node_s;
typedef struct skiplist_node_s skiplist_node_t;

struct skiplist_ele_s;
typedef struct skiplist_ele_s skiplist_ele_t;

struct skiplist_ele_s {
  skiplist_data_t *data;
  skiplist_ele_t *next;
};

struct skiplist_node_s {
  int level;                //** level of node
  skiplist_key_t *key;      //** Node value
  skiplist_node_t **next;   //** Pointers to the next node for each level
  skiplist_ele_t ele;       //** Pointer to the data list
};

typedef struct {
  int (*fn)(void *arg, skiplist_key_t *a, skiplist_key_t *b);  //** Element comparison function
  void *arg;
} skiplist_compare_t;

typedef struct {  //** Generic Skip Lists container
  int max_levels;         //** Max number of pointers/levels
  int current_max;        //** Current Max level
  int allow_dups;         //** Allow duplicate keys if 1
  int n_keys;             //** Number of unique keys
  int n_ele;              //** Number of elements
  double p;               //** Negative Binomial distribution fraction
  skiplist_node_t *head;  //** Node list
  skiplist_key_t *(*dup)(skiplist_key_t *a);  //** Duplicate key function
  void (*key_free)(skiplist_key_t *a);            //** Free'sa duped key
  void (*data_free)(skiplist_data_t *a);            //** Free'sa duped key
  skiplist_compare_t *compare;
  apr_thread_mutex_t *lock;
  apr_pool_t         *pool;
} skiplist_t;

typedef struct {
  skiplist_t *sl;
  skiplist_compare_t *compare;  //** Element comparison function
  skiplist_ele_t *ele;
  skiplist_ele_t *curr;
  skiplist_ele_t *prev;
  skiplist_node_t *sn;
  skiplist_node_t *ptr[SKIPLIST_MAX_LEVEL];
} skiplist_iter_t;


#define skiplist_lock(a) apr_thread_mutex_lock((a)->lock)
#define skiplist_unlock(a) apr_thread_mutex_unlock((a)->lock)
//#define skiplist_key_count(a) (a)->n_keys
//#define skiplist_ele_count(a) (a)->n_ele


#define find_key(sl, ptr, key, fixed_cmp_result) find_key_compare(sl, ptr, key, (sl)->compare, fixed_cmp_result)
#define search_skiplist(sl, key) search_skiplist_compare(sl, key, (sl)->compare)
#define iter_search_skiplist(sl, nkey, round_mode) iter_search_skiplist_compare(sl, nkey, (sl)->compare, round_mode)

skiplist_key_t *sl_passthru_dup(skiplist_key_t *key);
void sl_no_key_free(skiplist_key_t *key);
void sl_no_data_free(skiplist_data_t *data);
void sl_simple_free(skiplist_data_t *data);
skiplist_key_t *sl_string_dup(skiplist_key_t *data);
skiplist_key_t *sl_ptr_dup(skiplist_key_t *key);

extern skiplist_compare_t skiplist_compare_int;
extern skiplist_compare_t skiplist_compare_strcmp;
extern skiplist_compare_t skiplist_compare_ptr;

void skiplist_strncmp_set(skiplist_compare_t *compare, int n);

skiplist_t *create_skiplist_full(int maxlevels, double p, int allow_dups, 
   skiplist_compare_t *compare,
   skiplist_key_t *(*dup)(skiplist_key_t *a),
   void (*key_free)(skiplist_key_t *a),
   void (*data_free)(skiplist_data_t *a));
skiplist_t *create_skiplist(int allow_dups, 
   skiplist_compare_t *compare,
   skiplist_key_t *(*dup)(skiplist_key_t *a),
   void (*key_free)(skiplist_key_t *a),
   void (*data_free)(skiplist_data_t *a));
void destroy_skiplist(skiplist_t *sl);
void empty_skiplist(skiplist_t *sl);
int insert_skiplist(skiplist_t *sl, skiplist_key_t *key, skiplist_data_t *data);
int remove_skiplist(skiplist_t *sl, skiplist_key_t *key, skiplist_data_t *data);
int skiplist_key_count(skiplist_t *sl);
int skiplist_element_count(skiplist_t *sl);

skiplist_key_t *skiplist_first_key(skiplist_t *sl);
skiplist_key_t *skiplist_last_key(skiplist_t *sl);
skiplist_data_t *search_skiplist_compare(skiplist_t *sl, skiplist_key_t *key, skiplist_compare_t *compare);
skiplist_iter_t iter_search_skiplist_compare(skiplist_t *sl, skiplist_key_t *nkey, skiplist_compare_t *compare, int round_mode);
int next_skiplist(skiplist_iter_t *it, skiplist_key_t **nkey, skiplist_data_t **ndata);
int iter_remove_skiplist(skiplist_iter_t *it);

int find_key_compare(skiplist_t *sl, skiplist_node_t **ptr, skiplist_key_t *key, skiplist_compare_t *compare, int fixed_cmp);
skiplist_node_t *pos_insert_skiplist(skiplist_t *sl, skiplist_node_t **ptr, skiplist_key_t *key, skiplist_data_t *data);

#ifdef __cplusplus
}
#endif

#endif


