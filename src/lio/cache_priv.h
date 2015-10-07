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

#ifndef __CACHE_PRIV_H_
#define __CACHE_PRIV_H_

#include "list.h"
#include "pigeon_coop.h"
#include "ex3_abstract.h"
#include "atomic_counter.h"

#ifdef __cplusplus
extern "C" {
#endif

#define CACHE_MAX_PAGES_RETURNED 1000

#define CACHE_NONBLOCK  0
#define CACHE_DOBLOCK   1

#define CACHE_READ  0
#define CACHE_WRITE 1
#define CACHE_FLUSH 2
#define CACHE_SUM_SIZE  2

#define C_ISDIRTY   1
#define C_EMPTY     2
#define C_TORELEASE 4

struct cache_s;
typedef struct cache_s cache_t;

typedef struct {
  ex_off_t lo;
  ex_off_t hi;
  ex_off_t boff;
  int iov_index;
} cache_range_t;

typedef struct {
  ex_off_t read_count;
  ex_off_t write_count;
  ex_off_t read_bytes;
  ex_off_t write_bytes;
} cache_counters_t;

typedef struct {
  cache_counters_t user;
  cache_counters_t system;
  ex_off_t dirty_bytes;
  ex_off_t hit_bytes;
  ex_off_t miss_bytes;
  ex_off_t unused_bytes;
  apr_time_t hit_time;
  apr_time_t miss_time;
} cache_stats_t;

typedef struct {
  apr_thread_cond_t *cond;
  int count;
} cache_cond_t;

#define CPP_BEGIN 1
#define CPP_END   2
#define CPP_FULL  4

typedef struct {
  ex_off_t page_start;
  ex_off_t page_end;
  Stack_t *range_stack;
  char *data;
  int flags;
} cache_partial_page_t;

typedef struct {
  cache_t *c;
  void *cache_priv;
  segment_t *child_seg;
  thread_pool_context_t *tpc_unlimited;
  list_t *pages;
  list_t *partial_pages;
  apr_thread_mutex_t *lock;
  apr_thread_cond_t  *flush_cond;
  apr_thread_cond_t  *ppages_cond;
  Stack_t *flush_stack;
  Stack_t *ppages_unused;
  char *qname;
  cache_partial_page_t *ppage;
  char *ppages_buffer;
  int cache_check_in_progress;
  int dumping_pages;
  int close_requested;
  int n_ppages;
  int ppages_used;
  int ppages_flushing;
  ex_off_t ppage_max;
  ex_off_t page_size;
  ex_off_t child_last_page;
  ex_off_t total_size;
  cache_stats_t stats;
} cache_segment_t;

typedef struct {
  char *ptr;
  int  usage_count;
} data_page_t;

typedef struct {
   segment_t *seg;
   data_page_t *curr_data;
   data_page_t data[2];
   void *priv;
   ex_off_t offset;
   pigeon_coop_hole_t cond_pch;
   int  bit_fields;
   int access_pending[3];
   int used_count;
   int current_index;
}  cache_page_t;

typedef struct {
  cache_page_t *p;
  data_page_t *data;
} page_handle_t;

typedef struct {
  Stack_t *stack;
  segment_t *seg;
  ex_id_t   id;
  pigeon_coop_hole_t pch;
  ex_off_t lo, hi;
} page_table_t;

typedef struct cache_fn_s cache_fn_t;

struct cache_fn_s {
  void *priv;
  void (*adding_segment)(cache_t *c, segment_t *seg);
  void (*removing_segment)(cache_t *c, segment_t *seg);
  cache_page_t *(*create_empty_page)(cache_t *c, segment_t *seg, int doblock);
  void (*adjust_dirty)(cache_t *c, ex_off_t tweak);
  void (*destroy_pages)(cache_t *c, cache_page_t **p, int n_pages, int remove_from_segment);
  void (*cache_update)(cache_t *c, segment_t *seg, int rw_mode, ex_off_t lo, ex_off_t hi, void *miss);
  void (*cache_miss_tag)(cache_t *c, segment_t *seg, int rw_mode, ex_off_t lo, ex_off_t hi, ex_off_t missing_offset, void **miss);
  int (*s_page_access)(cache_t *c, cache_page_t *p, int rw_mode, ex_off_t request_len);
  int (*s_pages_release)(cache_t *c, cache_page_t **p, int n_pages);
  cache_t *(*get_handle)(cache_t *);
  int (*destroy)(cache_t *c);
};

struct cache_s {
   cache_fn_t fn;
   apr_pool_t *mpool;
   apr_thread_mutex_t *lock;
   list_t *segments;
   pigeon_coop_t *cond_coop;
   data_attr_t *da;
   ex_off_t default_page_size;
   cache_stats_t stats;
   ex_off_t max_fetch_size;
   ex_off_t write_temp_overflow_size;
   ex_off_t write_temp_overflow_used;
   double   max_fetch_fraction;
   double   write_temp_overflow_fraction;
   int n_ppages;
   int timeout;
   int  shutdown_request;
};

extern atomic_int_t _cache_count;

#define unique_cache_id() atomic_inc(_cache_count);
#define cache_lock(c) apr_thread_mutex_lock((c)->lock)
#define cache_unlock(c) apr_thread_mutex_unlock((c)->lock)
#define cache_get_handle(c) (c)->fn.get_handle(c)
#define cache_destroy(c) (c)->fn.destroy(c)

cache_stats_t get_cache_stats(cache_t *c);
cache_t *cache_base_handle(cache_t *);
void cache_base_destroy(cache_t *c);
void cache_base_create(cache_t *c, data_attr_t *da, int timeout);
void *cache_cond_new(void *arg, int size);
void cache_cond_free(void *arg, int size, void *data);
op_generic_t *cache_flush_range(segment_t *seg, data_attr_t *da, ex_off_t lo, ex_off_t hi, int timeout);
int cache_drop_pages(segment_t *seg, ex_off_t lo, ex_off_t hi);
int cache_release_pages(int n_pages, page_handle_t *page, int rw_mode);
void _cache_drain_writes(segment_t *seg, cache_page_t *p);
void cache_advise(segment_t *seg, segment_rw_hints_t *rw_hints, int rw_mode, ex_off_t lo, ex_off_t hi, page_handle_t *page, int *n_pages, int force_load);

void *free_page_tables_new(void *arg, int size);
void free_page_tables_free(void *arg, int size, void *data);
void *free_pending_table_new(void *arg, int size);
void free_pending_table_free(void *arg, int size, void *data);


#ifdef __cplusplus
}
#endif

#endif


