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

#ifndef __CACHE_H_
#define __CACHE_H_

#include <lio/cache.h>
#include <tbx/atomic_counter.h>
#include <tbx/list.h>
#include <tbx/pigeon_coop.h>

#include "ex3.h"

#ifdef __cplusplus
extern "C" {
#endif

#define CACHE_PRINT
#define CACHE_PRINT_LOCK
#define CACHE_LOAD_AVAILABLE "cache_load_available"
#define CACHE_CREATE_AVAILABLE "cache_create_available"


void print_lio_cache_table(int dolock);
typedef lio_cache_t *(cache_load_t)(void *arg, tbx_inip_file_t *ifd, char *section, data_attr_t *da, int timeout);
typedef lio_cache_t *(cache_create_t)(void *arg, data_attr_t *da, int timeout);

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

struct lio_cache_range_t {
    ex_off_t lo;
    ex_off_t hi;
    ex_off_t boff;
    int iov_index;
};

struct lio_cache_counters_t {
    ex_off_t read_count;
    ex_off_t write_count;
    ex_off_t read_bytes;
    ex_off_t write_bytes;
};

struct lio_cache_stats_get_t {
    lio_cache_counters_t user;
    lio_cache_counters_t system;
    ex_off_t dirty_bytes;
    ex_off_t hit_bytes;
    ex_off_t miss_bytes;
    ex_off_t unused_bytes;
    apr_time_t hit_time;
    apr_time_t miss_time;
};

struct lio_cache_cond_t {
    apr_thread_cond_t *cond;
    int count;
};

#define CPP_BEGIN 1
#define CPP_END   2
#define CPP_FULL  4

struct lio_cache_partial_page_t {
    ex_off_t page_start;
    ex_off_t page_end;
    tbx_stack_t *range_stack;
    char *data;
    int flags;
};

struct lio_cache_lio_segment_t {
    lio_cache_t *c;
    void *cache_priv;
    lio_segment_t *child_seg;
    gop_thread_pool_context_t *tpc_unlimited;
    tbx_list_t *pages;
    tbx_list_t *partial_pages;
    apr_thread_mutex_t *lock;
    apr_thread_cond_t  *flush_cond;
    apr_thread_cond_t  *ppages_cond;
    tbx_stack_t *flush_stack;
    tbx_stack_t *ppages_unused;
    char *qname;
    lio_cache_partial_page_t *ppage;
    char *ppages_buffer;
    int cache_check_in_progress;
    int flushing_count;
    int n_ppages;
    int ppages_used;
    int ppages_flushing;
    ex_off_t ppage_max;
    ex_off_t page_size;
    ex_off_t child_last_page;
    ex_off_t total_size;
    lio_cache_stats_get_t stats;
};

struct lio_data_page_t {
    char *ptr;
    int  usage_count;
};

struct lio_cache_page_t {
    lio_segment_t *seg;
    lio_data_page_t *curr_data;
    lio_data_page_t data[2];
    void *priv;
    ex_off_t offset;
    tbx_pch_t cond_pch;
    int  bit_fields;
    int access_pending[3];
    int used_count;
    int current_index;
};

struct lio_page_handle_t {
    lio_cache_page_t *p;
    lio_data_page_t *data;
};

struct lio_page_table_t {
    tbx_stack_t *stack;
    lio_segment_t *seg;
    ex_id_t   id;
    tbx_pch_t pch;
    ex_off_t lo, hi;
};

struct lio_cache_fn_t {
    void *priv;
    void (*adding_segment)(lio_cache_t *c, lio_segment_t *seg);
    void (*removing_segment)(lio_cache_t *c, lio_segment_t *seg);
    lio_cache_page_t *(*create_empty_page)(lio_cache_t *c, lio_segment_t *seg, int doblock);
    void (*adjust_dirty)(lio_cache_t *c, ex_off_t tweak);
    void (*destroy_pages)(lio_cache_t *c, lio_cache_page_t **p, int n_pages, int remove_from_segment);
    void (*cache_update)(lio_cache_t *c, lio_segment_t *seg, int rw_mode, ex_off_t lo, ex_off_t hi, void *miss);
    void (*cache_miss_tag)(lio_cache_t *c, lio_segment_t *seg, int rw_mode, ex_off_t lo, ex_off_t hi, ex_off_t missing_offset, void **miss);
    int (*s_page_access)(lio_cache_t *c, lio_cache_page_t *p, int rw_mode, ex_off_t request_len);
    int (*s_pages_release)(lio_cache_t *c, lio_cache_page_t **p, int n_pages);
    lio_cache_t *(*get_handle)(lio_cache_t *);
    int (*destroy)(lio_cache_t *c);
};

struct lio_cache_t {
    lio_cache_fn_t fn;
    apr_pool_t *mpool;
    apr_thread_mutex_t *lock;
    tbx_list_t *segments;
    tbx_pc_t *cond_coop;
    data_attr_t *da;
    ex_off_t default_page_size;
    lio_cache_stats_get_t stats;
    ex_off_t max_fetch_size;
    ex_off_t write_temp_overflow_size;
    ex_off_t write_temp_overflow_used;
    ex_off_t min_direct;
    double   max_fetch_fraction;
    double   write_temp_overflow_fraction;
    int n_ppages;
    int timeout;
    int  shutdown_request;
};

extern tbx_atomic_int_t _cache_count;

#define unique_cache_id() tbx_atomic_inc(_cache_count);
#define cache_lock(c) apr_thread_mutex_lock((c)->lock)
#define cache_unlock(c) apr_thread_mutex_unlock((c)->lock)
#define cache_get_handle(c) (c)->fn.get_handle(c)
#define cache_destroy(c) (c)->fn.destroy(c)

lio_cache_t *cache_base_handle(lio_cache_t *);
void cache_base_destroy(lio_cache_t *c);
void cache_base_create(lio_cache_t *c, data_attr_t *da, int timeout);
void *cache_cond_new(void *arg, int size);
void cache_cond_free(void *arg, int size, void *data);
gop_op_generic_t *cache_flush_range_gop(lio_segment_t *seg, data_attr_t *da, ex_off_t lo, ex_off_t hi, int timeout);
int cache_release_pages(int n_pages, lio_page_handle_t *page, int rw_mode);
void _cache_drain_writes(lio_segment_t *seg, lio_cache_page_t *p);
void cache_advise(lio_segment_t *seg, lio_segment_rw_hints_t *rw_hints, int rw_mode, ex_off_t lo, ex_off_t hi, lio_page_handle_t *page, int *n_pages, int force_load);

void *free_page_tables_new(void *arg, int size);
void free_page_tables_free(void *arg, int size, void *data);
void *free_pending_table_new(void *arg, int size);
void free_pending_table_free(void *arg, int size, void *data);



#ifdef __cplusplus
}
#endif

#endif
