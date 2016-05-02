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

#ifndef __CACHE_LRU_PRIV_H_
#define __CACHE_LRU_PRIV_H_


#ifdef __cplusplus
extern "C" {
#endif

#include "cache.h"

typedef struct {
    cache_page_t page;  //** Actual page
    tbx_stack_ele_t *ele;   //** LRU position
} page_lru_t;

typedef struct {
    tbx_stack_t *stack;
    tbx_stack_t *waiting_stack;
    tbx_stack_t *pending_free_tasks;
    tbx_pc_t *free_pending_tables;
    tbx_pc_t *free_page_tables;
    apr_thread_cond_t *dirty_trigger;
    apr_thread_t *dirty_thread;
    apr_time_t dirty_max_wait;
    ex_off_t max_bytes;
    ex_off_t bytes_used;
    ex_off_t dirty_bytes_trigger;
    double   dirty_fraction;
    int      flush_in_progress;
    int      limbo_pages;
} cache_lru_t;

typedef struct {
    apr_thread_cond_t *cond;
    ex_off_t  bytes_needed;
} lru_page_wait_t;

#ifdef __cplusplus
}
#endif

#endif


