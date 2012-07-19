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

#ifndef __CACHE_AMP_PRIV_H_
#define __CACHE_AMP_PRIV_H_


#ifdef __cplusplus
extern "C" {
#endif

#include "cache.h"

#define CAMP_ACCESSED 1  //** Page has been accessed
#define CAMP_TAG      2  //** Tag page for pretech
#define CAMP_OLD      4  //** Page has been recycled without a hit

typedef struct {
  cache_page_t page;  //** Actual page
  Stack_ele_t *ele;   //** LRU position
  ex_off_t stream_offset;
  int bit_fields;
} page_amp_t;

typedef struct {
  ex_off_t last_offset;
  ex_off_t nbytes;
  int prefetch_size;
  int trigger_distance;
} amp_page_stream_t;

typedef struct {
  int   max_streams;
  amp_page_stream_t *stream_table;
  list_t *streams;
  int index;
  int start_apt_pages;
} amp_stream_table_t;

typedef struct {
  Stack_t *stack;
  Stack_t *waiting_stack;
  Stack_t *pending_free_tasks;
  pigeon_coop_t *free_pending_tables;
  pigeon_coop_t *free_page_tables;
  apr_thread_cond_t *dirty_trigger;
  apr_thread_t *dirty_thread;
  apr_time_t dirty_max_wait;
  ex_off_t max_bytes;
  ex_off_t bytes_used;
  ex_off_t dirty_bytes_trigger;
  ex_off_t prefetch_in_process;
  ex_off_t async_prefetch_threshold;
  ex_off_t min_prefetch_size;
  double   dirty_fraction;
  int      max_streams;
  int      flush_in_progress;
  int      limbo_pages;
} cache_amp_t;

typedef struct {
  apr_thread_cond_t *cond;
  ex_off_t  bytes_needed;
} amp_page_wait_t;

#ifdef __cplusplus
}
#endif

#endif


