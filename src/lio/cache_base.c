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
// Routines for managing the the cache framework
//***********************************************************************

#define _log_module_index 142

#include "list.h"
#include "type_malloc.h"
#include "log.h"
#include "cache.h"
#include "ex3_abstract.h"
#include "ex3_compare.h"
#include "pigeon_coop.h"

typedef struct {
  cache_t *(*driver)(void *arg, data_attr_t *da, int timeout, char *fname);
  cache_t *(*create)(void *arg, data_attr_t *da, int timeout);
  void *arg;
} cache_driver_t;

typedef struct {
  list_t *table;
} cache_table_t;

cache_table_t *cache_driver_table = NULL;

//***********************************************************************
// install_cache - Installs a cache driver into the table
//***********************************************************************

int install_cache(char *type, cache_t *(*driver)(void *arg, data_attr_t *da, int timeout, char *fname), cache_t *(*create)(void *arg, data_attr_t *da, int timeout), void *arg)
{
  cache_driver_t *d;

  //** 1st time so create the struct
  if (cache_driver_table == NULL) {
     type_malloc_clear(cache_driver_table, cache_table_t, 1);
     cache_driver_table->table = list_create(0, &list_string_compare, list_string_dup, list_simple_free, list_no_data_free);
  }

  d = list_search(cache_driver_table->table, type);
  if (d != NULL) {
    log_printf(0, "install_cache: Matching driver for type=%s already exists!\n", type);
    return(1);
  }
  
  type_malloc_clear(d, cache_driver_t, 1);
  d->driver = driver;
  d->create = create;
  d->arg = arg;
  list_insert(cache_driver_table->table, type, (void *)d);

  return(0);
}

//***********************************************************************
// load_cache - Loads the given cache type from the file
//***********************************************************************

cache_t *load_cache(char *ctype, data_attr_t *da, int timeout, char *fname)
{
  cache_driver_t *d;

  d = list_search(cache_driver_table->table, ctype);
  if (d == NULL) {
    log_printf(0, "load_cache:  No matching driver for type=%s\n", ctype);
    return(NULL);
  }

  return(d->driver(d->arg, da, timeout, fname));
}

//***********************************************************************
// create_cache - Creates a cache of the given type
//***********************************************************************

cache_t *create_cache(char *type, data_attr_t *da, int timeout)
{
  cache_driver_t *d;

  d = list_search(cache_driver_table->table, type);
  if (d == NULL) {
    log_printf(0, "load_cache:  No matching driver for type=%s\n", type);
    return(NULL);
  }

  return(d->create(d->arg, da, timeout));
}

//*************************************************************************
// cache_system_init - Loads the caceh drivers
//*************************************************************************

void cache_system_init()
{
  install_cache(CACHE_TYPE_LRU, lru_cache_load, lru_cache_create, NULL);
  install_cache(CACHE_TYPE_AMP, amp_cache_load, amp_cache_create, NULL);
}

//*************************************************************************
// cache_system_destroy - Loads the caceh drivers
//*************************************************************************

void cache_system_destroy()
{
}

//*************************************************************************
// get_cache_stats - Returns the cumulative cache stats
//*************************************************************************

cache_stats_t DUMMY_get_cache_stats(cache_t *c)
{
  Stack_t stack;
  list_iter_t it;
  ex_id_t id;
  segment_t *seg;
  cache_segment_t *s;
  cache_stats_t cs;

  init_stack(&stack);

  //** Generate the list
  cache_lock(c);
  it = list_iter_search(c->segments, NULL, 0);
  list_next(&it, (list_key_t *)&id, (list_data_t *)&seg);
  while (seg != NULL) {
    push(&stack, seg);
    list_next(&it, (list_key_t *)&id, (list_data_t *)&seg);
  }
  cs = c->stats;
  cache_unlock(c);

  //** Now sum everything up
  seg = (segment_t *)pop(&stack);
  while (seg != NULL) {
    segment_lock(seg);
    s = (cache_segment_t *)seg->priv;

    cs.user.read_count += s->stats.user.read_count;
    cs.user.write_count += s->stats.user.write_count;
    cs.user.read_bytes += s->stats.user.read_bytes;
    cs.user.write_bytes += s->stats.user.write_bytes;
    cs.system.read_count += s->stats.system.read_count;
    cs.system.write_count += s->stats.system.write_count;
    cs.system.read_bytes += s->stats.system.read_bytes;
    cs.system.write_bytes += s->stats.system.write_bytes;
    segment_unlock(seg);

    seg = (segment_t *)pop(&stack);    
  }
  
  return(cs);
}

//*************************************************************************
// cache_base_destroy - Destroys the base cache elements
//*************************************************************************

void cache_base_destroy(cache_t *c)
{
  list_destroy(c->segments);
  destroy_pigeon_coop(c->cond_coop);
  apr_thread_mutex_destroy(c->lock);
  apr_pool_destroy(c->mpool);
}


//*************************************************************************
// cache_base_create - Creates the base cache elements
//*************************************************************************

void cache_base_create(cache_t *c, data_attr_t *da, int timeout)
{
  apr_pool_create(&(c->mpool), NULL);
  apr_thread_mutex_create(&(c->lock), APR_THREAD_MUTEX_DEFAULT, c->mpool);
  c->segments = list_create(0, &skiplist_compare_ex_id, NULL, NULL, NULL);
  c->cond_coop = new_pigeon_coop("cache_cond_coop", 50, sizeof(cache_cond_t), c->mpool, cache_cond_new, cache_cond_free);
  c->da = da;
  c->timeout = timeout;
  c->default_page_size = 16*1024;
}

//*************************************************************
// free_page_tables_new - Creates a new shelf of segment page tables used
//    when freeing pages
//*************************************************************

void *free_page_tables_new(void *arg, int size)
{
  page_table_t *shelf;
  int i;

  type_malloc_clear(shelf, page_table_t, size);

log_printf(15, "making new shelf of size %d\n", size);
  for (i=0; i<size; i++) {
      shelf[i].stack = new_stack();
  }

  return((void *)shelf);
}

//*************************************************************
// free_page_tables_free - Destroys a shelf of free tables
//*************************************************************

void free_page_tables_free(void *arg, int size, void *data)
{
  page_table_t *shelf = (page_table_t *)data;
  int i;

log_printf(15, "destroying shelf of size %d\n", size);

  for (i=0; i<size; i++) {
    free_stack(shelf[i].stack, 0);
  }

  free(shelf);
  return;
}

//*************************************************************
// free_pending_table_new - Creates a new shelf of segment tables used
//    when freeing pages
//*************************************************************

void *free_pending_table_new(void *arg, int size)
{
  list_t **shelf;
  int i;

  type_malloc_clear(shelf, list_t *, size);

log_printf(15, "making new shelf of size %d\n", size);
  for (i=0; i<size; i++) {
    shelf[i] = list_create(0, &skiplist_compare_ex_id, NULL, NULL, NULL);
  }

log_printf(15, " shelf[0]->max_levels=%d\n", shelf[0]->max_levels); flush_log();
  return((void *)shelf);
}

//*************************************************************
// free_pending_table_free - Destroys a shelf of free tables
//*************************************************************

void free_pending_table_free(void *arg, int size, void *data)
{
  list_t **shelf = (list_t **)data;
  int i;

log_printf(15, "destroying shelf of size %d\n", size);

  for (i=0; i<size; i++) {
      list_destroy(shelf[i]);
  }

  free(shelf);
  return;
}

