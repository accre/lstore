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
// Routines for managing the the cache framework
//***********************************************************************

#define _log_module_index 142

#include <tbx/list.h>
#include <tbx/type_malloc.h>
#include <tbx/log.h>
#include "cache.h"
#include "ex3_abstract.h"
#include "ex3_compare.h"
#include <tbx/pigeon_coop.h>

//*************************************************************************
//  cache_base_handle  - Simple get_handle method
//*************************************************************************

cache_t *cache_base_handle(cache_t *c)
{
    return(c);
}

//*************************************************************************
// cache_base_destroy - Destroys the base cache elements
//*************************************************************************

void cache_base_destroy(cache_t *c)
{
    tbx_list_destroy(c->segments);
    tbx_pc_destroy(c->cond_coop);
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
    c->segments = tbx_list_create(0, &skiplist_compare_ex_id, NULL, NULL, NULL);
    c->cond_coop = tbx_pc_new("cache_cond_coop", 50, sizeof(cache_cond_t), c->mpool, cache_cond_new, cache_cond_free);
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

    tbx_type_malloc_clear(shelf, page_table_t, size);

    log_printf(15, "making new shelf of size %d\n", size);
    for (i=0; i<size; i++) {
        shelf[i].stack = tbx_stack_new();
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
        tbx_free_stack(shelf[i].stack, 0);
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
    tbx_list_t **shelf;
    int i;

    tbx_type_malloc_clear(shelf, tbx_list_t *, size);

    log_printf(15, "making new shelf of size %d\n", size);
    for (i=0; i<size; i++) {
        shelf[i] = tbx_list_create(0, &skiplist_compare_ex_id, NULL, NULL, NULL);
    }

    log_printf(15, " shelf[0]->max_levels=%d\n", shelf[0]->max_levels);
    tbx_flush_log();
    return((void *)shelf);
}

//*************************************************************
// free_pending_table_free - Destroys a shelf of free tables
//*************************************************************

void free_pending_table_free(void *arg, int size, void *data)
{
    tbx_list_t **shelf = (tbx_list_t **)data;
    int i;

    log_printf(15, "destroying shelf of size %d\n", size);

    for (i=0; i<size; i++) {
        tbx_list_destroy(shelf[i]);
    }

    free(shelf);
    return;
}

