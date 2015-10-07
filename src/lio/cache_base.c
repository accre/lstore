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

    log_printf(15, " shelf[0]->max_levels=%d\n", shelf[0]->max_levels);
    flush_log();
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

