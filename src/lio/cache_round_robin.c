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

#define _log_module_index 220

#include "cache.h"
#include <tbx/type_malloc.h>
#include <tbx/log.h>
#include "ex3_compare.h"
#include <gop/thread_pool.h>

typedef struct {
    int n_cache;
    cache_t **child;
    tbx_atomic_unit32_t count;
} cache_rr_t;

//*************************************************************************
// rr_get_handle - Does a round robin handleing of the underlying cache structures
//*************************************************************************

cache_t *rr_get_handle(cache_t *c)
{
    cache_rr_t *cp = (cache_rr_t *)c->fn.priv;
    int slot = tbx_atomic_inc(cp->count) % cp->n_cache;

    log_printf(1, "n_cache=%d slot=%d\n", cp->n_cache, slot);
    return(cp->child[slot]);
}

//*************************************************************************
// rr_cache_destroy - Destroys the cache structure.
//     NOTE: Data is not flushed!
//*************************************************************************

int rr_cache_destroy(cache_t *c)
{
    int i;

    cache_rr_t *cp = (cache_rr_t *)c->fn.priv;

    log_printf(15, "Shutting down\n");
    tbx_log_flush();

    for (i=0; i<cp->n_cache; i++) {
        cache_destroy(cp->child[i]);
    }

    cache_base_destroy(c);

    if (cp->child) free(cp->child);
    free(cp);
    free(c);

    return(0);
}

//*************************************************************************
// round_robin_cache_create - Creates an empty amp cache structure
//*************************************************************************

cache_t *round_robin_cache_create(void *arg, data_attr_t *da, int timeout)
{

    cache_t *cache;
    cache_rr_t *c;

    tbx_type_malloc_clear(cache, cache_t, 1);
    tbx_type_malloc_clear(c, cache_rr_t, 1);
    cache->fn.priv = c;
    if (!cache->fn.priv) {
        log_printf(0,"ERROR: a null priv structure was malloc()d");
    }

    cache_base_create(cache, da, timeout);

    cache->fn.destroy = rr_cache_destroy;
    cache->fn.get_handle = rr_get_handle;

    return(cache);
}


//*************************************************************************
// round_robin_cache_load -Creates and configures an amp cache structure
//*************************************************************************

cache_t *round_robin_cache_load(void *arg, tbx_inip_file_t *fd, char *grp, data_attr_t *da, int timeout)
{
    cache_t *c;
    cache_rr_t *cp;
    cache_load_t *cache_create;
    char *child_section, *ctype;
    int i;

    if (grp == NULL) grp = "cache-round-robin";

    //** Create the default structure
    c = round_robin_cache_create(arg, da, timeout);
    cp = (cache_rr_t *)c->fn.priv;

    cache_lock(c);
    cp->n_cache = tbx_inip_get_integer(fd, grp, "n_cache", 2);
    child_section = tbx_inip_get_string(fd, grp, "child", "cache-amp");
    ctype = tbx_inip_get_string(fd, child_section, "type", NULL);

    tbx_type_malloc(cp->child, cache_t *, cp->n_cache);
    for (i=0; i<cp->n_cache; i++) {
        cache_create = lio_lookup_service(arg, CACHE_LOAD_AVAILABLE, ctype); assert(cache_create != NULL);
         cp->child[i] = (*cache_create)(arg, fd, child_section, da, timeout); assert(cp->child[i] != NULL);
    }

    if (child_section) free(child_section);
    if (ctype) free(ctype);

    cache_unlock(c);

    return(c);
}
