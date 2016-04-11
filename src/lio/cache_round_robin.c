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

#define _log_module_index 220

#include "cache.h"
#include "type_malloc.h"
#include "log.h"
#include "ex3_compare.h"
#include "thread_pool.h"

typedef struct {
    int n_cache;
    cache_t **child;
    atomic_int_t count;
} cache_rr_t;

//*************************************************************************
// rr_get_handle - Does a round robin handleing of the underlying cache structures
//*************************************************************************

cache_t *rr_get_handle(cache_t *c)
{
    cache_rr_t *cp = (cache_rr_t *)c->fn.priv;
    int slot = atomic_inc(cp->count) % cp->n_cache;

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
    flush_log();

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

    type_malloc_clear(cache, cache_t, 1);
    type_malloc_clear(c, cache_rr_t, 1);
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

cache_t *round_robin_cache_load(void *arg, inip_file_t *fd, char *grp, data_attr_t *da, int timeout)
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
    cp->n_cache = inip_get_integer(fd, grp, "n_cache", 2);
    child_section = inip_get_string(fd, grp, "child", "cache-amp");
    ctype = inip_get_string(fd, child_section, "type", NULL);

    type_malloc(cp->child, cache_t *, cp->n_cache);
    for (i=0; i<cp->n_cache; i++) {
        cache_create = lookup_service(arg, CACHE_LOAD_AVAILABLE, ctype); assert(cache_create != NULL);
         cp->child[i] = (*cache_create)(arg, fd, child_section, da, timeout); assert(cp->child[i] != NULL);
    }

    if (child_section) free(child_section);
    if (ctype) free(ctype);

    cache_unlock(c);

    return(c);
}
