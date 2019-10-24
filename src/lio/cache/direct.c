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

#include <assert.h>
#include <stdlib.h>
#include <tbx/assert_result.h>
#include <tbx/atomic_counter.h>
#include <tbx/iniparse.h>
#include <tbx/log.h>
#include <tbx/type_malloc.h>

#include "cache.h"
#include "ds.h"
#include "service_manager.h"
#include "direct.h"

typedef struct {
    char *section;
} cache_direct_t;

static cache_direct_t direct_default_options = {
    .section = "cache-direct",
};


//*************************************************************************
// direct_print_running_config - Prints the running config
//*************************************************************************

void direct_print_running_config(lio_cache_t *c, FILE *fd, int print_section_heading)
{
    cache_direct_t *cp = (cache_direct_t *)c->fn.priv;

    if (print_section_heading) fprintf(fd, "[%s]\n", cp->section);
    fprintf(fd, "type = %s\n", CACHE_TYPE_DIRECT);
    fprintf(fd, "\n");
}

//*************************************************************************
// direct_cache_destroy - Destroys the cache structure.
//     NOTE: Data is not flushed!
//*************************************************************************

int direct_cache_destroy(lio_cache_t *c)
{
    cache_direct_t *cp = (cache_direct_t *)c->fn.priv;

    log_printf(15, "Shutting down\n");
    tbx_log_flush();

    cache_base_destroy(c);

    if (cp->section) free(cp->section);
    free(cp);
    free(c);

    return(0);
}

//*************************************************************************
// direct_cache_create - Creates an empty amp cache structure
//*************************************************************************

lio_cache_t *direct_cache_create(void *arg, data_attr_t *da, int timeout)
{

    lio_cache_t *cache;
    cache_direct_t *c;

    tbx_type_malloc_clear(cache, lio_cache_t, 1);
    tbx_type_malloc_clear(c, cache_direct_t, 1);
    cache->type = CACHE_TYPE_DIRECT;
    cache->fn.priv = c;
    if (!cache->fn.priv) {
        log_printf(0,"ERROR: a null priv structure was malloc()d");
    }

    cache_base_create(cache, da, timeout);

    cache->fn.destroy = direct_cache_destroy;
    cache->fn.get_handle = cache_base_handle;
    cache->fn.print_running_config = direct_print_running_config;
    c->section = strdup(direct_default_options.section);

    return(cache);
}


//*************************************************************************
// direct_cache_load - Creates and configures a direct cache structure
//*************************************************************************

lio_cache_t *direct_cache_load(void *arg, tbx_inip_file_t *fd, char *grp, data_attr_t *da, int timeout)
{
    lio_cache_t *c;
    cache_direct_t *cp;

    //** Create the default structure
    c = direct_cache_create(arg, da, timeout);
    cp = (cache_direct_t *)c->fn.priv;

    if (grp != NULL) {
        if (cp->section) free(cp->section);
        cp->section = strdup(grp);
    }

    return(c);
}
