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
// Routines for managing RID blacklisting
//***********************************************************************

#define _log_module_index 200

#include <stdlib.h>
#include <tbx/type_malloc.h>

#include "blacklist.h"


//***************************************************************
// blacklist_remove_rs_added - Removes all the RIDs blakclisted due to the RS
//***************************************************************

void blacklist_remove_rs_added(lio_blacklist_t *bl)
{
    apr_ssize_t hlen;
    apr_hash_index_t *hi;
    lio_blacklist_ibp_rid_t *r;

    apr_thread_mutex_lock(bl->lock);

    //** Destroy all the blacklist RIDs
    for (hi=apr_hash_first(NULL, bl->table); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, &hlen, (void **)&r);
        if (r->rs_added > 0) {
            apr_hash_set(bl->table, r->rid, APR_HASH_KEY_STRING, NULL);
            free(r->rid);
            free(r);
        }
    }

    apr_thread_mutex_unlock(bl->lock);
}

//***************************************************************
// blacklist_add - Adds a blacklist RID
//***************************************************************

void blacklist_add(lio_blacklist_t *bl, char *rid_key, int rs_added, int do_lock)
{
    lio_blacklist_ibp_rid_t *bl_rid;

    if (do_lock) apr_thread_mutex_lock(bl->lock);
    bl_rid = apr_hash_get(bl->table, rid_key, APR_HASH_KEY_STRING);
    if (bl_rid == NULL) {
        log_printf(2, "Blacklisting RID=%s\n", rid_key);
        tbx_type_malloc(bl_rid, lio_blacklist_ibp_rid_t, 1);
        bl_rid->rid = strdup(rid_key);
        bl_rid->recheck_time = apr_time_now() + ((rs_added == 0) ? bl->timeout : apr_time_from_sec(7200));
        bl_rid->rs_added = rs_added;
        apr_hash_set(bl->table, bl_rid->rid, APR_HASH_KEY_STRING, bl_rid);
    } else if (rs_added == 1) {  //** If needed flag it as being added by the RS
        bl_rid->recheck_time = apr_time_now() + apr_time_from_sec(7200);
        bl_rid->rs_added = rs_added;
    }
    if (do_lock) apr_thread_mutex_unlock(bl->lock);
}

//***************************************************************
// blacklist_check - Checksto see if the RID is blacklisted
//    Returns 1 if blacklisted and 0 otherwise
//***************************************************************

int blacklist_check(lio_blacklist_t *bl, char *rid_key, int do_lock)
{
    lio_blacklist_ibp_rid_t *bl_rid;
    apr_time_t now;

    now = apr_time_now();

    if (do_lock) apr_thread_mutex_lock(bl->lock);
    bl_rid = apr_hash_get(bl->table, rid_key, APR_HASH_KEY_STRING);
    if (bl_rid) {
        if (bl_rid->recheck_time < now) { //** Expired blacklist so undo it
            log_printf(5, "EXPIRED rid=%s\n", bl_rid->rid);
            apr_hash_set(bl->table, bl_rid->rid, APR_HASH_KEY_STRING, NULL);
            free(bl_rid->rid);
            free(bl_rid);
            bl_rid = NULL;  //** No blacklisting
        }
    }
    if (do_lock) apr_thread_mutex_unlock(bl->lock);

    return((bl_rid == NULL) ? 0 : 1);
}

//***************************************************************
// blacklist_destroy - Destroys a blacklist structure
//***************************************************************

void blacktbx_list_destroy(lio_blacklist_t *bl)
{
    apr_ssize_t hlen;
    apr_hash_index_t *hi;
    lio_blacklist_ibp_rid_t *r;

    //** Destroy all the blacklist RIDs
    for (hi=apr_hash_first(NULL, bl->table); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, &hlen, (void **)&r);
        free(r->rid);
        free(r);
    }

    apr_pool_destroy(bl->mpool);
    apr_thread_mutex_destroy(bl->lock);
    free(bl);
}

//***************************************************************
// blacklist_load - Loads and creates a blacklist structure
//***************************************************************

lio_blacklist_t *blacklist_load(tbx_inip_file_t *ifd, char *section)
{
    lio_blacklist_t *bl;

    tbx_type_malloc_clear(bl, lio_blacklist_t, 1);

    assert_result(apr_pool_create(&(bl->mpool), NULL), APR_SUCCESS);
    apr_thread_mutex_create(&(bl->lock), APR_THREAD_MUTEX_DEFAULT, bl->mpool);
    bl->table = apr_hash_make(bl->mpool);

    bl->timeout = tbx_inip_get_integer(ifd, section, "timeout", apr_time_from_sec(120));
    bl->min_bandwidth = tbx_inip_get_integer(ifd, section, "min_bandwidth", 5*1024*1024);  //** default ro 5MB
    bl->min_io_time = tbx_inip_get_integer(ifd, section, "min_io_time", apr_time_from_sec(1));  //** default ro 5MB

    return(bl);
}

