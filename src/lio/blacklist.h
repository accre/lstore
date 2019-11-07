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
// Blacklist structure definition
//***********************************************************************

#ifndef _BLACKLIST_H_
#define _BLACKLIST_H_

#include <apr_hash.h>
#include <apr_pools.h>
#include <apr_thread_mutex.h>
#include <apr_time.h>
#include <lio/blacklist.h>
#include <tbx/iniparse.h>

#include "ex3/types.h"

#ifdef __cplusplus
extern "C" {
#endif

struct lio_blacklist_ibp_rid_t {
    char *rid;                   //** RID blacklisted
    int  rs_added;               //** RID was blacklisted by the Resource Service
    apr_time_t recheck_time;     //** Recheck time
};

struct lio_blacklist_t {
    apr_pool_t *mpool;
    apr_thread_mutex_t *lock;
    apr_hash_t *table;
    ex_off_t  min_bandwidth;
    apr_time_t min_io_time;
    apr_time_t timeout;
};

void blacklist_remove_rs_added(lio_blacklist_t *bl);
void blacklist_add(lio_blacklist_t *bl, char *rid_key, int rs_added, int do_lock);
int blacklist_check(lio_blacklist_t *bl, char *rid_key, int do_lock);
void blacklist_destroy(lio_blacklist_t *bl);
lio_blacklist_t *blacklist_load(tbx_inip_file_t *ifd, char *section);

#ifdef __cplusplus
}
#endif

#endif
