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

//**************************************************************************
//
//  Provides a simple DNS cache
//
//**************************************************************************

#define _log_module_index 115

#include <apr_errno.h>
#include <apr_hash.h>
#include <apr_network_io.h>
#include <apr_pools.h>
#include <apr_thread_mutex.h>
#include <apr_time.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "tbx/assert_result.h"
#include "tbx/dns_cache.h"
#include "tbx/fmttypes.h"
#include "tbx/log.h"
#include "tbx/string_token.h"

#define BUF_SIZE 128

typedef struct {
    char name[BUF_SIZE];
    unsigned char addr[DNS_ADDR_MAX];
    char ip_addr[256];
    tbx_dns_ip_t family;
} DNS_entry_t;

typedef struct {
    apr_pool_t *mpool;
    apr_pool_t *lockpool;
    apr_hash_t *table;
    unsigned int size;
    apr_time_t restart_time;
    apr_thread_mutex_t *lock;
} DNS_cache_t;

DNS_cache_t *_cache = NULL;


//**************************************************************************
//  wipe_entries - Wipes the existing entries and resets the timer
//**************************************************************************

void wipe_entries(DNS_cache_t *cache)
{
    if (cache->mpool != NULL) apr_pool_destroy(cache->mpool);

    assert_result(apr_pool_create(&(cache->mpool), NULL), APR_SUCCESS);
    cache->table = apr_hash_make(cache->mpool);FATAL_UNLESS(cache->table != NULL);

    cache->restart_time = apr_time_now() + apr_time_make(600, 0);
}

//**************************************************************************
// hostname2ip - Return the host's IP address
//**************************************************************************

int hostname2ip(const char *name, char *ip_bytes, char *ip_text, int ip_text_size)
{
    struct addrinfo hints;
    struct addrinfo *result;
    int err;

    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_CANONNAME;

    if ((err = getaddrinfo(name, NULL, &hints, &result)) != 0) {
        fprintf(stderr, "getaddrinfo: host:%s error:%s\n", name, gai_strerror(err));
        return(-1);
    }
    if (ip_bytes) memcpy(ip_bytes, &((struct sockaddr_in *)result->ai_addr)->sin_addr, 4);
    if (ip_text) inet_ntop(AF_INET, &((struct sockaddr_in *)result->ai_addr)->sin_addr, ip_text, ip_text_size);

    freeaddrinfo(result);
    return(0);
}


//**************************************************************************
//  lookup_host - Looks up the host.  Make sure that the lock/unlock routines
//      are used to make it threadsafe!
//
//**************************************************************************

int tbx_dnsc_lookup(const char *name, char *byte_addr, char *ip_addr)
{
    char ip_buffer[256];
    int err;
    DNS_entry_t *h;
    struct in_addr sa;

    log_printf(20, "lookup_host: start time=" TT " name=%s\n", apr_time_now(), name);
    if (_cache == NULL) log_printf(20, "lookup_host: _cache == NULL\n");

    if (name[0] == '\0') return(1);  //** Return early if name is NULL

    apr_thread_mutex_lock(_cache->lock);

    if ((apr_time_now() > _cache->restart_time) || (apr_hash_count(_cache->table) > _cache->size)) wipe_entries(_cache);

    h = (DNS_entry_t *)apr_hash_get(_cache->table, name, APR_HASH_KEY_STRING);

    if (h != NULL) {  //** Got a hit!!
        if (ip_addr != NULL)
            strcpy(ip_addr, h->ip_addr);
        if (byte_addr != NULL)
            memcpy(byte_addr, h->addr, DNS_ADDR_MAX);
        apr_thread_mutex_unlock(_cache->lock);
        return(0);
    }

    //** If we made it here that means we have to look it up
    err = hostname2ip(name, (char *)&sa, ip_buffer, sizeof(ip_buffer));
    if (err != 0) {
        apr_thread_mutex_unlock(_cache->lock);
        return(-1);
    }
    h = (DNS_entry_t *)apr_palloc(_cache->mpool, sizeof(DNS_entry_t)); //** This is created withthe pool for easy cleanup
    memset(h, 0, sizeof(DNS_entry_t));

    strncpy(h->name, name, sizeof(h->name));
    h->name[sizeof(h->name)-1] = '\0';
    h->family = DNS_IPV4;
    memcpy(h->addr, &sa, sizeof(sa));
    strcpy(h->ip_addr, ip_buffer);

    log_printf(20, "lookup_host: end host=%s address=%s\n", name, ip_buffer);

    //** Add the enry to the table
    apr_hash_set(_cache->table, h->name, APR_HASH_KEY_STRING, h);

    //** Return the address
    if (ip_addr != NULL) strcpy(ip_addr, h->ip_addr);
    if (byte_addr != NULL) memcpy(byte_addr, h->addr, sizeof(sa));

    apr_thread_mutex_unlock(_cache->lock);

    return(0);
}

//**************************************************************************

int tbx_dnsc_startup()
{
    return tbx_dnsc_startup_sized(100);
}

//**************************************************************************

int tbx_dnsc_startup_sized(int size)
{
    if (_cache != NULL) return 0;

    _cache = (DNS_cache_t *)malloc(sizeof(DNS_cache_t));
    FATAL_UNLESS(_cache != NULL);

    _cache->size = size;
    _cache->mpool = NULL;
    assert_result(apr_pool_create(&(_cache->lockpool), NULL), APR_SUCCESS);
    apr_thread_mutex_create(&(_cache->lock), APR_THREAD_MUTEX_DEFAULT,_cache->lockpool);

    wipe_entries(_cache);
    return 0;
}

//**************************************************************************

int tbx_dnsc_shutdown()
{
    apr_thread_mutex_destroy(_cache->lock);

    if (_cache->mpool != NULL) apr_pool_destroy(_cache->mpool);
    if (_cache->lockpool != NULL) apr_pool_destroy(_cache->lockpool);

    free(_cache);

    _cache = NULL;
    return 0;
}


