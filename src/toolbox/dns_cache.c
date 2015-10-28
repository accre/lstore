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

//**************************************************************************
//
//  Provides a simple DNS cache
//
//**************************************************************************

#define _log_module_index 115

#include <assert.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <apr_hash.h>
#include <apr_time.h>
#include <apr_network_io.h>

#include "log.h"
#include "fmttypes.h"
#include "dns_cache.h"
#include "string_token.h"

#define BUF_SIZE 128

typedef struct {
    char name[BUF_SIZE];
    unsigned char addr[DNS_ADDR_MAX];
    char ip_addr[256];
    int family;
} DNS_entry_t;

typedef struct {
    apr_pool_t *mpool;
    apr_pool_t *lockpool;
    apr_hash_t *table;
    int size;
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

    { int result = apr_pool_create(&(cache->mpool), NULL); assert(result == APR_SUCCESS); }
    { int result = (cache->table = apr_hash_make(cache->mpool)); assert(result != NULL); }

    cache->restart_time = apr_time_now() + apr_time_make(600, 0);
}

//**************************************************************************
//  lookup_host - Looks up the host.  Make sure that the lock/unlock routines
//      are used to make it threadsafe!
//
//**************************************************************************

int lookup_host(const char *name, char *byte_addr, char *ip_addr)
{
    char ip_buffer[256];
//  char byte_buffer[256];
    char *s, *bstate;
    int err, i;
    DNS_entry_t *h;
    apr_sockaddr_t *sa;
//  int family;

    log_printf(20, "lookup_host: start time=" TT " name=%s\n", apr_time_now(), name);
    if (_cache == NULL) log_printf(20, "lookup_host: _cache == NULL\n");

    if (name[0] == '\0') return(1);  //** Return early if name is NULL

//  ipaddr = (ip_addr == NULL) ? ip_buffer : ip_addr;
//  addr = (byte_addr == NULL) ? byte_buffer : byte_addr;

//log_printf(20, "lookup_host: before lock\n");
    apr_thread_mutex_lock(_cache->lock);
//log_printf(20, "lookup_host: after lock\n");

    if ((apr_time_now() > _cache->restart_time) || (apr_hash_count(_cache->table) > _cache->size)) wipe_entries(_cache);

    h = (DNS_entry_t *)apr_hash_get(_cache->table, name, APR_HASH_KEY_STRING);

    if (h != NULL) {  //** Got a hit!!
        if (ip_addr != NULL) strcpy(ip_addr, h->ip_addr);
        if (byte_addr != NULL) memcpy(byte_addr, h->addr, DNS_ADDR_MAX);
//     family = h->family;
        apr_thread_mutex_unlock(_cache->lock);
        return(0);
    }

    //** If we made it here that means we have to look it up
    err = apr_sockaddr_info_get(&sa, name, APR_INET, 80, 0, _cache->mpool);
//log_printf(20, "lookup_host: apr_sockaddr_info_get=%d\n", err);

    if (err != APR_SUCCESS) {
        apr_thread_mutex_unlock(_cache->lock);
        return(-1);
    }
    h = (DNS_entry_t *)apr_palloc(_cache->mpool, sizeof(DNS_entry_t)); //** This is created withthe pool for easy cleanup
    memset(h, 0, sizeof(DNS_entry_t));

    strncpy(h->name, name, sizeof(h->name));
    h->name[sizeof(h->name)-1] = '\0';

    apr_sockaddr_ip_getbuf(ip_buffer, sizeof(ip_buffer), sa);
    strcpy(h->ip_addr, ip_buffer);

    log_printf(20, "lookup_host: start host=%s address=%s\n", name, ip_buffer);

    h->family = DNS_IPV4;
    i = 0;
    for (s = string_token(ip_buffer, ".", &bstate, &err); err == 0; s = string_token(NULL, ".", &bstate, &err)) {
        h->addr[i] = atoi(s);
//n = h->addr[i];
//log_printf(20, "lookup_host: err=%d i=%d n=%d s=%s\n", err, i, n, s);

        i++;
    }
    if (i>4) h->family = DNS_IPV6;

    //** Add the enry to the table
    apr_hash_set(_cache->table, h->name, APR_HASH_KEY_STRING, h);

    //** Return the address
    if (ip_addr != NULL) strcpy(ip_addr, h->ip_addr);
    if (byte_addr != NULL) memcpy(byte_addr, h->addr, DNS_ADDR_MAX);
//  family = h->family;

    apr_thread_mutex_unlock(_cache->lock);

    return(0);
}

//**************************************************************************

void dns_cache_init(int size)
{
    log_printf(20, "dns_cache_init: Start!!!!!!!!!!!!\n");

    if (_cache != NULL) return;

    _cache = (DNS_cache_t *)malloc(sizeof(DNS_cache_t));
    assert(_cache != NULL);

    _cache->size = size;
    _cache->mpool = NULL;
    { int result = apr_pool_create(&(_cache->lockpool), NULL); assert(result == APR_SUCCESS); }
    apr_thread_mutex_create(&(_cache->lock), APR_THREAD_MUTEX_DEFAULT,_cache->lockpool);

    wipe_entries(_cache);
}

//**************************************************************************

void finalize_dns_cache()
{
    apr_thread_mutex_destroy(_cache->lock);

    if (_cache->mpool != NULL) apr_pool_destroy(_cache->mpool);
    if (_cache->lockpool != NULL) apr_pool_destroy(_cache->lockpool);

    free(_cache);

    _cache = NULL;
}


