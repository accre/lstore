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
// Routines for managing the automatic data service framework
//***********************************************************************

#define _log_module_index 191

#include <apr_strings.h>
#include "service_manager.h"
#include "type_malloc.h"
#include "log.h"

//***********************************************************************
//  remove_service - Removes a service
//***********************************************************************

int remove_service(service_manager_t *sm, char *service_section, char *service_name)
{
    apr_hash_t *section;

    apr_thread_mutex_lock(sm->lock);

    section = apr_hash_get(sm->table, service_section, APR_HASH_KEY_STRING);
    if (section == NULL) goto finished;

    apr_hash_set(section, service_name, APR_HASH_KEY_STRING, NULL);

finished:
    apr_thread_mutex_unlock(sm->lock);

    return(0);
}


//***********************************************************************
//  add_service - Adds a service to the appropriate list
//***********************************************************************

int add_service(service_manager_t *sm, char *service_section, char *service_name, void *service)
{
    char *key;
    apr_hash_t *section;

    apr_thread_mutex_lock(sm->lock);

    log_printf(15, "adding section=%s service=%s\n", service_section, service_name);

    section = apr_hash_get(sm->table, service_section, APR_HASH_KEY_STRING);
    if (section == NULL) {  //** New section so create the table and insert it
        log_printf(15, "Creating section=%s\n", service_section);
        section = apr_hash_make(sm->pool);
        key = apr_pstrdup(sm->pool, service_section);
        apr_hash_set(sm->table, key, APR_HASH_KEY_STRING, section);
    }

    key = apr_pstrdup(sm->pool, service_name);
    apr_hash_set(section, key, APR_HASH_KEY_STRING, service);

    apr_thread_mutex_unlock(sm->lock);

    return(0);
}

//***********************************************************************
// lookup_service - Returns the currrent object associated with the service
//***********************************************************************

void *lookup_service(service_manager_t *sm, char *service_section, char *service_name)
{
    void *s;
    apr_hash_t *section;

    apr_thread_mutex_lock(sm->lock);

    section = apr_hash_get(sm->table, service_section, APR_HASH_KEY_STRING);
    if (section == NULL) {  //** New section so create the table and insert it
        log_printf(10, "No matching section for section=%s name=%s\n", service_section, service_name);
        apr_thread_mutex_unlock(sm->lock);
        return(NULL);
    }

    s = apr_hash_get(section, service_name, APR_HASH_KEY_STRING);
    if (s == NULL) {
        log_printf(10, "No matching object for section=%s name=%s\n", service_section, service_name);
    }
    apr_thread_mutex_unlock(sm->lock);

    return(s);
}

//***********************************************************************
// clone_service_manager - Clones an existing SM
//***********************************************************************

service_manager_t *clone_service_manager(service_manager_t *sm)
{
    apr_ssize_t klen;
    service_manager_t *clone;
    apr_hash_index_t *his;
    apr_hash_t *section, *clone_section;
    char *key;

    //** Make an empty SM
    clone = create_service_manager(sm);

    //** Now cycle through all the tables and copy them
    apr_thread_mutex_lock(sm->lock);
    for (his = apr_hash_first(NULL, sm->table); his != NULL; his = apr_hash_next(his)) {
        apr_hash_this(his, (const void **)&key, &klen, (void **)&section);
        clone_section = apr_hash_copy(clone->pool, section);
        apr_hash_set(clone->table, apr_pstrdup(clone->pool, key), APR_HASH_KEY_STRING, clone_section);
    }
    apr_thread_mutex_unlock(sm->lock);

    return(clone);
}

//***********************************************************************
//  destroy_service_manager - Destroys an existing SM.
//***********************************************************************

void destroy_service_manager(service_manager_t *sm)
{
    apr_pool_destroy(sm->pool);
    free(sm);
}


//***********************************************************************
// create_service_manager - Creates a new SM for use
//***********************************************************************

service_manager_t *create_service_manager()
{
    service_manager_t *sm;

    type_malloc_clear(sm, service_manager_t, 1);

    apr_pool_create(&sm->pool, NULL);
    apr_thread_mutex_create(&sm->lock, APR_THREAD_MUTEX_DEFAULT, sm->pool);

    sm->table = apr_hash_make(sm->pool);

    return(sm);
}


