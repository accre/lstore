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
// Generic service manager header file
//***********************************************************************

struct service_manager_t;

#ifndef _SERVICE_MANAGER_H_
#define _SERVICE_MANAGER_H_

#include <apr_hash.h>
#include <apr_pools.h>
#include <apr_thread_mutex.h>

#include "lio/lio_visibility.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct service_manager_t service_manager_t;
struct service_manager_t {
    apr_pool_t *pool;
    apr_thread_mutex_t *lock;
    apr_hash_t *table;
};

service_manager_t *clone_service_manager(service_manager_t *sm);
service_manager_t *create_service_manager();
void destroy_service_manager(service_manager_t *sm);
LIO_API void *lio_lookup_service(service_manager_t *sm, char *service_section, char *service_name);
int add_service(service_manager_t *sm, char *service_section, char *service_name, void *service);
int remove_service(service_manager_t *sm, char *service_section, char *service_name);

#ifdef __cplusplus
}
#endif

#endif

