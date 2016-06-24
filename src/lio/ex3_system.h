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
#include "lio/ex3_system.h"
//***********************************************************************
// Linear exnode3 support
//***********************************************************************


#include <tbx/list.h>

#include "cache.h"
#include "ex3.h"
#include "service_manager.h"

#ifndef _EX3_SYSTEM_H_
#define _EX3_SYSTEM_H_

#ifdef __cplusplus
extern "C" {
#endif

#define ESS_RUNNING "ess_running"
#define ESS_DS      "ds"
#define ESS_DA      "da"
#define ESS_RS      "rs"
#define ESS_OS      "os"
#define ESS_TPC_UNLIMITED  "tpc_unlimited"
#define ESS_TPC_CACHE  "tpc_cache"
#define ESS_CACHE      "cache"
#define ESS_MQ      "mq"
#define ESS_ONGOING_CLIENT "ongoing_client"

#define MQ_TYPE_ZMQ "mq_zmq"
#define MQ_AVAILABLE "mq_available"

LIO_API extern service_manager_t *lio_exnode_service_set;


//** ex3_global functions
int ex3_set_default_ds(data_service_fn_t *ds);
data_service_fn_t *ex3_get_default_ds();
int ex3_set_default_rs(resource_service_fn_t *rs);
resource_service_fn_t *ex3_get_default_rs();
int ex3_set_default_os(object_service_fn_t *os);
object_service_fn_t *ex3_get_default_os();

int exnode_system_init();
int exnode_system_config(service_manager_t *ess, data_service_fn_t *ds, resource_service_fn_t *rs, object_service_fn_t *os, thread_pool_context_t *tpc_unlimited, cache_t *cache);

void exnode_system_destroy();

#ifdef __cplusplus
}
#endif

#endif

