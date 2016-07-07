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
// Linear exnode3 support
//***********************************************************************

#ifndef _EX3_SYSTEM_H_
#define _EX3_SYSTEM_H_

#include <lio/ex3.h>
#include <tbx/list.h>

#include "cache.h"
#include "ex3.h"
#include "service_manager.h"

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

LIO_API extern lio_service_manager_t *lio_exnode_service_set;


//** ex3_global functions
int ex3_set_default_ds(lio_data_service_fn_t *ds);
lio_data_service_fn_t *ex3_get_default_ds();
int ex3_set_default_rs(lio_resource_service_fn_t *rs);
lio_resource_service_fn_t *ex3_get_default_rs();
int ex3_set_default_os(lio_object_service_fn_t *os);
lio_object_service_fn_t *ex3_get_default_os();

int exnode_system_init();
int exnode_system_config(lio_service_manager_t *ess, lio_data_service_fn_t *ds, lio_resource_service_fn_t *rs, lio_object_service_fn_t *os, gop_thread_pool_context_t *tpc_unlimited, lio_cache_t *cache);

void exnode_system_destroy();

#ifdef __cplusplus
}
#endif

#endif
