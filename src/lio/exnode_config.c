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
// Routines for loading the varios drivvers - segment, layout, and views
//***********************************************************************

#define _log_module_index 152

#include "service_manager.h"
#include "exnode.h"
#include <tbx/list.h>
#include <tbx/type_malloc.h>
#include <tbx/random.h>

service_manager_t *exnode_service_set = NULL;

//***********************************************************************
//  exnode_service_set_create - Creates a default ESS
//***********************************************************************

service_manager_t *exnode_service_set_create()
{
    service_manager_t *ess;

    ess = create_service_manager();

    //** Install the drivers
    add_service(ess, SEG_SM_LOAD, SEGMENT_TYPE_LINEAR, segment_linear_load);
    add_service(ess, SEG_SM_CREATE, SEGMENT_TYPE_LINEAR, segment_linear_create);
    add_service(ess, SEG_SM_LOAD, SEGMENT_TYPE_FILE, segment_file_load);
    add_service(ess, SEG_SM_CREATE, SEGMENT_TYPE_FILE, segment_file_create);
    add_service(ess, SEG_SM_LOAD, SEGMENT_TYPE_CACHE, segment_cache_load);
    add_service(ess, SEG_SM_CREATE, SEGMENT_TYPE_CACHE, segment_cache_create);
    add_service(ess, SEG_SM_LOAD, SEGMENT_TYPE_LUN, segment_lun_load);
    add_service(ess, SEG_SM_CREATE, SEGMENT_TYPE_LUN, segment_lun_create);
    add_service(ess, SEG_SM_LOAD, SEGMENT_TYPE_JERASURE, segment_jerasure_load);
    add_service(ess, SEG_SM_CREATE, SEGMENT_TYPE_JERASURE, segment_jerasure_create);
    add_service(ess, SEG_SM_LOAD, SEGMENT_TYPE_LOG, segment_log_load);
    add_service(ess, SEG_SM_CREATE, SEGMENT_TYPE_LOG, segment_log_create);

    add_service(ess, RS_SM_AVAILABLE, RS_TYPE_SIMPLE, rs_simple_create);
    add_service(ess, RS_SM_AVAILABLE, RS_TYPE_REMOTE_CLIENT, rs_remote_client_create);
    add_service(ess, RS_SM_AVAILABLE, RS_TYPE_REMOTE_SERVER, rs_remote_server_create);

    add_service(ess, DS_SM_AVAILABLE, DS_TYPE_IBP, ds_ibp_create);

    add_service(ess, OS_AVAILABLE, OS_TYPE_FILE, object_service_file_create);
    add_service(ess, OS_AVAILABLE, OS_TYPE_REMOTE_CLIENT, object_service_remote_client_create);
    add_service(ess, OS_AVAILABLE, OS_TYPE_REMOTE_SERVER, object_service_remote_server_create);
    add_service(ess, OS_AVAILABLE, OS_TYPE_TIMECACHE, object_service_timecache_create);

    add_service(ess, AUTHN_AVAILABLE, AUTHN_TYPE_FAKE, authn_fake_create);

    add_service(ess, OSAZ_AVAILABLE, OSAZ_TYPE_FAKE, osaz_fake_create);

//***UNDOME  add_service(ess, CACHE_LOAD_AVAILABLE, CACHE_TYPE_LRU, lru_cache_load);   add_service(ess, CACHE_CREATE_AVAILABLE, CACHE_TYPE_LRU, lru_cache_create);
    add_service(ess, CACHE_LOAD_AVAILABLE, CACHE_TYPE_AMP, amp_cache_load);
    add_service(ess, CACHE_CREATE_AVAILABLE, CACHE_TYPE_AMP, amp_cache_create);
    add_service(ess, CACHE_LOAD_AVAILABLE, CACHE_TYPE_ROUND_ROBIN, round_robin_cache_load);
    add_service(ess, CACHE_CREATE_AVAILABLE, CACHE_TYPE_ROUND_ROBIN, round_robin_cache_create);

    return(ess);
}

//***********************************************************************
// exnode_service_set_destroy - Destroys an ESS
//***********************************************************************

void exnode_service_set_destroy(service_manager_t *ess)
{
    destroy_service_manager(ess);
}

//***********************************************************************
// exnode_system_init - Initializes the exnode system for use
//***********************************************************************

int exnode_system_init()
{
    tbx_random_startup();

    exnode_service_set = exnode_service_set_create();

    return(0);
}


//***********************************************************************
// exnode_system_config - Configures  the exnode system for use
//***********************************************************************

int exnode_system_config(service_manager_t *ess, data_service_fn_t *ds, resource_service_fn_t *rs, object_service_fn_t *os, thread_pool_context_t *tpc_unlimited, cache_t *cache)
{

    add_service(ess, ESS_RUNNING, ESS_DS, ds);
    add_service(ess, ESS_RUNNING, ESS_RS, rs);
    add_service(ess, ESS_RUNNING, ESS_OS, os);
    add_service(ess, ESS_RUNNING, ESS_TPC_UNLIMITED, tpc_unlimited);
    add_service(ess, ESS_RUNNING, ESS_CACHE, cache);

    return(0);
}


//***********************************************************************

void exnode_system_destroy()
{
    tbx_random_shutdown();

    exnode_service_set_destroy(exnode_service_set);
}
