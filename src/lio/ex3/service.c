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

#include <gop/tp.h>
#include <stddef.h>
#include <tbx/random.h>

#include "authn.h"
#include "authn/fake.h"
#include "cache.h"
#include "cache/amp.h"
#include "cache/round_robin.h"
#include "ds.h"
#include "ds/ibp.h"
#include "ex3.h"
#include "ex3/system.h"
#include "os.h"
#include "os/file.h"
#include "os/remote.h"
#include "os/timecache.h"
#include "osaz/fake.h"
#include "rs.h"
#include "rs/remote.h"
#include "rs/simple.h"
#include "segment/cache.h"
#include "segment/file.h"
#include "segment/jerasure.h"
#include "segment/linear.h"
#include "segment/log.h"
#include "segment/lun.h"
#include "service_manager.h"

lio_service_manager_t *lio_exnode_service_set = NULL;

//***********************************************************************
//  lio_exnode_service_set_create - Creates a default ESS
//***********************************************************************

lio_service_manager_t *lio_exnode_service_set_create()
{
    lio_service_manager_t *ess;

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

    add_service(ess, CACHE_LOAD_AVAILABLE, CACHE_TYPE_AMP, amp_cache_load);
    add_service(ess, CACHE_CREATE_AVAILABLE, CACHE_TYPE_AMP, amp_cache_create);
    add_service(ess, CACHE_LOAD_AVAILABLE, CACHE_TYPE_ROUND_ROBIN, round_robin_cache_load);
    add_service(ess, CACHE_CREATE_AVAILABLE, CACHE_TYPE_ROUND_ROBIN, round_robin_cache_create);

    return(ess);
}

//***********************************************************************
// lio_exnode_service_set_destroy - Destroys an ESS
//***********************************************************************

void lio_exnode_service_set_destroy(lio_service_manager_t *ess)
{
    destroy_service_manager(ess);
}

//***********************************************************************
// exnode_system_init - Initializes the exnode system for use
//***********************************************************************

int exnode_system_init()
{
    tbx_random_startup();

    lio_exnode_service_set = lio_exnode_service_set_create();

    return(0);
}


//***********************************************************************
// exnode_system_config - Configures  the exnode system for use
//***********************************************************************

int exnode_system_config(lio_service_manager_t *ess, lio_data_service_fn_t *ds, lio_resource_service_fn_t *rs, lio_object_service_fn_t *os, gop_thread_pool_context_t *tpc_unlimited, lio_cache_t *cache)
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

    lio_exnode_service_set_destroy(lio_exnode_service_set);
}
