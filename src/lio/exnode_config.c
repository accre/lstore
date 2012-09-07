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
// Routines for loading the varios drivvers - segment, layout, and views
//***********************************************************************

#define _log_module_index 152

#include "service_manager.h"
#include "exnode.h"
#include "list.h"
#include "type_malloc.h"
#include "random.h"

exnode_abstract_set_t *exnode_service_set = NULL;

//***********************************************************************
//  exnode_service_set_create - Creates a default ESS
//***********************************************************************

exnode_abstract_set_t *exnode_service_set_create()
{
  exnode_abstract_set_t *ess;

  type_malloc_clear(ess, exnode_abstract_set_t, 1);
  ess->dsm = create_service_manager(2);
  ess->rsm = create_service_manager(1);
  ess->ssm = create_service_manager(2);
  ess->osm = create_service_manager(1);
  ess->authn_sm = create_service_manager(1);
  ess->osaz_sm = create_service_manager(1);

  //** Install the drivers
  set_service_type_arg(ess->ssm, SEG_SM_LOAD, ess);
  set_service_type_arg(ess->ssm, SEG_SM_CREATE, ess);
  add_service(ess->ssm, SEG_SM_LOAD, SEGMENT_TYPE_LINEAR, segment_linear_load);  add_service(ess->ssm, SEG_SM_CREATE, SEGMENT_TYPE_LINEAR, segment_linear_create);
  add_service(ess->ssm, SEG_SM_LOAD, SEGMENT_TYPE_FILE, segment_file_load);  add_service(ess->ssm, SEG_SM_CREATE, SEGMENT_TYPE_FILE, segment_file_create);
  add_service(ess->ssm, SEG_SM_LOAD, SEGMENT_TYPE_CACHE, segment_cache_load);  add_service(ess->ssm, SEG_SM_CREATE, SEGMENT_TYPE_CACHE, segment_cache_create);
  add_service(ess->ssm, SEG_SM_LOAD, SEGMENT_TYPE_LUN, segment_lun_load);  add_service(ess->ssm, SEG_SM_CREATE, SEGMENT_TYPE_LUN, segment_lun_create);
  add_service(ess->ssm, SEG_SM_LOAD, SEGMENT_TYPE_JERASURE, segment_jerasure_load);  add_service(ess->ssm, SEG_SM_CREATE, SEGMENT_TYPE_JERASURE, segment_jerasure_create);
  add_service(ess->ssm, SEG_SM_LOAD, SEGMENT_TYPE_LOG, segment_log_load);  add_service(ess->ssm, SEG_SM_CREATE, SEGMENT_TYPE_LOG, segment_log_create);

  add_service(ess->rsm, RS_SM_AVAILABLE, RS_TYPE_SIMPLE, rs_simple_create);
  add_service(ess->rsm, RS_SM_AVAILABLE, RS_TYPE_ZMQ, rs_zmq_create);

  add_service(ess->dsm, DS_SM_AVAILABLE, DS_TYPE_IBP, ds_ibp_create);

  add_service(ess->osm, 0, OS_TYPE_FILE, object_service_file_create);

  add_service(ess->authn_sm, 0, AUTHN_TYPE_FAKE, authn_fake_create);

  add_service(ess->osaz_sm, 0, AUTHN_TYPE_FAKE, osaz_fake_create);

//  install_authn_service(AUTHN_TYPE_FAKE, authn_fake_create);

//  install_os_authz_service(OSAZ_TYPE_FAKE, osaz_fake_create);

  return(ess);
}

//***********************************************************************
// exnode_service_set_destroy - Destroys an ESS
//***********************************************************************

void exnode_service_set_destroy(exnode_abstract_set_t *ess)
{
  destroy_service_manager(ess->dsm);
  destroy_service_manager(ess->rsm);
  destroy_service_manager(ess->ssm);
  destroy_service_manager(ess->osm);
  destroy_service_manager(ess->authn_sm);
  destroy_service_manager(ess->osaz_sm);
  free(ess);
}

//***********************************************************************
// exnode_system_init - Initializes the exnode system for use
//***********************************************************************

int exnode_system_init()
{
  init_random();

  exnode_service_set = exnode_service_set_create();

  cache_system_init();

  return(0);
}


//***********************************************************************
// exnode_system_config - Configures  the exnode system for use
//***********************************************************************

int exnode_system_config(exnode_abstract_set_t *ess, data_service_fn_t *ds, resource_service_fn_t *rs, object_service_fn_t *os, thread_pool_context_t *tpc_unlimited, thread_pool_context_t *tpc_cpu, cache_t *cache)
{
  ess->ds = ds;
  ess->rs = rs;
  ess->os = os;
  ess->tpc_unlimited = tpc_unlimited;
  ess->tpc_cpu = tpc_cpu;
  ess->cache = cache;

  add_service(ess->dsm, DS_SM_RUNNING, DS_TYPE_IBP, ds);

  return(0);
}


//***********************************************************************

void exnode_system_destroy()
{
  destroy_random();

  exnode_service_set_destroy(exnode_service_set);
}
