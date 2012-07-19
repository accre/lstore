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

#include "exnode.h"
#include "list.h"
#include "type_malloc.h"
#include "random.h"

exnode_abstract_set_t *exnode_service_set = NULL;

//***********************************************************************
// exnode_system_init - Initializes the exnode system for use
//***********************************************************************

int exnode_system_init(data_service_fn_t *ds, resource_service_fn_t *rs, object_service_fn_t *os, thread_pool_context_t *tpc_unlimited, thread_pool_context_t *tpc_cpu, cache_t *cache)
{
  type_malloc_clear(exnode_service_set, exnode_abstract_set_t, 1);

  exnode_service_set->ds = ds;
  exnode_service_set->rs = rs;
  exnode_service_set->os = os;
  exnode_service_set->tpc_unlimited = tpc_unlimited;
  exnode_service_set->tpc_cpu = tpc_cpu;
  exnode_service_set->cache = cache;

  init_random();
  
  //** Install the drivers
  install_segment(SEGMENT_TYPE_LINEAR, segment_linear_load, segment_linear_create, exnode_service_set);
  install_segment(SEGMENT_TYPE_FILE, segment_file_load, segment_file_create, exnode_service_set);
  install_segment(SEGMENT_TYPE_CACHE, segment_cache_load, segment_cache_create, exnode_service_set);
  install_segment(SEGMENT_TYPE_LUN, segment_lun_load, segment_lun_create, exnode_service_set);
  install_segment(SEGMENT_TYPE_JERASURE, segment_jerasure_load, segment_jerasure_create, exnode_service_set);
  install_segment(SEGMENT_TYPE_LOG, segment_log_load, segment_log_create, exnode_service_set);

  if (rs != NULL) install_resource_service(rs_type(rs), rs);

  if (ds != NULL) install_data_service(ds_type(ds), ds);

  return(0);
}

//***********************************************************************

void exnode_system_destroy()
{
  destroy_random();

  free(exnode_service_set);
}
