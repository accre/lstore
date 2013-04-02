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
// Remote OS implementation for the Server side
//***********************************************************************

#define _log_module_index 214

#include "ex3_system.h"
#include "object_service_abstract.h"
#include "os_file.h"
#include "type_malloc.h"
#include "log.h"
#include "atomic_counter.h"
#include "thread_pool.h"
#include "os_remote.h"
#include "os_remote_priv.h"
#include "append_printf.h"


//***********************************************************************
// os_remote_server_destroy
//***********************************************************************

void os_remote_server_destroy(object_service_fn_t *os)
{
  osrs_priv_t *osrs = (osrs_priv_t *)os->priv;

  //** Shutdown the child OS
  os_destroy_service(osrs->os_child);

  //** Now do the normal cleanup
  apr_pool_destroy(osrs->mpool);

  free(osrs->hostname);
  free(osrs);
  free(os);
}


//***********************************************************************
//  object_service_remote_client_create - Creates a remote client OS
//***********************************************************************

object_service_fn_t *object_service_remote_server_create(service_manager_t *ess, inip_file_t *fd, char *section)
{
  object_service_fn_t *os;
  osrs_priv_t *osrs;
  os_create_t *os_create;
  mq_command_table_t *ctable;
  char *stype, *ctype;

  if (section == NULL) section = "os_remote_client";

  type_malloc_clear(os, object_service_fn_t, 1);
  type_malloc_clear(osrs, osrs_priv_t, 1);
  os->priv = (void *)osrs;

  //** Make the locks and cond variables
  assert(apr_pool_create(&(osrs->mpool), NULL) == APR_SUCCESS);
  apr_thread_mutex_create(&(osrs->lock), APR_THREAD_MUTEX_DEFAULT, osrs->mpool);
  apr_thread_cond_create(&(osrs->cond), osrs->mpool);

  //** Get the host name we bind to
  osrs->hostname= inip_get_string(fd, section, "address", NULL);

  //** Start the child OS.
  stype = inip_get_string(fd, section, "os_local", NULL);
  if (stype == NULL) {  //** Oops missing child OS
     log_printf(0, "ERROR: Mising child OS  section=%s key=rs_local!\n", section); flush_log();
     free(stype);
     abort();
  }

  //** and load it
  ctype = inip_get_string(fd, stype, "type", OS_TYPE_FILE);
  os_create = lookup_service(ess, OS_AVAILABLE, ctype);
  osrs->os_child = (*os_create)(ess, fd, stype);
  if (osrs->os_child == NULL) {
     log_printf(1, "ERROR loading child OS!  type=%s section=%s\n", ctype, stype); flush_log();
     abort();
  }
  free(ctype);
  free(stype);

  //** Get the MQC
  assert((osrs->mqc = lookup_service(ess, ESS_RUNNING, ESS_MQ)) != NULL);

  //** Make the server portal
  osrs->server_portal = mq_portal_create(osrs->mqc, osrs->hostname, MQ_CMODE_SERVER);
  ctable = mq_portal_command_table(osrs->server_portal);
//  mq_command_add(ctable, RSR_GET_RID_CONFIG_KEY, RSR_GET_RID_CONFIG_SIZE, rs, rsrs_rid_config_cb);
//  mq_command_add(ctable, RSR_GET_UPDATE_CONFIG_KEY, RSR_GET_UPDATE_CONFIG_SIZE, rs, rsrs_rid_config_cb);
//  mq_command_add(ctable, RSR_ABORT_KEY, RSR_ABORT_SIZE, rs, rsrs_abort_cb);
  mq_portal_install(osrs->mqc, osrs->server_portal);

  //** Set up the fn ptrs.  This is just for shutdown
  //** so very little is implemented
  os->destroy_service = os_remote_server_destroy;

  os->type = OS_TYPE_REMOTE_SERVER;

  return(os);
}

