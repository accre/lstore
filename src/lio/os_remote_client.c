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
// Remote OS implementation for the client side
//***********************************************************************

#define _log_module_index 213

#include "ex3_system.h"
#include "object_service_abstract.h"
#include "type_malloc.h"
#include "log.h"
#include "atomic_counter.h"
#include "thread_pool.h"
#include "os_remote.h"
#include "os_remote_priv.h"
#include "append_printf.h"

//***********************************************************************
// os_remote_client_destroy
//***********************************************************************

void os_remote_client_destroy(object_service_fn_t *os)
{
  osrc_priv_t *osrc = (osrc_priv_t *)os->priv;


  osaz_destroy(osrc->osaz);
  authn_destroy(osrc->authn);

  apr_pool_destroy(osrc->mpool);

  free(osrc->host_ros);
  free(osrc);
  free(os);
}


//***********************************************************************
//  object_service_remote_client_create - Creates a remote client OS
//***********************************************************************

object_service_fn_t *object_service_remote_client_create(service_manager_t *ess, inip_file_t *fd, char *section)
{
  object_service_fn_t *os;
  osrc_priv_t *osrc;
  char *str;

  if (section == NULL) section = "os_remote_client";

  type_malloc_clear(os, object_service_fn_t, 1);
  type_malloc_clear(osrc, osrc_priv_t, 1);
  os->priv = (void *)osrc;

  str = inip_get_string(fd, section, "os_temp", NULL);
  if (str != NULL) {  //** Running in test/temp
     log_printf(0, "NOTE: Running in debug mode by loading Remote server locally!\n");
     osrc->os_temp = object_service_remote_server_create(ess, fd, str);
     assert(osrc->os_temp != NULL);
     free(str);
  }

  //** Get the MQC
  assert((osrc->mqc = lookup_service(ess, ESS_RUNNING, ESS_MQ)) != NULL);

  //** Set up the fn ptrs
  os->type = OS_TYPE_REMOTE_CLIENT;


  return(os);
}

