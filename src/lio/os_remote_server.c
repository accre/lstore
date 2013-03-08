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

#include "object_service_abstract.h"
#include "type_malloc.h"
#include "log.h"
#include "atomic_counter.h"
#include "thread_pool.h"
#include "os_remote_client.h"
#include "os_remote_server_priv.h"
#include "append_printf.h"


//***********************************************************************
// os_remote_server_destroy
//***********************************************************************

void os_remote_server_destroy(object_service_fn_t *os)
{
  osrs_priv_t *osrs = (osrs_priv_t *)os->priv;

  apr_pool_destroy(osrs->mpool);

  free(osrs->host_ros);
  free(osrs);
  free(os);
}


//***********************************************************************
//  object_service_remote_client_create - Creates a remote client OS
//***********************************************************************

object_service_fn_t *object_service_remote_server_create(service_manager_t *ess, inip_file_t *ifd, char *section)
{
  object_service_fn_t *os;
  osrs_priv_t *osrs;

  if (section == NULL) section = "os_remote_client";

  type_malloc_clear(os, object_service_fn_t, 1);
  type_malloc_clear(osrs, osrs_priv_t, 1);
  os->priv = (void *)osrs;

  return(os);
}

