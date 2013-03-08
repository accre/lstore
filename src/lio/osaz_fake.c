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
// Dummy/Fake AuthN service.  Always returns success!
//***********************************************************************

#define _log_module_index 186

#include "list.h"
#include "type_malloc.h"
#include "log.h"
#include "authn_abstract.h"
#include "object_service_abstract.h"

//***********************************************************************

int osaz_fake_object_create_remove(os_authz_t *osa, creds_t *c, char *path)
{
  return(1);
}

//***********************************************************************

int osaz_fake_object_access(os_authz_t *osa, creds_t *c, char *path, int mode)
{
  return(1);
}

//***********************************************************************

int osaz_fake_attr_create_remove(os_authz_t *osa, creds_t *c, char *path, char *key)
{
  return(1);
}

//***********************************************************************

int osaz_fake_attr_access(os_authz_t *osa, creds_t *c, char *path, char *key, int mode)
{
  return(1);
}

//***********************************************************************

void osaz_fake_destroy(os_authz_t *az)
{
  free(az);
}


//***********************************************************************
// osaz_fake_create - Create a Fake AuthN service
//***********************************************************************

os_authz_t *osaz_fake_create(service_manager_t *ess, inip_file_t *ifd, char *section, object_service_fn_t *os)
{
  os_authz_t *osaz;

  type_malloc(osaz, os_authz_t, 1);

  osaz->priv = NULL;
  osaz->object_create = osaz_fake_object_create_remove;
  osaz->object_remove = osaz_fake_object_create_remove;
  osaz->object_access = osaz_fake_object_access;
  osaz->attr_create = osaz_fake_attr_create_remove;
  osaz->attr_remove = osaz_fake_attr_create_remove;
  osaz->attr_access = osaz_fake_attr_access;
  osaz->destroy = osaz_fake_destroy;

  return(osaz);
}
