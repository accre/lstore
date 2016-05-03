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

os_authz_t *osaz_fake_create(service_manager_t *ess, tbx_inip_file_t *ifd, char *section, object_service_fn_t *os)
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
