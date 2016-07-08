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

#include <stdlib.h>
#include <tbx/assert_result.h>
#include <tbx/iniparse.h>
#include <tbx/type_malloc.h>

#include "authn.h"
#include "os.h"
#include "service_manager.h"

//***********************************************************************

int osaz_fake_object_create_remove(lio_os_authz_t *osa, lio_creds_t *c, char *path)
{
    return(1);
}

//***********************************************************************

int osaz_fake_object_access(lio_os_authz_t *osa, lio_creds_t *c, char *path, int mode)
{
    return(1);
}

//***********************************************************************

int osaz_fake_attr_create_remove(lio_os_authz_t *osa, lio_creds_t *c, char *path, char *key)
{
    return(1);
}

//***********************************************************************

int osaz_fake_attr_access(lio_os_authz_t *osa, lio_creds_t *c, char *path, char *key, int mode)
{
    return(1);
}

//***********************************************************************

void osaz_fake_destroy(lio_os_authz_t *az)
{
    free(az);
}


//***********************************************************************
// osaz_fake_create - Create a Fake AuthN service
//***********************************************************************

lio_os_authz_t *osaz_fake_create(lio_service_manager_t *ess, tbx_inip_file_t *ifd, char *section, lio_object_service_fn_t *os)
{
    lio_os_authz_t *osaz;

    tbx_type_malloc(osaz, lio_os_authz_t, 1);

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
