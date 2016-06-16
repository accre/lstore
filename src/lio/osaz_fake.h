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
// Dummy OS AuthZ implementation
//***********************************************************************

#ifndef _OSAZ_FAKE_H_
#define _OSAZ_FAKE_H_

#include "object_service_abstract.h"

#ifdef __cplusplus
extern "C" {
#endif

#define OSAZ_TYPE_FAKE "fake"

os_authz_t *osaz_fake_create(service_manager_t *ess, tbx_inip_file_t *ifd, char *section, object_service_fn_t *os);

#ifdef __cplusplus
}
#endif

#endif

