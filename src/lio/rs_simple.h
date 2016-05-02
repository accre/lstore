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
// Simple resource managment implementation
//***********************************************************************

#include <tbx/list.h>
#include "resource_service_abstract.h"

#ifndef _RS_SIMPLE_H_
#define _RS_SIMPLE_H_

#ifdef __cplusplus
extern "C" {
#endif

#define RS_TYPE_SIMPLE "simple"

resource_service_fn_t *rs_simple_create(void *arg, tbx_inip_file_t *fd, char *section);

#ifdef __cplusplus
}
#endif

#endif

