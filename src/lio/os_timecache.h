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
// OS Remote Server/Client header file
//***********************************************************************

#ifndef _OS_TIMECACHE_H_
#define _OS_TIMECACHE_H_

#include <tbx/iniparse.h>

#include "os.h"
#include "service_manager.h"

#ifdef __cplusplus
extern "C" {
#endif

#define OS_TYPE_TIMECACHE "os_timecache"

object_service_fn_t *object_service_timecache_create(service_manager_t *ess, tbx_inip_file_t *ifd, char *section);

#ifdef __cplusplus
}
#endif

#endif

