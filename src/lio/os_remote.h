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

#include "object_service_abstract.h"

#ifndef _OS_REMOTE_H_
#define _OS_REMOTE_H_

#ifdef __cplusplus
extern "C" {
#endif

#define OS_TYPE_REMOTE_SERVER "os_remote_server"
#define OS_TYPE_REMOTE_CLIENT "os_remote_client"

object_service_fn_t *object_service_remote_server_create(service_manager_t *ess, tbx_inip_file_t *fd, char *section);
object_service_fn_t *object_service_remote_client_create(service_manager_t *ess, tbx_inip_file_t *ifd, char *section);


#ifdef __cplusplus
}
#endif

#endif

