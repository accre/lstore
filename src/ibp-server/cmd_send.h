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

#ifndef _CMD_SEND_H_
#define _CMD_SEND_H_

#include "visibility.h"
#include <tbx/network.h>
#include "ibp_server.h"

#ifdef __cplusplus
extern "C" {
#endif

IBPS_API tbx_ns_t *cmd_send(char *host, int port, char *cmd, char **res_buffer, int timeout);

#ifdef __cplusplus
}
#endif


#endif

