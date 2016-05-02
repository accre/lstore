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

#pragma once
#ifndef ACCRE_NET_SOCK_H_INCLUDED
#define ACCRE_NET_SOCK_H_INCLUDED

#include "tbx/toolbox_visibility.h"

#ifdef __cplusplus
extern "C" {
#endif

// Types
typedef struct tbx_net_sock_t tbx_net_sock_t;  //** Contains the private raw socket network fields

// Functions
TBX_API void tbx_ns_sock_config(tbx_ns_t *ns, int tcpsize);

#ifdef __cplusplus
}
#endif

#endif
