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

//*************************************************************************
//*************************************************************************

#ifndef __NET_1_SSL_H_
#define __NET_1_SSL_H_

#include "tbx/toolbox_visibility.h"
#define N_BUFSIZE  1024

//#include <sys/select.h>
//#include <sys/time.h>
//#include <pthread.h>
#include "network.h"

#ifdef __cplusplus
extern "C" {
#endif

int ns_socket2ssl(tbx_ns_t *ns);
TBX_API void ns_config_1_ssl(tbx_ns_t *ns, int fd, int tcpsize);

#ifdef __cplusplus
}
#endif

#endif

