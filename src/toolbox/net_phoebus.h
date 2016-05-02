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

#ifndef __NET_PHOEBUS_H_
#define __NET_PHOEBUS_H_

#include "tbx/toolbox_visibility.h"
#include "toolbox_config.h"

#define N_BUFSIZE  1024

typedef struct {
    tbx_phoebus_t *p_path;
    liblslSess *sess;
    int family;
    int fd;
    int tcpsize;
    char address[16];
} tbx_net_phoebus_t;

#ifdef __cplusplus
extern "C" {
#endif

#ifdef _ENABLE_PHOEBUS   //** These routines are only needed if we have phoebus
void phoebus_set_peer(net_sock_t *sock, char *address, int add_size);
int phoebus_status(net_sock_t *sock);
int phoebus_close(net_sock_t *sock);
long int phoebus_write(net_sock_t *sock, const void *buf, size_t count, Net_timeout_t tm);
long int phoebus_read(net_sock_t *sock, void *buf, size_t count, Net_timeout_t tm);
int phoebus_connect(net_sock_t *sock, const char *hostname, int port, Net_timeout_t timeout);
#endif
 
TBX_API void ns_config_phoebus(tbx_ns_t *ns, tbx_phoebus_t *path, int tcpsize);
 
#ifdef __cplusplus
}
#endif
 
#endif
 
 
