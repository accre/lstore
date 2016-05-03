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

#ifndef __NET_SOCK_H_
#define __NET_SOCK_H_

#define N_BUFSIZE  1024

//#include <sys/select.h>
//#include <sys/time.h>
#include "tbx/toolbox_visibility.h"
#include <apr_network_io.h>
#include <apr_poll.h>
#include "network.h"

typedef struct {  //** Contains the private raw socket network fields
    apr_socket_t  *fd;
    apr_sockaddr_t *sa;
    apr_pollset_t *pollset;
    apr_pollfd_t pfd;
    apr_pool_t *mpool;
    apr_thread_mutex_t *lock; //** Global lock
    int tcpsize;
    int state;
} tbx_net_sock_t;

#ifdef __cplusplus
extern "C" {
#endif

void sock_set_peer(net_sock_t *sock, char *address, int add_size);
int sock_status(net_sock_t *sock);
int sock_close(net_sock_t *sock);
long int sock_write(net_sock_t *sock, tbx_tbuf_t *buf, size_t bpos, size_t size, Net_timeout_t tm);
long int sock_read(net_sock_t *sock, tbx_tbuf_t *buf, size_t bpos, size_t size, Net_timeout_t tm);
int sock_connect(net_sock_t *sock, const char *hostname, int port, Net_timeout_t timeout);
int sock_connection_request(net_sock_t *nsock, int timeout);
net_sock_t *sock_accept(net_sock_t *nsock);
int sock_bind(net_sock_t *nsock, char *address, int port);
int sock_listen(net_sock_t *nsock, int max_pending);
TBX_API void ns_config_sock(tbx_ns_t *ns, int tcpsize);

#ifdef __cplusplus
}
#endif

#endif

