/*
Advanced Computing Center for Research and Education Proprietary License
Version 1.0 (April 2006)

Copyright (c) 2006, Advanced Computing Center for Research and Education,
 Vanderbilt University, All rights reserved.

This Work is the sole and exclusive property of the Advanced Computing Center
for Research and Education department at Vanderbilt University.  No right to
disclose or otherwise disseminate any of the information contained herein is
granted by virtue of your possession of this software except in accordance with
the terms and conditions of a separate License Agreement entered into with
Vanderbilt University.

THE AUTHOR OR COPYRIGHT HOLDERS PROVIDES THE "WORK" ON AN "AS IS" BASIS,
WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, TITLE, FITNESS FOR A PARTICULAR
PURPOSE, AND NON-INFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Vanderbilt University
Advanced Computing Center for Research and Education
230 Appleton Place
Nashville, TN 37203
http://www.accre.vanderbilt.edu
*/

//*************************************************************************
//*************************************************************************

#ifndef __NET_SOCK_H_
#define __NET_SOCK_H_

#define N_BUFSIZE  1024

//#include <sys/select.h>
//#include <sys/time.h>
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
} network_sock_t;

#ifdef __cplusplus
extern "C" {
#endif

void sock_set_peer(net_sock_t *sock, char *address, int add_size);
int sock_status(net_sock_t *sock);
int sock_close(net_sock_t *sock);
long int sock_write(net_sock_t *sock, tbuffer_t *buf, size_t bpos, size_t size, Net_timeout_t tm);
long int sock_read(net_sock_t *sock, tbuffer_t *buf, size_t bpos, size_t size, Net_timeout_t tm);
int sock_connect(net_sock_t *sock, const char *hostname, int port, Net_timeout_t timeout);
int sock_connection_request(net_sock_t *nsock, int timeout);
net_sock_t *sock_accept(net_sock_t *nsock);
int sock_bind(net_sock_t *nsock, char *address, int port);
int sock_listen(net_sock_t *nsock, int max_pending);
void ns_config_sock(NetStream_t *ns, int tcpsize);

#ifdef __cplusplus
}
#endif

#endif

