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

//*********************************************************************
//*********************************************************************

#define _log_module_index 119

#include "network.h"
#include "debug.h"
#include "log.h"
#include "phoebus.h"

#ifndef _ENABLE_PHOEBUS    //** Phoebus stub goes below
 
#include "net_sock.h"
 
void ns_config_phoebus(NetStream_t *ns, phoebus_t *path, int tcpsize)
{
ns_config_sock(ns, tcpsize);
}
 
#else //*** All the phoebus code.  IF not enables only ns_phoebus_config is defined
 
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <sys/uio.h>
#include <netdb.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <assert.h>
#include <signal.h>
#include <errno.h>
#include <fcntl.h>
#include "dns_cache.h"
#include "fmttypes.h"
#include "net_sock.h"
#include "net_phoebus.h"
#include "net_fd.h"
 
 
//*********************************************************************
// phoebus_set_peer - Gets the remote sockets hostname 
//*********************************************************************
 
void phoebus_set_peer(net_sock_t *nsock, char *address, int add_size)
{
network_phoebus_t *sock = (network_phoebus_t *)nsock;
 
if (sock == NULL) {
address[0] = '\0';
} else {
inet_ntop(sock->family, sock->address, address, add_size);
address[add_size-1] = '\0';
}
 
return;
}
 
//*********************************************************************
//  phoebus_status - Returns 0 if the socket is connected and -1 otherwise
//*********************************************************************
 
int phoebus_status(net_sock_t *nsock)
{
network_phoebus_t *sock = (network_phoebus_t *)nsock;
 
if (sock == NULL) return(0);
if (sock->fd != -1) return(1);
 
return(0);
}
 
//*********************************************************************
//  phoebus_close - Socket close call
//*********************************************************************
 
int phoebus_close(net_sock_t *nsock)
{
network_phoebus_t *sock = (network_phoebus_t *)nsock;
 
if (sock == NULL) return(0);
if (sock->fd == -1) return(0);
 
int err = close(sock->fd);
 
lsl_close(sock->sess);
 
free(sock);
 
return(err);
}
 
//*********************************************************************
//  phoebus_write
//*********************************************************************
 
long int phoebus_write(net_sock_t *nsock, const void *buf, size_t count, Net_timeout_t tm)
{
network_phoebus_t *sock = (network_phoebus_t *)nsock;
 
if (sock == NULL) return(-1);   //** If closed return
 
return(fd_write(sock->fd, buf, count, tm));
}
 
//*********************************************************************
//  phoebus_read
//*********************************************************************
 
long int phoebus_read(net_sock_t *nsock, void *buf, size_t count, Net_timeout_t tm)
{
network_phoebus_t *sock = (network_phoebus_t *)nsock;
 
if (sock == NULL) return(-1);   //** If closed return
 
return(fd_read(sock->fd, buf, count, tm));
}
 
 
//*********************************************************************
// phoebus_connect - Creates a connection to a remote host
//*********************************************************************
 
int phoebus_connect(net_sock_t *nsock, const char *hostname, int port, Net_timeout_t timeout)
{
network_phoebus_t *sock = (network_phoebus_t *)nsock;
 
char in_addr[16];
int sfd, err;
int i;
fd_set wfd;
apr_time_t endtime;
struct timeval to;
char dest[LSL_DEPOTID_LEN];
 
log_printf(5, "phoebus_connect: Trying to make connection to Hostname: %s  Port: %d\n", hostname, port);
snprintf(dest, LSL_DEPOTID_LEN, "%s/%d", hostname, port);
 
if (sock == NULL) return(1);
 
sock->fd = -1;
 
// Get ip address
if (lookup_host(hostname, in_addr, NULL) != 0) {
log_printf(0, "phoebus_connect: lookup_host failed.  Hostname: %s  Port: %d\n", hostname, port);
return(1);
}
 
// Store the host for phoebus_set_peer ---Needs work to be ipv6 compatible
sock->family = AF_INET;
i = (sock->family == AF_INET) ? 4 : 16;
memcpy(&(sock->address[0]), in_addr, i);
 
if (sock->p_path != NULL) {
sock->sess = lsl_session();
 
if (sock->tcpsize > 0) {
log_printf(10, "phoebus_connect: Setting tcpbufsize=%d\n", sock->tcpsize);
if (lsl_setsockopt(sock->sess, SOL_SOCKET, SO_SNDBUF, (char*)&(sock->tcpsize), sizeof(sock->tcpsize)) != 0) {
log_printf(0, "phoebus_connect: Can't configure SO_SNDBUF to %d!\n", sock->tcpsize);
}
if (lsl_setsockopt(sock->sess, SOL_SOCKET, SO_RCVBUF, (char*)&(sock->tcpsize), sizeof(sock->tcpsize)) != 0) {
log_printf(0, "phoebus_connect: Can't configure SO_RCVBUF to %d!\n", sock->tcpsize);
}
}
 
for (i=0; i<sock->p_path->p_count; i++) {
log_printf(15, "phoebus_connect: hop %d : %s\n", i, sock->p_path->path[i]);
lsl_sess_appendchild(sock->sess, sock->p_path->path[i], LSL_DEPOT_NATIVE);
}
 
lsl_sess_appendchild(sock->sess, dest, 0);
 
if (lsl_connect(sock->sess)) {
log_printf(0, "phoebus_connect: could not connect to %s\n", dest);
return (1);
}
 
sfd = lsl_get_session_socket(sock->sess);
} else {
log_printf(0, "phoebus_connect: called without valid phoebus path!\n");
return(1);
}
 
// Configure it correctly
int flag=1;
if (setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, (char*)&flag, sizeof(flag)) != 0) {
log_printf(0, "phoebus_connect: Can't configure SO_REUSEADDR!\n");
}
 
 
//Configure the socket for non-blocking I/O
if ((err = fcntl(sfd, F_SETFL, O_NONBLOCK)) == -1) {
log_printf(0, "phoebus_connect: Can't configure connection for non-blocking I/O!");
}
 
log_printf(20, "phoebus_connect: Before select timeout=%lu time=" TT "\n", timeout, apr_time_now());
endtime = apr_time_now() + apr_time_make(5, 0);
do {
FD_ZERO(&wfd);
FD_SET(sfd, &wfd);
to.tv_sec = 1; 
to.tv_usec = 0;
err = select(sfd+1, NULL, &wfd, NULL, &to);
log_printf(20, "phoebus_connect: After select\n");
if (err != 1) {
if (errno != EINPROGRESS) {
log_printf(0, "phoeubs_connect: select failed.  Hostname: %s  Port: %d select=%d errno: %d error: %s\n", hostname, port, err, errno, strerror(errno));
return(1);
} else {
log_printf(10, "phoebus_connect: In progress.....  time=" TT " Hostname: %s  Port: %d select=%d errno: %d error: %s\n", apr_time_now(), hostname, port, err, errno, strerror(errno));
}
}
} while ((err != 1) && (apr_time_now() < endtime));
 
sock->fd = sfd;
 
return(0);
}
 
//*********************************************************************
// ns_config_phoebus - Configure the connection to use "phoebus" sockets 
//*********************************************************************
 
void ns_config_phoebus(NetStream_t *ns, phoebus_t *path, int tcpsize)
{
network_phoebus_t *sock = (network_phoebus_t *)malloc(sizeof(network_phoebus_t));
 
_ns_init(ns, 0);
 
ns->sock_type = NS_TYPE_PHOEBUS;
ns->sock = (net_sock_t *)sock;
 
sock->p_path = path;
if (path == NULL) sock->p_path = global_phoebus;
 
sock->fd = -1;
sock->family = -1;
sock->tcpsize = tcpsize;
 
ns->connect = phoebus_connect;
ns->sock_status = sock_status;
ns->set_peer = phoebus_set_peer;
ns->close = phoebus_close;
ns->read = phoebus_read;
ns->write = phoebus_write;
}
 
#endif
 