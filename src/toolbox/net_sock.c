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

#define _log_module_index 120

#include <sys/types.h>
#include <apr_network_io.h>
#include <apr_poll.h>
#include <apr_portable.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <assert.h>
#include <signal.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include "network.h"
#include "debug.h"
#include "log.h"
#include "dns_cache.h"
#include "fmttypes.h"
#include "net_sock.h"
#include <poll.h>

typedef struct {
   apr_pool_t *pool;
   int fd;
} sock_apr_overlay_t;
//typedef union {
//  apr_socket_t sock;
//  sock_apr_overlay_t overlay;
//} sock_apr_union_t;

//#define SOCK_DEFAULT_TIMEOUT 1000*1000
#define SOCK_DEFAULT_TIMEOUT 0
#define SOCK_WAIT_READ  POLLIN
#define SOCK_WAIT_WRITE POLLOUT

//*********************************************************************
// sock_set_peer - Gets the remote sockets hostname
//*********************************************************************

void sock_set_peer(net_sock_t *nsock, char *address, int add_size)
{
   network_sock_t *sock = (network_sock_t *)nsock;
   apr_sockaddr_t *sa;

   address[0] = '\0';
   if (sock == NULL) return;
   if (sock->fd == NULL) return;

   if (apr_socket_addr_get(&sa, APR_REMOTE, sock->fd) != APR_SUCCESS) return;
   apr_sockaddr_ip_getbuf(address, add_size, sa);

   return;
}

//*********************************************************************
//  sock_status - Returns 1 if the socket is connected and 0 otherwise
//*********************************************************************

int sock_status(net_sock_t *nsock)
{
  network_sock_t *sock = (network_sock_t *)nsock;
  if (sock == NULL) return(0);

  return(1);
}

//*********************************************************************
//  sock_close - Base socket close call
//*********************************************************************

int sock_close(net_sock_t *nsock)
{
  network_sock_t *sock = (network_sock_t *)nsock;

  if (sock == NULL) return(0);

//log_printf(15, "sock_close: closing fd=%d\n", sock->fd);

  //**QWERT
  apr_thread_mutex_destroy(sock->lock);
  //**QWERTY


  if (sock->fd != NULL) apr_socket_close(sock->fd);
  if (sock->pollset != NULL) apr_pollset_destroy(sock->pollset);
  apr_pool_destroy(sock->mpool);

  free(sock);

  return(0);
}

//*********************************************************************
// sock_io_wait
//*********************************************************************

int sock_io_wait(network_sock_t *sock, Net_timeout_t tm, int mode)
{
   struct pollfd pfd;
   int state, dt;
   sock_apr_overlay_t *u = (sock_apr_overlay_t *)(sock->fd);

apr_time_t start = apr_time_now();
double ddt;
//log_printf(10, "sock_io_wait: START tm=" TT " mode=%d\n", tm, mode);  flush_log();

   dt = tm / 1000; if (dt == 0) dt = 1;
   pfd.fd = u->fd;
//log_printf(10, "sock_io_wait: fd=%d tm=" TT " mode=%d\n", pfd.fd, tm, mode);

   pfd.events = mode;

   do {
     state = poll(&pfd, 1, dt);
   } while ((state == -1) && (errno == EINTR));

ddt = apr_time_now() - start;
ddt /= (1.0*APR_USEC_PER_SEC);
log_printf(5, "sock_io_wait: fd=%d tm=" TT " mode=%d state=%d errno=%d dt=%lf\n", pfd.fd, tm, mode, state, errno, ddt);

   if (state > 0) {
      return(1);
   } else if (state == 0) {
      return(0);
   }

   return(errno);
}

//*********************************************************************
// my_read
//*********************************************************************

apr_size_t my_read(network_sock_t *sock, tbuffer_t *buf, apr_size_t pos, apr_size_t len, apr_size_t *count)
{
   apr_size_t n;
   sock_apr_overlay_t *s = (sock_apr_overlay_t *)(sock->fd);
   tbuffer_var_t tbv;

int leni, ni, nbi;

   tbuffer_var_init(&tbv);

   do {
      tbv.nbytes = len;
      tbuffer_next(buf, pos, &tbv);
      if (tbv.n_iov > IOV_MAX) tbv.n_iov = IOV_MAX;  //** Make sure we don't have to many entries
      n = readv(s->fd, tbv.buffer, tbv.n_iov);
leni=tbv.buffer[0].iov_len;
ni = n;
nbi = tbv.nbytes;
log_printf(5, "my_read: s->fd=%d  readv()=%d errno=%d nio=%d iov[0].len=%d nbytes=%d\n", s->fd, ni, errno, tbv.n_iov, leni, nbi);
   } while ((n==-1) && (errno==EINTR));

//log_printf(10, "my_read: fd=%d errno=%d n=" I64T "\n", s->fd, errno, n);

   if (n==-1) {
      *count = 0;
      if (errno == EAGAIN) {
         return(APR_TIMEUP);
      } else {
         return(errno);
      }
   }

   *count = n;
   if (n == 0) return(APR_TIMEUP);

   return(APR_SUCCESS);
}

//*********************************************************************
// my_write
//*********************************************************************

apr_size_t my_write(network_sock_t *sock, tbuffer_t *buf, apr_size_t bpos, apr_size_t len, apr_size_t *count)
{
   size_t n;
   sock_apr_overlay_t *s = (sock_apr_overlay_t *)(sock->fd);
   tbuffer_var_t tbv;

int leni, ni, len2;
apr_time_t start = apr_time_now();
double dt;

   tbuffer_var_init(&tbv);
//ni=bpos;
//log_printf(15, "START bpos=%d\n", ni);

   do {
//ni = bpos;
//log_printf(15, "before tbuffer_next call bpos=%d\n", ni);
      tbv.nbytes = len;
//len2 = 1024*1024;
//if (len > len2) tbv.nbytes = len2;
      tbuffer_next(buf, bpos, &tbv);
//leni=len;
//len2 = tbv.nbytes;
//ni=tbv.n_iov;
//log_printf(15, "s->fd=%d requested=%d got tbv.nbytes=%d tbv.n_iov=%d\n", s->fd, leni, len2, ni); flush_log();
      if (tbv.n_iov > IOV_MAX) tbv.n_iov = IOV_MAX;  //** Make sure we don't have to many entries
      n = writev(s->fd, tbv.buffer, tbv.n_iov);
leni=tbv.buffer->iov_len;
ni = n;
log_printf(5, "s->fd=%d  writev()=%d errno=%d nio=%d iov[0].len=%d\n", s->fd, ni, errno, tbv.n_iov, leni);
   } while ((n==-1) && (errno==EINTR));

dt = apr_time_now() - start;
dt /= (1.0*APR_USEC_PER_SEC);
log_printf(5, "dt=%lf\n", dt);
//log_printf(15, "BOTTOM\n");

   if (n==-1) {
      *count = 0;
      if (errno == EAGAIN) {
         return(APR_TIMEUP);
      } else {
         return(errno);
      }
   }

   *count = n;
   if (n == 0) return(APR_TIMEUP);

   return(APR_SUCCESS);
}


//*********************************************************************
//  sock_write
//*********************************************************************

long int sock_write(net_sock_t *nsock, tbuffer_t *buf, size_t bpos, size_t len, Net_timeout_t tm)
{
  int err, ewait; // eno;
  apr_size_t nbytes;
//  Net_timeout_t end_time;
  network_sock_t *sock = (network_sock_t *)nsock;

//if (sock == NULL) log_printf(15, "sock_write: sock == NULL\n");

  if (sock == NULL) return(-1);   //** If closed return
  if (sock->fd == NULL) return(-1);

//  end_time = apr_time_now() + tm;

  err = my_write(sock, buf, bpos, len, &nbytes);
  if (err != APR_SUCCESS) {
     ewait = sock_io_wait(sock, tm, SOCK_WAIT_WRITE);
     err = my_write(sock, buf, bpos, len, &nbytes);
     if ((ewait == 1) && (nbytes < 1)) nbytes = -1;
  }

//eno = errno;
//if (nbytes == -1) log_printf(4, "sock_write: count=" ST " nbytes=%ld err=%d errno=%d\n", count, nbytes, err, eno);
//log_printf(15, "sock_write: count=" ST " nbytes=%ld\n", count, nbytes);
  return(nbytes);
}

//*********************************************************************
//  sock_read
//*********************************************************************

long int sock_read(net_sock_t *nsock, tbuffer_t *buf, size_t bpos, size_t len, Net_timeout_t tm)
{
  int err, ewait; // eno;
  apr_size_t nbytes;
//  Net_timeout_t end_time;
  network_sock_t *sock = (network_sock_t *)nsock;

  if (sock == NULL) return(-1);   //** If closed return
  if (sock->fd == NULL) return(-1);

//  end_time = apr_time_now() + tm;

  err = my_read(sock, buf, bpos, len, &nbytes);
  if (err != APR_SUCCESS) {
     ewait = sock_io_wait(sock, tm, SOCK_WAIT_READ);
     err = my_read(sock, buf, bpos, len, &nbytes);
//log_printf(10, "sock_read: ewait=%d err=%d nbytes=" I64T "\n", ewait, err, nbytes);
     if ((ewait == 1) && (nbytes < 1)) nbytes = -1;
  }

//eno = errno;
//if (nbytes == -1) log_printf(4, "sock_read: count=" ST " nbytes=%ld err=%d errno=%d\n", count, nbytes, err, errno);
//log_printf(15, "sock_read: count=" ST " nbytes=%ld err=%d\n", count, nbytes, err);
  return(nbytes);
}

//*********************************************************************
// sock_connect - Creates a connection to a remote host
//*********************************************************************

int sock_connect(net_sock_t *nsock, const char *hostname, int port, Net_timeout_t timeout)
{
   int err;
   Net_timeout_t tm;

   network_sock_t *sock = (network_sock_t *)nsock;

   if (sock == NULL) return(-1);   //** If NULL exit

   if (sock->fd != NULL) apr_socket_close(sock->fd);

   sock->fd = NULL;
   sock->sa = NULL;

//log_printf(0, " sock_connect: hostname=%s:%d\n", hostname, port);
//   err = apr_sockaddr_info_get(&(sock->sa), hostname, APR_INET, port, APR_IPV4_ADDR_OK, sock->mpool);
   err = apr_sockaddr_info_get(&(sock->sa), hostname, APR_INET, port, 0, sock->mpool);
//log_printf(0, "sock_connect: apr_sockaddr_info_get: err=%d\n", err);
//if (sock->sa == NULL) log_printf(0, "sock_connect: apr_sockaddr_info_get: sock->sa == NULL\n");

   if (err != APR_SUCCESS) return(err);

   err = apr_socket_create(&(sock->fd), APR_INET, SOCK_STREAM, APR_PROTO_TCP, sock->mpool);
//log_printf(0, "sock_connect: apr_sockcreate: err=%d\n", err);
   if (err != APR_SUCCESS) return(err);


////   apr_socket_opt_set(sock->fd, APR_SO_NONBLCK, 1);
   apr_socket_timeout_set(sock->fd, timeout);
   if (sock->tcpsize > 0) {
      apr_socket_opt_set(sock->fd, APR_SO_SNDBUF, sock->tcpsize);
      apr_socket_opt_set(sock->fd, APR_SO_RCVBUF, sock->tcpsize);
   }

   err = apr_socket_connect(sock->fd, sock->sa);
//log_printf(0, "sock_connect: apr_socket_connect: err=%d\n", err); flush_log();

   //** Set a 50ms timeout
   set_net_timeout(&tm, 0, SOCK_DEFAULT_TIMEOUT);
   apr_socket_timeout_set(sock->fd, tm);

   return(err);
}

//*********************************************************************
// sock_connection_request - Waits for a connection request or times out 
//     If a request is made then 1 is returned otherwise 0 for timeout.
//     -1 signifies an error.
//*********************************************************************

int sock_connection_request(net_sock_t *nsock, int timeout)
{
  apr_int32_t n;
  apr_interval_time_t dt;
  const apr_pollfd_t *ret_fd;

  network_sock_t *sock = (network_sock_t *)nsock;

//dt= apr_time_make(0, 100*1000);
//apr_sleep(dt);

  if (sock == NULL) return(-1);
  dt = apr_time_make(timeout,0);
  n = 0;
  apr_pollset_poll(sock->pollset, dt, &n, &ret_fd);
//int i=n;
//log_printf(15, "sock_connection_request: err=%d n=%d APR_SUCCESS=%d\n", err, i, APR_SUCCESS);
  if (n == 1) {
     return(1);
  } else {
     return(0);
  }
  return(-1);
}

//*********************************************************************
//  sock_accept - Accepts a socket request
//*********************************************************************

net_sock_t *sock_accept(net_sock_t *nsock)
{
  int err;
  Net_timeout_t tm;
  network_sock_t *psock = (network_sock_t *)nsock;   

  network_sock_t *sock = (network_sock_t *)malloc(sizeof(network_sock_t));
  assert(sock != NULL);
  memset(sock, 0, sizeof(network_sock_t));

  if (apr_pool_create(&(sock->mpool), NULL) != APR_SUCCESS) {
     free(sock);
     return(NULL);
  }


  sock->tcpsize = psock->tcpsize;

  err = apr_socket_accept(&(sock->fd), psock->fd, sock->mpool);
  if (err != APR_SUCCESS) {
     apr_pool_destroy(sock->mpool);
     free(sock);
     sock = NULL;
     log_printf(0, "ERROR with apr_socket_accept err=%d\n", err);
  } else {
    apr_thread_mutex_create(&(sock->lock), APR_THREAD_MUTEX_DEFAULT,sock->mpool);

    //** Set the with a minimal timeout of 10ms
    set_net_timeout(&tm, 0, SOCK_DEFAULT_TIMEOUT);
    apr_socket_timeout_set(sock->fd, tm);
  }

  return(sock);    
}

//*********************************************************************
//  sock_bind - Binds a socket to the requested port
//*********************************************************************

int sock_bind(net_sock_t *nsock, char *address, int port)
{
  int err;
  network_sock_t *sock = (network_sock_t *)nsock;   

  if (sock == NULL) return(1);

log_printf(10, "sock_bind: host=%s:%d\n", address, port);

//   err = apr_sockaddr_info_get(&(sock->sa), address, APR_INET, port, APR_IPV4_ADDR_OK, sock->mpool);
   err = apr_sockaddr_info_get(&(sock->sa), address, APR_INET, port, 0, sock->mpool);
//log_printf(10, "sock_bind: apr_sockaddr_info_get=%d APR_SUCCESS=%d\n", err, APR_SUCCESS);
   if (err != APR_SUCCESS) return(err);
   
   err = apr_socket_create(&(sock->fd), APR_INET, SOCK_STREAM, APR_PROTO_TCP, sock->mpool);
//log_printf(10, "sock_bind: apr_socket_create=%d APR_SUCCESS=%d\n", err, APR_SUCCESS);
   if (err != APR_SUCCESS) return(err);

   apr_socket_opt_set(sock->fd, APR_SO_NONBLOCK, 1);
   apr_socket_opt_set(sock->fd, APR_SO_REUSEADDR, 1);
//log_printf(10, "sock_bind: apr_socket_opt_set=%d APR_SUCCESS=%d\n", err, APR_SUCCESS);
   if (sock->tcpsize > 0) {
      apr_socket_opt_set(sock->fd, APR_SO_SNDBUF, sock->tcpsize);
      apr_socket_opt_set(sock->fd, APR_SO_RCVBUF, sock->tcpsize);
   }

  err = apr_socket_bind(sock->fd, sock->sa);
log_printf(10, "sock_bind: apr_socket_bind=%d APR_SUCCESS=%d\n", err, APR_SUCCESS);

  return(err);
}

//*********************************************************************
//  sock_listen
//*********************************************************************

int sock_listen(net_sock_t *nsock, int max_pending)
{
  int err;
  network_sock_t *sock = (network_sock_t *)nsock;   

  if (sock == NULL) return(1);

  err = apr_socket_listen(sock->fd, max_pending);
  if (err != APR_SUCCESS) return(err);

  //** Create the polling info
  apr_pollset_create(&(sock->pollset), 1, sock->mpool, APR_POLLSET_THREADSAFE);
//  sock->pfd = { sock->mpool, APR_POLL_SOCKET, APR_POLLIN, 0, { NULL }, NULL };
  sock->pfd.p = sock->mpool;
  sock->pfd.desc_type = APR_POLL_SOCKET;
  sock->pfd.reqevents = APR_POLLIN;
  sock->pfd.rtnevents = 0;
  sock->pfd.desc.s = sock->fd;
  sock->pfd.client_data = NULL;

  apr_pollset_add(sock->pollset, &(sock->pfd));

  return(0);
}

//********************************************************************* 
// ns_config_sock - Configure the connection to use standard sockets 
//*********************************************************************

int sock_native_fd(net_sock_t *nsock) 
{
  network_sock_t *sock = (network_sock_t *)nsock;   
  int fd;

  if (apr_os_sock_get(&fd, sock->fd) != APR_SUCCESS) fd = -1;

  return(fd);
}


//********************************************************************* 
// ns_config_sock - Configure the connection to use standard sockets 
//*********************************************************************

void ns_config_sock(NetStream_t *ns, int tcpsize) {
  log_printf(10, "ns_config_sock: ns=%d, \n", ns->id);

  _ns_init(ns, 0);

  ns->sock_type = NS_TYPE_SOCK;
  network_sock_t *sock = (network_sock_t *)malloc(sizeof(network_sock_t));
  assert(sock != NULL);
  memset(sock, 0, sizeof(network_sock_t));
  ns->sock = (net_sock_t *)sock;
//  assert(apr_pool_create(&(sock->mpool), NULL) != APR_SUCCESS);
int err = apr_pool_create(&(sock->mpool), NULL);
if (err != APR_SUCCESS) {
  log_printf(0, "ns_config_sock:  apr_pool_crete error = %d\n", err); flush_log();
  assert(err == APR_SUCCESS);
}

 //**QWERT
 apr_thread_mutex_create(&(sock->lock), APR_THREAD_MUTEX_DEFAULT,sock->mpool);
 //**QWERTY
  
  sock->tcpsize = tcpsize;
  ns->native_fd = sock_native_fd;
  ns->connect = sock_connect;
  ns->sock_status = sock_status;
  ns->set_peer = sock_set_peer;
  ns->close = sock_close;
  ns->read = sock_read;
  ns->write = sock_write;
  ns->accept = sock_accept;
  ns->bind = sock_bind;
  ns->listen = sock_listen;
  ns->connection_request = sock_connection_request;
}

