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

//*********************************************************************
//*********************************************************************

#define _log_module_index 120

#include <apr.h>
#include <apr_errno.h>
#include <apr_network_io.h>
#include <apr_poll.h>
#include <apr_portable.h>
#include <apr_time.h>
#include <limits.h>
#include <poll.h>
#include <string.h>
#include <stdlib.h>
#include <sys/errno.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/uio.h>

#include "net_sock.h"
#include "network.h"
#include "tbx/assert_result.h"
#include "tbx/fmttypes.h"
#include "tbx/log.h"
#include "transfer_buffer.h"

typedef struct {
    apr_pool_t *pool;
    int fd;
} sock_apr_overlay_t;
//typedef union {
//  apr_socket_t sock;
//  sock_apr_overlay_t overlay;
//} sock_apr_union_t;

//#define SOCK_DEFAULT_TIMEOUT 1000*1000
#define SOCK_DEFAULT_TIMEOUT (100*1000)
#define SOCK_WAIT_READ  POLLIN
#define SOCK_WAIT_WRITE POLLOUT

//*********************************************************************
// sock_set_peer - Gets the remote sockets hostname
//*********************************************************************

void sock_set_peer(net_sock_t *nsock, char *address, int add_size)
{
    tbx_net_sock_t *sock = (tbx_net_sock_t *)nsock;
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
    tbx_net_sock_t *sock = (tbx_net_sock_t *)nsock;
    if (sock == NULL) return(0);

    return(1);
}

//*********************************************************************
//  sock_close - Base socket close call
//*********************************************************************

int sock_close(net_sock_t *nsock)
{
    tbx_net_sock_t *sock = (tbx_net_sock_t *)nsock;

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

int sock_io_wait(tbx_net_sock_t *sock, tbx_ns_timeout_t tm, int mode)
{
    struct pollfd pfd;
    int state, dt;
    sock_apr_overlay_t *u = (sock_apr_overlay_t *)(sock->fd);

    apr_time_t start = apr_time_now();
    double ddt;
//log_printf(10, "sock_io_wait: START tm=" TT " mode=%d\n", tm, mode);  tbx_log_flush();

    dt = tm / 1000;
    if (dt == 0) dt = 1;
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

apr_size_t my_read(tbx_net_sock_t *sock, tbx_tbuf_t *buf, apr_size_t pos, apr_size_t len, apr_size_t *count)
{
    ssize_t n;
    sock_apr_overlay_t *s = (sock_apr_overlay_t *)(sock->fd);
    tbx_tbuf_var_t tbv;

    int leni, ni, nbi;

    tbx_tbuf_var_init(&tbv);

    do {
        tbv.nbytes = len;
        tbx_tbuf_next(buf, pos, &tbv);
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

apr_size_t my_write(tbx_net_sock_t *sock, tbx_tbuf_t *buf, apr_size_t bpos, apr_size_t len, apr_size_t *count)
{
    ssize_t n;
    sock_apr_overlay_t *s = (sock_apr_overlay_t *)(sock->fd);
    tbx_tbuf_var_t tbv;

    int leni, ni;
    apr_time_t start = apr_time_now();
    double dt;

    tbx_tbuf_var_init(&tbv);
//ni=bpos;
//log_printf(15, "START bpos=%d\n", ni);

    do {
//ni = bpos;
//log_printf(15, "before tbuffer_next call bpos=%d\n", ni);
        tbv.nbytes = len;
//len2 = 1024*1024;
//if (len > len2) tbv.nbytes = len2;
        tbx_tbuf_next(buf, bpos, &tbv);
//leni=len;
//len2 = tbv.nbytes;
//ni=tbv.n_iov;
//log_printf(15, "s->fd=%d requested=%d got tbv.nbytes=%d tbv.n_iov=%d\n", s->fd, leni, len2, ni); tbx_log_flush();
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

long int sock_write(net_sock_t *nsock, tbx_tbuf_t *buf, size_t bpos, size_t len, tbx_ns_timeout_t tm)
{
    int err, ewait; // eno;
    apr_size_t nbytes;
//  tbx_ns_timeout_t end_time;
    tbx_net_sock_t *sock = (tbx_net_sock_t *)nsock;

//if (sock == NULL) log_printf(15, "sock_write: sock == NULL\n");

    if (sock == NULL) return(-1);   //** If closed return
    if (sock->fd == NULL) return(-1);

//  end_time = apr_time_now() + tm;

    err = my_write(sock, buf, bpos, len, &nbytes);
    if (err != APR_SUCCESS) {
        ewait = sock_io_wait(sock, tm, SOCK_WAIT_WRITE);
        my_write(sock, buf, bpos, len, &nbytes);
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

long int sock_read(net_sock_t *nsock, tbx_tbuf_t *buf, size_t bpos, size_t len, tbx_ns_timeout_t tm)
{
    int err, ewait; // eno;
    apr_size_t nbytes;
//  tbx_ns_timeout_t end_time;
    tbx_net_sock_t *sock = (tbx_net_sock_t *)nsock;

    if (sock == NULL) return(-1);   //** If closed return
    if (sock->fd == NULL) return(-1);

//  end_time = apr_time_now() + tm;

    err = my_read(sock, buf, bpos, len, &nbytes);
    if (err != APR_SUCCESS) {
        ewait = sock_io_wait(sock, tm, SOCK_WAIT_READ);
        my_read(sock, buf, bpos, len, &nbytes);
//log_printf(10, "sock_read: ewait=%d err=%d nbytes=" I64T "\n", ewait, err, nbytes);
        if ((ewait == 1) && (nbytes < 1)) nbytes = -1;
    }

//eno = errno;
//if (nbytes == -1) log_printf(4, "sock_read: count=" ST " nbytes=%ld err=%d errno=%d\n", count, nbytes, err, errno);
//log_printf(15, "sock_read: count=" ST " nbytes=%ld err=%d\n", count, nbytes, err);
    return(nbytes);
}


//*********************************************************************
//  sock_apr_read
//*********************************************************************
apr_status_t my_apr_socket_recvv(apr_socket_t * sock, const struct iovec *vec,
                              apr_int32_t nvec, apr_size_t *len)
{
#ifdef HAVE_WRITEV
    apr_ssize_t rv;
    apr_size_t requested_len = 0;
    apr_int32_t i;

    for (i = 0; i < nvec; i++) {
        requested_len += vec[i].iov_len;
    }

    do {
        rv = readv(sock->socketdes, vec, nvec);
    } while (rv == -1 && errno == EINTR);

    while ((rv == -1) && (errno == EAGAIN || errno == EWOULDBLOCK)
                      && (sock->timeout > 0)) {
        apr_status_t arv;
do_select:
        arv = apr_wait_for_io_or_timeout(NULL, sock, 1);
        if (arv != APR_SUCCESS) {
            *len = 0;
            return arv;
        }
        else {
            do {
                rv = readv(sock->socketdes, vec, nvec);
            } while (rv == -1 && errno == EINTR);
        }
    }
    if (rv == -1) {
        *len = 0;
        return errno;
    }
    (*len) = rv;
    return APR_SUCCESS;
#else
    *len = vec[0].iov_len;
    return apr_socket_recv(sock, vec[0].iov_base, len);
#endif
}


long int sock_apr_read(net_sock_t *nsock, tbx_tbuf_t *buf, size_t bpos, size_t len, tbx_ns_timeout_t tm)
{
    int err;
    apr_size_t nbytes;
    tbx_tbuf_var_t tbv;
    tbx_net_sock_t *sock = (tbx_net_sock_t *)nsock;

    if (sock == NULL) return(-1);   //** If closed return
    if (sock->fd == NULL) return(-1);

    tbx_tbuf_var_init(&tbv);
    tbv.nbytes = len;
    tbx_tbuf_next(buf, bpos, &tbv);
    if (tbv.n_iov > IOV_MAX) tbv.n_iov = IOV_MAX;  //** Make sure we don't have to many entries

    err = my_apr_socket_recvv(sock->fd, tbv.buffer, tbv.n_iov, &nbytes);
    log_printf(5, "apr_socket_recvv=%d nbytes=%lu APR_SUCCESS=%d APR_TIMEUP=%d\n", err, nbytes, APR_SUCCESS, APR_TIMEUP);

    if (err == APR_SUCCESS) {
        if (nbytes == 0) nbytes = -1;  //** Dead connection
    } else if (err == APR_TIMEUP) {   //** Try again
        nbytes = 0;
    } else {                          //** Generic error
        nbytes = -1;
    }

    return(nbytes);
}


//*********************************************************************
//  sock_apr_write
//*********************************************************************

long int sock_apr_write(net_sock_t *nsock, tbx_tbuf_t *buf, size_t bpos, size_t len, tbx_ns_timeout_t tm)
{
    int err;
    apr_size_t nbytes;
    tbx_tbuf_var_t tbv;
    tbx_net_sock_t *sock = (tbx_net_sock_t *)nsock;

    if (sock == NULL) return(-1);   //** If closed return
    if (sock->fd == NULL) return(-1);

    tbx_tbuf_var_init(&tbv);
    tbv.nbytes = len;
    tbx_tbuf_next(buf, bpos, &tbv);
    if (tbv.n_iov > IOV_MAX) tbv.n_iov = IOV_MAX;  //** Make sure we don't have to many entries

    err = apr_socket_sendv(sock->fd, tbv.buffer, tbv.n_iov, &nbytes);
    log_printf(5, "apr_socket_sendv=%d nbytes=%lu APR_SUCCESS=%d APR_TIMEUP=%d\n", err, nbytes, APR_SUCCESS, APR_TIMEUP);


    if (err == APR_SUCCESS) {
        if (nbytes == 0) nbytes = -1;  //** Dead connection
    } else if (err == APR_TIMEUP) {   //** Try again
        nbytes = 0;
    } else {                          //** Generic error
        nbytes = -1;
    }

    return(nbytes);
}

//*********************************************************************
// sock_connect - Creates a connection to a remote host
//*********************************************************************

int sock_connect(net_sock_t *nsock, const char *hostname, int port, tbx_ns_timeout_t timeout)
{
    int err;
    tbx_ns_timeout_t tm;

    tbx_net_sock_t *sock = (tbx_net_sock_t *)nsock;

    if (sock == NULL) return(-1);   //** If NULL exit

    if (sock->fd != NULL) apr_socket_close(sock->fd);

    sock->fd = NULL;
    sock->sa = NULL;

    log_printf(0, " sock_connect: hostname=%s:%d to=" TT "\n", hostname, port, timeout);
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
//log_printf(0, "sock_connect: apr_socket_connect: err=%d\n", err); tbx_log_flush();

    //** Set a default timeout
    tbx_ns_timeout_set(&tm, 0, SOCK_DEFAULT_TIMEOUT);
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

    tbx_net_sock_t *sock = (tbx_net_sock_t *)nsock;

    if (sock == NULL) return(-1);
    dt = apr_time_make(timeout,0);
    n = 0;
    apr_pollset_poll(sock->pollset, dt, &n, &ret_fd);
    if (n == 1) {
        return(1);
    } else {
        return(0);
    }
}

//*********************************************************************
//  sock_accept - Accepts a socket request
//*********************************************************************

net_sock_t *sock_accept(net_sock_t *nsock)
{
    int err;
    tbx_ns_timeout_t tm;
    tbx_net_sock_t *psock = (tbx_net_sock_t *)nsock;

    tbx_net_sock_t *sock = (tbx_net_sock_t *)malloc(sizeof(tbx_net_sock_t));
   FATAL_UNLESS(sock != NULL);
    memset(sock, 0, sizeof(tbx_net_sock_t));

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
        tbx_ns_timeout_set(&tm, 0, SOCK_DEFAULT_TIMEOUT);
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
    tbx_net_sock_t *sock = (tbx_net_sock_t *)nsock;

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
    tbx_net_sock_t *sock = (tbx_net_sock_t *)nsock;

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
    tbx_net_sock_t *sock = (tbx_net_sock_t *)nsock;
    int fd;

    if (apr_os_sock_get(&fd, sock->fd) != APR_SUCCESS) fd = -1;

    return(fd);
}


//*********************************************************************
// ns_config_sock - Configure the connection to use standard sockets
//*********************************************************************

void tbx_ns_sock_config(tbx_ns_t *ns, int tcpsize)
{
    log_printf(10, "ns_config_sock: ns=%d, \n", ns->id);

    _ns_init(ns, 0);

    ns->sock_type = NS_TYPE_SOCK;
    tbx_net_sock_t *sock = (tbx_net_sock_t *)malloc(sizeof(tbx_net_sock_t));
   FATAL_UNLESS(sock != NULL);
    memset(sock, 0, sizeof(tbx_net_sock_t));
    ns->sock = (tbx_net_sock_t *)sock;
//  assert_result_not(apr_pool_create(&(sock->mpool), NULL), APR_SUCCESS);
    int err = apr_pool_create(&(sock->mpool), NULL);
    if (err != APR_SUCCESS) {
        log_printf(0, "ns_config_sock:  apr_pool_crete error = %d\n", err);
        tbx_log_flush();
       FATAL_UNLESS(err == APR_SUCCESS);
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
//  ns->read = sock_read;
//  ns->write = sock_write;
    ns->read = sock_apr_read;
    ns->write = sock_apr_write;
    ns->accept = sock_accept;
    ns->bind = sock_bind;
    ns->listen = sock_listen;
    ns->connection_request = sock_connection_request;
}

