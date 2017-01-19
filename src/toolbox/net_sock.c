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
#include <arpa/inet.h>
#include <limits.h>
#include <netdb.h>
#include <poll.h>
#include <string.h>
#include <stdlib.h>
#include <sys/errno.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include "net_sock.h"
#include "network.h"
#include "tbx/assert_result.h"
#include <tbx/dns_cache.h>
#include "tbx/fmttypes.h"
#include "tbx/log.h"
#include "transfer_buffer.h"
#include <tbx/type_malloc.h>

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
    socklen_t alen;
    struct sockaddr_in sa;

    address[0] = '\0';
    if (sock == NULL) return;
    if (sock->fd == -1) return;

    alen = sizeof(sa);
    if (getpeername(sock->fd, (struct sockaddr *)&sa, &alen) != 0) return;
    inet_ntop(AF_INET, &sa.sin_addr, address, add_size);

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

    if (sock->fd != -1) close(sock->fd);

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

    apr_time_t start = apr_time_now();
    double ddt;

    dt = tm / 1000;
    if (dt == 0) dt = 1;
    pfd.fd = sock->fd;
    pfd.events = mode;

    do {
        state = poll(&pfd, 1, dt);
    } while ((state == -1) && (errno == EINTR));

    ddt = apr_time_now() - start;
    ddt /= (1.0*APR_USEC_PER_SEC);
    log_printf(10, "sock_io_wait: fd=%d tm=" TT " mode=%d state=%d errno=%d dt=%lf\n", pfd.fd, tm, mode, state, errno, ddt);

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
    tbx_tbuf_var_t tbv;

    int leni, ni, nbi;

    tbx_tbuf_var_init(&tbv);

    do {
        tbv.nbytes = len;
        tbx_tbuf_next(buf, pos, &tbv);
        if (tbv.n_iov > IOV_MAX) tbv.n_iov = IOV_MAX;  //** Make sure we don't have to many entries
        n = readv(sock->fd, tbv.buffer, tbv.n_iov);
        leni=tbv.buffer[0].iov_len;
        ni = n;
        nbi = tbv.nbytes;
        log_printf(10, "my_read: s->fd=%d  readv()=%d errno=%d nio=%d iov[0].len=%d nbytes=%d\n", sock->fd, ni, errno, tbv.n_iov, leni, nbi);
    } while ((n==-1) && (errno==EINTR));

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
    tbx_tbuf_var_t tbv;

    int leni, ni;
    apr_time_t start = apr_time_now();
    double dt;

    tbx_tbuf_var_init(&tbv);

    do {
        tbv.nbytes = len;
        tbx_tbuf_next(buf, bpos, &tbv);
        if (tbv.n_iov > IOV_MAX) tbv.n_iov = IOV_MAX;  //** Make sure we don't have to many entries
        n = writev(sock->fd, tbv.buffer, tbv.n_iov);
        leni=tbv.buffer->iov_len;
        ni = n;
        log_printf(10, "s->fd=%d  writev()=%d errno=%d nio=%d iov[0].len=%d\n", sock->fd, ni, errno, tbv.n_iov, leni);
    } while ((n==-1) && (errno==EINTR));

    dt = apr_time_now() - start;
    dt /= (1.0*APR_USEC_PER_SEC);
    log_printf(10, "dt=%lf\n", dt);

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
    tbx_net_sock_t *sock = (tbx_net_sock_t *)nsock;

    if (sock == NULL) return(-1);   //** If closed return
    if (sock->fd == -1) return(-1);

    err = my_write(sock, buf, bpos, len, &nbytes);
    if (err != APR_SUCCESS) {
        ewait = sock_io_wait(sock, tm, SOCK_WAIT_WRITE);
        my_write(sock, buf, bpos, len, &nbytes);
        if ((ewait == 1) && (nbytes < 1)) nbytes = -1;
    }

    return(nbytes);
}

//*********************************************************************
//  sock_read
//*********************************************************************

long int sock_read(net_sock_t *nsock, tbx_tbuf_t *buf, size_t bpos, size_t len, tbx_ns_timeout_t tm)
{
    int err, ewait; // eno;
    apr_size_t nbytes;
    tbx_net_sock_t *sock = (tbx_net_sock_t *)nsock;

    if (sock == NULL) return(-1);   //** If closed return
    if (sock->fd == -1) return(-1);

    err = my_read(sock, buf, bpos, len, &nbytes);
    if (err != APR_SUCCESS) {
        ewait = sock_io_wait(sock, tm, SOCK_WAIT_READ);
        my_read(sock, buf, bpos, len, &nbytes);
        if ((ewait == 1) && (nbytes < 1)) nbytes = -1;
    }

    return(nbytes);
}

//*********************************************************************
// sock_timeout_set -Sets the socket timeout
//*********************************************************************

int sock_timeout_set(tbx_net_sock_t *sock, tbx_ns_timeout_t timeout)
{
    struct timeval tm;
    tm.tv_sec = apr_time_sec(timeout);
    tm.tv_usec = apr_time_usec(timeout);

    if (setsockopt(sock->fd, SOL_SOCKET, SO_RCVTIMEO, (char *)&tm, sizeof(timeout)) < 0) return(-1);
    if (setsockopt(sock->fd, SOL_SOCKET, SO_SNDTIMEO, (char *)&tm, sizeof(timeout)) < 0) return(-1);

    return(0);
}

//*********************************************************************
// sock_connect - Creates a connection to a remote host
//*********************************************************************

int sock_connect(net_sock_t *nsock, const char *hostname, int port, tbx_ns_timeout_t timeout)
{
    tbx_net_sock_t *sock = (tbx_net_sock_t *)nsock;
    tbx_ns_timeout_t tm;
    struct sockaddr_in sa;

    if (sock == NULL) return(-1);   //** If NULL exit

    if (sock->fd != -1) close(sock->fd);

    sock->fd = -1;

    log_printf(20, "hostname=%s:%d to=" TT "\n", hostname, port, timeout);

    if ((sock->fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) return(-1);

    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons(port);

    if (tbx_dnsc_lookup(hostname, (char *)&sa.sin_addr, NULL) != 0) goto fail;

    sock_timeout_set(sock, timeout);

    if (sock->tcpsize > 0) {
        if (setsockopt(sock->fd, SOL_SOCKET, SO_SNDBUF, (char *)&sock->tcpsize, sizeof(sock->tcpsize)) < 0) goto fail;
        if (setsockopt(sock->fd, SOL_SOCKET, SO_RCVBUF, (char *)&sock->tcpsize, sizeof(sock->tcpsize)) < 0) goto fail;
    }

    if (connect(sock->fd, &sa, sizeof(sa)) == -1) goto fail;

    tbx_ns_timeout_set(&tm, 0, SOCK_DEFAULT_TIMEOUT);
    sock_timeout_set(sock, tm);

    log_printf(20, "SUCCESS host=%s\n", hostname);
    return(0);

fail:
    log_printf(20, "FAIL host=%s errno=%d\n", hostname, errno);
    close(sock->fd);
    return(-1);
}

//*********************************************************************
// sock_connection_request - Waits for a connection request or times out
//     If a request is made then 1 is returned otherwise 0 for timeout.
//     -1 signifies an error.
//     timeout is in seconds
//*********************************************************************

int sock_connection_request(net_sock_t *nsock, int timeout)
{
    tbx_net_sock_t *sock = (tbx_net_sock_t *)nsock;
    struct pollfd pfd;

    if (sock == NULL) return(-1);

    pfd.fd = sock->fd;
    pfd.events = POLLIN;
    pfd.revents = 0;

    return(poll(&pfd, 1, timeout*1000));
}

//*********************************************************************
//  sock_accept - Accepts a socket request
//*********************************************************************

net_sock_t *sock_accept(net_sock_t *nsock)
{
    tbx_ns_timeout_t tm;
    tbx_net_sock_t *psock = (tbx_net_sock_t *)nsock;
    tbx_net_sock_t *sock;

    tbx_type_malloc_clear(sock, tbx_net_sock_t, 1);

    sock->tcpsize = psock->tcpsize;

    sock->fd = accept(psock->fd, NULL, NULL);

    if (sock->fd == -1) {
        free(sock);
        sock = NULL;
        log_printf(0, "ERROR with apr_socket_accept err=%d\n", errno);
    } else {
        //** Set the with a minimal timeout of 10ms
        tbx_ns_timeout_set(&tm, 0, SOCK_DEFAULT_TIMEOUT);
        sock_timeout_set(sock, tm);
    }

    return(sock);
}

//*********************************************************************
//  sock_bind - Binds a socket to the requested port
//*********************************************************************

int sock_bind(net_sock_t *nsock, char *address, int port)
{
    tbx_net_sock_t *sock = (tbx_net_sock_t *)nsock;
    int flags;
    struct sockaddr_in sa;

    if (sock == NULL) return(-1);   //** If NULL exit

    if (sock->fd != -1) close(sock->fd);

    sock->fd = -1;

    log_printf(10, "host=%s:%d\n", address, port);

    if ((sock->fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) return(-1);

    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons(port);

    if (tbx_dnsc_lookup(address, (char *)&sa.sin_addr, NULL) != 0) goto fail;

    flags = fcntl(sock->fd, F_GETFL, 0);
    if (flags < 0) goto fail;
    flags = flags|O_NONBLOCK;
    if (fcntl(sock->fd, F_SETFL, flags) == -1) goto fail;

    flags = 1;
    if (setsockopt(sock->fd, SOL_SOCKET, SO_REUSEADDR, &flags, sizeof(int)) < 0) goto fail;


    if (sock->tcpsize > 0) {
        if (setsockopt(sock->fd, SOL_SOCKET, SO_SNDBUF, (char *)&sock->tcpsize, sizeof(sock->tcpsize)) < 0) goto fail;
        if (setsockopt(sock->fd, SOL_SOCKET, SO_RCVBUF, (char *)&sock->tcpsize, sizeof(sock->tcpsize)) < 0) goto fail;
    }


    if (bind(sock->fd, &sa, sizeof(sa)) != 0) goto fail;

    return(0);

fail:
    close(sock->fd);
    return(-1);
}

//*********************************************************************
//  sock_listen
//*********************************************************************

int sock_listen(net_sock_t *nsock, int max_pending)
{
    tbx_net_sock_t *sock = (tbx_net_sock_t *)nsock;

    if (sock == NULL) return(1);

    if (listen(sock->fd, max_pending) != 0) return(-1);

    return(0);
}

//*********************************************************************
// ns_config_sock - Configure the connection to use standard sockets
//*********************************************************************

int sock_native_fd(net_sock_t *nsock)
{
    tbx_net_sock_t *sock = (tbx_net_sock_t *)nsock;

    return(sock->fd);
}


//*********************************************************************
// ns_config_sock - Configure the connection to use standard sockets
//*********************************************************************

void tbx_ns_sock_config(tbx_ns_t *ns, int tcpsize)
{
    tbx_net_sock_t *sock;

    log_printf(10, "ns_config_sock: ns=%d, \n", ns->id);

    _ns_init(ns, 0);

    ns->sock_type = NS_TYPE_SOCK;
    tbx_type_malloc_clear(sock, tbx_net_sock_t, 1);
    ns->sock = (tbx_net_sock_t *)sock;

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

