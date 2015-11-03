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

#define _log_module_index 118

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
#include "assert_result.h"
#include <signal.h>
#include <errno.h>
#include <fcntl.h>
#include <apr_time.h>
#include "network.h"
#include "net_fd.h"
#include "debug.h"
#include "log.h"
#include "dns_cache.h"
#include "fmttypes.h"

//*********************************************************************
// set_timeval - Initializes the timeval data structure
//*********************************************************************

struct timeval *set_timeval(struct timeval *tv, Net_timeout_t tm)
{
    tv->tv_sec = tm / 1000000;
    tv->tv_usec = tm % 1000000;

    return(tv);
}

//*********************************************************************
// fd_connection_request - Waits for a connection request or times out
//     If a request is made then 1 is returned otherwise 0 for timeout.
//     -1 signifies an error.
//*********************************************************************

int fd_connection_request(int fd, int timeout)
{
    struct timeval dt;
    fd_set rfd;

    if (fd == -1) return(-1);

    dt.tv_sec = timeout;
    dt.tv_usec = 0;
    FD_ZERO(&rfd);
    FD_SET(fd, &rfd);
    return(select(fd+1, &rfd, NULL, NULL, &dt));
}

//*********************************************************************
// fd_accept - Accepts a socket connection request
//*********************************************************************

int fd_accept(int monfd)
{
    int fd, i;
    struct sockaddr addr;
    socklen_t len = sizeof(addr);
    memset(&addr, 0, sizeof(addr));

    fd = accept(monfd, &addr, &len);
    if (fd == -1) {
        log_printf(0, "fd_accept: Accept error!");
        return(-1);
    }

    //Configure the socket for non-blocking I/O
    if ((i = fcntl(fd, F_SETFL, O_NONBLOCK)) == -1) {
        log_printf(0, "fd_accept: Can't configure connection for non-blocking I/O!");
    }

    return(fd);
}

//*********************************************************************
// fd_bind - Binds a socket to a port
//*********************************************************************

int fd_bind(char *address, int port)
{
    int              fd;
    struct addrinfo  hints;
    struct addrinfo *res;
    char             sport[12];
    int              flag, err;

    memset(&hints, 0, sizeof(hints));
    hints.ai_flags = AI_PASSIVE;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    hints.ai_family = AF_UNSPEC;

    memset(sport, 0, sizeof(sport));
    snprintf(sport, sizeof(sport)-1, "%d", port);
    if (getaddrinfo(address, sport, &hints, &res)) {
        log_printf(0, "fd_bind: getaddrinfo() error!\n");
        return(-1);
    }

    if ((fd = socket(res->ai_family, SOCK_STREAM, 0)) < 1) {
        if ((fd = socket(AF_INET, SOCK_STREAM, 0)) < 1) {
            log_printf(0, "fd_bind: socket() error!\n");
            return(-1);
        }
    }

    flag=1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char*)&flag, sizeof(int));
    //Configure the socket for non-blocking I/O
    if ((err = fcntl(fd, F_SETFL, O_NONBLOCK)) == -1) {
        log_printf(0, "fd_bind: Can't configure connection for non-blocking I/O!");
    }

    if((err = bind(fd, res->ai_addr, res->ai_addrlen)) == -1) {
        log_printf(0, "fd_bind: hostname: %s", address);
        log_printf(0, "fd_bind: %s", strerror(err));
        return(-1);
    }

    freeaddrinfo(res);

    return(fd);
}

//*********************************************************************
// fd_listen
//*********************************************************************

int fd_listen(int fd, int max_pending)
{
    return(listen(fd, max_pending));
}

//*********************************************************************
// fd_set_peer - Gets the remote sockets hostname
//*********************************************************************

void fd_set_peer(int fd, char *address, int add_size)
{
    union sock_u {
        struct sockaddr s;
        struct sockaddr_in i;
    };
    union sock_u psa;

    address[0] = '\0';

    socklen_t plen = sizeof(struct sockaddr);
    if (getpeername(fd, &(psa.s), &plen) != 0) {
        char errmsg[1024];
        strerror_r(errno, errmsg, sizeof(errmsg));
        log_printf(15, "fd_set_peer: Can't get the peers socket!  Error(%d):%s\n", errno,  errmsg);
        return;
    }

    inet_ntop(AF_INET, (void *)&(psa.i.sin_addr), address, add_size);
    address[add_size-1] = '\0';

    return;
}

//*********************************************************************
//  fd_status - Returns 1 if the socket is connected and 0 otherwise
//*********************************************************************

int fd_status(int fd)
{
    if (fd != -1) return(1);

    return(0);
}

//*********************************************************************
//  fd_close - Base socket close call
//*********************************************************************

int fd_close(int fd)
{
    if (fd == -1) return(0);

    log_printf(15, "fd_close: closing fd=%d\n", fd);

    int err = close(fd);

    return(err);
}

//*********************************************************************
//  fd_write
//*********************************************************************

long int fd_write(int fd, const void *buf, size_t count, Net_timeout_t tm)
{
    long int n;
    int err;
    fd_set wfd;
    struct timeval tv;

    if (fd == -1) return(-1);   //** If closed return

    n = write(fd, buf, count);
    log_printf(15, "fd_write: fd=%d n=%ld errno=%d\n", fd, n, errno);

    if ((n==-1) && ((errno == EAGAIN) || (errno == EINTR))) n = 0;

    if (n == 0) {  //** Nothing written to let's try and wait
        FD_ZERO(&wfd);
        FD_SET(fd, &wfd);

        set_timeval(&tv, tm);
        err = select(fd+1, NULL, &wfd, NULL, &tv);
        if (err > 0) {
            n = write(fd, buf, count);
            log_printf(15, "fd_write2: fd=%d n=%ld select=%d errno=%d\n", fd, n, err,errno);
            if (n == 0) {
                n = -1;        //** Dead connection
            } else if ((n==-1) && ((errno == EAGAIN) || (errno == EINTR))) {
                n = 0;     //** Try again later
            }
        }
    }

    return(n);
}

//*********************************************************************
//  fd_read
//*********************************************************************

long int fd_read(int fd, void *buf, size_t count, Net_timeout_t tm)
{
    long int n;
    int err;
    fd_set rfd;
    struct timeval tv;

    if (fd == -1) return(-1);   //** If closed return

    n = read(fd, buf, count);
    log_printf(15, "fd_read: fd=%d n=%ld errno=%d\n", fd, n, errno);
    if ((n==-1) && ((errno == EAGAIN) || (errno == EINTR))) n = 0;

    if (n == 0) {  //** Nothing written to let's try and wait
        FD_ZERO(&rfd);
        FD_SET(fd, &rfd);

        set_timeval(&tv, tm);
        err = select(fd+1, &rfd, NULL, NULL, &tv);
        if (err > 0) {
            n = read(fd, buf, count);
            log_printf(15, "fd_read2: fd=%d n=%ld select=%d errno=%d\n", fd, n, err,errno);
            if (n == 0) {
                n = -1;        //** Dead connection
            } else if ((n==-1) && ((errno == EAGAIN) || (errno == EINTR))) {
                n = 0;     //** Try again later
            }
        }
    }

    return(n);
}

//*********************************************************************
// fd_connect - Creates a connection to a remote host
//*********************************************************************

int fd_connect(int *fd, const char *hostname, int port, int tcpsize, Net_timeout_t timeout)
{
    struct sockaddr_in addr;
    char in_addr[6];
    int sfd, err;
    fd_set wfd;
    apr_time_t endtime;
    Net_timeout_t to;
    struct timeval tv;

    log_printf(15, "fd_connect: Trying to make connection to Hostname: %s  Port: %d\n", hostname, port);

    *fd = -1;
    if (hostname == NULL) {
        log_printf(15, "fd_connect: lookup_host failed.  Missing hostname!  Port: %d\n",port);
        return(1);
    }

    // Get ip address
    if (lookup_host(hostname, in_addr, NULL) != 0) {
        log_printf(15, "fd_connect: lookup_host failed.  Hostname: %s  Port: %d\n", hostname, port);
        return(1);
    }

    // get the socket
    sfd = socket(PF_INET, SOCK_STREAM, 0);
    *fd = sfd;
    if (sfd == -1) {
        log_printf(10, "fd_connect: socket open failed.  Hostname: %s  Port: %d\n", hostname, port);
        return(1);
    }

    // Configure it correctly
    int flag=1;
    if (setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, (char*)&flag, sizeof(flag)) != 0) {
        log_printf(0, "fd_connect: Can't configure SO_REUSEADDR!\n");
    }

    if (tcpsize > 0) {
        log_printf(10, "fd_connect: Setting tcpbufsize=%d\n", tcpsize);
        if (setsockopt(sfd, SOL_SOCKET, SO_SNDBUF, (char*)&tcpsize, sizeof(tcpsize)) != 0) {
            log_printf(0, "fd_connect: Can't configure SO_SNDBUF to %d!\n", tcpsize);
        }
        if (setsockopt(sfd, SOL_SOCKET, SO_RCVBUF, (char*)&tcpsize, sizeof(tcpsize)) != 0) {
            log_printf(0, "fd_connect: Can't configure SO_RCVBUF to %d!\n", tcpsize);
        }
    }

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    memcpy(&(addr.sin_addr), &in_addr, 4);
//   memset(&(addr.sin_zero), 0, 8);

    // Connect it
    err = connect(sfd, (struct sockaddr *)&addr, sizeof(struct sockaddr));
    if (err != 0) {
        if (errno != EINPROGRESS) {
            log_printf(0, "fd_connect: connect failed.  Hostname: %s  Port: %d err=%d errno: %d error: %s\n", hostname, port, err, errno, strerror(errno));
            return(1);
        }
//      err = 0;
    }

    //Configure the socket for non-blocking I/O
    if ((err = fcntl(sfd, F_SETFL, O_NONBLOCK)) == -1) {
        log_printf(0, "fd_connect: Can't configure connection for non-blocking I/O!");
    }


    log_printf(20, "fd_connect: Before select time=" TT "\n", apr_time_now());
    endtime = apr_time_now() + apr_time_make(5, 0);
    do {
        FD_ZERO(&wfd);
        FD_SET(sfd, &wfd);
        set_net_timeout(&to, 1, 0);
        set_timeval(&tv, to);
        err = select(sfd+1, NULL, &wfd, NULL, &tv);
        log_printf(20, "fd_connect: After select err=%d\n", err);
        if (err != 1) {
            if (errno != EINPROGRESS) {
                log_printf(0, "fd_connect: select failed.  Hostname: %s  Port: %d select=%d errno: %d error: %s\n", hostname, port, err, errno, strerror(errno));
                return(1);
            } else {
                log_printf(10, "fd_connect: In progress.....  time=" TT " Hostname: %s  Port: %d select=%d errno: %d error: %s\n", apr_time_now(), hostname, port, err, errno, strerror(errno));
            }
        }
    } while ((err != 1) && (apr_time_now() < endtime));

    if (err != 1) {   //** There were problems so return with an error
        log_printf(0, "fd_connect: Couldn\'t make connection.  Hostname: %s  Port: %d select=%d errno: %d error: %s\n", hostname, port, err, errno, strerror(errno));
        close(sfd);
        return(1);
    }

    *fd = sfd;

    return(0);
}

