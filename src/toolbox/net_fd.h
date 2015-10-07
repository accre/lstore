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

#ifndef __NET_FD_H_
#define __NET_FD_H_

#define N_BUFSIZE  1024

#include <sys/select.h>
#include <sys/time.h>
#include <pthread.h>
#include "network.h"

#ifdef __cplusplus
extern "C" {
#endif

int fd_connection_request(int fd, int timeout);
int fd_accept(int monfd);
int fd_bind(char *address, int port);
int fd_listen(int fd, int max_pending);
void fd_set_peer(int fd, char *address, int add_size);
int fd_status(int fd);
int fd_close(int fd);
long int fd_write(int fd, const void *buf, size_t count, Net_timeout_t tm);
long int fd_read(int fd, void *buf, size_t count, Net_timeout_t tm);
int fd_connect(int *fd, const char *hostname, int port, int tcpsize, Net_timeout_t timeout);

#ifdef __cplusplus
}
#endif

#endif

