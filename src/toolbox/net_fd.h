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

#ifndef __NET_FD_H_
#define __NET_FD_H_

#define N_BUFSIZE  1024

#include "tbx/toolbox_visibility.h"
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

