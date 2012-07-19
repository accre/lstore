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

#ifndef __NET_PHOEBUS_H_
#define __NET_PHOEBUS_H_

#include "toolbox_config.h"

#define N_BUFSIZE  1024

typedef struct {
   phoebus_t *p_path;
   liblslSess *sess;
   int family;
   int fd;
   int tcpsize;
   char address[16];
} network_phoebus_t;

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

void ns_config_phoebus(NetStream_t *ns, phoebus_t *path, int tcpsize);

#ifdef __cplusplus
}
#endif

#endif

