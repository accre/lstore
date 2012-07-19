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

#ifndef __NET_2_SSL_H_
#define __NET_2_SSL_H_

#define N_BUFSIZE  1024

//#include <sys/select.h>
//#include <sys/time.h>
//#include <pthread.h>
#include "network.h"

typedef struct {
//  pthread_mutex_t lock;
  int rfd;
  int wfd;
} network_2ssl_t;

#ifdef __cplusplus
extern "C" {
#endif

int d_ssl_close(net_sock_t *sock);
long int d_ssl_write(net_sock_t *sock, const void *buf, size_t count, Net_timeout_t tm);
long int d_ssl_read(net_sock_t *sock, void *buf, size_t count, Net_timeout_t tm);
int ns_merge_ssl(NetStream_t *ns1, NetStream_t *ns2);

#ifdef __cplusplus
}
#endif

#endif

