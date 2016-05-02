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

#ifndef __NET_2_SSL_H_
#define __NET_2_SSL_H_

#include "tbx/toolbox_visibility.h"
#define N_BUFSIZE  1024

//#include <sys/select.h>
//#include <sys/time.h>
//#include <pthread.h>
#include "network.h"

typedef struct {
//  pthread_mutex_t lock;
    int rfd;
    int wfd;
} tbx_net_2ssl_t;

#ifdef __cplusplus
extern "C" {
#endif

int d_ssl_close(net_sock_t *sock);
long int d_ssl_write(net_sock_t *sock, const void *buf, size_t count, Net_timeout_t tm);
long int d_ssl_read(net_sock_t *sock, void *buf, size_t count, Net_timeout_t tm);
int ns_merge_ssl(tbx_ns_t *ns1, tbx_ns_t *ns2);

#ifdef __cplusplus
}
#endif

#endif

