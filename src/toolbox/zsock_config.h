/*
Advanced Computing Center for Research and Education Proprietary License
Version 1.0 (September 2012)

Copyright (c) 2012, Advanced Computing Center for Research and Education,
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

#ifndef _ZSOCK_CONFIG_H_
#define _ZSOCK_CONFIG_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "opque.h"
#include "network.h"
#include "net_zsock.h"
#include "host_portal.h"
#include "log.h"
#include "type_malloc.h"
#include "apr_wrapper.h"
#include "dns_cache.h"
#include "iniparse.h"

#define ZSOCK_MAX_NUM_CMDS 2 

typedef int64_t zsock_off_t;  //** Base ZSOCK offset/size data type

//** Holds data for zsock connection
typedef struct {
    int conn_type;  
    int sock_type;
//    int sock_bind_type;
    char *prtcl; //** Connection protocol
    void *arg;   //** Generic container for context data - ZMQ socket options 
} zsock_connect_context_t; 

typedef struct { //** Do I need other fields in ibp_context_t?
    int min_idle;        //** Connection minimum idle time before disconnecting
    int min_threads;     //** Min and max threads allowed to a depot
    int max_threads;     //** Max number of simultaneous connection to a depot
    int max_connections; //** Max number of connections across all connections
    int max_wait;         //** Max time to wait and retry a connection
    int wait_stable_time; //** Time to wait before opening a new connection for a heavily loaded depot
    int abort_conn_attempts; //** If this many failed connection requests occur in a row we abort
    int check_connection_interval;  //**# of secs to wait between checks if we need more connections to a depot
    int max_retry;        //** Max number of times to retry a command before failing.. only for dead socket retries

    zsock_connect_context_t cc[ZSOCK_MAX_NUM_CMDS]; //** Default connection contexts 
    portal_context_t *pc;
    int64_t max_workload;    //** Max workload allowed in a given connection
    apr_thread_mutex_t *lock;
    apr_pool_t *mpool;
    atomic_int_t n_ops;
} zsock_context_t;

void zsock_destroy_context(zsock_context_t *zc);
zsock_context_t *zsock_create_context();
void _zsock_submit_op(void *arg, op_generic_t *gop);
int _zsock_connect(NetStream_t *ns, void *connect_context, char *host, int port, Net_timeout_t timeout);
void _zsock_destroy_connect_context(void *connect_context);
void *_zsock_dup_connect_context(void *connect_context);
void default_zsock_config(zsock_context_t *zc);
void zsock_cc_load(inip_file_t *kf, zsock_context_t *cfg);
int zsock_load_config(zsock_context_t *zc, char *fname, char *section);
void copy_zsock_config(zsock_context_t *cfg);
int zsock_get_type(char *type);
void zsock_option_load(inip_file_t *kf, const char *group, zsocket_opt_t *option);

#ifdef __cplusplus
}
#endif

#endif
