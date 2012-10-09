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

#ifndef _NET_ZMQ_H_
#define _NET_ZMQ_H_

#ifdef __cplusplus
extern "C" {
#endif
 
#include <zmq.h>
#include <czmq.h>
#include <unistd.h>
#include "network.h"
#include "log.h"
#include "type_malloc.h"
#include "string_token.h"

#define ZSOCK_CONNECT 0
#define ZSOCK_BIND 1

typedef void zsocket_t;

/*typedef struct {
    void *requester; // ZMQ_REQ
    void *responder; // ZMQ_REP
    void *publisher; // ZMQ_PUB
    void *subscriber;// ZMQ_SUB
    void *sender;    // ZMQ_PUSH 
    void *receiver;  // ZMQ_PULL
    void *router;    // ZMQ_ROUTER
    void *dealer;    // ZMQ_DEALER
} zsocket_t;
*/

//** Flags
enum {
    SNDHWM = 0x000001,
    RCVHWM = 0x000002,
    AFFINITY = 0x000004,
    RATE = 0x000008,
    RECOVERY_IVL = 0x000010,
    SNDBUF = 0x000020,
    RCVBUF = 0x000040,
    RECONNECT_IVL = 0x000080,
    RECONNECT_IVL_MAX = 0x000100,
    BACKLOG = 0x000200,
    MAXMSGSIZE = 0x000400,
    MULTICAST_HOPS = 0x000800,
    RCVTIMEO = 0x001000,
    SNDTIMEO = 0x002000,
    IPV4ONLY = 0x004000,
    ROUTER_BEHAVIOR = 0x008000,
    HWM = 0x010000,
    SUBSCRIBE = 0x020000,
    UNSUBSCRIBE = 0x040000,
    IDENTITY = 0x080000     
};

//** Contains zmq socket options, not all opts
typedef struct {
    unsigned long int flag; //** Flag to indicate which options needs to be set
    int sndhwm;
    int rcvhwm;
    int affinity;
    int rate;
    int recovery_ivl;
    int sndbuf;
    int rcvbuf;
    int reconnect_ivl;
    int reconnect_ivl_max;
    int backlog;
    int maxmsgsize;
    int multicast_hops;
    int rcvtimeo;
    int sndtimeo;
    int ipv4only;
    int router_behavior;
    int hwm;
    int sub_num; 	//** Number of subscribes
    int unsub_num; 	//** Number of unsubscribes
    char **subscribe; 	//** A list of subscribes
    char **unsubscribe; //** A list of unsubscribes
    char *identity;
} zsocket_opt_t;

//** Contains zmq network fields 
typedef struct { 
    zctx_t *context;
    zsocket_t *socket;
    int type; 			//** Socket type
    char *prtcl;		//** Protocol type
    int zmq_device; 		//** Built-in zmq device
    zsocket_opt_t *option;
    zmq_pollitem_t *items; 
    apr_pool_t *mpool;
    apr_thread_mutex_t *lock;
} network_zsock_t;

#define set_flag(f, b) ((f) |= (b))
#define unset_flag(f, b) ((f) &= ~(b))
#define check_flag(f, b) ((f) & (b))

int zsock_status(net_sock_t *sock);
int zsock_close(net_sock_t *sock);
void zsock_setopt(void *socket, zsocket_opt_t *option);
void zsock_set_peer(net_sock_t *sock, char *address, int add_size);
zmsg_t *zsock_decode(tbuffer_t *buf, size_t bpos, size_t size);
size_t zsock_encode(zmsg_t *msg, tbuffer_t *buf);
long int zsock_write(net_sock_t *sock, tbuffer_t *buf, size_t bpos, size_t size, Net_timeout_t tm);
long int zsock_read(net_sock_t *sock, tbuffer_t *buf, size_t bpos, size_t size, Net_timeout_t tm);
int _zsock_act(net_sock_t *sock, const char *hostname, int port, Net_timeout_t timeout, int action);
int zsock_connect(net_sock_t *sock, const char *hostname, int port, Net_timeout_t timeout);
int zsock_bind(net_sock_t *sock, char *hostname, int port);
void ns_config_zsock(NetStream_t *ns, int type, char *prtcl, zsocket_opt_t *option);
void zsock_default_opt(zsocket_opt_t *option);
zsocket_opt_t *zsock_option_create();
void zsock_option_destroy(zsocket_opt_t *option);

#ifdef __cplusplus
}
#endif

#endif
