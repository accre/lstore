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

#ifndef __NETWORK_H_
#define __NETWORK_H_

#define N_BUFSIZE  1024

#include <apr_network_io.h>
#include <apr_thread_proc.h>
#include <apr_thread_mutex.h>
#include <apr_thread_cond.h>
#include <apr_pools.h>
#include <string.h>
#include "transfer_buffer.h"
#include "phoebus.h"
#include "chksum.h"

#ifdef __cplusplus
extern "C" {
#endif

#define NETWORK_MON_MAX 10   //** Max number of ports allowed to monitor

   //** Return values for write_netstream_block **
#define NS_OK       0   //** Command completed without errors
#define NS_TIMEOUT -1   //** Didn't complete in given time
#define NS_SOCKET  -2   //** Socket error
#define NS_CHKSUM  -3   //** Chksum error

#define NS_STATE_DISCONNECTED  0   //NetStream is disconnected
#define NS_STATE_CONNECTED     1   //NS is connected with no ongoing transaction
#define NS_STATE_ONGOING_READ  2   //NS is connected and has partially processed a command (in read state)
#define NS_STATE_ONGOING_WRITE 3   //NS is connected and has partially processed a command (in write state)
#define NS_STATE_READ_WRITE    4   //NS is connected and doing both Rread and write operations
#define NS_STATE_IGNORE        5   //NS is connected but is in a holding pattern so don't monitor it for traffic

//******** Type of network connections ***
#define NS_TYPE_UNKNOWN  0      //** Unspecified type
#define NS_TYPE_SOCK     1      //** Base socket implementation
#define NS_TYPE_PHOEBUS  2      //** Phoebus socket implementation
#define NS_TYPE_1_SSL    3      //** Single SSL connection -- openssl/gnutls/NSS are not thread safe so this is **slow**
#define NS_TYPE_2_SSL    4      //** Dual SSL connection -- Allows use of separate R/W locks over SSL much faster than prev
#define NS_TYPE_ZSOCK	 5	//** ZMQ implementation
#define NS_TYPE_MAX      6      //** Not an actual type just the number of different types

typedef int ns_native_fd_t;

typedef apr_time_t Net_timeout_t;

typedef void net_sock_t;

struct ns_monitor_s;   //** Forward declaration

typedef struct {  //** NetStream checksum container
   int64_t blocksize;   //** Checksum block size or how often to inject/extract the checksum information
   int64_t bytesleft;       //** Current byte count until a full block
   int    is_running;  //** Current state.  1=running
   int    is_valid;     //** Has a valid chksum stored
   chksum_t chksum;    //** Checksum to use
} ns_chksum_t;

typedef struct {
   int id;                  //ID for tracking purposes
   int cuid;                //Unique ID for the connection.  Changes each time the connection is open/closed
   int start;               //Starting position of buffer data
   int end;                 //End position of buffer data
   int sock_type;           //Socket type
   net_sock_t *sock;        //Private socket data.  Depends on socket type
   apr_time_t last_read;        //Last time this connection was used
   apr_time_t last_write;        //Last time this connection was used
   char buffer[N_BUFSIZE];  //intermediate buffer for the conection
   apr_pool_t *mpool;       //** Memory pool for the connection (workaround since APR pools aren't thread safe)
   apr_thread_mutex_t *read_lock;    //Read lock
   apr_thread_mutex_t *write_lock;   //Write lock
   char peer_address[128];
   struct ns_monitor_s *nm;      //This is only used for an accept call to tell which bind was accepted
   ns_chksum_t read_chksum;      //Read chksum
   ns_chksum_t write_chksum;     //Write chksum
   ns_native_fd_t (*native_fd)(net_sock_t *sock);  //** Native socket if supported   
   int (*close)(net_sock_t *sock);  //** Close socket
   long int(*write)(net_sock_t *sock, tbuffer_t *buf, size_t boff, size_t count, Net_timeout_t tm);
   long int (*read)(net_sock_t *sock, tbuffer_t *buf, size_t boff, size_t count, Net_timeout_t tm);
   void (*set_peer)(net_sock_t *sock, char *address, int add_size);
   int (*sock_status)(net_sock_t *sock);
   int (*connect)(net_sock_t *sock, const char *hostname, int port, Net_timeout_t timeout);
   net_sock_t *(*accept)(net_sock_t *sock);
   int (*bind)(net_sock_t *sock, char *address, int port);
   int (*listen)(net_sock_t *sock, int max_pending);
   int (*connection_request)(net_sock_t *sock, int timeout);
} NetStream_t;

typedef struct ns_monitor_s {   //** Struct used to handle ports being monitored
   NetStream_t *ns;       //** Connection actually being monitored
   char *address;         //** Interface to bind to
   int port;              //** Port to use
   int is_pending;        //** Flags the connections as ready for an accept call
   int shutdown_request;  //** Flags the connection to shutdown
   apr_thread_t *thread;  //** Execution thread handle
   apr_pool_t *mpool;     //** Memory pool for the thread
   apr_thread_mutex_t *lock;  //** Lock used for blocking pending accept
   apr_thread_cond_t *cond;   //** cond used for blocking pending accept
   apr_thread_mutex_t *trigger_lock; //** Lock used for sending globabl pending trigger 
   apr_thread_cond_t *trigger_cond;   //** cond used for sending globabl pending accept
   int *trigger_count;             //** Gloabl count of pending requests
} ns_monitor_t;

typedef struct {
   int accept_pending;      //New connection is pending
   int used_ports;          //Number of monitor ports used
   int monitor_index;       //Last ns checked in accept polling
   ns_monitor_t nm[NETWORK_MON_MAX];  //List of ports being monitored
   apr_pool_t *mpool;       //** Memory pool
   apr_thread_mutex_t *ns_lock; //Lock for serializing ns modifications
   apr_thread_cond_t *cond;   //** cond used for blocking pending accept
} Network_t;

#define ns_getid(ns) ns->id
#define ns_native_fd(ns) (ns)->native_fd((ns)->sock)
#define ns_native_enabled(ns) (ns)->native_fd
#define ns_get_type(ns) (ns)->sock_type
#define ns_get_monitor(ns) ns->nm
#define nm_get_port(nm) nm->port
#define nm_get_host(nm) nm->address

int ns_chksum_reset(ns_chksum_t *ncs);
int ns_chksum_set(ns_chksum_t *ncs, chksum_t *chksum, size_t blocksize);
int ns_chksum_is_valid(ns_chksum_t *ncs);

#define ns_chksum_init(ncs)    memset((ncs), 0, sizeof(ns_chksum_t))
#define ns_chksum_enable(ncs)  (ncs)->is_running = 1
#define ns_chksum_disable(ncs)  (ncs)->is_running = 0
//#define ns_chksum_is_valid(ncs) (ncs)->is_valid
#define ns_chksum_clear(ncs) (ncs)->is_valid = 0
#define ns_chksum_type(ncs) (ncs)->chksum.type
#define ns_chksum_blocksize(ncs) (ncs)->blocksize

#define ns_read_chksum_set(ns, ncs) (ns)->read_chksum = (ncs)
//#define ns_read_chksum_reset(ns)  ns_chksum_reset(&((ns)->read_chksum))
#define ns_read_chksum_clear(ns)  (ns)->read_chksum.is_valid = 0
#define ns_read_chksum_enable(ns)  (ns)->read_chksum.is_running = 1 
#define ns_read_chksum_disable(ns)  (ns)->read_chksum.is_running = 0
#define ns_read_chksum_bytesleft(ns) (ns)->read_chksum.bytesleft
#define ns_read_chksum_state(ns)  (ns)->read_chksum.is_running
int ns_read_chksum_flush(NetStream_t *ns);

#define ns_write_chksum_set(ns, ncs) (ns)->write_chksum = (ncs)
#define ns_write_chksum_clear(ns)  (ns)->write_chksum.is_valid = 0
#define ns_write_chksum_enable(ns)  (ns)->write_chksum.is_running = 1 
#define ns_write_chksum_disable(ns)  (ns)->write_chksum.is_running = 0 
#define ns_write_chksum_bytesleft(ns) (ns)->write_chksum.bytesleft
#define ns_write_chksum_state(ns)  (ns)->write_chksum.is_running
int ns_write_chksum_flush(NetStream_t *ns);

int ns_generate_id();
void set_network_tcpsize(int tcpsize);
int get_network_tcpsize(int tcpsize);
int ns_merge_ssl(NetStream_t *ns1, NetStream_t *ns2);
int ns_socket2ssl(NetStream_t *ns);
void set_ns_slave(NetStream_t *ns, int slave);
int connection_is_pending(Network_t *net);
int wait_for_connection(Network_t *net, int max_wait);
void lock_ns(NetStream_t *ns);
void unlock_ns(NetStream_t *ns);
int network_counter(Network_t *net);
int net_connect(NetStream_t *ns, const char *host, int port, Net_timeout_t timeout);
int bind_server_port(Network_t *net, NetStream_t *ns, char *address, int port, int max_pending);
Network_t *network_init();
void close_netstream(NetStream_t *ns);
void destroy_netstream(NetStream_t *ns);
NetStream_t *new_netstream();
void network_close(Network_t *net);
void network_destroy(Network_t *net);
int sniff_connection(NetStream_t *ns);
int write_netstream(NetStream_t *ns, tbuffer_t *buffer, int boff, int bsize, Net_timeout_t timeout);
int write_netstream_block(NetStream_t *ns, apr_time_t end_time, tbuffer_t *buffer, int boff, int bsize);
int read_netstream_block(NetStream_t *ns, apr_time_t end_time, tbuffer_t *buffer, int boff, int bsize);
int read_netstream(NetStream_t *ns, tbuffer_t *buffer, int boff, int size, Net_timeout_t timeout);
int readline_netstream_raw(NetStream_t *ns, tbuffer_t *buffer, int boff, int size, Net_timeout_t timeout, int *status);
int readline_netstream(NetStream_t *ns, tbuffer_t *buffer, int boff, int size, Net_timeout_t timeout);
int accept_pending_connection(Network_t *net, NetStream_t *ns);
Net_timeout_t *set_net_timeout(Net_timeout_t *tm, int sec, int us);
void get_net_timeout(Net_timeout_t tm, int *sec, int *us);
void ns_init(NetStream_t *ns);
void _ns_init(NetStream_t *ns, int incid);
void wakeup_network(Network_t *net);

#ifdef __cplusplus
}
#endif

#endif


