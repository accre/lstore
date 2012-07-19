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

#define _log_module_index 121

#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <assert.h>
#include <signal.h>
#include <errno.h>
#include <fcntl.h>
#include <apr_time.h>
#include "network.h"
#include "debug.h"
#include "log.h"
#include "dns_cache.h"
#include "fmttypes.h"

#include "net_sock.h"
#include "net_1_ssl.h"
#include "net_2_ssl.h"
#include "net_phoebus.h"

int _read_netstream(NetStream_t *ns, tbuffer_t *buffer, int boff, int bsize, Net_timeout_t timeout, int dolock);
int _read_netstream_block(NetStream_t *ns, apr_time_t end_time, tbuffer_t *buffer, int boff, int bsize, int dolock);
int _write_netstream(NetStream_t *ns, tbuffer_t *buffer, int boff, int size, Net_timeout_t timeout, int dolock);
int _write_netstream_block(NetStream_t *ns, apr_time_t end_time, tbuffer_t *buffer, int boff, int size, int dolock);

int tcp_bufsize = 0;   //** 0 means use the default TCP buffer sizes for the OS

//*** These are used for counters to track connections
int _cuid_counter = 0;
apr_thread_mutex_t *_net_counter_lock = NULL;
apr_pool_t *_net_counter_pool = NULL;
//------------------

NetStream_t *_get_free_conn(Network_t *net);

int ns_generate_id() {
   int id;

   if (_net_counter_lock == NULL) {      
      if (_net_counter_pool == NULL) assert(apr_pool_create(&_net_counter_pool, NULL) == APR_SUCCESS);
      assert(apr_thread_mutex_create(&_net_counter_lock, APR_THREAD_MUTEX_DEFAULT, _net_counter_pool) == APR_SUCCESS);
   }

   apr_thread_mutex_lock(_net_counter_lock);
   id = _cuid_counter;
   log_printf(15, "ns_generate_id: _cuid=%d\n", _cuid_counter);
   _cuid_counter++;
   if (_cuid_counter > 999999999) _cuid_counter = 0;
   apr_thread_mutex_unlock(_net_counter_lock);

   return(id);
}

//*********************************************************************
// set/get_network_tcpsize - Sets/gets the default TCP window size.
//     If 0 then the OS defaults are used.
//*********************************************************************

void set_network_tcpsize(int tcpsize)  { tcp_bufsize = tcpsize; }
int get_network_tcpsize(int tcpsize)  { return(tcp_bufsize); }

//*********************************************************************
//  connection_is_pending - Returns if a new connection is needed
//*********************************************************************

int connection_is_pending(Network_t *net)
{

  apr_thread_mutex_lock(net->ns_lock);
  int i = net->accept_pending;
  apr_thread_mutex_unlock(net->ns_lock);

  return(i);
}

//*********************************************************************
//  Locks for R/W 
//*********************************************************************

void lock_read_ns(NetStream_t *ns)
{
  apr_thread_mutex_lock(ns->read_lock);
}

//*********************************************************************

void unlock_read_ns(NetStream_t *ns)
{
  apr_thread_mutex_unlock(ns->read_lock);
}

//*********************************************************************

void lock_write_ns(NetStream_t *ns)
{
  apr_thread_mutex_lock(ns->write_lock);
}

//*********************************************************************

void unlock_write_ns(NetStream_t *ns)
{
  apr_thread_mutex_unlock(ns->write_lock);
}

//*********************************************************************
// lock_ns - Locks a netstream
//*********************************************************************

void lock_ns(NetStream_t *ns)
{
  lock_read_ns(ns);
  lock_write_ns(ns);
}

//*********************************************************************
// unlock_ns - Unlocks a netstream
//*********************************************************************

void unlock_ns(NetStream_t *ns)
{
  unlock_write_ns(ns);
  unlock_read_ns(ns);
}

//*********************************************************************
// ns_getid - Returns the ID
//*********************************************************************

#define ns_getid(ns) ns->id

//*********************************************************************
// ns_set_chksum - Associates a chksum to the stream
//*********************************************************************

int ns_chksum_set(ns_chksum_t *ncs, chksum_t *cks, size_t blocksize)
{
  ncs->blocksize = blocksize;
  ncs->bytesleft = blocksize;
  ncs->chksum = *cks;
  ncs->is_running = 0;
  ncs->is_valid = 1;

  return(0);
}

//*********************************************************************
// ns_chksum_is_valid - Returns if the chksum can be used
//*********************************************************************

int ns_chksum_is_valid(ns_chksum_t *ncs)
{
  int i = 0;
  
  if (ncs->is_valid == 1) {
     if (chksum_type(&(ncs->chksum)) != CHKSUM_NONE) i = 1;
  }

  return(i);
}


//*********************************************************************
// ns_read_chksum_flush - Assumes the next bytes in the stream are
//   the chksum and reads them and does a comparison
//*********************************************************************

int ns_read_chksum_flush(NetStream_t *ns)
{
  char ns_value[CHKSUM_MAX_SIZE], chksum_value[CHKSUM_MAX_SIZE];
  int err, n;
  tbuffer_t buf;

  log_printf(15, "ns_read_chksum_flush: Reading chksum!  ns=%d type=%d bleft=" I64T " bsize=" I64T " state=%d\n", 
     ns_getid(ns), chksum_type(&(ns->read_chksum.chksum)), ns->read_chksum.bytesleft, ns->read_chksum.blocksize, ns_read_chksum_state(ns));  flush_log();

  if (ns_read_chksum_state(ns) == 0) return(0);
  if (ns->read_chksum.bytesleft == ns->read_chksum.blocksize) return(0);  //** Nothing to do

  n = chksum_size(&(ns->read_chksum.chksum), CHKSUM_DIGEST_HEX);


  ns->read_chksum.is_running = 0;  //** Don't want to get in an endless loop
  tbuffer_single(&buf, n, ns_value);
  err = _read_netstream_block(ns, apr_time_now() + apr_time_make(5,0), &buf, 0, n, 0);
  ns_value[n] = '\0';
  ns->read_chksum.is_running = 1;
  
  log_printf(15, "ns_read_chksum_flush: Finished reading chksum!  ns=%d\n", ns_getid(ns));  flush_log();

  if (err != 0) {
     log_printf(10, "ns_read_chksum_flush: ns=%d Error reading chksum! error=%d\n", ns_getid(ns), err);
     return(err);
  }

  chksum_get(&(ns->read_chksum.chksum), CHKSUM_DIGEST_HEX, chksum_value);
  log_printf(15, "ns_read_chksum_flush: after chksum_get!  ns=%d\n", ns_getid(ns));  flush_log();
  err = (strncmp(chksum_value, ns_value, n) == 0) ? 0 : 1;

log_printf(15, "ns_read_chksum_flush: ns=%d     ns_value=%s  cmp=%d\n", ns_getid(ns), ns_value, err);
log_printf(15, "ns_read_chksum_flush: ns=%d chksum_value=%s\n", ns_getid(ns), chksum_value);
  if (err != 0) {
     log_printf(1, "ns_read_chksum_flush: ns=%d chksum error!\n", ns_getid(ns));
     log_printf(1, "ns_read_chksum_flush: ns=%d     ns_value=%s  cmp=%d\n", ns_getid(ns), ns_value, err);
     log_printf(1, "ns_read_chksum_flush: ns=%d chksum_value=%s\n", ns_getid(ns), chksum_value);
  }

  log_printf(15, "ns_read_chksum_flush: end of routine!  ns=%d\n err=%d", ns_getid(ns), err);  flush_log();

  return(err);
}

//*********************************************************************
// ns_write_chksum_flush - Injects the chksum into the stream
//*********************************************************************

int ns_write_chksum_flush(NetStream_t *ns)
{
  char chksum_value[CHKSUM_MAX_SIZE];
  int err, n;
  tbuffer_t buf;

  log_printf(15, "ns_write_chksum_flush: injecting chksum!  ns=%d type=%d bytesleft=" I64T " bsize=" I64T "\n", 
     ns_getid(ns), chksum_type(&(ns->write_chksum.chksum)), ns->write_chksum.bytesleft, ns->write_chksum.blocksize);  flush_log();

  if (ns_write_chksum_state(ns) == 0) return(0);
  if (ns->write_chksum.bytesleft == ns->write_chksum.blocksize) return(0);  //** Nothing to do

  n = chksum_size(&(ns->write_chksum.chksum), CHKSUM_DIGEST_HEX);
  chksum_get(&(ns->write_chksum.chksum), CHKSUM_DIGEST_HEX, chksum_value);
  
  ns->write_chksum.is_running = 0;  //** Don't want to get in an endless loop  
  tbuffer_single(&buf, n, chksum_value);
  err = _write_netstream_block(ns, apr_time_now() + apr_time_make(5,0), &buf, 0, n, 0);
  ns->write_chksum.is_running = 1;

  if (err != 0) {
     log_printf(10, "ns_write_chksum_flush: ns=%d Error writing chksum! error=%d\n", ns_getid(ns), err);
     return(err);
  }

chksum_value[n] = '\0';
log_printf(15, "ns_write_chksum_flush: ns=%d chksum_value=%s\n", ns_getid(ns), chksum_value);
log_printf(15, "ns_write_chksum_flush: end of routine!  ns=%d\n err=%d", ns_getid(ns), err);  flush_log();

  return(err);
}

//*********************************************************************
// ns_chksum_reset - Resets the chksum counter
//*********************************************************************

int ns_chksum_reset(ns_chksum_t *ncs)
{
  ncs->bytesleft = ncs->blocksize;
  chksum_reset(&(ncs->chksum));

  return(0);
}

//*********************************************************************
//  network_counter - Returns the network counter or the number
//     of sockets used so far
//*********************************************************************

int network_counter(Network_t *net)
{
   int count;

   apr_thread_mutex_lock(_net_counter_lock);
   count = _cuid_counter;
   apr_thread_mutex_unlock(_net_counter_lock);

   return(count);
}

//*********************************************************************
// get_net_timeout - Initializes the timout data structure
//*********************************************************************

void get_net_timeout(Net_timeout_t tm, int *sec, int *us)
{
  *sec = tm / 1000000;
  *us = tm % 1000000;
//log_printf(0, "get_net_timoeut: tm=" TT " sec=%d us=%d\n", tm, *sec, *us);

  return;;
}

//*********************************************************************
// set_net_timeout - Initializes the timout data structure
//*********************************************************************

Net_timeout_t *set_net_timeout(Net_timeout_t *tm, int sec, int us)
{
  *tm = (apr_time_t)sec*1000000 + (apr_time_t)us;
//log_printf(0, "set_net_timoeut: tm=" TT " sec=%d us=%d\n", *tm, sec, us);

  return(tm);
}

//*********************************************************************
// _ns_init - Inits a NetStream data structure assuming a connected state
//*********************************************************************

void _ns_init(NetStream_t *ns, int incid)
{
  //** Initialize the socket type information **
  ns->sock_type = NS_TYPE_UNKNOWN;
  ns->sock = NULL;
  ns->close = NULL;
  ns->read = NULL;
  ns->write = NULL;
  ns->sock_status = NULL;
  ns->set_peer = NULL;
  ns->connect = NULL;
  ns->nm = NULL;

  ns->last_read = apr_time_now();
  ns->last_write = apr_time_now();
  ns->start = 0;
  ns->end = -1;
  memset(ns->peer_address, 0, sizeof(ns->peer_address));

  memset(&(ns->write_chksum), 0, sizeof(ns_chksum_t)); ns->write_chksum.is_valid = 0;
  memset(&(ns->read_chksum), 0, sizeof(ns_chksum_t));  ns->read_chksum.is_valid = 0;
  
  if (incid == 1)  ns->id = ns_generate_id();

//  log_printf(15, "_ns_init: incid=%d ns=%d\n", incid, ns->id);

}


//*********************************************************************
// ns_init - inits a NetStream_t data structure
//*********************************************************************

void ns_init(NetStream_t *ns)
{
  _ns_init(ns, 1);
}

//*********************************************************************
// ns_clone - Clones the ns settings from one ns to another.
//     The sock is also copied but it can lead to problems if
//     not used properly.  Normally this field should be set to NULL
//*********************************************************************

void ns_clone(NetStream_t *dest_ns, NetStream_t *src_ns)
{
  apr_thread_mutex_t *rl, *wl;
  apr_pool_t *mpool;

   //** Need to preserve the locks and pool they came from
   rl = dest_ns->read_lock; wl = dest_ns->write_lock;
   mpool = dest_ns->mpool;

   lock_ns(src_ns);
   memcpy(dest_ns, src_ns, sizeof(NetStream_t));
   unlock_ns(src_ns);

   dest_ns->read_lock = rl; dest_ns->write_lock = wl;
   dest_ns->mpool = mpool;
}

//*********************************************************************
// net_connect - Creates a connection to a remote host.
//*********************************************************************

int net_connect(NetStream_t *ns, const char *hostname, int port, Net_timeout_t timeout)
{
  int err;

  lock_ns(ns);
  
  //** Simple check on the connection type **
  switch (ns->sock_type) {          
     case NS_TYPE_SOCK:
     case NS_TYPE_PHOEBUS:
     case NS_TYPE_1_SSL:
         break;
     default:
         log_printf(0, "net_connect: Invalid ns_type=%d Exiting!\n", ns->sock_type);
         unlock_ns(ns);
         return(1);
  }

   err = ns->connect(ns->sock, hostname, port, timeout);
   if (err != 0) {
      log_printf(5, "net_connect: select failed.  Hostname: %s  Port: %d select=%d errno: %d error: %s\n", hostname, port, err, errno, strerror(errno));
      unlock_ns(ns);
      return(1);
   }

   ns->set_peer(ns->sock, ns->peer_address, sizeof(ns->peer_address));

   ns->id = ns_generate_id();

   log_printf(10, "net_connect:  Made connection to %s:%d on ns=%d address=%s\n", hostname, port, ns->id, ns->peer_address);

   log_printf(10, "net_connect: final ns=%d\n", ns->id);
   unlock_ns(ns);

   return(0);
}

//*********************************************************************
//  monitor_thread - Thread for monitoring a network connection for 
//     incoming connection requests.
//*********************************************************************

void *monitor_thread(apr_thread_t *th, void *data)
{
   ns_monitor_t *nm = (ns_monitor_t *)data;
   NetStream_t *ns = nm->ns;
   int i;

   log_printf(15, "monitor_thread: Monitoring port %d\n", nm->port);

   apr_thread_mutex_lock(nm->lock);
   while (nm->shutdown_request == 0) {
      apr_thread_mutex_unlock(nm->lock);

      i = ns->connection_request(ns->sock, 1);

      if (i == 1) {  //** Got a request
         log_printf(15, "monitor_thread: port=%d ns=%d Got a connection request time=" TT "\n", nm->port, ns_getid(ns), apr_time_now());

         //** Mark that I have a connection pending
         apr_thread_mutex_lock(nm->lock); 
         nm->is_pending = 1;
         apr_thread_mutex_unlock(nm->lock);

         //** Wake up the calling thread
         apr_thread_mutex_lock(nm->trigger_lock);
         (*(nm->trigger_count))++;
         apr_thread_cond_signal(nm->trigger_cond);
         apr_thread_mutex_unlock(nm->trigger_lock);

         log_printf(15, "monitor_thread: port=%d ns=%d waiting for accept\n", nm->port, ns_getid(ns));

         //** Sleep until my connection is accepted
         apr_thread_mutex_lock(nm->lock); 
         while ((nm->is_pending == 1) && (nm->shutdown_request == 0)) {
             apr_thread_cond_wait(nm->cond, nm->lock);
             log_printf(15, "monitor_thread: port=%d ns=%d Cond triggered=" TT " trigger_count=%d\n", nm->port, ns_getid(ns), apr_time_now(), *(nm->trigger_count));
         }
         apr_thread_mutex_unlock(nm->lock);          
         log_printf(15, "monitor_thread: port=%d ns=%d Connection accepted time=" TT "\n", nm->port, ns_getid(ns), apr_time_now());

         //** Update pending count
//         apr_thread_mutex_lock(nm->trigger_lock);
//         *(nm->trigger_count)--;
//         apr_thread_mutex_unlock(nm->trigger_lock);
      } 

      apr_thread_mutex_lock(nm->lock);
   }

   apr_thread_mutex_unlock(nm->lock);

   //** Lastly shutdown my socket
   close_netstream(ns);

   log_printf(15, "monitor_thread: Closing port %d\n", nm->port);

   apr_thread_exit(th, 0);

   return(NULL);
}

//*********************************************************************
// bind_server_port - Creates the main port for listening
//*********************************************************************

int bind_server_port(Network_t *net, NetStream_t *ns, char *address, int port, int max_pending)
{
   int err, slot;
   ns_monitor_t *nm;

   apr_thread_mutex_lock(net->ns_lock);

   slot = net->used_ports;
   nm = &(net->nm[slot]);

   log_printf(15, "bind_server_port: connection=%s:%d being stored in slot=%d\n", address, port, slot);

   err = ns->bind(ns->sock, address, port);
   if (err != 0) {
      log_printf(0, "bind_server_port: Error with bind address=%s port=%d err=%d\n", address, port, err);
      return(err);
   }

   err = ns->listen(ns->sock, max_pending);
   if (err != 0) {
      log_printf(0, "bind_server_port: Error with listen address=%s port=%d err=%d\n", address, port, err);
      return(err);
   }

   apr_pool_create(&(nm->mpool), NULL);
   apr_thread_mutex_create(&(nm->lock), APR_THREAD_MUTEX_DEFAULT, nm->mpool);
   apr_thread_cond_create(&(nm->cond), nm->mpool);

   nm->shutdown_request = 0;
   nm->is_pending = 0;
   nm->ns = ns;
   nm->address = strdup(address);
   nm->port = port;
   nm->trigger_cond = net->cond;
   nm->trigger_lock = net->ns_lock;
   nm->trigger_count = &(net->accept_pending);
   ns->id = ns_generate_id();

   apr_pool_create(&(nm->mpool), NULL);
   apr_thread_create(&(nm->thread), NULL, monitor_thread, (void *)nm, nm->mpool);

   net->used_ports++;
   apr_thread_mutex_unlock(net->ns_lock);

   return(0);
}

//*********************************************************************
// close_server_port - Closes a server port
//*********************************************************************

void close_server_port(ns_monitor_t *nm)
{
   apr_status_t dummy;

   //** Trigger a port shutdown
   apr_thread_mutex_lock(nm->lock);
   nm->shutdown_request = 1;
log_printf(15, "close_server_port: port=%d Before cond_signal\n", nm->port); flush_log();
   apr_thread_cond_signal(nm->cond);
log_printf(15, "close_server_port: port=%d After cond_signal\n", nm->port); flush_log();
   apr_thread_mutex_unlock(nm->lock);

log_printf(15, "close_server_port: port=%d After unlock\n", nm->port); flush_log();

   //** Wait until the thread closes
   apr_thread_join(&dummy, nm->thread);

log_printf(15, "close_server_port: port=%d After join\n", nm->port); flush_log();

   //** Free the actual struct
   free(nm->address);
   apr_thread_mutex_destroy(nm->lock);
   apr_thread_cond_destroy(nm->cond);

   apr_pool_destroy(nm->mpool);

   nm->port = -1;
}


//*********************************************************************
// network_init - Initialize the network for use
//*********************************************************************

Network_t *network_init()
{
  int i;
  Network_t *net;

  //**** Allocate space for the data structures ***
  assert((net = (Network_t *)malloc(sizeof(Network_t))) != NULL);


  net->used_ports = 0;
  net->accept_pending = 0;
  net->monitor_index = 0;
  assert(apr_pool_create(&(net->mpool), NULL) == APR_SUCCESS);
  apr_thread_mutex_create(&(net->ns_lock), APR_THREAD_MUTEX_DEFAULT,net->mpool);
  apr_thread_cond_create(&(net->cond), net->mpool);

  net->used_ports = 0;
  for (i=0; i<NETWORK_MON_MAX; i++) {
      net->nm[i].port = -1;
  }

  return(net);
}

//*********************************************************************
// _close_ns - Close a network connection
//*********************************************************************

void _close_ns(NetStream_t *ns)
{

   log_printf(10, "close_netstream:  Closing stream ns=%d type=%d\n", ns->id, ns->sock_type);
   flush_log();

   ns->cuid = -1;
   if (ns->sock == NULL) return;

   if (ns->sock_status(ns->sock) != 1) return;

   ns->close(ns->sock);

   ns->sock = NULL;

   return;
}

//*********************************************************************
// close_netstream - Close a network connection
//*********************************************************************

void close_netstream(NetStream_t *ns)
{
   lock_ns(ns);
   _close_ns(ns);
   unlock_ns(ns);
}

//********************************************************************* 
// teardown_netstream - closes an NS and also frees the mutex
//********************************************************************* 

void teardown_netstream(NetStream_t *ns)
{
  close_netstream(ns);
  apr_thread_mutex_destroy(ns->read_lock);
  apr_thread_mutex_destroy(ns->write_lock);
  apr_pool_destroy(ns->mpool);
}

//********************************************************************* 
// destroy_netstream - Completely destroys a netstream created with new_netstream
//********************************************************************* 

void destroy_netstream(NetStream_t *ns)
{
  teardown_netstream(ns);
  free(ns);
}

//********************************************************************* 
// new_netstream - Creates a new NS
//********************************************************************* 

NetStream_t *new_netstream()
{
  NetStream_t *ns = (NetStream_t *)malloc(sizeof(NetStream_t));

  if (ns == NULL) {
     log_printf(0, "new_netstream: Failed malloc!!\n");
     abort();
  }

  assert(apr_pool_create(&(ns->mpool), NULL) == APR_SUCCESS);
  apr_thread_mutex_create(&(ns->read_lock), APR_THREAD_MUTEX_DEFAULT,ns->mpool);
  apr_thread_mutex_create(&(ns->write_lock), APR_THREAD_MUTEX_DEFAULT,ns->mpool);
 
  _ns_init(ns, 0);
  ns->id = ns->cuid = -1;

  return(ns);
}


//********************************************************************* 
// network_close - Closes down all the network connections
//********************************************************************* 

void network_close(Network_t *net)
{
  int i;

  //** Close the attached server ports
  for (i=0; i<NETWORK_MON_MAX; i++) {
      if (net->nm[i].port > 0) {
         close_server_port(&(net->nm[i]));
      }
  }
}

//********************************************************************* 
// network_destroy - Closes and destroys the network struct
//********************************************************************* 

void network_destroy(Network_t *net)
{
  network_close(net);

  //** Free the main net variables
  apr_thread_mutex_destroy(net->ns_lock);   
  apr_thread_cond_destroy(net->cond);   
  apr_pool_destroy(net->mpool);

  free(net);
}

//********************************************************************* 
// write_netstream - Writes characters to the stream with a max wait
//********************************************************************* 

int _write_netstream(NetStream_t *ns, tbuffer_t *buffer, int boff, int bsize, Net_timeout_t timeout, int dolock)
{
   int total_bytes, i;

   if (dolock == 1) lock_write_ns(ns);

   
   if (ns->sock_status(ns->sock) != 1) {      
      log_printf(15, "write_netstream: connection closed!  ns=%d\n", ns->id);
      if (dolock == 1) unlock_write_ns(ns);
      return(-1);
   }

   if (bsize == 0) { 
      if (dolock == 1) unlock_write_ns(ns);
      return(0); 
   }
  
   if (ns_write_chksum_state(ns) == 1) {  //** We have chksumming enabled
      if (bsize > ns->write_chksum.bytesleft) {
          bsize = ns->write_chksum.bytesleft;  //** Truncate at the block
      }
   }

   total_bytes = ns->write(ns->sock, buffer, boff, bsize, timeout);

   if (total_bytes == -1) {
      log_printf(10, "write_netstream:  Dead connection! ns=%d\n", ns_getid(ns));
   }

   ns->last_write = apr_time_now();

   if ((ns_write_chksum_state(ns) == 1) && (total_bytes > 0)) {  //** We have chksumming enabled
      chksum_add(&(ns->write_chksum.chksum), total_bytes, buffer, boff);  //** Chksum it
      ns->write_chksum.bytesleft -= total_bytes;
      if (ns->write_chksum.bytesleft <= 0) { //** Reached the block size so inject the chksum
          i = ns_write_chksum_flush(ns);
          if (i != 0) total_bytes = NS_CHKSUM;

          //** Reset the chksum
          ns->write_chksum.bytesleft = ns->write_chksum.blocksize;
          chksum_reset(&(ns->write_chksum.chksum));
      }
   }

   if (dolock == 1) unlock_write_ns(ns);

   return(total_bytes); 
}

//********************************************************************* 
// write_netstream - Writes characters to the stream with a max wait
//********************************************************************* 

int write_netstream(NetStream_t *ns, tbuffer_t *buffer, int boff, int bsize, Net_timeout_t timeout)
{
   return(_write_netstream(ns, buffer, boff, bsize, timeout, 1));
}

//********************************************************************* 
//  _write_netstream_block - Same as write_netstream but blocks until the
//     data is sent or end_time is reached
//********************************************************************* 

int _write_netstream_block(NetStream_t *ns, apr_time_t end_time, tbuffer_t *buffer, int boff, int size, int dolock)
{
  int pos, nleft, nbytes, err;
 
  Net_timeout_t dt;

  set_net_timeout(&dt, 1, 0);
  pos = boff;
  nleft = size;
  nbytes = -100;
  err = NS_OK;
  while ((nleft > 0) && (err == NS_OK)) {
     nbytes = _write_netstream(ns, buffer, pos, nleft, dt, dolock);
     log_printf(15, "write_netstream_block: ns=%d size=%d nleft=%d nbytes=%d pos=%d time=" TT "\n", 
             ns_getid(ns), size, nleft, nbytes, pos, apr_time_now());

     if (apr_time_now() > end_time) {
        log_printf(15, "write_netstream_block: ns=%d Command timed out! to=" TT " ct=" TT " \n", ns_getid(ns), end_time, apr_time_now());
        err = NS_TIMEOUT;
     }

     if (nbytes < 0) {
        err = nbytes;   //** Error with write
     } else if (nbytes > 0) {   //** Normal write
        pos = pos + nbytes;
        nleft = nleft - nbytes;
        err = NS_OK;
     }
  }

  log_printf(15, "write_netstream_block: END ns=%d size=%d nleft=%d nbytes=%d pos=%d\n", ns_getid(ns), size, nleft, nbytes, pos);

  return(err);
}

//********************************************************************* 
//  write_netstream_block - Same as write_netstream but blocks until the
//     data is sent or end_time is reached
//********************************************************************* 

int write_netstream_block(NetStream_t *ns, apr_time_t end_time, tbuffer_t *buffer, int boff, int bsize)
{
   return(_write_netstream_block(ns, end_time, buffer, boff, bsize, 1));
}

//********************************************************************* 
//  read_netstream_block - Same as read_netstream but blocks until the
//     data is sent or end_time is reached
//********************************************************************* 

int _read_netstream_block(NetStream_t *ns, apr_time_t end_time, tbuffer_t *buffer, int pos, int size, int dolock)
{
  int nleft, nbytes, err;
 
  Net_timeout_t dt;

  set_net_timeout(&dt, 1, 0);
  nleft = size;
  nbytes = -100;
  err = NS_OK;
  while ((nleft > 0) && (err == NS_OK)) {
     nbytes = _read_netstream(ns, buffer, pos, nleft, dt, dolock);
     log_printf(15, "read_netstream_block: ns=%d size=%d nleft=%d nbytes=%d pos=%d time=" TT "\n", 
             ns_getid(ns), size, nleft, nbytes, pos, apr_time_now());

     if (apr_time_now() > end_time) {
        log_printf(15, "read_netstream_block: ns=%d Command timed out! to=" TT " ct=" TT " \n", ns_getid(ns), end_time, apr_time_now());
        err = NS_TIMEOUT;
     }

     if (nbytes < 0) {
        err = nbytes;   //** Error with write
     } else if (nbytes > 0) {   //** Normal write
        pos = pos + nbytes;
        nleft = nleft - nbytes;
        err = NS_OK;
     }
  }

  log_printf(15, "read_netstream_block: END ns=%d size=%d nleft=%d nbytes=%d pos=%d\n", ns_getid(ns), size, nleft, nbytes, pos);

  return(err);
}

//********************************************************************* 
//  read_netstream_block - Same as read_netstream but blocks until the
//     data is sent or end_time is reached
//********************************************************************* 

int read_netstream_block(NetStream_t *ns, apr_time_t end_time, tbuffer_t *buffer, int boff, int bsize)
{
   return(_read_netstream_block(ns, end_time, buffer, boff, bsize, 1));
}

//********************************************************************* 
//  scan_and_copy_netstream - Scans the input stream for "\n" or "\r"
//********************************************************************* 

int scan_and_copy_stream(char *inbuf, int insize, char *outbuf, int outsize, int *finished)
{
   int max_char;
   int nbytes;

   *finished = 0;

   if (outsize > insize) {
      max_char = insize - 1;
   } else {
      max_char = outsize - 1;
   }

   if (max_char < 0) return(0);  //** Nothing to parse
   if (insize == 0) {
      return(0);
   }

   nbytes = -1;
   do {
      nbytes++;
      outbuf[nbytes] = inbuf[nbytes];
   } while ((outbuf[nbytes] != '\r') && (outbuf[nbytes] != '\n') && (nbytes < max_char));

   if ((outbuf[nbytes] == '\r') || (outbuf[nbytes] == '\n')) {
      *finished = 1;
   }

   log_printf(15, "scan_and_copy_stream: insize=%d outsize=%d  max_char=%d nbytes=%d finished=%d\n", insize, outsize, max_char, nbytes+1, *finished);

//   log_printf(0, "scan_and_copy_stream: insize=%d nbytes=%d buffer=!", insize, nbytes+1);
//   int i;
//   if (insize > nbytes+2) insize = nbytes+2; 
//   for (i=0; i<insize; i++) log_printf(0, "%c", inbuf[i]);
//   log_printf(0, "!\n");

   return(nbytes+1);
}

//********************************************************************* 
// read_netstream - Reads characters from the stream with a max wait
//********************************************************************* 

int _read_netstream(NetStream_t *ns, tbuffer_t *buffer, int boff, int size, Net_timeout_t timeout, int dolock)
{
   int total_bytes, i;
   tbuffer_t ns_tb;

   if (size == 0) return(0);

   if (dolock == 1) lock_read_ns(ns);

   if (ns->sock_status(ns->sock) != 1) {      
      log_printf(15, "read_netstream: Dead connection!  ns=%d\n", ns->id);
      if (dolock == 1) unlock_read_ns(ns);
      return(-1); 
   }

   total_bytes = 0;
   
   if (ns_read_chksum_state(ns) == 1) {  //** We have chksumming enabled
      if (size > ns->read_chksum.bytesleft) {
         size = ns->read_chksum.bytesleft;  //** Truncate at the block
      }
   }

   //*** 1st grab anything currently in the network buffer ***
   if (ns->end >= ns->start) {
       i = ns->end - ns->start + 1;
       if (i>size) {
          total_bytes = size;
          tbuffer_single(&ns_tb, size, &(ns->buffer[ns->start]));
          tbuffer_copy(&ns_tb, 0, buffer, boff, size);
          ns->start = ns->start + total_bytes;
       } else {
          total_bytes = i;
          tbuffer_single(&ns_tb, i, &(ns->buffer[ns->start]));
          tbuffer_copy(&ns_tb, 0, buffer, boff, i);
          ns->start = 0;
          ns->end = -1;
       }
   } else {  //*** Now grab some data off the network port ****
      total_bytes = ns->read(ns->sock, buffer, boff, size, timeout);
   }

   debug_code(  
     if (total_bytes > 0) {
//        debug_printf(10, "read_netstream: Command : !");
//        for (i=0; i< total_bytes; i++) debug_printf(10, "%c", buffer[i]);
//        debug_printf(10, "! * nbytes =%d\n", total_bytes); flush_debug();
     } else if (total_bytes == 0) {
        debug_printf(10, "read_netstream: No data!\n");
     } else {
        log_printf(10, "read_netstream:  Dead connection! ns=%d\n", ns_getid(ns));
     }
   )

   ns->last_read = apr_time_now();

   if ((ns_read_chksum_state(ns) == 1) && (total_bytes > 0)) {  //** We have chksumming enabled
      chksum_add(&(ns->read_chksum.chksum), total_bytes, buffer, boff);

      ns->read_chksum.bytesleft -= total_bytes;
      if (ns->read_chksum.bytesleft <= 0) { //** Compare the chksums
          i = ns_read_chksum_flush(ns);
          if (i != 0) total_bytes = NS_CHKSUM;

          //** Reset the chksum
          ns->read_chksum.bytesleft = ns->read_chksum.blocksize;
          chksum_reset(&(ns->read_chksum.chksum));
      }
   }

   if (dolock == 1) unlock_read_ns(ns);

   return(total_bytes);
}

//********************************************************************* 
// read_netstream - Reads characters fomr the stream with a max wait
//********************************************************************* 

int read_netstream(NetStream_t *ns, tbuffer_t *buffer, int boff, int bsize, Net_timeout_t timeout)
{
   return(_read_netstream(ns, buffer, boff, bsize, timeout, 1));
}

//*********************************************************************
// readline_netstream_raw - Performs an attempt to read a complete line
//    if it fails it returns the partial read
//*********************************************************************

int readline_netstream_raw(NetStream_t *ns, tbuffer_t *buffer, int boff, int size, Net_timeout_t timeout, int *status)
{
   tbuffer_t ns_tb;
   int nbytes, total_bytes, i;
   int finished = 0;
   char *buf;
   *status = 0;

   buf = (buffer->buf.iov[0].iov_base + boff);

   //*** 1st grab anything currently in the network buffer ***
   total_bytes = 0;
   lock_read_ns(ns);
   i = ns->end - ns->start + 1;
   debug_printf(15, "readline_netstream_raw: ns=%d buffer pos start=%d end=%d\n", ns->id, ns->start, ns->end);
   if (i > 0) {
       //** Assumes buffer has a single iovec element
       total_bytes = scan_and_copy_stream(&(ns->buffer[ns->start]), i, buf, size, &finished);
       ns->start = ns->start + total_bytes;
       if (ns->start > ns->end) {
          ns->start = 0;
          ns->end = -1;
       }
  
       if (finished == 1) {
          *status = 1;
          total_bytes--;
          buf[total_bytes] = '\0';   //** Make sure and NULL terminate the string remove the \n
          debug_printf(15, "readline_stream_raw: BUFFER ns=%d Command : %s * nbytes=%d\n", ns->id, buffer,total_bytes); flush_debug();
          unlock_read_ns(ns);
          return(total_bytes);
       }
   }
   unlock_read_ns(ns);

   //*** Now grab the data off the network port ****
   nbytes = 0;
   if (finished == 0) {
      tbuffer_single(&ns_tb, N_BUFSIZE, ns->buffer);      
      nbytes = read_netstream(ns, &ns_tb, 0, N_BUFSIZE, timeout);  //**there should be 0 bytes in buffer now
      debug_printf(15, "readline_netstream_raw: ns=%d Command : !", ns->id);
      for (i=0; i< nbytes; i++) debug_printf(15, "%c", ns->buffer[i]);
      debug_printf(15, "! * nbytes =%d\n", nbytes); flush_debug();

      if (nbytes > 0) {
         //** Assumes buffer has a single iovec element
         lock_read_ns(ns);
         i = scan_and_copy_stream(ns->buffer, nbytes, &(buf[total_bytes]), size-total_bytes, &finished);
         unlock_read_ns(ns);
         total_bytes += i;
      }
   }
  
   buf[total_bytes] = '\0';   //** Make sure and NULL terminate the string

   if (finished == 1) {  //*** Push the unprocessed characters back onto the stream buffer ****
      *status = 1;
      total_bytes--;
      buf[total_bytes] = '\0';   //** Make sure and NULL terminate the string remove the \n
      lock_read_ns(ns);
      ns->start = i;
      ns->end = nbytes-1;
      if (ns->start > ns->end) {
         ns->start = 0;
         ns->end = -1;
      }
      unlock_read_ns(ns);

      debug_printf(15, "readline_stream_raw: ns=%d Command : %s * nbytes=%d\n", ns->id, buffer,total_bytes); flush_debug();
   } else if (nbytes == -1) {  //** Socket error
      *status = -1;
      debug_printf(15, "readline_stream_raw: Socket error! ns=%d nbytes=%d  buffer=%s\n", ns->id, total_bytes, buffer); flush_debug();
      return(0);
   } else {       //*** Not enough space in input buffer
      *status = 0;
      lock_read_ns(ns);
      ns->start = i;
      ns->end = nbytes-1;
      if (ns->start > ns->end) {
         ns->start = 0;
         ns->end = -1;
      }
      unlock_read_ns(ns);
      debug_printf(15, "readline_stream_raw: Out of buffer space or nothing read! ns=%d nbytes=%d  buffer=%s\n", ns->id, total_bytes, buffer); flush_debug();
//      return(-1);   //**Force the socket to be closed
   }         

   return(total_bytes);
}

//********************************************************************* 
// readline_netstream - Reads a line of text from the stream 
//*********************************************************************

int readline_netstream(NetStream_t *ns, tbuffer_t *buffer, int boff, int bsize, Net_timeout_t timeout)
{
  int status;
  int n = readline_netstream_raw(ns, buffer, boff, bsize, timeout, &status);

//log_printf(15, "readline_netstream: ns=%d status=%d\n", ns_getid(ns), status);
  if (status == 1) {
    n = 0;
  } else if (status == -1) {
    n = -1;
  } else if (status == 0) {
    n = 1;
  }

  return(n);
}

//*********************************************************************
//  accept_pending_connection - Accepts a pending connection and stores
//    it in the provided ns.  The ns should be uninitialize, ie closed
//    since the sock structure is inherited from the server ports
//    ns type
//*********************************************************************

int accept_pending_connection(Network_t *net, NetStream_t *ns)
{
   int i, j, k, err;
   ns_monitor_t *nm = NULL;

   //** Get the global settings
   apr_thread_mutex_lock(net->ns_lock);

   err = 0;
   //** Find the port.  Make sure and use the next port in the list
   j = -1;
   k = net->monitor_index % net->used_ports; 
   for (i=0; i<net->used_ports; i++) {
      k = (i + net->monitor_index) % net->used_ports;
      nm = &(net->nm[k]);
      apr_thread_mutex_lock(nm->lock);
      if (nm->is_pending == 1) {   //** Found a slot
         j = k; 
         break;  
      }
      apr_thread_mutex_unlock(nm->lock);
   }

   net->monitor_index = (k + 1) % net->used_ports;

   //** Check if there is nothing to do.
   if (j == -1) {   
       apr_thread_mutex_unlock(net->ns_lock);
       return(1);
   }

   ns_clone(ns, nm->ns);  //** Clone the settings
   ns->nm = nm;           //** Specify the bind accepted

   ns->sock = nm->ns->accept(nm->ns->sock);   //** Accept the connection
   if (ns->sock == NULL) err = 1;

   nm->is_pending = 0;                  //** Clear the pending flag
   net->accept_pending--;
   if (net->accept_pending < 0) net->accept_pending = 0;
   apr_thread_mutex_unlock(net->ns_lock);

   apr_thread_cond_signal(nm->cond);    //** Wake up the pending monitor thread
   apr_thread_mutex_unlock(nm->lock);   //** This was locked in the fop loop above

   if (err == 0) {   
      ns->id = ns_generate_id();
      ns->set_peer(ns->sock, ns->peer_address, sizeof(ns->peer_address));

      log_printf(10, "accept_pending_connection: Got a new connection from %s! Storing in ns=%d \n", ns->peer_address, ns->id);
   } else {
      log_printf(10, "accept_pending_connection: Failed getting a new connection\n");
   }

   return(err);
}


//*********************************************************************
// wait_for_connection - Waits for a new connection
//*********************************************************************

int wait_for_connection(Network_t *net, int max_wait)
{
  apr_time_t t;
  apr_time_t end_time = apr_time_now() + apr_time_make(max_wait, 0);
  int n;

log_printf(15, "wait_for_connection: max_wait=%d starttime=" TT " endtime=" TT "\n", max_wait, apr_time_now(), end_time);
  apr_thread_mutex_lock(net->ns_lock);

log_printf(15, "wait_for_connection: accept_pending=%d\n", net->accept_pending);
 
  while ((end_time > apr_time_now()) && (net->accept_pending == 0)) {
//    log_printf(15, "wait_for_connection: accept_pending=%d time=" TT "\n", net->accept_pending, apr_time_now());
    set_net_timeout(&t, 1, 0);  //** Wait for at least 1 second
    apr_thread_cond_timedwait(net->cond, net->ns_lock, t);
  }   

  log_printf(15, "wait_for_connection: exiting loop accept_pending=%d time=" TT "\n", net->accept_pending, apr_time_now());

  n = net->accept_pending;
//  net->accept_pending--;
//  if (net->accept_pending < 0) net->accept_pending = 0;
  apr_thread_mutex_unlock(net->ns_lock);

  return(n);  
}

//*********************************************************************
// wakeup_network - Wakes up the network monitor thread
//*********************************************************************

void wakeup_network(Network_t *net)
{
   apr_thread_mutex_lock(net->ns_lock);
   net->accept_pending++;
   apr_thread_cond_signal(net->cond);
   apr_thread_mutex_unlock(net->ns_lock);
}

