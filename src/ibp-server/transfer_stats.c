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

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <apr_thread_mutex.h>
#include <apr_pools.h>
#include <ibp-server/ibp_server.h>
#include "ibp_time.h"
#include <tbx/network.h>
#include <tbx/log.h>

typedef struct {
   Transfer_stat_t *table;  //** This is where we store everything
   int pos;                 //** Current position
   int size;                //** Size of the stats
   char *host;              //** Depot IP address
   apr_thread_mutex_t *lock;
   apr_pool_t *pool;
   uint64_t read_bytes, write_bytes, copy_bytes;  //** total amount of data R/W
} Stats_t;

Stats_t stats;    //** Global container

//****************************************************************
//  init_stats - Inits the stats data structures
//****************************************************************

void init_stats(int n)
{
  int i;

  assert((stats.table = (Transfer_stat_t *)malloc(sizeof(Transfer_stat_t)*n)) != NULL);
  
  apr_pool_create(&(stats.pool), NULL);
  apr_thread_mutex_create(&(stats.lock), APR_THREAD_MUTEX_DEFAULT,stats.pool);

  stats.host = strdup(global_config->server.iface[0].hostname);
  stats.pos = 0;
  stats.size = n;
  stats.read_bytes = 0;
  stats.write_bytes = 0;
  stats.copy_bytes = 0;

  for (i=0; i<stats.size; i++) {
     stats.table[i].start = 0;
     stats.table[i].end = 0;
  }  
}

//****************************************************************
//  free_stats - Frees the stats data structures
//****************************************************************

void free_stats()
{
  free(stats.table);
  apr_thread_mutex_destroy(stats.lock);
  apr_pool_destroy(stats.pool);
}

//****************************************************************
// clear_stat - Wipes a stats contents
//****************************************************************

void clear_stat(Transfer_stat_t *s)
{
  memset((void *)s, 0, sizeof(Transfer_stat_t));
}

//****************************************************************
// get_transfer_stats - Returns the amount of data R/W
//****************************************************************

void get_transfer_stats(uint64_t *rbytes, uint64_t *wbytes, uint64_t *copybytes)
{
   apr_thread_mutex_lock(stats.lock);
   *rbytes = stats.read_bytes;
   *wbytes = stats.write_bytes;
   *copybytes = stats.copy_bytes;
   apr_thread_mutex_unlock(stats.lock);
}

//****************************************************************
// add_stat - Adds a completed task to the stats list
//****************************************************************

void add_stat(Transfer_stat_t *s)
{
   Transfer_stat_t *entry;

   apr_thread_mutex_lock(stats.lock);

   //** Update the global bytes transfered
   if (s->dir == DIR_IN) {
      stats.write_bytes += s->nbytes;   //** This is not from the NIC but from the apps standpoint
   } else {
      stats.read_bytes += s->nbytes;
      if (s->dir == DEPOT_COPY_OUT) stats.copy_bytes += s->nbytes;
   }

   entry = &(stats.table[stats.pos]);
   *entry = *s;
   entry->end = ibp_time_now();
   log_printf(15, "add_stat: i=%d host=%s start=%d end=%d nbytes=%d dir=%d\n", 
        stats.pos, entry->address, entry->start, entry->end, entry->nbytes, entry->dir);
   stats.pos = (stats.pos + 1) % stats.size;
   clear_stat(s);
   apr_thread_mutex_unlock(stats.lock);
}

//****************************************************************
// send_stats - Sends the stats to the selected ns
//****************************************************************

int send_stats(tbx_ns_t *ns, ibp_time_t start_time, tbx_ns_timeout_t dt)
{
  int bufsize = 1024*1024;
  int nmax = bufsize - 200;
  char buffer[bufsize];
  char *sptr;
  int i, j, n, nchar, nw, currtime;
  Transfer_stat_t *s;
  Transfer_stat_t ts;

  apr_thread_mutex_lock(stats.lock);

  //** Send all the ops currently in play
  sptr = buffer;
  nchar = 0;
  currtime = ibp_time_now();
  n = sprintf(sptr, "%d\n", currtime);
  sptr = sptr + n;
  nchar += n;

  for (i=0; i < global_config->server.max_threads; i++) {
     ts = global_task[i].stat;
     if (ts.start > 0) {
       if (ts.dir == DIR_IN) {
          n = snprintf(sptr, sizeof(buffer) - nchar, "%s %s %d %d %d %d 0\n", ts.address, stats.host, ts.start, currtime, ts.nbytes, ts.id);
       } else {
          n = snprintf(sptr, sizeof(buffer) - nchar, "%s %s %d %d %d %d 0\n", stats.host, ts.address, ts.start, currtime, ts.nbytes, ts.id);
       }

       sptr = sptr + n;
       nchar += n;
     }
  }

  //** Now iterate through the history
  i = stats.pos-1; j = 0;
  s = stats.table;

log_printf(15, "send_stats: ns=%d start=%lu s[i].start=%d\n", tbx_ns_getid(ns), start_time, s[i].start);
  while ((s[i].start > start_time) && (j != stats.size)) {
     if (ts.dir == DIR_IN) {
        n = snprintf(sptr, sizeof(buffer) - nchar, "%s %s %d %d %d %d 1\n", s[i].address, stats.host, s[i].start, s[i].end, s[i].nbytes, s[i].id);
     } else {
        n = snprintf(sptr, sizeof(buffer) - nchar, "%s %s %d %d %d %d 1\n", stats.host, s[i].address, s[i].start, s[i].end, s[i].nbytes, s[i].id);
     }

//log_printf(15, "send_stats: ns=%d pos=%d  str=%s\n", tbx_ns_getid(ns), i, sptr);
     
     sptr = sptr + n;
     nchar += n;

     if (nchar > nmax) {
//log_printf(15, "send_stats: internal ns=%d sending: %s\n", tbx_ns_getid(ns), buffer);
        n = 0;
        do {
          nw = server_ns_write(ns, &(buffer[n]), nchar, dt);
          if (nw > 0) { nchar -= nw; n = n + nw; }
        } while ((nw > 0) && (nchar > 0));

        if (nw < 0) { //** dead connection
           apr_thread_mutex_unlock(stats.lock);
           return(-1); 
        }

        sptr = buffer;
     }

     i--;  if (i<0) i = stats.size;
     j++;
  }

  n = snprintf(sptr, sizeof(buffer) - nchar, "\n");
  sptr = sptr + n;
  nchar = nchar + n;  

  log_printf(15, "send_stats: ns=%d nchar=%d strlen=" ST " sending: %s\n", tbx_ns_getid(ns), nchar, strlen(buffer), buffer);

  n = 0;
  do {
     nw = server_ns_write(ns, &(buffer[n]), nchar, dt);
     if (nw > 0) { nchar -= nw; n = n + nw; }
  } while ((nw > 0) && (nchar > 0));

  apr_thread_mutex_unlock(stats.lock);

  if (nw < 0) { return(-1); }  //** dead connection

  return(0);
}

