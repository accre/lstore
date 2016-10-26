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

#include <apr_time.h>
#include <tbx/chksum.h>
#include "ibp_time.h"
#include "allocation.h"
#include "cap_timestamp.h"
#include <tbx/fmttypes.h>
#include <tbx/log.h>
#include <tbx/append_printf.h>
#include "subnet.h"
#include <ibp-server/print_alloc.h>

//****************************************************************************
// print_manage_history - Prints the manage history to the buffer
//****************************************************************************

void print_manage_history(char *buffer, int *used, int nbytes, Allocation_manage_ts_t *ts_list, int start)
{
  char print_time[128];
  char print_time2[128];
  char hostip[256];
  char subbuf[256];
  char cmdbuf[256];
  char relbuf[256];
  const char *cmd, *subcmd, *rel;
  apr_time_t t, t2;
  int i, slot, n;
  Allocation_manage_ts_t *ts;

  for (i=0; i<ALLOC_HISTORY; i++) {
     slot = (i + start) % ALLOC_HISTORY;
     ts = &(ts_list[slot]);

     t = ibp2apr_time(ts->ts.time);
     if (t != 0) {
        switch (ts->subcmd) {
          case IBP_PROBE: subcmd = "IBP_PROBE   "; break;        
          case IBP_INCR : subcmd = "IBP_INCR    "; break;        
          case IBP_DECR : subcmd = "IBP_DECR    "; break;        
          case IBP_CHNG : subcmd = "IBP_CHNG    "; break;        
          case IBP_TRUNCATE : subcmd = "IBP_TRUNCATE"; break;        
          default : 
             sprintf(subbuf, "UNKNOWN(%d)", ts->subcmd);
             subcmd = subbuf;
        }
    
        switch (ts->cmd) {
          case IBP_MANAGE:         cmd = "IBP_MANAGE        "; break;        
          case IBP_ALIAS_MANAGE:   cmd = "IBP_ALIAS_MANAGE  "; break;        
          case IBP_RENAME:         cmd = "IBP_RENAME        "; subcmd = "        "; break;
          case IBP_SPLIT_ALLOCATE: cmd = "IBP_SPLIT_ALLOCATE"; subcmd = "        "; break;
          case IBP_ALIAS_ALLOCATE: cmd = "IBP_ALIAS_ALLOCATE"; subcmd = "        "; break;
          default : 
             sprintf(cmdbuf, "UNKNOWN(%d)", ts->cmd);
             cmd = cmdbuf;
        }
        switch (ts->reliability) {
          case ALLOC_HARD: rel = "IBP_HARD"; break;        
          case ALLOC_SOFT: rel = "IBP_SOFT"; break;
          default :
             n = ts->reliability;
             sprintf(relbuf, "UNKNOWN(%d)", n);
             rel = relbuf;
        }
       
        apr_ctime(print_time, t);
        t2 = ibp2apr_time(ts->expiration);
        apr_ctime(print_time2, t2);
        address2ipdecstr(hostip, ts->ts.host.ip, ts->ts.host.atype);
        if ((ts->cmd == IBP_ALIAS_ALLOCATE) || (ts->cmd == IBP_ALIAS_MANAGE)) {
          tbx_append_printf(buffer, used, nbytes, "   " TT " * %s  * %s * " LU " * %s * %s * " LU " * " LU " * " TT " * %s\n", t, print_time, hostip, ts->id, cmd, subcmd, ts->reliability, ts->size, t2, print_time2);
        } else {
          tbx_append_printf(buffer, used, nbytes, "   " TT " * %s  * %s * " LU " * %s * %s * %s * " LU " * " TT " * %s\n", t, print_time, hostip, ts->id, cmd, subcmd, rel, ts->size, t2, print_time2);
        }
     }
  }
}

//****************************************************************************
// print_rw_history - Prints the Read/Write history to the buffer
//****************************************************************************

void print_rw_history(char *buffer, int *used, int nbytes, Allocation_rw_ts_t *ts_list, int start)
{
  char print_time[128];
  char hostip[256];
  apr_time_t t;
  int i, slot;
  Allocation_rw_ts_t *ts;

  for (i=0; i<ALLOC_HISTORY; i++) {
     slot = (i + start) % ALLOC_HISTORY;
     ts = &(ts_list[slot]);

     t = ibp2apr_time(ts->ts.time);
     if (t != 0) {
        apr_ctime(print_time, t);
        address2ipdecstr(hostip, ts->ts.host.ip, ts->ts.host.atype);
        tbx_append_printf(buffer, used, nbytes, "   " TT " * %s * %s * " LU " * " LU " * " LU "\n", t, print_time, hostip, ts->id, ts->offset, ts->size);
     }
  }
}

//****************************************************************************
// print_allocation - Prints the allocation to the buffer
//****************************************************************************

void print_allocation(char *buffer, int *used, int nbytes, Allocation_t *a, Allocation_history_t *h,
      int state, int cs_type, osd_off_t hbs, osd_off_t bs)
{
  apr_time_t t;
  char print_time[128];
  char hostip[256];
  int i, n;
  tbx_chksum_t cs;


  tbx_append_printf(buffer, used, nbytes, "Allocation summary\n");
  tbx_append_printf(buffer, used, nbytes, "-----------------------------------------\n");
  tbx_append_printf(buffer, used, nbytes, "ID: " LU "\n", a->id); 
  tbx_append_printf(buffer, used, nbytes, "Master ID: " LU " (only used if split)\n", a->split_parent_id); 
  t = ibp2apr_time(a->creation_ts.time);
  apr_ctime(print_time, t);
  address2ipdecstr(hostip, a->creation_ts.host.ip, a->creation_ts.host.atype);
  tbx_append_printf(buffer, used, nbytes, "Created on: " TT " -- %s by host %s\n", t, print_time, hostip);

  address2ipdecstr(hostip, a->creation_cert.ca_host.ip, a->creation_cert.ca_host.atype);
  tbx_append_printf(buffer, used, nbytes, "Certifed by %s with CA id: " LU ":" LU ":" LU ":" LU " (", hostip, 
       a->creation_cert.id[0], a->creation_cert.id[1], a->creation_cert.id[2], a->creation_cert.id[3]);
  for (i=0; i<32-1; i++) { n = (unsigned char)(a->creation_cert.cid[i]); tbx_append_printf(buffer, used, nbytes, "%d.", n); };
  n = (unsigned char)(a->creation_cert.cid[15]); tbx_append_printf(buffer, used, nbytes, "%d)\n", n); 
  
  t = ibp2apr_time(a->expiration);
  apr_ctime(print_time, t); 
  if (apr_time_now() > t) {
     tbx_append_printf(buffer, used, nbytes, "Expiration: " TT " -- %s (EXPIRED)\n", t, print_time);
  } else {
     tbx_append_printf(buffer, used, nbytes, "Expiration: " TT " -- %s\n", t, print_time);
  }

  tbx_append_printf(buffer, used, nbytes, "is_alias: %d\n", a->is_alias);
  tbx_append_printf(buffer, used, nbytes, "Read cap: %s\n", a->caps[READ_CAP].v);
  tbx_append_printf(buffer, used, nbytes, "Write cap: %s\n", a->caps[WRITE_CAP].v);
  tbx_append_printf(buffer, used, nbytes, "Manage cap: %s\n", a->caps[MANAGE_CAP].v);

  switch (a->type) {
    case IBP_BYTEARRAY: tbx_append_printf(buffer, used, nbytes, "Type: IBP_BYTEARRAY\n"); break;
    case IBP_BUFFER:    tbx_append_printf(buffer, used, nbytes, "Type: IBP_BUFFER\n"); break;
    case IBP_FIFO:      tbx_append_printf(buffer, used, nbytes, "Type: IBP_FIFO\n"); break;
    case IBP_CIRQ:      tbx_append_printf(buffer, used, nbytes, "Type: IBP_CIRQ\n"); break;
    default:            tbx_append_printf(buffer, used, nbytes, "Type: (%d) UNKNOWN TYPE!!!! \n", a->type); break;
  }

  switch (a->reliability) {
    case ALLOC_HARD: tbx_append_printf(buffer, used, nbytes, "Reliability: IBP_HARD\n"); break;
    case ALLOC_SOFT: tbx_append_printf(buffer, used, nbytes, "Reliability: IBP_SOFT\n"); break;
    default: tbx_append_printf(buffer, used, nbytes, "Reliability: (%d) UNKNOWN RELIABILITY!!!\n", a->reliability);
  }

  tbx_append_printf(buffer, used, nbytes, "Current size: " LU "\n", a->size);
  tbx_append_printf(buffer, used, nbytes, "Max size: " LU "\n", a->max_size);
  tbx_append_printf(buffer, used, nbytes, "Read pos: " LU "\n", a->r_pos);
  tbx_append_printf(buffer, used, nbytes, "Write pos: " LU "\n", a->w_pos);
  tbx_append_printf(buffer, used, nbytes, "Read ref count: %u\n", a->read_refcount);
  tbx_append_printf(buffer, used, nbytes, "Write ref count: %u\n", a->write_refcount);

  if (a->is_alias) {
     tbx_append_printf(buffer, used, nbytes, "Alias offset: " LU "\n", a->alias_offset);
     tbx_append_printf(buffer, used, nbytes, "Alias size: " LU "\n", a->alias_size);
     tbx_append_printf(buffer, used, nbytes, "Alias ID: " LU "\n", a->alias_id);

  }

  //** Print the chksum info if available
  tbx_append_printf(buffer, used, nbytes, "\n");
  tbx_blank_chksum_set(&cs);
  if (tbx_chksum_type_valid(cs_type) == 1) {
     tbx_chksum_set(&cs, cs_type);
     i = tbx_chksum_size(&cs, CHKSUM_DIGEST_BIN);
     n = tbx_chksum_size(&cs, CHKSUM_DIGEST_HEX);
     tbx_append_printf(buffer, used, nbytes, "State: %d\n", state);
     tbx_append_printf(buffer, used, nbytes, "Chksum Type: %s(%d)  Binary length: %d   Hex length: %d\n", tbx_chksum_name(&cs), 
        cs_type, i, n);
     tbx_append_printf(buffer, used, nbytes, "Chksum Header size: " I64T "   Block size: " I64T "\n", hbs, bs);      
  } else {
     tbx_append_printf(buffer, used, nbytes, "State: %d\n", state);
     tbx_append_printf(buffer, used, nbytes, "Chksum Type: %s(%d)\n", tbx_chksum_name(&cs), CHKSUM_NONE);
  }

  tbx_append_printf(buffer, used, nbytes, "\n");
  tbx_append_printf(buffer, used, nbytes, "Read history (slot=%d) (epoch, time, host, id, offset, size)\n", h->read_slot);
  tbx_append_printf(buffer, used, nbytes, "---------------------------------------------\n");
  print_rw_history(buffer, used, nbytes, h->read_ts, h->read_slot);

  tbx_append_printf(buffer, used, nbytes, "\n");
  tbx_append_printf(buffer, used, nbytes, "Write history (slot=%d) (epoch, time, host, id, offset, size)\n", h->write_slot);
  tbx_append_printf(buffer, used, nbytes, "---------------------------------------------\n");
  print_rw_history(buffer, used, nbytes, h->write_ts, h->write_slot);

  tbx_append_printf(buffer, used, nbytes, "\n");
  tbx_append_printf(buffer, used, nbytes, "Manage history (slot=%d) (epoch, time, host, id, cmd, subcmd, reliability, size, expiration_epoch, expiration)\n", h->manage_slot);
  tbx_append_printf(buffer, used, nbytes, "---------------------------------------------\n");
  print_manage_history(buffer, used, nbytes, h->manage_ts, h->manage_slot);

  return;
}

