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

//*****************************************************************
//*****************************************************************

#include <assert.h>
#include <string.h>
#include <limits.h>
#include <time.h>
#include <apr_thread_proc.h>
#include <apr_thread_mutex.h>
#include <tbx/dns_cache.h>
#include "ibp_ClientLib.h"
#include <ibp-server/ibp_server.h>
#include <tbx/log.h>
#include "debug.h"
#include "allocation.h"
#include "resource.h"
#include "db_resource.h"
#include <tbx/network.h>
#include <tbx/net_sock.h>
//FIXME #include "net_phoebus.h"
#include "ibp_task.h"
#include "ibp_protocol.h"
#include "ibp-server_version.h"
#include "lock_alloc.h"
#include "cap_timestamp.h"
#include "activity_log.h"
#include <tbx/fmttypes.h>

//*****************************************************************
// handle_allocate - Processes the allocate command
//
// Results:
//    status readCap writeCap manageCap
//*****************************************************************

int handle_allocate(ibp_task_t *task)
{
   int d, err, got_lock=0;
   osd_id_t cid = 3862277;
   Resource_t *res;
   char token[4096];
   Allocation_t a, ma;
   Cmd_state_t *cmd = &(task->cmd);

   debug_printf(1, "handle_allocate: Starting to process command\n");

   Allocation_t *alloc = &(cmd->cargs.allocate.a);
   rid_t *rid = &(cmd->cargs.allocate.rid);
   Cmd_allocate_t *ca = &(cmd->cargs.allocate);

   err = 0;

   if (ibp_rid_is_empty(*rid)) { //** Pick a random resource to use
       resource_pick(global_config->rl, rid);
       log_printf(10, "handle_allocate: Picking random resource %s\n", ibp_rid2str(*rid, token));
   }

      //** Check the resource **
   res = resource_lookup(global_config->rl, ibp_rid2str(*rid, token));
   if (res == NULL) {    //**Can't find the resource
      log_printf(1, "handle_allocate: Invalid resource: %s\n", ibp_rid2str(*rid, token));
      alog_append_ibp_allocate(task->myid, -1, alloc->max_size, alloc->type, alloc->reliability, alloc->expiration);
      send_cmd_result(task, IBP_E_INVALID_RID);
      return(global_config->soft_fail);
   }

   //** Check the duration **
   if (alloc->expiration == INT_MAX) {
     alloc->expiration = res->max_duration + ibp_time_now();
   }

   if ((cmd->command == IBP_SPLIT_ALLOCATE) || (cmd->command == IBP_SPLIT_ALLOCATE_CHKSUM)) {
      //** Get the Master allocation ***
      if ((err = get_allocation_by_cap_resource(res, MANAGE_CAP, &(cmd->cargs.allocate.master_cap), &ma)) != 0) {
         log_printf(10, "handle_split_allocate: Invalid master cap: %s rid=%s\n", cmd->cargs.allocate.master_cap.v, res->name);
         send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
         return(global_config->soft_fail);
      }

      alog_append_ibp_split_allocate(task->myid, res->rl_index, ma.id, alloc->max_size, alloc->type, alloc->reliability, alloc->expiration);

      lock_osd_id(ma.id);  //** Redo the get allocation again with the lock enabled ***
      got_lock = 1;
      if ((err = get_allocation_by_cap_resource(res, MANAGE_CAP, &(cmd->cargs.allocate.master_cap), &ma)) != 0) {
         log_printf(10, "handle_split_allocate: Invalid master cap: %s rid=%s\n", cmd->cargs.allocate.master_cap.v, res->name);
         unlock_osd_id(ma.id);
         send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
         return(global_config->soft_fail);
      }
   } else {
      alog_append_ibp_allocate(task->myid, res->rl_index, alloc->max_size, alloc->type, alloc->reliability, alloc->expiration);
   }

   d = alloc->expiration - ibp_time_now();
   debug_printf(10, "handle_allocate:  expiration: %u (%d sec from now)\n", alloc->expiration, d); flush_debug();

   if (res->max_duration < d) {
       log_printf(1, "handle_allocate: Duration(%d sec) exceeds that for RID %s of %d sec\n", d, res->name, res->max_duration);
       if (got_lock == 1) unlock_osd_id(ma.id);
       send_cmd_result(task, IBP_E_LONG_DURATION);
       return(global_config->soft_fail);
   }

   //** Perform the allocation **
   d = (global_config->server.lazy_allocate == 1) ? 0 : 1;
   if ((cmd->command == IBP_SPLIT_ALLOCATE) || (cmd->command == IBP_SPLIT_ALLOCATE_CHKSUM)) {
      if ((d = split_allocation_resource(res, &ma, &a, alloc->max_size, alloc->type, alloc->reliability, alloc->expiration, 0, d, ca->cs_type, ca->cs_blocksize)) != 0) {
         log_printf(1, "handle_allocate: split_allocation_resource failed on RID %s!  Error=%d\n", res->name, d);
         unlock_osd_id(ma.id);  //** Now we can unlock the master
         send_cmd_result(task, IBP_E_WOULD_EXCEED_LIMIT);
         return(global_config->soft_fail);
      }

      //** Update the master's manage timestamp
      update_manage_history(res, ma.id, ma.is_alias, &(task->ipadd), cmd->command, 0, a.reliability, a.expiration, a.max_size, 0);

      unlock_osd_id(ma.id);  //** Now we can unlock the master
   } else {
      if ((d = create_allocation_resource(res, &a, alloc->max_size, alloc->type, alloc->reliability, alloc->expiration, 0, d, ca->cs_type, ca->cs_blocksize)) != 0) {
         log_printf(1, "handle_allocate: create_allocation_resource failed on RID %s!  Error=%d\n", res->name, d);
         send_cmd_result(task, IBP_E_WOULD_EXCEED_LIMIT);
         return(global_config->soft_fail);
      }
  }

   if (a.type != IBP_BYTEARRAY) {  //** Set the size to be 0 for all the Queue types
     a.size = 0;
     a.w_pos = 0;
     a.r_pos = 0;
   }

   //** Store the creation timestamp **
   set_alloc_timestamp(&(a.creation_ts), &(task->ipadd));
//log_printf(15, "handle_allocate: create time =" TT " ns=%d\n", a.creation_ts.time, tbx_ns_getid(task->ns));
   err = modify_allocation_resource(res, a.id, &a);
   if (err != 0) {
      log_printf(0, "handle_allocate:  Error with modify_allocation_resource for new queue allocation!  err=%d, type=%d\n", err, a.type); 
   }

   //** Send the result back **
   if (global_config->server.return_cap_id == 1) cid = a.id;
   tbx_ns_monitor_t *nm = tbx_ns_monitor_get(task->ns);
   snprintf(token, sizeof(token), "%d ibp://%s:%d/%s#%s/" LU "/READ "
       "ibp://%s:%d/%s#%s/" LU "/WRITE "
       "ibp://%s:%d/%s#%s/" LU "/MANAGE \n",
       IBP_OK,
       tbx_nm_host_get(nm), tbx_nm_port_get(nm), res->name, a.caps[READ_CAP].v, cid,
       tbx_nm_host_get(nm), tbx_nm_port_get(nm), res->name, a.caps[WRITE_CAP].v, cid,
       tbx_nm_host_get(nm), tbx_nm_port_get(nm), res->name, a.caps[MANAGE_CAP].v, cid);

   tbx_ns_timeout_t dt;
   convert_epoch_time2net(&dt, task->cmd_timeout);

   debug_code(apr_time_t tt=apr_time_now();)
   debug_printf(1, "handle_allocate: before sending result time: " TT "\n", tt);
   err = server_ns_write_block(task->ns, task->cmd_timeout, token, strlen(token));

   alog_append_osd_id(task->myid, a.id);

   debug_printf(1, "handle_allocate: Allocation: %s", token);

   debug_code(
      if (debug_level() > 5) print_allocation_resource(res, log_fd(), &a);
   )

//Allocation_history_t h1;
//get_history_table(res, a.id, &h1);
//log_printf(0, "handle_allocate history: r=%s id=" LU " h.id=" LU " write_slot=%d\n", res->name, a.id, h1.id, h1.write_slot);

   return(err);
}

//*****************************************************************
//  handle_merge - Merges 2 allocations
//*****************************************************************

int handle_merge(ibp_task_t *task)
{
   int err;
   Resource_t *r;
   Allocation_t ma, ca; 
   Cmd_state_t *cmd = &(task->cmd);
   Cmd_merge_t *op = &(cmd->cargs.merge);

   debug_printf(1, "handle_merge: Starting to process command\n");

      //** Check the resource **
   r = resource_lookup(global_config->rl, op->crid);
   if (r == NULL) {    //**Can't find the resource
      log_printf(1, "handle_merge: Invalid resource: %s\n", op->crid);
      alog_append_ibp_merge(task->myid, 0, 0, -1);
      send_cmd_result(task, IBP_E_INVALID_RID);
      return(global_config->soft_fail);
   }   

   //** Get the master allocation ***
  if ((err = get_allocation_by_cap_resource(r, MANAGE_CAP, &(op->mkey), &ma)) != 0) {
     log_printf(10, "handle_merge: Invalid mcap: %s rid=%s\n", op->mkey.v, r->name);
     alog_append_ibp_merge(task->myid, 0, 0, r->rl_index);
     send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
     return(global_config->soft_fail);
  }

  //** and the child allocation
  if ((err = get_allocation_by_cap_resource(r, MANAGE_CAP, &(op->ckey), &ca)) != 0) {
     log_printf(10, "handle_merge: Invalid childcap: %s rid=%s\n", op->ckey.v, r->name);
     alog_append_ibp_merge(task->myid, ma.id, 0, r->rl_index);
     send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
     return(global_config->soft_fail);
  }

  alog_append_ibp_merge(task->myid, ma.id, ca.id, r->rl_index);

  //** Now do the same thing with locks enabled
  lock_osd_id_pair(ma.id, ca.id);

   //** Get the master allocation ***
  if ((err = get_allocation_by_cap_resource(r, MANAGE_CAP, &(op->mkey), &ma)) != 0) {
     log_printf(10, "handle_merge: Invalid mcap: %s rid=%s\n", op->mkey.v, r->name);
     alog_append_ibp_merge(task->myid, 0, 0, r->rl_index);
     unlock_osd_id_pair(ma.id, ca.id);
     send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
     return(global_config->soft_fail);
  }

  //** and the child allocation
  if ((err = get_allocation_by_cap_resource(r, MANAGE_CAP, &(op->ckey), &ca)) != 0) {
     log_printf(10, "handle_merge: Invalid childcap: %s rid=%s\n", op->ckey.v, r->name);
     alog_append_ibp_merge(task->myid, ma.id, 0, r->rl_index);
     unlock_osd_id_pair(ma.id, ca.id);
     send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
     return(global_config->soft_fail);
  }

   //** Update the manage timestamp
   update_manage_history(r, ca.id, ca.is_alias, &(task->ipadd), cmd->command, 0, 0, 0, ca.max_size, 0);

   //** Check the child ref count
   if ((ca.read_refcount == 1) && (ca.write_refcount == 0)) {
      //** merge the allocation **
      if ((err = merge_allocation_resource(r, &ma, &ca)) != 0) {
         log_printf(1, "handle_merge: merge_allocation_resource failed on RID %s!  Error=%d\n", r->name, err);
         unlock_osd_id_pair(ma.id, ca.id);   
         send_cmd_result(task, IBP_E_GENERIC);
         return(global_config->soft_fail);
      }
   } else {
      log_printf(15, "handle_merge: Bad child refcount! RID=%s  ca.read=%d ca.write=%d\n", r->name, ca.read_refcount, ca.write_refcount);
      unlock_osd_id_pair(ma.id, ca.id);   
      send_cmd_result(task, IBP_E_GENERIC);
      return(global_config->soft_fail);
   }

   unlock_osd_id_pair(ma.id, ca.id);   

   send_cmd_result(task, IBP_OK);

   debug_printf(1, "handle_merge: completed\n");
   return(0);
}

//*****************************************************************
// handle_rename - Processes the allocation rename command
//
// Results:
//    status readCap writeCap manageCap
//*****************************************************************

int handle_rename(ibp_task_t *task)
{
   int d, err;
   Resource_t *res;

   char token[4096];
   Allocation_t a; 
   Cmd_state_t *cmd = &(task->cmd);
   Cmd_manage_t *manage = &(cmd->cargs.manage);

   debug_printf(1, "handle_rename: Starting to process command\n");

   err = 0;

      //** Check the resource **
   res = resource_lookup(global_config->rl, ibp_rid2str(manage->rid, token));
   if (res == NULL) {    //**Can't find the resource
      log_printf(1, "handle_allocate: Invalid resource: %s\n", ibp_rid2str(manage->rid, token));
      alog_append_ibp_rename(task->myid, -1, 0);
      send_cmd_result(task, IBP_E_INVALID_RID);
      return(global_config->soft_fail);
   }   

   //** Get the allocation ***
  if ((err = get_allocation_by_cap_resource(res, MANAGE_CAP, &(manage->cap), &a)) != 0) {
     log_printf(10, "handle_rename: Invalid cap: %s rid=%s\n", manage->cap.v, res->name);
     alog_append_ibp_rename(task->myid, -1, 0);
     send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
     return(global_config->soft_fail);
  }

  alog_append_ibp_rename(task->myid, res->rl_index, a.id);

  lock_osd_id(a.id);

   //** Get the allocation again with the lock enabled ***
  if ((err = get_allocation_by_cap_resource(res, MANAGE_CAP, &(manage->cap), &a)) != 0) {
     log_printf(10, "handle_rename: Invalid cap: %s rid=%s\n", manage->cap.v, res->name);
     send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
     unlock_osd_id(a.id);
     return(global_config->soft_fail);
  }

   //** Update the manage timestamp
   update_manage_history(res, a.id, a.is_alias, &(task->ipadd), cmd->command, 0, a.reliability, a.expiration, a.max_size, 0);

   //** Rename the allocation **
   if ((d = rename_allocation_resource(res, &a)) != 0) {
      log_printf(1, "handle_rename: rename_allocation_resource failed on RID %s!  Error=%d\n", res->name, d);
      send_cmd_result(task, IBP_E_GENERIC);
      unlock_osd_id(a.id);
      return(global_config->soft_fail);
   }

   unlock_osd_id(a.id);

   //** Send the result back **
   //** Send the result back **
   tbx_ns_monitor_t *nm = tbx_ns_monitor_get(task->ns);
   snprintf(token, sizeof(token), "%d ibp://%s:%d/%s#%s/3862277/READ "
       "ibp://%s:%d/%s#%s/3862277/WRITE "
       "ibp://%s:%d/%s#%s/3862277/MANAGE \n",
       IBP_OK,
       tbx_nm_host_get(nm), tbx_nm_port_get(nm), res->name, a.caps[READ_CAP].v,
       tbx_nm_host_get(nm), tbx_nm_port_get(nm), res->name, a.caps[WRITE_CAP].v,
       tbx_nm_host_get(nm), tbx_nm_port_get(nm), res->name, a.caps[MANAGE_CAP].v);

   debug_code(time_t tt=ibp_time_now();)
   debug_printf(1, "handle_rename: before sending result time: %s\n", ctime(&tt));
   err = server_ns_write_block(task->ns, task->cmd_timeout, token, strlen(token));

   alog_append_osd_id(task->myid, a.id);

   debug_printf(1, "handle_rename: Allocation: %s", token);

   debug_code(
      if (debug_level() > 5) print_allocation_resource(res, log_fd(), &a);
   )
   return(err);
}

//*****************************************************************
// handle_internal_get_corrupt - Processes the internal routine returning
//         the list of corrupt ID's
//
// Results:
//    status n_allocs\n
//    alloc_1\n
//    alloc_2\n
//      ...
//    alloc_n\n
//*****************************************************************

int handle_internal_get_corrupt(ibp_task_t *task)
{
   int bufsize = 1024*4;
   char buffer[bufsize];
   int err;
   osd_id_t id;
   osd_off_t n;
   osd_iter_t *cit;
   Resource_t *res;
   
   Cmd_state_t *cmd = &(task->cmd);
   Cmd_internal_get_alloc_t *arg = &(cmd->cargs.get_alloc);

   debug_printf(1, "handle_internal_get_corrupt: Starting to process command\n");

   err = 0;

      //** Check the resource **
   res = resource_lookup(global_config->rl, ibp_rid2str(arg->rid, buffer));
   if (res == NULL) {    //**Can't find the resource
      log_printf(1, "handle_internal_get_corrupt: Invalid resource: %s\n", ibp_rid2str(arg->rid, buffer));
//      alog_append_internal_get_corrupt(task->myid, -1, 0);
      send_cmd_result(task, IBP_E_INVALID_RID);
      return(global_config->soft_fail);
   }   

   n = osd_get_corrupt_count(res->dev);   //** Get how many are corrupt

   //*** Send back the results ***
   sprintf(buffer, "%d " I64T " \n",IBP_OK, n); 
   err = server_ns_write_block(task->ns, task->cmd_timeout, buffer, strlen(buffer));

   //** Now iterate over all the elements
   if (n > 0) {
      err = 0;
      cit = osd_new_corrupt_iterator(res->dev);
      while ((osd_corrupt_iterator_next(cit, &id) == 0) && (err == 0)) {
        sprintf(buffer, LU "\n", id); 
        err = server_ns_write_block(task->ns, task->cmd_timeout, buffer, strlen(buffer));          
      }
      osd_destroy_corrupt_iterator(cit);
   }

   debug_printf(1, "handle_internal_get_corrupt: completed\n");

   return(err);
}


//*****************************************************************
// handle_internal_get_alloc - Processes the raw internal get allocation
//
// Results:
//    status nblocks state cs_type header_blocksize blocksize ndatabytes\n
//    ..raw allocation..
//    block0 block_bytes0 disk_chksum0 [(calc_chksum0)]\n
//    block1 block_bytes1 disk_chksum1 [(calc_chksum1)]\n
//    blockN block_bytesN disk_chksumN [(calc_chksumN)]\n
//    ...ndatabytes...
//*****************************************************************

int handle_internal_get_alloc(ibp_task_t *task)
{
   int bufsize = 1024*1024;
   char buffer[bufsize];
   int err, cs_type, bin_len, state, i;
   ibp_off_t blocksize, nblocks, hbs, blen, nleft;
   long long int bytes_used;
   uint64_t nbytes;
   Resource_t *res;
   tbx_chksum_t chksum;
   osd_fd_t *fd;
   char block_chksum[CHKSUM_MAX_SIZE], calc_chksum[CHKSUM_MAX_SIZE], good_block;
   char hex_digest[CHKSUM_MAX_SIZE], hex_digest2[CHKSUM_MAX_SIZE];
   char token[4096];
   Allocation_t a;
   Allocation_history_t h;
   Cmd_state_t *cmd = &(task->cmd);
   Cmd_internal_get_alloc_t *arg = &(cmd->cargs.get_alloc);

   debug_printf(1, "handle_internal_get_alloc: Starting to process command\n");

   err = 0;

      //** Check the resource **
   res = resource_lookup(global_config->rl, ibp_rid2str(arg->rid, token));
   if (res == NULL) {    //**Can't find the resource
      log_printf(1, "handle_internal_get_alloc: Invalid resource: %s\n", ibp_rid2str(arg->rid, token));
      alog_append_internal_get_alloc(task->myid, -1, 0);
      send_cmd_result(task, IBP_E_INVALID_RID);
      return(global_config->soft_fail);
   }

   err = -1;
   a.id = 0;
   switch(arg->key_type) {
     case IBP_READCAP:
         err = get_allocation_by_cap_resource(res, READ_CAP, &(arg->cap), &a);
         break;
     case IBP_WRITECAP:
         err = get_allocation_by_cap_resource(res, WRITE_CAP, &(arg->cap), &a);
         break;
     case IBP_MANAGECAP:
         err = get_allocation_by_cap_resource(res, MANAGE_CAP, &(arg->cap), &a);
         break;
     case INTERNAL_ID:
         err = get_allocation_resource(res, arg->id, &a);
         break;
   }

   alog_append_internal_get_alloc(task->myid, res->rl_index, a.id);

   if (err != 0) {
     log_printf(10, "handle_internal_get_alloc: Invalid cap/id: rid=%s ns=%d\n",res->name, tbx_ns_getid(task->ns));
     send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
     return(global_config->soft_fail);
   }

   err = get_history_table(res, a.id, &h);

   if (err != 0) {
     log_printf(10, "handle_internal_get_alloc: Cant read the history! rid=%s ns=%d\n",res->name, tbx_ns_getid(task->ns));
     send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
     return(global_config->soft_fail);
   }


   nbytes = 0;
   if (arg->offset > -1) {
      if (arg->len == 0) {
         nbytes = a.size - arg->offset;
      } else if (arg->offset > a.size) {
         nbytes = 0;
      } else if ((arg->len + arg->offset) > a.size) {
         nbytes = a.size - arg->offset;
      } else {
         nbytes = arg->len;
      }
   }

   //** Get the chksum information
   hbs = ALLOC_HEADER;
   nblocks = 0; blocksize = 0; state=0;  bin_len = 0;
   err = get_allocation_chksum_info(res, a.id, &cs_type, &hbs, &blocksize);

   if ((cs_type != CHKSUM_NONE) && (err == 0)) {
      tbx_chksum_set(&chksum, cs_type);
      bin_len = tbx_chksum_size(&chksum, CHKSUM_DIGEST_BIN);
 
      nblocks = (a.size / blocksize) + 1;  //** +1 is for the header
      if ((a.size%blocksize) > 0) nblocks++;  //** Handle a partial block

      //** Get the current state
      fd = open_allocation(res, a.id, OSD_READ_MODE);
      state = -1;
      if (fd != NULL) {
         state = get_allocation_state(res, fd);
         close_allocation(res, fd);
      }
   }

   //*** Send back the results ***
   sprintf(buffer, "%d " I64T " %d %d " I64T " " I64T " " LU " \n", IBP_OK, nblocks, state, cs_type, hbs, blocksize, nbytes); 
   err = server_ns_write_block(task->ns, task->cmd_timeout, buffer, strlen(buffer));

   //** Send the allocation
   err = server_ns_write_block(task->ns, task->cmd_timeout, (char *)&a, sizeof(a));

   //** ...and the history
   err = server_ns_write_block(task->ns, task->cmd_timeout, (char *)&h, sizeof(h));

   //** ..and the block information if needed
log_printf(10, "handle_internal_get_alloc: ns=%d print_blocks=%d nblocks=" I64T "\n", tbx_ns_getid(task->ns), arg->print_blocks, nblocks);
   if (arg->print_blocks > 0) {
      for (i=0; i<nblocks; i++) {
//log_printf(10, "handle_internal_get_alloc: ns=%d i=%d\n", tbx_ns_getid(task->ns), i);
         osd_get_chksum(res->dev, a.id, block_chksum, calc_chksum, sizeof(block_chksum), &blen, &good_block, i, i);
         bytes_used = blen;
         state = good_block;
         tbx_chksum_bin2hex(bin_len, (unsigned char *)block_chksum, hex_digest);
         if (state == 0) {
            sprintf(buffer, "%7d  %10lld   %2d     %s\n", i, bytes_used, state, hex_digest);
         } else {
            tbx_chksum_bin2hex(bin_len, (unsigned char *)calc_chksum, hex_digest2);
            sprintf(buffer, "%7d  %10lld   %2d     %s (%s)\n", i, bytes_used, state, hex_digest, hex_digest2);
         }      
log_printf(10, "handle_internal_get_alloc: ns=%d i=%d buf=%s\n", tbx_ns_getid(task->ns), i, buffer);
         err = server_ns_write_block(task->ns, task->cmd_timeout, buffer, strlen(buffer));
      }
   }

   //** Send back the data if needed **
   if (arg->offset > -1) {
      ibp_task_t rtask;
      Cmd_read_t *rcmd = &(rtask.cmd.cargs.read);
      rcmd->a = a;
      rcmd->r = res;
      iovec_single(&(rcmd->iovec), arg->offset, nbytes);
      rtask.ns = task->ns;
      nleft = nbytes;
      err = read_from_disk(&rtask, &a, &nleft, rcmd->r);
   }

   debug_printf(1, "handle_internal_get_alloc: Allocation:\n");

   debug_code(
      if (debug_level() > 5) print_allocation_resource(res, log_fd(), &a);
   )
   debug_printf(1, "handle_internal_get_alloc: completed\n");

   return(err);
}


//*****************************************************************
// handle_alias_allocate - Generates a alias allocation 
//
// Results:
//    status readCap writeCap manageCap
//*****************************************************************

int handle_alias_allocate(ibp_task_t *task)
{
   int d, err;
   Resource_t *res;

   char token[4096];
   Allocation_t a, alias_alloc; 
   Cmd_state_t *cmd = &(task->cmd);
   Cmd_alias_alloc_t *pa = &(cmd->cargs.alias_alloc);

   debug_printf(1, "handle_alias_allocate: Starting to process command\n");

   err = 0;

      //** Check the resource **
   res = resource_lookup(global_config->rl, ibp_rid2str(pa->rid, token));
   if (res == NULL) {    //**Can't find the resource
      log_printf(1, "handle_alias_allocate: Invalid resource: %s\n", ibp_rid2str(pa->rid, token));
      alog_append_alias_alloc(task->myid, -1, 0, 0, 0, 0);
      send_cmd_result(task, IBP_E_INVALID_RID);
      return(global_config->soft_fail);
   }   

   //** Get the original allocation ***
   if ((err = get_allocation_by_cap_resource(res, MANAGE_CAP, &(pa->cap), &a)) != 0) {
     log_printf(10, "handle_alias_allocate: Invalid cap: %s rid=%s\n", pa->cap.v, res->name);
     alog_append_alias_alloc(task->myid, -1, 0, 0, 0, 0);
     send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
     return(global_config->soft_fail);
   }

   lock_osd_id(a.id);

   //** Get the original allocation again with the lock ***
   if ((err = get_allocation_by_cap_resource(res, MANAGE_CAP, &(pa->cap), &a)) != 0) {
     log_printf(10, "handle_alias_allocate: Invalid cap: %s rid=%s\n", pa->cap.v, res->name);
     alog_append_alias_alloc(task->myid, -1, 0, 0, 0, 0);
     send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
     unlock_osd_id(a.id);
     return(global_config->soft_fail);
   }

   //** Validate the range and duration **
//   log_printf(1, "handle_alias_alloc:  pa->duration= %u\n", pa->expiration);
   if (pa->expiration == 0) pa->expiration = a.expiration;

   alog_append_alias_alloc(task->myid, res->rl_index, a.id, pa->offset, pa->len, pa->expiration);

   if (pa->expiration > a.expiration) {
      log_printf(1, "handle_alias_alloc: ALIAS duration > actual allocation! alias= %u alloc = %u\n", pa->expiration, a.expiration);

      send_cmd_result(task, IBP_E_LONG_DURATION);
      unlock_osd_id(a.id);
      return(global_config->soft_fail);
   }

   if ((pa->len + pa->offset) > a.max_size) {  
      uint64_t epos = pa->len + pa->offset;
      log_printf(1, "handle_alias_alloc: ALIAS range > actual allocation! alias= " LU " alloc = " LU "\n", epos, a.max_size);
      send_cmd_result(task, IBP_E_WOULD_EXCEED_LIMIT);
      unlock_osd_id(a.id);
      return(global_config->soft_fail);
   }

   //*** Create the alias ***
   if ((d =  create_allocation_resource(res, &alias_alloc, 0, a.type, a.reliability, pa->expiration, 1, 0, 0, 0)) != 0) {
      log_printf(1, "handle_alias_alloc: create_allocation_resource failed on RID %s!  Error=%d\n", res->name, d);
      send_cmd_result(task, IBP_E_GENERIC);
      unlock_osd_id(a.id);
      return(global_config->soft_fail);
   }

   //** Store the creation timestamp **
   set_alloc_timestamp(&(alias_alloc.creation_ts), &(task->ipadd));

   //*** Specifify the allocation as a alias ***
   alias_alloc.alias_id = a.id;
   alias_alloc.alias_offset = pa->offset;
   alias_alloc.alias_size = pa->len;

   //** and store it back in the DB only **
   if ((d = modify_allocation_resource(res, alias_alloc.alias_id, &alias_alloc)) != 0) {
      log_printf(1, "handle_alias_allocate: modify_allocation_resource failed on RID %s!  Error=%d\n", res->name, d);
      send_cmd_result(task, IBP_E_GENERIC);
      unlock_osd_id(a.id);
      return(global_config->soft_fail);
   }

   //** Update the parent timestamp
   update_manage_history(res, a.id, a.is_alias, &(task->ipadd), IBP_ALIAS_ALLOCATE, 0, alias_alloc.alias_offset, alias_alloc.expiration, alias_alloc.alias_size, alias_alloc.id);

   unlock_osd_id(a.id);

   //** Send the result back **
   tbx_ns_monitor_t *nm = tbx_ns_monitor_get(task->ns);
   snprintf(token, sizeof(token), "%d ibp://%s:%d/%s#%s/3862277/READ "
       "ibp://%s:%d/%s#%s/3862277/WRITE "
       "ibp://%s:%d/%s#%s/3862277/MANAGE \n",
       IBP_OK,
       tbx_nm_host_get(nm), tbx_nm_port_get(nm), res->name, alias_alloc.caps[READ_CAP].v,
       tbx_nm_host_get(nm), tbx_nm_port_get(nm), res->name, alias_alloc.caps[WRITE_CAP].v,
       tbx_nm_host_get(nm), tbx_nm_port_get(nm), res->name, alias_alloc.caps[MANAGE_CAP].v);

   debug_code(time_t tt=ibp_time_now();)
   debug_printf(1, "handle_alias_allocate: before sending result time: %s\n", ctime(&tt));
   err = server_ns_write_block(task->ns, task->cmd_timeout, token, strlen(token));

   alog_append_osd_id(task->myid, alias_alloc.id);

   debug_printf(1, "handle_alias_allocate: Allocation: %s", token);

   debug_code(
      if (debug_level() > 5) print_allocation_resource(res, log_fd(), &alias_alloc);
   )
   return(err);
}


//*****************************************************************
// handle_status - Processes a status request.
//
//  IBP_ST_INQ
//    v1.3
//      status nbytes \n
//      hard_max_mb hard_used_mb soft_max_mb soft_used_mb max_duration \n
//    v1.4
//      status nbytes \n
//      VS:1.4 DT:dm_type RID:rid RT:rtype CT:ct_bytes ST:st_bytes UT:ut_bytes UH:uh_bytes SH:sh_bytes
//      CH:ch_bytes AT:at_bytes AH:ah_bytes DT:max_duration RE \n
//
//  IBP_ST_CHANGE 
//      status \n
//
//  IBP_ST_RES 
//      status RID1 RID2 ... RIDn \n
//
//*****************************************************************

int handle_status(ibp_task_t *task)
{
  Cmd_state_t *cmd = &(task->cmd); 
  Cmd_status_t *status = &(cmd->cargs.status);

  char buffer[100*1024];
  char result[100*1024];
  char *prid;
  uint64_t total, total_used, total_diff, total_free, rbytes, wbytes, cbytes;
  uint64_t r_total, r_diff, r_free, r_used, r_alloc, r_alias, total_alloc, total_alias;
  uint64_t del_total, exp_total, n_del_total, n_exp_total;
  uint64_t del_free, exp_free, n_del, n_exp;
  uint64_t bad_count, bad_total_count;
  double r_total_gb, r_diff_gb, r_free_gb, r_used_gb;
  tbx_ns_timeout_t dt;
  resource_list_iterator_t it;
  Resource_t *r;
  interface_t *iface;
  int i;

  debug_printf(1, "handle_status: Starting to process command timoue=" TT "\n", task->cmd_timeout);


  if (status->subcmd == IBP_ST_VERSION) {
     alog_append_status_version(task->myid);

     tbx_ns_timeout_set(&dt, 1, 0);
     char *version = server_version();

     result[0] = '\0'; result[sizeof(result)-1] = '\0';
     buffer[0] = '\0'; buffer[sizeof(buffer)-1] = '\0';
     sprintf(result, "%d\n", IBP_OK);

     strncat(result, version, sizeof(result)-1 - strlen(result));

     //** Add info on the interfaces
     sprintf(buffer, "Interfaces(%d): ", global_config->server.n_iface);
     strncat(result, buffer, sizeof(result)-1 - strlen(result));

     for (i=0; i<global_config->server.n_iface; i++) {
         iface = &(global_config->server.iface[i]);
         sprintf(buffer, "%s:%d; ", iface->hostname, iface->port);
         strncat(result, buffer, sizeof(result)-1 - strlen(result));
     }
     strncat(result, "\n", sizeof(result)-1 - strlen(result));


     //** Add the uptime data  **
     print_uptime(buffer, sizeof(buffer));
     strncat(result, buffer, sizeof(result)-1 - strlen(result));

     //** Add some stats **
     snprintf(buffer, sizeof(buffer)-1, "Total Commands: " LU "  Connections: %d\n", task->tid,
           tbx_network_counter(global_network));
     strncat(result, buffer, sizeof(result)-1 - strlen(result));
     snprintf(buffer, sizeof(buffer)-1, "Active Threads: %d\n", currently_running_tasks());
     strncat(result, buffer, sizeof(result)-1 - strlen(result));
     reject_count(&i, &total);
     snprintf(buffer, sizeof(buffer)-1, "Reject stats --- Current: %d  Total: " LU "\n", i, total);
     strncat(result, buffer, sizeof(result)-1 - strlen(result));

     //** Get the RID pending list
     //** This should really be done in the resource_list but it's only needed this one place
     //** so I'm just adding it here directly.  If it's ever needed someplace else it should be redone properly
     apr_thread_mutex_lock(global_config->rl->lock);
     snprintf(buffer, sizeof(buffer)-1, "Pending RID count: %d\n", tbx_stack_count(global_config->rl->pending));
     strncat(result, buffer, sizeof(result)-1 - strlen(result));
     if (tbx_stack_count(global_config->rl->pending) > 0) {
        snprintf(buffer, sizeof(buffer)-1, "Pending RID list: ");
        strncat(result, buffer, sizeof(result)-1 - strlen(result));
        tbx_stack_move_to_top(global_config->rl->pending);
        while ((prid = (char *)tbx_stack_get_current_data(global_config->rl->pending)) != NULL) {
          tbx_stack_move_down(global_config->rl->pending);
          if (tbx_stack_get_current_data(global_config->rl->pending) == NULL) {
             snprintf(buffer, sizeof(buffer)-1, "%s\n", prid);
          } else {
             snprintf(buffer, sizeof(buffer)-1, "%s ", prid);
          }
          strncat(result, buffer, sizeof(result)-1 - strlen(result));
        }
     }
     apr_thread_mutex_unlock(global_config->rl->lock);

     //** Now do th4e transfer stats
     get_transfer_stats(&rbytes, &wbytes, &cbytes);
     total = rbytes+wbytes;
     r_total_gb = total / (1024.0*1024.0*1024.0);
     r_used_gb = rbytes / (1024.0*1024.0*1024.0);
     r_free_gb = wbytes / (1024.0*1024.0*1024.0);
     snprintf(buffer, sizeof(buffer)-1, "Depot Transfer Stats --  Read: " LU " b (%.2lf GB) Write: " LU " b (%.2lf GB) Total: " LU " b (%.2lf GB)\n",
          rbytes, r_used_gb, wbytes, r_free_gb, total, r_total_gb);
     strncat(result, buffer, sizeof(result)-1 - strlen(result));

     r_total_gb = cbytes / (1024.0*1024.0*1024.0);
     snprintf(buffer, sizeof(buffer)-1, "Depot-Depot copies: " LU " b (%.2lf GB)\n", cbytes, r_total_gb);
     strncat(result, buffer, sizeof(result)-1 - strlen(result));

     total = 0; total_used = 0; total_free = 0; total_diff = 0; total_alloc = 0; total_alias = 0;
     del_total = 0; exp_total = 0; n_del_total = 0; n_exp_total = 0;
     bad_total_count = 0;


     it = resource_list_iterator(global_config->rl);
     while ((r = resource_list_iterator_next(global_config->rl, &it)) != NULL) {
         r_free = resource_allocable(r, 0);
         bad_count = resource_get_corrupt_count(r);
         bad_total_count = bad_total_count + bad_count;

         apr_thread_mutex_lock(r->mutex);
         r_total = r->max_size[ALLOC_TOTAL];
         r_used = r->used_space[ALLOC_HARD] + r->used_space[ALLOC_SOFT];
         r_alloc = r->n_allocs;
         r_alias = r->n_alias;
         del_free = r->trash_size[RES_DELETE_INDEX]; exp_free = r->trash_size[RES_EXPIRE_INDEX];
         n_del = r->n_trash[RES_DELETE_INDEX]; n_exp = r->n_trash[RES_EXPIRE_INDEX];
         apr_thread_mutex_unlock(r->mutex);

         if (r_total > r_used) {
            r_diff = r_total - r_used;
         } else {
            r_diff = 0;
         }
         total = total + r_total;
         total_used = total_used + r_used;
         total_diff = total_diff + r_diff;
         total_free = total_free + r_free;
         total_alloc = total_alloc + r_alloc;
         total_alias = total_alias + r_alias;
         del_total = del_total + del_free;   exp_total = exp_total + exp_free;
         n_del_total = n_del_total + n_del;   n_exp_total = n_exp_total + n_exp;

         r_total_gb = r_total / (1024.0*1024.0*1024.0);
         r_used_gb = r_used / (1024.0*1024.0*1024.0);
         r_diff_gb = r_diff / (1024.0*1024.0*1024.0);
         r_free_gb = r_free / (1024.0*1024.0*1024.0);
         snprintf(buffer, sizeof(buffer)-1, "RID: %s Max: " LU " b (%.2lf GB) Used: " LU " b (%.2lf GB) Diff: " LU " b (%.2lf GB) Free: " LU " b (%.2lf GB) Allocations: " LU " (" LU " alias) Corrupt count: " LU " Activity count: %d\n", 
             r->name, r_total, r_total_gb, r_used, r_used_gb, r_diff, r_diff_gb, r_free, r_free_gb, r_alloc, r_alias, bad_count, resource_get_counter(r));
         strncat(result, buffer, sizeof(result)-1 - strlen(result));

         r_diff_gb = exp_free / (1024.0*1024.0*1024.0);
         r_free_gb = del_free / (1024.0*1024.0*1024.0);
         snprintf(buffer, sizeof(buffer)-1, "Trash stats for RID: %s -- Deleted: " LU " b (%.2lf GB) in " LU " files  -- Expired: " LU " b (%.2lf GB) in " LU " files\n",
             r->name, del_free, r_free_gb, n_del, exp_free, r_diff_gb, n_exp);
         strncat(result, buffer, sizeof(result)-1 - strlen(result));
     }
     resource_list_iterator_destroy(global_config->rl, &it);

     r_total_gb = total / (1024.0*1024.0*1024.0);
     r_used_gb = total_used / (1024.0*1024.0*1024.0);
     r_diff_gb = total_diff / (1024.0*1024.0*1024.0);
     r_free_gb = total_free / (1024.0*1024.0*1024.0);
     i = resource_list_n_used(global_config->rl);
     snprintf(buffer, sizeof(buffer)-1, "Total resources: %d  Max: " LU " b (%.2lf GB) Used: " LU " b (%.2lf GB) Diff: " LU " b (%.2lf GB) Free: " LU " b (%.2lf GB) Allocations: " LU " (" LU " alias)  Corrupt count: " LU "\n", 
             i, total, r_total_gb, total_used, r_used_gb, total_diff, r_diff_gb, total_free, r_free_gb, total_alloc, total_alias, bad_total_count);
     strncat(result, buffer, sizeof(result)-1 - strlen(result));

     r_free_gb = del_total / (1024.0*1024.0*1024.0);
     r_diff_gb = exp_total / (1024.0*1024.0*1024.0);
     snprintf(buffer, sizeof(buffer)-1, "Total Trash stats -- Deleted: " LU " b (%.2lf GB) in " LU " files  -- Expired: " LU " b (%.2lf GB) in " LU " files\n", 
             del_total, r_free_gb, n_del_total, exp_total, r_diff_gb, n_exp_total);
     strncat(result, buffer, sizeof(result)-1 - strlen(result));


     snprintf(buffer,sizeof(result)-1 - strlen(result), "\n");
     snprintf(buffer, sizeof(result)-1 - strlen(result), "END\n");
     strncat(result, buffer, sizeof(result) - 1 - strlen(result));
     i = strlen(result);
     server_ns_write_block(task->ns, task->cmd_timeout, result, i);

     alog_append_cmd_result(task->myid, IBP_OK);

     return(0);
  }

  if (strcmp(global_config->server.password, status->password) != 0) {
    log_printf(10, "handle_status:  Invalid password: %s\n", status->password);
    send_cmd_result(task, IBP_E_WRONG_PASSWD);
    return(global_config->soft_fail);
  }

  if (status->subcmd == IBP_ST_RES) {
     alog_append_status_res(task->myid);

     char buffer[2048];
     char result[2048];

     snprintf(result, sizeof(result), "%d ", IBP_OK);
     it = resource_list_iterator(global_config->rl);
     while ((r = resource_list_iterator_next(global_config->rl, &it)) != NULL) {
         snprintf(buffer, sizeof(buffer), "%s ", r->name);
         strncat(result, buffer, sizeof(result) -1 - strlen(result));
     }
     resource_list_iterator_destroy(global_config->rl, &it);

     sprintf(buffer, "\n");
     strncat(result, buffer, sizeof(result) -1 - strlen(result));

     log_printf(10, "handle_status: Sending resource list: %s\n", result);
     server_ns_write_block(task->ns, task->cmd_timeout, result, strlen(result));

     alog_append_cmd_result(task->myid, IBP_OK);
  } else if (status->subcmd == IBP_ST_STATS) {  //** Send the depot stats
     alog_append_status_stats(task->myid, status->start_time);
     tbx_ns_timeout_t dt;
     convert_epoch_time2net(&dt, task->cmd_timeout);
     log_printf(10, "handle_status: Sending stats\n");
     send_stats(task->ns, status->start_time, dt);
     alog_append_cmd_result(task->myid, IBP_OK);
  } else if (status->subcmd == IBP_ST_INQ) {
     char buffer[2048]; 
     char result[32];
     int n, nres;

     Resource_t *r = resource_lookup(global_config->rl, status->crid);
     if (r == NULL) {
        log_printf(10, "handle_status:  Invalid RID :%s\n",status->crid);
        alog_append_status_inq(task->myid, -1);
        send_cmd_result(task, IBP_E_INVALID_RID);
        return(global_config->soft_fail);
     }

     alog_append_status_inq(task->myid, r->rl_index);

     uint64_t totalconfigured;
     uint64_t totalused;
     uint64_t soft_alloc, hard_alloc, total_alloc;

     total_alloc = resource_allocable(r, 0);

     apr_thread_mutex_lock(r->mutex);
     totalconfigured = r->max_size[ALLOC_TOTAL];
     totalused = r->used_space[ALLOC_HARD] + r->used_space[ALLOC_SOFT];
     if (totalused > totalconfigured) {
        soft_alloc = 0;
        hard_alloc = 0;
     } else {
        soft_alloc = r->max_size[ALLOC_SOFT] - r->used_space[ALLOC_SOFT];
        if (soft_alloc > total_alloc) soft_alloc = total_alloc;
        hard_alloc = r->max_size[ALLOC_HARD] - r->used_space[ALLOC_HARD];
        if (hard_alloc > total_alloc) hard_alloc = total_alloc;
     }

     if (cmd->version == IBPv040) {
                                            //***  1       2       3     4      5       6       7       8        9       10      11     12     13   13
        n = snprintf(buffer, sizeof(buffer), "%s:1.4:1.0 %s:%d  %s:%s %s:%d %s:" LU " %s:" LU " %s:" LU " %s:" LU " %s:" LU " %s:" LU " %s:" LU " %s:" LU " %s:%d %s \n", 
           ST_VERSION,                                   //** 1
           ST_DATAMOVERTYPE, DM_TCP,                     //** 2
           ST_RESOURCEID, r->name,                       //** 3
           ST_RESOURCETYPE, RS_DISK,                     //** 4
           ST_CONFIG_TOTAL_SZ, totalconfigured,          //** 5
           ST_SERVED_TOTAL_SZ, totalconfigured,          //** 6
           ST_USED_TOTAL_SZ, totalused,                  //** 7
           ST_USED_HARD_SZ, r->used_space[ALLOC_HARD],   //** 8
           ST_SERVED_HARD_SZ, r->max_size[ALLOC_HARD],   //** 9
           ST_CONFIG_HARD_SZ, r->max_size[ALLOC_HARD],   //** 10
           ST_ALLOC_TOTAL_SZ, total_alloc,                //** 11
           ST_ALLOC_HARD_SZ, hard_alloc,                 //** 12
           ST_DURATION, r->max_duration,                 //** 13
           ST_RS_END);                                   //** 14
     } else {
       uint64_t soft, soft_used, hard, hard_used;
       soft = r->max_size[ALLOC_SOFT] >> 20;    soft_used = r->used_space[ALLOC_SOFT] >> 20;
       hard = r->max_size[ALLOC_HARD] >> 20;    hard_used = r->used_space[ALLOC_HARD] >> 20;

       n = snprintf(buffer, sizeof(buffer), "" LU " " LU " " LU " " LU " %d \n",
              hard, hard_used, soft, soft_used, r->max_duration);
     }

     apr_thread_mutex_unlock(r->mutex);

     nres = snprintf(result, sizeof(result), "%d %d \n", IBP_OK, n);
     if (cmd->version != IBPv031) server_ns_write_block(task->ns, task->cmd_timeout, result, nres);
     server_ns_write_block(task->ns, task->cmd_timeout, buffer, n);

     alog_append_cmd_result(task->myid, IBP_OK);

     log_printf(10, "handle_status: Succesfully processed IBP_ST_INQ on RID %s\n", r->name);
     log_printf(10, "handle_status: depot info: %s\n", buffer);
     return(0);
  } else if (status->subcmd == IBP_ST_CHANGE) {
//==================IBP_ST_CHANGE NOT ALLOWED==============================
log_printf(10, "handle_status:  IBP_ST_CHANGE!!!! rid=%s ns=%d  IGNORING!!\n",status->crid, tbx_ns_getid(task->ns));
alog_append_status_change(task->myid);
send_cmd_result(task, IBP_E_INVALID_CMD);
tbx_ns_close(task->ns);
return(-1);


     Resource_t *r = resource_lookup(global_config->rl, status->crid);
     if (r == NULL) {
        log_printf(10, "handle_status:  Invalid RID :%s\n",status->crid);
        send_cmd_result(task, IBP_E_INVALID_RID);
        return(global_config->soft_fail);
     }

     log_printf(10, "handle_status: Requested change on RID %s hard:" LU " soft:" LU " expiration:%ld\n",
           r->name, status->new_size[ALLOC_HARD], status->new_size[ALLOC_SOFT], status->new_duration);

     apr_thread_mutex_lock(r->mutex);
     if (r->used_space[ALLOC_HARD] < status->new_size[ALLOC_HARD]) r->max_size[ALLOC_HARD] = status->new_size[ALLOC_HARD];
     if (r->used_space[ALLOC_SOFT] < status->new_size[ALLOC_SOFT]) r->max_size[ALLOC_SOFT] = status->new_size[ALLOC_SOFT];
     if (status->new_duration < 0) status->new_duration = INT_MAX;
     r->max_duration = status->new_duration;
     apr_thread_mutex_unlock(r->mutex);

     log_printf(10, "handle_status: Succesfully processed IBP_ST_CHANGE on RID %s hard:" LU " soft:" LU " expiration:%d\n", 
           r->name, r->max_size[ALLOC_HARD], r->max_size[ALLOC_SOFT], r->max_duration);
     cmd->state = CMD_STATE_FINISHED;
     send_cmd_result(task, IBP_OK);
     return(0);
  } else {
     log_printf(10, "handle_status:  Invalid sub command :%d\n",status->subcmd);
     send_cmd_result(task, IBP_E_INVALID_CMD);
     return(0);
  }

  debug_printf(1, "handle_status: Successfully processed command\n");

  return(0);
}

//*****************************************************************
// handle_manage - Processes the manage allocation command
//
//  IBP_INCR | IBP_DECR | IBP_CHNG | IBP_TRUNCATE
//     status \n
//
//  IBP_PROBE (for an IBP_MANAGE command)
//     status read_refcnt write_refcnt curr_size max_size time_remaining \n

//  IBP_PROBE (for an IBP_ALIAS_MANAGE command)
//     status read_refcnt write_refcnt offset len time_remaining \n
//
//  NOTE: From my unsderstanding this command is flawed since any only READ counts
//    are used to delete an allocation!!  Both READ and WRITE counts
//    should be used.
//*****************************************************************

int handle_manage(ibp_task_t *task)
{
  Cmd_state_t *cmd = &(task->cmd);
  Cmd_manage_t *manage = &(cmd->cargs.manage);
  Allocation_t *a = &(manage->a);
  Allocation_t ma;
  osd_id_t id, pid;
  int err, is_alias, status;
  uint64_t alias_offset, alias_len;
  char buf[1024];
  int rel = IBP_SOFT;
  uint64_t psize, pmax_size;
  int dir = -1;

  if (cmd->command == IBP_MANAGE) {
     debug_printf(1, "handle_manage: Starting to process IBP_MANAGE command ns=%d\n", tbx_ns_getid(task->ns));
  } else {
     debug_printf(1, "handle_manage: Starting to process IBP_ALIAS_MANAGE command ns=%d\n", tbx_ns_getid(task->ns));
  }

  Resource_t *r = resource_lookup(global_config->rl, manage->crid);
  if (r == NULL) {
     log_printf(10, "handle_manage:  Invalid RID :%s\n",manage->crid); 
     alog_append_manage_bad(task->myid, cmd->command, manage->subcmd);
     send_cmd_result(task, IBP_E_INVALID_RID);
     return(global_config->soft_fail);
  }

  //** Resource is not mounted with manage access
  if ((resource_get_mode(r) & RES_MODE_MANAGE) == 0) {
     log_printf(10, "handle_manage: Manage access is disabled cap: %s RID=%s\n", manage->cap.v, r->name);
     alog_append_manage_bad(task->myid, cmd->command, manage->subcmd);
     send_cmd_result(task, IBP_E_FILE_ACCESS);
     return(0);
  }


  if ((err = get_allocation_by_cap_resource(r, MANAGE_CAP, &(manage->cap), a)) != 0) {
     log_printf(10, "handle_manage: Invalid cap: %s rid=%s\n", manage->cap.v, r->name);
     alog_append_manage_bad(task->myid, cmd->command, manage->subcmd);
     send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
     return(global_config->soft_fail);
  }


  pid = a->id;

  is_alias = a->is_alias;
  alias_offset = 0;
  alias_len = a->max_size;
  if ((a->is_alias == 1) && (cmd->command == IBP_MANAGE)) {  //** This is a alias cap so load the master and invoke restrictions
     is_alias = 1;
     id = a->alias_id;
     alias_offset = a->alias_offset;
     alias_len =  a->alias_size;

     log_printf(10, "handle_manage: ns=%d got a alias cap! loading id " LU "\n", tbx_ns_getid(task->ns), id);

     if ((err = get_allocation_resource(r, id, a)) != 0) {
        log_printf(10, "handle_manage: Invalid alias id: " LU " rid=%s\n", id, r->name);
        alog_append_manage_bad(task->myid, cmd->command, manage->subcmd);
        send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
        return(global_config->soft_fail);
     }

     if (alias_len == 0) alias_len = a->max_size - alias_offset;

     //** Can only isue a probe on a alias allocation through ibp_manage
     if (manage->subcmd != IBP_PROBE) {
        log_printf(10, "handle_manage: Invalid subcmd for alias id: " LU " rid=%s\n", id, r->name);
        alog_append_manage_bad(task->myid, cmd->command, manage->subcmd);
        send_cmd_result(task, IBP_E_INVALID_CMD);
        return(global_config->soft_fail);
     }
  }

  //** Verify the master allocation if needed.  ***
  if ((is_alias == 1) && (cmd->command == IBP_ALIAS_MANAGE) && (manage->subcmd != IBP_PROBE)) {
     if ((err = get_allocation_resource(r, a->alias_id, &ma)) != 0) {
        log_printf(10, "handle_manage: Invalid alias id: " LU " rid=%s\n", a->alias_id, r->name);
        alog_append_manage_bad(task->myid, cmd->command, manage->subcmd);
        send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
        return(global_config->soft_fail);
     }

     if (strcmp(ma.caps[MANAGE_CAP].v, manage->master_cap.v) != 0) {
        log_printf(10, "handle_manage: Master cap doesn't match with alias read: %s actual: %s  rid=%s ns=%d\n", 
             manage->master_cap.v, ma.caps[MANAGE_CAP].v, r->name, tbx_ns_getid(task->ns));
        alog_append_manage_bad(task->myid, cmd->command, manage->subcmd);
        send_cmd_result(task, IBP_E_INVALID_MANAGE_CAP);
        return(global_config->soft_fail);
     }
  }

  lock_osd_id(a->id);  //** Lock it so we don't get race updates

  //** Re-read the data with the lock enabled
  id = a->id;
  if ((err = get_allocation_resource(r, id, a)) != 0) {
     log_printf(10, "handle_manage: Error reading id after lock_osd_id  id: " LU " rid=%s\n", id, r->name);
     alog_append_manage_bad(task->myid, cmd->command, manage->subcmd);
     send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
     unlock_osd_id(id);
     return(global_config->soft_fail);
  }

  snprintf(manage->cid,sizeof(manage->cid), "" LU "", a->id);

  switch (manage->subcmd) {
     case IBP_INCR:
        dir = 1;
     case IBP_DECR:
        alog_append_manage_incdec(task->myid, cmd->command, manage->subcmd, r->rl_index, pid, id, manage->captype);
        err = IBP_OK;
        if (manage->captype == READ_CAP) {
           a->read_refcount = a->read_refcount + dir;
           if (a->read_refcount < 0) a->read_refcount = 0;
        } else if (manage->captype == WRITE_CAP) {
           a->write_refcount = a->write_refcount + dir;
           if (a->write_refcount < 0) a->write_refcount = 0;
        } else {
          log_printf(0, "handle_manage: Invalid captype foe IBP_INCR/DECR!\n");
          err = IBP_E_WRONG_CAP_FORMAT;
        }

        //** Update the manage timestamp
        update_manage_history(r, a->id, a->is_alias, &(task->ipadd), cmd->command, manage->subcmd, a->read_refcount, a->write_refcount, a->max_size, pid);

        if ((a->read_refcount > 0) || (a->write_refcount > 0)) {
           if ((err = modify_allocation_resource(r, a->id, a)) == 0) err = IBP_OK;
        }

        //** Check if we need to remove the allocation **
        if ((a->read_refcount == 0) && (a->write_refcount == 0)) {
           remove_allocation_resource(r, OSD_DELETE_ID, a);
        }
        send_cmd_result(task, err);
        break;
     case IBP_TRUNCATE:
        status = IBP_OK;
        if (is_alias == 1) {  //** If this is an IBP_ALIAS_MANAGE call we have different fields
          if (ma.max_size < (a->alias_offset + manage->new_size-1)) {
              status = IBP_E_WOULD_EXCEED_POLICY;
          } else {
              a->alias_size = manage->new_size;
          }
          alog_append_alias_manage_change(task->myid, r->rl_index, a->id, a->alias_offset, a->alias_size, a->expiration);
        } else {
          if (a->size > manage->new_size) {
             a->size = manage->new_size;
          }
          a->max_size = manage->new_size;
          alog_append_manage_change(task->myid, r->rl_index, a->id, a->max_size, a->reliability, a->expiration);
        }

        //** Update the manage timestamp
        if (is_alias == 1) {
           update_manage_history(r, a->id, a->is_alias, &(task->ipadd), cmd->command, manage->subcmd, a->alias_offset, a->expiration, a->alias_size, pid);
        } else {
           update_manage_history(r, a->id, a->is_alias, &(task->ipadd), cmd->command, manage->subcmd, a->reliability, a->expiration, a->max_size, pid);
        }

        err = modify_allocation_resource(r, a->id, a);

        if (err == 0) {
           send_cmd_result(task, status);
        } else {
           send_cmd_result(task, IBP_E_WOULD_EXCEED_POLICY);
        }
        break;

     case IBP_CHNG:
        status = IBP_OK;
        if (is_alias == 1) {  //** If this is an IBP_ALIAS_MANAGE call we have different fields
          if (manage->new_size > 0) {
             if (ma.max_size < (manage->offset + manage->new_size-1)) {
                log_printf(10, "handle_manage: curr_size>new_size! id: " LU " rid=%s\n", id, r->name);
                status = IBP_E_WOULD_EXCEED_POLICY;
             } else {
                a->alias_size = manage->new_size;
                a->alias_offset = manage->offset;
             }
          }

          if (manage->new_duration >= 0) {
             if (manage->new_duration == INT_MAX) manage->new_duration = ibp_time_now() + r->max_duration;
             if (manage->new_duration > (ibp_time_now()+r->max_duration)) {
                status = IBP_E_WOULD_EXCEED_POLICY;
                log_printf(10, "handle_manage: Duration >max_duration  id: " LU " rid=%s\n", id, r->name);
             }

             if (manage->new_duration == 0) { //** Inherit duration from master allocation
                manage->new_duration = ma.expiration;
             }
             a->expiration = manage->new_duration;
          }

          alog_append_alias_manage_change(task->myid, r->rl_index, a->id, a->alias_offset, a->alias_size, a->expiration);
        } else {
          if (manage->new_size > 0) {
             if (a->size > manage->new_size) {
                log_printf(10, "handle_manage: curr_size>new_size! id: " LU " rid=%s\n", id, r->name);
                status = IBP_E_WOULD_DAMAGE_DATA;
             } else {
                a->max_size = manage->new_size;
             }
          }
          if (manage->new_reliability >= 0) a->reliability = manage->new_reliability;

          if (manage->new_duration > 0) {
             if (manage->new_duration == INT_MAX) manage->new_duration = ibp_time_now() + r->max_duration;
             if (manage->new_duration > (ibp_time_now()+r->max_duration)) {
                status = IBP_E_WOULD_EXCEED_POLICY;
                log_printf(10, "handle_manage: Duration >max_duration  id: " LU " rid=%s\n", id, r->name);
             }
             a->expiration = manage->new_duration;
          }

          alog_append_manage_change(task->myid, r->rl_index, a->id, a->max_size, a->reliability, a->expiration);
        }

        //** Update the manage timestamp
        if (is_alias == 1) {
           update_manage_history(r, a->id, a->is_alias, &(task->ipadd), cmd->command, manage->subcmd, a->alias_offset, a->expiration, a->alias_size, pid);
        } else {
           update_manage_history(r, a->id, a->is_alias, &(task->ipadd), cmd->command, manage->subcmd, a->reliability, a->expiration, a->max_size, pid);
        }

        err = modify_allocation_resource(r, a->id, a);

        if (err == 0) {
           send_cmd_result(task, status);
        } else {
           send_cmd_result(task, IBP_E_WOULD_EXCEED_POLICY);
        }

        break;
     case IBP_PROBE:
        if (a->reliability == ALLOC_HARD) rel = IBP_HARD;

        if (cmd->command == IBP_MANAGE) {
           alog_append_manage_probe(task->myid, r->rl_index, id);
           log_printf(15, "handle_manage: poffset=" LU " plen=" LU " a->size=" LU " a->max_size=" LU " ns=%d\n", 
                   alias_offset, alias_len, a->size, a->max_size, tbx_ns_getid(task->ns));
           pmax_size = a->max_size - alias_offset;
           if (pmax_size > alias_len) pmax_size = alias_len;
           psize = a->size - alias_offset;
           if (psize > alias_len) psize = alias_len;

           snprintf(buf, sizeof(buf)-1, "%d %d %d " LU " " LU " %ld %d %d \n",
                  IBP_OK, a->read_refcount, a->write_refcount, psize, pmax_size, a->expiration - ibp_time_now(),
                  rel, a->type);
        } else { //** IBP_ALIAS_MANAGE
           alog_append_alias_manage_probe(task->myid, r->rl_index, pid, id);
           snprintf(buf, sizeof(buf)-1, "%d %d %d " LU " " LU " %ld \n",
               IBP_OK, a->read_refcount, a->write_refcount, a->alias_offset, a->alias_size, a->expiration - ibp_time_now());
        }

        //** Update the manage timestamp
        update_manage_history(r, a->id, a->is_alias, &(task->ipadd), cmd->command, manage->subcmd, a->reliability, a->expiration, a->max_size, pid);
        err = modify_allocation_resource(r, a->id, a);
        if (err != 0) {
           log_printf(0, "handle_manage/probe:  Error with modify_allocation_resource for new queue allocation!  err=%d\n", err); 
        }

        log_printf(10, "handle_manage: probe results = %s\n",buf);
        server_ns_write_block(task->ns, task->cmd_timeout, buf, strlen(buf));

        alog_append_cmd_result(task->myid, IBP_OK);
        break;
  }

//  //** Update the manage timestamp
//  update_manage_history(r, a->id, a->is_alias, &(task->ipadd), cmd->command, manage->subcmd, a->reliability, a->expiration, a->max_size, pid);
//  err = modify_allocation_resource(r, a->id, a);
//  if (err != 0) {
//     log_printf(0, "handle_manage/probe:  Error with modify_allocation_resource for new queue allocation!  err=%d\n", err); 
//  }

  cmd->state = CMD_STATE_FINISHED;
  log_printf(10, "handle_manage: Sucessfully processed manage command\n");

  unlock_osd_id(a->id); 

  return(0);
}

//*****************************************************************
// handle_validate_chksum  - Handles the IBP_VALIDATE_CHKSUM commands
//
// Returns
//    status n_errors\n
//
//*****************************************************************

int handle_validate_chksum(ibp_task_t *task)
{
  Cmd_state_t *cmd = &(task->cmd); 
  Cmd_write_t *w = &(cmd->cargs.write);
  Allocation_t *a = &(w->a);
  osd_id_t id, pid, aid, apid;
  int err;
  ibp_off_t alias_offset, alias_len;
  char buffer[128];

  debug_printf(1, "handle_validate_chksum: Starting to process command tid=" LU " ns=%d\n", task->tid, tbx_ns_getid(task->ns));

  pid = 0; id = 0;

  w->r = resource_lookup(global_config->rl, w->crid);
  if (w->r == NULL) {
     log_printf(10, "handle_validate_chksum:  Invalid RID :%s  tid=" LU "\n",w->crid, task->tid); 
     alog_append_write(task->myid, cmd->command, w->r->rl_index, pid, id, w->iovec.vec[0].off, w->iovec.vec[0].len);
     alog_append_validate_get_chksum(task->myid, cmd->command, -1, 0, 0);
     send_cmd_result(task, IBP_E_INVALID_RID);
     return(global_config->soft_fail);
  }

  if ((err = get_allocation_by_cap_resource(w->r, MANAGE_CAP, &(w->cap), a)) != 0) {
     log_printf(10, "handle_validate_chksum: Invalid cap: %s for resource = %s  tid=" LU "\n", w->cap.v, w->r->name,task->tid);
     alog_append_validate_get_chksum(task->myid, cmd->command, w->r->rl_index, 0, 0);
     send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
     return(global_config->soft_fail);
  }

  pid = a->id;
  apid = 0; aid = a->id;
  alias_offset = 0;
  alias_len = a->max_size;
  if (a->is_alias == 1) {  //** This is a alias allocation so load the actual one
     id = a->alias_id;
     apid = pid; aid = id;

     alias_offset = a->alias_offset;
     alias_len =  a->alias_size;

     if ((err = get_allocation_resource(w->r, id, a)) != 0) {
        alog_append_validate_get_chksum(task->myid, cmd->command, w->r->rl_index, pid, id);
        log_printf(10, "handle_validate_chksum: Invalid alias_id: " LU " for resource = %s  tid=" LU " ns=%d\n", id, w->r->name,task->tid, tbx_ns_getid(task->ns));
        send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
        return(global_config->soft_fail);
     }

     if (alias_len == 0) alias_len = a->max_size - alias_offset;
  } else {
    apid = 0;
  }

  alog_append_validate_get_chksum(task->myid, cmd->command, w->r->rl_index, apid, aid);

  //** Can only chksum  alias allocation is for the whole allocation
  if ((alias_offset != 0) && (alias_len != 0)) {
     log_printf(10, "handle_validate_chksum: Attempt to validate an alias allocation without full access! cap: %s r = %s  tid=" LU "\n", w->cap.v, w->r->name, task->tid);
     send_cmd_result(task, IBP_E_WOULD_EXCEED_LIMIT);
     return(global_config->soft_fail);
  }

  //** Now we can execute the validate operation
  err = validate_allocation(w->r, a->id, w->iovec.total_len);

  //** Now send the result back
  snprintf(buffer, sizeof(buffer), "%d %d\n", IBP_OK, err);
  server_ns_write_block(task->ns, task->cmd_timeout, buffer, strlen(buffer));
  log_printf(10, "handle_validate_chksum: ns=%d Return string: %s", tbx_ns_getid(task->ns), buffer);
  log_printf(10, "handle_validate_chksum: Exiting write tid=" LU " err=%d\n", task->tid, err);

  return(0); 
}

//*****************************************************************
// handle_get_chksum  - Handles the IBP_GET_CHKSUM commands
//
// Returns if chksum_info_only == 1
//    status cs_type cs_size block_size nblocks  nbytes\n
//
// if chksum_info_only == 0
//    status cs_type cs_size block_size nblocks nbytes\n
//    ...nbytes_of_chksum...
//
// or if no chksum inforation is available
//    IBP_OK CHKSUM_NONE \n
//*****************************************************************

int handle_get_chksum(ibp_task_t *task)
{
  Cmd_state_t *cmd = &(task->cmd);
  Cmd_write_t *w = &(cmd->cargs.write);
  Allocation_t *a = &(w->a);
  osd_id_t id, pid, aid, apid;
  int err, cs_type, cs_size;
  ibp_off_t start_block, end_block, nblocks, blocksize, nbuf, i, csbytes;
  tbx_chksum_t chksum;
  ibp_off_t alias_offset, alias_len;
  int bufsize = 1024*1024;
  char buffer[bufsize];

  log_printf(10, "handle_get_chksum: Starting to process command tid=" LU " ns=%d chksum_only=" I64T "\n", task->tid, tbx_ns_getid(task->ns), w->iovec.total_len);

  pid = 0; id = 0;
  w->r = resource_lookup(global_config->rl, w->crid);
  if (w->r == NULL) {
     log_printf(10, "handle_get_chksum:  Invalid RID :%s  tid=" LU "\n",w->crid, task->tid); 
     alog_append_validate_get_chksum(task->myid, cmd->command, -1, 0, 0);
     send_cmd_result(task, IBP_E_INVALID_RID);
     return(global_config->soft_fail);
  }

  if ((err = get_allocation_by_cap_resource(w->r, MANAGE_CAP, &(w->cap), a)) != 0) {
     log_printf(10, "handle_get_chksum: Invalid cap: %s for resource = %s  tid=" LU "\n", w->cap.v, w->r->name,task->tid);
     alog_append_validate_get_chksum(task->myid, cmd->command, w->r->rl_index, 0, 0);
     send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
     return(global_config->soft_fail);
  }

  pid = a->id;

  alias_offset = 0;
  alias_len = a->max_size;
  if (a->is_alias == 1) {  //** This is a alias allocation so load the actual one
     id = a->alias_id;

     apid = pid; aid = id;

     alias_offset = a->alias_offset;
     alias_len =  a->alias_size;

     if ((err = get_allocation_resource(w->r, id, a)) != 0) {
        alog_append_validate_get_chksum(task->myid, cmd->command, w->r->rl_index, pid, id);
        log_printf(10, "handle_get_chksum: Invalid alias_id: " LU " for resource = %s  tid=" LU " ns=%d\n", id, w->r->name,task->tid, tbx_ns_getid(task->ns));
        send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
        return(global_config->soft_fail);
     }

     if (alias_len == 0) alias_len = a->max_size - alias_offset;
  } else {
    apid = 0; aid = pid;
  }

  alog_append_validate_get_chksum(task->myid, cmd->command, w->r->rl_index, apid, aid);

  //** Can only chksum  alias allocation is for the whole allocation
  if ((alias_offset != 0) && (alias_len != 0)) {
     log_printf(10, "handle_get_chksum: Attempt to validate an alias allocation without full access! cap: %s r = %s  tid=" LU "\n", w->cap.v, w->r->name, task->tid);
     send_cmd_result(task, IBP_E_WOULD_EXCEED_LIMIT);
     return(global_config->soft_fail);
  }

  //** Get the chksum information
  err = get_allocation_chksum_info(w->r, a->id, &cs_type, &nblocks, &blocksize);
  if (err != 0) {  //** No chksum stored
     log_printf(10, "handle_get_chksum: No chksum info. cap: %s r = %s tid=" LU "\n", w->cap.v, w->r->name, task->tid);
     send_cmd_result(task, IBP_E_CHKSUM);
     return(global_config->soft_fail);
  }
  tbx_chksum_set(&chksum, cs_type);
  cs_size = tbx_chksum_size(&chksum, CHKSUM_DIGEST_BIN);

  nblocks = a->size / blocksize;
  if ((a->size%blocksize) > 0) nblocks++;  //** Handle a partial block
//not counted in a->size  nblocks--; //** Don't send the header block
  nbuf = cs_size * nblocks;
  
  //*Format: status cs_type block_size nblocks cs_size nbytes\n
  snprintf(buffer, sizeof(buffer), "%d %d %d " I64T " " I64T " " I64T "\n", IBP_OK, cs_type, cs_size, blocksize, nblocks, nbuf);
  log_printf(10, "handle_get_chksum: ns=%d Sending result: %s", tbx_ns_getid(task->ns), buffer);
  server_ns_write_block(task->ns, task->cmd_timeout, buffer, strlen(buffer));

  //** See if the want the chksum data as well  otherwise exit
  if (w->iovec.total_len == 1) {
     log_printf(10, "handle_validate_chksum: Exiting chksum_only=1 tid=" LU " err=%d\n", task->tid, err);
     return(0);
  }

  //** Now stream the chksum data back
//  nblocks++;  //** Add it back in for the loop
  nbuf = nblocks * cs_size; nbuf = bufsize / nbuf;
  csbytes = nbuf * cs_size;

  for (i=1; i<nblocks; i=i+nbuf) {
     start_block = i;
     end_block = i + nbuf - 1;
     if (end_block > nblocks) end_block = nblocks;
     csbytes = (end_block - start_block + 1) * cs_size;

     err = get_allocation_chksum(w->r, a->id, buffer, NULL, bufsize, NULL, NULL, start_block, end_block);
     if (err != csbytes) {
        log_printf(10, "handle_get_chksum:  Disk error probably occured! ns=%d err=%d\n", tbx_ns_getid(task->ns), err);
        tbx_ns_close(task->ns);
        return(-1);
     }

     //** Send the data back
     err = server_ns_write_block(task->ns, task->cmd_timeout, buffer, csbytes);    
     if (err != 0) {
        log_printf(10, "handle_get_chksum: Command or network error! ns=%d err=%d\n", tbx_ns_getid(task->ns), err);
        tbx_ns_close(task->ns);
        return(-1);
     }
  }

  log_printf(10, "handle_validate_chksum: Exiting write tid=" LU " err=%d\n", task->tid, err);

  return(0); 
}


//*****************************************************************
// handle_write  - HAndles the IBP_STORE and IBP_WRITE commands
//
// Returns
//    status \n
//
//*****************************************************************

int handle_write(ibp_task_t *task)
{
  Cmd_state_t *cmd = &(task->cmd);
  Cmd_write_t *w = &(cmd->cargs.write);
  Allocation_t *a = &(w->a);
  Allocation_t a_final;
  osd_fd_t *fd;
  osd_id_t id, pid, aid, apid;
  int err, state, i;
  ibp_off_t alias_offset, alias_len, alias_end, nleft, off, len;
  int append_mode = 0;

  err = 0;

  debug_printf(1, "handle_write: Starting to process command tid=" LU " ns=%d chksum_enable=%d\n", task->tid, tbx_ns_getid(task->ns), task->enable_chksum);

  task->stat.start = ibp_time_now();
  task->stat.dir = DIR_IN;
  task->stat.id = task->tid;

  if (task->enable_chksum == 1) { tbx_ns_chksum_read_set(task->ns, task->ncs); tbx_ns_chksum_read_enable(task->ns); }

  w->r = resource_lookup(global_config->rl, w->crid);
  if (w->r == NULL) {
     log_printf(10, "handle_write:  Invalid RID :%s  tid=" LU "\n",w->crid, task->tid); 
     alog_append_write(task->myid, cmd->command, -1, 0, 0, w->iovec.vec[0].off, w->iovec.vec[0].len);
     send_cmd_result(task, IBP_E_INVALID_RID);
     return(global_config->soft_fail);
  }

  //** Resource is not mounted with write access
  if ((resource_get_mode(w->r) & RES_MODE_WRITE) == 0) {
     log_printf(10, "handle_write: Write access is disabled cap: %s RID=%s\n", w->cap.v, w->r->name);
     alog_append_write(task->myid, cmd->command, w->r->rl_index, 0, 0, w->iovec.vec[0].off, w->iovec.vec[0].len);
     send_cmd_result(task, IBP_E_FILE_WRITE);
     return(0);
  }

  if ((err = get_allocation_by_cap_resource(w->r, WRITE_CAP, &(w->cap), a)) != 0) {
     log_printf(10, "handle_write: Invalid cap: %s for resource = %s  tid=" LU "\n", w->cap.v, w->r->name,task->tid);
     alog_append_write(task->myid, cmd->command, w->r->rl_index, 0, 0, w->iovec.vec[0].off, w->iovec.vec[0].len);
     send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
     return(global_config->soft_fail);
  }

  pid = a->id;
  id = 0;

  alias_offset = 0;
  alias_len = a->max_size;
  if (a->is_alias == 1) {  //** This is a alias allocation so load the actual one
     id = a->alias_id;

     apid = pid; aid = id;

     alias_offset = a->alias_offset;
     alias_len =  a->alias_size;

     if ((err = get_allocation_resource(w->r, id, a)) != 0) {
        alog_append_write(task->myid, cmd->command, w->r->rl_index, pid, id, w->iovec.vec[0].off, w->iovec.vec[0].len);
        log_printf(10, "handle_write: Invalid alias_id: " LU " for resource = %s  tid=" LU " ns=%d\n", id, w->r->name,task->tid, tbx_ns_getid(task->ns));
        send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
        return(global_config->soft_fail);
     }

     if (alias_len == 0) alias_len = a->max_size - alias_offset;
  } else {
    apid = 0; aid = pid;
  }

  //** Validate the writing range **
  if (w->iovec.vec[0].off == -1)  { //Wants to append data
     w->iovec.vec[0].off = a->size;
     append_mode = 1;
  }

//  if ( w->iovec.n == 1) {
     alog_append_write(task->myid, cmd->command, w->r->rl_index, apid, aid, w->iovec.vec[0].off, w->iovec.vec[0].len);
//  } else {
//     alog_append_write_iovec(task->myid, cmd->command, w->r->rl_index, apid, aid, &(w->iovec));
//  }

  //** Can only append if the alias allocation is for the whole allocation
  if ((append_mode == 1) && (alias_offset != 0) && (alias_len != 0)) {
     log_printf(10, "handle_write: Attempt to append to an allocation with a alias cap without full access! cap: %s r = %s tid=" LU "\n", w->cap.v, w->r->name, task->tid);
     send_cmd_result(task, IBP_E_WOULD_EXCEED_LIMIT);
     return(global_config->soft_fail);

  }

  //** Cycle through each off/len combination
  for (i=0; i<w->iovec.n; i++) {
     w->iovec.vec[i].off += alias_offset;
     off = w->iovec.vec[i].off;
     len = w->iovec.vec[i].len;

     if (((off+len) > a->max_size) && (a->type == IBP_BYTEARRAY)) {
        log_printf(10, "handle_write: Attempt to write beyond end of allocation! cap: %s r = %s i=%d off=" LU " len=" LU "  tid=" LU "\n", w->cap.v, w->r->name, i, off, len, task->tid);
        send_cmd_result(task, IBP_E_WOULD_EXCEED_LIMIT);
        return(global_config->soft_fail);
     }

     //** check if we are inside the alias bounds
     alias_end = alias_offset + alias_len;
     if (((off+len) > alias_end) && (a->type == IBP_BYTEARRAY)) {
        log_printf(10, "handle_write: Attempt to write beyond end of alias range! cap: %s r = %s  i=%d off=" I64T " len=" I64T " poff = " I64T " plen= " I64T " tid=" LU "\n", 
                 w->cap.v, w->r->name, i, off, len, alias_offset, alias_len, task->tid);
        send_cmd_result(task, IBP_E_WOULD_EXCEED_LIMIT);
        return(global_config->soft_fail);
     }
  }

  //** Verify the allocation is good
  fd = open_allocation(w->r, a->id, OSD_READ_MODE);
  state = get_allocation_state(w->r, fd);
  if (state != OSD_STATE_GOOD) {
     close_allocation(w->r, fd);
     send_cmd_result(task, IBP_E_CHKSUM);
     return(global_config->soft_fail);
  }

  send_cmd_result(task, IBP_OK); //** Notify the client that everything is Ok and start sending data

  if (a->type == IBP_BYTEARRAY) {
     err = 0;
     nleft = w->iovec.total_len;
     w->iovec.transfer_total = 0;
     if (a->type == IBP_BYTEARRAY) a->w_pos = w->iovec.vec[0].off;

     while (err == 0) {
        log_printf(20, "handle_write: a->w_pos=" LU " left=" LU " ns=%d\n", a->w_pos, nleft, tbx_ns_getid(task->ns));
        err = write_to_disk(task, a, &nleft, w->r);
        if (err == 0) {
           if (apr_time_now() > task->cmd_timeout) {
              log_printf(15, "handle_write: EXPIRED command!  ctime=" TT " to=" TT "\n", apr_time_now(), task->cmd_timeout);
              err = -1;
           }
        }
     }

     //** Validate the partial block if needed
     if ((task->enable_chksum == 1) && (err == 1)) {
        err = tbx_ns_chksum_read_flush(task->ns);
        tbx_ns_chksum_read_disable(task->ns);
        if (err != 0) {
          log_printf(15, "handle_write: ns=%d last block chksum error!\n", tbx_ns_getid(task->ns));
          close_allocation(w->r, fd);
          send_cmd_result(task, IBP_E_CHKSUM);
          return(global_config->soft_fail);
        } else {
          err = 1;
        }
     }
  }

  add_stat(&(task->stat));

  int bufsize = 128;
  char buffer[bufsize];

  if (err == -1) {  //** Dead connection
     log_printf(10, "handle_write:  Disk error occured! ns=%d\n", tbx_ns_getid(task->ns));
     alog_append_cmd_result(task->myid, IBP_E_SOCK_WRITE);
     tbx_ns_close(task->ns);
  } else if (err == 1) {  //** Finished command
     //** Update the amount of data written if needed
     lock_osd_id(a->id);

     //** Update the write timestamp
     update_write_history(w->r, a->id, a->is_alias, &(task->ipadd), w->iovec.vec[0].off, w->iovec.vec[0].len, pid);

     err = get_allocation_resource(w->r, a->id, &a_final);
     log_printf(15, "handle_write: ns=%d a->size=" LU " db_size=" LU "\n", tbx_ns_getid(task->ns), a->size, a_final.size);
     if (err == 0) {
        if (a->size > a_final.size ) {
           a_final.size = a->size;
           if (a->type == IBP_BYTEARRAY) a->size = a->w_pos;

           err = modify_allocation_resource(w->r, a->id, &a_final);
           if (err != 0) {
              log_printf(10, "handle_write:  ns=%d ERROR with modify_allocation_resource(%s, " LU ", a)=%d\n", tbx_ns_getid(task->ns), w->crid, a->id, err);
              err = IBP_E_INTERNAL;
           }
        }
     } else {
        log_printf(10, "handle_write: ns=%d error with final get_allocation_resource(%s, " LU ", a)=%d\n", tbx_ns_getid(task->ns),
             w->r->name, a->id, err);
        if (err != 0) err = IBP_E_INTERNAL;
     }
     unlock_osd_id(a->id);


     len = w->iovec.total_len;
     if (err == 0) {
        state = get_allocation_state(w->r, fd);
        if (state != OSD_STATE_GOOD) {
           err = IBP_E_CHKSUM;
        } else {
           err = IBP_OK;
        }
     }

     alog_append_cmd_result(task->myid, err);
     snprintf(buffer, bufsize-1, "%d " LU " \n", err, len);
     log_printf(10, "handle_write:  ns=%d Sending result: %s\n", tbx_ns_getid(task->ns), buffer);
     server_ns_write_block(task->ns, task->cmd_timeout, buffer, strlen(buffer));
     if (err == IBP_OK) err = 0;
  }

  close_allocation(w->r, fd);

  log_printf(10, "handle_write: Exiting write tid=" LU "\n", task->tid);
  return(err);
}

//*****************************************************************
//  handle_read  - Handles the IBP_LOAD command
//
//  Returns
//    status nbytes \n
//    ...raw data stream...
//
//*****************************************************************

int handle_read(ibp_task_t *task)
{
  Cmd_state_t *cmd = &(task->cmd);
  Cmd_read_t *r = &(cmd->cargs.read);
  Allocation_t *a = &(r->a);
  int err, state, i;
  osd_fd_t *fd;
  osd_id_t id, pid;
  ibp_off_t alias_offset, alias_len, alias_end, nleft, off, len;

  err = 0;

  debug_printf(1, "handle_read: Starting to process command ns=%d\n", tbx_ns_getid(task->ns));

  task->stat.start = ibp_time_now();
  task->stat.dir = DIR_OUT;
  task->stat.id = task->tid;

  r->r = resource_lookup(global_config->rl, r->crid);
  if (r->r == NULL) {
     alog_append_read(task->myid, -1, 0, 0, r->iovec.vec[0].off, r->iovec.vec[0].len);
     log_printf(10, "handle_read:  Invalid RID :%s\n",r->crid); 
     send_cmd_result(task, IBP_E_INVALID_RID);
     return(0);
  }

  //** Resource is not mounted with read access
  if ((resource_get_mode(r->r) & RES_MODE_READ) == 0) {
     log_printf(10, "handle_read: Read access is disabled cap: %s RID=%s\n", r->cap.v, r->r->name);
     alog_append_read(task->myid, r->r->rl_index, 0, 0, r->iovec.vec[0].off, r->iovec.vec[0].len);
     send_cmd_result(task, IBP_E_FILE_READ);
     return(0);
  }

  if ((err = get_allocation_by_cap_resource(r->r, READ_CAP, &(r->cap), a)) != 0) {
     log_printf(10, "handle_read: Invalid cap: %s for resource = %s\n", r->cap.v, r->r->name);
     alog_append_read(task->myid, r->r->rl_index, 0, 0, r->iovec.vec[0].off, r->iovec.vec[0].len);
     send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
     return(0);
  }

  pid = a->id;
  alias_offset = 0;
  alias_len = a->max_size;
log_printf(10, "handle_read: id: " LU " is_alias=%d cmd offset=" OT " ns=%d\n", a->id, a->is_alias, r->iovec.vec[0].off, tbx_ns_getid(task->ns));

  if (a->is_alias == 1) {  //** This is a alias allocation so load the actual one
     id = a->alias_id;
     alias_offset = a->alias_offset;
     alias_len =  a->alias_size;
log_printf(10, "handle_read: mid: " LU " offset=" I64T " len=" I64T " ns=%d\n", id, alias_offset, alias_len, tbx_ns_getid(task->ns));

     if ((err = get_allocation_resource(r->r, id, a)) != 0) {
        log_printf(10, "handle_read: Invalid alias_id: " LU " for resource = %s  tid=" LU " ns=%d\n", id, r->r->name,task->tid, tbx_ns_getid(task->ns));
        alog_append_read(task->myid, r->r->rl_index, pid, id, r->iovec.vec[0].off, r->iovec.vec[0].len);
        send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
        return(global_config->soft_fail);
     }

     if (alias_len == 0) alias_len = a->max_size - alias_offset;

     alog_append_read(task->myid, r->r->rl_index, pid, id, r->iovec.vec[0].off, r->iovec.vec[0].len);
  } else {
    alog_append_read(task->myid, r->r->rl_index, 0, pid, r->iovec.vec[0].off, r->iovec.vec[0].len);
  }

  //** Validate the reading range **
  for (i=0; i<r->iovec.n; i++) {
     r->iovec.vec[i].off += alias_offset;
     off = r->iovec.vec[i].off;
     len = r->iovec.vec[i].len;

     if (((off+len) > a->max_size) && (a->type == IBP_BYTEARRAY)) {
        log_printf(10, "handle_read: Attempt to read beyond end of allocation! cap: %s r = %s i=%d off=" LU " len=" LU "\n", r->cap.v, r->r->name, i, off, len);
        send_cmd_result(task, IBP_E_WOULD_EXCEED_LIMIT);
        return(0);
     }

     //** check if we are inside the alias bounds
     alias_end = alias_offset + alias_len;
     if (((off+len) > alias_end) && (a->type == IBP_BYTEARRAY)) {
        log_printf(10, "handle_read: Attempt to write beyond end of alias range! cap: %s r = %s i=%d off=" I64T " len=" I64T " poff = " I64T " plen= " I64T " tid=" LU "\n", 
                 r->cap.v, r->r->name, i, off, len, alias_offset, alias_len, task->tid);
        send_cmd_result(task, IBP_E_WOULD_EXCEED_LIMIT);
        return(global_config->soft_fail);
     }

     //*** and make sure there is data ***
     if (((off+len) > a->size) && (a->type == IBP_BYTEARRAY)) {
        log_printf(10, "handle_read: Not enough data! cap: %s r = %s i=%d off=" LU " alen=" LU " curr_size=" LU "\n", 
              r->cap.v, r->r->name, i, off, len, a->size);
        send_cmd_result(task, IBP_E_WOULD_EXCEED_LIMIT);
        return(0);
     }

  }

  if (a->type == IBP_BYTEARRAY) {
     a->r_pos = r->iovec.vec[0].off;
  }


  //** Verify the allocation is good
  fd = open_allocation(r->r, a->id, OSD_READ_MODE);
  state = get_allocation_state(r->r, fd);
  if (state != OSD_STATE_GOOD) {
     close_allocation(r->r, fd);
     send_cmd_result(task, IBP_E_CHKSUM);
     return(global_config->soft_fail);
  }

  char buffer[1024];
  snprintf(buffer, sizeof(buffer)-1, "%d " LU " \n", IBP_OK, r->iovec.total_len);
  log_printf(15, "handle_read: response=%s\n", buffer);
  server_ns_write_block(task->ns, task->cmd_timeout, buffer, strlen(buffer));

  if (a->type == IBP_BYTEARRAY) {
     if (task->enable_chksum == 1) { tbx_ns_chksum_write_set(task->ns, task->ncs); tbx_ns_chksum_write_enable(task->ns); }
     err = 0;
     nleft = r->iovec.total_len;
     r->iovec.transfer_total = 0;

     while (err == 0) {
        log_printf(20, "handle_read: a->r_pos=" LU " left=" LU " ns=%d\n", a->r_pos, nleft, tbx_ns_getid(task->ns));
        err = read_from_disk(task, a, &nleft, r->r);
        if (err == 0) {
           if (apr_time_now() > task->cmd_timeout) {
              log_printf(15, "handle_read: EXPIRED command!  ctime=" TT " to=" TT "\n", apr_time_now(), task->cmd_timeout);
              err = -1;
           }
        }
     }

    //** Validate the partial block if needed
     if ((task->enable_chksum == 1) && (err == 1)) {
        err = tbx_ns_chksum_write_flush(task->ns);
        tbx_ns_chksum_write_disable(task->ns);
        if (err != 0) {
          log_printf(15, "handle_read: ns=%d last block chksum error!\n", tbx_ns_getid(task->ns));
          err = -1;
        } else {
          err = 1;
        }
     }
  }

  //** Update the read timestamp
  lock_osd_id(a->id);
  update_read_history(r->r, a->id, a->is_alias, &(task->ipadd), r->iovec.vec[0].off, r->iovec.vec[0].len, pid);
  unlock_osd_id(a->id);

  add_stat(&(task->stat));


  if (err == -1) {  //** Dead connection
     log_printf(10, "handle_read:  Dead connection!\n");
     alog_append_cmd_result(task->myid, IBP_E_SOCK_WRITE);
     tbx_ns_close(task->ns);
  } else {
    alog_append_cmd_result(task->myid, IBP_OK);
    err = 0;
    state = get_allocation_state(r->r, fd);
    if (state != OSD_STATE_GOOD) { err = IBP_E_CHKSUM; }
  }

  close_allocation(r->r, fd);

  log_printf(10, "handle_read: Exiting read\n");
  return(err);
}

//*****************************************************************
// same_depot_copy - Makes a same depot-depot copy
//     if rem_offset < 0 then the data is appended
//*****************************************************************

int same_depot_copy(ibp_task_t *task, char *rem_cap, int rem_offset, osd_id_t rpid)
{
   Cmd_state_t *cmd = &(task->cmd); 
   Cmd_read_t *r = &(cmd->cargs.read);
   Allocation_t *a = &(r->a);
   int alias_end, cmode;
   int err, finished;
   int src_state, dest_state;
   osd_fd_t *src_fd, *dest_fd;
   Resource_t *rem_r, *src_r, *dest_r;
   Allocation_t rem_a, temp_a;
   Allocation_t *src_a, *dest_a;
   osd_id_t wpid, did;
   char dcrid[128];
   char *bstate;
   rid_t drid;
   Cap_t dcap;

   log_printf(10, "same_depot_copy: Starting to process command ns=%d rem_cap=%s\n", tbx_ns_getid(task->ns), rem_cap);
   alias_end = -1;

   //** Get the RID and key...... the format is RID#key
   char *tmp;
   dcrid[sizeof(dcrid)-1] = '\0';
   tmp = tbx_stk_string_token(rem_cap, "#", &bstate, &finished);
   if (ibp_str2rid(tmp, &(drid)) != 0) {
      log_printf(1, "same_depot_copy: Bad RID: %s\n", tmp);
      send_cmd_result(task, IBP_E_INVALID_RID);
      cmd->state = CMD_STATE_FINISHED;
      return(0);
   }
   ibp_rid2str(drid, dcrid);

   //** Get the write key
   dcap.v[sizeof(dcap.v)-1] = '\0';
   strncpy(dcap.v, tbx_stk_string_token(NULL, " ", &bstate, &finished), sizeof(dcap.v)-1);
   debug_printf(10, "same_depot_copy: remote_cap=%s\n", dcap.v);

   debug_printf(10, "same_depot_copy: RID=%s\n", dcrid);

   rem_r = resource_lookup(global_config->rl, dcrid);
   if (rem_r == NULL) {
      log_printf(10, "same_depot_copy:  Invalid RID :%s\n",dcrid); 
      send_cmd_result(task, IBP_E_INVALID_RID);
      return(0);
   }

   cmode = WRITE_CAP;
   if ((cmd->command == IBP_PULL) || (cmd->command == IBP_PULL_CHKSUM)) cmode = READ_CAP;

   if ((err = get_allocation_by_cap_resource(rem_r, cmode, &(dcap), &rem_a)) != 0) {
      log_printf(10, "same_depot_copy: Invalid destcap: %s for resource = %s\n", dcap.v, rem_r->name);
      send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
      return(0);
   }

   wpid = rem_a.id;
   did = rem_a.id;

   //*** ALIAS cap so load the master **
   if (rem_a.is_alias) {
      if ((rem_a.alias_offset > 0) && (rem_offset < 0)) {  //** This is an append but the alias can't do it
         log_printf(10, "same_depot_copy: destcap: %s on resource = %s is a alias with offset=" LU "\n", dcap.v, rem_r->name, rem_a.alias_offset);
         send_cmd_result(task, IBP_E_CAP_ACCESS_DENIED);
         return(0);
      }

      alias_end = rem_a.alias_offset + rem_a.alias_size;
      did = rem_a.alias_id;
   }

   lock_osd_id(did);

   //*** Now get the true allocation ***
   if ((err = get_allocation_resource(rem_r, did, &rem_a)) != 0) {
      log_printf(10, "same_depot_copy: Invalid destcap: %s for resource = %s\n", dcap.v, rem_r->name);
      send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
      unlock_osd_id(did);
      return(0);
   }

   if (rem_offset < 0) rem_offset = rem_a.size;  //** This is an append copy

   if (rem_a.is_alias) {
      rem_offset = rem_offset + rem_a.alias_offset;  //** Tweak the offset based on the alias bounds

      //** Validate the alias writing range **
      if (((rem_offset + r->iovec.vec[0].len) > alias_end) && (rem_a.type ==IBP_BYTEARRAY)) {
         log_printf(10, "same_depot_copy: Attempt to write beyond end of alias allocation! cap: %s r = %s off=%d len=" LU "\n", 
             dcap.v, rem_r->name, rem_offset, r->iovec.vec[0].len);
         send_cmd_result(task, IBP_E_WOULD_EXCEED_LIMIT);
         unlock_osd_id(did);
         return(0);
      }

   }

   //** Validate the writing/reading range **
   if (((rem_offset + r->iovec.vec[0].len) > rem_a.max_size) && (rem_a.type ==IBP_BYTEARRAY)) {
      log_printf(10, "same_depot_copy: Attempt to write beyond end of allocation! cap: %s r = %s off=%d len=" LU "\n", 
          dcap.v, rem_r->name, rem_offset, r->iovec.vec[0].len);
      send_cmd_result(task, IBP_E_WOULD_EXCEED_LIMIT);
      unlock_osd_id(did);
      return(0);
   }

   unlock_osd_id(did);

//---------------
   //*** Now do the copy ***
   int soff, doff, src_off, dest_off;
   osd_id_t spid, dpid;
   if (r->transfer_dir == IBP_PULL) {
      src_a = &rem_a;   src_r = rem_r;  soff = rem_offset;            spid = wpid;
      dest_a = a;       dest_r = r->r;  doff = r->iovec.vec[0].off;   dpid = rpid;
   } else {
      src_a = a;        src_r = r->r;   soff = r->iovec.vec[0].off;   spid = rpid;
      dest_a = &rem_a;  dest_r = rem_r; doff = rem_offset;            dpid = wpid;
   }
   src_off = soff; dest_off = doff;

   //** Verify both allocation states
   src_fd = open_allocation(src_r, src_a->id, OSD_READ_MODE);
   src_state = get_allocation_state(src_r, src_fd);
   if (src_state != OSD_STATE_GOOD) {
      close_allocation(src_r, src_fd);
      send_cmd_result(task, IBP_E_CHKSUM);
      return(global_config->soft_fail);
   }
   dest_fd = open_allocation(dest_r, dest_a->id, OSD_READ_MODE);
   dest_state = get_allocation_state(dest_r, dest_fd);
   if (dest_state != OSD_STATE_GOOD) {
      close_allocation(src_r, src_fd);
      close_allocation(dest_r, dest_fd);
      send_cmd_result(task, IBP_E_CHKSUM);
      return(global_config->soft_fail);
   }

log_printf(15, "same_depot_copy: rem_offset=%d r->offset=" OT " ns=%d\n", rem_offset, r->iovec.vec[0].off, tbx_ns_getid(task->ns));

   err = disk_to_disk_copy(src_r, src_a->id, soff, dest_r, dest_a->id, doff, r->iovec.vec[0].len, task->cmd_timeout);
   if (err != IBP_OK) {
      send_cmd_result(task, err);
      cmd->state = CMD_STATE_FINISHED;
      return(0);
   }

   //** Verify no chksum errors during the transfer process
   src_state = get_allocation_state(src_r, src_fd);
   dest_state = get_allocation_state(dest_r, dest_fd);
   close_allocation(src_r, src_fd);
   close_allocation(dest_r, dest_fd);
   if ((src_state != OSD_STATE_GOOD) || (dest_state != OSD_STATE_GOOD)) {
      send_cmd_result(task, IBP_E_CHKSUM);
      cmd->state = CMD_STATE_FINISHED;
      return(0);
   }

   soff = soff + r->iovec.vec[0].len; doff = doff + r->iovec.vec[0].len;

   char result[512];
   result[511] = '\0';


   //** Update the amount of data written if needed
   lock_osd_id(dest_a->id);
   //** update the dest write history
   update_write_history(dest_r, dest_a->id, dest_a->is_alias, &(task->ipadd), dest_off, r->iovec.vec[0].len, dpid);

   err = get_allocation_resource(dest_r, dest_a->id, &temp_a);
   log_printf(15, "same_depot_copy: ns=%d dest->size=" LU " doff=%d\n", tbx_ns_getid(task->ns), rem_a.size, doff);
   if (err == 0) {
      if (doff > temp_a.size) {
         temp_a.size = doff;
         temp_a.w_pos = doff;
         err = modify_allocation_resource(dest_r, dest_a->id, &temp_a);
         if (err != 0) {
            log_printf(10, "same_depot_copy:  ns=%d ERROR with modify_allocation_resource(%s, " LU ", a)=%d for dest\n", tbx_ns_getid(task->ns),
                  dcrid, rem_a.id, err);
            err = IBP_E_INTERNAL;
         }
      }
   } else {
     log_printf(10, "same_depot_copy: ns=%d error with final get_allocation_resource(%s, " LU ", a)=%d for dest\n", tbx_ns_getid(task->ns),
           dcrid, rem_a.id, err);
     err = IBP_E_INTERNAL;
   }

   //** Now update the timestamp for the read allocation
   update_read_history(src_r, src_a->id, src_a->is_alias, &(task->ipadd), src_off, r->iovec.vec[0].len, spid);

   unlock_osd_id(dest_a->id);

   if (err == 0) {
      err = IBP_OK;
   }

   alog_append_cmd_result(task->myid, err);
   snprintf(result, 511, "%d " LU " \n", err, r->iovec.vec[0].len);

   log_printf(10, "same_depot_copy: ns=%d Completed successfully.  Sending result: %s", tbx_ns_getid(task->ns), result); 
   server_ns_write_block(task->ns, task->cmd_timeout, result, strlen(result));
   log_printf(15, "handle_copysend: END pns=%d cns=%d---same_depot_copy-------------------------\n", tbx_ns_getid(task->ns), tbx_ns_getid(task->ns));

   return(0);
}

//*****************************************************************
//  handle_copy  - Handles the IBP_SEND command
//
//  Returns
//    results from child command - status nbytes
//
//*****************************************************************

int handle_copy(ibp_task_t *task)
{
  Cmd_state_t *cmd = &(task->cmd);
  Cmd_read_t *r = &(cmd->cargs.read);
  Allocation_t *a = &(r->a);
  char key[sizeof(r->remote_cap)], typekey[sizeof(r->remote_cap)];
  char addr[16];
  int err, cmode, i;
  osd_id_t id, rpid, aid, apid;
  ibp_off_t alias_offset, alias_len, alias_end;
  phoebus_t ppath;

  memset(addr, 0, sizeof(addr));

  err = 0;

  log_printf(10, "handle_copy: Starting to process command ns=%d cmd=%d\n", tbx_ns_getid(task->ns), cmd->command);

ibp_iovec_t *iov= &(r->iovec);
log_printf(15, "iovec->n=%d off=" OT " len=" OT " cumulative_len=" OT "\n", iov->n, iov->vec[0].off, iov->vec[0].len, iov->vec[0].cumulative_len);

  task->stat.start = ibp_time_now();
  task->stat.dir = DEPOT_COPY_OUT;
  task->stat.id = task->tid;

  r->r = resource_lookup(global_config->rl, r->crid);
  if (r->r == NULL) {
     log_printf(10, "handle_copy:  Invalid RID :%s\n",r->crid);
     alog_append_dd_copy(cmd->command, task->myid, -1, 0, 0, r->iovec.vec[0].len, 0, 0, r->write_mode, r->ctype,
           r->path, 0, AF_INET, addr, r->remote_cap, "");
     send_cmd_result(task, IBP_E_INVALID_RID);
     return(0);
  }

  cmode = READ_CAP;
  if ((cmd->command == IBP_PULL) || (cmd->command == IBP_PULL_CHKSUM)) cmode = WRITE_CAP;

  if ((err = get_allocation_by_cap_resource(r->r, cmode, &(r->cap), a)) != 0) {
     log_printf(10, "handle_copy: Invalid cap: %s for resource = %s\n", r->cap.v, r->r->name);
     alog_append_dd_copy(cmd->command, task->myid, r->r->rl_index, 0, 0, r->iovec.vec[0].len, 0, 0, r->write_mode,
          r->ctype, r->path, 0, AF_INET, addr, r->remote_cap, "");
     send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
     return(0);
  }

  rpid = a->id;
  aid = a->id;
  apid = 0;

  alias_offset = 0;
  alias_len = a->max_size;
  if (a->is_alias == 1) {  //** This is a alias allocation so load the actual one
     id = a->alias_id;
     aid = id;
     apid = a->id;

     alias_offset = a->alias_offset;
     alias_len =  a->alias_size;

     if ((err = get_allocation_resource(r->r, id, a)) != 0) {
        log_printf(10, "handle_copy: Invalid alias_id: " LU " for resource = %s  tid=" LU " ns=%d\n", id, r->r->name,task->tid, tbx_ns_getid(task->ns));
        send_cmd_result(task, IBP_E_CAP_NOT_FOUND);
        return(global_config->soft_fail);
     }

     if (alias_len == 0) alias_len = a->max_size - alias_offset;
  }

  //** Validate the reading or writing range for the local allocation **
  if (((cmd->command == IBP_PULL) || (cmd->command == IBP_PULL_CHKSUM)) && (r->write_mode == 1)) {
     r->iovec.vec[0].off = a->size;  //** PULL with append
  } else {
     r->iovec.vec[0].off += alias_offset;
  }
  if (((r->iovec.vec[0].off + r->iovec.vec[0].len) > a->max_size) && (a->type ==IBP_BYTEARRAY)) {
     log_printf(10, "handle_copy: Attempt to read beyond end of allocation! cap: %s r = %s off=" LU " len=" LU "\n", r->cap.v, r->r->name, r->iovec.vec[0].off, r->iovec.vec[0].len);
     alog_append_dd_copy(cmd->command, task->myid, r->r->rl_index, apid, aid, r->iovec.vec[0].len, r->iovec.vec[0].off,
           r->remote_offset, r->write_mode, r->ctype, r->path, 0, AF_INET, addr, r->remote_cap, "");
     send_cmd_result(task, IBP_E_WOULD_EXCEED_LIMIT);
     return(0);
  }

  //** check if we are inside the alias bounds
  alias_end = alias_offset + alias_len;
  if (((r->iovec.vec[0].off + r->iovec.vec[0].len) > alias_end) && (a->type == IBP_BYTEARRAY)) {
     log_printf(10, "handle_copy: Attempt to write beyond end of alias range! cap: %s r = %s off=" I64T " len=" I64T " poff = " I64T " plen= " I64T " tid=" LU "\n", 
              r->cap.v, r->r->name, r->iovec.vec[0].off, r->iovec.vec[0].len, alias_offset, alias_len, task->tid);
     alog_append_dd_copy(cmd->command, task->myid, r->r->rl_index, apid, aid, r->iovec.vec[0].len, r->iovec.vec[0].off,
           r->remote_offset, r->write_mode, r->ctype, r->path, 0, AF_INET, addr, r->remote_cap, "");
     send_cmd_result(task, IBP_E_WOULD_EXCEED_LIMIT);
     return(global_config->soft_fail);
  }


  //*** and make sure there is data for PUSH commands***
  if (r->transfer_dir == IBP_PUSH) {
     if (((r->iovec.vec[0].off + r->iovec.vec[0].len) > a->size) && (a->type ==IBP_BYTEARRAY)) {
        log_printf(10, "handle_copy: Not enough data! cap: %s r = %s off=" LU " alen=" LU " curr_size=" LU "\n",
              r->cap.v, r->r->name, r->iovec.vec[0].off, r->iovec.vec[0].len, a->size);
        alog_append_dd_copy(cmd->command, task->myid, r->r->rl_index, apid, aid, r->iovec.vec[0].len,
              r->iovec.vec[0].off, r->remote_offset, r->write_mode, r->ctype, r->path, 0, AF_INET, addr, r->remote_cap, "");
        send_cmd_result(task, IBP_E_WOULD_EXCEED_LIMIT);
        return(0);
     }
  }

log_printf(15, "iovec->n=%d off=" OT " len=" OT " cumulative_len=" OT "\n", iov->n, iov->vec[0].off, iov->vec[0].len, iov->vec[0].cumulative_len);

  //*** Parse the remote cap for the host/port
  int fin, rport;
  char *bstate;
  char *rhost;
  char *temp = strdup(r->remote_cap);
  rhost = tbx_stk_string_token(temp, "/", &bstate, &fin); //** gets the ibp:/
  rhost = tbx_stk_string_token(NULL, ":", &bstate, &fin); //** This should be the host name
  rhost = &(rhost[1]);  //** Skip the extra "/"
  sscanf(tbx_stk_string_token(NULL, "/", &bstate, &fin), "%d", &rport);
  strncpy(key, tbx_stk_string_token(NULL, "/", &bstate, &fin), sizeof(key)-1); key[sizeof(key)-1] = '\0';
  strncpy(typekey, tbx_stk_string_token(NULL, "/", &bstate, &fin), sizeof(typekey)-1); typekey[sizeof(typekey)-1] = '\0';

  tbx_dnsc_lookup(rhost, addr, NULL);
  alog_append_dd_copy(cmd->command, task->myid, r->r->rl_index, apid, aid, r->iovec.vec[0].len,
       r->iovec.vec[0].off, r->remote_offset, r->write_mode, r->ctype, r->path, rport, AF_INET, addr, key, typekey);

  log_printf(15, "handle_copy: rhost=%s rport=%d cap=%s key=%s typekey=%s\n", rhost, rport, r->remote_cap, key, typekey);

  //** check if it's a copy to myself
  for (i=0; i<global_config->server.n_iface; i++) {
     if ((strcmp(rhost, global_config->server.iface[i].hostname) == 0) && (rport == global_config->server.iface[i].port)) {
        err = same_depot_copy(task, key, r->remote_offset, rpid);
        free(temp);
        return(err);
     }
  }

  //** Make the connection
  tbx_ns_timeout_t tm;
  tbx_ns_t *ns = tbx_ns_new();
  apr_time_t to = task->cmd_timeout - apr_time_now() - apr_time_make(5, 0);
  if (apr_time_sec(to) > 20) to = apr_time_make(20, 0);  //** If no connection in 20 sec die anyway
  if (apr_time_sec(to) <= 0 ) to = apr_time_make(1, 0);
  tbx_ns_timeout_set(&tm, to, 0);
  ppath.p_count = 0;

  if (r->ctype  == IBP_PHOEBUS) {
abort();  //******FIXME
//FIXME
//     if (r->path[0] == '\0') {
//        log_printf(5, "handle_copy: using default phoebus path to %s:%d\n", rhost, rport);
//        ns_config_phoebus(ns, NULL, 0);
//     } else {
//        log_printf(5, "handle_copy: using phoebus path r->path= %s to %s:%d\n", r->path, rhost, rport);
//        phoebus_path_set(&ppath, r->path);
//        ns_config_phoebus(ns, &ppath, 0);
//     }
  } else {
     tbx_ns_sock_config(ns, 0);
  }
  err = tbx_ns_connect(ns, rhost, rport, tm);
  if (err != 0) {
     log_printf(5, "handle_copy: tbx_ns_connect returned an error err=%d to host %s:%d\n",err, rhost, rport);
     send_cmd_result(task, IBP_E_CONNECTION);
     if (ppath.p_count != 0) phoebus_path_destroy(&ppath);
     tbx_ns_destroy(ns);
     free(temp);
     return(0);
  }

log_printf(15, "iovec->n=%d off=" OT " len=" OT " cumulative_len=" OT "\n", iov->n, iov->vec[0].off, iov->vec[0].len, iov->vec[0].cumulative_len);

  //** Send the data
  err = handle_transfer(task, rpid, ns, key, typekey);

  log_printf(10, "handle_copy: End of routine Remote host:%s:%d ns=%d\n", rhost, rport, tbx_ns_getid(task->ns));

  tbx_ns_destroy(ns);
  if (ppath.p_count != 0) phoebus_path_destroy(&ppath);

  free(temp);
  return(err);
}

//*****************************************************************
//  handle_transfer  - Handles the actual transfer
//*****************************************************************

int handle_transfer(ibp_task_t *task, osd_id_t rpid, tbx_ns_t *ns, const char *key, const char *typekey)
{
  Cmd_state_t *cmd = &(task->cmd);
  Cmd_read_t *r = &(cmd->cargs.read);
  Allocation_t *a = &(r->a);
  Allocation_t a_final;
  char *bstate;
  int return_err, err, myid, state;
  int fin;
  osd_fd_t *fd;
  long long unsigned int llu;
  char write_cmd[1024];
  ibp_off_t rlen, nleft, iov_off;

  myid = tbx_ns_getid(task->ns);

  log_printf(10, "handle_transfer: Starting to process command ns=%d\n", tbx_ns_getid(task->ns));

ibp_iovec_t *iov= &(r->iovec);
log_printf(15, "iovec->n=%d off=" OT " len=" OT " cumulative_len=" OT "\n", iov->n, iov->vec[0].off, iov->vec[0].len, iov->vec[0].cumulative_len);

  //** Verify the allocation is good
  fd = open_allocation(r->r, a->id, OSD_READ_MODE);
  state = get_allocation_state(r->r, fd);
  if (state != OSD_STATE_GOOD) {
     close_allocation(r->r, fd);
     send_cmd_result(task, IBP_E_CHKSUM);
     return(global_config->soft_fail);
  }

  int rto = r->remote_sto;
  rlen = r->iovec.vec[0].len;

  switch (r->transfer_dir) {
    case IBP_PUSH:
       if (r->write_mode == 1) {
          snprintf(write_cmd, 1024, "%d %d %s %s " OT " %d \n", IBPv040, IBP_STORE, key, typekey, rlen, rto);
       } else {
          snprintf(write_cmd, 1024, "%d %d %s %s " OT " " OT " %d \n", IBPv040, IBP_WRITE, key, typekey, r->remote_offset, rlen, rto);
       }
       break;
    case IBP_PUSH_CHKSUM:
       if (r->write_mode == 1) {
          snprintf(write_cmd, 1024, "%d %d %d " I64T " %s %s " OT " %d \n", IBPv040, IBP_STORE_CHKSUM, tbx_ns_chksum_type(&(task->ncs)), tbx_ns_chksum_blocksize(&(task->ncs)), key, typekey, rlen, rto);
       } else {
          snprintf(write_cmd, 1024, "%d %d %d " I64T " %s %s " OT " " OT " %d \n", IBPv040, IBP_WRITE_CHKSUM, tbx_ns_chksum_type(&(task->ncs)), tbx_ns_chksum_blocksize(&(task->ncs)),key, typekey, r->remote_offset, rlen, rto);
       }
       break;
    case IBP_PULL:
       snprintf(write_cmd, 1024, "%d %d %s %s " OT " " OT " %d \n", IBPv040, IBP_LOAD, key, typekey, r->remote_offset, rlen, rto);
       break;
    case IBP_PULL_CHKSUM:
       snprintf(write_cmd, 1024, "%d %d %d " I64T " %s %s " OT " " OT " %d \n", IBPv040, IBP_LOAD_CHKSUM, tbx_ns_chksum_type(&(task->ncs)), tbx_ns_chksum_blocksize(&(task->ncs)), key, typekey, r->remote_offset, rlen, rto);
       break;
  }
  log_printf(15, "handle_transfer: ns=%d sending command: %s", tbx_ns_getid(task->ns), write_cmd);
  write_cmd[1023] = '\0';

  err = server_ns_write_block(ns, task->cmd_timeout, write_cmd, strlen(write_cmd));
  if (err != NS_OK) {
     if (task->cmd_timeout > apr_time_now()) {
        log_printf(10, "handle_transfer: TIMEOUT Error sending command! server_ns_write_block=%d  cmd=%s\n", err, write_cmd);
        send_cmd_result(task, IBP_E_SERVER_TIMEOUT);
     } else {
        log_printf(10, "handle_transfer: Error sending command! server_ns_write_block=%d  cmd=%s\n", err, write_cmd);
        send_cmd_result(task, IBP_E_CONNECTION);
     }
     return(0);
  }

log_printf(15, "handle_transfer: ns=%d AAAAAAAAAAAAAAAAAAAA\n", tbx_ns_getid(task->ns));
  tbx_ns_timeout_t dt2;
  tbx_ns_timeout_set(&dt2, rto, 0);
  err = server_ns_readline(ns, write_cmd, sizeof(write_cmd), dt2);
  if (err == -1) { //** Dead connection
     log_printf(10, "handle_transfer:  Failed receving command response rns=%d\n", tbx_ns_getid(ns));
     close_allocation(r->r, fd);
     tbx_ns_close(ns);
     send_cmd_result(task, IBP_E_CONNECTION);
     return(0);
  } else if (write_cmd[strlen(write_cmd)-1] == '\n') {  //** Got a complete line
     log_printf(15, "handle_transfer: ns=%d BBBBBBBBBBBBB response=%s\n", tbx_ns_getid(task->ns), write_cmd);
     err = 0;
     sscanf(tbx_stk_string_token(write_cmd, " ", &bstate, &fin), "%d", &err);
     if (err != IBP_OK) {  //** Got an error
        log_printf(10, "handle_transfer:  Response had an error remote_ns=%d  Error=%d\n", tbx_ns_getid(ns), err);
        close_allocation(r->r, fd);
        send_cmd_result(task, err);
        tbx_ns_close(ns);
        return(0);
     }
     if (r->transfer_dir == IBP_PULL) { //** Check the nbytes
        sscanf(tbx_stk_string_token(NULL, " ", &bstate, &fin), "%llu", &llu);
        if (llu != rlen) {  //** Not enough bytes
           log_printf(10, "handle_transfer:  Not enough bytes! remote_ns=%d  nbytes=%llu\n", tbx_ns_getid(ns), llu);
           close_allocation(r->r, fd);
           send_cmd_result(task, IBP_E_WOULD_EXCEED_LIMIT);
           tbx_ns_close(ns);
           return(0);
        }
     }
  }

  log_printf(15, "handle_send: ns=%d rns=%dCCCCCCCCCCCCCCCCCCCCCC\n", tbx_ns_getid(task->ns), tbx_ns_getid(ns));

  //** For the send copy the remote ns into the task
  tbx_ns_t *pns = task->ns;
  task->ns = ns;
  if (r->transfer_dir == IBP_PUSH) {
     if (task->enable_chksum == 1) { tbx_ns_chksum_write_set(task->ns, task->ncs); tbx_ns_chksum_write_enable(task->ns); }
  } else {
     if (task->enable_chksum == 1) { tbx_ns_chksum_read_set(task->ns, task->ncs); tbx_ns_chksum_read_enable(task->ns); }
  }
  err = 0;
  nleft = rlen;


log_printf(15, "before transfer. nleft=" OT "\n", nleft);
log_printf(15, "iovec->n=%d off=" OT " len=" OT " cumulative_len=" OT "\n", iov->n, iov->vec[0].off, iov->vec[0].len, iov->vec[0].cumulative_len);
tbx_log_flush();

//asm volatile ("int3;");

  while (err == 0) {
     if (r->transfer_dir == IBP_PUSH) {
        err = read_from_disk(task, a, &nleft, r->r);
     } else {
        iov_off = r->iovec.vec[0].off;  //** Write uses the iov for write command so need to copy the contents over
        iovec_single(&(cmd->cargs.write.iovec), iov_off, nleft);
        err = write_to_disk(task, a, &nleft, r->r);
     }
     if (err == 0) {
        if (apr_time_now() > (task->cmd_timeout - apr_time_make(1,0))) err = -1;
     }
  }

  //** Validate the partial block if needed
  if ((task->enable_chksum == 1) && (err == 1)) {
     if (r->transfer_dir == IBP_PUSH) {
        err = tbx_ns_chksum_write_flush(ns);
        tbx_ns_chksum_write_disable(ns);
     } else {
        err = tbx_ns_chksum_read_flush(ns);
        tbx_ns_chksum_read_disable(ns);
     }

     if (err != 0) {
        log_printf(15, "handle_transfer: ns=%d last block chksum error!\n", tbx_ns_getid(ns));
        err = IBP_E_CHKSUM;
     }
  }

  task->ns = pns;  //** Put it back

  add_stat(&(task->stat));

  state = get_allocation_state(r->r, fd);
  if (state != OSD_STATE_GOOD) { err = IBP_E_CHKSUM; }

  if (err == -1) {  //** Dead connection
     if (task->cmd_timeout > apr_time_now()) {
        log_printf(10, "handle_transfer: TIMEOUT Error sencind/recving data! err=%d\n", err);
        send_cmd_result(task, IBP_E_SERVER_TIMEOUT);
     } else {
        log_printf(10, "handle_transfer:  Dead connection\n");
        send_cmd_result(task, IBP_E_CONNECTION);
     }
     tbx_ns_close(ns);
  } else if (err == 1) {  //** Finished command
     char result[512];
     tbx_ns_timeout_t dt;
     tbx_ns_timeout_set(&dt, 1, 0);

     err = 0;

     //** Handle my return result from IBP_STORE/WRITE
     result[0] = '\0';
     server_ns_readline(ns, result, sizeof(result), dt);
     result[511] = '\0';
     log_printf(10, "handle_transfer: ns=%d server returned: %s\n", tbx_ns_getid(task->ns), result);

     sscanf(tbx_stk_string_token(write_cmd, " ", &bstate, &fin), "%d", &err);
     if (err != IBP_OK) {  //** Got an error
        log_printf(10, "handle_transfer:  Response had an error remote_ns=%d  Error=%d\n", tbx_ns_getid(ns), err);
     }

     alog_append_cmd_result(task->myid, err);

     return_err = err;  //** This is what gets returned back to the client.
     err = 0;  //** This gets returned if everything is good.  0=success and waits for the next command

     //** Update the read timestamp
     lock_osd_id(a->id);
     if (r->transfer_dir == IBP_PUSH) {
        update_read_history(r->r, a->id, a->is_alias, &(task->ipadd), r->iovec.vec[0].off, r->iovec.vec[0].len, rpid);
     } else {     //** And also the size if needed
        update_write_history(r->r, a->id, a->is_alias, &(task->ipadd), r->iovec.vec[0].off, r->iovec.vec[0].len, rpid);

        err = get_allocation_resource(r->r, a->id, &a_final);
        log_printf(15, "handle_transfer: ns=%d a->size=" LU " db_size=" LU "\n", tbx_ns_getid(task->ns), a->size, a_final.size);
        if (err == 0) {
           if (a->size > a_final.size ) {
              a_final.size = a->size;

              err = modify_allocation_resource(r->r, a->id, &a_final);
              if (err != 0) {
                 log_printf(10, "handle_transfer:  ns=%d ERROR with modify_allocation_resource(%s, " LU ", a)=%d\n",
                       tbx_ns_getid(task->ns), r->crid, a->id, err);
                 err = IBP_E_INTERNAL;
              }
           }
        } else {
           log_printf(10, "handle_transfer: ns=%d error with final get_allocation_resource(%s, " LU ", a)=%d\n", tbx_ns_getid(task->ns),
                r->r->name, a->id, err);
           if (err != 0) err = IBP_E_INTERNAL;
        }
     }
     unlock_osd_id(a->id);

     //** Send parents result back **
     snprintf(result, 511, "%d " LU " \n", return_err, r->iovec.total_len);
     result[511] = '\0';
     log_printf(10, "handle_transfer: ns=%d Completed.  Sending result: %s", tbx_ns_getid(task->ns), result);
     server_ns_write_block(task->ns, task->cmd_timeout, result, strlen(result));
     log_printf(15, "handle_transfer: END ns=%d rns=%d----------------------------\n", tbx_ns_getid(task->ns), tbx_ns_getid(ns));
  } else if (err == IBP_E_CHKSUM) {  //** chksum error
     send_cmd_result(task, IBP_E_CHKSUM);
  } else {  //** Error!!!
     send_cmd_result(task,  err);
     log_printf(0, "handle_transfer:  Invalid result!!!! err=%d\n", err);
  }

  log_printf(10, "handle_transfer: Completed. ns=%d err=%d\n", myid, err);

  close_allocation(r->r, fd);

  return(err);
}

//*****************************************************************
//  handle_internal_get_config  - Returns the depot config
//
//   returns:
//      IBP_OK  nbytes\n
//      ...config..data....
//*****************************************************************

int handle_internal_get_config(ibp_task_t *task)
{
  char result[128];
  char buffer[100*1024];
  int used = 0;
  int err;

  log_printf(5, "handle_internal_get_config: Start of routine.  ns=%d\n",tbx_ns_getid(task->ns));

  alog_append_internal_get_config(task->myid);

  print_config(buffer, &used, sizeof(buffer), global_config);

  //** Send back the size info
  snprintf(result, sizeof(result), "%d %d\n", IBP_OK, used);
  err = server_ns_write_block(task->ns, task->cmd_timeout, result, strlen(result));

  //** and the config
  err = server_ns_write_block(task->ns, task->cmd_timeout, buffer, used);

  log_printf(5, "handle_internal_get_config: End of routine.  ns=%d error=%d\n",tbx_ns_getid(task->ns), err);

  return(0);
}


//*****************************************************************
//  handle_internal_date_free  - Handles the internal command for determining
//      when xxx amount of space becomes free using eith hard or soft allocations
//
//*****************************************************************

int handle_internal_date_free(ibp_task_t *task)
{
  Cmd_state_t *cmd = &(task->cmd); 
  Cmd_internal_date_free_t *arg = &(cmd->cargs.date_free);
  int a_count, p_count, err;
  uint64_t total_bytes, bytes, bytes_used;
  apr_time_t curr_time;
  Allocation_t a;
  walk_expire_iterator_t *wei;
  Resource_t *r;
  char text[512];

  log_printf(5, "handle_internal_date_free: Start of routine.  ns=%d size= " LU "\n",tbx_ns_getid(task->ns), arg->size);

  r = resource_lookup(global_config->rl, arg->crid);
  if (r == NULL) {
     log_printf(10, "handle_internal_date_free:  Invalid RID :%s\n",arg->crid); 
     alog_append_internal_date_free(task->myid, -1, arg->size);
     send_cmd_result(task, IBP_E_INVALID_RID);
     return(0);
  }

  alog_append_internal_date_free(task->myid, r->rl_index, arg->size);

  //*** Send back the results ***
  send_cmd_result(task, IBP_OK);
  
  wei = walk_expire_iterator_begin(r);
  
  a_count = p_count = 0;
  bytes = bytes_used = total_bytes = 0;
  curr_time = 0;
  
  err = get_next_walk_expire_iterator(wei, DBR_NEXT, &a);
  while ((err == 0) && (arg->size > total_bytes)) {
    if (a.expiration != curr_time) {  //** time change so print the current stats
       if (curr_time != 0) {
          sprintf(text, TT " %d %d " LU " " LU "\n", curr_time, a_count, p_count, bytes_used, bytes);
          log_printf(10, "handle_internal_date_free:  ns=%d sending %s", tbx_ns_getid(task->ns), text);
          err = server_ns_write_block(task->ns, task->cmd_timeout, text, strlen(text));
          if (err != strlen(text)) {
             log_printf(10, "handle_internal_date_free:  ns=%d erro with server_ns_write=%d\n", tbx_ns_getid(task->ns), err);
          } else {
             err = 0;
          }
       }

       curr_time = a.expiration;
       a_count = p_count = bytes = bytes_used = 0;
    } 

    if (a.is_alias) { 
       p_count++;
    } else {
       a_count++;
    }

    bytes_used = bytes_used + a.size;
    bytes = bytes + a.max_size;
    total_bytes = total_bytes + a.max_size;

    log_printf(15, "handle_internal_date_free: ns=%d time=" TT " a_count=%d p_count=%d bytes=" LU " total=" LU "\n", tbx_ns_getid(task->ns), ibp2apr_time(curr_time), a_count, p_count, bytes, total_bytes);

    if (err == 0) err = get_next_walk_expire_iterator(wei, DBR_NEXT, &a);    
  }

  if (curr_time != 0) {
     sprintf(text, TT " %d %d " LU " " LU "\n", curr_time, a_count, p_count, bytes_used, bytes);
     log_printf(10, "handle_internal_date_free:  ns=%d sending %s", tbx_ns_getid(task->ns), text);
     err = server_ns_write_block(task->ns, task->cmd_timeout, text, strlen(text));
     if (err != strlen(text)) {
        log_printf(10, "handle_internal_date_free:  ns=%d erro with server_ns_write=%d\n", tbx_ns_getid(task->ns), err);
     }
  }

  walk_expire_iterator_end(wei);

  //**send the terminator
  err = server_ns_write_block(task->ns, task->cmd_timeout, "END\n", 4);
  if (err == 4) err = 0;

  log_printf(5, "handle_internal_date_free: End of routine.  ns=%d error=%d\n",tbx_ns_getid(task->ns), err);

  return(0);
}


//*****************************************************************
//  handle_internal_expire_list  - Handles the internal command for generating
//      a list of alloctionsto be expired
//
//*****************************************************************

int handle_internal_expire_list(ibp_task_t *task)
{
  Cmd_state_t *cmd = &(task->cmd); 
  Cmd_internal_expire_log_t *arg = &(cmd->cargs.expire_log);
  int i, err;
  ibp_time_t expire_time;
  Allocation_t a;
  walk_expire_iterator_t *wei;
  Resource_t *r;
  char text[512];

  log_printf(5, "handle_internal_expire_list: Start of routine.  ns=%d time= " TT " count= %d\n",tbx_ns_getid(task->ns), arg->start_time, arg->max_rec);

  r = resource_lookup(global_config->rl, arg->crid);
  if (r == NULL) {
     log_printf(10, "handle_internal_expire_list:  Invalid RID :%s\n",arg->crid); 
     alog_append_internal_expire_list(task->myid, -1, arg->start_time, arg->max_rec);
     send_cmd_result(task, IBP_E_INVALID_RID);
     return(0);
  }

  alog_append_internal_expire_list(task->myid, r->rl_index, arg->start_time, arg->max_rec);

  //*** Send back the results ***
  send_cmd_result(task, IBP_OK);
  
  wei = walk_expire_iterator_begin(r);

  set_walk_expire_iterator(wei, arg->start_time);
  
  err = get_next_walk_expire_iterator(wei, DBR_NEXT, &a);
  i = 0;
  while ((err == 0) && (i < arg->max_rec)) {
    expire_time = a.expiration;
    sprintf(text, TT " " LU " " LU "\n", expire_time, a.id, a.max_size);
    log_printf(10, "handle_internal_expire_list:  ns=%d sending %s", tbx_ns_getid(task->ns), text);
    err = server_ns_write_block(task->ns, task->cmd_timeout, text, strlen(text));
    if (err != strlen(text)) {
       log_printf(10, "handle_internal_expire_list:  ns=%d error with server_ns_write=%d\n", tbx_ns_getid(task->ns), err);
    } else {
       err = 0;
    }

    if (err == 0) err = get_next_walk_expire_iterator(wei, DBR_NEXT, &a);    
    i++;
  }

  walk_expire_iterator_end(wei);

  //**send the terminator
  err = server_ns_write_block(task->ns, task->cmd_timeout, "END\n", 4);
  if (err == 4) err = 0;

  log_printf(5, "handle_internal_expire_list: End of routine.  ns=%d error=%d\n",tbx_ns_getid(task->ns), err);

  return(0);
}


//*****************************************************************
//  handle_internal_undelete  - Handles the internal command for undeleting
//      alloction located in an expire bin
//
//*****************************************************************

int handle_internal_undelete(ibp_task_t *task)
{
  Cmd_state_t *cmd = &(task->cmd); 
  Cmd_internal_undelete_t *arg = &(cmd->cargs.undelete);
  int err = IBP_OK;
  ibp_time_t expire_time;
  Resource_t *r;

  log_printf(5, "handle_internal_undelete: Start of routine.  ns=%d trash_type=%d trash_id=%s\n",tbx_ns_getid(task->ns), arg->trash_type, arg->trash_id);

  r = resource_lookup(global_config->rl, arg->crid);
  if (r == NULL) {
     log_printf(10, "handle_internal_undelete:  Invalid RID :%s\n",arg->crid); 
//     alog_append_internal_undelete(task->myid, -1, arg->trash_type, arg->trash_id, arg->duration);
     send_cmd_result(task, IBP_E_INVALID_RID);
     return(0);
  }

//  alog_append_internal_undelete(task->myid, r->rl_index, arg->trash_type, arg->trash_id, arg->duration);

  expire_time = ibp_time_now() + arg->duration;
  err = resource_undelete(r, arg->trash_type, arg->trash_id, expire_time, NULL);
  if (err == 0) err = IBP_OK;

  //*** Send back the results ***
  send_cmd_result(task, err);

  log_printf(5, "handle_internal_undelete: End of routine.  ns=%d error=%d\n",tbx_ns_getid(task->ns), err);

  return(0);
}

//*****************************************************************
//  handle_internal_rescan  - Handles the internal command for rescanning
//     the expired and deleted trash bins.
//
//   NOTE: If RID=0 then all resources are rescanned
//
//*****************************************************************

int handle_internal_rescan(ibp_task_t *task)
{
  Cmd_state_t *cmd = &(task->cmd);
  Cmd_internal_rescan_t *arg = &(cmd->cargs.rescan);
  resource_list_iterator_t it;
  Resource_t *r;

  log_printf(5, "handle_internal_rescan: Start of routine.  ns=%d RID=%s\n",tbx_ns_getid(task->ns), arg->crid);

  if (ibp_rid_is_empty(arg->rid)) {
     it = resource_list_iterator(global_config->rl);
     while ((r = resource_list_iterator_next(global_config->rl, &it)) != NULL) {
        resource_rescan(r);
     }
     resource_list_iterator_destroy(global_config->rl, &it);
  } else {
     r = resource_lookup(global_config->rl, arg->crid);
     if (r == NULL) {
        log_printf(10, "handle_internal_rescn:  Invalid RID :%s\n",arg->crid);
        send_cmd_result(task, IBP_E_INVALID_RID);
        return(0);
     }
     resource_rescan(r);
  }

  //*** Send back the results ***
  send_cmd_result(task, IBP_OK);

  log_printf(5, "handle_internal_rescan: End of routine.  ns=%d\n",tbx_ns_getid(task->ns));

  return(0);
}

//*****************************************************************
//  rid_log_append - Appends the text to the RID log file
//*****************************************************************

void rid_log_append(char *crid, char *mode, char *result, char *msg, apr_time_t start, apr_time_t end)
{
  FILE *fd;
  char sbuf[128], ebuf[128];

  fd = fopen(global_config->server.rid_log, "a");
  if (fd == NULL) {
     log_printf(0, "ERROR opening RID log!  rid_log=%s\n", global_config->server.rid_log);
     return;
  }

  apr_ctime(sbuf, start);
  apr_ctime(ebuf, end);

  apr_thread_mutex_lock(shutdown_lock);
  fprintf(fd, "%s (" TT ") - %s (" TT ")|%s|%s|%s|%s\n", sbuf, start, ebuf, end, crid, mode, result, msg);
  fclose(fd);
  apr_thread_mutex_unlock(shutdown_lock);

}


//*****************************************************************
//  handle_internal_mount  - Handles the internal command for mounting
//     a new RID.   THe RID must exist in the depot configuration file
//
//*****************************************************************

int handle_internal_mount(ibp_task_t *task)
{
  Cmd_state_t *cmd = &(task->cmd);
  Cmd_internal_mount_t *arg = &(cmd->cargs.mount);
  int fin, found, err;
  char *bstate;
  char *str, *sgrp, *srid, *smode;
  apr_time_t start;

  tbx_inip_group_t *igrp;
  Resource_t *r;
  tbx_inip_file_t *keyfile;

  log_printf(5, "handle_internal_mount: Start of routine.  ns=%d RID=%s\n",tbx_ns_getid(task->ns), arg->crid);

  start = apr_time_now();

  //* Load the config file
  keyfile = tbx_inip_file_read(global_config->config_file);
  if (keyfile == NULL) {
    log_printf(0, "handle_internal_mount:  Error parsing config file! file=%s\n", global_config->config_file);
    return(IBP_E_INTERNAL);
  }

  //** Find the group
  sgrp = NULL;
  igrp = tbx_inip_group_first(keyfile);
  found = 0;
  for (igrp = tbx_inip_group_first(keyfile); igrp != NULL; igrp = tbx_inip_group_next(igrp)) {
     sgrp = tbx_inip_group_get(igrp);
     if (strncmp("resource", sgrp, 8) == 0) {
        str = strdup(sgrp);
        tbx_stk_string_token(str, " ", &bstate, &fin);  //** This retreives the "resource"
        srid = tbx_stk_string_token(NULL, " ", &bstate, &fin);  //** This should be the RID
        tbx_stk_string_token(NULL, " ", &bstate, &fin);
        if ((fin == 1) && (strcmp(srid, arg->crid) == 0)) { //** Got a match!
           free(str);
           found = 1;
           break;
        }
        free(str);
     }
  }

  if (found == 0) {
     log_printf(0, "handle_internal_mount:  Can't find RID %s in config file! Exiting\n", arg->crid);
     tbx_inip_destroy(keyfile);
     rid_log_append(arg->crid, "ATTACH", "Not in config", arg->msg, start, apr_time_now());
     send_cmd_result(task, IBP_E_INVALID_RID);
     return(0);
  }

  //** Make sure it's not already being added
  apr_thread_mutex_lock(shutdown_lock);
  if (resource_lookup(global_config->rl, arg->crid) != NULL) {
     err = 1;
     smode = "Already mounted";
     log_printf(10, "handle_internal_mount: Already mounted RID :%s\n",arg->crid);
  } else {
     smode = "Pending";
     err = resource_list_pending_insert(global_config->rl, arg->crid);
  }
  apr_thread_mutex_unlock(shutdown_lock);
  if (err != 0) {
     log_printf(0, "Already trying to insert RID=%s\n", arg->crid);
     tbx_inip_destroy(keyfile);
     rid_log_append(arg->crid, "ATTACH", smode, arg->msg, start, apr_time_now());

     //*** Send back the results ***
     send_cmd_result(task, IBP_E_WOULD_DAMAGE_DATA);
     return(0);
  }

  //*** Send back the results while I go ahead and process things***
  send_cmd_result(task, IBP_OK);

//log_printf(0, "Sleeping for 30\n"); tbx_log_flush();
//sleep(30);
//log_printf(0, "Waking up\n"); tbx_log_flush();

  //** NOTE: read_internal_mount verified the RID isn't mounted.
  assert((r = (Resource_t *)malloc(sizeof(Resource_t))) != NULL);

  err = mount_resource(r, keyfile, sgrp, global_config->dbenv,
        arg->force_rebuild, global_config->server.lazy_allocate,
        global_config->truncate_expiration);

  tbx_inip_destroy(keyfile);

  if (err != 0) {
     log_printf(0, "handle_internal_mount:  Error mounting resource!! Exiting\n");
     rid_log_append(arg->crid, "ATTACH", "ERROR during mount", arg->msg, start, apr_time_now());
     send_cmd_result(task, IBP_E_INTERNAL);
     free(r);
     return(0);
  }

  //** Activate it
  r->rl_index = resource_list_pending_activate(global_config->rl, arg->crid, r);

  rid_log_append(arg->crid, "ATTACH", "SUCCESS", arg->msg, start, apr_time_now());

  //** Launch the garbage collection threads
  launch_resource_cleanup_thread(r);

  log_printf(5, "handle_internal_mount: End of routine.  ns=%d\n",tbx_ns_getid(task->ns));

  return(0);
}


//*****************************************************************
//  handle_internal_umount  - Handles the internal command for unmounting
//     an existing RID.
//
//*****************************************************************

int handle_internal_umount(ibp_task_t *task)
{
  Cmd_state_t *cmd = &(task->cmd);
  Cmd_internal_mount_t *arg = &(cmd->cargs.mount);
  Resource_t *r;
  int err, last_count, count;
  apr_time_t last_change, dt;
  apr_time_t start;

  log_printf(5, "handle_internal_umount: Start of routine. RID=%s\n", arg->crid);

  start = apr_time_now();

  //** Get the RID.... read_internal_umount verified the RID was valid
  apr_thread_mutex_lock(shutdown_lock);
  r = resource_lookup(global_config->rl, arg->crid);

  //** First remove the RID from the resource_list
  if (r != NULL) resource_list_delete(global_config->rl, r);
  apr_thread_mutex_unlock(shutdown_lock);

  if (r == NULL) {
     log_printf(5, "Missing resource!  rid=%s\n", arg->crid);
     err = 1;
     goto fail;
  }

  //** Now sleep until the RID has quiesced
  last_count = resource_get_counter(r);
  last_change = apr_time_now();
  dt = 0;
  while (dt < arg->delay) {
    sleep(1);
    count = resource_get_counter(r);
    if (count != last_count) {
       last_change = apr_time_now();
       last_count = count;
    }

    dt = apr_time_now() - last_change;
    dt = apr_time_sec(dt);
    log_printf(5, "dt=%ld delay=%d count=%d last_count=%d\n", dt, arg->delay, count, last_count);
  }

//JUST FOR TESTING PLEASE REMOVE and UNCOMMENT earlier line
//resource_list_delete(global_config->rl, r);

  //** Actually umount the resource
  if ((err = umount_resource(r)) != 0) {
     log_printf(0, "handle_internal_umount: Error closing Resource %s!  Err=%d\n",arg->crid, err);
  }

  free(r);  //** Free the space

fail:

  if (err != 0) {
     log_printf(0, "handle_internal_umount:  Error mounting resource!! Exiting\n");
     rid_log_append(arg->crid, "DETACH", "ERROR", arg->msg, start, apr_time_now());
     send_cmd_result(task, IBP_E_INTERNAL);
  } else {
     rid_log_append(arg->crid, "DETACH", "SUCCESS", arg->msg, start, apr_time_now());
     send_cmd_result(task, IBP_OK);
  }

  log_printf(5, "handle_internal_umount: End of routine.\n");

  return(0);
}

//*****************************************************************
//  handle_internal_set_mode  - Handles the internal command for
//      setting the Read/Write/Manage mode for the RID
//
//*****************************************************************

int handle_internal_set_mode(ibp_task_t *task)
{
  Cmd_state_t *cmd = &(task->cmd);
  Cmd_internal_mode_t *arg = &(cmd->cargs.mode);
  Resource_t *r;

  log_printf(5, "handle_internal_set_mode: Start of routine.  ns=%d rid=%s mode=%d\n",tbx_ns_getid(task->ns), arg->crid, arg->mode);

  r = resource_lookup(global_config->rl, arg->crid);
  if (r == NULL) {
     log_printf(10, "handle_internal_set_mode:  Invalid RID :%s\n",arg->crid);
     send_cmd_result(task, IBP_E_INVALID_RID);
     return(0);
  }

  resource_set_mode(r, arg->mode);

  //*** Send back the results ***
  send_cmd_result(task, IBP_OK);

  log_printf(5, "handle_internal_set_mode: End of routine.  ns=%d\n",tbx_ns_getid(task->ns));

  return(0);
}


