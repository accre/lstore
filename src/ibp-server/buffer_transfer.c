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

//************************************************************************************
//************************************************************************************
#ifndef _GNU_SOURCE
#  define _GNU_SOURCE
#endif

#include <fcntl.h>
#include <unistd.h>

#include <string.h>
#include <limits.h>
#include <time.h>
#include <ibp-server/ibp_server.h>
#include <tbx/log.h>
#include "debug.h"
#include "allocation.h"
#include "resource.h"
#include <tbx/network.h>
#include "ibp_task.h"
#include "ibp_protocol.h"

//************************************************************************************
// iovec_start - Determines the starting position in the iovec list
//************************************************************************************

void iovec_start(ibp_iovec_t *iovec, int *index, ibp_off_t *ioff, ibp_off_t *ileft)
{
  int i;
  ibp_off_t n;

  for (i=0; i<iovec->n; i++) {
log_printf(15, "iovec_start: i=%d transfer_total=" I64T " off=" I64T " len=" I64T " cumlen= " I64T "\n", 
	i, iovec->transfer_total, iovec->vec[i].off, iovec->vec[i].len, iovec->vec[i].cumulative_len);
     if (iovec->transfer_total < iovec->vec[i].cumulative_len) break;  
  }

  if (i < iovec->n) {
     *index = i;
     n = iovec->transfer_total;
     if (i > 0) n = iovec->transfer_total - iovec->vec[i-1].cumulative_len;
     (*ioff) = iovec->vec[i].off + n;
     (*ileft) = iovec->vec[i].len - n;
  } else {
     *index = -1;
     *ioff = 0;
     *ileft = 0;
  }

log_printf(15, "iovec_start: n=%d transfer_total=" I64T " index= %d ioff=" I64T " ileft=" I64T "\n", 
   iovec->n, iovec->transfer_total, *index, *ioff, *ileft);
  return;
}


//************************************************************************************
// iovec_single - Initializes an iovec structure with a single task
//************************************************************************************

void iovec_single(ibp_iovec_t *iovec, ibp_off_t off, ibp_off_t len)
{
  iovec->n = 1;
  iovec->transfer_total = 0;
  iovec->total_len = len;
  iovec->vec[0].off = off;
  iovec->vec[0].len = len;
  iovec->vec[0].cumulative_len = len;
}

//************************************************************************************
// ---------------------------- User space routines ----------------------------------
//************************************************************************************

//************************************************************************************
//  disk_to_disk_copy_user - Copies data between allocations using user space buffers
//     IBP_OK - Successful copy
//     any other value  - error
//************************************************************************************

int disk_to_disk_copy_user(Resource_t *src_res, osd_id_t src_id, ibp_off_t src_offset,
                      Resource_t *dest_res, osd_id_t dest_id, ibp_off_t dest_offset, ibp_off_t len, apr_time_t end_time)
{
  int i;
  osd_fd_t *sfd, *dfd;
  ibp_off_t nleft, soff, doff, nbytes, err;

  const int bufsize = 1048576;
  char buffer[bufsize];

  log_printf(15, "disk_to_disk_copy_user: src_id=" LU " src_offset=" I64T " dest_id=" LU " dest_offset=" I64T "\n", src_id, src_offset, dest_id, dest_offset);
  
  nleft = len;
  soff = src_offset; doff = dest_offset;

  sfd = open_allocation(src_res, src_id, OSD_READ_MODE);
  if (sfd == NULL) {
     log_printf(0, "disk_to_disk_copy_user: Error with src open_allocation(-res-, " LU ", " I64T ", " I64T ", buffer) = %d\n", src_id, soff, len, errno);
     return(IBP_E_FILE_READ);
  }

  dfd = open_allocation(dest_res, dest_id, OSD_WRITE_MODE);
  if (dfd == NULL) {
     err = errno;
     log_printf(0, "disk_to_disk_copy_user: Error with dest open_allocation(-res-, " LU ", " I64T ", " I64T ", buffer) = " I64T "\n", dest_id, doff, len, err);
     close_allocation(dest_res, dfd);
     return(IBP_E_FILE_WRITE);
  }

  for (i=0; i<len; i=i+bufsize) {
     nbytes = (nleft > bufsize) ? bufsize : nleft;
     nleft = nleft - bufsize;
     err = read_allocation(src_res, sfd, soff, nbytes, buffer);
     if (err != 0) {
        log_printf(0, "disk_to_disk_copy_user: Error with read_allocation(-res-, " LU ", " I64T ", " I64T ", buffer) = " I64T "\n", src_id, soff, nbytes, err);
        close_allocation(src_res, sfd);
        close_allocation(dest_res, dfd);
        return(IBP_E_FILE_READ);
     }

     err = write_allocation(dest_res, dfd, doff, nbytes, buffer);
     if (err != 0) {
        log_printf(0, "disk_to_disk_copy_user: Error with write_allocation(-res-, " LU ", " I64T ", " I64T ", buffer) = " I64T "\n", dest_id, doff, nbytes, err);
        close_allocation(src_res, sfd);
        close_allocation(dest_res, dfd);
        return(IBP_E_FILE_WRITE);
     }

     soff = soff + nbytes;
     doff = doff + nbytes;
  } 

  close_allocation(src_res, sfd);
  close_allocation(dest_res, dfd);

  return(IBP_OK);
}

//************************************************************************************
//  read_from_disk_user - Reads data from the disk buffer and transfers it using
//     "user space" bufers.  Return values are
//    -1 -- Dead connection
//     0 -- Transfered as much as data as possible
//     1 -- Completed provided task
//************************************************************************************

int read_from_disk_user(ibp_task_t *task, Allocation_t *a, ibp_off_t *left, Resource_t *res)
{
  tbx_ns_t *ns = task->ns;
  int bufsize = 2*1048576;
  osd_fd_t *fd;
  ibp_off_t  nbytes, nwrite, shortwrite, nleft, err;
  ibp_off_t bpos, btotal, bleft, ioff, ileft;
  char buffer[bufsize];
  tbx_ns_timeout_t dt;
  int task_status;
  int finished, index;
  ibp_off_t cleft;
  Cmd_state_t *cmd = &(task->cmd);
  Cmd_read_t *r = &(cmd->cargs.read);
  ibp_iovec_t *iovec = &(r->iovec);

  nbytes = a->size;
  log_printf(10, "read_from_disk: ns=%d id=" LU " a.size=" I64T " a.r_pos=" I64T " len=" I64T "\n", tbx_ns_getid(task->ns), a->id, nbytes, a->r_pos, *left);
tbx_log_flush();
  if (*left == 0) return(1);  //** Nothing to do

  task_status = 0;
  tbx_ns_timeout_set(&dt, 1, 0);  //** set the max time we'll wait for data

  shortwrite = 0;

  fd = open_allocation(res, a->id, OSD_READ_MODE);
  if (fd == NULL) {
     log_printf(0, "read_disk_user: Error with open_allocation(-res-, " LU ") = %d\n", a->id,  errno); 
     return(IBP_E_FILE_READ);
  }

  iovec_start(&(r->iovec), &index, &ioff, &ileft);

  nleft = *left;
  do {
     shortwrite = 0;

     //** Fill the buffer
     bpos = 0;
     bleft = bufsize;
     nbytes = 0;
     finished = 0;
     do {
        cleft = (bleft > ileft) ? ileft : bleft;
        err = read_allocation(res, fd, ioff, cleft, &(buffer[bpos]));
        if (err != 0) {
           char tmp[128];
           log_printf(0, "read_disk: Error with read_allocation(%s, " LU ", " I64T ", " I64T ", buffer) = " I64T "\n",
                ibp_rid2str(res->rid, tmp), a->id, ioff, cleft, err); 
           shortwrite = 100;
           nwrite = err;
        }

        bleft -= cleft;
        bpos += cleft;
        ileft -= cleft;
        ioff += cleft;
        if ((ileft <= 0) && (index < (iovec->n-1))) {
           index++;
           ileft = iovec->vec[index].len;
           ioff = iovec->vec[index].off;
        } else if (index >= (iovec->n - 1)) {
           finished = 1;
        }
     } while ((bleft > 0) && (finished==0));

     //** and send it
     bleft = bpos; 
     bpos = 0; btotal = 0;
     do {  //** Loop until data is completely sent or blocked
        nwrite = server_ns_write(task->ns, &(buffer[bpos]), bleft, dt);
        if (nwrite > 0) {
           btotal += nwrite;
           bpos += nwrite;
           bleft -= nwrite;
           task->stat.nbytes += nwrite;
        } else if (nwrite == 0) {
           shortwrite++;
        } else {
           shortwrite = 100;  //** closed connection
        }

        log_printf(15, "read_from_disk: id=" LU " -- bpos=" I64T " bleft=" I64T ", ntotal=" I64T ", nwrite=" I64T " * shortwrite=" I64T " ns=%d\n", 
             a->id, bpos, bleft, btotal, nwrite, shortwrite, tbx_ns_getid(task->ns));
     } while ((bleft > 0) && (shortwrite < 3));

     //** Update totals
     nleft -= btotal;
     *left -= btotal;
     a->r_pos += btotal;
     iovec->transfer_total += btotal;

     log_printf(15, "read_from_disk: nleft=" I64T " nwrite=" I64T " off=" I64T " shortwrite=" I64T "\n", nleft, nwrite, ioff, shortwrite);
  } while ((nleft > 0) && (shortwrite < 3));

  close_allocation(res, fd);

  if ((nwrite < 0) || (shortwrite >= 100)) {        //** Dead connection
     log_printf(10, "read_from_disk: Socket error with ns=%dfrom closing connection\n", tbx_ns_getid(ns));
     task_status = -1;
  } else {           //** short write
     if (*left == 0) {   //** Finished data transfer
        log_printf(10, "read_from_disk: Completed transfer! ns=%d tid=" LU "\n", tbx_ns_getid(task->ns), task->tid);
        task_status = 1;
     } else {
        log_printf(10, "read_from_disk: returning ns=%d back to caller.  short read.  tid=" LU "\n", tbx_ns_getid(task->ns), task->tid);
        task_status = 0;
     }
  }

  if (task_status != 0) {  //** Error on send so unwind the iovec buffer
                
  }

  return(task_status);
}

//************************************************************************************
//  write_to_disk_user - Writes data to the disk buffer and transfers it using
//     user space buffers.  Return values are
//    -1 -- Dead connection
//     0 -- Transfered as much as data as possible
//     1 -- Completed provided task
//     2 -- Buffer full so block
//************************************************************************************

int write_to_disk_user(ibp_task_t *task, Allocation_t *a, ibp_off_t *left, Resource_t *res)
{
  int bufsize = 2*1048576;
  ibp_off_t nbytes, ntotal, nread, nleft, err, cleft;
  Cmd_state_t *cmd = &(task->cmd);
  Cmd_write_t *w = &(cmd->cargs.write);
  ibp_iovec_t *iovec = &(w->iovec);
  char buffer[bufsize];
  tbx_ns_timeout_t dt;
  int task_status, shortread, index;
  osd_fd_t *fd;
  ibp_off_t bpos, ncurrread, ioff, ileft;
  log_printf(10, "write_to_disk_user: id=" LU " ns=%d\n", a->id, tbx_ns_getid(task->ns));

  if (*left == 0) return(1);   //** Nothing to do

  task_status = 0;
  tbx_ns_timeout_set(&dt, 1, 0);  //** set the max time we'll wait for data

  shortread = 0;

  nleft = *left;
  if (a->type == IBP_BYTEARRAY) {
     nleft = *left;   //** Already validated range in calling routine
  } else {
     nleft = (*left > (a->max_size - a->size)) ? (a->max_size - a->size) : *left;
  }

  ntotal = 0;
  debug_printf(10, "write_to_disk_user(BA): start.... id=" LU " * max_size=" I64T " * curr_size=" I64T " * max_transfer=" I64T " left=" I64T " ns=%d\n",
         a->id, a->max_size, a->size, nleft, *left, tbx_ns_getid(task->ns));

  if (nleft == 0) {  //** no space to store anything
     return(0);
  } 

  fd = open_allocation(res, a->id, OSD_WRITE_MODE);
  if (fd == NULL) {
     log_printf(0, "write_disk_user: Error with open_allocation(-res-, " LU ") = %d\n", a->id,  errno); 
     return(IBP_E_FILE_WRITE);
  }

  iovec_start(&(w->iovec), &index, &ioff, &ileft);

  do {
     bpos = 0;
     nbytes = (nleft < bufsize) ? nleft : bufsize;
     do {
        ncurrread = server_ns_read(task->ns, &(buffer[bpos]), nbytes, dt);
        if (ncurrread > 0) {
            nbytes -= ncurrread;
            bpos += ncurrread;
            task->stat.nbytes += ncurrread;
        } else if (ncurrread == 0) {
            shortread++;
        } else {
            shortread = 100;
        }
log_printf(10, "write_to_disk_user: id=" LU " ns=%d inner loop ncurrread= " I64T " bpos=" I64T " nbytes=" I64T " shortread=%d bufsize=" ST "\n",
    a->id, tbx_ns_getid(task->ns), ncurrread, bpos, nbytes, shortread, sizeof(buffer));
     } while ((nbytes > 0) && (shortread < 3));
     nread = bpos;

log_printf(10, "write_to_disk_user: id=" LU " ns=%d after loop ncurrread= " I64T " bpos=" I64T " shortread=%d\n", a->id, tbx_ns_getid(task->ns), ncurrread, bpos, shortread);

     if (nread > 0) {
        bpos = 0;
        do {
           cleft = (nread > ileft) ? ileft : nread;
           err = write_allocation(res, fd, ioff, cleft, &(buffer[bpos]));
           if (err != 0) {
              char tmp[128];
              log_printf(0, "write_to_disk_user: Error with write_allocation(%s, " LU ", " I64T ", " I64T ", buffer) = " I64T "  tid=" LU "\n",
                      ibp_rid2str(res->rid, tmp), a->id, ioff, cleft, err, task->tid); 
              shortread = 100;
              nread = err;
           }

           ileft -= cleft;
           ioff += cleft;
           ntotal += cleft;
           iovec->transfer_total += cleft;
           nleft -= cleft;
           bpos += cleft;
           nread -= cleft;

           if (a->type == IBP_BYTEARRAY) {  //** Update the size before moving on
             if (ioff > a->size) a->size = ioff;
           }

           if ((ileft <= 0) && (index < (iovec->n-1))) {
              index++;
              ileft = iovec->vec[index].len;
              ioff = iovec->vec[index].off;
           }
        } while (nread > 0);
      } else {
         shortread++;
      }

     log_printf(15, "write_to_disk_user: id=" LU " left=" I64T " -- pos=" I64T ", nleft=" I64T ", ntotal=" I64T ", nread=" I64T " ns=%d shortread=%d\n", 
              a->id, *left, ioff, nleft, ntotal, nread, tbx_ns_getid(task->ns), shortread);
  } while ((nleft > 0) && (shortread < 3));
//  } while ((ntotal < nleft) && (shortread < 3));

  *left = nleft;

  if (shortread >= 100) {        //** Dead connection
     log_printf(10, "write_to_disk_user: network error  ns=%d\n", tbx_ns_getid(task->ns));
     task_status = -1;
  } else {           //** short write
     task_status = 0;

     if (*left == 0) {   //** Finished data transfer
        log_printf(10, "write_to_disk_user: Completed transfer! ns=%d tid=" LU " a.size=" I64T "\n", tbx_ns_getid(task->ns), task->tid, a->size);
        task_status = 1;
     } else {
        log_printf(10, "write_to_disk_user: task_status=%d returning ns=%d back to caller.  a.size=" LU " short read.  tid=" LU "\n", task_status, tbx_ns_getid(task->ns), a->size, task->tid);
     }
  }

  close_allocation(res, fd);

  return(task_status);
}

//************************************************************************************
// ------------------------------ Wrapper routines------------------------------------
//************************************************************************************


//************************************************************************************
//  read_from_disk - Reads data from the disk buffer and transfers it.
//    Return values are
//    -1 -- Dead connection
//     0 -- Transfered as much as data as possible
//     1 -- Completed provided task
//************************************************************************************

int read_from_disk(ibp_task_t *task, Allocation_t *a, ibp_off_t *left, Resource_t *res)
{
  return(read_from_disk_user(task, a, left, res));
}

//************************************************************************************
//  write_to_disk - Writes data to the disk buffer and transfers it.
//    Return values are
//    -1 -- Dead connection
//     0 -- Transfered as much as data as possible
//     1 -- Completed provided task
//************************************************************************************

int write_to_disk(ibp_task_t *task, Allocation_t *a, ibp_off_t *left, Resource_t *res)
{
  return(write_to_disk_user(task, a, left, res));
}

//************************************************************************************
//  disk_to_disk_copy - Copies data between allocations.
//    Return values are
//    -1 -- Dead connection
//     0 -- Transfered as much as data as possible
//     1 -- Completed provided task
//************************************************************************************

int disk_to_disk_copy(Resource_t *src_res, osd_id_t src_id, ibp_off_t src_offset,
                      Resource_t *dest_res, osd_id_t dest_id, ibp_off_t dest_offset, ibp_off_t len, apr_time_t end_time)
{
  return(disk_to_disk_copy_user(src_res, src_id, src_offset, dest_res, dest_id, dest_offset, len, end_time));
}
