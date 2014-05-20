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

//***********************************************************************
// Routines for managing the segment loading framework
//***********************************************************************

#define _log_module_index 160

#include "ex3_abstract.h"
#include "ex3_system.h"
#include "list.h"
#include "random.h"
#include "type_malloc.h"
#include "log.h"

typedef struct {
  segment_t *src;
  segment_t *dest;
  data_attr_t *da;
  char *buffer;
  FILE *fd;
  ex_off_t src_offset;
  ex_off_t dest_offset;
  ex_off_t len;
  ex_off_t bufsize;
  int timeout;
  int truncate;
} segment_copy_t;

//***********************************************************************
// load_segment - Loads the given segment from the file/struct
//***********************************************************************

segment_t *load_segment(service_manager_t *ess, ex_id_t id, exnode_exchange_t *ex)
{
  char *type = NULL;
  char name[1024];
  segment_load_t *sload;

  if (ex->type == EX_TEXT) {
    snprintf(name, sizeof(name), "segment-" XIDT, id);
    inip_file_t *fd = ex->text.fd;
    type = inip_get_string(fd, name, "type", "");
  } else if (ex->type == EX_PROTOCOL_BUFFERS) {
    log_printf(0, "load_segment:  segment exnode parsing goes here\n");
  } else {
    log_printf(0, "load_segment:  Invalid exnode type type=%d for id=" XIDT "\n", ex->type, id);
    return(NULL);
  }

  sload = lookup_service(ess, SEG_SM_LOAD, type);
  if (sload == NULL) {
    log_printf(0, "load_segment:  No matching driver for type=%s  id=" XIDT "\n", type, id);
    return(NULL);
  }

  free(type);
  return((*sload)(ess, id, ex));
}

//***********************************************************************
// segment_copy_func - Does the actual segment copy operation
//***********************************************************************

op_status_t segment_copy_func(void *arg, int id)
{
  segment_copy_t *sc = (segment_copy_t *)arg;
  tbuffer_t *wbuf, *rbuf, *tmpbuf;
  tbuffer_t tbuf1, tbuf2;
  int err;
  ex_off_t bufsize;
  ex_off_t rpos, wpos, rlen, wlen, tlen, nbytes;
  ex_iovec_t rex, wex;
  opque_t *q;
  op_generic_t *rgop, *wgop;
  op_status_t status;

  //** Set up the buffers
  bufsize = sc->bufsize / 2;  //** The buffer is split for R/W
  tbuffer_single(&tbuf1, bufsize, sc->buffer);
  tbuffer_single(&tbuf2, bufsize, &(sc->buffer[bufsize]));
  rbuf = &tbuf1;  wbuf = &tbuf2;

  //** Check the length
  nbytes = segment_size(sc->src) - sc->src_offset;
  if (nbytes < 0) {
    rlen = bufsize;
  } else {
    rlen = (nbytes > bufsize) ? bufsize : nbytes;
  }
//  if ((sc->len != -1) && (sc->len < nbytes)) nbytes = sc->len;

  //** Read the initial block
  rpos = sc->src_offset;  wpos = sc->dest_offset;
//  rlen = (nbytes > bufsize) ? bufsize : nbytes;
  wlen = 0;
  ex_iovec_single(&rex, rpos, rlen);
  rpos += rlen;
  nbytes -= rlen;
  rgop = segment_read(sc->src, sc->da, 1, &rex, rbuf, 0, sc->timeout);
  err = gop_waitall(rgop);
  if (err != OP_STATE_SUCCESS) {
     log_printf(1, "Intial read failed! src=" XIDT " rpos=" XOT, " len=" XOT "\n", segment_id(sc->src), rpos, rlen);
     gop_free(rgop, OP_DESTROY);
     return(op_failure_status);
  }
  gop_free(rgop, OP_DESTROY);

  q = new_opque();
  do {
     //** Swap the buffers
     tmpbuf = rbuf;  rbuf = wbuf; wbuf = tmpbuf;
     tlen = rlen; rlen = wlen; wlen = tlen;

     log_printf(1, "sseg=" XIDT " dseg=" XIDT " wpos=%d rlen=%d wlen=%d\n", segment_id(sc->src), segment_id(sc->dest), wpos, rlen, wlen);

     //** Start the write
     ex_iovec_single(&wex, wpos, wlen);
     wpos += wlen;
     wgop = segment_write(sc->dest, sc->da, 1, &wex, wbuf, 0, sc->timeout);
     opque_add(q, wgop);

     //** Read in the next block
//     rlen = (nbytes > bufsize) ? bufsize : nbytes;
     if (nbytes < 0) {
        rlen = bufsize;
     } else {
        rlen = (nbytes > bufsize) ? bufsize : nbytes;
     }
     if (rlen > 0) {
        ex_iovec_single(&rex, rpos, rlen);
        rpos += rlen;
        nbytes -= rlen;
        rgop = segment_read(sc->src, sc->da, 1, &rex, rbuf, 0, sc->timeout);
        opque_add(q, rgop);
     }

     err = opque_waitall(q);
     if (err != OP_STATE_SUCCESS) {
        log_printf(1, "ERROR read/write failed! src=" XIDT " rpos=" XOT, " len=" XOT "\n", segment_id(sc->src), rpos, rlen);
        opque_free(q, OP_DESTROY);
        return(op_failure_status);
     }
  } while (rlen > 0);

  opque_free(q, OP_DESTROY);

  if (sc->truncate == 1) {  //** Truncate if wanted
     gop_sync_exec(segment_truncate(sc->dest, sc->da, wpos, sc->timeout));
  }

  status = op_success_status;  status.error_code = rpos;

  return(status);
}

//***********************************************************************
// segment_copy - Copies data between segments.  This copy is performed
//      by reading from the source and writing to the destination.
//      This is not a depot-depot copy.  The data goes through the client.
//
//      If len == -1 then all available data from src is copied
//***********************************************************************

op_generic_t *segment_copy(thread_pool_context_t *tpc, data_attr_t *da, segment_t *src_seg, segment_t *dest_seg, ex_off_t src_offset, ex_off_t dest_offset, ex_off_t len, ex_off_t bufsize, char *buffer, int do_truncate, int timeout)
{
  segment_copy_t *sc;

  type_malloc(sc, segment_copy_t, 1);

  sc->da = da;
  sc->timeout = timeout;
  sc->src = src_seg;
  sc->dest = dest_seg;
  sc->src_offset = src_offset;
  sc->dest_offset = dest_offset;
  sc->len = len;
  sc->bufsize = bufsize;
  sc->buffer = buffer;
  sc->truncate = do_truncate;

  return(new_thread_pool_op(tpc, NULL, segment_copy_func, (void *)sc, free, 1));
}


//***********************************************************************
// segment_get_func - Does the actual segment get operation
//***********************************************************************

op_status_t segment_get_func(void *arg, int id)
{
  segment_copy_t *sc = (segment_copy_t *)arg;
  tbuffer_t *wbuf, *rbuf, *tmpbuf;
  tbuffer_t tbuf1, tbuf2;
  char *rb, *wb, *tb;
  ex_off_t bufsize;
  int err;
  ex_off_t rpos, wpos, rlen, wlen, tlen, nbytes, got, total;
  ex_iovec_t rex;
  apr_time_t loop_start, file_start;
  double dt_loop, dt_file;
  op_generic_t *gop;
  op_status_t status;

  //** Set up the buffers
  bufsize = sc->bufsize / 2;  //** The buffer is split for R/W
  rb = sc->buffer;  wb = &(sc->buffer[bufsize]);
  tbuffer_single(&tbuf1, bufsize, rb);
  tbuffer_single(&tbuf2, bufsize, wb);
  rbuf = &tbuf1;  wbuf = &tbuf2;

  status = op_success_status;

  nbytes = sc->len;

  //** Read the initial block
  rpos = sc->src_offset; wpos = 0;
  nbytes = segment_size(sc->src) - sc->src_offset;
  if (nbytes < 0) {
    rlen = bufsize;
  } else {
    rlen = (nbytes > bufsize) ? bufsize : nbytes;
  }

log_printf(0, "FILE fd=%p\n", sc->fd);

  dt_loop = dt_file = 0;
  ex_iovec_single(&rex, rpos, rlen);
  wlen = 0;
  rpos += rlen;
  nbytes -= rlen;
  loop_start = apr_time_now();
  gop = segment_read(sc->src, sc->da, 1, &rex, rbuf, 0, sc->timeout);
  err = gop_waitall(gop);
  dt_loop = apr_time_now() - loop_start;  dt_loop /= (double)APR_USEC_PER_SEC;
  if (err != OP_STATE_SUCCESS) {
     log_printf(1, "Intial read failed! src=" XIDT " rpos=" XOT, " len=" XOT "\n", segment_id(sc->src), rpos, rlen);
     gop_free(gop, OP_DESTROY);
     return(op_failure_status);
  }
  gop_free(gop, OP_DESTROY);

total = 0;

  do {
     //** Swap the buffers
     tb = rb; rb = wb; wb = tb;
     tmpbuf = rbuf;  rbuf = wbuf; wbuf = tmpbuf;
     tlen = rlen; rlen = wlen; wlen = tlen;

     log_printf(1, "sseg=" XIDT " rpos=" XOT " wpos=" XOT " rlen=" XOT " wlen=" XOT " nbytes=" XOT "\n", segment_id(sc->src), rpos, wpos, rlen, wlen, nbytes);

     //** Read in the next block
     if (nbytes < 0) {
       rlen = bufsize;
     } else {
       rlen = (nbytes > bufsize) ? bufsize : nbytes;
     }
     if (rlen > 0) {
        ex_iovec_single(&rex, rpos, rlen);
        loop_start = apr_time_now();
        gop = segment_read(sc->src, sc->da, 1, &rex, rbuf, 0, sc->timeout);
        gop_start_execution(gop);  //** Start doing the transfer
        rpos += rlen;
        nbytes -= rlen;
     }

     //** Start the write
     file_start = apr_time_now();
     got = fwrite(wb, 1, wlen, sc->fd);
     dt_file = apr_time_now() - file_start;  dt_file /= (double)APR_USEC_PER_SEC;
total += got;
log_printf(0, "sid=" XIDT " fwrite(wb,1," XOT ", sc->fd)=" XOT " total=" XOT "\n", segment_id(sc->src), wlen, got, total);
     if (wlen != got) { 
        log_printf(1, "ERROR from fread=%d  dest sid=" XIDT "\n", errno, segment_id(sc->dest));
        status = op_failure_status; 
        err = gop_waitall(gop);
        gop_free(gop, OP_DESTROY);
        goto fail; 
     }

     wpos += wlen;

     //** Wait for the read to complete
     if (rlen > 0) {
        err = gop_waitall(gop);
        gop_free(gop, OP_DESTROY);
        if (err != OP_STATE_SUCCESS) {
           log_printf(1, "ERROR write(dseg=" XIDT ") failed! wpos=" XOT, " len=" XOT "\n", segment_id(sc->dest), wpos, wlen);
           status = op_failure_status;
           goto fail;
        }
     }

     dt_loop = apr_time_now() - loop_start;  dt_loop /= (double)APR_USEC_PER_SEC;
     log_printf(1, "dt_loop=%lf  dt_file=%lf\n", dt_loop, dt_file);

  } while (rlen > 0);

fail:

  return(status);
}



//***********************************************************************
// segment_get - Reads data from the given segment and copies it to the given FD
//      If len == -1 then all available data from src is copied
//***********************************************************************

op_generic_t *segment_get(thread_pool_context_t *tpc, data_attr_t *da, segment_t *src_seg, FILE *fd, ex_off_t src_offset, ex_off_t len, ex_off_t bufsize, char *buffer, int timeout)
{
  segment_copy_t *sc;

  type_malloc(sc, segment_copy_t, 1);

  sc->da = da;
  sc->timeout = timeout;
  sc->fd = fd;
  sc->src = src_seg;
  sc->src_offset = src_offset;
  sc->len = len;
  sc->bufsize = bufsize;
  sc->buffer = buffer;

  return(new_thread_pool_op(tpc, NULL, segment_get_func, (void *)sc, free, 1));
}

//***********************************************************************
// segment_put_func - Does the actual segment put operation
//***********************************************************************

op_status_t segment_put_func(void *arg, int id)
{
  segment_copy_t *sc = (segment_copy_t *)arg;
  tbuffer_t *wbuf, *rbuf, *tmpbuf;
  tbuffer_t tbuf1, tbuf2;
  char *rb, *wb, *tb;
  ex_off_t bufsize;
  int err;
  ex_off_t rpos, wpos, rlen, wlen, tlen, nbytes, got;
  ex_iovec_t wex;
  op_generic_t *gop;
  op_status_t status;
  apr_time_t loop_start, file_start;
  double dt_loop, dt_file;

  //** Set up the buffers
  bufsize = sc->bufsize / 2;  //** The buffer is split for R/W
  rb = sc->buffer;  wb = &(sc->buffer[bufsize]);
  tbuffer_single(&tbuf1, bufsize, rb);
  tbuffer_single(&tbuf2, bufsize, wb);
  rbuf = &tbuf1;  wbuf = &tbuf2;

  nbytes = sc->len;
  status = op_success_status;

  //** Read the initial block
  rpos = 0; wpos = sc->dest_offset;
  if (nbytes < 0) {
    rlen = bufsize;
  } else {
    rlen = (nbytes > bufsize) ? bufsize : nbytes;
  }

  wlen = 0;
  rpos += rlen;
  nbytes -= rlen;
log_printf(0, "FILE fd=%p bufsize=" XOT " rlen=" XOT " nbytes=" XOT "\n", sc->fd, bufsize, rlen, nbytes);

  loop_start = apr_time_now();
  got = fread(rb, 1, rlen, sc->fd);
  dt_file = apr_time_now() - loop_start;  dt_file /= (double)APR_USEC_PER_SEC;
  
  if (got == 0) { 
     if (feof(sc->fd) == 0)  {
        log_printf(1, "ERROR from fread=%d  dest sid=" XIDT " rlen=" XOT " got=" XOT "\n", errno, segment_id(sc->dest), rlen, got);
       status = op_failure_status; 
     }
     goto finished;
  }
  rlen = got;

  do {
     //** Swap the buffers
     tb = rb; rb = wb; wb = tb;
     tmpbuf = rbuf;  rbuf = wbuf; wbuf = tmpbuf;
     tlen = rlen; rlen = wlen; wlen = tlen;

     log_printf(1, "dseg=" XIDT " wpos=%d rlen=%d wlen=%d\n", segment_id(sc->dest), wpos, rlen, wlen);

     //** Start the write
     ex_iovec_single(&wex, wpos, wlen);
     wpos += wlen;
     loop_start = apr_time_now();
     gop = segment_write(sc->dest, sc->da, 1, &wex, wbuf, 0, sc->timeout);
     gop_start_execution(gop);  //** Start doing the transfer

     //** Read in the next block
     if (nbytes < 0) {
       rlen = bufsize;
     } else {
       rlen = (nbytes > bufsize) ? bufsize : nbytes;
     }
     if (rlen > 0) {
        file_start = apr_time_now();
        got = fread(rb, 1, rlen, sc->fd);
        dt_file = apr_time_now() - file_start;  dt_file /= (double)APR_USEC_PER_SEC;
        if (got == 0) {
           if (feof(sc->fd) == 0)  {
              log_printf(1, "ERROR from fread=%d  dest sid=" XIDT " got=" XOT " rlen=" XOT "\n", errno, segment_id(sc->dest), got, rlen);
              status = op_failure_status; 
              err = gop_waitall(gop);
              gop_free(gop, OP_DESTROY);
              goto finished; 
           }
        }
        rlen = got;
        rpos += rlen;
        nbytes -= rlen;
     }

     //** Wait for it to complete
     err = gop_waitall(gop);
     dt_loop = apr_time_now() - loop_start;  dt_loop /= (double)APR_USEC_PER_SEC;

     log_printf(1, "dt_loop=%lf  dt_file=%lf\n", dt_loop, dt_file);

     if (err != OP_STATE_SUCCESS) {
        log_printf(1, "ERROR write(dseg=" XIDT ") failed! wpos=" XOT, " len=" XOT "\n", segment_id(sc->dest), wpos, wlen);
        status = op_failure_status;
        gop_free(gop, OP_DESTROY);
        goto finished;
     }
     gop_free(gop, OP_DESTROY);
  } while (rlen > 0);

  if (sc->truncate == 1) {  //** Truncate if wanted
     gop_sync_exec(segment_truncate(sc->dest, sc->da, wpos, sc->timeout));
  }

finished:
//  status.error_code = rpos;

  return(status);
}



//***********************************************************************
// segment_put - Stores data from the given FD into the segment.
//      If len == -1 then all available data from src is copied
//***********************************************************************

op_generic_t *segment_put(thread_pool_context_t *tpc, data_attr_t *da, FILE *fd, segment_t *dest_seg, ex_off_t dest_offset, ex_off_t len, ex_off_t bufsize, char *buffer, int do_truncate, int timeout)
{
  segment_copy_t *sc;

  type_malloc(sc, segment_copy_t, 1);

  sc->da = da;
  sc->timeout = timeout;
  sc->fd = fd;
  sc->dest = dest_seg;
  sc->dest_offset = dest_offset;
  sc->len = len;
  sc->bufsize = bufsize;
  sc->buffer = buffer;
  sc->truncate = do_truncate;

  return(new_thread_pool_op(tpc, NULL, segment_put_func, (void *)sc, free, 1));
}

