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
  ex_off_t src_offset;
  ex_off_t dest_offset;
  ex_off_t len;
  ex_off_t bufsize;
  int timeout;
} segment_copy_t;

typedef struct {
  segment_t *(*driver)(void *arg, ex_id_t id, exnode_exchange_t *ex);
  segment_t *(*create)(void *arg);
  void *arg;
} segment_driver_t;

typedef struct {
  list_t *table;
} segment_table_t;

segment_table_t *segment_driver_table = NULL;

//***********************************************************************
// install_segment- Installs a segment driver into the table
//***********************************************************************

int install_segment(char *type, segment_t *(*driver)(void *arg, ex_id_t id, exnode_exchange_t *ex), segment_t *(*create)(void *arg), void *arg)
{
  segment_driver_t *d;

  //** 1st time so create the struct
  if (segment_driver_table == NULL) {
     type_malloc_clear(segment_driver_table, segment_table_t, 1);
     segment_driver_table->table = list_create(0, &list_string_compare, list_string_dup, list_simple_free, list_no_data_free);
  }

  d = list_search(segment_driver_table->table, type);
  if (d != NULL) {
    log_printf(0, "install_segment: Matching driver for type=%s already exists!\n", type);
    return(1);
  }

  type_malloc_clear(d, segment_driver_t, 1);
  d->driver = driver;
  d->create = create;
  d->arg = arg;
  list_insert(segment_driver_table->table, type, (void *)d);

  return(0);
}

//***********************************************************************
// load_segment - Loads the given segment from the file/struct
//***********************************************************************

segment_t *load_segment(ex_id_t id, exnode_exchange_t *ex)
{
  char *type = NULL;
  char name[1024];
  segment_driver_t *d;

  if (ex->type == EX_TEXT) {
    snprintf(name, sizeof(name), "segment-" XIDT, id);
    inip_file_t *fd = inip_read_text(ex->text);
    type = inip_get_string(fd, name, "type", "");
    inip_destroy(fd);
  } else if (ex->type == EX_PROTOCOL_BUFFERS) {
    log_printf(0, "load_segment:  segment exnode parsing goes here\n");
  } else {
    log_printf(0, "load_segment:  Invalid exnode type type=%d for id=" XIDT "\n", ex->type, id);
    return(NULL);
  }

  d = list_search(segment_driver_table->table, type);
  if (d == NULL) {
    log_printf(0, "load_segment:  No matching driver for type=%s  id=" XIDT "\n", type, id);
    return(NULL);
  }

  free(type);
  return(d->driver(d->arg, id, ex));
}

//***********************************************************************
// create_segment - Creates a segment of the given type
//***********************************************************************

segment_t *create_segment(char *type)
{
  segment_driver_t *d;

  d = list_search(segment_driver_table->table, type);
  if (d == NULL) {
    log_printf(0, "load_segment:  No matching driver for type=%s\n", type);
    return(NULL);
  }

  return(d->create(d->arg));
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

  //** Set up the buffers
  bufsize = sc->bufsize / 2;  //** The buffer is split for R/W
  tbuffer_single(&tbuf1, bufsize, sc->buffer);
  tbuffer_single(&tbuf2, bufsize, &(sc->buffer[bufsize]));
  rbuf = &tbuf1;  wbuf = &tbuf2;

  //** Check the length
  nbytes = segment_size(sc->src) - sc->src_offset;
  if ((sc->len != -1) && (sc->len < nbytes)) nbytes = sc->len;

  //** Read the initial block
  rpos = sc->src_offset;  wpos = sc->dest_offset;
  rlen = (nbytes > bufsize) ? bufsize : nbytes;
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
     rlen = (nbytes > bufsize) ? bufsize : nbytes;
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

  return(op_success_status);
}

//***********************************************************************
// segment_copy - Copies data between segments.  This copy is performed
//      by reading from the source and writing to the destination.
//      This is not a depot-depot copy.  The data goes through the client.
//
//      If len == -1 then all available data from src is copied
//***********************************************************************

op_generic_t *segment_copy(data_attr_t *da, segment_t *src_seg, segment_t *dest_seg, ex_off_t src_offset, ex_off_t dest_offset, ex_off_t len, ex_off_t bufsize, char *buffer, int timeout)
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

  return(new_thread_pool_op(exnode_service_set->tpc_unlimited, NULL, segment_copy_func, (void *)sc, free, 1));
}

