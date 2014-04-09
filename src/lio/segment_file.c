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
// Routines for managing a linear segment
//***********************************************************************

#define _log_module_index 162

#include <libgen.h>
#include "ex3_abstract.h"
#include "ex3_system.h"
#include "interval_skiplist.h"
#include "ex3_compare.h"
#include "log.h"
#include "string_token.h"
#include "segment_linear.h"
#include "iniparse.h"
#include "random.h"
#include "append_printf.h"
#include "type_malloc.h"
#include "segment_file.h"
#include "atomic_counter.h"

typedef struct {
  char *fname;
  char *qname;
  thread_pool_context_t *tpc;
  atomic_int_t hard_errors;
  atomic_int_t soft_errors;
  atomic_int_t write_errors;
} segfile_priv_t;

typedef struct {
  segment_t *seg;
  tbuffer_t *buffer;
  ex_iovec_t *iov;
  ex_off_t  boff;
  ex_off_t len;
  int n_iov;
  int timeout;
  int mode;
} segfile_rw_op_t;

typedef struct {
  segment_t *seg;
  int new_size;
} segfile_multi_op_t;

typedef struct {
  segment_t *sseg;
  segment_t *dseg;
  int copy_data;
} segfile_clone_t;

//***********************************************************************
// segfile_rw_func - Read/Write from a file segment
//***********************************************************************

op_status_t segfile_rw_func(void *arg, int id)
{
  segfile_rw_op_t *srw = (segfile_rw_op_t *)arg;
  segfile_priv_t *s = (segfile_priv_t *)srw->seg->priv;
  ex_off_t bleft, boff;
  size_t nbytes, blen;
  tbuffer_var_t tbv;
  int i, err_cnt;
  op_status_t err;

double r;
//if (srw->mode == 1) {
  r = random_double(0, 1);
r = 1;
  if (r < 0.25) {
     log_printf(0, "Triggering a failure!  fname=%s n_iov=%d off[0]=" XOT " len[0]=" XOT " mode=%d\n", s->fname, srw->n_iov, srw->iov[0].offset, srw->iov[0].len, srw->mode); 
     return(op_failure_status);
  }
//}

  err = op_success_status;

  FILE *fd = fopen(s->fname, "r+");
  if (fd == NULL) fd = fopen(s->fname, "w+");

log_printf(15, "segfile_rw_func: tid=%d fname=%s n_iov=%d off[0]=" XOT " len[0]=" XOT " mode=%d\n", atomic_thread_id, s->fname, srw->n_iov, srw->iov[0].offset, srw->iov[0].len, srw->mode); 
flush_log();
  tbuffer_var_init(&tbv);

  blen = srw->len;
  boff = srw->boff;
  bleft = blen;
  err_cnt = 0;
  for (i=0; i<srw->n_iov; i++) {
     fseeko(fd, srw->iov[i].offset, SEEK_SET);
     bleft = srw->iov[i].len;
     err = op_success_status;
     while ((bleft > 0) && (err.op_status == OP_STATE_SUCCESS)) {
       tbv.nbytes = bleft;
       tbuffer_next(srw->buffer, boff, &tbv);
       blen = tbv.nbytes;
       if (srw->mode == 0) {
          nbytes = readv(fileno(fd), tbv.buffer, tbv.n_iov);
       } else {
          nbytes = writev(fileno(fd), tbv.buffer, tbv.n_iov);
       }

int ib = blen;
int inb = nbytes;
log_printf(15, "segfile_rw_func: tid=%d fname=%s n_iov=%d off[0]=" XOT " len[0]=" XOT " blen=%d nbytes=%d err_cnt=%d\n", atomic_thread_id, s->fname, srw->n_iov, srw->iov[0].offset, srw->iov[0].len, ib, inb, err_cnt);
flush_log();

       if (nbytes > 0) {
          boff = boff + nbytes;
          bleft = bleft - nbytes;
       } else {
         err = op_failure_status;
         err_cnt++;
       }
     }
  }

  err =  (err_cnt > 0) ? op_failure_status : op_success_status;

  if (err_cnt > 0) {  //** Update the error count if needed
log_printf(15, "segfile_rw_func: ERROR tid=%d fname=%s n_iov=%d off[0]=" XOT " len[0]=" XOT " bleft=" XOT " err_cnt=%d\n", atomic_thread_id, s->fname, srw->n_iov, srw->iov[0].offset, srw->iov[0].len, bleft, err_cnt);
     atomic_inc(s->hard_errors);
     if (srw->mode != 0) atomic_inc(s->write_errors);
  }

log_printf(15, "segfile_rw_func: tid=%d fname=%s n_iov=%d off[0]=" XOT " len[0]=" XOT " bleft=" XOT " err_cnt=%d\n", atomic_thread_id, s->fname, srw->n_iov, srw->iov[0].offset, srw->iov[0].len, bleft, err_cnt);
flush_log();
//log_printf(15, "segfile_rw_func: buf=%20s\n", (char *)srw->buffer->buf.iov[0].iov_base);
  fclose(fd);
  return(err);
}

//***********************************************************************
// segfile_read - Read from a file segment
//***********************************************************************

op_generic_t *segfile_read(segment_t *seg, data_attr_t *da, int n_iov, ex_iovec_t *iov, tbuffer_t *buffer, ex_off_t boff, int timeout)
{
  segfile_priv_t *s = (segfile_priv_t *)seg->priv;
  segfile_rw_op_t *srw;

  type_malloc_clear(srw, segfile_rw_op_t, 1);

  srw->seg = seg;
  srw->n_iov = n_iov;
  srw->iov = iov;
  srw->boff = boff;
  srw->timeout = timeout;
  srw->buffer = buffer;
  srw->mode = 0;

  return(new_thread_pool_op(s->tpc, s->qname, segfile_rw_func, (void *)srw, free, 1));
}

//***********************************************************************
// segfile_write - Writes to a linear segment
//***********************************************************************

op_generic_t *segfile_write(segment_t *seg, data_attr_t *da, int n_iov, ex_iovec_t *iov, tbuffer_t *buffer, ex_off_t boff, int timeout)
{
  segfile_priv_t *s = (segfile_priv_t *)seg->priv;
  segfile_rw_op_t *srw;

  type_malloc_clear(srw, segfile_rw_op_t, 1);

  srw->seg = seg;
  srw->n_iov = n_iov;
  srw->iov = iov;
  srw->boff = boff;
  srw->timeout = timeout;
  srw->buffer = buffer;
  srw->mode = 1;

  return(new_thread_pool_op(s->tpc, s->qname, segfile_rw_func, (void *)srw, free, 1));
}

//***********************************************************************
// segfile_multi_func - PErforms the truncate and remove ops for a file segment
//***********************************************************************

op_status_t segfile_multi_func(void *arg, int id)
{
  segfile_multi_op_t *cmd = (segfile_multi_op_t *)arg;
  segfile_priv_t *s = (segfile_priv_t *)cmd->seg->priv;
  int err;
  op_status_t status = op_success_status;

  if (cmd->new_size >= 0) {  //** Truncate operation
     err = truncate(s->fname, cmd->new_size);
     if (err != 0) status = op_failure_status;
  } else {  //** REmove op
    if (s->fname != NULL) {
       remove(s->fname);
    }
  }

  return(status);
}

//***********************************************************************
// segfile_remove - DECrements the ref counts for the segment which could
//     result in the data being removed.
//***********************************************************************

op_generic_t *segfile_remove(segment_t *seg, data_attr_t *da, int timeout)
{
  segfile_priv_t *s = (segfile_priv_t *)seg->priv;
  segfile_multi_op_t *cmd;

  type_malloc_clear(cmd, segfile_multi_op_t, 1);

  cmd->seg = seg;
  cmd->new_size = -1;

  return(new_thread_pool_op(s->tpc, s->qname, segfile_multi_func, (void *)cmd, free, 1));
}

//***********************************************************************
// segfile_truncate - Expands or contracts a segment
//***********************************************************************

op_generic_t *segfile_truncate(segment_t *seg, data_attr_t *da, ex_off_t new_size, int timeout)
{
  segfile_priv_t *s = (segfile_priv_t *)seg->priv;
  segfile_multi_op_t *cmd;

  type_malloc_clear(cmd, segfile_multi_op_t, 1);

  cmd->seg = seg;
  cmd->new_size = new_size;

  return(new_thread_pool_op(s->tpc, s->qname, segfile_multi_func, (void *)cmd, free, 1));
}


//***********************************************************************
// segfile_inspect - Checks if the file exists
//***********************************************************************

op_generic_t *segfile_inspect(segment_t *seg, data_attr_t *da, info_fd_t *ifd, int mode, ex_off_t bufsize, rs_query_t *query, int timeout)
{
  segfile_priv_t *s = (segfile_priv_t *)seg->priv;
  FILE *fd;
  op_status_t err;

  err= op_failure_status;
  switch (mode) {
    case (INSPECT_QUICK_CHECK):
    case (INSPECT_SCAN_CHECK):
    case (INSPECT_FULL_CHECK):
        fd = fopen(s->fname, "r");
        if (fd == NULL) {
           err = op_failure_status;
        } else {
           err = op_success_status;
           fclose(fd);
        }
        break;
    case (INSPECT_QUICK_REPAIR):
    case (INSPECT_SCAN_REPAIR):
    case (INSPECT_FULL_REPAIR):
        fd = fopen(s->fname, "w+");
        if (fd == NULL) {
           err = op_failure_status;
        } else {
           err = op_success_status;
           fclose(fd);
        }
        break;
    case (INSPECT_SOFT_ERRORS):
        err.error_code = atomic_get(s->soft_errors);
        err.op_status = (err.error_code == 0) ? OP_STATE_SUCCESS : OP_STATE_FAILURE;
        break;
    case (INSPECT_HARD_ERRORS):
        err.error_code = atomic_get(s->hard_errors);
        err.op_status = (err.error_code == 0) ? OP_STATE_SUCCESS : OP_STATE_FAILURE;
        break;
    case (INSPECT_WRITE_ERRORS):
        err.error_code = atomic_get(s->write_errors);
        err.op_status = (err.error_code == 0) ? OP_STATE_SUCCESS : OP_STATE_FAILURE;
        break;
  }

  return(gop_dummy(err));
}

//***********************************************************************
// segfile_flush - Flushes a segment
//***********************************************************************

op_generic_t *segfile_flush(segment_t *seg, data_attr_t *da, ex_off_t lo, ex_off_t hi, int timeout)
{
  return(gop_dummy(op_success_status));
}

//***********************************************************************
// segfile_signature - Generates the segment signature
//***********************************************************************

int segfile_signature(segment_t *seg, char *buffer, int *used, int bufsize)
{
  append_printf(buffer, used, bufsize, "file()\n");

  return(0);
}


//***********************************************************************
// segfile_clone_func - Clone data from a file segment
//***********************************************************************

op_status_t segfile_clone_func(void *arg, int id)
{
  segfile_clone_t *sfc = (segfile_clone_t *)arg;
  segfile_priv_t *ss = (segfile_priv_t *)sfc->sseg->priv;
  segfile_priv_t *sd = (segfile_priv_t *)sfc->dseg->priv;
  int bufsize = 10*1024*1024;
  char *buffer;
  int n, m;
  FILE *sfd, *dfd;

  dfd = fopen(sd->fname, "w+");
  if (dfd == NULL) return(op_failure_status);  //** Failed making the dest file

  //** If no data then return
  if (sfc->copy_data == 0) { fclose(dfd); return(op_success_status); }


  sfd = fopen(ss->fname, "r");
  if (sfd == NULL) { fclose(dfd); return(op_success_status); } //** Nothing to copy

  type_malloc(buffer, char, bufsize);
  while ((n = fread(buffer, 1, bufsize, sfd)) > 0) {
     m = fwrite(buffer, 1, n, dfd);
//log_printf(0, "r=%d w=%d bufsize=%d\n", n, m, bufsize);
     if (m != n) { fclose(sfd); fclose(dfd); return(op_failure_status); }
  }

  fclose(sfd);
  fclose(dfd);

  free(buffer);

  return(op_success_status);
}

//***********************************************************************
// segfile_clone - Clones a segment
//***********************************************************************

op_generic_t *segfile_clone(segment_t *seg, data_attr_t *da, segment_t **clone_seg, int mode, void *attr, int timeout)
{
  segment_t *clone;
  segfile_priv_t *ss, *sd;
  char *root, *fname;
  char fid[4096];
  op_generic_t *gop;
  segfile_clone_t *sfc;
  int use_existing = (*clone_seg != NULL) ? 1 : 0;

  //** Make the base segment
  if (use_existing == 0) *clone_seg = segment_file_create(seg->ess);
  clone = *clone_seg;

  ss = (segfile_priv_t *)seg->priv;
  sd = (segfile_priv_t *)clone->priv;

  //** Copy the header
  if ((seg->header.name != NULL) && (use_existing == 0)) clone->header.name = strdup(seg->header.name);

  if (use_existing == 0) {
     if (attr == NULL) {    //** make a new file using the segment id as the name if none specified
        fname = strdup(ss->fname);
        root = dirname(fname);
        sprintf(fid, "%s/" XIDT ".dat", root, segment_id(clone));
        sd->fname = strdup(fid);
        free(fname);
     } else {  //** User specified the path so use it
        sd->fname = strdup((char *)attr);
     }
  }

  type_malloc(sfc, segfile_clone_t, 1);
  sfc->sseg = seg;
  sfc->dseg = clone;
  sfc->copy_data = 0;

  if (mode == CLONE_STRUCT_AND_DATA) sfc->copy_data = 1;

  gop = new_thread_pool_op(sd->tpc, sd->qname, segfile_clone_func, (void *)sfc, free, 1);

  return(gop);
}


//***********************************************************************
// segfile_size - Returns the segment size.
//***********************************************************************

ex_off_t segfile_size(segment_t *seg)
{
  segfile_priv_t *s = (segfile_priv_t *)seg->priv;
  ex_off_t nbytes;
  FILE *fd = fopen(s->fname, "r+");

  if (fd == NULL) return(-1);
  fseeko(fd, 0, SEEK_END);
  nbytes = ftell(fd);

  fclose(fd);
  return(nbytes);
}

//***********************************************************************
// segfile_block_size - Returns the segment block size.
//***********************************************************************

ex_off_t segfile_block_size(segment_t *seg)
{
//  segfile_priv_t *s = (segfile_priv_t *)seg->priv;

  return(1);
}


//***********************************************************************
// segfile_serialize_text -Convert the segment to a text based format
//***********************************************************************

int segfile_serialize_text(segment_t *seg, exnode_exchange_t *exp)
{
  segfile_priv_t *s = (segfile_priv_t *)seg->priv;
  int bufsize=10*1024;
  char segbuf[bufsize];
  char *etext;
  int sused;

  segbuf[0] = 0;

  sused = 0;

  //** Store the segment header
  append_printf(segbuf, &sused, bufsize, "[segment-" XIDT "]\n", seg->header.id);
  if ((seg->header.name != NULL) && (strcmp(seg->header.name, "") != 0)) {
     etext = escape_text("=", '\\', seg->header.name);
     append_printf(segbuf, &sused, bufsize, "name=%s\n", etext);  free(etext);
  }
  append_printf(segbuf, &sused, bufsize, "type=%s\n", seg->header.type);
  append_printf(segbuf, &sused, bufsize, "ref_count=%d\n", seg->ref_count);
  append_printf(segbuf, &sused, bufsize, "file=%s\n\n", s->fname);

  exnode_exchange_append_text(exp, segbuf);

  return(0);
}

//***********************************************************************
// segfile_serialize_proto -Convert the segment to a protocol buffer
//***********************************************************************

int segfile_serialize_proto(segment_t *seg, exnode_exchange_t *exp)
{
//  segfile_priv_t *s = (segfile_priv_t *)seg->priv;

  return(-1);
}

//***********************************************************************
// segfile_serialize -Convert the segment to a more portable format
//***********************************************************************

int segfile_serialize(segment_t *seg, exnode_exchange_t *exp)
{
  if (exp->type == EX_TEXT) {
     return(segfile_serialize_text(seg, exp));
  } else if (exp->type == EX_PROTOCOL_BUFFERS) {
     return(segfile_serialize_proto(seg, exp));
  }
 
  return(-1);
}

//***********************************************************************
// segfile_deserialize_text -Read the text based segment
//***********************************************************************

int segfile_deserialize_text(segment_t *seg, ex_id_t id, exnode_exchange_t *exp)
{
  segfile_priv_t *s = (segfile_priv_t *)seg->priv;
  int bufsize=1024;
  char seggrp[bufsize];
  char qname[512];
  int err;
  inip_file_t *fd;

  err = 0;

  //** Parse the ini text
  fd = exp->text.fd;

  //** Make the segment section name
  snprintf(seggrp, bufsize, "segment-" XIDT, id);

  //** Get the segment header info
  seg->header.id = id;
  if (s->qname != NULL) free(s->qname);
  snprintf(qname, sizeof(qname), XIDT HP_HOSTPORT_SEPARATOR "1" HP_HOSTPORT_SEPARATOR "0" HP_HOSTPORT_SEPARATOR "0", seg->header.id);
  s->qname = strdup(qname);

  seg->header.type = SEGMENT_TYPE_FILE;
  seg->header.name = inip_get_string(fd, seggrp, "name", "");

  //** and the local file name
  s->fname = inip_get_string(fd, seggrp, "file", "");

  if (strcmp(s->fname, "") == 0) {
     s->fname = NULL;
     log_printf(5, "segfile_deserialize_text: Error opening file %s for segment " XIDT "\n", s->fname, id);
     err = 1;
  }

  return(err);
}

//***********************************************************************
// segfile_deserialize_proto - Read the prot formatted segment
//***********************************************************************

int segfile_deserialize_proto(segment_t *seg, ex_id_t id, exnode_exchange_t *exp)
{
  return(-1);
}

//***********************************************************************
// segfile_deserialize -Convert from the portable to internal format
//***********************************************************************

int segfile_deserialize(segment_t *seg, ex_id_t id, exnode_exchange_t *exp)
{
  if (exp->type == EX_TEXT) {
     return(segfile_deserialize_text(seg, id, exp));
  } else if (exp->type == EX_PROTOCOL_BUFFERS) {
     return(segfile_deserialize_proto(seg, id, exp));
  }

  return(-1);
}


//***********************************************************************
// segfile_destroy - Destroys a linear segment struct (not the data)
//***********************************************************************

void segfile_destroy(segment_t *seg)
{
  segfile_priv_t *s = (segfile_priv_t *)seg->priv;

  //** Check if it's still in use
log_printf(15, "segfile_destroy: seg->id=" XIDT " ref_count=%d\n", segment_id(seg), seg->ref_count);

  if (seg->ref_count > 0) return;

  if (s->fname != NULL) free(s->fname);
  if (s->qname != NULL) free(s->qname);

  free(s);

  ex_header_release(&(seg->header));

  free(seg);
}


//***********************************************************************
// segment_file_make - Creates a File segment
//***********************************************************************

op_generic_t *segment_file_make(segment_t *seg, data_attr_t *da, char *fname)
{
  segfile_priv_t *s = (segfile_priv_t *)seg->priv;
  FILE *fd;

  s->fname = strdup(fname);
  fd = fopen(fname, "r+");

  if (fd ==  NULL) {
    return(gop_dummy(op_failure_status));  //** Return an error
  }

  fclose(fd);
  return(gop_dummy(op_success_status));
}

//***********************************************************************
// segment_file_create - Creates a file segment
//***********************************************************************

segment_t *segment_file_create(void *arg)
{
  service_manager_t *es = (service_manager_t *)arg;
  segfile_priv_t *s;
  segment_t *seg;
  char qname[512];

  //** Make the space
  type_malloc_clear(seg, segment_t, 1);
  type_malloc_clear(s, segfile_priv_t, 1);

  s->fname = NULL;

  generate_ex_id(&(seg->header.id));
  atomic_set(seg->ref_count, 0);
  seg->header.type = SEGMENT_TYPE_FILE;

  s->tpc = lookup_service(es, ESS_RUNNING, ESS_TPC_UNLIMITED);
  snprintf(qname, sizeof(qname), XIDT HP_HOSTPORT_SEPARATOR "1" HP_HOSTPORT_SEPARATOR "0" HP_HOSTPORT_SEPARATOR "0", seg->header.id);
  s->qname = strdup(qname);

  seg->priv = s;
  seg->ess = es;
  seg->fn.read = segfile_read;
  seg->fn.write = segfile_write;
  seg->fn.inspect = segfile_inspect;
  seg->fn.truncate = segfile_truncate;
  seg->fn.remove = segfile_remove;
  seg->fn.flush = segfile_flush;
  seg->fn.clone = segfile_clone;
  seg->fn.signature = segfile_signature;
  seg->fn.size = segfile_size;
  seg->fn.block_size = segfile_block_size;
  seg->fn.serialize = segfile_serialize;
  seg->fn.deserialize = segfile_deserialize;
  seg->fn.destroy = segfile_destroy;

  return(seg);
}

//***********************************************************************
// segment_file_load - Loads a file segment from ini/ex3
//***********************************************************************

segment_t *segment_file_load(void *arg, ex_id_t id, exnode_exchange_t *ex)
{
  segment_t *seg = segment_file_create(arg);
  if (segment_deserialize(seg, id, ex) != 0) {
     segment_destroy(seg);
     seg = NULL;
  }
  return(seg);
}
