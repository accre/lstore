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
// Remote OS implementation for the client side
//***********************************************************************

#define _log_module_index 213

#include "ex3_system.h"
#include "object_service_abstract.h"
#include "type_malloc.h"
#include "log.h"
#include "atomic_counter.h"
#include "thread_pool.h"
#include "os_remote.h"
#include "os_remote_priv.h"
#include "append_printf.h"
#include "random.h"
#include "mq_helpers.h"
#include "mq_stream.h"
#include "varint.h"
#include "authn_fake.h"

//#define OSRS_HANDLE(ofd) ((osrs_ongoing_object_t *)((ofd)->data))->handle
#define OSRS_HANDLE(ofd) (void *)(*(intptr_t *)(ofd)->data)

#define OSRC_ITER_ALIST  0
#define OSRC_ITER_AREGEX 1

typedef struct {
  object_service_fn_t *os;
  void *data;
  int size;
} osrc_object_fd_t;

typedef struct {
   uint64_t id;
   object_service_fn_t *os;
} osrc_arg_t;

typedef struct {
  object_service_fn_t *os;
//  void *it;
  mq_stream_t *mqs;
  mq_msg_t *response;
  int v_max;
  int is_sub_iter;
  int no_more_attr;
} osrc_attr_iter_t;

typedef struct {
  object_service_fn_t *os;
  mq_stream_t *mqs;
  mq_msg_t *response;
  int mode;
  int finished;
} osrc_fsck_iter_t;

typedef struct {
  object_service_fn_t *os;
//  void *it;
  os_attr_iter_t **ait;
  void **val;
  int *v_size;
  int *v_size_initial;
  int n_keys;
  int v_max;
  int iter_type;
  mq_stream_t *mqs;
  mq_msg_t *response;
} osrc_object_iter_t;

typedef struct {
  char handle[1024];
  osrc_object_fd_t **pfd;
  object_service_fn_t *os;
} osrc_open_t;

typedef struct {
  object_service_fn_t *os;
  os_fd_t *fd;
  os_fd_t *fd_dest;
  char **src_path;
  char **key;
  char **key_dest;
  void **val;
  char *key_tmp;
  char *src_tmp;
  void *val_tmp;
  int *v_size;
  int v_tmp;
  int n;
} osrc_mult_attr_t;

typedef struct {
  object_service_fn_t *os;
  creds_t *creds;
  os_regex_table_t *path;
  os_regex_table_t *object_regex;
  int obj_types;
  int recurse_depth;
  uint64_t my_id;
} osrc_remove_regex_t;

typedef struct {
  object_service_fn_t *os;
  creds_t *creds;
  char *id;
  os_regex_table_t *path;
  os_regex_table_t *object_regex;
  int obj_types;
  int recurse_depth;
  char **key;
  void **val;
  int *v_size;
  int n_attrs;
  uint64_t my_id;
} osrc_set_regex_t;

//***********************************************************************
// osrc_add_creds - Adds the creds to the message
//***********************************************************************

int osrc_add_creds(object_service_fn_t *os, creds_t *creds, mq_msg_t *msg)
{
  void *chandle;
  int len;

  chandle = an_cred_get_type_field(creds, AUTHN_INDEX_SHARED_HANDLE, &len);
  mq_msg_append_mem(msg, chandle, len, MQF_MSG_KEEP_DATA);

  return(0);
}

//***********************************************************************
// osrc_response_status - Handles a response that just returns the status
//***********************************************************************

op_status_t osrc_response_status(void *task_arg, int tid)
{
  mq_task_t *task = (mq_task_t *)task_arg;
  op_status_t status;

log_printf(5, "START\n");

  //** Parse the response
  mq_remove_header(task->response, 1);

  status = mq_read_status_frame(mq_msg_first(task->response), 0);
log_printf(5, "END status=%d %d\n", status.op_status, status.error_code);

  return(status);
}

//***********************************************************************
// osrc_response_stream_status - Handles getting a stream status response
//***********************************************************************

op_status_t osrc_response_stream_status(void *task_arg, int tid)
{
  mq_task_t *task = (mq_task_t *)task_arg;
  object_service_fn_t *os = (object_service_fn_t *)task->arg;
  osrc_priv_t *osrc = (osrc_priv_t *)os->priv;
  mq_stream_t *mqs;
  op_status_t status;
  int err;

log_printf(5, "START\n");

  status = op_success_status;

  //** Parse the response
  mq_remove_header(task->response, 1);

  mqs = mq_stream_read_create(osrc->mqc, osrc->ongoing, osrc->host_id, osrc->host_id_len, mq_msg_first(task->response), osrc->remote_host, osrc->stream_timeout);

  //** Parse the status
  status.op_status = mq_stream_read_varint(mqs, &err);
log_printf(15, "op_status=%d\n", status.op_status);
  status.error_code = mq_stream_read_varint(mqs, &err);
log_printf(15, "error_code%d\n", status.error_code);

  mq_stream_destroy(mqs);

  if (err != 0) status = op_failure_status;
log_printf(5, "END err=%d status=%d %d\n", err, status.op_status, status.error_code);

  return(status);
}

//***********************************************************************
// osrc_remove_regex_object - Does a bulk regex remove.
//     Each matching object is removed.  If the object is a directory
//     then the system will recursively remove it's contents up to the
//     recursion depth.  Setting recurse_depth=0 will only remove the dir
//     if it is empty.
//***********************************************************************

op_status_t osrc_remove_regex_object_func(void *arg, int id)
{
  osrc_remove_regex_t *op = (osrc_remove_regex_t *)arg;
  osrc_priv_t *osrc = (osrc_priv_t *)op->os->priv;
  int bpos, bufsize, again, n;
  unsigned char *buffer;
  mq_msg_t *msg, *spin;
  op_generic_t *gop, *g;
  op_status_t status;

log_printf(5, "START\n");

  status = op_success_status;

  //** Form the message
  msg = mq_make_exec_core_msg(osrc->remote_host, 1);
  mq_msg_append_mem(msg, OSR_REMOVE_REGEX_OBJECT_KEY, OSR_REMOVE_REGEX_OBJECT_SIZE, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
  osrc_add_creds(op->os, op->creds, msg);

  bufsize = 4096;
  type_malloc(buffer, unsigned char, bufsize);
  do {
    again = 0;
    bpos = 0;

    n = zigzag_encode(osrc->timeout, buffer);
    if (n<0) { again = 1; n=4; }
    bpos += n;

    n = zigzag_encode(sizeof(op->my_id), &(buffer[bpos]));
    if (n<0) { again = 1; n=4; }
    bpos += n;
    memcpy(&(buffer[bpos]), &(op->my_id), sizeof(op->my_id));
    bpos += sizeof(op->my_id);

    n = zigzag_encode(osrc->spin_fail, &(buffer[bpos]));
    if (n<0) { again = 1; n=4; }
    bpos += n;

    n = zigzag_encode(op->recurse_depth, &(buffer[bpos]));
    if (n<0) { again = 1; n=4; }
    bpos += n;

    n = zigzag_encode(op->obj_types, &(buffer[bpos]));
    if (n<0) { again = 1; n=4; }
    bpos += n;

    n = os_regex_table_pack(op->path, &(buffer[(again==0) ? bpos : 0]), bufsize-bpos);
    if (n < 0) { again = 1; n = -n; }
    bpos += n;

    n = os_regex_table_pack(op->object_regex, &(buffer[(again==0) ? bpos : 0]), bufsize-bpos);
    if (n < 0) { again = 1; n = -n; }
    bpos += n;

    if (again == 1) {
       bufsize = bpos + 10;
       free(buffer);
       type_malloc(buffer, unsigned char, bufsize);
    }
  } while (again == 1);


  mq_msg_append_mem(msg, buffer, bpos, MQF_MSG_AUTO_FREE);
  mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

log_printf(5, "END\n");

  //** Make the gop and submit it
  gop = new_mq_op(osrc->mqc, msg, osrc_response_stream_status, op->os, NULL, osrc->timeout);
  gop_start_execution(gop);

  //** Wait for it to complete Sending hearbeats as needed
  while ((g = gop_timed_waitany(gop, osrc->spin_interval)) == NULL) {
     //** Form the spin message
     spin = mq_make_exec_core_msg(osrc->remote_host, 0);
     mq_msg_append_mem(spin, OSR_SPIN_HB_KEY, OSR_SPIN_HB_SIZE, MQF_MSG_KEEP_DATA);
     mq_msg_append_mem(spin, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
     osrc_add_creds(op->os, op->creds, spin);
     mq_msg_append_mem(spin, &(op->my_id), sizeof(op->my_id), MQF_MSG_KEEP_DATA);

     //** And send it
     g = new_mq_op(osrc->mqc, spin, NULL, NULL, NULL, osrc->timeout);
     log_printf(5, "spin hb sent. gid=%d\n", gop_id(g));
     gop_set_auto_destroy(g, 1);
     gop_start_execution(g);
  }

  gop_waitall(gop);
  status = gop_get_status(gop);
  gop_free(gop, OP_DESTROY);

log_printf(5, "END status=%d\n", status.op_status);

  return(status);
}


//***********************************************************************
// osrc_remove_regex_object - Does a bulk regex remove.
//     Each matching object is removed.  If the object is a directory
//     then the system will recursively remove it's contents up to the
//     recursion depth.  Setting recurse_depth=0 will only remove the dir
//     if it is empty.
//***********************************************************************

op_generic_t *osrc_remove_regex_object(object_service_fn_t *os, creds_t *creds, os_regex_table_t *path, os_regex_table_t *object_regex, int obj_types, int recurse_depth)
{
  osrc_priv_t *osrc = (osrc_priv_t *)os->priv;
  osrc_remove_regex_t *op;
  op_generic_t *gop;

  type_malloc(op, osrc_remove_regex_t, 1);
  op->os = os;
  op->creds = creds;
  op->path = path;
  op->object_regex = object_regex;
  op->obj_types = obj_types;
  op->recurse_depth = recurse_depth;
  op->my_id = 0;
  get_random(&(op->my_id), sizeof(op->my_id));

  gop = new_thread_pool_op(osrc->tpc, NULL, osrc_remove_regex_object_func, (void *)op, free, 1);
  return(gop);
}

//***********************************************************************
// osrc_abort_remove_regex_object - Aborts a bulk remove call
//***********************************************************************

op_generic_t *osrc_abort_remove_regex_object(object_service_fn_t *os, op_generic_t *gop)
{
  osrc_priv_t *osrc = (osrc_priv_t *)os->priv;
  mq_msg_t *msg;
  unsigned char buf[512];
  int bpos;
  op_generic_t *g;
  osrc_set_regex_t *op;

log_printf(5, "START\n");
  op = gop_get_private(gop);

  //** Form the message
  msg = mq_make_exec_core_msg(osrc->remote_host, 1);
  mq_msg_append_mem(msg, OSR_ABORT_REMOVE_REGEX_OBJECT_KEY, OSR_ABORT_REMOVE_REGEX_OBJECT_SIZE, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
  osrc_add_creds(os, op->creds, msg);

  bpos = zigzag_encode(sizeof(op->my_id), buf);
  memcpy(&(buf[bpos]), &(op->my_id), sizeof(op->my_id));
  bpos += sizeof(op->my_id);
  mq_msg_append_mem(msg, buf, bpos, MQF_MSG_KEEP_DATA);

  mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

  //** Make the gop
  g = new_mq_op(osrc->mqc, msg, osrc_response_status, NULL, NULL, osrc->timeout);

log_printf(5, "END\n");

  return(g);
}

//***********************************************************************
// osrc_remove_object - Makes a remove object operation
//***********************************************************************

op_generic_t *osrc_remove_object(object_service_fn_t *os, creds_t *creds, char *path)
{
  osrc_priv_t *osrc = (osrc_priv_t *)os->priv;
  mq_msg_t *msg;
  op_generic_t *gop;

log_printf(5, "START fname=%s\n", path);

  //** Form the message
  msg = mq_make_exec_core_msg(osrc->remote_host, 1);
  mq_msg_append_mem(msg, OSR_REMOVE_OBJECT_KEY, OSR_REMOVE_OBJECT_SIZE, MQF_MSG_KEEP_DATA);
  osrc_add_creds(os, creds, msg);
  mq_msg_append_mem(msg, path, strlen(path)+1, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

  //** Make the gop
  gop = new_mq_op(osrc->mqc, msg, osrc_response_status, NULL, NULL, osrc->timeout);

log_printf(5, "END\n");

  return(gop);
}


//***********************************************************************
// osrc_regex_object_set_multiple_attrs - Does a bulk regex change.
//     Each matching object's attr are changed.  If the object is a directory
//     then the system will recursively change it's contents up to the
//     recursion depth.
//***********************************************************************

op_status_t osrc_regex_object_set_multiple_attrs_func(void *arg, int id)
{
  osrc_set_regex_t *op = (osrc_set_regex_t *)arg;
  osrc_priv_t *osrc = (osrc_priv_t *)op->os->priv;
  int bpos, bufsize, again, n, i, len;
  unsigned char *buffer;
  mq_msg_t *msg, *spin;
  op_generic_t *gop, *g;
  op_status_t status;

log_printf(5, "START\n");

  status = op_success_status;

  //** Form the message
  msg = mq_make_exec_core_msg(osrc->remote_host, 1);
  mq_msg_append_mem(msg, OSR_REGEX_SET_MULT_ATTR_KEY, OSR_REGEX_SET_MULT_ATTR_SIZE, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
  osrc_add_creds(op->os, op->creds, msg);
  if (op->id != NULL) {
     mq_msg_append_mem(msg, op->id, strlen(op->id), MQF_MSG_KEEP_DATA);
  } else {
     mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
  }

  bufsize = 4096;
  type_malloc(buffer, unsigned char, bufsize);
  do {
    again = 0;
    bpos = 0;

    n = zigzag_encode(osrc->timeout, buffer);
    if (n<0) { again = 1; n=4; }
    bpos += n;

    n = zigzag_encode(sizeof(op->my_id), &(buffer[bpos]));
    if (n<0) { again = 1; n=4; }
    bpos += n;
    memcpy(&(buffer[bpos]), &(op->my_id), sizeof(op->my_id));
    bpos += sizeof(op->my_id);

    n = zigzag_encode(osrc->spin_fail, &(buffer[bpos]));
    if (n<0) { again = 1; n=4; }
    bpos += n;

    n = zigzag_encode(op->recurse_depth, &(buffer[bpos]));
    if (n<0) { again = 1; n=4; }
    bpos += n;

    n = zigzag_encode(op->obj_types, &(buffer[bpos]));
    if (n<0) { again = 1; n=4; }
    bpos += n;

    n = os_regex_table_pack(op->path, &(buffer[(again==0) ? bpos : 0]), bufsize-bpos);
    if (n < 0) { again = 1; n = -n; }
    bpos += n;

    n = os_regex_table_pack(op->object_regex, &(buffer[(again==0) ? bpos : 0]), bufsize-bpos);
    if (n < 0) { again = 1; n = -n; }
    bpos += n;

    if (again == 1) {
       bufsize = bpos + 10;
       free(buffer);
       type_malloc(buffer, unsigned char, bufsize);
    }

    bpos += zigzag_encode(op->n_attrs, (unsigned char *)&(buffer[bpos]));

    for (i=0; i<op->n_attrs; i++) {
log_printf(15, "i=%d key=%s val=%s bpos=%d\n", i, op->key[i], op->val[i], bpos);
      len = strlen(op->key[i]);
      n = (again == 0) ? zigzag_encode(len, (unsigned char *)&(buffer[bpos])) : 4;
      if (n<0) { again = 1; n=4; }
      bpos += n;
      if (again == 0) memcpy(&(buffer[bpos]), op->key[i], len);
      bpos += len;

      len = op->v_size[i];
      n = (again == 0) ? zigzag_encode(len, (unsigned char *)&(buffer[bpos])) : 4;
      if (n<0) { again = 1; n=4; }
      bpos += n;
      if (len > 0) {
         if (again == 0) memcpy(&(buffer[bpos]), op->val[i], len);
         bpos += len;
      }
    }
  } while (again == 1);


  mq_msg_append_mem(msg, buffer, bpos, MQF_MSG_AUTO_FREE);
  mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);


  //** Make the gop and submit it
  gop = new_mq_op(osrc->mqc, msg, osrc_response_stream_status, op->os, NULL, osrc->timeout);
  gop_start_execution(gop);

  //** Wait for it to complete Sending hearbeats as needed
  while ((g = gop_timed_waitany(gop, osrc->spin_interval)) == NULL) {
     //** Form the spin message
     spin = mq_make_exec_core_msg(osrc->remote_host, 0);
     mq_msg_append_mem(spin, OSR_SPIN_HB_KEY, OSR_SPIN_HB_SIZE, MQF_MSG_KEEP_DATA);
     mq_msg_append_mem(spin, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
     osrc_add_creds(op->os, op->creds, spin);
     mq_msg_append_mem(spin, &(op->my_id), sizeof(op->my_id), MQF_MSG_KEEP_DATA);

     //** And send it
     g = new_mq_op(osrc->mqc, spin, NULL, NULL, NULL, osrc->timeout);
     log_printf(5, "spin hb sent. gid=%d\n", gop_id(g));
     gop_set_auto_destroy(g, 1);
     gop_start_execution(g);
  }

  gop_waitall(gop);
  status = gop_get_status(gop);
  gop_free(gop, OP_DESTROY);

log_printf(5, "END status=%d\n", status.op_status);


  return(status);
}

//***********************************************************************
// osrc_regex_object_set_multiple_attrs - Does a bulk regex change.
//     Each matching object's attr are changed.  If the object is a directory
//     then the system will recursively change it's contents up to the
//     recursion depth.
//***********************************************************************

op_generic_t *osrc_regex_object_set_multiple_attrs(object_service_fn_t *os, creds_t *creds, char *id, os_regex_table_t *path, os_regex_table_t *object_regex, int object_types, int recurse_depth, char **key, void **val, int *v_size, int n_attrs)
{
  osrc_priv_t *osrc = (osrc_priv_t *)os->priv;
  osrc_set_regex_t *op;
  op_generic_t *gop;

  type_malloc(op, osrc_set_regex_t, 1);
  op->os = os;
  op->creds = creds;
  op->id = id;
  op->path = path;
  op->object_regex = object_regex;
  op->obj_types = object_types;
  op->recurse_depth = recurse_depth;
  op->key = key;
  op->val = val;
  op->v_size= v_size;
  op->n_attrs = n_attrs;
  op->my_id = 0;
  get_random(&(op->my_id), sizeof(op->my_id));

  gop = new_thread_pool_op(osrc->tpc, NULL, osrc_regex_object_set_multiple_attrs_func, (void *)op, free, 1);
  gop_set_private(gop, op);

  return(gop);
}

//***********************************************************************
// osrc_abort_regex_object_set_multiple_attrs - Aborts a bulk attr call
//***********************************************************************

op_generic_t *osrc_abort_regex_object_set_multiple_attrs(object_service_fn_t *os, op_generic_t *gop)
{
  osrc_priv_t *osrc = (osrc_priv_t *)os->priv;
  mq_msg_t *msg;
  unsigned char buf[512];
  int bpos;
  op_generic_t *g;
  osrc_set_regex_t *op;

log_printf(5, "START\n");
  op = gop_get_private(gop);

  //** Form the message
  msg = mq_make_exec_core_msg(osrc->remote_host, 1);
  mq_msg_append_mem(msg, OSR_ABORT_REGEX_SET_MULT_ATTR_KEY, OSR_ABORT_REGEX_SET_MULT_ATTR_SIZE, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
  osrc_add_creds(os, op->creds, msg);

  bpos = zigzag_encode(sizeof(op->my_id), buf);
  memcpy(&(buf[bpos]), &(op->my_id), sizeof(op->my_id));
  bpos += sizeof(op->my_id);
  mq_msg_append_mem(msg, buf, bpos, MQF_MSG_KEEP_DATA);

  mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

  //** Make the gop
  g = new_mq_op(osrc->mqc, msg, osrc_response_status, NULL, NULL, osrc->timeout);

log_printf(5, "END\n");

  return(g);
}


//***********************************************************************
//  osrc_exists - Returns the object type  and 0 if it doesn't exist
//***********************************************************************

op_generic_t *osrc_exists(object_service_fn_t *os, creds_t *creds, char *path)
{
  osrc_priv_t *osrc = (osrc_priv_t *)os->priv;
  mq_msg_t *msg;
  op_generic_t *gop;

log_printf(5, "START fname=%s\n", path);

  //** Form the message
  msg = mq_make_exec_core_msg(osrc->remote_host, 1);
  mq_msg_append_mem(msg, OSR_EXISTS_KEY, OSR_EXISTS_SIZE, MQF_MSG_KEEP_DATA);
  osrc_add_creds(os, creds, msg);
  mq_msg_append_mem(msg, path, strlen(path)+1, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

  //** Make the gop
  gop = new_mq_op(osrc->mqc, msg, osrc_response_status, NULL, NULL, osrc->timeout);

log_printf(5, "END\n");

  return(gop);
}

//***********************************************************************
// osrc_create_object - Creates an object
//***********************************************************************

op_generic_t *osrc_create_object(object_service_fn_t *os, creds_t *creds, char *path, int type, char *id)
{
  osrc_priv_t *osrc = (osrc_priv_t *)os->priv;
  mq_msg_t *msg;
  op_generic_t *gop;
  unsigned char buffer[10], *sent;
  int n;

//return(os_create_object(osrc->os_temp, creds, path, type, id));

log_printf(5, "START fname=%s\n", path);

  //** Form the message
  msg = mq_make_exec_core_msg(osrc->remote_host, 1);
  mq_msg_append_mem(msg, OSR_CREATE_OBJECT_KEY, OSR_CREATE_OBJECT_SIZE, MQF_MSG_KEEP_DATA);
  osrc_add_creds(os, creds, msg);
  mq_msg_append_mem(msg, path, strlen(path)+1, MQF_MSG_KEEP_DATA);

  n = zigzag_encode(type, buffer);
  type_malloc(sent, unsigned char, n);
  memcpy(sent, buffer, n);
  mq_msg_append_frame(msg, mq_frame_new(sent, n, MQF_MSG_AUTO_FREE));

  if (id == NULL) {
     mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
  } else {
     mq_msg_append_mem(msg, id, strlen(id)+1, MQF_MSG_KEEP_DATA);
  }
  mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

  //** Make the gop
  gop = new_mq_op(osrc->mqc, msg, osrc_response_status, NULL, NULL, osrc->timeout);

log_printf(5, "END\n");

  return(gop);
}


//***********************************************************************
// osrc_symlink_object - Generates a symbolic link object operation
//***********************************************************************

op_generic_t *osrc_symlink_object(object_service_fn_t *os, creds_t *creds, char *src_path, char *dest_path, char *id)
{
  osrc_priv_t *osrc = (osrc_priv_t *)os->priv;
  mq_msg_t *msg;
  op_generic_t *gop;

log_printf(5, "START src_fname=%s\n", src_path);

  //** Form the message
  msg = mq_make_exec_core_msg(osrc->remote_host, 1);
  mq_msg_append_mem(msg, OSR_SYMLINK_OBJECT_KEY, OSR_SYMLINK_OBJECT_SIZE, MQF_MSG_KEEP_DATA);
  osrc_add_creds(os, creds, msg);
  mq_msg_append_mem(msg, src_path, strlen(src_path)+1, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, dest_path, strlen(dest_path)+1, MQF_MSG_KEEP_DATA);
  if (id == NULL) {
     mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
  } else {
     mq_msg_append_mem(msg, id, strlen(id)+1, MQF_MSG_KEEP_DATA);
  }
  mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

  //** Make the gop
  gop = new_mq_op(osrc->mqc, msg, osrc_response_status, NULL, NULL, osrc->timeout);

log_printf(5, "END\n");

  return(gop);
}


//***********************************************************************
// osrc_hardlink_object - Generates a hard link object operation
//***********************************************************************

op_generic_t *osrc_hardlink_object(object_service_fn_t *os, creds_t *creds, char *src_path, char *dest_path, char *id)
{
  osrc_priv_t *osrc = (osrc_priv_t *)os->priv;
  mq_msg_t *msg;
  op_generic_t *gop;

log_printf(5, "START src_fname=%s\n", src_path);

  //** Form the message
  msg = mq_make_exec_core_msg(osrc->remote_host, 1);
  mq_msg_append_mem(msg, OSR_HARDLINK_OBJECT_KEY, OSR_HARDLINK_OBJECT_SIZE, MQF_MSG_KEEP_DATA);
  osrc_add_creds(os, creds, msg);
  mq_msg_append_mem(msg, src_path, strlen(src_path)+1, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, dest_path, strlen(dest_path)+1, MQF_MSG_KEEP_DATA);
  if (id == NULL) {
     mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
  } else {
     mq_msg_append_mem(msg, id, strlen(id)+1, MQF_MSG_KEEP_DATA);
  }
  mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

  //** Make the gop
  gop = new_mq_op(osrc->mqc, msg, osrc_response_status, NULL, NULL, osrc->timeout);

log_printf(5, "END\n");

  return(gop);
}


//***********************************************************************
// osrc_move_object - Generates a move object operation
//***********************************************************************

op_generic_t *osrc_move_object(object_service_fn_t *os, creds_t *creds, char *src_path, char *dest_path)
{
  osrc_priv_t *osrc = (osrc_priv_t *)os->priv;
  mq_msg_t *msg;
  op_generic_t *gop;

log_printf(5, "START src_fname=%s\n", src_path);

  //** Form the message
  msg = mq_make_exec_core_msg(osrc->remote_host, 1);
  mq_msg_append_mem(msg, OSR_MOVE_OBJECT_KEY, OSR_MOVE_OBJECT_SIZE, MQF_MSG_KEEP_DATA);
  osrc_add_creds(os, creds, msg);
  mq_msg_append_mem(msg, src_path, strlen(src_path)+1, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, dest_path, strlen(dest_path)+1, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

  //** Make the gop
  gop = new_mq_op(osrc->mqc, msg, osrc_response_status, NULL, NULL, osrc->timeout);

log_printf(5, "END\n");

  return(gop);
}

//***********************************************************************
// osrc_copy_mult_attrs_internal - Copies multiple object attributes between
//    objects
//***********************************************************************

op_generic_t *osrc_copy_mult_attrs_internal(object_service_fn_t *os, osrc_mult_attr_t *ma, creds_t *creds)
{
  osrc_priv_t *osrc = (osrc_priv_t *)os->priv;
  osrc_object_fd_t *sfd = (osrc_object_fd_t *)ma->fd;
  osrc_object_fd_t *dfd = (osrc_object_fd_t *)ma->fd_dest;
  mq_msg_t *msg;
  op_generic_t *gop;
  int i, bpos, len, nmax;
  char *data;

log_printf(5, "START\n");

  //** Form the message
  msg = mq_make_exec_core_msg(osrc->remote_host, 1);
  mq_msg_append_mem(msg, OSR_COPY_MULTIPLE_ATTR_KEY, OSR_COPY_MULTIPLE_ATTR_SIZE, MQF_MSG_KEEP_DATA);
  osrc_add_creds(os, creds, msg);

  //** Form the heartbeat and handle frames
  mq_msg_append_mem(msg, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, sfd->data, sfd->size, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, dfd->data, dfd->size, MQF_MSG_KEEP_DATA);

  //** Form the attribute frame
  nmax = 12;  //** Add just a little extra
  for (i=0; i<ma->n; i++) { nmax += strlen(ma->key[i]) + 4 + strlen(ma->key_dest[i]) + 4; }
  type_malloc(data, char, nmax);
  bpos = 0;
  bpos += zigzag_encode(osrc->timeout, (unsigned char *)&(data[bpos]));
  bpos += zigzag_encode(ma->n, (unsigned char *)&(data[bpos]));
  for (i=0; i<ma->n; i++) {
     len = strlen(ma->key[i]);
     bpos += zigzag_encode(len, (unsigned char *)&(data[bpos]));
     memcpy(&(data[bpos]), ma->key[i], len);
     bpos += len;

     len = strlen(ma->key_dest[i]);
     bpos += zigzag_encode(len, (unsigned char *)&(data[bpos]));
     memcpy(&(data[bpos]), ma->key_dest[i], len);
     bpos += len;
  }
  mq_msg_append_mem(msg, data, bpos, MQF_MSG_AUTO_FREE);

  mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

  //** Make the gop
  gop = new_mq_op(osrc->mqc, msg, osrc_response_status, ma, free, osrc->timeout);

log_printf(5, "END\n");

  return(gop);
}


//***********************************************************************
// osrc_copy_multiple_attrs - Generates a copy object multiple attribute operation
//***********************************************************************

op_generic_t *osrc_copy_multiple_attrs(object_service_fn_t *os, creds_t *creds, os_fd_t *fd_src, char **key_src, os_fd_t *fd_dest, char **key_dest, int n)
{
  osrc_mult_attr_t *ma;

  type_malloc_clear(ma, osrc_mult_attr_t, 1);
  ma->os = os;
  ma->fd = fd_src;
  ma->fd_dest = fd_dest;
  ma->key = key_src;
  ma->key_dest = key_dest;
  ma->n = n;

  return(osrc_copy_mult_attrs_internal(os, ma, creds));
}


//***********************************************************************
// osrc_copy_attr - Generates a copy object attribute operation
//***********************************************************************

op_generic_t *osrc_copy_attr(object_service_fn_t *os, creds_t *creds, os_fd_t *fd_src, char *key_src, os_fd_t *fd_dest, char *key_dest)
{
  osrc_mult_attr_t *ma;

  type_malloc_clear(ma, osrc_mult_attr_t, 1);
  ma->os = os;
  ma->fd = fd_src;
  ma->fd_dest = fd_dest;
  ma->key = &(ma->key_tmp);
  ma->key_tmp = key_src;
  ma->key_dest = (char **)&(ma->val_tmp);
  ma->val_tmp = key_dest;

  ma->n = 1;

  return(osrc_copy_mult_attrs_internal(os, ma, creds));
}


//***********************************************************************
// osrc_symlink_mult_attrs_internal - Symlinks multiple object attributes between
//    objects
//***********************************************************************

op_generic_t *osrc_symlink_mult_attrs_internal(object_service_fn_t *os, osrc_mult_attr_t *ma, creds_t *creds)
{
  osrc_priv_t *osrc = (osrc_priv_t *)os->priv;
  osrc_object_fd_t *dfd = (osrc_object_fd_t *)ma->fd_dest;
  mq_msg_t *msg;
  op_generic_t *gop;
  int i, bpos, len, nmax;
  char *data;

log_printf(5, "START\n");

  //** Form the message
  msg = mq_make_exec_core_msg(osrc->remote_host, 1);
  mq_msg_append_mem(msg, OSR_SYMLINK_MULTIPLE_ATTR_KEY, OSR_SYMLINK_MULTIPLE_ATTR_SIZE, MQF_MSG_KEEP_DATA);
  osrc_add_creds(os, creds, msg);

  //** Form the heartbeat and handle frames
  mq_msg_append_mem(msg, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, dfd->data, dfd->size, MQF_MSG_KEEP_DATA);

  //** Form the attribute frame
  nmax = 12;  //** Add just a little extra
  for (i=0; i<ma->n; i++) { nmax += strlen(ma->src_path[i]) + 4 + strlen(ma->key[i]) + 4 + strlen(ma->key_dest[i]) + 4; }
  type_malloc(data, char, nmax);
  bpos = 0;
  bpos += zigzag_encode(osrc->timeout, (unsigned char *)&(data[bpos]));
  bpos += zigzag_encode(ma->n, (unsigned char *)&(data[bpos]));
  for (i=0; i<ma->n; i++) {
     len = strlen(ma->src_path[i]);
     bpos += zigzag_encode(len, (unsigned char *)&(data[bpos]));
     memcpy(&(data[bpos]), ma->src_path[i], len);
     bpos += len;

     len = strlen(ma->key[i]);
     bpos += zigzag_encode(len, (unsigned char *)&(data[bpos]));
     memcpy(&(data[bpos]), ma->key[i], len);
     bpos += len;

     len = strlen(ma->key_dest[i]);
     bpos += zigzag_encode(len, (unsigned char *)&(data[bpos]));
     memcpy(&(data[bpos]), ma->key_dest[i], len);
     bpos += len;
  }
  mq_msg_append_mem(msg, data, bpos, MQF_MSG_AUTO_FREE);

  mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

  //** Make the gop
  gop = new_mq_op(osrc->mqc, msg, osrc_response_status, ma, free, osrc->timeout);

log_printf(5, "END\n");

  return(gop);
}


//***********************************************************************
// osrc_symlink_multiple_attrs - Generates a link multiple attribute operation
//***********************************************************************

op_generic_t *osrc_symlink_multiple_attrs(object_service_fn_t *os, creds_t *creds, char **src_path, char **key_src, os_fd_t *fd_dest, char **key_dest, int n)
{
  osrc_mult_attr_t *ma;

  type_malloc_clear(ma, osrc_mult_attr_t, 1);
  ma->os = os;
  ma->src_path = src_path;
  ma->key = key_src;
  ma->fd_dest = fd_dest;
  ma->key_dest = key_dest;
  ma->n = n;

  return(osrc_symlink_mult_attrs_internal(os, ma, creds));
}


//***********************************************************************
// osrc_symlink_attr - Generates a link attribute operation
//***********************************************************************

op_generic_t *osrc_symlink_attr(object_service_fn_t *os, creds_t *creds, char *src_path, char *key_src, os_fd_t *fd_dest, char *key_dest)
{
  osrc_mult_attr_t *ma;

  type_malloc_clear(ma, osrc_mult_attr_t, 1);
  ma->os = os;
  ma->src_path = &(ma->src_tmp);
  ma->src_tmp = src_path;
  ma->key = &(ma->key_tmp);
  ma->key_tmp = key_src;
  ma->fd_dest = fd_dest;
  ma->key_dest = (char **)&(ma->val_tmp);
  ma->val_tmp = key_dest;

  ma->n = 1;

  return(osrc_symlink_mult_attrs_internal(os, ma, creds));
}

//***********************************************************************
// osrc_move_mult_attrs_internal - Renames multiple object attributes
//***********************************************************************

op_generic_t *osrc_move_mult_attrs_internal(object_service_fn_t *os, osrc_mult_attr_t *ma, creds_t *creds)
{
  osrc_priv_t *osrc = (osrc_priv_t *)os->priv;
  osrc_object_fd_t *sfd = (osrc_object_fd_t *)ma->fd;
  mq_msg_t *msg;
  op_generic_t *gop;
  int i, bpos, len, nmax;
  char *data;

log_printf(5, "START\n");

  //** Form the message
  msg = mq_make_exec_core_msg(osrc->remote_host, 1);
  mq_msg_append_mem(msg, OSR_MOVE_MULTIPLE_ATTR_KEY, OSR_MOVE_MULTIPLE_ATTR_SIZE, MQF_MSG_KEEP_DATA);
  osrc_add_creds(os, creds, msg);

  //** Form the heartbeat and handle frames
  mq_msg_append_mem(msg, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, sfd->data, sfd->size, MQF_MSG_KEEP_DATA);

  //** Form the attribute frame
  nmax = 12;  //** Add just a little extra
  for (i=0; i<ma->n; i++) { nmax += strlen(ma->key[i]) + 4 + strlen(ma->key_dest[i]) + 4; }
  type_malloc(data, char, nmax);
  bpos = 0;
  bpos += zigzag_encode(osrc->timeout, (unsigned char *)&(data[bpos]));
  bpos += zigzag_encode(ma->n, (unsigned char *)&(data[bpos]));
  for (i=0; i<ma->n; i++) {
     len = strlen(ma->key[i]);
     bpos += zigzag_encode(len, (unsigned char *)&(data[bpos]));
     memcpy(&(data[bpos]), ma->key[i], len);
     bpos += len;

     len = strlen(ma->key_dest[i]);
     bpos += zigzag_encode(len, (unsigned char *)&(data[bpos]));
     memcpy(&(data[bpos]), ma->key_dest[i], len);
     bpos += len;
  }
  mq_msg_append_mem(msg, data, bpos, MQF_MSG_AUTO_FREE);

  mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

  //** Make the gop
  gop = new_mq_op(osrc->mqc, msg, osrc_response_status, ma, free, osrc->timeout);

log_printf(5, "END\n");

  return(gop);
}


//***********************************************************************
// osrc_move_multiple_attrs - Generates a move object attributes operation
//***********************************************************************

op_generic_t *osrc_move_multiple_attrs(object_service_fn_t *os, creds_t *creds, os_fd_t *fd, char **key_old, char **key_new, int n)
{
  osrc_mult_attr_t *ma;

  type_malloc_clear(ma, osrc_mult_attr_t, 1);
  ma->os = os;
  ma->fd = fd;
  ma->key = key_old;
  ma->key_dest = key_new;
  ma->n = n;

  return(osrc_move_mult_attrs_internal(os, ma, creds));
}


//***********************************************************************
// osrc_move_attr - Generates a move object attribute operation
//***********************************************************************

op_generic_t *osrc_move_attr(object_service_fn_t *os, creds_t *creds, os_fd_t *fd, char *key_old, char *key_new)
{
  osrc_mult_attr_t *ma;

  type_malloc_clear(ma, osrc_mult_attr_t, 1);
  ma->os = os;
  ma->fd = fd;
  ma->key = &(ma->key_tmp);
  ma->key_tmp = key_old;
  ma->key_dest = (char **)&(ma->val_tmp);
  ma->val_tmp = key_new;

  ma->n = 1;

  return(osrc_move_mult_attrs_internal(os, ma, creds));
}


//*************************************************************
//  osf_store_val - Stores the return attribute value
//*************************************************************

int osrc_store_val(mq_stream_t *mqs, int src_size, void **dest, int *v_size)
{
  char *buf;

  if (*v_size >= 0) {
     if (*v_size < src_size) {
        *v_size = -src_size;
        mq_stream_read(mqs, NULL, src_size);  //** This drops the values
        return(1);
     } else if (*v_size > src_size) {
        buf = *dest; buf[src_size] = 0;  //** IF have the space NULL terminate
     }
  } else {
     if (src_size > 0) {
        *dest = malloc(src_size+1);
        buf = *dest; buf[src_size] = 0;  //** IF have the space NULL terminate
     } else {
        *v_size = src_size;
        *dest = NULL;
        return(0);
     }
  }

  *v_size = src_size;
  mq_stream_read(mqs, *dest, src_size);

  return(0);
}

//***********************************************************************
// osrc_response_get_multiple_attrs - Handles a get multiple attr response
//***********************************************************************

op_status_t osrc_response_get_multiple_attrs(void *task_arg, int tid)
{
  mq_task_t *task = (mq_task_t *)task_arg;
  osrc_mult_attr_t *ma = task->arg;
  osrc_priv_t *osrc = (osrc_priv_t *)ma->os->priv;
  mq_stream_t *mqs;
  op_status_t status;
  int len, err, i;

log_printf(5, "START\n");

  status = op_success_status;

  //** Parse the response
  mq_remove_header(task->response, 1);

  mqs = mq_stream_read_create(osrc->mqc, osrc->ongoing, osrc->host_id, osrc->host_id_len, mq_msg_first(task->response), osrc->remote_host, osrc->stream_timeout);

  //** Parse the status
  status.op_status = mq_stream_read_varint(mqs, &err);
log_printf(15, "op_status=%d\n", status.op_status);
  status.error_code = mq_stream_read_varint(mqs, &err);
log_printf(15, "error_code%d\n", status.error_code);

  if (err != 0) { status.op_status= OP_STATE_FAILURE; }  //** Trigger a failure if error reading from the stream
  if (status.op_status == OP_STATE_FAILURE) goto fail;

  //** Not get the attributes
  for (i=0; i < ma->n; i++) {
    len = mq_stream_read_varint(mqs, &err);
    if (err != 0) { status = op_failure_status; goto fail; }

    osrc_store_val(mqs, len, &(ma->val[i]), &(ma->v_size[i]));
log_printf(15, "val[%d]=%s\n", i, (char *)ma->val[i]);
  }

fail:
  mq_stream_destroy(mqs);

log_printf(5, "END status=%d %d\n", status.op_status, status.error_code);

  return(status);
}


//***********************************************************************
// osrc_get_mult_attrs_internal - Retreives multiple object attribute
//   If *v_size < 0 then space is allocated up to a max of abs(v_size)
//   and upon return *v_size contains the bytes loaded
//***********************************************************************

op_generic_t *osrc_get_mult_attrs_internal(object_service_fn_t *os, osrc_mult_attr_t *ma, creds_t *creds)
{
  osrc_priv_t *osrc = (osrc_priv_t *)os->priv;
  osrc_object_fd_t *ofd = (osrc_object_fd_t *)ma->fd;
  mq_msg_t *msg;
  op_generic_t *gop;
  int i, bpos, len, nmax;
  char *data;

log_printf(5, "START\n");

  //** Form the message
  msg = mq_make_exec_core_msg(osrc->remote_host, 1);
  mq_msg_append_mem(msg, OSR_GET_MULTIPLE_ATTR_KEY, OSR_GET_MULTIPLE_ATTR_SIZE, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
  osrc_add_creds(os, creds, msg);

  //** Form the heartbeat and handle frames
  mq_msg_append_mem(msg, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, ofd->data, ofd->size, MQF_MSG_KEEP_DATA);

  //** Form the attribute frame
  nmax = 12;  //** Add just a little extra
  for (i=0; i<ma->n; i++) { nmax += strlen(ma->key[i]) + 4 + 4; }
  type_malloc(data, char, nmax);
  bpos = zigzag_encode(osrc->max_stream, (unsigned char *)data);
  bpos += zigzag_encode(osrc->timeout, (unsigned char *)&(data[bpos]));
  bpos += zigzag_encode(ma->n, (unsigned char *)&(data[bpos]));
  for (i=0; i<ma->n; i++) {
     len = strlen(ma->key[i]);
     bpos += zigzag_encode(len, (unsigned char *)&(data[bpos]));
     memcpy(&(data[bpos]), ma->key[i], len);
     bpos += len;
     bpos += zigzag_encode(ma->v_size[i], (unsigned char *)&(data[bpos]));
  }
  mq_msg_append_mem(msg, data, bpos, MQF_MSG_AUTO_FREE);

  mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

  //** Make the gop
  gop = new_mq_op(osrc->mqc, msg, osrc_response_get_multiple_attrs, ma, free, osrc->timeout);

log_printf(5, "END\n");

  return(gop);
}

//***********************************************************************
// osrc_get_multiple_attrs - Retreives multiple object attribute
//   If *v_size < 0 then space is allocated up to a max of abs(v_size)
//   and upon return *v_size contains the bytes loaded
//***********************************************************************

op_generic_t *osrc_get_multiple_attrs(object_service_fn_t *os, creds_t *creds, os_fd_t *fd, char **key, void **val, int *v_size, int n)
{
  osrc_mult_attr_t *ma;

  type_malloc_clear(ma, osrc_mult_attr_t, 1);
  ma->os = os;
  ma->fd = fd;
  ma->key = key;
  ma->val = val;
  ma->v_size = v_size;
  ma->n = n;

  return(osrc_get_mult_attrs_internal(os, ma, creds));
}

//***********************************************************************
// osrc_get_attr - Retreives a single object attribute
//   If *v_size < 0 then space is allocated up to a max of abs(v_size)
//   and upon return *v_size contains the bytes loaded
//***********************************************************************

op_generic_t *osrc_get_attr(object_service_fn_t *os, creds_t *creds, os_fd_t *fd, char *key, void **val, int *v_size)
{
  osrc_mult_attr_t *ma;

  type_malloc_clear(ma, osrc_mult_attr_t, 1);
  ma->os = os;
  ma->fd = fd;
  ma->key = &(ma->key_tmp);
  ma->key_tmp = key;
  ma->val = val;
  ma->v_size = v_size;
  ma->n = 1;

  return(osrc_get_mult_attrs_internal(os, ma, creds));
}


//***********************************************************************
// osrc_set_mult_attrs_internal - Sets multiple object attributes
//***********************************************************************

op_generic_t *osrc_set_mult_attrs_internal(object_service_fn_t *os, osrc_mult_attr_t *ma, creds_t *creds)
{
  osrc_priv_t *osrc = (osrc_priv_t *)os->priv;
  osrc_object_fd_t *ofd = (osrc_object_fd_t *)ma->fd;
  mq_msg_t *msg;
  op_generic_t *gop;
  int i, bpos, len, nmax;
  char *data;

log_printf(5, "START\n");

  //** Form the message
  msg = mq_make_exec_core_msg(osrc->remote_host, 1);
  mq_msg_append_mem(msg, OSR_SET_MULTIPLE_ATTR_KEY, OSR_SET_MULTIPLE_ATTR_SIZE, MQF_MSG_KEEP_DATA);
  osrc_add_creds(os, creds, msg);

  //** Form the heartbeat and handle frames
  mq_msg_append_mem(msg, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, ofd->data, ofd->size, MQF_MSG_KEEP_DATA);

  //** Form the attribute frame
  nmax = 8;  //** Add just a little extra
  for (i=0; i<ma->n; i++) { nmax += strlen(ma->key[i]) + 4 + ma->v_size[i] + 4; }
  type_malloc(data, char, nmax);
  bpos = 0;
  bpos += zigzag_encode(osrc->timeout, (unsigned char *)&(data[bpos]));
  bpos += zigzag_encode(ma->n, (unsigned char *)&(data[bpos]));
  for (i=0; i<ma->n; i++) {
     len = strlen(ma->key[i]);
     bpos += zigzag_encode(len, (unsigned char *)&(data[bpos]));
     memcpy(&(data[bpos]), ma->key[i], len);
     bpos += len;

     bpos += zigzag_encode(ma->v_size[i], (unsigned char *)&(data[bpos]));
     if (ma->v_size[i] > 0) {
        memcpy(&(data[bpos]), ma->val[i], ma->v_size[i]);
        bpos += ma->v_size[i];
     }
  }
  mq_msg_append_mem(msg, data, bpos, MQF_MSG_AUTO_FREE);

  mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

  //** Make the gop
  gop = new_mq_op(osrc->mqc, msg, osrc_response_status, ma, free, osrc->timeout);

log_printf(5, "END\n");

  return(gop);

}

//***********************************************************************
// osrc_set_multiple_attrs - Sets multiple object attributes
//   If val[i] == NULL for the attribute is deleted
//***********************************************************************

op_generic_t *osrc_set_multiple_attrs(object_service_fn_t *os, creds_t *creds, os_fd_t *fd, char **key, void **val, int *v_size, int n)
{
  osrc_mult_attr_t *ma;

  type_malloc_clear(ma, osrc_mult_attr_t, 1);
  ma->os = os;
  ma->fd = fd;
  ma->key = key;
  ma->val = val;
  ma->v_size = v_size;
  ma->n = n;

  return(osrc_set_mult_attrs_internal(os, ma, creds));
}


//***********************************************************************
// osrc_set_attr - Sets a single object attribute
//   If val == NULL the attribute is deleted
//***********************************************************************

op_generic_t *osrc_set_attr(object_service_fn_t *os, creds_t *creds, os_fd_t *fd, char *key, void *val, int v_size)
{
  osrc_mult_attr_t *ma;

  type_malloc_clear(ma, osrc_mult_attr_t, 1);
  ma->os = os;
  ma->fd = fd;
  ma->key = &(ma->key_tmp);
  ma->key_tmp = key;
  ma->val = &(ma->val_tmp);
  ma->val_tmp = val;
  ma->v_size = &(ma->v_tmp);
  ma->v_tmp = v_size;
  ma->n = 1;


  return(osrc_set_mult_attrs_internal(os, ma, creds));
}


//***********************************************************************
// osrc_next_attr - Returns the next matching attribute
//***********************************************************************

int osrc_next_attr(os_attr_iter_t *oit, char **key, void **val, int *v_size)
{
  osrc_attr_iter_t *it = (osrc_attr_iter_t *)oit;
  int n, err;

  //** Init the return variables
  if (key != NULL) *key = NULL;
  *v_size = -1;
  if ((*v_size <= 0) && (val != NULL)) *val = NULL;

log_printf(5, "START\n");

  //** Check if already read the last attr
  if (it->no_more_attr == 1) {
     log_printf(5, "Finished No more attrs\n");
     return(-1);
  }

  //** Read the key len
  n = mq_stream_read_varint(it->mqs, &err);
  if (err != 0) {
     log_printf(5, "ERROR reading key len!\n");
     *v_size = -1;
     it->no_more_attr = 1;
     return(-2);
  }

  //** Check if Last attr. If so return
  if (n <= 0) {
     log_printf(5, "Finished No more attrs\n");
     *v_size = -1;
     it->no_more_attr = 1;
     return(-1);
  }

  //** Valid key so read it
  if (key != NULL) {
     type_malloc(*key, char, n+1);  (*key)[n] = 0;
     err = mq_stream_read(it->mqs, *key, n);
  } else {
     err = mq_stream_read(it->mqs, NULL, n); //** Want to drop the key name
  }
  if (err != 0) {
     log_printf(5, "ERROR reading key!");
     if (key != NULL) { free(*key); *key = NULL; }
     *v_size = -1;
     it->no_more_attr = 1;
     return(-2);
  }

  //** Read the value len
  n = mq_stream_read_varint(it->mqs, &err);
  if (err != 0) {
     log_printf(5, "ERROR reading prefix_len!");
     it->no_more_attr = 1;
     return(-1);
  }

  *v_size = it->v_max;
  osrc_store_val(it->mqs, n, val, v_size);

  log_printf(5, "key=%s val=%s v_size=%d\n", *key, (char *)(*val), *v_size);

  log_printf(5, "END\n");

  return(0);
}

//***********************************************************************
// osrc_response_attr_iter - Handles the create_attr_iter() response
//***********************************************************************

op_status_t osrc_response_attr_iter(void *task_arg, int tid)
{
  mq_task_t *task = (mq_task_t *)task_arg;
  osrc_attr_iter_t *it = (osrc_attr_iter_t *)task->arg;
  osrc_priv_t *osrc = (osrc_priv_t *)it->os->priv;
 op_status_t status;
  int err;

log_printf(5, "START\n");

  status = op_success_status;

  //** Parse the response
  mq_remove_header(task->response, 1);

  it->mqs = mq_stream_read_create(osrc->mqc, osrc->ongoing, osrc->host_id, osrc->host_id_len, mq_msg_first(task->response), osrc->remote_host, osrc->stream_timeout);

  //** Parse the status
  status.op_status = mq_stream_read_varint(it->mqs, &err);
log_printf(15, "op_status=%d\n", status.op_status);
  status.error_code = mq_stream_read_varint(it->mqs, &err);
log_printf(15, "error_code%d\n", status.error_code);

  if (err != 0) { status.op_status= OP_STATE_FAILURE; }  //** Trigger a failure if error reading from the stream
  if (status.op_status == OP_STATE_FAILURE) {
     mq_stream_destroy(it->mqs);
  } else {
     //** Remove the response from the task to keep it from being freed.
     //** We'll do it manually
     it->response = task->response;
     task->response = NULL;
  }

log_printf(5, "END status=%d %d\n", status.op_status, status.error_code);

  return(status);
}

//***********************************************************************
// osrc_create_attr_iter - Creates an attribute iterator
//   Each entry in the attr table corresponds to a different regex
//   for selecting attributes
//***********************************************************************

os_attr_iter_t *osrc_create_attr_iter(object_service_fn_t *os, creds_t *creds, os_fd_t *fd, os_regex_table_t *attr, int v_max)
{
  osrc_priv_t *osrc = (osrc_priv_t *)os->priv;
  osrc_object_fd_t *ofd = (osrc_object_fd_t *)fd;
  osrc_attr_iter_t *it;
  int bpos, bufsize, again, n, err;
  unsigned char *buffer;
  mq_msg_t *msg;
  op_generic_t *gop;

log_printf(5, "START\n");

  //** Form the message
  msg = mq_make_exec_core_msg(osrc->remote_host, 1);
  mq_msg_append_mem(msg, OSR_ATTR_ITER_KEY, OSR_ATTR_ITER_SIZE, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, osrc->host_id, strlen(osrc->host_id)+1, MQF_MSG_KEEP_DATA);
  osrc_add_creds(os, creds, msg);
  mq_msg_append_mem(msg, ofd->data, ofd->size, MQF_MSG_KEEP_DATA);

  bufsize = 4096;
  type_malloc(buffer, unsigned char, bufsize);
  do {
    again = 0;
    bpos = 0;

    bpos += zigzag_encode(osrc->timeout, buffer);
    bpos += zigzag_encode(v_max, &(buffer[bpos]));

    n = os_regex_table_pack(attr, &(buffer[(again==0) ? bpos : 0]), bufsize-bpos);
    if (n < 0) { again = 1; n = -n; }
    bpos += n;

    if (again == 1) {
       bufsize = bpos + 10;
       free(buffer);
       type_malloc(buffer, unsigned char, bufsize);
    }
  } while (again == 1);

  mq_msg_append_mem(msg, buffer, bpos, MQF_MSG_AUTO_FREE);
  mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);


  //** Make the iterator handle
  type_malloc_clear(it, osrc_attr_iter_t, 1);
  it->os = os;
  it->v_max = v_max;

  //** Make the gop and execute it
  gop = new_mq_op(osrc->mqc, msg, osrc_response_attr_iter, it, NULL, osrc->timeout);
  err = gop_waitall(gop);
  if (err != OP_STATE_SUCCESS) {
     log_printf(5, "ERROR status=%d\n", err);
     free(it);
     return(NULL);
  }
  gop_free(gop, OP_DESTROY);

log_printf(5, "END\n");

  return(it);
}


//***********************************************************************
// osrc_destroy_attr_iter - Destroys an attribute iterator
//***********************************************************************

void osrc_destroy_attr_iter(os_attr_iter_t *oit)
{
  osrc_attr_iter_t *it = (osrc_attr_iter_t *)oit;

  if (it->mqs != NULL) mq_stream_destroy(it->mqs);
  if ((it->response != NULL) && (it->is_sub_iter == 0)) mq_msg_destroy(it->response);

  free(it);
}


//***********************************************************************
// osrc_next_object - Returns the iterators next matching object
//***********************************************************************

int osrc_next_object(os_object_iter_t *oit, char **fname, int *prefix_len)
{
  osrc_object_iter_t *it = (osrc_object_iter_t *)oit;
  osrc_attr_iter_t *ait;
  int i, n, err, ftype;

log_printf(5, "START\n");

  if (it == NULL) { log_printf(0, "ERROR: it=NULL\n"); return(-2); }
  ait = NULL;

  //** If a regex attr iter make sure and flush any remaining attrs
  // from the previous object
  if (it->ait != NULL) {
     ait = *(osrc_attr_iter_t **)it->ait;
     if (ait->no_more_attr == 0) {
        do {
          n = os_next_attr(it->os, ait, NULL, NULL, 0);
        } while (n == 0);
     }
  }

  //** Read the object type
  ftype = mq_stream_read_varint(it->mqs, &err);
  if (err != 0) {
     log_printf(5, "ERROR reading object type!\n");
     return(0);
  }

  //** Last object so return
  if (ftype <= 0) {
     *fname = NULL; *prefix_len = -1;
     log_printf(5, "No more objects\n");
     return(ftype);
  }

  //** Read the prefix len
  *prefix_len = mq_stream_read_varint(it->mqs, &err);
  if (err != 0) {
     log_printf(5, "ERROR reading prefix_len!");
     return(-1);
  }

  //** Read the object name
  n = mq_stream_read_varint(it->mqs, &err);
  if (err != 0) {
     log_printf(5, "ERROR reading object len!");
     return(-1);
  }
  type_malloc(*fname, char, n+1); (*fname)[n] = 0;
  err = mq_stream_read(it->mqs, *fname, n);
  if (err != 0) {
     log_printf(5, "ERROR reading fname!");
     free(*fname);
     *fname = NULL;
     return(-1);
  }

  log_printf(5, "ftype=%d fname=%s prefix_len=%d\n", ftype, *fname, *prefix_len);

  if (it->iter_type == OSRC_ITER_ALIST) { //** Now load the fixed attribute list
     for (i=0; i < it->n_keys; i++) { if (it->v_size[i] < 0) it->val[i] = NULL; }

     for (i=0; i < it->n_keys; i++) {
       n = mq_stream_read_varint(it->mqs, &err);
       if (err != 0) {
          log_printf(5, "ERROR reading attribute #%d!", i);
          return(-1);
       }

       it->v_size[i] = it->v_size_initial[i];
       osrc_store_val(it->mqs, n, &(it->val[i]), &(it->v_size[i]));
       log_printf(15, "val[%d]=%s\n", i, (char *)it->val[i]);
     }
  } else if (it->ait != NULL) {  //It's a regex for the attributes so reset the attr iter
     ait->no_more_attr = 0;
    log_printf(5, "Resetting att iter\n");
  }

  log_printf(5, "END\n");

  return(ftype);
}

//***********************************************************************
// osrc_response_object_iter - Handles a alist/regex iter response
//***********************************************************************

op_status_t osrc_response_object_iter(void *task_arg, int tid)
{
  mq_task_t *task = (mq_task_t *)task_arg;
  osrc_object_iter_t *it = (osrc_object_iter_t *)task->arg;
  osrc_priv_t *osrc = (osrc_priv_t *)it->os->priv;
  op_status_t status;
  int err;

log_printf(5, "START\n");

  status = op_success_status;

  //** Parse the response
  mq_remove_header(task->response, 1);

  it->mqs = mq_stream_read_create(osrc->mqc, osrc->ongoing, osrc->host_id, osrc->host_id_len, mq_msg_first(task->response), osrc->remote_host, osrc->stream_timeout);

  //** Parse the status
  status.op_status = mq_stream_read_varint(it->mqs, &err);
log_printf(15, "op_status=%d\n", status.op_status);
  status.error_code = mq_stream_read_varint(it->mqs, &err);
log_printf(15, "error_code%d\n", status.error_code);

  if (err != 0) { status.op_status= OP_STATE_FAILURE; }  //** Trigger a failure if error reading from the stream
  if (status.op_status == OP_STATE_FAILURE) {
     mq_stream_destroy(it->mqs);
  } else {
     //** Remove the response from the task to keep it from being freed.
     //** We'll do it manually
     it->response = task->response;
     task->response = NULL;
  }

log_printf(5, "END status=%d %d\n", status.op_status, status.error_code);

  return(status);
}


//***********************************************************************
// osrc_create_object_iter - Creates an object iterator to selectively
//  retreive object/attribute combinations
//
//***********************************************************************

os_object_iter_t *osrc_create_object_iter(object_service_fn_t *os, creds_t *creds, os_regex_table_t *path, os_regex_table_t *object_regex, int object_types,
                     os_regex_table_t *attr, int recurse_depth, os_attr_iter_t **it_attr, int v_max)
{
  osrc_priv_t *osrc = (osrc_priv_t *)os->priv;
  osrc_object_iter_t *it;
  osrc_attr_iter_t *ait;
  int bpos, bufsize, again, n, err;
  unsigned char *buffer;
  mq_msg_t *msg;
  op_generic_t *gop;

log_printf(5, "START\n");

  //** Form the message
  msg = mq_make_exec_core_msg(osrc->remote_host, 1);
  mq_msg_append_mem(msg, OSR_OBJECT_ITER_AREGEX_KEY, OSR_OBJECT_ITER_AREGEX_SIZE, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
  osrc_add_creds(os, creds, msg);

  bufsize = 4096;
  type_malloc(buffer, unsigned char, bufsize);
  do {
    again = 0;
    bpos = 0;

    bpos += zigzag_encode(osrc->timeout, buffer);
    bpos += zigzag_encode(recurse_depth, &(buffer[bpos]));
    bpos += zigzag_encode(object_types, &(buffer[bpos]));
    bpos += zigzag_encode(v_max, &(buffer[bpos]));

    n = os_regex_table_pack(path, &(buffer[(again==0) ? bpos : 0]), bufsize-bpos);
    if (n < 0) { again = 1; n = -n; }
    bpos += n;

    n = os_regex_table_pack(object_regex, &(buffer[(again==0) ? bpos : 0]), bufsize-bpos);
    if (n < 0) { again = 1; n = -n; }
    bpos += n;

    n = os_regex_table_pack(attr, &(buffer[(again==0) ? bpos : 0]), bufsize-bpos);
    if (n < 0) { again = 1; n = -n; }
    bpos += n;


    if (again == 1) {
       bufsize = bpos + 10;
       free(buffer);
       type_malloc(buffer, unsigned char, bufsize);
    }
  } while (again == 1);

  mq_msg_append_mem(msg, buffer, bpos, MQF_MSG_AUTO_FREE);
  mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);


  //** Make the iterator handle
  type_malloc_clear(it, osrc_object_iter_t, 1);
  it->iter_type = OSRC_ITER_AREGEX;
  it->os = os;
  it->v_max = v_max;
  it->ait = it_attr;

  //** Make the gop and execute it
  gop = new_mq_op(osrc->mqc, msg, osrc_response_object_iter, it, NULL, osrc->timeout);
  err = gop_waitall(gop);
  if (err != OP_STATE_SUCCESS) {
     log_printf(5, "ERROR status=%d\n", err);
     free(it);
     return(NULL);
  }
  gop_free(gop, OP_DESTROY);

  //** Go ahead and make the regex attr iter if needed
  if (it_attr != NULL) {
     type_malloc_clear(ait, osrc_attr_iter_t, 1);
     ait->os = os;
     ait->v_max = v_max;
     ait->is_sub_iter = 1;
     ait->no_more_attr = 1;
     ait->response = it->response;
     ait->mqs = it->mqs;
     *it_attr = ait;
  }

log_printf(5, "END\n");

  return(it);
}

//***********************************************************************
// osrc_create_object_iter_alist - Creates an object iterator to selectively
//  retreive object/attribute from a fixed attr list
//
//***********************************************************************

os_object_iter_t *osrc_create_object_iter_alist(object_service_fn_t *os, creds_t *creds, os_regex_table_t *path, os_regex_table_t *object_regex, int object_types,
                     int recurse_depth, char **key, void **val, int *v_size, int n_keys)
{
  osrc_priv_t *osrc = (osrc_priv_t *)os->priv;
  osrc_object_iter_t *it;

  int bpos, bufsize, again, n, i, err;
  unsigned char *buffer;
  mq_msg_t *msg;
  op_generic_t *gop;

log_printf(5, "START\n");

  //** Form the message
  msg = mq_make_exec_core_msg(osrc->remote_host, 1);
  mq_msg_append_mem(msg, OSR_OBJECT_ITER_ALIST_KEY, OSR_OBJECT_ITER_ALIST_SIZE, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
  osrc_add_creds(os, creds, msg);

  //** Estimate the size of the keys
  n = 0;
  for (i=0; i<n_keys; i++) {
     n += strlen(key[i]);
  }

  bufsize = 4096 + n_keys + 2*n + 1;
  type_malloc(buffer, unsigned char, bufsize);
  do {
    again = 0;
    bpos = 0;

    bpos += zigzag_encode(osrc->timeout, buffer);
    bpos += zigzag_encode(recurse_depth, &(buffer[bpos]));
    bpos += zigzag_encode(object_types, &(buffer[bpos]));

    bpos += zigzag_encode(n_keys, &(buffer[bpos]));
    for (i=0; i< n_keys; i++) {
       n = strlen(key[i]);
       bpos += zigzag_encode(n, &(buffer[bpos]));
       memcpy(&(buffer[bpos]), key[i], n);
       bpos += n;
       bpos += zigzag_encode(v_size[i], &(buffer[bpos]));
    }

    n = os_regex_table_pack(path, &(buffer[(again==0) ? bpos : 0]), bufsize-bpos);
    if (n < 0) { again = 1; n = -n; }
    bpos += n;

    n = os_regex_table_pack(object_regex, &(buffer[(again==0) ? bpos : 0]), bufsize-bpos);
    if (n < 0) { again = 1; n = -n; }
    bpos += n;


    if (again == 1) {
       bufsize = bpos + 10;
       free(buffer);
       type_malloc(buffer, unsigned char, bufsize);
    }
  } while (again == 1);

  mq_msg_append_mem(msg, buffer, bpos, MQF_MSG_AUTO_FREE);
  mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);


  //** Make the iterator handle
  type_malloc_clear(it, osrc_object_iter_t, 1);
  it->iter_type = OSRC_ITER_ALIST;
  it->os = os;
  it->val = val;
  it->v_size = v_size;
  it->n_keys = n_keys;
  type_malloc(it->v_size_initial, int, n_keys);
  memcpy(it->v_size_initial, it->v_size, n_keys*sizeof(int));

  //** Make the gop and execute it
  gop = new_mq_op(osrc->mqc, msg, osrc_response_object_iter, it, NULL, osrc->timeout);
  err = gop_waitall(gop);
  if (err != OP_STATE_SUCCESS) {
     log_printf(5, "ERROR status=%d\n", err);
     free(it);
     return(NULL);
  }
  gop_free(gop, OP_DESTROY);

log_printf(5, "END\n");

  return(it);
}

//***********************************************************************
// osrc_destroy_object_iter - Destroy the object iterator
//***********************************************************************

void osrc_destroy_object_iter(os_object_iter_t *oit)
{
  osrc_object_iter_t *it = (osrc_object_iter_t *)oit;

  if (it == NULL) { log_printf(0, "ERROR: it=NULL\n"); return; }

  if (it->mqs != NULL) mq_stream_destroy(it->mqs);
  if (it->response != NULL) mq_msg_destroy(it->response);
  if (it->v_size_initial != NULL) free(it->v_size_initial);
  if (it->ait != NULL) free(*(it->ait));

  free(it);
}

//***********************************************************************
// osrc_response_open - Handles an open request response
//***********************************************************************

op_status_t osrc_response_open(void *task_arg, int tid)
{
  mq_task_t *task = (mq_task_t *)task_arg;
  osrc_open_t *arg = (osrc_open_t *)task->arg;
  osrc_priv_t *osrc = (osrc_priv_t *)arg->os->priv;
  op_status_t status;
  void *data;
  osrc_object_fd_t *fd;

log_printf(5, "START\n");

  //** Parse the response
  mq_remove_header(task->response, 1);

  status = mq_read_status_frame(mq_msg_first(task->response), 0);
  if (status.op_status == OP_STATE_SUCCESS) {
     type_malloc(fd, osrc_object_fd_t, 1);
     fd->os = arg->os;
//mq_frame_t *frame = mq_msg_next(task->response);
//mq_get_frame(frame, (void **)&data, &(fd->size));
     mq_get_frame(mq_msg_next(task->response), (void **)&data, &(fd->size));
//     log_printf(5, "fd->size=%d\n", fd->size);

//intptr_t *kptr;
//mq_get_frame(frame, (void **)&kptr, &(fd->size));
//intptr_t key = *kptr;
//log_printf(5, "PTR key=%" PRIdPTR " size=%d\n", key, fd->size);
     type_malloc(fd->data, char, fd->size);
     memcpy(fd->data, data, fd->size);
     *(arg->pfd) = fd;
//     mq_ongoing_host_inc(osrc->ongoing, osrc->remote_host, fd->data, fd->size, osrc->heartbeat);
     mq_ongoing_host_inc(osrc->ongoing, osrc->remote_host, osrc->host_id, osrc->host_id_len, osrc->heartbeat);
  } else {
     *(arg->pfd) = NULL;
  }

//  if (arg->handle != NULL) free(arg->handle);
log_printf(5, "END status=%d %d\n", status.op_status, status.error_code);

  return(status);
}


//***********************************************************************
//  osrc_open_object - Makes the open file op
//***********************************************************************

op_generic_t *osrc_open_object(object_service_fn_t *os, creds_t *creds, char *path, int mode, char *id, os_fd_t **pfd, int max_wait)
{
  osrc_priv_t *osrc = (osrc_priv_t *)os->priv;
  osrc_open_t *arg;
  op_generic_t *gop;
  mq_msg_t *msg;
  unsigned char buffer[1024];
  unsigned char *sent;
  int n, hlen;

log_printf(5, "START fname=%s id=%s\n", path, id);
  type_malloc(arg, osrc_open_t, 1);
  arg->os = os;
  arg->pfd = (osrc_object_fd_t **)pfd;
  hlen = snprintf(arg->handle, 1024, "%s:%d", osrc->host_id, atomic_global_counter());

  //** Form the message
  msg = mq_make_exec_core_msg(osrc->remote_host, 1);
  mq_msg_append_mem(msg, OSR_OPEN_OBJECT_KEY, OSR_OPEN_OBJECT_SIZE, MQF_MSG_KEEP_DATA);
  osrc_add_creds(os, creds, msg);
  if (id != NULL) {
     mq_msg_append_mem(msg, id, strlen(id)+1, MQF_MSG_KEEP_DATA);
  } else {
     mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
  }

  mq_msg_append_mem(msg, path, strlen(path)+1, MQF_MSG_KEEP_DATA);

  //** Same for the mode
  n = zigzag_encode(mode, buffer);
  n += zigzag_encode(max_wait, &(buffer[n]));
  type_malloc(sent, unsigned char, n);
  memcpy(sent, buffer, n);
  mq_msg_append_frame(msg, mq_frame_new(sent, n, MQF_MSG_AUTO_FREE));

  //** Form the heartbeat and handle frames
log_printf(5, "host_id=%s\n", osrc->host_id);
log_printf(5, "handle=%s hlen=%d\n", arg->handle, hlen);

  mq_msg_append_mem(msg, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, arg->handle, hlen+1, MQF_MSG_KEEP_DATA);

  mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

  //** Make the gop
  gop = new_mq_op(osrc->mqc, msg, osrc_response_open, arg, free, osrc->timeout);
  gop_set_private(gop, arg);

log_printf(5, "END\n");

  return(gop);

//  return(os_open_object(osrc->os_temp, creds, path, mode, id, pfd, max_wait));
}

//***********************************************************************
//  osrc_abort_open_object - Aborts an ongoing open file op
//***********************************************************************

op_generic_t *osrc_abort_open_object(object_service_fn_t *os, op_generic_t *gop)
{
  osrc_priv_t *osrc = (osrc_priv_t *)os->priv;
  osrc_open_t *arg = (osrc_open_t *)gop_get_private(gop);
  mq_msg_t *msg;

  //** Form the message
  msg = mq_make_exec_core_msg(osrc->remote_host, 1);
  mq_msg_append_mem(msg, OSR_ABORT_OPEN_OBJECT_KEY, OSR_ABORT_OPEN_OBJECT_SIZE, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, arg->handle, strlen(arg->handle)+1, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

  //** Make the gop
  gop = new_mq_op(osrc->mqc, msg, osrc_response_status, NULL, NULL, osrc->timeout);

log_printf(5, "END\n");

  return(gop);


//  return(os_abort_open_object(osrc->os_temp, gop));
}


//***********************************************************************
// osrc_response_close_object - Handles the response to a clos_object call
//***********************************************************************

op_status_t osrc_response_close_object(void *task_arg, int tid)
{
  mq_task_t *task = (mq_task_t *)task_arg;
  osrc_object_fd_t *fd = (osrc_object_fd_t *)task->arg;
  osrc_priv_t *osrc = (osrc_priv_t *)fd->os->priv;

  op_status_t status;

log_printf(5, "START\n");

  //** Parse the response
  mq_remove_header(task->response, 1);

  status = mq_read_status_frame(mq_msg_first(task->response), 0);

  //** Quit tracking it
  mq_ongoing_host_dec(osrc->ongoing, osrc->remote_host, fd->data, fd->size);

log_printf(5, "END status=%d %d\n", status.op_status, status.error_code);

  free(fd->data);
  free(fd);

  return(status);
}

//***********************************************************************
//  osrc_close_object - Closes the object
//***********************************************************************

op_generic_t *osrc_close_object(object_service_fn_t *os, os_fd_t *ofd)
{
  osrc_priv_t *osrc = (osrc_priv_t *)os->priv;
  osrc_object_fd_t *fd = (osrc_object_fd_t *)ofd;
  op_generic_t *gop;
  mq_msg_t *msg;

log_printf(5, "START fd->size=%d\n", fd->size);

//intptr_t key = *(intptr_t *)fd->data;
//log_printf(5, "PTR key=%" PRIdPTR "\n", key);

  //** Form the message
  msg = mq_make_exec_core_msg(osrc->remote_host, 1);
  mq_msg_append_mem(msg, OSR_CLOSE_OBJECT_KEY, OSR_CLOSE_OBJECT_SIZE, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, osrc->host_id, strlen(osrc->host_id)+1, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, fd->data, fd->size, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

  //** Make the gop
  gop = new_mq_op(osrc->mqc, msg, osrc_response_close_object, fd, NULL, osrc->timeout);

log_printf(5, "END\n");

  return(gop);
}

//***********************************************************************
//  osrc_fsck_object - Allocates space for the object check
//***********************************************************************

op_generic_t *osrc_fsck_object(object_service_fn_t *os, creds_t *creds, char *fname, int ftype, int resolution)
{
  osrc_priv_t *osrc = (osrc_priv_t *)os->priv;
  int n;
  unsigned char buf[32];
  mq_msg_t *msg;
  op_generic_t *gop;

log_printf(5, "START\n");

  //** Form the message
  msg = mq_make_exec_core_msg(osrc->remote_host, 1);
  mq_msg_append_mem(msg, OSR_FSCK_OBJECT_KEY, OSR_FSCK_OBJECT_SIZE, MQF_MSG_KEEP_DATA);
  osrc_add_creds(os, creds, msg);
  mq_msg_append_mem(msg, fname, strlen(fname), MQF_MSG_KEEP_DATA);

  n = zigzag_encode(ftype, buf);
  n += zigzag_encode(resolution, &(buf[n]));
  n += zigzag_encode(osrc->timeout, &(buf[n]));
  mq_msg_append_mem(msg, buf, n, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

  //** Make the gop
  gop = new_mq_op(osrc->mqc, msg, osrc_response_status, NULL, NULL, osrc->timeout);

  return(gop);
}


//***********************************************************************
// osrc_next_fsck - Returns the next problem object
//***********************************************************************

int osrc_next_fsck(object_service_fn_t *os, os_fsck_iter_t *oit, char **bad_fname, int *bad_atype)
{
  osrc_fsck_iter_t *it = (osrc_fsck_iter_t *)oit;
  int n, err, fsck_err;

  *bad_fname = NULL;
  *bad_atype = 0;
  if (it->finished == 1) return(OS_FSCK_FINISHED);

 //** Read the bad fname len
  n = mq_stream_read_varint(it->mqs, &err);
  if (err != 0) {
     log_printf(5, "ERROR reading key len!\n");
     it->finished = 1;
     return(OS_FSCK_ERROR);
  }

  //** Check if Last bad object. If so return
  if (n <= 0) {
     log_printf(5, "Finished No more bad objects found\n");
     it->finished = 1;
     return(OS_FSCK_FINISHED);
  }

  //** Valid object name so read it
  type_malloc(*bad_fname, char, n+1);  (*bad_fname)[n] = 0;
  err = mq_stream_read(it->mqs, *bad_fname, n);
  if (err != 0) {
     log_printf(5, "ERROR reading key!");
     free(*bad_fname); *bad_fname = NULL;
     it->finished = 1;
     return(OS_FSCK_ERROR);
  }

  //** Read the object type
  *bad_atype = mq_stream_read_varint(it->mqs, &err);
  if (err != 0) {
     log_printf(5, "ERROR reading object type!");
     it->finished = 1;
     return(OS_FSCK_ERROR);
  }

  //** Read the  FSCK error
  fsck_err = mq_stream_read_varint(it->mqs, &err);
  if (err != 0) {
     log_printf(5, "ERROR reading FSCK error!");
     it->finished = 1;
     return(OS_FSCK_ERROR);
  }

  return(fsck_err);
}


//***********************************************************************
// osrc_response_fsck_iter - Handles the create_fsck_iter() response
//***********************************************************************

op_status_t osrc_response_fsck_iter(void *task_arg, int tid)
{
  mq_task_t *task = (mq_task_t *)task_arg;
  osrc_fsck_iter_t *it = (osrc_fsck_iter_t *)task->arg;
  osrc_priv_t *osrc = (osrc_priv_t *)it->os->priv;
  op_status_t status;
  int err;

log_printf(5, "START\n");

  status = op_success_status;

  //** Parse the response
  mq_remove_header(task->response, 1);

  it->mqs = mq_stream_read_create(osrc->mqc, osrc->ongoing, osrc->host_id, osrc->host_id_len, mq_msg_first(task->response), osrc->remote_host, osrc->stream_timeout);

  //** Parse the status
  status.op_status = mq_stream_read_varint(it->mqs, &err);
log_printf(15, "op_status=%d\n", status.op_status);
  status.error_code = mq_stream_read_varint(it->mqs, &err);
log_printf(15, "error_code%d\n", status.error_code);

  if (err != 0) { status.op_status= OP_STATE_FAILURE; }  //** Trigger a failure if error reading from the stream
  if (status.op_status == OP_STATE_FAILURE) {
     mq_stream_destroy(it->mqs);
  } else {
     //** Remove the response from the task to keep it from being freed.
     //** We'll do it manually
     it->response = task->response;
     task->response = NULL;
  }

log_printf(5, "END status=%d %d\n", status.op_status, status.error_code);

  return(status);
}


//***********************************************************************
// osrc_create_fsck_iter - Creates an fsck iterator
//***********************************************************************

os_fsck_iter_t *osrc_create_fsck_iter(object_service_fn_t *os, creds_t *creds, char *path, int mode)
{
  osrc_priv_t *osrc = (osrc_priv_t *)os->priv;
  osrc_fsck_iter_t *it;
  int err, n;
  unsigned char buf[16];
  mq_msg_t *msg;
  op_generic_t *gop;

log_printf(5, "START\n");

  //** Form the message
  msg = mq_make_exec_core_msg(osrc->remote_host, 1);
  mq_msg_append_mem(msg, OSR_FSCK_ITER_KEY, OSR_FSCK_ITER_SIZE, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, osrc->host_id, strlen(osrc->host_id)+1, MQF_MSG_KEEP_DATA);
  osrc_add_creds(os, creds, msg);
  mq_msg_append_mem(msg, path, strlen(path), MQF_MSG_KEEP_DATA);

  n = zigzag_encode(mode, buf);
  n += zigzag_encode(osrc->timeout, &(buf[n]));
  mq_msg_append_mem(msg, buf, n, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

  //** Make the iterator handle
  type_malloc_clear(it, osrc_fsck_iter_t, 1);
  it->os = os;
  it->mode = mode;

  //** Make the gop and execute it
  gop = new_mq_op(osrc->mqc, msg, osrc_response_fsck_iter, it, NULL, osrc->timeout);
  err = gop_waitall(gop);
  if (err != OP_STATE_SUCCESS) {
     log_printf(5, "ERROR status=%d\n", err);
     free(it);
     return(NULL);
  }
  gop_free(gop, OP_DESTROY);

  return(it);
}


//***********************************************************************
// osrc_destroy_fsck_iter - Destroys an fsck iterator
//***********************************************************************

void osrc_destroy_fsck_iter(object_service_fn_t *os, os_fsck_iter_t *oit)
{
  osrc_fsck_iter_t *it = (osrc_fsck_iter_t *)oit;

  if (it->mqs != NULL) mq_stream_destroy(it->mqs);
  if (it->response != NULL) mq_msg_destroy(it->response);

  free(it);
}


//***********************************************************************
// osrc_cred_init - Intialize a set of credentials
//***********************************************************************

creds_t *osrc_cred_init(object_service_fn_t *os, int type, void **args)
{
  osrc_priv_t *osrc = (osrc_priv_t *)os->priv;
  creds_t *creds;

  if (osrc->os_temp == NULL) {
     creds = authn_cred_init(osrc->authn, type, args);

     //** Right now this is filled with dummy routines until we get an official authn/authz implementation
     an_cred_set_id(creds, args[1]);

     return(creds);
  }

  return(os_cred_init(osrc->os_temp, type, args));
}

//***********************************************************************
// osrc_cred_destroy - Destroys a set ot credentials
//***********************************************************************

void osrc_cred_destroy(object_service_fn_t *os, creds_t *creds)
{
  osrc_priv_t *osrc = (osrc_priv_t *)os->priv;

  if (osrc->os_temp == NULL) {
     an_cred_destroy(creds);
     return;
  }

  return(os_cred_destroy(osrc->os_temp, creds));
}


//***********************************************************************
// os_remote_client_destroy
//***********************************************************************

void osrc_destroy(object_service_fn_t *os)
{
  osrc_priv_t *osrc = (osrc_priv_t *)os->priv;

  if (osrc->os_remote != NULL) {
     os_destroy(osrc->os_remote);
  }

//  osaz_destroy(osrc->osaz);
  if (osrc->authn != NULL) authn_destroy(osrc->authn);

//  apr_pool_destroy(osrc->mpool);

  free(osrc->host_id);
  free(osrc->remote_host);
  free(osrc);
  free(os);
}


//***********************************************************************
//  object_service_remote_client_create - Creates a remote client OS
//***********************************************************************

object_service_fn_t *object_service_remote_client_create(service_manager_t *ess, inip_file_t *fd, char *section)
{
  object_service_fn_t *os;
  osrc_priv_t *osrc;
  unsigned int n;
  char *str, *asection, *atype;
  char hostname[1024], buffer[1024];
  authn_create_t *authn_create;

log_printf(10, "START\n");
  if (section == NULL) section = "os_remote_client";

  type_malloc_clear(os, object_service_fn_t, 1);
  type_malloc_clear(osrc, osrc_priv_t, 1);
  os->priv = (void *)osrc;

  str = inip_get_string(fd, section, "os_temp", NULL);
  if (str != NULL) {  //** Running in test/temp
     log_printf(0, "NOTE: Running in debug mode by loading Remote server locally!\n");
     osrc->os_remote = object_service_remote_server_create(ess, fd, str);
     assert(osrc->os_remote != NULL);
     osrc->os_temp = ((osrs_priv_t *)(osrc->os_remote->priv))->os_child;
     free(str);
  } else {
     asection = inip_get_string(fd, section, "authn", NULL);
     atype = (asection == NULL) ? strdup(AUTHN_TYPE_FAKE) : inip_get_string(fd, asection, "type", AUTHN_TYPE_FAKE);
     authn_create = lookup_service(ess, AUTHN_AVAILABLE, atype);
     osrc->authn = (*authn_create)(ess, fd, asection);
     free(atype); free(asection);
  }

  osrc->timeout = inip_get_integer(fd, section, "timeout", 60);
  osrc->heartbeat = inip_get_integer(fd, section, "heartbeat", 600);
  osrc->remote_host = inip_get_string(fd, section, "remote_address", NULL);
  osrc->max_stream = inip_get_integer(fd, section, "max_stream", 1024*1024);
  osrc->stream_timeout = inip_get_integer(fd, section, "stream_timeout", 65);
  osrc->spin_interval = inip_get_integer(fd, section, "spin_interval", 1);
  osrc->spin_fail = inip_get_integer(fd, section, "spin_fail", 4);

  apr_pool_create(&osrc->mpool, NULL);
  apr_thread_mutex_create(&(osrc->lock), APR_THREAD_MUTEX_DEFAULT, osrc->mpool);
  apr_thread_cond_create(&(osrc->cond), osrc->mpool);
  apr_gethostname(hostname, sizeof(hostname), osrc->mpool);
  n = 0;
  get_random(&n, sizeof(n));
  snprintf(buffer, sizeof(buffer), "%d:%s:%u", osrc->heartbeat, hostname, n);
  osrc->host_id = strdup(buffer);
  osrc->host_id_len = strlen(osrc->host_id)+1;

  log_printf(1, "My host_id=%s\n", osrc->host_id);

  //** Get the MQC
  assert((osrc->mqc = lookup_service(ess, ESS_RUNNING, ESS_MQ)) != NULL);

  //** Get the Global ongoing handle
  assert((osrc->ongoing = lookup_service(ess, ESS_RUNNING, ESS_ONGOING_CLIENT)) != NULL);

  //** Get the thread pool to use
  assert((osrc->tpc = lookup_service(ess, ESS_RUNNING, ESS_TPC_UNLIMITED)) != NULL);

  //** Set up the fn ptrs
  os->type = OS_TYPE_REMOTE_CLIENT;

  os->destroy_service = osrc_destroy;
  os->cred_init = osrc_cred_init;
  os->cred_destroy = osrc_cred_destroy;
  os->exists = osrc_exists;//DONE
  os->create_object = osrc_create_object;//DONE
  os->remove_object = osrc_remove_object;//DONE
  os->remove_regex_object = osrc_remove_regex_object;//DONE
  os->abort_remove_regex_object = osrc_abort_remove_regex_object;//DONE
  os->move_object = osrc_move_object;//DONE
  os->symlink_object = osrc_symlink_object;//DONE
  os->hardlink_object = osrc_hardlink_object;//DONE
  os->create_object_iter = osrc_create_object_iter;
  os->create_object_iter_alist = osrc_create_object_iter_alist;//DONE
  os->next_object = osrc_next_object;//1/2 DONE
  os->destroy_object_iter = osrc_destroy_object_iter;//1/2 DONE
  os->open_object = osrc_open_object;//DONE
  os->close_object = osrc_close_object;//DONE
  os->abort_open_object = osrc_abort_open_object;//DONE
  os->get_attr = osrc_get_attr;//DONE
  os->set_attr = osrc_set_attr;//DONE
  os->symlink_attr = osrc_symlink_attr;//DONE
  os->copy_attr = osrc_copy_attr;//DONE
  os->get_multiple_attrs = osrc_get_multiple_attrs;//DONE
  os->set_multiple_attrs = osrc_set_multiple_attrs;//DONE
  os->copy_multiple_attrs = osrc_copy_multiple_attrs;//DONE
  os->symlink_multiple_attrs = osrc_symlink_multiple_attrs;//DONE
  os->move_attr = osrc_move_attr;//DONE
  os->move_multiple_attrs = osrc_move_multiple_attrs;//DONE
  os->regex_object_set_multiple_attrs = osrc_regex_object_set_multiple_attrs;//DONE
  os->abort_regex_object_set_multiple_attrs = osrc_abort_regex_object_set_multiple_attrs;
  os->create_attr_iter = osrc_create_attr_iter;
  os->next_attr = osrc_next_attr;
  os->destroy_attr_iter = osrc_destroy_attr_iter;

  os->create_fsck_iter = osrc_create_fsck_iter;
  os->destroy_fsck_iter = osrc_destroy_fsck_iter;
  os->next_fsck = osrc_next_fsck;
  os->fsck_object = osrc_fsck_object;

log_printf(10, "END\n");

  return(os);
}

