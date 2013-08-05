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
// Remote OS implementation for the Server side
//***********************************************************************

#define _log_module_index 214

#include "ex3_system.h"
#include "object_service_abstract.h"
#include "os_file.h"
#include "type_malloc.h"
#include "log.h"
#include "atomic_counter.h"
#include "thread_pool.h"
#include "os_remote.h"
#include "os_remote_priv.h"
#include "append_printf.h"
#include "mq_helpers.h"
#include "varint.h"
#include "string_token.h"
#include "mq_stream.h"
#include "authn_fake.h"

#define FIXME_SIZE 1024*1024

typedef struct {
  char *handle;
  apr_ssize_t handle_len;
  op_generic_t *gop;
} osrs_abort_handle_t;

//***********************************************************************
// osrs_get_creds - Retreives the creds from the message
//***********************************************************************

creds_t *osrs_get_creds(object_service_fn_t *os, mq_frame_t *f)
{
  osrs_priv_t *osrs = (osrs_priv_t *)os->priv;

  return(osrs->dummy_creds);
}

//***********************************************************************
// osrs_add_abort_handle - Installs the provided handle into the table
//    to allow an abort.
//***********************************************************************

void osrs_add_abort_handle(object_service_fn_t *os, osrs_abort_handle_t *ah)
{
  osrs_priv_t *osrs = (osrs_priv_t *)os->priv;

  apr_thread_mutex_lock(osrs->abort_lock);
  apr_hash_set(osrs->abort, ah->handle, ah->handle_len, ah);
  apr_thread_mutex_unlock(osrs->abort_lock);
}


//***********************************************************************
// osrs_remove_abort_handle - Removes the provided handle from the abort table
//***********************************************************************

void osrs_remove_abort_handle(object_service_fn_t *os, osrs_abort_handle_t *ah)
{
  osrs_priv_t *osrs = (osrs_priv_t *)os->priv;

  apr_thread_mutex_lock(osrs->abort_lock);
  apr_hash_set(osrs->abort, ah->handle, ah->handle_len, NULL);
  apr_thread_mutex_unlock(osrs->abort_lock);
}

//***********************************************************************
// osrs_perform_abort_handle - Performs the actual abort
//***********************************************************************

op_status_t osrs_perform_abort_handle(object_service_fn_t *os, char *handle, int handle_len)
{
  osrs_priv_t *osrs = (osrs_priv_t *)os->priv;
  osrs_abort_handle_t *ah;
  op_generic_t *gop;
  op_status_t status;

  status = op_failure_status;

  apr_thread_mutex_lock(osrs->abort_lock);
  ah = apr_hash_get(osrs->abort, handle, handle_len);

  if (ah != NULL) {
     gop = os_abort_open_object(osrs->os_child, ah->gop);
     gop_waitall(gop);
     status = gop_get_status(gop);
     gop_free(gop, OP_DESTROY);
  }
  apr_thread_mutex_unlock(osrs->abort_lock);

  return(status);
}

//***********************************************************************
// osrs_exists_cb - Processes the object exists command
//***********************************************************************

void osrs_exists_cb(void *arg, mq_task_t *task)
{
  object_service_fn_t *os = (object_service_fn_t *)arg;
  osrs_priv_t *osrs = (osrs_priv_t *)os->priv;
  mq_frame_t *fid, *fname, *fcred;
  char *name;
  creds_t *creds;
  int fsize;
  mq_msg_t *msg, *response;
  op_generic_t *gop;
  op_status_t status;

  log_printf(5, "Processing incoming request\n");

  //** Parse the command. Don't have to
  msg = task->msg;
  mq_remove_header(msg, 0);

  fid = mq_msg_pop(msg);  //** This is the ID
  mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

  fcred = mq_msg_pop(msg);  //** This has the creds
  creds = osrs_get_creds(os, fcred);

  fname = mq_msg_pop(msg);  //** This has the filename
  mq_get_frame(fname, (void **)&name, &fsize);

  if (creds != NULL) {
     gop = os_exists(osrs->os_child, creds, name);
     gop_waitall(gop);
     status = gop_get_status(gop);
     gop_free(gop, OP_DESTROY);
  } else {
     status = op_failure_status;
  }

  mq_frame_destroy(fname);
  mq_frame_destroy(fcred);

  //** Form the response
  response = mq_make_response_core_msg(msg, fid);
  mq_msg_append_frame(response, mq_make_status_frame(status));
  mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

  //** Lastly send it
  mq_submit(osrs->server_portal, mq_task_new(osrs->mqc, response, NULL, NULL, 30));

  log_printf(5, "END\n");

}

//***********************************************************************
// osrs_create_object_cb - Processes the create object command
//***********************************************************************

void osrs_create_object_cb(void *arg, mq_task_t *task)
{
  object_service_fn_t *os = (object_service_fn_t *)arg;
  osrs_priv_t *osrs = (osrs_priv_t *)os->priv;
  mq_frame_t *fid, *fname, *fcred, *f;
  char *name;
  char *data;
  creds_t *creds;
  int fsize, nbytes;
  mq_msg_t *msg, *response;
  op_generic_t *gop;
  op_status_t status;
  int64_t ftype;

  log_printf(5, "Processing incoming request\n");

  //** Parse the command.
  msg = task->msg;
  mq_remove_header(msg, 0);

  fid = mq_msg_pop(msg);  //** This is the ID
  mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

  fcred = mq_msg_pop(msg);  //** This has the creds
  creds = osrs_get_creds(os, fcred);

  fname = mq_msg_pop(msg);  //** This has the filename
  mq_get_frame(fname, (void **)&name, &fsize);

  f = mq_msg_pop(msg);  //** This has the Object type
  mq_get_frame(f, (void **)&data, &nbytes);
  zigzag_decode((unsigned char *)data, nbytes, &ftype);
//log_printf(5, "ftype=%d\n", ftype);
  mq_frame_destroy(f);

  f = mq_msg_pop(msg);  //** This has the ID used for the create attribute
  mq_get_frame(f, (void **)&data, &nbytes);

  if (creds != NULL) {
     gop = os_create_object(osrs->os_child, creds, name, ftype, data);
     gop_waitall(gop);
     status = gop_get_status(gop);
     gop_free(gop, OP_DESTROY);
  } else {
     status = op_failure_status;
  }

  mq_frame_destroy(fname);
  mq_frame_destroy(fcred);
  mq_frame_destroy(f);

  //** Form the response
  response = mq_make_response_core_msg(msg, fid);
  mq_msg_append_frame(response, mq_make_status_frame(status));
  mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

  //** Lastly send it
  mq_submit(osrs->server_portal, mq_task_new(osrs->mqc, response, NULL, NULL, 30));

  log_printf(5, "END\n");
}

//***********************************************************************
// osrs_remove_object_cb - Processes the object remove command
//***********************************************************************

void osrs_remove_object_cb(void *arg, mq_task_t *task)
{
  object_service_fn_t *os = (object_service_fn_t *)arg;
  osrs_priv_t *osrs = (osrs_priv_t *)os->priv;
  mq_frame_t *fid, *fname, *fcred;
  char *name;
  creds_t *creds;
  int fsize;
  mq_msg_t *msg, *response;
  op_generic_t *gop;
  op_status_t status;

  log_printf(5, "Processing incoming request\n");

  //** Parse the command.
  msg = task->msg;
  mq_remove_header(msg, 0);

  fid = mq_msg_pop(msg);  //** This is the ID
  mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

  fcred = mq_msg_pop(msg);  //** This has the creds
  creds = osrs_get_creds(os, fcred);

  fname = mq_msg_pop(msg);  //** This has the filename
  mq_get_frame(fname, (void **)&name, &fsize);

  if (creds != NULL) {
    gop = os_remove_object(osrs->os_child, creds, name);
    gop_waitall(gop);
    status = gop_get_status(gop);
    gop_free(gop, OP_DESTROY);
  } else {
     status = op_failure_status;
  }

  mq_frame_destroy(fname);
  mq_frame_destroy(fcred);

  //** Form the response
  response = mq_make_response_core_msg(msg, fid);
  mq_msg_append_frame(response, mq_make_status_frame(status));
  mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

  //** Lastly send it
  mq_submit(osrs->server_portal, mq_task_new(osrs->mqc, response, NULL, NULL, 30));
}

//***********************************************************************
// osrs_remove_regex_object_cb - Processes the regex based object remove command
//***********************************************************************

void osrs_remove_regex_object_cb(void *arg, mq_task_t *task)
{
  object_service_fn_t *os = (object_service_fn_t *)arg;
  osrs_priv_t *osrs = (osrs_priv_t *)os->priv;
  mq_frame_t *fid, *fcred, *fdata, *hid;
  unsigned char *buffer;
  unsigned char tbuf[32];
  os_regex_table_t *path, *object_regex;
  creds_t *creds;
  int fsize, bpos, n;
  int64_t recurse_depth, obj_types, timeout;
  mq_msg_t *msg;
  op_generic_t *gop;
  mq_stream_t *mqs;
  op_status_t status;

  log_printf(5, "Processing incoming request\n");

  status = op_failure_status;

  //** Parse the command.
  msg = task->msg;
  mq_remove_header(msg, 0);

  fid = mq_msg_pop(msg);  //** This is the ID
  mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame
  hid = mq_msg_pop(msg);  //** This is the Host ID

  fcred = mq_msg_pop(msg);  //** This has the creds
  creds = osrs_get_creds(os, fcred);

  fdata = mq_msg_pop(msg);  //** This has the data
  mq_get_frame(fdata, (void **)&buffer, &fsize);

  //** Create the stream so we can get the heartbeating while we work
  mqs = mq_stream_write_create(osrs->mqc, osrs->server_portal, osrs->ongoing, MQS_PACK_COMPRESS, osrs->max_stream, timeout, msg, fid, hid, 0);

  //** Parse the buffer
  path = NULL;
  object_regex = NULL;
  bpos = 0;

  n = zigzag_decode(&(buffer[bpos]), fsize-bpos, &timeout);
  if (n < 0) { timeout = 60; goto fail; }
  bpos += n;

  n = zigzag_decode(&(buffer[bpos]), fsize-bpos, &recurse_depth);
  if (n < 0) goto fail;
  bpos += n;

  n = zigzag_decode(&(buffer[bpos]), fsize-bpos, &obj_types);
  if (n < 0) goto fail;
  bpos += n;

  path = os_regex_table_unpack(&(buffer[bpos]), fsize-bpos, &n);
  if (n == 0) goto fail;
  bpos += n;

  object_regex = os_regex_table_unpack(&(buffer[bpos]), fsize-bpos, &n);
  if (n == 0) goto fail;
  bpos += n;

  //** run the task
  if (creds != NULL) {
    gop = os_remove_regex_object(osrs->os_child, creds, path, object_regex, obj_types, recurse_depth);
    gop_waitall(gop);
    status = gop_get_status(gop);
    gop_free(gop, OP_DESTROY);
  } else {
     status = op_failure_status;
  }

fail:
  mq_frame_destroy(fdata);
  mq_frame_destroy(fcred);

  if (path != NULL) os_regex_table_destroy(path);
  if (object_regex != NULL) os_regex_table_destroy(object_regex);

  //** Send the response
  n = zigzag_encode(status.op_status, tbuf);
  n = n + zigzag_encode(status.error_code, &(tbuf[n]));
  mq_stream_write(mqs, tbuf, n);
  mq_stream_destroy(mqs);
}

//***********************************************************************
// osrs_symlink_object_cb - Processes the symlink object command
//***********************************************************************

void osrs_symlink_object_cb(void *arg, mq_task_t *task)
{
  object_service_fn_t *os = (object_service_fn_t *)arg;
  osrs_priv_t *osrs = (osrs_priv_t *)os->priv;
  mq_frame_t *fid, *fsname, *fdname, *fcred, *fuserid;
  char *src_name, *dest_name, *userid;
  creds_t *creds;
  int fsize;
  mq_msg_t *msg, *response;
  op_generic_t *gop;
  op_status_t status;

  log_printf(5, "Processing incoming request\n");

  //** Parse the command. Don't have to
  msg = task->msg;
  mq_remove_header(msg, 0);

  fid = mq_msg_pop(msg);  //** This is the ID
  mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

  fcred = mq_msg_pop(msg);  //** This has the creds
  creds = osrs_get_creds(os, fcred);

  fsname = mq_msg_pop(msg);  //** Source file
  mq_get_frame(fsname, (void **)&src_name, &fsize);

  fdname = mq_msg_pop(msg);  //** Destination file
  mq_get_frame(fdname, (void **)&dest_name, &fsize);

  fuserid = mq_msg_pop(msg);  //** User ID
  mq_get_frame(fuserid, (void **)&userid, &fsize);

  if (creds != NULL) {
     gop = os_symlink_object(osrs->os_child, creds, src_name, dest_name, userid);
     gop_waitall(gop);
     status = gop_get_status(gop);
     gop_free(gop, OP_DESTROY);
  } else {
     status = op_failure_status;
  }

  mq_frame_destroy(fsname);
  mq_frame_destroy(fdname);
  mq_frame_destroy(fuserid);
  mq_frame_destroy(fcred);

  //** Form the response
  response = mq_make_response_core_msg(msg, fid);
  mq_msg_append_frame(response, mq_make_status_frame(status));
  mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

  //** Lastly send it
  mq_submit(osrs->server_portal, mq_task_new(osrs->mqc, response, NULL, NULL, 30));
}

//***********************************************************************
// osrs_hardlink_object_cb - Processes the hard link object command
//***********************************************************************

void osrs_hardlink_object_cb(void *arg, mq_task_t *task)
{
  object_service_fn_t *os = (object_service_fn_t *)arg;
  osrs_priv_t *osrs = (osrs_priv_t *)os->priv;
  mq_frame_t *fid, *fsname, *fdname, *fcred, *fuserid;
  char *src_name, *dest_name, *userid;
  creds_t *creds;
  int fsize;
  mq_msg_t *msg, *response;
  op_generic_t *gop;
  op_status_t status;

  log_printf(5, "Processing incoming request\n");

  //** Parse the command. Don't have to
  msg = task->msg;
  mq_remove_header(msg, 0);

  fid = mq_msg_pop(msg);  //** This is the ID
  mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

  fcred = mq_msg_pop(msg);  //** This has the creds
  creds = osrs_get_creds(os, fcred);

  fsname = mq_msg_pop(msg);  //** Source file
  mq_get_frame(fsname, (void **)&src_name, &fsize);

  fdname = mq_msg_pop(msg);  //** Destination file
  mq_get_frame(fdname, (void **)&dest_name, &fsize);

  fuserid = mq_msg_pop(msg);  //** User ID
  mq_get_frame(fuserid, (void **)&userid, &fsize);

  if (creds != NULL) {
     gop = os_hardlink_object(osrs->os_child, creds, src_name, dest_name, userid);
     gop_waitall(gop);
     status = gop_get_status(gop);
     gop_free(gop, OP_DESTROY);
  } else {
     status = op_failure_status;
  }

  mq_frame_destroy(fsname);
  mq_frame_destroy(fdname);
  mq_frame_destroy(fuserid);
  mq_frame_destroy(fcred);

  //** Form the response
  response = mq_make_response_core_msg(msg, fid);
  mq_msg_append_frame(response, mq_make_status_frame(status));
  mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

  //** Lastly send it
  mq_submit(osrs->server_portal, mq_task_new(osrs->mqc, response, NULL, NULL, 30));
}

//***********************************************************************
// osrs_move_object_cb - Processes the move object command
//***********************************************************************

void osrs_move_object_cb(void *arg, mq_task_t *task)
{
  object_service_fn_t *os = (object_service_fn_t *)arg;
  osrs_priv_t *osrs = (osrs_priv_t *)os->priv;
  mq_frame_t *fid, *fsname, *fdname, *fcred;
  char *src_name, *dest_name;
  creds_t *creds;
  int fsize;
  mq_msg_t *msg, *response;
  op_generic_t *gop;
  op_status_t status;

  log_printf(5, "Processing incoming request\n");

  //** Parse the command. Don't have to
  msg = task->msg;
  mq_remove_header(msg, 0);

  fid = mq_msg_pop(msg);  //** This is the ID
  mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

  fcred = mq_msg_pop(msg);  //** This has the creds
  creds = osrs_get_creds(os, fcred);

  fsname = mq_msg_pop(msg);  //** Source file
  mq_get_frame(fsname, (void **)&src_name, &fsize);

  fdname = mq_msg_pop(msg);  //** Destination file
  mq_get_frame(fdname, (void **)&dest_name, &fsize);

  if (creds != NULL) {
     gop = os_move_object(osrs->os_child, creds, src_name, dest_name);
     gop_waitall(gop);
     status = gop_get_status(gop);
     gop_free(gop, OP_DESTROY);
  } else {
     status = op_failure_status;
  }

  mq_frame_destroy(fsname);
  mq_frame_destroy(fdname);
  mq_frame_destroy(fcred);

  //** Form the response
  response = mq_make_response_core_msg(msg, fid);
  mq_msg_append_frame(response, mq_make_status_frame(status));
  mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

  //** Lastly send it
  mq_submit(osrs->server_portal, mq_task_new(osrs->mqc, response, NULL, NULL, 30));
}

//***********************************************************************
// osrs_open_object_cb - Processes the object open command
//***********************************************************************

void osrs_open_object_cb(void *arg, mq_task_t *task)
{
  object_service_fn_t *os = (object_service_fn_t *)arg;
  osrs_priv_t *osrs = (osrs_priv_t *)os->priv;
  mq_frame_t *fid, *fsname, *fmode, *fcred, *fhandle, *fhb, *fuid;
  char *src_name, *id, *handle;
  osrs_abort_handle_t ah;
  mq_ongoing_object_t *oo;
  unsigned char *data;
  creds_t *creds;
  int fsize, handle_len, n;
  int64_t mode, max_wait;
  mq_msg_t *msg, *response;
  op_status_t status;
  os_fd_t *fd;

  log_printf(5, "Processing incoming request\n");

  //** Parse the command. Don't have to
  msg = task->msg;
  mq_remove_header(msg, 0);

  fid = mq_msg_pop(msg);  //** This is the ID
  mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

  fcred = mq_msg_pop(msg);  //** This has the creds
  creds = osrs_get_creds(os, fcred);

  fuid = mq_msg_pop(msg);  //** User ID for storing in lock attribute
  mq_get_frame(fuid, (void **)&id, &fsize);

  fsname = mq_msg_pop(msg);  //** Source file
  mq_get_frame(fsname, (void **)&src_name, &fsize);

  fmode = mq_msg_pop(msg);  //** Mode and max wait
  mq_get_frame(fmode, (void **)&data, &fsize);
  n = zigzag_decode(data, fsize, &mode);
  zigzag_decode(&(data[n]), fsize, &max_wait);
log_printf(5, "fname=%s mode=%d max_wait=%d\n", src_name, mode, max_wait);

  fhb = mq_msg_pop(msg);  //** Heartbeat frame on success
  fhandle = mq_msg_pop(msg);  //** Handle for aborts
  if (creds != NULL) {
     mq_get_frame(fhandle, (void **)&(ah.handle), &n);
     ah.handle_len = n;
     ah.gop = os_open_object(osrs->os_child, creds, src_name, mode, id, &fd, max_wait);
     osrs_add_abort_handle(os, &ah);  //** Add us to the abort list

     gop_waitall(ah.gop);

     osrs_remove_abort_handle(os, &ah);  //** Can remove us now since finished

     status = gop_get_status(ah.gop);
     gop_free(ah.gop, OP_DESTROY);
  } else {
     status = op_failure_status;
  }

  //** Form the response
  response = mq_make_response_core_msg(msg, fid);
  mq_msg_append_frame(response, mq_make_status_frame(status));

  //** On success add us to the ongoing monitor thread and return the handle
  if (status.op_status == OP_STATE_SUCCESS) {
handle = NULL;
     mq_get_frame(fhb, (void **)&handle, &handle_len);
log_printf(5, "handle=%s\n", handle);
log_printf(5, "handle_len=%d\n", handle_len);
     oo = mq_ongoing_add(osrs->ongoing, handle, handle_len, (void *)fd, (mq_ongoing_fail_t *)osrs->os_child->close_object, osrs->os_child);
n=sizeof(intptr_t);
log_printf(5, "PTR key=%" PRIdPTR " len=%d\n", oo->key, n);
     mq_msg_append_mem(response, &(oo->key), sizeof(intptr_t), MQF_MSG_KEEP_DATA);
  }

  //** Do some house cleaning
  mq_frame_destroy(fhb);
  mq_frame_destroy(fsname);
  mq_frame_destroy(fcred);
  mq_frame_destroy(fuid);
  mq_frame_destroy(fmode);
  mq_frame_destroy(fhandle);

  mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

  //** Lastly send it
  mq_submit(osrs->server_portal, mq_task_new(osrs->mqc, response, NULL, NULL, 30));
}

//***********************************************************************
// osrs_close_object_cb - Processes an object close
//***********************************************************************

void osrs_close_object_cb(void *arg, mq_task_t *task)
{
  object_service_fn_t *os = (object_service_fn_t *)arg;
  osrs_priv_t *osrs = (osrs_priv_t *)os->priv;
  op_generic_t *gop;
  mq_frame_t *fid, *fuid, *fhid;
  char *id, *fhandle;
  void *handle;
  int fsize, hsize;
  intptr_t key;
  mq_msg_t *msg, *response;
  op_status_t status;

  log_printf(5, "Processing incoming request\n");

  //** Parse the command.
  msg = task->msg;
  mq_remove_header(msg, 0);

  fid = mq_msg_pop(msg);  //** This is the ID for responses
  mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

  fuid = mq_msg_pop(msg);  //** Host/user ID
  mq_get_frame(fuid, (void **)&id, &fsize);

  fhid = mq_msg_pop(msg);  //** Host handle
  mq_get_frame(fhid, (void **)&fhandle, &hsize);
  assert(hsize == sizeof(intptr_t));

  key = *(intptr_t *)fhandle;
log_printf(5, "PTR key=%" PRIdPTR "\n", key);

  //** Do the host lookup
  if ((handle = mq_ongoing_remove(osrs->ongoing, id, fsize, key)) != NULL) {
     log_printf(6, "Found handle\n");

     gop = os_close_object(osrs->os_child, handle);
     gop_waitall(gop);
     status = gop_get_status(gop);
     gop_free(gop, OP_DESTROY);
  } else {
    log_printf(6, "ERROR missing host=%s\n", id);
    status = op_failure_status;
  }

  mq_frame_destroy(fhid);
  mq_frame_destroy(fuid);

  //** Form the response
  response = mq_make_response_core_msg(msg, fid);
  mq_msg_append_frame(response, mq_make_status_frame(status));
  mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

  //** Lastly send it
  mq_submit(osrs->server_portal, mq_task_new(osrs->mqc, response, NULL, NULL, 30));
}

//***********************************************************************
// osrs_abort_open_object_cb - Aborts a pending open object call
//***********************************************************************

void osrs_abort_open_object_cb(void *arg, mq_task_t *task)
{
  object_service_fn_t *os = (object_service_fn_t *)arg;
  osrs_priv_t *osrs = (osrs_priv_t *)os->priv;
  mq_frame_t *fid, *fuid;
  char *id;
  int fsize;
  mq_msg_t *msg, *response;
  op_status_t status;

  log_printf(5, "Processing incoming request\n");

  //** Parse the command.
  msg = task->msg;
  mq_remove_header(msg, 0);

  fid = mq_msg_pop(msg);  //** This is the ID for responses
  mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

  fuid = mq_msg_pop(msg);  //** Host/user ID
  mq_get_frame(fuid, (void **)&id, &fsize);

  //** Perform the abort
  status = osrs_perform_abort_handle(os, id, fsize);

  mq_frame_destroy(fuid);

  //** Form the response
  response = mq_make_response_core_msg(msg, fid);
  mq_msg_append_frame(response, mq_make_status_frame(status));
  mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

  //** Lastly send it
  mq_submit(osrs->server_portal, mq_task_new(osrs->mqc, response, NULL, NULL, 30));
}

//***********************************************************************
// osrs_get_mult_attr_cb - Retrieves object attributes
//***********************************************************************

void osrs_get_mult_attr_cb(void *arg, mq_task_t *task)
{
  object_service_fn_t *os = (object_service_fn_t *)arg;
  osrs_priv_t *osrs = (osrs_priv_t *)os->priv;
  mq_frame_t *fid, *fuid, *fcred, *fdata, *ffd, *hid;
  creds_t *creds;
  char *id;
  unsigned char *data;
  op_generic_t *gop;
  int fsize, bpos, len, id_size, i;
  int64_t max_stream, timeout, n, v, nbytes;
  mq_msg_t *msg;
  mq_stream_t *mqs;
  op_status_t status;
  unsigned char buffer[32];
  char **key;
  void **val;
  int *v_size;
  os_fd_t *fd;
  intptr_t fd_key;

  log_printf(5, "Processing incoming request\n");

  mqs = NULL; key = NULL; val = NULL; v_size = NULL;

  //** Parse the command.
  msg = task->msg;
  mq_remove_header(msg, 0);

  fid = mq_msg_pop(msg);  //** This is the ID for responses
  mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame
  hid = mq_msg_pop(msg);  //** This is the Host ID for the ongoing stream

  fcred = mq_msg_pop(msg);  //** This has the creds
  creds = osrs_get_creds(os, fcred);

  fuid = mq_msg_pop(msg);  //** Host/user ID
  mq_get_frame(fuid, (void **)&id, &id_size);

  //** Get the fd handle
  ffd = mq_msg_pop(msg);  //** Host/user ID
  mq_get_frame(ffd, (void **)&data, &len);
  fd_key = *(intptr_t *)data;

log_printf(5, "PTR key=%" PRIdPTR " len=%d\n", fd_key, len);

  fdata = mq_msg_pop(msg);  //** attr list
  mq_get_frame(fdata, (void **)&data, &fsize);

log_printf(5, "PTR key=%" PRIdPTR " len=%d id=%s id_len=%d\n", fd_key, len, id, id_size);

  //** Now check if the handle is valid
  if ((fd = mq_ongoing_get(osrs->ongoing, (char *)id, id_size, fd_key)) == NULL) {
     log_printf(5, "Invalid handle!\n");
     goto fail_fd;
  }

  //** Parse the attr list
  i = zigzag_decode(data, fsize, &max_stream);
  if (i<0) goto fail_fd;
  if ((max_stream <= 0) || (max_stream > osrs->max_stream)) max_stream = osrs->max_stream;
  bpos = i;
  fsize -= i;

  i = zigzag_decode(&(data[bpos]), fsize, &timeout);
  if (i<0) goto fail_fd;
  if (timeout < 0) timeout = 10;
  bpos += i;
  fsize -= i;

  i = zigzag_decode(&(data[bpos]), fsize, &n);
  if ((i<0) || (n<=0)) goto fail_fd;
  bpos += i;
  fsize -= i;

log_printf(5, "max_stream=%d timeout=%d n=%d\n", max_stream, timeout, n);
  type_malloc_clear(key, char *, n);
  type_malloc_clear(val, void *, n);
  type_malloc(v_size, int, n);

  for (i=0; i<n; i++) {
     nbytes = zigzag_decode(&(data[bpos]), fsize, &v);
log_printf(5, "i=%d klen=" XOT " bpos=%d\n", i, v, bpos);

     if ((nbytes<0) || (v<=0)) goto fail;
     bpos += nbytes;
     fsize -= nbytes;

     type_malloc(key[i], char, v+1);
     if (v > fsize) goto fail;
     memcpy(key[i], &(data[bpos]), v);
     key[i][v] = 0;
     bpos += v;
     fsize -= nbytes;
log_printf(5, "i=%d key=%s bpos=%d\n", i, key[i], bpos);

     nbytes = zigzag_decode(&(data[bpos]), fsize, &v);
     if (nbytes<0) goto fail;
     bpos += nbytes;
     fsize -= nbytes;
     v_size[i] = -abs(v);
log_printf(5, "i=%d v_size=" XOT " bpos=%d\n", i, v, bpos);
  }

  //** Execute the get attribute call
  if (creds != NULL) {
     gop = os_get_multiple_attrs(osrs->os_child, creds, fd, key, val, v_size, n);
     gop_waitall(gop);
     status = gop_get_status(gop);
     gop_free(gop, OP_DESTROY);
  } else {
     status = op_failure_status;
  }

  //** Create the stream
  mqs = mq_stream_write_create(osrs->mqc, osrs->server_portal, osrs->ongoing, MQS_PACK_COMPRESS, max_stream, timeout, msg, fid, hid, 0);

  //** Return the results
  i = zigzag_encode(status.op_status, buffer);
  i = i + zigzag_encode(status.error_code, &(buffer[i]));
  mq_stream_write(mqs, buffer, i);

log_printf(5, "status.op_status=%d status.error_code=%d len=%d\n", status.op_status, status.error_code, i);
  if (status.op_status == OP_STATE_SUCCESS) {
     for (i=0; i<n; i++) {
        mq_stream_write_varint(mqs, v_size[i]);
        if (v_size[i] > 0) {mq_stream_write(mqs, val[i], v_size[i]); }
if (v_size[i] > 0) {
   log_printf(15, "val[%d]=%s\n", i, (char *)val[i]);
} else {
   log_printf(15, "val[%d]=NULL\n", i, NULL);
}
     }
  }

fail_fd:
fail:
  if (fd != NULL) mq_ongoing_release(osrs->ongoing, (char *)id, id_size, fd_key);

  mq_frame_destroy(ffd);
  mq_frame_destroy(fuid);
  mq_frame_destroy(fdata);
  mq_frame_destroy(fcred);

  if (mqs != NULL) {
     mq_stream_destroy(mqs);  //** This also flushes the data to the client
  } else {  //** there was an error processing the record
     log_printf(5, "ERROR status being returned!\n");
     mqs = mq_stream_write_create(osrs->mqc, osrs->server_portal, osrs->ongoing, MQS_PACK_RAW, 1024, 30, msg, fid, hid, 0);
     status = op_failure_status;
     i = zigzag_encode(status.op_status, buffer);
     i = i + zigzag_encode(status.error_code, &(buffer[i]));
     mq_stream_write(mqs, buffer, i);
     mq_stream_destroy(mqs);
  }

  if (key) {
    for (i=0; i<n; i++) if (key[i]) free(key[i]);
    free(key);
  }

  if (val) {
    for (i=0; i<n; i++) if (val[i]) free(val[i]);
    free(val);
  }

  if (v_size) free(v_size);
}

//***********************************************************************
// osrs_set_mult_attr_cb - Sets the given object attributes
//***********************************************************************

void osrs_set_mult_attr_cb(void *arg, mq_task_t *task)
{
  object_service_fn_t *os = (object_service_fn_t *)arg;
  osrs_priv_t *osrs = (osrs_priv_t *)os->priv;
  mq_frame_t *fid, *fuid, *fcred, *fdata, *ffd;
  creds_t *creds;
  char *id;
  unsigned char *data;
  op_generic_t *gop;
  int fsize, bpos, len, id_size, i;
  int64_t timeout, n, v, nbytes;
  mq_msg_t *msg, *response;
  op_status_t status;
  char **key;
  char **val;
  int *v_size;
  os_fd_t *fd;
  intptr_t fd_key;

  log_printf(5, "Processing incoming request\n");

  key = NULL; val = NULL; v_size = NULL;
  status = op_failure_status;

  //** Parse the command.
  msg = task->msg;
  mq_remove_header(msg, 0);

  fid = mq_msg_pop(msg);  //** This is the ID for responses
  mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

  fcred = mq_msg_pop(msg);  //** This has the creds
  creds = osrs_get_creds(os, fcred);

  fuid = mq_msg_pop(msg);  //** Host/user ID
  mq_get_frame(fuid, (void **)&id, &id_size);

  //** Get the fd handle
  ffd = mq_msg_pop(msg);  //** Host/user ID
  mq_get_frame(ffd, (void **)&data, &len);
  fd_key = *(intptr_t *)data;

log_printf(5, "PTR key=%" PRIdPTR " len=%d\n", fd_key, len);

  fdata = mq_msg_pop(msg);  //** attr list to set
  mq_get_frame(fdata, (void **)&data, &fsize);

log_printf(5, "PTR key=%" PRIdPTR " len=%d id=%s id_len=%d\n", fd_key, len, id, id_size);

  //** Now check if the handle is valid
  if ((fd = mq_ongoing_get(osrs->ongoing, (char *)id, id_size, fd_key)) == NULL) {
     log_printf(5, "Invalid handle!\n");
     goto fail_fd;
  }

  //** Parse the attr list
  bpos = 0;
  i = zigzag_decode(&(data[bpos]), fsize, &timeout);
  if (i<0) goto fail_fd;
  if (timeout < 0) timeout = 10;
  bpos += i;
  fsize -= i;

  i = zigzag_decode(&(data[bpos]), fsize, &n);
  if ((i<0) || (n<=0)) goto fail_fd;
  bpos += i;
  fsize -= i;

log_printf(5, "timeout=%d n=%d\n", timeout, n);
  type_malloc_clear(key, char *, n);
  type_malloc_clear(val, char *, n);
  type_malloc(v_size, int, n);

  for (i=0; i<n; i++) {
     nbytes = zigzag_decode(&(data[bpos]), fsize, &v);
log_printf(5, "i=%d klen=" XOT " bpos=%d\n", i, v, bpos);

     if ((nbytes<0) || (v<=0)) goto fail;
     bpos += nbytes;
     fsize -= nbytes;

     type_malloc(key[i], char, v+1);
     if (v > fsize) goto fail;
     memcpy(key[i], &(data[bpos]), v);
     key[i][v] = 0;
     bpos += v;
     fsize -= nbytes;
log_printf(5, "i=%d key=%s bpos=%d\n", i, key[i], bpos);

     nbytes = zigzag_decode(&(data[bpos]), fsize, &v);
log_printf(5, "i=%d klen=" XOT " bpos=%d\n", i, v, bpos);

     if (nbytes<0) goto fail;
     bpos += nbytes;
     fsize -= nbytes;

     v_size[i] = v;
     if (v > 0) {
        type_malloc(val[i], char, v+1);
        if (v > fsize) goto fail;
        memcpy(val[i], &(data[bpos]), v);
        val[i][v] = 0;
        bpos += v;
        fsize -= nbytes;
     } else {
        val[i] = NULL;
     }
log_printf(5, "i=%d val=%s bpos=%d\n", i, val[i], bpos);

log_printf(5, "i=%d v_size=%d bpos=%d\n", i, v_size[i], bpos);
  }

  //** Execute the get attribute call
  if (creds != NULL) {
     gop = os_set_multiple_attrs(osrs->os_child, creds, fd, key, (void **)val, v_size, n);
     gop_waitall(gop);
     status = gop_get_status(gop);
     gop_free(gop, OP_DESTROY);
  } else {
     status = op_failure_status;
  }

fail_fd:
fail:
  if (fd != NULL) mq_ongoing_release(osrs->ongoing, (char *)id, id_size, fd_key);

  mq_frame_destroy(ffd);
  mq_frame_destroy(fuid);
  mq_frame_destroy(fdata);
  mq_frame_destroy(fcred);


  //** Form the response
  response = mq_make_response_core_msg(msg, fid);
  mq_msg_append_frame(response, mq_make_status_frame(status));
  mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

log_printf(5, "status.op_status=%d\n", status.op_status);
  //** Lastly send it
  mq_submit(osrs->server_portal, mq_task_new(osrs->mqc, response, NULL, NULL, 30));

  if (key) {
    for (i=0; i<n; i++) if (key[i]) free(key[i]);
    free(key);
  }

  if (val) {
    for (i=0; i<n; i++) if (val[i]) free(val[i]);
    free(val);
  }

  if (v_size) free(v_size);
}

//***********************************************************************
// osrs_regex_set_set_mult_attr_cb - Processes the regex based object attrribute setting
//***********************************************************************

void osrs_regex_set_mult_attr_cb(void *arg, mq_task_t *task)
{
  object_service_fn_t *os = (object_service_fn_t *)arg;
  osrs_priv_t *osrs = (osrs_priv_t *)os->priv;
  mq_frame_t *fid, *fcred, *fdata, *fcid, *hid;
  unsigned char *buffer;
  unsigned char tbuf[32];
  int *v_size;
  char **key;
  char **val;
  char *call_id;
  os_regex_table_t *path, *object_regex;
  creds_t *creds;
  int fsize, bpos, n, i;
  int64_t recurse_depth, obj_types, timeout, n_attrs, len;
  mq_msg_t *msg;
  op_generic_t *gop;
  mq_stream_t *mqs;
  op_status_t status;

  log_printf(5, "Processing incoming request\n");

  status = op_failure_status;
  key = NULL; val = NULL, v_size = NULL; n_attrs = 0;

  //** Parse the command.
  msg = task->msg;
  mq_remove_header(msg, 0);

  fid = mq_msg_pop(msg);  //** This is the ID
  mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame
  hid = mq_msg_pop(msg);  //** This is the Host ID for the ongoing stream

  fcred = mq_msg_pop(msg);  //** This has the creds
  creds = osrs_get_creds(os, fcred);

  fcid = mq_msg_pop(msg);  //** This has the call ID
  mq_get_frame(fcid, (void **)&call_id, &fsize);

  fdata = mq_msg_pop(msg);  //** This has the data
  mq_get_frame(fdata, (void **)&buffer, &fsize);

  //** Create the stream so we can get the heartbeating while we work
  mqs = mq_stream_write_create(osrs->mqc, osrs->server_portal, osrs->ongoing, MQS_PACK_COMPRESS, osrs->max_stream, timeout, msg, fid, hid, 0);

  //** Parse the buffer
  path = NULL;
  object_regex = NULL;
  bpos = 0;

  n = zigzag_decode(&(buffer[bpos]), fsize-bpos, &timeout);
  if (n < 0) { timeout = 60; goto fail; }
  bpos += n;

  n = zigzag_decode(&(buffer[bpos]), fsize-bpos, &recurse_depth);
  if (n < 0) goto fail;
  bpos += n;

  n = zigzag_decode(&(buffer[bpos]), fsize-bpos, &obj_types);
  if (n < 0) goto fail;
  bpos += n;

  path = os_regex_table_unpack(&(buffer[bpos]), fsize-bpos, &n);
  if (n == 0) goto fail;
  bpos += n;

  object_regex = os_regex_table_unpack(&(buffer[bpos]), fsize-bpos, &n);
  if (n == 0) goto fail;
  bpos += n;

  n = zigzag_decode(&(buffer[bpos]), fsize-bpos, &n_attrs);
  if (n < 0) goto fail;
  bpos += n;

  type_malloc_clear(key, char *, n_attrs);
  type_malloc_clear(val, char *, n_attrs);
  type_malloc_clear(v_size, int, n_attrs);

  for (i=0; i<n_attrs; i++) {
     n = zigzag_decode(&(buffer[bpos]), fsize-bpos, &len);
     if (n < 0)  goto fail;
     bpos += n;

     if ((bpos+len) > fsize) goto fail;
     type_malloc(key[i], char, len+1);
     memcpy(key[i], &(buffer[bpos]), len);
     key[i][len] = 0;
     bpos += len;

     n = zigzag_decode(&(buffer[bpos]), fsize-bpos, &len);
     if (n < 0)  goto fail;
     bpos += n;

     v_size[i] = len;
     if ((len > 0) && ((bpos+len) > fsize)) goto fail;
     if (len > 0) {
        type_malloc(val[i], char, len+1);
        memcpy(val[i], &(buffer[bpos]), len);
        val[i][len] = 0;
     } else {
        val[i] = NULL;
     }
     bpos += len;
log_printf(15, "i=%d key=%s val=%s bpos=%d\n", i, key[i], val[i], bpos);
  }

  //** run the task
  if (creds != NULL) {
     gop = os_regex_object_set_multiple_attrs(osrs->os_child, creds, call_id, path, object_regex, obj_types, recurse_depth, key, (void **)val, v_size, n_attrs);
     gop_waitall(gop);
     status = gop_get_status(gop);
     gop_free(gop, OP_DESTROY);
  } else {
     status = op_failure_status;
  }

fail:
  mq_frame_destroy(fdata);
  mq_frame_destroy(fcred);
  mq_frame_destroy(fcid);

  if (path != NULL) os_regex_table_destroy(path);
  if (object_regex != NULL) os_regex_table_destroy(object_regex);

  if (key != NULL) {
    for (i=0; i<n_attrs; i++) {
      if (key[i] != NULL) free(key[i]);
      if (val[i] != NULL) free(val[i]);
    }
    free(key); free(val); free(v_size);
  }

  //** Send the response
  n = zigzag_encode(status.op_status, tbuf);
  n = n + zigzag_encode(status.error_code, &(tbuf[n]));
  mq_stream_write(mqs, tbuf, n);
  mq_stream_destroy(mqs);
}

//***********************************************************************
// osrs_copy_mult_attr_cb - Copies multiple attributes between objects
//***********************************************************************

void osrs_copy_mult_attr_cb(void *arg, mq_task_t *task)
{
  object_service_fn_t *os = (object_service_fn_t *)arg;
  osrs_priv_t *osrs = (osrs_priv_t *)os->priv;
  mq_frame_t *fid, *fuid, *fcred, *fdata, *ffd_src, *ffd_dest;
  creds_t *creds;
  char *id;
  unsigned char *data;
  op_generic_t *gop;
  int fsize, bpos, len, id_size, i;
  int64_t timeout, n, v, nbytes;
  mq_msg_t *msg, *response;
  op_status_t status;
  char **key_src;
  char **key_dest;
  os_fd_t *fd_src, *fd_dest;
  intptr_t fd_key_src, fd_key_dest;

  log_printf(5, "Processing incoming request\n");

  key_src = NULL; key_dest = NULL;
  status = op_failure_status;

  //** Parse the command.
  msg = task->msg;
  mq_remove_header(msg, 0);

  fid = mq_msg_pop(msg);  //** This is the ID for responses
  mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

  fcred = mq_msg_pop(msg);  //** This has the creds
  creds = osrs_get_creds(os, fcred);

  fuid = mq_msg_pop(msg);  //** Host/user ID
  mq_get_frame(fuid, (void **)&id, &id_size);

  //** Get the fd handles
  ffd_src = mq_msg_pop(msg);  //** Host/user ID
  mq_get_frame(ffd_src, (void **)&data, &len);
  fd_key_src = *(intptr_t *)data;

  ffd_dest = mq_msg_pop(msg);  //** Host/user ID
  mq_get_frame(ffd_dest, (void **)&data, &len);
  fd_key_dest = *(intptr_t *)data;

log_printf(5, "PTR key_src=%" PRIdPTR " len=%d\n", fd_key_src, len);

  fdata = mq_msg_pop(msg);  //** attr list to set
  mq_get_frame(fdata, (void **)&data, &fsize);

log_printf(5, "PTR key_src=%" PRIdPTR " len=%d id=%s id_len=%d\n", fd_key_src, len, id, id_size);

  //** Now check if the handles are valid
  if ((fd_src = mq_ongoing_get(osrs->ongoing, (char *)id, id_size, fd_key_src)) == NULL) {
     log_printf(5, "Invalid SOURCE handle!\n");
     goto fail_fd;
  }
  if ((fd_dest = mq_ongoing_get(osrs->ongoing, (char *)id, id_size, fd_key_dest)) == NULL) {
     log_printf(5, "Invalid DEST handle!\n");
     goto fail_fd;
  }

  //** Parse the attr list
  bpos = 0;
  i = zigzag_decode(&(data[bpos]), fsize, &timeout);
  if (i<0) goto fail_fd;
  if (timeout < 0) timeout = 10;
  bpos += i;
  fsize -= i;

  i = zigzag_decode(&(data[bpos]), fsize, &n);
  if ((i<0) || (n<=0)) goto fail_fd;
  bpos += i;
  fsize -= i;

log_printf(5, "timeout=%d n=%d\n", timeout, n);
  type_malloc_clear(key_src, char *, n);
  type_malloc_clear(key_dest, char *, n);

  for (i=0; i<n; i++) {
     nbytes = zigzag_decode(&(data[bpos]), fsize, &v);
log_printf(5, "i=%d klen=" XOT " bpos=%d\n", i, v, bpos);

     if ((nbytes<0) || (v<=0)) goto fail;
     bpos += nbytes;
     fsize -= nbytes;

     type_malloc(key_src[i], char, v+1);
     if (v > fsize) goto fail;
     memcpy(key_src[i], &(data[bpos]), v);
     key_src[i][v] = 0;
     bpos += v;
     fsize -= nbytes;
log_printf(5, "i=%d key_src=%s bpos=%d\n", i, key_src[i], bpos);

     nbytes = zigzag_decode(&(data[bpos]), fsize, &v);
log_printf(5, "i=%d klen=" XOT " bpos=%d\n", i, v, bpos);

     if (nbytes<0) goto fail;
     bpos += nbytes;
     fsize -= nbytes;


     type_malloc(key_dest[i], char, v+1);
     if (v > fsize) goto fail;
     memcpy(key_dest[i], &(data[bpos]), v);
     key_dest[i][v] = 0;
     bpos += v;
     fsize -= nbytes;
log_printf(5, "i=%d key_dest=%s bpos=%d\n", i, key_dest[i], bpos);
  }

  //** Execute the get attribute call
  if (creds != NULL) {
     gop = os_copy_multiple_attrs(osrs->os_child, creds, fd_src, key_src, fd_dest, key_dest, n);
     gop_waitall(gop);
     status = gop_get_status(gop);
     gop_free(gop, OP_DESTROY);
  } else {
     status = op_failure_status;
  }

fail_fd:
fail:

  if (fd_src != NULL) mq_ongoing_release(osrs->ongoing, (char *)id, id_size, fd_key_src);
  if (fd_dest != NULL) mq_ongoing_release(osrs->ongoing, (char *)id, id_size, fd_key_dest);

  mq_frame_destroy(ffd_src);
  mq_frame_destroy(ffd_dest);
  mq_frame_destroy(fuid);
  mq_frame_destroy(fdata);
  mq_frame_destroy(fcred);


  //** Form the response
  response = mq_make_response_core_msg(msg, fid);
  mq_msg_append_frame(response, mq_make_status_frame(status));
  mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

log_printf(5, "status.op_status=%d\n", status.op_status);
  //** Lastly send it
  mq_submit(osrs->server_portal, mq_task_new(osrs->mqc, response, NULL, NULL, 30));

  if (key_src) {
    for (i=0; i<n; i++) if (key_src[i]) free(key_src[i]);
    free(key_src);
  }

  if (key_dest) {
    for (i=0; i<n; i++) if (key_dest[i]) free(key_dest[i]);
    free(key_dest);
  }
}


//***********************************************************************
// osrs_move_mult_attr_cb - Moves multiple attributes
//***********************************************************************

void osrs_move_mult_attr_cb(void *arg, mq_task_t *task)
{
  object_service_fn_t *os = (object_service_fn_t *)arg;
  osrs_priv_t *osrs = (osrs_priv_t *)os->priv;
  mq_frame_t *fid, *fuid, *fcred, *fdata, *ffd_src;
  creds_t *creds;
  char *id;
  unsigned char *data;
  op_generic_t *gop;
  int fsize, bpos, len, id_size, i;
  int64_t timeout, n, v, nbytes;
  mq_msg_t *msg, *response;
  op_status_t status;
  char **key_src;
  char **key_dest;
  os_fd_t *fd_src;
  intptr_t fd_key_src;

  log_printf(5, "Processing incoming request\n");

  key_src = NULL; key_dest = NULL;
  status = op_failure_status;

  //** Parse the command.
  msg = task->msg;
  mq_remove_header(msg, 0);

  fid = mq_msg_pop(msg);  //** This is the ID for responses
  mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

  fcred = mq_msg_pop(msg);  //** This has the creds
  creds = osrs_get_creds(os, fcred);

  fuid = mq_msg_pop(msg);  //** Host/user ID
  mq_get_frame(fuid, (void **)&id, &id_size);

  //** Get the fd handles
  ffd_src = mq_msg_pop(msg);  //** Host/user ID
  mq_get_frame(ffd_src, (void **)&data, &len);
  fd_key_src = *(intptr_t *)data;

log_printf(5, "PTR key_src=%" PRIdPTR " len=%d\n", fd_key_src, len);

  fdata = mq_msg_pop(msg);  //** attr list to set
  mq_get_frame(fdata, (void **)&data, &fsize);

log_printf(5, "PTR key_src=%" PRIdPTR " len=%d id=%s id_len=%d\n", fd_key_src, len, id, id_size);

  //** Now check if the handles are valid
  if ((fd_src = mq_ongoing_get(osrs->ongoing, (char *)id, id_size, fd_key_src)) == NULL) {
     log_printf(5, "Invalid SOURCE handle!\n");
     goto fail_fd;
  }

  //** Parse the attr list
  bpos = 0;
  i = zigzag_decode(&(data[bpos]), fsize, &timeout);
  if (i<0) goto fail_fd;
  if (timeout < 0) timeout = 10;
  bpos += i;
  fsize -= i;

  i = zigzag_decode(&(data[bpos]), fsize, &n);
  if ((i<0) || (n<=0)) goto fail_fd;
  bpos += i;
  fsize -= i;

log_printf(5, "timeout=%d n=%d\n", timeout, n);
  type_malloc_clear(key_src, char *, n);
  type_malloc_clear(key_dest, char *, n);

  for (i=0; i<n; i++) {
     nbytes = zigzag_decode(&(data[bpos]), fsize, &v);
log_printf(5, "i=%d klen=" XOT " bpos=%d\n", i, v, bpos);

     if ((nbytes<0) || (v<=0)) goto fail;
     bpos += nbytes;
     fsize -= nbytes;

     type_malloc(key_src[i], char, v+1);
     if (v > fsize) goto fail;
     memcpy(key_src[i], &(data[bpos]), v);
     key_src[i][v] = 0;
     bpos += v;
     fsize -= nbytes;
log_printf(5, "i=%d key_src=%s bpos=%d\n", i, key_src[i], bpos);

     nbytes = zigzag_decode(&(data[bpos]), fsize, &v);
log_printf(5, "i=%d klen=" XOT " bpos=%d\n", i, v, bpos);

     if (nbytes<0) goto fail;
     bpos += nbytes;
     fsize -= nbytes;


     type_malloc(key_dest[i], char, v+1);
     if (v > fsize) goto fail;
     memcpy(key_dest[i], &(data[bpos]), v);
     key_dest[i][v] = 0;
     bpos += v;
     fsize -= nbytes;
log_printf(5, "i=%d key_dest=%s bpos=%d\n", i, key_dest[i], bpos);
  }

  //** Execute the get attribute call
  if (creds != NULL) {
     gop = os_move_multiple_attrs(osrs->os_child, creds, fd_src, key_src, key_dest, n);
     gop_waitall(gop);
     status = gop_get_status(gop);
     gop_free(gop, OP_DESTROY);
  } else {
     status = op_failure_status;
  }

fail_fd:
fail:

  if (fd_src != NULL) mq_ongoing_release(osrs->ongoing, (char *)id, id_size, fd_key_src);

  mq_frame_destroy(ffd_src);
  mq_frame_destroy(fuid);
  mq_frame_destroy(fdata);
  mq_frame_destroy(fcred);


  //** Form the response
  response = mq_make_response_core_msg(msg, fid);
  mq_msg_append_frame(response, mq_make_status_frame(status));
  mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

log_printf(5, "status.op_status=%d\n", status.op_status);
  //** Lastly send it
  mq_submit(osrs->server_portal, mq_task_new(osrs->mqc, response, NULL, NULL, 30));

  if (key_src) {
    for (i=0; i<n; i++) if (key_src[i]) free(key_src[i]);
    free(key_src);
  }

  if (key_dest) {
    for (i=0; i<n; i++) if (key_dest[i]) free(key_dest[i]);
    free(key_dest);
  }
}

//***********************************************************************
// osrs_symlink_mult_attr_cb - Symlinks multiple attributes between objects
//***********************************************************************

void osrs_symlink_mult_attr_cb(void *arg, mq_task_t *task)
{
  object_service_fn_t *os = (object_service_fn_t *)arg;
  osrs_priv_t *osrs = (osrs_priv_t *)os->priv;
  mq_frame_t *fid, *fuid, *fcred, *fdata, *ffd_dest;
  creds_t *creds;
  char *id;
  unsigned char *data;
  op_generic_t *gop;
  int fsize, bpos, len, id_size, i;
  int64_t timeout, n, v, nbytes;
  mq_msg_t *msg, *response;
  op_status_t status;
  char **src_path;
  char **key_src;
  char **key_dest;
  os_fd_t *fd_dest;
  intptr_t fd_key_dest;

  log_printf(5, "Processing incoming request\n");

  key_src = NULL; key_dest = NULL;
  status = op_failure_status;

  //** Parse the command.
  msg = task->msg;
  mq_remove_header(msg, 0);

  fid = mq_msg_pop(msg);  //** This is the ID for responses
  mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

  fcred = mq_msg_pop(msg);  //** This has the creds
  creds = osrs_get_creds(os, fcred);

  fuid = mq_msg_pop(msg);  //** Host/user ID
  mq_get_frame(fuid, (void **)&id, &id_size);

  //** Get the fd handles
  ffd_dest = mq_msg_pop(msg);  //** Host/user ID
  mq_get_frame(ffd_dest, (void **)&data, &len);
  fd_key_dest = *(intptr_t *)data;

log_printf(5, "PTR key_dest=%" PRIdPTR " len=%d\n", fd_key_dest, len);

  fdata = mq_msg_pop(msg);  //** attr list to set
  mq_get_frame(fdata, (void **)&data, &fsize);

log_printf(5, "PTR key_dest=%" PRIdPTR " len=%d id=%s id_len=%d\n", fd_key_dest, len, id, id_size);

  //** Now check if the handles are valid
  if ((fd_dest = mq_ongoing_get(osrs->ongoing, (char *)id, id_size, fd_key_dest)) == NULL) {
     log_printf(5, "Invalid SOURCE handle!\n");
     goto fail_fd;
  }

  //** Parse the attr list
  bpos = 0;
  i = zigzag_decode(&(data[bpos]), fsize, &timeout);
  if (i<0) goto fail_fd;
  if (timeout < 0) timeout = 10;
  bpos += i;
  fsize -= i;

  i = zigzag_decode(&(data[bpos]), fsize, &n);
  if ((i<0) || (n<=0)) goto fail_fd;
  bpos += i;
  fsize -= i;

log_printf(5, "timeout=%d n=%d\n", timeout, n);
  type_malloc_clear(key_src, char *, n);
  type_malloc_clear(key_dest, char *, n);
  type_malloc_clear(src_path, char *, n);

  for (i=0; i<n; i++) {
     nbytes = zigzag_decode(&(data[bpos]), fsize, &v);
log_printf(5, "i=%d slen=" XOT " bpos=%d\n", i, v, bpos);
     if ((nbytes<0) || (v<=0)) goto fail;
     bpos += nbytes;
     fsize -= nbytes;

     type_malloc(src_path[i], char, v+1);
     if (v > fsize) goto fail;
     memcpy(src_path[i], &(data[bpos]), v);
     src_path[i][v] = 0;
     bpos += v;
     fsize -= nbytes;
log_printf(5, "i=%d src_path=%s bpos=%d\n", i, src_path[i], bpos);

     nbytes = zigzag_decode(&(data[bpos]), fsize, &v);
log_printf(5, "i=%d klen=" XOT " bpos=%d\n", i, v, bpos);
     if ((nbytes<0) || (v<=0)) goto fail;
     bpos += nbytes;
     fsize -= nbytes;

     type_malloc(key_src[i], char, v+1);
     if (v > fsize) goto fail;
     memcpy(key_src[i], &(data[bpos]), v);
     key_src[i][v] = 0;
     bpos += v;
     fsize -= nbytes;
log_printf(5, "i=%d key_src=%s bpos=%d\n", i, key_src[i], bpos);

     nbytes = zigzag_decode(&(data[bpos]), fsize, &v);
log_printf(5, "i=%d klen=" XOT " bpos=%d\n", i, v, bpos);

     if (nbytes<0) goto fail;
     bpos += nbytes;
     fsize -= nbytes;


     type_malloc(key_dest[i], char, v+1);
     if (v > fsize) goto fail;
     memcpy(key_dest[i], &(data[bpos]), v);
     key_dest[i][v] = 0;
     bpos += v;
     fsize -= nbytes;
log_printf(5, "i=%d key_dest=%s bpos=%d\n", i, key_dest[i], bpos);
  }

  //** Execute the get attribute call
  if (creds != NULL) {
     gop = os_symlink_multiple_attrs(osrs->os_child, creds, src_path, key_src, fd_dest, key_dest, n);
     gop_waitall(gop);
     status = gop_get_status(gop);
     gop_free(gop, OP_DESTROY);
  } else {
     status = op_failure_status;
  }

fail_fd:
fail:

  if (fd_dest != NULL) mq_ongoing_release(osrs->ongoing, (char *)id, id_size, fd_key_dest);

  mq_frame_destroy(ffd_dest);
  mq_frame_destroy(fuid);
  mq_frame_destroy(fdata);
  mq_frame_destroy(fcred);


  //** Form the response
  response = mq_make_response_core_msg(msg, fid);
  mq_msg_append_frame(response, mq_make_status_frame(status));
  mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

log_printf(5, "status.op_status=%d\n", status.op_status);
  //** Lastly send it
  mq_submit(osrs->server_portal, mq_task_new(osrs->mqc, response, NULL, NULL, 30));

  if (key_src) {
    for (i=0; i<n; i++) if (key_src[i]) free(key_src[i]);
    free(key_src);
  }

  if (key_dest) {
    for (i=0; i<n; i++) if (key_dest[i]) free(key_dest[i]);
    free(key_dest);
  }

  if (src_path) {
    for (i=0; i<n; i++) if (src_path[i]) free(src_path[i]);
    free(src_path);
  }
}

//***********************************************************************
// osrs_object_iter_alist_cb - Handles the alist object iterator
//***********************************************************************

void osrs_object_iter_alist_cb(void *arg, mq_task_t *task)
{
  object_service_fn_t *os = (object_service_fn_t *)arg;
  osrs_priv_t *osrs = (osrs_priv_t *)os->priv;
  mq_frame_t *fid, *fcred, *fdata, *hid;
  unsigned char *buffer;
  unsigned char tbuf[32];
  int *v_size;
  char **key;
  char **val;
  char *fname;
  os_regex_table_t *path, *object_regex;
  creds_t *creds;
  int fsize, bpos, n, i, err, ftype, prefix_len;
  int64_t recurse_depth, obj_types, timeout, n_attrs, len;
  mq_msg_t *msg;
  os_object_iter_t *it;
  mq_stream_t *mqs;
  op_status_t status;

  log_printf(5, "Processing incoming request\n");

  status = op_failure_status;
  key = NULL; val = NULL, v_size = NULL; n_attrs = 0; it = NULL;

  //** Parse the command.
  msg = task->msg;
  mq_remove_header(msg, 0);

  fid = mq_msg_pop(msg);  //** This is the ID
  mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame
  hid = mq_msg_pop(msg);  //** This is the Host ID for the ongoing stream

  fcred = mq_msg_pop(msg);  //** This has the creds
  creds = osrs_get_creds(os, fcred);

  fdata = mq_msg_pop(msg);  //** This has the data
  mq_get_frame(fdata, (void **)&buffer, &fsize);

  //** Create the stream so we can get the heartbeating while we work
  mqs = mq_stream_write_create(osrs->mqc, osrs->server_portal, osrs->ongoing, MQS_PACK_COMPRESS, osrs->max_stream, timeout, msg, fid, hid, 0);

  //** Parse the buffer
  path = NULL;
  object_regex = NULL;
  bpos = 0;

  n = zigzag_decode(&(buffer[bpos]), fsize-bpos, &timeout);
  if (n < 0) { timeout = 60; goto fail; }
  bpos += n;

  n = zigzag_decode(&(buffer[bpos]), fsize-bpos, &recurse_depth);
  if (n < 0) goto fail;
  bpos += n;

  n = zigzag_decode(&(buffer[bpos]), fsize-bpos, &obj_types);
  if (n < 0) goto fail;
  bpos += n;

  n = zigzag_decode(&(buffer[bpos]), fsize-bpos, &n_attrs);
  if (n < 0) goto fail;
  bpos += n;

  type_malloc_clear(key, char *, n_attrs);
  type_malloc_clear(val, char *, n_attrs);
  type_malloc_clear(v_size, int, n_attrs);

  for (i=0; i<n_attrs; i++) {
     n = zigzag_decode(&(buffer[bpos]), fsize-bpos, &len);
     if (n < 0)  goto fail;
     bpos += n;

     if ((bpos+len) > fsize) goto fail;
     type_malloc(key[i], char, len+1);
     memcpy(key[i], &(buffer[bpos]), len);
     key[i][len] = 0;
     bpos += len;

     n = zigzag_decode(&(buffer[bpos]), fsize-bpos, &len);
     if (n < 0)  goto fail;
     bpos += n;
     v_size[i] = -abs(len);
log_printf(15, "i=%d key=%s v_size=%d bpos=%d\n", i, key[i], len, bpos);
  }

  path = os_regex_table_unpack(&(buffer[bpos]), fsize-bpos, &n);
  if (n == 0) goto fail;
  bpos += n;

log_printf(15, "1. bpos=%d fsize=%d\n", bpos, fsize);

  object_regex = os_regex_table_unpack(&(buffer[bpos]), fsize-bpos, &n);
  if (n == 0) goto fail;
  bpos += n;

log_printf(15, "2. bpos=%d fsize=%d\n", bpos, fsize);

  //** run the task
  if (creds != NULL) {
     it = os_create_object_iter_alist(osrs->os_child, creds, path, object_regex, obj_types, recurse_depth, key, (void **)val, v_size, n_attrs);
  } else {
     it = NULL;
  }

fail:
  mq_frame_destroy(fdata);
  mq_frame_destroy(fcred);

  //** Encode the status
  status = (it != NULL) ? op_success_status : op_failure_status;
  n = zigzag_encode(status.op_status, tbuf);
  n = n + zigzag_encode(status.error_code, &(tbuf[n]));
  mq_stream_write(mqs, tbuf, n);

  //** Check if we kick out due to an error
  if (it == NULL) goto finished;

  //** Pack up the data and send it out
  err = 0;
  while (((ftype = os_next_object(osrs->os_child, it, &fname, &prefix_len)) > 0) && (err == 0)) {
     len = strlen(fname);
     n = zigzag_encode(ftype, tbuf);
     n += zigzag_encode(prefix_len, &(tbuf[n]));
     n += zigzag_encode(len, &(tbuf[n]));
     err += mq_stream_write(mqs, tbuf, n);
     err += mq_stream_write(mqs, fname, len);

log_printf(5, "ftype=%d prefix_len=%d len=%d fname=%s n_attrs=%d\n", ftype, prefix_len, len, fname, n_attrs);
     //** Now dump the attributes
     for (i=0; i<n_attrs; i++) {
        n = zigzag_encode(v_size[i], tbuf);
        err += mq_stream_write(mqs, tbuf, n);
log_printf(5, "v_size[%d]=%d\n", i, v_size[i]);
        if (v_size[i] > 0) {
log_printf(5, "val[%d]=%s\n", i, val[i]);
          err += mq_stream_write(mqs, val[i], v_size[i]);
          free(val[i]); val[i] = NULL;
        }
     }

     free(fname);
  }

  //** Flag this as the last object
  n = zigzag_encode(0, tbuf);
  mq_stream_write(mqs, tbuf, n);

  //** Destroy the object iterator
  os_destroy_object_iter(osrs->os_child, it);

finished:
  //** Flush the buffer
  mq_stream_destroy(mqs);

  //** Clean up
  if (path != NULL) os_regex_table_destroy(path);
  if (object_regex != NULL) os_regex_table_destroy(object_regex);

  if (key != NULL) {
    for (i=0; i<n_attrs; i++) {
      if (key[i] != NULL) free(key[i]);
      if (val[i] != NULL) free(val[i]);
    }
    free(key); free(val); free(v_size);
  }

}

//***********************************************************************
// osrs_object_iter_aregex_cb - Handles the attr regex object iterator
//***********************************************************************

void osrs_object_iter_aregex_cb(void *arg, mq_task_t *task)
{
  object_service_fn_t *os = (object_service_fn_t *)arg;
  osrs_priv_t *osrs = (osrs_priv_t *)os->priv;
  mq_frame_t *fid, *fcred, *fdata, *hid;
  unsigned char *buffer;
  unsigned char tbuf[32], null[32];
  int  v_size;
  char *key;
  char *val;
  char *fname;
  os_regex_table_t *path, *object_regex, *attr_regex;
  creds_t *creds;
  int fsize, bpos, n, err, ftype, prefix_len, null_len;
  int64_t recurse_depth, obj_types, timeout, v_max, len;
  mq_msg_t *msg;
  os_object_iter_t *it;
  os_attr_iter_t *ait;
  mq_stream_t *mqs;
  op_status_t status;

  log_printf(5, "Processing incoming request\n");

  status = op_failure_status;
  it = NULL;
  path = object_regex = attr_regex = NULL;

  //** Parse the command.
  msg = task->msg;
  mq_remove_header(msg, 0);

  fid = mq_msg_pop(msg);  //** This is the ID
  mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame
  hid = mq_msg_pop(msg);  //** This is the Host ID for the ongoing stream

  fcred = mq_msg_pop(msg);  //** This has the creds
  creds = osrs_get_creds(os, fcred);

  fdata = mq_msg_pop(msg);  //** This has the data
  mq_get_frame(fdata, (void **)&buffer, &fsize);

  //** Create the stream so we can get the heartbeating while we work
  mqs = mq_stream_write_create(osrs->mqc, osrs->server_portal, osrs->ongoing, MQS_PACK_COMPRESS, osrs->max_stream, timeout, msg, fid, hid, 0);

  //** Parse the buffer
  path = NULL;
  object_regex = NULL;
  bpos = 0;

  n = zigzag_decode(&(buffer[bpos]), fsize-bpos, &timeout);
  if (n < 0) { timeout = 60; goto fail; }
  bpos += n;

  n = zigzag_decode(&(buffer[bpos]), fsize-bpos, &recurse_depth);
  if (n < 0) goto fail;
  bpos += n;

  n = zigzag_decode(&(buffer[bpos]), fsize-bpos, &obj_types);
  if (n < 0) goto fail;
  bpos += n;

  n = zigzag_decode(&(buffer[bpos]), fsize-bpos, &v_max);
  if (n < 0) goto fail;
  bpos += n;

  path = os_regex_table_unpack(&(buffer[bpos]), fsize-bpos, &n);
  if (n == 0) goto fail;
  bpos += n;

log_printf(15, "1. bpos=%d fsize=%d\n", bpos, fsize);

  object_regex = os_regex_table_unpack(&(buffer[bpos]), fsize-bpos, &n);
  if (n == 0) goto fail;
  bpos += n;

log_printf(15, "2. bpos=%d fsize=%d\n", bpos, fsize);

  attr_regex = os_regex_table_unpack(&(buffer[bpos]), fsize-bpos, &n);
  if (n == 0) goto fail;
  bpos += n;

log_printf(15, "2. bpos=%d fsize=%d attr_regex=%p\n", bpos, fsize, attr_regex);

  //** run the task
  if (creds != NULL) {
     v_max = -abs(v_max);
     ait = NULL;
     it = os_create_object_iter(osrs->os_child, creds, path, object_regex, obj_types, attr_regex, recurse_depth, &ait, v_max);
  } else {
     it = NULL;
  }

fail:
  mq_frame_destroy(fdata);
  mq_frame_destroy(fcred);

  //** Encode the status
  status = (it != NULL) ? op_success_status : op_failure_status;
  n = zigzag_encode(status.op_status, tbuf);
  n = n + zigzag_encode(status.error_code, &(tbuf[n]));
  mq_stream_write(mqs, tbuf, n);

  //** Check if we kick out due to an error
  if (it == NULL) goto finished;

  null_len = zigzag_encode(0, null);

  //** Pack up the data and send it out
  err = 0;
  while (((ftype = os_next_object(osrs->os_child, it, &fname, &prefix_len)) > 0) && (err == 0)) {
     len = strlen(fname);
     n = zigzag_encode(ftype, tbuf);
     n += zigzag_encode(prefix_len, &(tbuf[n]));
     n += zigzag_encode(len, &(tbuf[n]));
     err += mq_stream_write(mqs, tbuf, n);
     err += mq_stream_write(mqs, fname, len);

log_printf(5, "ftype=%d prefix_len=%d len=%d fname=%s\n", ftype, prefix_len, len, fname);
     //** Now dump the attributes
     if (attr_regex != NULL) {
        v_size = v_max;
        while (os_next_attr(osrs->os_child, ait, &key, (void **)&val, &v_size) == 0) {
log_printf(15, "key=%s v_size=%d\n", key, v_size);
           len = strlen(key);
           n = zigzag_encode(len, tbuf);
           err += mq_stream_write(mqs, tbuf, n);
           err += mq_stream_write(mqs, key, len);
           free(key); key = NULL;

           n = zigzag_encode(v_size, tbuf);
           err += mq_stream_write(mqs, tbuf, n);
           if (v_size > 0) {
             err += mq_stream_write(mqs, val, v_size);
             free(val); val = NULL;
           }
           v_size = v_max;
        }

        //** Flag this as the last attr
        mq_stream_write(mqs, null, null_len);
     }


     free(fname);
  }

  //** Flag this as the last object
  mq_stream_write(mqs, null, null_len);

  //** Destroy the object iterator
  os_destroy_object_iter(osrs->os_child, it);

finished:
  //** Flush the buffer
  mq_stream_destroy(mqs);

  //** Clean up
  if (path != NULL) os_regex_table_destroy(path);
  if (object_regex != NULL) os_regex_table_destroy(object_regex);
  if (attr_regex != NULL) os_regex_table_destroy(attr_regex);
}

//***********************************************************************
// osrs_attr_iter_cb - Handles the attr regex iterator
//***********************************************************************

void osrs_attr_iter_cb(void *arg, mq_task_t *task)
{
  object_service_fn_t *os = (object_service_fn_t *)arg;
  osrs_priv_t *osrs = (osrs_priv_t *)os->priv;
  mq_frame_t *fid, *fcred, *fdata, *ffd, *fhid;
  unsigned char *buffer;
  unsigned char tbuf[32];
  int v_size;
  char *key, *val, *id;
  void *fhandle;
  os_regex_table_t *attr_regex;
  creds_t *creds;
  int fsize, bpos, n, err, id_size, hsize;
  int64_t timeout, len, v_size_init;
  mq_msg_t *msg;
  intptr_t fhkey;
  void *handle;
  os_attr_iter_t *it;
  mq_stream_t *mqs;
  op_status_t status;

  log_printf(5, "Processing incoming request\n");

  status = op_failure_status;
  key = NULL; val = NULL, v_size = -1; it = NULL;

  //** Parse the command.
  msg = task->msg;
  mq_remove_header(msg, 0);

  fid = mq_msg_pop(msg);  //** This is the ID
  mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame
  fhid = mq_msg_pop(msg);  //** Host handle
  mq_get_frame(fhid, (void **)&id, &id_size);

  fcred = mq_msg_pop(msg);  //** This has the creds
  creds = osrs_get_creds(os, fcred);

  ffd = mq_msg_pop(msg);  //** This has the file handle
  mq_get_frame(ffd, (void **)&fhandle, &hsize);

  fdata = mq_msg_pop(msg);  //** This has the data
  mq_get_frame(fdata, (void **)&buffer, &fsize);

  //** Create the stream so we can get the heartbeating while we work
  mqs = mq_stream_write_create(osrs->mqc, osrs->server_portal, osrs->ongoing, MQS_PACK_COMPRESS, osrs->max_stream, timeout, msg, fid, fhid, 0);

  //** Check if the file handle is the correect size
  if (hsize != sizeof(intptr_t)) {
     log_printf(6, "ERROR invalid handle size=%d\n", hsize);
     status = op_failure_status;
     goto fail;
  }

  //** Get the local file handle
  fhkey = *(intptr_t *)fhandle;
log_printf(5, "PTR key=%" PRIdPTR "\n", key);

  //** Do the host lookup for the file handle
  if ((handle = mq_ongoing_get(osrs->ongoing, id, id_size, fhkey)) == NULL) {
    log_printf(6, "ERROR missing host=%s\n", id);
    status = op_failure_status;
  }

  //** Parse the buffer
  attr_regex = NULL;
  bpos = 0;

  n = zigzag_decode(&(buffer[bpos]), fsize-bpos, &timeout);
  if (n < 0) { timeout = 60; goto fail; }
  bpos += n;

  n = zigzag_decode(&(buffer[bpos]), fsize-bpos, &v_size_init);
  if (n < 0) goto fail;
  bpos += n;

  attr_regex = os_regex_table_unpack(&(buffer[bpos]), fsize-bpos, &n);
  if (n == 0) goto fail;
  bpos += n;

log_printf(15, "bpos=%d fsize=%d\n", bpos, fsize);

  //** run the task
  if (creds != NULL) {
     it = os_create_attr_iter(osrs->os_child, creds, handle, attr_regex, v_size_init);
  } else {
     it = NULL;
  }

fail:

  //** Encode the status
  status = (it != NULL) ? op_success_status : op_failure_status;
  n = zigzag_encode(status.op_status, tbuf);
  n = n + zigzag_encode(status.error_code, &(tbuf[n]));
  mq_stream_write(mqs, tbuf, n);

  //** Check if we kick out due to an error
  if (it == NULL) goto finished;

  //** Pack up the data and send it out
  err = 0;
  v_size = v_size_init;
  while ((os_next_attr(osrs->os_child, it, &key, (void **)&val, &v_size) == 0) && (err == 0)) {
log_printf(5, "err=%d key=%s v_size=%d\n", err, key, v_size);
     len = strlen(key);
     n = zigzag_encode(len, tbuf);
     err += mq_stream_write(mqs, tbuf, n);
     err += mq_stream_write(mqs, key, len);
     free(key);

     n = zigzag_encode(v_size, tbuf);
     err += mq_stream_write(mqs, tbuf, n);
     if (v_size > 0) {
        err += mq_stream_write(mqs, val, v_size);
        free(val);
     }

     v_size = v_size_init;
  }

  //** Flag this as the last object
  n = zigzag_encode(0, tbuf);
  mq_stream_write(mqs, tbuf, n);

  //** Destroy the object iterator
  os_destroy_attr_iter(osrs->os_child, it);

finished:

  if (handle != NULL) mq_ongoing_release(osrs->ongoing, (char *)id, id_size, fhkey);

  //** Clean up
  mq_frame_destroy(fdata);
  mq_frame_destroy(fcred);
  mq_frame_destroy(ffd);
//  mq_frame_destroy(fhid);

  if (attr_regex != NULL) os_regex_table_destroy(attr_regex);

  //** Flush the buffer
  mq_stream_destroy(mqs);

}

//***********************************************************************
// osrs_fsck_iter_cb - Handles the FSCK regex iterator
//***********************************************************************

void osrs_fsck_iter_cb(void *arg, mq_task_t *task)
{
  object_service_fn_t *os = (object_service_fn_t *)arg;
  osrs_priv_t *osrs = (osrs_priv_t *)os->priv;
  mq_frame_t *fid, *fcred, *fdata, *fhid;
  unsigned char *buffer;
  unsigned char tbuf[32];
  char *path, *bad_fname, *id;
  creds_t *creds;
  int fsize, n, err, id_size, bad_atype, fsck_err;
  int64_t timeout, len, mode;
  mq_msg_t *msg;
  os_fsck_iter_t *it;
  mq_stream_t *mqs;
  op_status_t status;

  log_printf(5, "Processing incoming request\n");

  status = op_failure_status;
  it = NULL;
  err = 0;

  //** Parse the command.
  msg = task->msg;
  mq_remove_header(msg, 0);

  fid = mq_msg_pop(msg);  //** This is the ID
  mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

  fhid = mq_msg_pop(msg);  //** Host handle
  mq_get_frame(fhid, (void **)&id, &id_size);

  fcred = mq_msg_pop(msg);  //** This has the creds
  creds = osrs_get_creds(os, fcred);

  fdata = mq_msg_pop(msg);  //** This has the path
  mq_get_frame(fdata, (void **)&buffer, &fsize);
  if (fsize > 0) {
     type_malloc(path, char, fsize+1);
     memcpy(path, buffer, fsize);
     path[fsize] = 0; //** NULL terminate the path name
  } else {
     err = 1;
  }
  mq_frame_destroy(fdata);

  fdata = mq_msg_pop(msg);  //** This has the mode and timeout
  mq_get_frame(fdata, (void **)&buffer, &fsize);
  if (fsize > 0) {
    if (err == 0) {
       n = zigzag_decode(buffer, fsize, &mode);
       timeout = 300;
       zigzag_decode(&(buffer[n]), fsize-n, &timeout);
    }
  } else {
    err = 1;
  }
  mq_frame_destroy(fdata);

  //** Create the stream so we can get the heartbeating while we work
  mqs = mq_stream_write_create(osrs->mqc, osrs->server_portal, osrs->ongoing, MQS_PACK_COMPRESS, osrs->max_stream, timeout, msg, fid, fhid, 0);

log_printf(5, "1.err=%d\n", err);

  if (err != 0) goto fail;

  //** Create the fsck iterator
  if (creds == NULL) {
     it = os_create_fsck_iter(osrs->os_child, creds, path, mode);
  } else {
     it = NULL;
  }

  if (it == NULL) { err = 1; }

log_printf(5, "2.err=%d\n", err);

fail:

  //** Encode the status
  status = (err == 0) ? op_success_status : op_failure_status;
  n = zigzag_encode(status.op_status, tbuf);
  n = n + zigzag_encode(status.error_code, &(tbuf[n]));
  mq_stream_write(mqs, tbuf, n);

  //** Check if we kick out due to an error
  if (it == NULL) goto finished;

  //** Pack up the data and send it out
  err = 0;
  while (((fsck_err = os_next_fsck(osrs->os_child, it, &bad_fname, &bad_atype)) != OS_FSCK_FINISHED) && (err == 0)) {
log_printf(5, "err=%d bad_fname=%s bad_atype=%d\n", err, bad_fname, bad_atype);
     len = strlen(bad_fname);
     n = zigzag_encode(len, tbuf);
     err += mq_stream_write(mqs, tbuf, n);
     err += mq_stream_write(mqs, bad_fname, len);
     free(bad_fname);

     n = zigzag_encode(bad_atype, tbuf);
     n += zigzag_encode(fsck_err, &(tbuf[n]));
     err += mq_stream_write(mqs, tbuf, n);
  }

  //** Flag this as the last object
  n = zigzag_encode(0, tbuf);
  mq_stream_write(mqs, tbuf, n);

  //** Destroy the object iterator
  os_destroy_fsck_iter(osrs->os_child, it);

finished:

  //** Clean up
  mq_frame_destroy(fcred);
//  mq_frame_destroy(fhid);

  if (path != NULL) free(path);

  //** Flush the buffer
  mq_stream_destroy(mqs);

}

//***********************************************************************
// osrs_fsck_object_cb - Handles the FSCK object check
//***********************************************************************

void osrs_fsck_object_cb(void *arg, mq_task_t *task)
{
  object_service_fn_t *os = (object_service_fn_t *)arg;
  osrs_priv_t *osrs = (osrs_priv_t *)os->priv;
  mq_frame_t *fid, *fcred, *fdata;
  unsigned char *buffer;
  char *path;
  creds_t *creds;
  int fsize, n, err;
  int64_t timeout, ftype, resolution;
  mq_msg_t *msg, *response;
  op_generic_t *gop;
  op_status_t status;

  log_printf(5, "Processing incoming request\n");

  err = 0;
  status = op_failure_status;

  //** Parse the command.
  msg = task->msg;
  mq_remove_header(msg, 0);

  fid = mq_msg_pop(msg);  //** This is the ID
  mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

  fcred = mq_msg_pop(msg);  //** This has the creds
  creds = osrs_get_creds(os, fcred);

  fdata = mq_msg_pop(msg);  //** This has the path
  mq_get_frame(fdata, (void **)&buffer, &fsize);
  if (fsize > 0) {
     type_malloc(path, char, fsize+1);
     memcpy(path, buffer, fsize);
     path[fsize] = 0; //** NULL terminate the path name
  } else {
     err = 1;
  }
  mq_frame_destroy(fdata);

  fdata = mq_msg_pop(msg);  //** This has the ftype and resolution
  mq_get_frame(fdata, (void **)&buffer, &fsize);
  if (fsize > 0) {
    if (err == 0) {
       n = zigzag_decode(buffer, fsize, &ftype);
       n += zigzag_decode(&(buffer[n]), fsize-n, &resolution);
       n += zigzag_decode(&(buffer[n]), fsize-n, &timeout);
    }
  } else {
    err = 1;
  }
  mq_frame_destroy(fdata);

log_printf(5, "err=%d\n", err);
  if ((err != 0) || (creds == NULL)) {
     status = op_failure_status;
  } else {
     gop = os_fsck_object(osrs->os_child, creds, path, ftype, resolution);
     gop_waitall(gop);
     status = gop_get_status(gop);
     gop_free(gop, OP_DESTROY);
  }

  //** Form the response
  response = mq_make_response_core_msg(msg, fid);
  mq_msg_append_frame(response, mq_make_status_frame(status));
  mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

  //** Lastly send it
  mq_submit(osrs->server_portal, mq_task_new(osrs->mqc, response, NULL, NULL, 30));

  //** Clean up
  mq_frame_destroy(fcred);

  if (path != NULL) free(path);
}


//***********************************************************************
// os_remote_server_destroy
//***********************************************************************

void os_remote_server_destroy(object_service_fn_t *os)
{
  osrs_priv_t *osrs = (osrs_priv_t *)os->priv;

  //** Drop the fake creds
  an_cred_destroy(osrs->dummy_creds);
  if (osrs->authn != NULL) authn_destroy(osrs->authn);

  //** Remove the server portal
  mq_portal_remove(osrs->mqc, osrs->server_portal);

  //** Shutdown the ongoing thread and task
  mq_ongoing_destroy(osrs->ongoing);

  //** Now destroy it
  mq_portal_destroy(osrs->server_portal);

  //** Shutdown the child OS
  os_destroy_service(osrs->os_child);

  //** Now do the normal cleanup
  apr_pool_destroy(osrs->mpool);


  free(osrs->hostname);
  free(osrs);
  free(os);
}


//***********************************************************************
//  object_service_remote_client_create - Creates a remote client OS
//***********************************************************************

object_service_fn_t *object_service_remote_server_create(service_manager_t *ess, inip_file_t *fd, char *section)
{
  object_service_fn_t *os;
  osrs_priv_t *osrs;
  os_create_t *os_create;
  mq_command_table_t *ctable;
  char *stype, *ctype;
  authn_create_t *authn_create;
  char *cred_args[2];

log_printf(0, "START\n");
  if (section == NULL) section = "os_remote_client";

  type_malloc_clear(os, object_service_fn_t, 1);
  type_malloc_clear(osrs, osrs_priv_t, 1);
  os->priv = (void *)osrs;

  //** Make the locks and cond variables
  assert(apr_pool_create(&(osrs->mpool), NULL) == APR_SUCCESS);
  apr_thread_mutex_create(&(osrs->lock), APR_THREAD_MUTEX_DEFAULT, osrs->mpool);
  apr_thread_mutex_create(&(osrs->abort_lock), APR_THREAD_MUTEX_DEFAULT, osrs->mpool);

  osrs->abort = apr_hash_make(osrs->mpool);
  assert(osrs->abort != NULL);

  //** Get the host name we bind to
  osrs->hostname= inip_get_string(fd, section, "address", NULL);

  //** Ongoing check interval
  osrs->ongoing_interval = inip_get_integer(fd, section, "ongoing_interval", 300);

  //** Max Stream size
  osrs->max_stream = inip_get_integer(fd, section, "max_stream", 1024*1024);

  //** Start the child OS.
  stype = inip_get_string(fd, section, "os_local", NULL);
  if (stype == NULL) {  //** Oops missing child OS
     log_printf(0, "ERROR: Mising child OS  section=%s key=rs_local!\n", section); flush_log();
     free(stype);
     abort();
  }

  //** and load it
  ctype = inip_get_string(fd, stype, "type", OS_TYPE_FILE);
  os_create = lookup_service(ess, OS_AVAILABLE, ctype);
  osrs->os_child = (*os_create)(ess, fd, stype);
  if (osrs->os_child == NULL) {
     log_printf(1, "ERROR loading child OS!  type=%s section=%s\n", ctype, stype); flush_log();
     abort();
  }
  free(ctype);
  free(stype);

  //** Make the dummy credentials
  cred_args[0] = NULL;
  cred_args[1] = "FIXME_tacketar";
  authn_create = lookup_service(ess, AUTHN_AVAILABLE, AUTHN_TYPE_FAKE);
  osrs->authn = (*authn_create)(ess, fd, "missing");
  osrs->dummy_creds = authn_cred_init(osrs->authn, OS_CREDS_INI_TYPE, (void **)cred_args);
  an_cred_set_id(osrs->dummy_creds, cred_args[1]);


  //** Get the MQC
  assert((osrs->mqc = lookup_service(ess, ESS_RUNNING, ESS_MQ)) != NULL);

  //** Make the server portal
  osrs->server_portal = mq_portal_create(osrs->mqc, osrs->hostname, MQ_CMODE_SERVER);
  ctable = mq_portal_command_table(osrs->server_portal);
  mq_command_set(ctable, OSR_EXISTS_KEY, OSR_EXISTS_SIZE, os, osrs_exists_cb);
  mq_command_set(ctable, OSR_CREATE_OBJECT_KEY, OSR_CREATE_OBJECT_SIZE, os, osrs_create_object_cb);
  mq_command_set(ctable, OSR_REMOVE_OBJECT_KEY, OSR_REMOVE_OBJECT_SIZE, os, osrs_remove_object_cb);
  mq_command_set(ctable, OSR_REMOVE_REGEX_OBJECT_KEY, OSR_REMOVE_REGEX_OBJECT_SIZE, os, osrs_remove_regex_object_cb);
  mq_command_set(ctable, OSR_MOVE_OBJECT_KEY, OSR_MOVE_OBJECT_SIZE, os, osrs_move_object_cb);
  mq_command_set(ctable, OSR_SYMLINK_OBJECT_KEY, OSR_SYMLINK_OBJECT_SIZE, os, osrs_symlink_object_cb);
  mq_command_set(ctable, OSR_HARDLINK_OBJECT_KEY, OSR_HARDLINK_OBJECT_SIZE, os, osrs_hardlink_object_cb);
  mq_command_set(ctable, OSR_OPEN_OBJECT_KEY, OSR_OPEN_OBJECT_SIZE, os, osrs_open_object_cb);
  mq_command_set(ctable, OSR_CLOSE_OBJECT_KEY, OSR_CLOSE_OBJECT_SIZE, os, osrs_close_object_cb);
  mq_command_set(ctable, OSR_ABORT_OPEN_OBJECT_KEY, OSR_ABORT_OPEN_OBJECT_SIZE, os, osrs_abort_open_object_cb);
  mq_command_set(ctable, OSR_REGEX_SET_MULT_ATTR_KEY, OSR_REGEX_SET_MULT_ATTR_SIZE, os, osrs_regex_set_mult_attr_cb);
  mq_command_set(ctable, OSR_GET_MULTIPLE_ATTR_KEY, OSR_GET_MULTIPLE_ATTR_SIZE, os, osrs_get_mult_attr_cb);
  mq_command_set(ctable, OSR_SET_MULTIPLE_ATTR_KEY, OSR_SET_MULTIPLE_ATTR_SIZE, os, osrs_set_mult_attr_cb);
  mq_command_set(ctable, OSR_COPY_MULTIPLE_ATTR_KEY, OSR_COPY_MULTIPLE_ATTR_SIZE, os, osrs_copy_mult_attr_cb);
  mq_command_set(ctable, OSR_MOVE_MULTIPLE_ATTR_KEY, OSR_MOVE_MULTIPLE_ATTR_SIZE, os, osrs_move_mult_attr_cb);
  mq_command_set(ctable, OSR_SYMLINK_MULTIPLE_ATTR_KEY, OSR_SYMLINK_MULTIPLE_ATTR_SIZE, os, osrs_symlink_mult_attr_cb);
  mq_command_set(ctable, OSR_OBJECT_ITER_ALIST_KEY, OSR_OBJECT_ITER_ALIST_SIZE, os, osrs_object_iter_alist_cb);
  mq_command_set(ctable, OSR_OBJECT_ITER_AREGEX_KEY, OSR_OBJECT_ITER_AREGEX_SIZE, os, osrs_object_iter_aregex_cb);
  mq_command_set(ctable, OSR_ATTR_ITER_KEY, OSR_ATTR_ITER_SIZE, os, osrs_attr_iter_cb);
  mq_command_set(ctable, OSR_FSCK_ITER_KEY, OSR_FSCK_ITER_SIZE, os, osrs_fsck_iter_cb);
  mq_command_set(ctable, OSR_FSCK_OBJECT_KEY, OSR_FSCK_OBJECT_SIZE, os, osrs_fsck_object_cb);

  //** Make the ongoing checker
  osrs->ongoing = mq_ongoing_create(osrs->mqc, osrs->server_portal, osrs->ongoing_interval, ONGOING_SERVER);
  assert(osrs->ongoing != NULL);

  //** This is to handle client stream responses
  mq_command_set(ctable, MQS_MORE_DATA_KEY, MQS_MORE_DATA_SIZE, osrs->ongoing, mqs_server_more_cb);

  mq_portal_install(osrs->mqc, osrs->server_portal);

  //** Set up the fn ptrs.  This is just for shutdown
  //** so very little is implemented
  os->destroy_service = os_remote_server_destroy;

  os->type = OS_TYPE_REMOTE_SERVER;

log_printf(0, "END\n");

  return(os);
}

