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
// MQ Ongoing task implementation
//***********************************************************************

#define _log_module_index 222

#include "opque.h"
#include "mq_ongoing.h"
#include "mq_helpers.h"
#include "type_malloc.h"
#include "log.h"

//***********************************************************************
// mq_ongoing_add - Adds an onging object to the tracking tables
//***********************************************************************

mq_ongoing_object_t *mq_ongoing_add(mq_ongoing_t *mqon, char *id, int id_len, void *handle, mq_ongoing_fail_t *on_fail, void *on_fail_arg)
{
  mq_ongoing_object_t *ongoing;
  mq_ongoing_host_t *oh;

  type_malloc(ongoing, mq_ongoing_object_t, 1);
  ongoing->handle = handle;
  ongoing->key = (intptr_t)handle;
  ongoing->on_fail = on_fail;
  ongoing->on_fail_arg = on_fail_arg;
  ongoing->count = 0;

  log_printf(5, "host=%s len=%d handle=%p key=%" PRIdPTR "\n", id, id_len, handle, ongoing->key);

  apr_thread_mutex_lock(mqon->lock);

  //** Make sure the host entry exists
  oh = apr_hash_get(mqon->id_table, id, id_len);
  if (oh == NULL) {
     log_printf(5, "new host=%s\n", id);

     type_malloc(oh, mq_ongoing_host_t, 1);
     type_malloc(oh->id, char, id_len+1);
     memcpy(oh->id, id, id_len);
     oh->id[id_len] = 0;  //** NULL terminate the string
     oh->id_len = id_len;

     oh->heartbeat = 600;
     sscanf(id, "%d:", &(oh->heartbeat));
     log_printf(5, "heartbeat interval=%d\n", oh->heartbeat);
     oh->next_check = apr_time_now() + apr_time_from_sec(oh->heartbeat);
     assert(apr_pool_create(&(oh->mpool), NULL) == APR_SUCCESS);
     oh->table = apr_hash_make(oh->mpool);

     apr_hash_set(mqon->id_table, oh->id, id_len, oh);
  }

  //** Add the object
  apr_hash_set(oh->table, &(ongoing->key), sizeof(intptr_t), ongoing);

  apr_thread_mutex_unlock(mqon->lock);

  return(ongoing);
}


//***********************************************************************
// mq_ongoing_get - Retreives the handle if it's active
//***********************************************************************

void *mq_ongoing_get(mq_ongoing_t *mqon, char *id, int id_len, intptr_t key)
{
  mq_ongoing_object_t *ongoing;
  mq_ongoing_host_t *oh;
  void *ptr = NULL;

  apr_thread_mutex_lock(mqon->lock);

  //** Get the host entry
  oh = apr_hash_get(mqon->id_table, id, id_len);
log_printf(6, "looking for host=%s len=%d oh=%p key=%" PRIdPTR "\n", id, id_len, oh, key);
  if (oh != NULL) {
     ongoing = apr_hash_get(oh->table, &key, sizeof(intptr_t));  //** Lookup the object
     if (ongoing != NULL) {
        log_printf(6, "Found!\n");
        ptr = ongoing->handle;
        ongoing->count++;
        oh->next_check = apr_time_now() + apr_time_from_sec(oh->heartbeat);
     }
  }

  apr_thread_mutex_unlock(mqon->lock);

  return(ptr);
}

//***********************************************************************
// mq_ongoing_release - Releases the handle active handle
//***********************************************************************

void mq_ongoing_release(mq_ongoing_t *mqon, char *id, int id_len, intptr_t key)
{
  mq_ongoing_object_t *ongoing;
  mq_ongoing_host_t *oh;

  apr_thread_mutex_lock(mqon->lock);

  //** Get the host entry
  oh = apr_hash_get(mqon->id_table, id, id_len);
log_printf(6, "looking for host=%s len=%d oh=%p key=%" PRIdPTR "\n", id, id_len, oh, key);
  if (oh != NULL) {
     ongoing = apr_hash_get(oh->table, &key, sizeof(intptr_t));  //** Lookup the object
     if (ongoing != NULL) {
        log_printf(6, "Found!\n");
        ongoing->count--;
        oh->next_check = apr_time_now() + apr_time_from_sec(oh->heartbeat);

        apr_thread_cond_broadcast(mqon->cond); //** Let everyone know it;'s been released
     }
  }

  apr_thread_mutex_unlock(mqon->lock);

  return;
}

//***********************************************************************
// mq_ongoing_remove - Removes an onging object from the tracking table
//***********************************************************************

void *mq_ongoing_remove(mq_ongoing_t *mqon, char *id, int id_len, intptr_t key)
{
  mq_ongoing_object_t *ongoing;
  mq_ongoing_host_t *oh;
//  intptr_t key;
  void *ptr = NULL;

  apr_thread_mutex_lock(mqon->lock);

  //** Get the host entry
  oh = apr_hash_get(mqon->id_table, id, id_len);
log_printf(6, "looking for host=%s len=%d oh=%p key=%" PRIdPTR "\n", id, id_len, oh, key);
  if (oh != NULL) {
log_printf(6, "Found host=%s key=%" PRIdPTR "\n", oh->id, key);

     ongoing = apr_hash_get(oh->table, &key, sizeof(intptr_t));  //** Lookup the object
     if (ongoing != NULL) {
log_printf(6, "Found handle\n");
         ptr = ongoing->handle;
         while (ongoing->count > 0) {
            apr_thread_cond_wait(mqon->cond, mqon->lock);
         }
         apr_hash_set(oh->table, &key, sizeof(intptr_t), NULL);
         free(ongoing);
     }
  }

  apr_thread_mutex_unlock(mqon->lock);

  return(ptr);
}


//***********************************************************************
// mq_ongoing_cb - Processes a heartbeat for clients with ongoing open handles
//***********************************************************************

void mq_ongoing_cb(void *arg, mq_task_t *task)
{
  mq_ongoing_t *mqon = (mq_ongoing_t *)arg;
  mq_frame_t *fid, *fuid;
  char *id;
  mq_ongoing_host_t *oh;
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

  //** Do the lookup and update the heartbeat timeout
  apr_thread_mutex_lock(mqon->lock);
  oh = apr_hash_get(mqon->id_table, id, fsize);
  if (oh != NULL) {
    log_printf(5, "Updating heartbeat for %s\n", id);
    status = op_success_status;
    oh->next_check = apr_time_now() + apr_time_from_sec(oh->heartbeat);
  } else {
    status = op_failure_status;
  }
  apr_thread_mutex_unlock(mqon->lock);

  mq_frame_destroy(fuid);

  //** Form the response
  response = mq_make_response_core_msg(msg, fid);
  mq_msg_append_frame(response, mq_make_status_frame(status));
  mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

  //** Lastly send it
  mq_submit(mqon->server_portal, mq_task_new(mqon->mqc, response, NULL, NULL, 30));
}

//***********************************************************************
// _mq_ongoing_close - Closes all ongoing objects associated with the connection
//     NOTE:  Assumes the ongoing_lock is held by the calling process
//***********************************************************************

void _mq_ongoing_close(mq_ongoing_t *mqon, mq_ongoing_host_t *oh)
{
  apr_hash_index_t *hi;
  char *key;
  apr_ssize_t klen;
  mq_ongoing_object_t *oo;
  op_generic_t *gop;
  opque_t *q;

log_printf(5, "closing host now=" TT " next_check=" TT " hb=%d\n", apr_time_now(), oh->next_check, oh->heartbeat);
  q = new_opque();
  opque_start_execution(q);

  for (hi = apr_hash_first(NULL, oh->table); hi != NULL; hi = apr_hash_next(hi)) {
     apr_hash_this(hi, (const void **)&key, &klen, (void **)&oo);
//     apr_hash_set(oh->table, key, klen, NULL);

     gop = oo->on_fail(oo->on_fail_arg, oo->handle);
     gop_set_private(gop, oo);
     opque_add(q, gop);
  }

  while ((gop = opque_waitany(q)) != NULL) {
//     oo = gop_get_private(gop);
//     free(oo);
     gop_free(gop, OP_DESTROY);
  }

  opque_free(q, OP_DESTROY);
}

//***********************************************************************
// mq_ongoing_thread - Checks to make sure heartbeats are being
//     being received from clients with open handles
//***********************************************************************

void *mq_ongoing_thread(apr_thread_t *th, void *data)
{
  mq_ongoing_t *mqon = (mq_ongoing_t *)data;
  apr_hash_index_t *hi;
  mq_ongoing_host_t *oh;
  char *key;
  apr_ssize_t klen;
  apr_time_t now, timeout;
  int n;

  timeout = apr_time_make(mqon->check_interval, 0);

  n = 0;
  do {
    //** Cycle through checking each host's heartbeat
    now = apr_time_now();
    for (hi = apr_hash_first(NULL, mqon->id_table); hi != NULL; hi = apr_hash_next(hi)) {
       apr_hash_this(hi, (const void **)&key, &klen, (void **)&oh);

       if (oh->next_check < now) { //** Expired heartbeat so shut everything associated with the connection
           _mq_ongoing_close(mqon, oh);
//           apr_hash_set(mqon->id_table, key, klen, NULL);
//           free(oh->id);
//           apr_pool_destroy(oh->mpool);
//           free(oh);
       }
    }

    //** Sleep until time for the next heartbeat or time to exit
    apr_thread_mutex_lock(mqon->lock);
    apr_thread_cond_timedwait(mqon->cond, mqon->lock, timeout);
    n = mqon->shutdown;
    apr_thread_mutex_unlock(mqon->lock);

log_printf(15, "loop end n=%d\n", n);
  } while (n == 0);

log_printf(15, "EXITING\n");

  return(NULL);
}

//***********************************************************************
// mq_ongoing_destroy - Destroys an ongoing task tracker
//***********************************************************************

void mq_ongoing_destroy(mq_ongoing_t *mqon)
{
  apr_status_t value;
  apr_hash_index_t *hi;
  mq_ongoing_host_t *oh;
  char *key;
  apr_ssize_t klen;

  //** Wake up the ongoing thread
  apr_thread_mutex_lock(mqon->lock);
  mqon->shutdown = 1;
  apr_thread_cond_broadcast(mqon->cond);
  apr_thread_mutex_unlock(mqon->lock);

  //** Wait for it to shutdown
  apr_thread_join(&value, mqon->ongoing_thread);

  //** Close all ongoing processes
  for (hi = apr_hash_first(NULL, mqon->id_table); hi != NULL; hi = apr_hash_next(hi)) {
     apr_hash_this(hi, (const void **)&key, &klen, (void **)&oh);
     _mq_ongoing_close(mqon, oh);
     apr_hash_set(mqon->id_table, key, klen, NULL);
     free(oh->id);
     apr_pool_destroy(oh->mpool);
     free(oh);
  }

  apr_pool_destroy(mqon->mpool);
  free(mqon);
}

//***********************************************************************
// mq_ongoing_create - Creates an object to handle ongoing tasks
//***********************************************************************

mq_ongoing_t *mq_ongoing_create(mq_context_t *mqc, mq_portal_t *server_portal, int check_interval)
{
  mq_ongoing_t *mqon;

  type_malloc_clear(mqon, mq_ongoing_t, 1);

  mqon->mqc = mqc;
  mqon->server_portal = server_portal;
  mqon->check_interval = check_interval;

  assert(apr_pool_create(&(mqon->mpool), NULL) == APR_SUCCESS);
  apr_thread_mutex_create(&(mqon->lock), APR_THREAD_MUTEX_DEFAULT, mqon->mpool);
  apr_thread_cond_create(&(mqon->cond), mqon->mpool);

  mqon->id_table = apr_hash_make(mqon->mpool);
  assert(mqon->id_table != NULL);

  apr_thread_create(&(mqon->ongoing_thread), NULL, mq_ongoing_thread, (void *)mqon, mqon->mpool);
  return(mqon);
}


