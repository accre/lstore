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
// MQ ongoing task management header
//***********************************************************************

#include "opque.h"
#include "mq_portal.h"

#ifndef _MQ_ONGOING_H_
#define _MQ_ONGOING_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef op_generic_t *(mq_ongoing_fail_t)(void *arg, void *handle);

typedef struct {
  int type;
  int count;
  void *handle;
  intptr_t key;
  mq_ongoing_fail_t *on_fail;
  void *on_fail_arg;
} mq_ongoing_object_t;

typedef struct {
  int heartbeat;
  apr_time_t next_check;
  char *id;
  int id_len;
  apr_hash_t *table;
  apr_pool_t *mpool;
} mq_ongoing_host_t;

typedef struct {
  apr_pool_t *mpool;
  apr_thread_mutex_t *lock;
  apr_thread_cond_t *cond;
  apr_hash_t *id_table;
  apr_thread_t *ongoing_thread;
  mq_context_t *mqc;
  mq_portal_t *server_portal;
  int check_interval;
  int shutdown;
} mq_ongoing_t;

void mq_ongoing_cb(void *arg, mq_task_t *task);
mq_ongoing_object_t *mq_ongoing_add(mq_ongoing_t *mqon, char *id, int id_len, void *handle, mq_ongoing_fail_t *on_fail, void *on_fail_arg);
void *mq_ongoing_remove(mq_ongoing_t *mqon, char *id, int id_len, intptr_t key);
void *mq_ongoing_get(mq_ongoing_t *mqon, char *id, int id_len, intptr_t key);
void mq_ongoing_release(mq_ongoing_t *mqon, char *id, int id_len, intptr_t key);
mq_ongoing_t *mq_ongoing_create(mq_context_t *mqc, mq_portal_t *server_portal, int check_interval);
void mq_ongoing_destroy(mq_ongoing_t *mqon);

#ifdef __cplusplus
}
#endif

#endif

