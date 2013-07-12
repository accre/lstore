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
// OS Remote Server private header file
//***********************************************************************

#include "object_service_abstract.h"
#include "authn_abstract.h"
#include "mq_portal.h"
#include "mq_ongoing.h"

#ifndef _OS_REMOTE_PRIV_H_
#define _OS_REMOTE_PRIV_H_

#ifdef __cplusplus
extern "C" {
#endif

       //*** List the various OS commands
#define OSR_EXISTS_KEY             "os_exists"
#define OSR_EXISTS_SIZE            9
#define OSR_CREATE_OBJECT_KEY      "os_create_object"
#define OSR_CREATE_OBJECT_SIZE     16
#define OSR_REMOVE_OBJECT_KEY      "os_remove_object"
#define OSR_REMOVE_OBJECT_SIZE     16
#define OSR_REMOVE_REGEX_OBJECT_KEY  "os_remove_regex_object"
#define OSR_REMOVE_REGEX_OBJECT_SIZE 22
#define OSR_MOVE_OBJECT_KEY        "os_move_object"
#define OSR_MOVE_OBJECT_SIZE       14
#define OSR_SYMLINK_OBJECT_KEY     "os_symlink_object"
#define OSR_SYMLINK_OBJECT_SIZE    17
#define OSR_HARDLINK_OBJECT_KEY    "os_hardlink_object"
#define OSR_HARDLINK_OBJECT_SIZE   18
#define OSR_OPEN_OBJECT_KEY        "os_open_object"
#define OSR_OPEN_OBJECT_SIZE       14
#define OSR_ABORT_OPEN_OBJECT_KEY  "os_abort_open_object"
#define OSR_ABORT_OPEN_OBJECT_SIZE 20
#define OSR_CLOSE_OBJECT_KEY       "os_close_object"
#define OSR_CLOSE_OBJECT_SIZE      15
#define OSR_ONGOING_KEY            "os_ongoing"
#define OSR_ONGOING_SIZE           10
#define OSR_REGEX_SET_MULT_ATTR_KEY  "os_regex_set_mult_object"
#define OSR_REGEX_SET_MULT_ATTR_SIZE 24
#define OSR_GET_MULTIPLE_ATTR_KEY  "os_get_mult"
#define OSR_GET_MULTIPLE_ATTR_SIZE 11
#define OSR_SET_MULTIPLE_ATTR_KEY  "os_set_mult"
#define OSR_SET_MULTIPLE_ATTR_SIZE 11
#define OSR_COPY_MULTIPLE_ATTR_KEY  "os_copy_mult"
#define OSR_COPY_MULTIPLE_ATTR_SIZE 12
#define OSR_MOVE_MULTIPLE_ATTR_KEY  "os_move_mult"
#define OSR_MOVE_MULTIPLE_ATTR_SIZE 12
#define OSR_SYMLINK_MULTIPLE_ATTR_KEY  "os_symlink_mult"
#define OSR_SYMLINK_MULTIPLE_ATTR_SIZE 15
#define OSR_OBJECT_ITER_ALIST_KEY   "os_object_iter_alist"
#define OSR_OBJECT_ITER_ALIST_SIZE  20
#define OSR_OBJECT_ITER_AREGEX_KEY  "os_object_iter_aregex"
#define OSR_OBJECT_ITER_AREGEX_SIZE  21
#define OSR_ATTR_ITER_KEY           "os_attr_iter"
#define OSR_ATTR_ITER_SIZE          12
#define OSR_FSCK_ITER_KEY           "os_fsck_iter"
#define OSR_FSCK_ITER_SIZE          12
#define OSR_FSCK_OBJECT_KEY         "os_fsck_object"
#define OSR_FSCK_OBJECT_SIZE        14

        //** Types of ongoing objects stored
#define OSR_ONGOING_FD_TYPE    0
#define OSR_ONGOING_OBJECT_ITER 1
#define OSR_ONGOING_ATTR_ITER   2
#define OSR_ONGOING_FSCK_ITER   3

typedef struct {
  object_service_fn_t *os_child;  //** Actual OS used
  apr_thread_mutex_t *lock;
  apr_thread_mutex_t *abort_lock;
  apr_thread_cond_t *cond;
  apr_pool_t *mpool;
  mq_context_t *mqc;          //** Portal for connecting to he remote OS server
  mq_ongoing_t *ongoing;      //** Ongoing open files or iterators
  apr_hash_t *abort;          //** Abort open handles
  char *hostname;             //** Addres to bind to
  mq_portal_t *server_portal;
  int ongoing_interval;       //** Ongoing command check interval
  int shutdown;
  int max_stream;
} osrs_priv_t;

typedef struct {
  object_service_fn_t *os_temp;  //** Used only for initial debugging of the client/server
  object_service_fn_t *os_remote;//** Used only for initial debugging of the client/server
  apr_thread_mutex_t *lock;
  apr_thread_cond_t *cond;
  mq_context_t *mqc;             //** Portal for connecting to he remote OS server
  char *remote_host;             //** Address of the Remote OS server
  char *host_id;                 //** Used for forming the open handle id;
  int host_id_len;               //** Length of host id
  os_authz_t *osaz;
  authn_t *authn;
  apr_pool_t *mpool;
  apr_thread_t *heartbeat_thread;
  int timeout;
  int heartbeat;
  int shutdown;
  int max_stream;
} osrc_priv_t;

#ifdef __cplusplus
}
#endif

#endif

