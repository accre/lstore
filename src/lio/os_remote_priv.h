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
//#include "chksum.h"
//#include <openssl/md5.h>

#ifndef _OS_REMOTE_PRIV_H_
#define _OS_REMOTE_PRIV_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  object_service_fn_t *os_child;  //** Actual OS used
  apr_thread_mutex_t *lock;
  apr_thread_cond_t *cond;
  apr_pool_t *mpool;
  mq_context_t *mqc;          //** Portal for connecting to he remote OS server
  char *hostname;             //** Addres to bind to
  mq_portal_t *server_portal;
} osrs_priv_t;

typedef struct {
  object_service_fn_t *os_temp;  //** Used only for initial debugging of the client/server
  mq_context_t *mqc;          //** Portal for connecting to he remote OS server
  char *host_ros;              //** ADdress of the Remote OS server
  os_authz_t *osaz;
  authn_t *authn;
  apr_pool_t *mpool;
} osrc_priv_t;


#ifdef __cplusplus
}
#endif

#endif

