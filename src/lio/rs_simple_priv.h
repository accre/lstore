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
// Simple resource managment implementation
//***********************************************************************

#include "list.h"
#include "data_service_abstract.h"
#include "opque.h"
#include "service_manager.h"

#ifndef _RS_SIMPLE_PRIV_H_
#define _RS_SIMPLE_PRIV_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  char *rid_key;
  char *ds_key;
  list_t *attr;
  int  status;
  int  slot;
  ex_off_t space_total;
  ex_off_t space_used;
  ex_off_t space_free;
} rss_rid_entry_t;

typedef struct {
  char *ds_key;
  char *rid_key;
  data_inquire_t *space;
  rss_rid_entry_t *re;
} rss_check_entry_t;

typedef struct {
  list_t *rid_table;
  rss_rid_entry_t **random_array;
  data_service_fn_t *ds;
  data_attr_t *da;
  apr_thread_mutex_t *lock;
  apr_thread_mutex_t *update_lock;
  apr_thread_cond_t *cond;
  apr_thread_t *check_thread;
  apr_pool_t *mpool;
  apr_hash_t *mapping_updates;
  apr_hash_t *rid_mapping;
  time_t modify_time;
  time_t current_check;
  char *fname;
  uint64_t min_free;
  int n_rids;
  int shutdown;
  int dynamic_mapping;
  int unique_rids;
  int check_interval;
  int check_timeout;
  int last_config_size;
} rs_simple_priv_t;

#ifdef __cplusplus
}
#endif

#endif

