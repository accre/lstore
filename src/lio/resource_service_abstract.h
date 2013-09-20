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
// Generic resource managment service
//***********************************************************************


#ifndef _RESOURCE_SERVICE_H_
#define _RESOURCE_SERVICE_H_

#include "ex3_types.h"
#include "data_service_abstract.h"

#ifdef __cplusplus
extern "C" {
#endif

#define RS_SM_AVAILABLE "rs_available"
#define RS_SM_RUNNING   "rs_running"

#define RS_ERROR_OK                  -100 //** No error everything is good
#define RS_ERROR_NOT_ENOUGH_RIDS     -101 //** Didn't find enough RIDs for the request
#define RS_ERROR_FIXED_MATCH_FAIL    -102 //** The fixed RID failed to match the request
#define RS_ERROR_FIXED_NOT_FOUND     -103 //** The fixed RID couldn't be located in the RID table
#define RS_ERROR_HINTS_INVALID_LOCAL -104 //** The fixed RID's local RSQ contained a pickone or unique which isn't supported
#define RS_ERROR_EMPTY_STACK         -105 //** The Query stack is empty so the query is probably malformed

#define RS_STATUS_UP           0  //** The RID is enabled and available for use
#define RS_STATUS_IGNORE       1  //** Disabled via config file
#define RS_STATUS_OUT_OF_SPACE 2  //** The RID is disabled due to space
#define RS_STATUS_DOWN         3  //** Can't connect to RID

typedef struct resource_service_fn_s resource_service_fn_t;

typedef struct {
  int n_rids_total;
  int n_rids_free;
  int n_rids_status[3];
  int64_t used_total;   //** *_total is for everything in the config file.  Ignoring state
  int64_t free_total;
  int64_t total_total;
  int64_t used_up;;     //** Just totals for depot up, i.e. RS_STATUS_ON
  int64_t free_up;
  int64_t total_up;
} rs_space_t;

typedef void rs_query_t;

typedef struct {  //** Used for passing existing RID's and individual queries to the RS requestor and validation or repair
  char *fixed_rid_key;  //** RID key for existing/fixed index
  int  status;    //** Status of the fixed match or INVALID_LOCAL if a problem occurs with the local_rsq.  Returns one of the error codes above
  rs_query_t *local_rsq;  //** Local query appended to the global queury just for this allocation  used for both fixed and new
} rs_hints_t;

typedef struct {
  op_generic_t *gop;
  int rid_index;
  ex_off_t size;
  char *rid_key;
} rs_request_t;

typedef struct {
  apr_thread_mutex_t *lock;
  apr_thread_cond_t *cond;
  int map_version;
  int status_version;
} rs_mapping_notify_t;

struct resource_service_fn_s {
  void *priv;
  char *type;
  char *(*get_rid_value)(resource_service_fn_t *arg, char *rid_key, char *key);
  char *(*get_rid_config)(resource_service_fn_t *arg);
  void (*translate_cap_set)(resource_service_fn_t *arg, char *rid_key, data_cap_set_t *cap);
  void (*register_mapping_updates)(resource_service_fn_t *arg, rs_mapping_notify_t *map_version);
  void (*unregister_mapping_updates)(resource_service_fn_t *arg, rs_mapping_notify_t *map_version);
  int  (*query_add)(resource_service_fn_t *arg, rs_query_t **q, int op, char *key, int key_op, char *val, int val_op);
  void (*query_append)(resource_service_fn_t *arg, rs_query_t *q, rs_query_t *qappend);
  rs_query_t *(*query_dup)(resource_service_fn_t *arg, rs_query_t *q);
  rs_query_t *(*query_new)(resource_service_fn_t *arg);
  void (*query_destroy)(resource_service_fn_t *arg, rs_query_t *q);
  op_generic_t *(*data_request)(resource_service_fn_t *arg, data_attr_t *da, rs_query_t *q, data_cap_set_t **caps, rs_request_t *req, int req_size, rs_hints_t *hints_list, int fixed_size, int n_rid, int timeout);
  rs_query_t *(*query_parse)(resource_service_fn_t *arg, char *value);
  char *(*query_print)(resource_service_fn_t *arg, rs_query_t *q);
  void (*destroy_service)(resource_service_fn_t *rs);
};

typedef resource_service_fn_t *(rs_create_t)(void *arg, inip_file_t *ifd, char *section);


#define rs_type(rs)  (rs)->type
#define rs_get_rid_value(rs, rid_key, key) (rs)->get_rid_value(rs, rid_key, key)
#define rs_get_rid_config(rs) (rs)->get_rid_config(rs)
#define rs_translate_cap_set(rs, rid_key, cs) (rs)->translate_cap_set(rs, rid_key, cs)
#define rs_register_mapping_updates(rs, notify) (rs)->register_mapping_updates(rs, notify)
#define rs_unregister_mapping_updates(rs, notify) (rs)->unregister_mapping_updates(rs, notify)
#define rs_query_add(rs, q, op, key, kop, val, vop) (rs)->query_add(rs, q, op, key, kop, val, vop)
#define rs_query_append(rs, q, qappend) (rs)->query_append(rs, q, qappend)
#define rs_query_new(rs) (rs)->query_new(rs)
#define rs_query_dup(rs, q) (rs)->query_dup(rs, q)
#define rs_query_destroy(rs, q) (rs)->query_destroy(rs, q)
#define rs_query_parse(rs, value) (rs)->query_parse(rs, value)
#define rs_query_print(rs, q) (rs)->query_print(rs, q)
#define rs_data_request(rs, da, q, caps, req, n_req, hints_list, fixed_size, n_rid, to) (rs)->data_request(rs, da, q, caps, req, n_req, hints_list, fixed_size, n_rid, to)
#define rs_destroy_service(rs) (rs)->destroy_service(rs)

rs_space_t rs_space(char *config);

void resource_service_destroy(resource_service_fn_t *rsf);

#ifdef __cplusplus
}
#endif

#endif

