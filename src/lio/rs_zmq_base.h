/*
Advanced Computing Center for Research and Education Proprietary License
Version 1.0 (July 2012)

Copyright (c) 2012, Advanced Computing Center for Research and Education,
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
// ZMQ resource managment implementation
//***********************************************************************

#ifndef _RS_ZMQ_BASE_H_
#define _RS_ZMQ_BASE_H_

//#define DEBUG
#ifdef __cplusplus
extern "C" {
#endif

#include <assert.h>
#include "assert_result.h"
#include <sys/uio.h>  //** ZMQ uses struct iovec but fogets to include the header
#include <zmq.h>
#include "resource_service_abstract.h"
#include "rs_query_base.h"
#include "rs_zmq_priv.h"
#include "iniparse.h"
#include "log.h"
#include "type_malloc.h"
#include "string_token.h"
#include "random.h"
#include "ds_ibp.h" //** This should be replaced by a generic data servic
#include "rsz_request.pb-c.h"
#include "rsz_response.pb-c.h"
#include "rsz_rid_key_value.pb-c.h"
#include "rs_simple_priv.h"
 
//** Contains rs request 
typedef struct {
resource_service_fn_t *rs;
data_attr_t *da;
rs_query_t *rsq;
data_cap_set_t **caps;
rs_request_t *req;
int req_size;
rs_hints_t *hints_list;
int fixed_size;
int n_rid;
int timeout;
} rs_zmq_req_t;
 
//** Contains rs reponse
typedef struct {
data_service_fn_t *ds;
data_cap_set_t **caps;
rs_request_t *req;
int n_ele;
} rs_zmq_rep_t;
 
//** Contains thread arguments
typedef struct {
void *zmq_context;
resource_service_fn_t *rs;
data_service_fn_t *ds;
data_attr_t *da;
int timeout;
} rs_zmq_thread_arg_t;
 
//** Contains rs rid keys
typedef struct {
char *rid_key;
char *key;
} rs_zmq_rid_key_t;
 
//** Contains rs rid value
typedef struct {
char *rid_value;
} rs_zmq_rid_value_t;
 
void *rs_zmq_worker_routine(void *arg);
int rs_zmq_send(void *socket, void *buf, int len);
int rs_zmq_recv(void *socket, void **buf);
 
Rs__Zmq__RszReqSet **rs_zmq_reqlist_serialize(rs_request_t *req, int num_ele);
rs_request_t *rs_zmq_reqlist_deserialize(Rs__Zmq__RszReqSet **reqlist, int num_ele);
void rs_zmq_reqlist_destroy(Rs__Zmq__RszReqSet **req_set, int num_ele);
Rs__Zmq__RszCapSet **rs_zmq_caplist_serialize(data_service_fn_t *ds, data_cap_set_t **caps, int num_ele);
data_cap_set_t **rs_zmq_caplist_deserialize(data_service_fn_t *ds, Rs__Zmq__RszCapSet **caplist, int num_ele);
void rs_zmq_caplist_destroy(Rs__Zmq__RszCapSet **cap_set, int num_ele);
void *rs_zmq_req_serialize(rs_zmq_req_t *req, int *len);
void rs_zmq_req_deserialize(rs_zmq_req_t *req, void *buf, int len);
int rs_zmq_req_recv(rs_zmq_req_t *req, void *socket);
void *rs_zmq_rep_serialize(rs_zmq_rep_t *rep, int *len);
void rs_zmq_rep_deserialize(rs_zmq_rep_t *rep, void *buf, int len);
int rs_zmq_rep_send(rs_zmq_rep_t* response, void *socket);
int rs_zmq_rep_recv(rs_zmq_rep_t* rep, void *socket);
char *rs_query_type_replace(char *query, char *old_rst, char *new_rst);
 
void *rs_zmq_ridkey_serialize(rs_zmq_rid_key_t *keys, int *len);
void rs_zmq_ridkey_deserialize(rs_zmq_rid_key_t *keys, void *buf, int len);
void *rs_zmq_ridvalue_serialize(rs_zmq_rid_value_t *value, int *len);
void rs_zmq_ridvalue_deserialize(rs_zmq_rid_value_t *value, void *buf, int len);
 
int rs_zmq_ridkey_send(rs_zmq_rid_key_t *keys, void *socket);
int rs_zmq_ridvalue_send(rs_zmq_rid_value_t *value, void *socket);
int rs_zmq_ridkey_recv(rs_zmq_rid_key_t *keys, void *socket);
int rs_zmq_ridvalue_recv(rs_zmq_rid_value_t *value, void *socket);
 
void rs_zmq_send_rid_value(resource_service_fn_t *rs, void *socket);
#ifdef __cplusplus
}
#endif
 
#endif
 