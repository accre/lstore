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

//** Randomly selects an index larger than all existing indexs
#define _log_module_index 183

#include <assert.h>
#include <sys/uio.h>
#include <zmq.h>
#include "ex3_system.h"
#include "list.h"
#include "resource_service_abstract.h"
#include "rs_query_base.h"
#include "rs_zmq.h"
#include "rs_zmq_priv.h"
#include "iniparse.h"
#include "log.h"
#include "stack.h"
#include "type_malloc.h"
#include "random.h"
#include "rsz_request.pb-c.h"
#include "zhelpers.h"
#include "ds_ibp.h" //** This should be replaced by a generic data service
#include "thread_pool.h"
#include "rs_zmq_base.h"
 
//********************************************************************
// rs_zmq_req_func - Does the actual zmq RS request operation 
//********************************************************************
op_status_t rs_zmq_req_func(void *arg, int id)
{
//** Serializes the request
int len;
void *buf;
rs_zmq_req_t *req;
req = (rs_zmq_req_t *)arg;
buf = rs_zmq_req_serialize(req, &len);
 
printf("Length of serialized request %d bytes\n", len);
 
//** Sends serialized request through ZMQ
rs_zmq_priv_t *rsz = (rs_zmq_priv_t *) req->rs->priv;
 
int rc;
rc = zmq_connect(rsz->zmq_socket, rsz->zmq_svr);
assert(rc == 0);
 
rc = zmq_send(rsz->zmq_socket, buf, len , 0);
assert(rc == len);
 
printf("Sent request %d bytes\n", len);
 
//** Receives response through ZMQ 
//** ReqList and CapList are filled in here
rs_zmq_rep_t *rep;
type_malloc_clear(rep, rs_zmq_rep_t, 1);
rep->ds = rsz->ds;
 
len = rs_zmq_rep_recv(rep, rsz->zmq_socket);
 
//** Print the caps
char *qstr = rs_query_base_print(req->rs, req->rsq);
printf("---------------------------------------------------------------------\n");
printf("\nQuery: %s  n_alloc: %d\n", qstr, req->n_rid);
printf("\n");
int i;
for (i=0; i < rep->n_ele; i++) {
printf("%d.\tRID key: %s\n", i, rep->req[i].rid_key);
printf("\tRead  : %s\n", (char *)ds_get_cap(rep->ds, rep->caps[i], DS_CAP_READ));
printf("\tWrite : %s\n", (char *)ds_get_cap(rep->ds, rep->caps[i], DS_CAP_WRITE));
printf("\tManage: %s\n", (char *)ds_get_cap(rep->ds, rep->caps[i], DS_CAP_MANAGE));
 
ds_set_cap(rep->ds, req->caps[i], DS_CAP_READ, ds_get_cap(rep->ds, rep->caps[i], DS_CAP_READ));
ds_set_cap(rep->ds, req->caps[i], DS_CAP_WRITE, ds_get_cap(rep->ds, rep->caps[i], DS_CAP_WRITE));
ds_set_cap(rep->ds, req->caps[i], DS_CAP_MANAGE, ds_get_cap(rep->ds, rep->caps[i], DS_CAP_MANAGE));
ds_set_cap(rep->ds, rep->caps[i], DS_CAP_READ, NULL);
ds_set_cap(rep->ds, rep->caps[i], DS_CAP_WRITE, NULL);
ds_set_cap(rep->ds, rep->caps[i], DS_CAP_MANAGE, NULL);
}
printf("\n");
 
//** Destroy the caps and reqs
for (i=0; i<rep->n_ele; i++) {
ds_cap_set_destroy(rep->ds, rep->caps[i], 1);
free(rep->req[i].rid_key);
}
free(rep->caps);
free(rep->req);
free(rep);
 
//** Destroy allocatend space
free(buf);
free(qstr);
return op_success_status;
}
 
//********************************************************************
// rs_zmq_request - Requests a zmq resource service 
//********************************************************************
op_generic_t *rs_zmq_request(resource_service_fn_t *arg, data_attr_t *da, rs_query_t *rsq, data_cap_set_t **caps, rs_request_t *req, int req_size, rs_hints_t *hints_list, int fixed_size, int n_rid, int timeout)
{
//** Allocates handle
rs_zmq_req_t *rsr;  //** Is the fifth argument of new_thread_pool gonna free this space????
type_malloc_clear(rsr, rs_zmq_req_t, 1);
 
//** Fills in handle
rsr->rs = arg;
rsr->da = da;
rsr->rsq = rsq;
rsr->caps = caps;
rsr->req = req;
rsr->req_size = req_size;
rsr->hints_list = hints_list;
rsr->fixed_size = fixed_size;
rsr->n_rid = n_rid;
rsr->timeout = timeout;
 
//** Returns new op
op_generic_t *gop;
rs_zmq_priv_t *rsz = (rs_zmq_priv_t *)arg->priv;
 
//** Which thread context am I supposed to use?  
gop = new_thread_pool_op(rsz->tpc, NULL, rs_zmq_req_func, (void *)rsr, free, 1);
 
return(gop);
}
 
//********************************************************************
// rs_zmq_get_rid_value - Sends a zmq request to find the value 
// associated with the RID key provided. 
//********************************************************************
char *rs_zmq_get_rid_value(resource_service_fn_t *arg, char *rid_key, char *key) 
{
 
rs_zmq_priv_t *rsz = (rs_zmq_priv_t *)arg->priv;
 
//** Sends the ridkey 
int len;
rs_zmq_rid_key_t *keys;
type_malloc_clear(keys, rs_zmq_rid_key_t, 1);
keys->rid_key = rid_key;
keys->key = key;
len = rs_zmq_ridkey_send(keys, rsz->zmq_socket);
 
printf("Length of serialized ridkey: %d bytes\n", len);
fflush(stdout);
 
//** Receives ridvalue 
rs_zmq_rid_value_t *value;
type_malloc_clear(value, rs_zmq_rid_value_t, 1);
len = rs_zmq_ridvalue_recv(value, rsz->zmq_socket);
printf("Length of recvd ridvalue:%d bytes\n", len);
fflush(stdout);
 
char *rid_value;
 
asprintf(&rid_value, "%s", value->rid_value);
 
//** Frees allocations
free(keys);
free(value->rid_value);
free(value);
 
return rid_value;
}
 
//********************************************************************
// rs_zmq_destroy - Destroys zmq resource management service.
//********************************************************************
void rs_zmq_destroy(resource_service_fn_t *rs)
{
rs_zmq_priv_t *rsz = (rs_zmq_priv_t *)rs->priv;
 
zmq_close(rsz->zmq_socket);
zmq_ctx_destroy(rsz->zmq_context);
 
free(rsz->zmq_svr);
free(rsz);
free(rs);
}
 
//********************************************************************
// rs_zmq_create - Creates a zmq resource management service.
//********************************************************************
resource_service_fn_t *rs_zmq_create(void *arg, inip_file_t *kf, char *section)
{
service_manager_t *ess = (service_manager_t *)arg;
 
log_printf(15, "START!!!!!!!!!!!!!!!!!!\n"); 
flush_log();
//** Creates zmq context
void *context = zmq_ctx_new();
assert(context != NULL);
 
//** Creates zmq request (ZMQ_REQ) socket
void *socket = zmq_socket(context, ZMQ_REQ);
assert(socket != NULL);
 
//** Creates rs zmq private data 
rs_zmq_priv_t *rsz;
type_malloc_clear(rsz, rs_zmq_priv_t, 1);
rsz->zmq_context = context;
rsz->zmq_socket = socket;
rsz->ds = lookup_service(ess, ESS_RUNNING, ESS_DS);
 
//** Don't know which thread pool context I should use. So create a new one for rs zmq. 
rsz->tpc = lookup_service(ess, ESS_RUNNING, ESS_TPC_UNLIMITED); //** Refers to lio_gc->tpc_unlimited in lio_login.c
 
char *svr_proto, *svr_addr, *svr_port;
 
//** Retrieves remote zmq server name, transport protocol, and lisenting port
svr_proto = inip_get_string(kf, section, "protocol", RS_ZMQ_DFT_PROTO);
svr_addr = inip_get_string(kf, section, "server", NULL);
svr_port = inip_get_string(kf, section, "port", RS_ZMQ_DFT_PORT);
asprintf(&rsz->zmq_svr, "%s://%s:%s", string_trim(svr_proto), string_trim(svr_addr), string_trim(svr_port));
 
free(svr_proto);
free(svr_addr);
free(svr_port);
 
//** Creates the resource service
resource_service_fn_t *rs;
type_malloc_clear(rs, resource_service_fn_t, 1);
 
//** zmq version related fields 
rs->priv = rsz;
rs->get_rid_value = rs_zmq_get_rid_value;
rs->data_request = rs_zmq_request;
rs->destroy_service = rs_zmq_destroy;
rs->type = RS_TYPE_ZMQ;
 
//** Leave the following fields unchanged 
rs->query_dup = rs_query_base_dup;
rs->query_add = rs_query_base_add;
rs->query_append = rs_query_base_append;
rs->query_destroy = rs_query_base_destroy;
rs->query_print = rs_query_base_print;
rs->query_parse = rs_query_base_parse;
 
return(rs);
}
 