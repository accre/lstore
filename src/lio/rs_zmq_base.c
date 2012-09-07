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

#include "rs_zmq_base.h"

//********************************************************************
// rs_zmq_worker_routine -  Receives request, processes request, 
// and sends response back
//********************************************************************
void *rs_zmq_worker_routine(void *arg)
{
    rs_zmq_thread_arg_t *thread_arg = (rs_zmq_thread_arg_t *)arg;
    
    //** Receives zmq message and generates the data request
    int len;
    int i;
    op_generic_t *gop = NULL;
    op_status_t status; 
    rs_zmq_req_t *req = NULL;
    rs_zmq_rep_t *response = NULL;

    type_malloc_clear(req, rs_zmq_req_t, 1);
    req->da = thread_arg->da; 
    req->rs = thread_arg->rs;
    req->timeout = thread_arg->timeout;

    void *context = thread_arg->zmq_context;

    //** Creates ZMQ socket    
    void *rssever = zmq_socket(context, ZMQ_REP);
    
    //** Connects to Dealer
    int rc = zmq_connect(rssever, "inproc://worker");
    assert(rc != -1);
    while(1) {
        len = rs_zmq_req_recv(req, rssever);
        if (len == -1) break; //** zmq_ctx_zmq is called; so break the loop here 
	printf("Deserialized request %d bytes\n", len);
    
        gop = rs_data_request(req->rs, req->da, req->rsq, req->caps, req->req, req->req_size, NULL, 0, req->n_rid, req->timeout);

        //** Wait for it to complete
        gop_waitall(gop);

        status = gop_get_status(gop);
        gop_free(gop, OP_DESTROY);
    
        if (status.op_status != OP_STATE_SUCCESS) {
            printf("Error with data request! err_code=%d\n", status.error_code);
            //abort();
        }
    
        printf("Finished with receiving request. Now start to send response.\n");

        //** Creates reponse
        type_malloc_clear(response, rs_zmq_rep_t, 1);
        response->n_ele = req->n_rid;  //** Is this always true that cap list and req list have the same number of elements? 
        response->caps = req->caps;
        response->req = req->req;
        response->ds = thread_arg->ds;
 
        //** Sends response back over zmq
        len = rs_zmq_rep_send(response, rssever);
        printf("Serialized response %d bytes\n", len);

        //** Print the caps
        char *qstr = rs_query_base_print(thread_arg->rs, req->rsq);
        printf("Query: %s  n_alloc: %d\n", qstr, req->n_rid);
        printf("\n");

        for (i=0; i<req->n_rid; i++) {
            printf("%d.\tRID key: %s\n", i, req->req[i].rid_key);
            printf("\tRead  : %s\n", (char *)ds_get_cap(thread_arg->ds, req->caps[i], DS_CAP_READ));
            printf("\tWrite : %s\n", (char *)ds_get_cap(thread_arg->ds, req->caps[i], DS_CAP_WRITE));
            printf("\tManage: %s\n", (char *)ds_get_cap(thread_arg->ds, req->caps[i], DS_CAP_MANAGE));
        }
        printf("\n");
    
        //** Destroy query string
        free(qstr);

        //** Now destroy the allocation I just created
//        opque_t *q = new_opque();
//        for (i=0; i<req->n_rid; i++) {
//            gop = ds_remove(thread_arg->ds, thread_arg->da, ds_get_cap(thread_arg->ds, req->caps[i], DS_CAP_MANAGE), thread_arg->timeout);
//            opque_add(q, gop);
//        }

        //** Wait for it to complete
//        int err = opque_waitall(q);
//        opque_free(q, OP_DESTROY);

//        if (err != OP_STATE_SUCCESS) {
//            printf("Error removing allocations!\n");
//        }
  
        //** Destroy the caps and reqs
        for (i=0; i<req->n_rid; i++) {
           ds_cap_set_destroy(thread_arg->ds, req->caps[i], 1);
           //   free(req->req[i].rid_key); //** This will be freed by rs_simple
        }
        free(req->caps);
        free(req->req);

        //** Clean up
        if (req->rsq != NULL)
            rs_query_destroy(thread_arg->rs, req->rsq);

        //** Destroy response
        free(response);

//-----------------------------------------------------------------------------------------
//** Temporary codes for testing get rid value
       // rs_zmq_send_rid_value(thread_arg->rs, rssever);
//-----------------------------------------------------------------------------------------

    } //** end while(1)

    //** Destroy req
    free(req);

    //** Close socket
    zmq_close(rssever);

#ifdef DEBUG
    printf("Thread exit!");
    fflush(stdout);
#endif

    pthread_exit(NULL);    
}

//********************************************************************
// rs_zmq_reqlist_serialize - Serializes reqlist
//********************************************************************
Rs__Zmq__RszReqSet **rs_zmq_reqlist_serialize(rs_request_t *req, int num_ele)
{
    //** Creates req sets
    Rs__Zmq__RszReqSet **req_set;
    type_malloc_clear(req_set, Rs__Zmq__RszReqSet *, num_ele);

    int i;
    for (i = 0; i < num_ele; i++) {
        type_malloc_clear(req_set[i], Rs__Zmq__RszReqSet, 1);
        rs__zmq__rsz_req_set__init(req_set[i]);

        //** Assigns request list
        req_set[i]->rid_index = req[i].rid_index;
        req_set[i]->size = req[i].size;
        if (req[i].rid_key != NULL)
	    asprintf(&req_set[i]->rid_key, "%s", req[i].rid_key);
#ifdef DEBUG
	printf("Reqlist serialize\n");
        printf("\tRid index: %d\n", req_set[i]->rid_index);
        printf("\tRid size: %d\n", req_set[i]->size);
        printf("\tRid key: %s\n", req_set[i]->rid_key);
#endif
    }

    return req_set;
}

//********************************************************************
// rs_zmq_reqlist_destroy - Destroy reqlist
//********************************************************************
void rs_zmq_reqlist_destroy(Rs__Zmq__RszReqSet **req_set, int num_ele)
{
    int i;

    for (i = 0; i < num_ele; i++) {
        free(req_set[i]->rid_key); 
        free(req_set[i]);
    }
    free(req_set);
}

//********************************************************************
// rs_zmq_caplist_serialize - Serializes caplist                    
//********************************************************************
Rs__Zmq__RszCapSet **rs_zmq_caplist_serialize(data_service_fn_t *ds, data_cap_set_t **caps, int num_ele)
{
    //** Creates cap sets
    Rs__Zmq__RszCapSet **cap_set;
    type_malloc_clear(cap_set, Rs__Zmq__RszCapSet *, num_ele);

    int i;
    for (i = 0; i < num_ele; i++) {
        type_malloc_clear(cap_set[i], Rs__Zmq__RszCapSet, 1);
        rs__zmq__rsz_cap_set__init(cap_set[i]);

        //** Assigns caps  
        //** Needs to make sure the use of ds is correct 
	char *readcap = (char *)ds_get_cap(ds, caps[i], DS_CAP_READ);
	char *writecap =  (char *)ds_get_cap(ds, caps[i], DS_CAP_WRITE);
	char *managecap = (char *)ds_get_cap(ds, caps[i], DS_CAP_MANAGE);
	if (readcap != NULL)
            asprintf(&cap_set[i]->read, "%s", readcap);
	if (writecap != NULL)
	    asprintf(&cap_set[i]->write, "%s", writecap);
        if (managecap != NULL)
	    asprintf(&cap_set[i]->manage, "%s", managecap);

#ifdef DEBUG
	printf("Caplist serialize\n");
        printf("\tCap read: %s\n", cap_set[i]->read);
        printf("\tCap write: %s\n", cap_set[i]->write);
        printf("\tCap manage: %s\n", cap_set[i]->manage);
#endif
    }

    return cap_set;
}

//********************************************************************
// rs_zmq_caplist_destroy - Destroy caplist
//********************************************************************
void rs_zmq_caplist_destroy(Rs__Zmq__RszCapSet **cap_set, int num_ele)
{
    int i;
    for (i = 0; i < num_ele; i++) {
        free(cap_set[i]->read);
        free(cap_set[i]->write);
        free(cap_set[i]->manage);
        free(cap_set[i]);
    }
    free(cap_set);
}

//********************************************************************
// rs_req_serialize - Serialize request 
// Calling function should destroy the returned buffer
//********************************************************************
void *rs_zmq_req_serialize(rs_zmq_req_t *req, int *len)
{
    Rs__Zmq__RszRequest msg = RS__ZMQ__RSZ_REQUEST__INIT; //** Request Message
  
    msg.request_size = req->req_size;
    msg.num_rid = req->n_rid;
    msg.timeout = req->timeout;
    msg.fixed_size = req->fixed_size;
    msg.rs_query = rs_query_base_print(req->rs, req->rsq); 


    Rs__Zmq__RszRequest__RszDataAttr data_attr = RS__ZMQ__RSZ_REQUEST__RSZ_DATA_ATTR__INIT;

    //** Needs to make sure the use of ds is correct 
    rs_zmq_priv_t *rsz = (rs_zmq_priv_t *) req->rs->priv;
    data_service_fn_t *ds = (data_service_fn_t *)rsz->ds;

    ds_get_attr(ds, req->da, DS_ATTR_DURATION, (void *)&data_attr.duration, sizeof(data_attr.duration) );
    //** Uses DS_IBP_* to sepecify the attribute. This should be replaced by sth like DS_ATTR* 
    ds_get_attr(ds, req->da, DS_IBP_ATTR_RELIABILITY, (void *)&data_attr.reliability, sizeof(data_attr.reliability) );
    ds_get_attr(ds, req->da, DS_IBP_ATTR_TYPE, (void *)&data_attr.type, sizeof(data_attr.type));
    
#ifdef DEBUG
    printf("Serialized request\n");
    printf("\tRequest size: %d\n", msg.request_size);
    printf("\tNum rid: %d\n", msg.num_rid);
    printf("\tTimeout: %d\n", msg.timeout);
    printf("\tFixed size: %d\n", msg.fixed_size);
    printf("\trs_query: %s\n", msg.rs_query);
    printf("\tDS Attr Duration: %d\n", data_attr.duration);
    printf("\tDS Attr Reliability: %d\n", data_attr.reliability);
    printf("\tDS Attr Type: %d\n", data_attr.type);
#endif
    
    Rs__Zmq__RszCapSet **cap_set = rs_zmq_caplist_serialize(ds, req->caps, req->n_rid); 
    Rs__Zmq__RszReqSet **req_set = rs_zmq_reqlist_serialize(req->req, req->n_rid);
    
    msg.data_attr = &data_attr;
    msg.n_req = req->n_rid;
    msg.cap = cap_set;
    msg.n_cap = req->n_rid;
    msg.req = req_set;
    msg.n_hints = 0;
    //** Needs to make sure hints is always NULL
    msg.hints = NULL;

    *len = rs__zmq__rsz_request__get_packed_size(&msg);

    void *buf; 
    buf = malloc(*len);
    if (buf == NULL) {
        fprintf(stderr, "Memory allocation failed.\n");
        exit(1);
    }
 
    rs__zmq__rsz_request__pack(&msg, buf);
  
#ifdef DEBUG
    fprintf(stderr, "Writing %d serialized bytes\n", *len);
#endif

    //** Free allocated space 
    free(msg.rs_query);
    rs_zmq_caplist_destroy(cap_set, req->n_rid);
    rs_zmq_reqlist_destroy(req_set, req->n_rid);

    return buf;      
}

//********************************************************************
// rs_query_type_replace - Replaces resource service type
// Calling function needs to destroy returned string
// Assumes query following fixed format like: simple:1:rid_key:1:any:67
//********************************************************************
char *rs_query_type_replace(char *query, char *old_rst, char *new_rst)
{
    //** Find the position of old_rst
    char *pos;
    if ( (pos = strstr(query, old_rst)) == NULL) {
	fprintf(stderr, "Illegal query string q=%s old=%s\n", query, old_rst);
	exit(1);
    }
   
    //** Makes a new query string 
    char *new_query;
    asprintf(&new_query, "%s%s", new_rst, pos + strlen(old_rst));

    return new_query; 
}

//********************************************************************
// rs_zmq_caplist_deserialize 
//********************************************************************
data_cap_set_t **rs_zmq_caplist_deserialize(data_service_fn_t *ds, Rs__Zmq__RszCapSet **caplist, int num_ele)
{
    data_cap_set_t **caps;
    type_malloc_clear(caps, data_cap_set_t *, num_ele);

    int i;
    for (i = 0; i < num_ele; i++) {
        //type_malloc_clear(req->caps[i], data_cap_set_t, 1);
        caps[i] = ds_cap_set_create(ds);

        //** The NULL pointer is treated as an empty string in Protobuf-c which allows people to leave required strings blank.
	//** ds_set_cap just does pointer assignment, so we use this strdup
        if (caplist[i]->read[0] != '\0')
            ds_set_cap(ds, caps[i], DS_CAP_READ, (data_cap_t *)strdup(caplist[i]->read));
        if (caplist[i]->write[0] != '\0')
            ds_set_cap(ds, caps[i], DS_CAP_WRITE, (data_cap_t *)strdup(caplist[i]->write));
        if (caplist[i]->manage[0] != '\0')
            ds_set_cap(ds, caps[i], DS_CAP_MANAGE, (data_cap_t *)strdup(caplist[i]->manage));
        
#ifdef DEBUG
        printf("Caplist deserialize %d\n", i);
        printf("\tRead: (%s)\n", (char *)ds_get_cap(ds, caps[i], DS_CAP_READ));
        printf("\tWrite: (%s)\n", (char *)ds_get_cap(ds, caps[i], DS_CAP_WRITE));
        printf("\tManage: (%s)\n", (char *)ds_get_cap(ds, caps[i], DS_CAP_MANAGE));
#endif
    }
    return caps;
}

//********************************************************************
// rs_zmq_reqlist_deserialize
//********************************************************************
rs_request_t *rs_zmq_reqlist_deserialize(Rs__Zmq__RszReqSet **reqlist, int num_ele)
{
    rs_request_t *req;
    type_malloc_clear(req, rs_request_t, num_ele);

    int i;
    for (i = 0; i < num_ele; i++) {
        req[i].rid_index = reqlist[i]->rid_index;
        req[i].size = reqlist[i]->size;

        //** The NULL pointer is treated as an empty string in Protobuf-c which allows people to leave required strings blank.
        if (reqlist[i]->rid_key[0] != '\0') {
            asprintf(&req[i].rid_key, "%s", reqlist[i]->rid_key); //** Memory used by rid_key should be destroied by the calling function 
        }

#ifdef DEBUG
        printf("Reqlist deserialize %d\n", i); 
	printf("\tRequest rid index: %d\n", req[i].rid_index);
        printf("\tRequest size: %d\n", req[i].size);
        printf("\tRequest rid key: (%s)\n", req[i].rid_key);
#endif
    }
    return req;
}

//********************************************************************
// rs_zmq_req_deserialize - Deserialize request
//********************************************************************
void rs_zmq_req_deserialize(rs_zmq_req_t* req, void *buf, int len)
{
    Rs__Zmq__RszRequest *msg;
    
    msg = rs__zmq__rsz_request__unpack(NULL, len, buf);
    if (msg == NULL) {
        fprintf(stderr, "error unpacking\n");
        exit(1);
    }
    
#ifdef DEBUG
    printf("---------------------------------------------------------------------------\n");
    printf("Data received from zmq client\n");
    printf("\tRS query: %s\n", msg->rs_query);
    printf("\tRequest size: %d, Fixed size: %d, Num rid: %d, Timeout: %d\n", msg->request_size, msg->fixed_size, msg->num_rid, msg->timeout);
    printf("\tData attr duration:%d, reliability: %d, type: %d\n", msg->data_attr->duration, msg->data_attr->reliability, msg->data_attr->type);

    int index; 
    for (index = 0; index < msg->n_cap; index++) {
        printf("\tRead: (%s)\n", msg->cap[index]->read);
        printf("\tWrite: (%s)\n", msg->cap[index]->write);
        printf("\tManage: (%s)\n", msg->cap[index]->manage);
        printf("\tRequest rid index: %d\n", msg->req[index]->rid_index);
        printf("\tRequest size: %d\n", msg->req[index]->size);
        printf("\tRequest rid key: (%s)\n", msg->req[index]->rid_key);
    }
#endif 
 
    //** rs_query_base_parse's implementation doesn't use its first parameter   
    //** msg->rs_query generates problem because the type would be 'zmq'
    //** Replaces 'zmq' by 'simple'
   char *query_replaced = rs_query_type_replace(msg->rs_query, "zmq", "simple");
    
#ifdef DEBUG
    printf("New query string: %s\n", query_replaced); 
#endif

    req->rsq = rs_query_base_parse(req->rs, query_replaced); 
    req->req_size =  msg->request_size;  
    req->fixed_size = msg->fixed_size;
    req->n_rid = msg->num_rid;
    req->timeout = msg->timeout;

    //** req->da should be created and rs->priv should be initialized before calling this function
    //** Sets some of its values 
    //** type_malloc_clear(req->da, data_attr_t, 1);
    rs_simple_priv_t *rsz = (rs_simple_priv_t *)req->rs->priv; //** Notice that rs is simple resource service 

    ds_set_attr(rsz->ds, req->da, DS_ATTR_DURATION, &msg->data_attr->duration );    
    ds_set_attr(rsz->ds, req->da, DS_IBP_ATTR_RELIABILITY, &msg->data_attr->reliability);    
    ds_set_attr(rsz->ds, req->da, DS_IBP_ATTR_TYPE, &msg->data_attr->type);    

#ifdef DEBUG
    printf("------------------------------------------------------------------------\n");
    printf("Data in request structure\n");
    
    int64_t duration, reliability, type;

    ds_get_attr(rsz->ds, req->da, DS_ATTR_DURATION, (void *)&duration, sizeof(duration) );
    //** Uses DS_IBP_* to sepecify the attribute. This should be replaced by sth like DS_ATTR* 
    ds_get_attr(rsz->ds, req->da, DS_IBP_ATTR_RELIABILITY, (void *)&reliability, sizeof(reliability) );
    ds_get_attr(rsz->ds, req->da, DS_IBP_ATTR_TYPE, (void *)&type, sizeof(type));
    printf("\tData attr duration: %d\n", duration);
    printf("\tData attr relia: %d\n", reliability);
    printf("\tData attr type: %d\n", type);
#endif    
   
    req->req = rs_zmq_reqlist_deserialize(msg->req, msg->n_req);
    req->caps = rs_zmq_caplist_deserialize(rsz->ds, msg->cap, msg->n_cap);    
    
    //** This piece of code needs to be rewritten after talking to alan
    //** It should work for both NULL and real hint list data 
    req->hints_list = NULL;

    free(query_replaced);
   
    // Free the unpacked message
    rs__zmq__rsz_request__free_unpacked(msg, NULL);
}

//****************************************************************************************
// rs_zmq_req_recv - Receives request through zmq
// Returns the length of message
//****************************************************************************************
int rs_zmq_req_recv(rs_zmq_req_t* req, void *socket)
{
    void *request;
    int rc = rs_zmq_recv(socket, &request); //** This blocking call returns either because of calling zmq_ctx_destroy in main function, or receving one message
    if (rc == -1) //** Indicates that zmq_ctx_destroy is called
	return -1;

    printf("Length of recvd request message %d bytes\n", rc);

    //** Deserializes request received from zmq client 
    rs_zmq_req_deserialize(req, request, rc);
    
    free(request);

    return rc;
}

//********************************************************************
// rs_zmq_rep_serialize - Serialize response 
// Caller should release the returned buffer
//********************************************************************
void *rs_zmq_rep_serialize(rs_zmq_rep_t *rep, int *len)
{
    Rs__Zmq__RszResponse msg = RS__ZMQ__RSZ_RESPONSE__INIT; //** Request Message

    Rs__Zmq__RszCapSet **cap_set = rs_zmq_caplist_serialize(rep->ds, rep->caps, rep->n_ele);
    Rs__Zmq__RszReqSet **req_set = rs_zmq_reqlist_serialize(rep->req, rep->n_ele);
    
    msg.n_req = rep->n_ele;
    msg.cap = cap_set;
    msg.n_cap = rep->n_ele;
    msg.req = req_set;

    *len = rs__zmq__rsz_response__get_packed_size(&msg);

    void *buf;
    buf = malloc(*len);
    if (buf == NULL) {
        fprintf(stderr, "Memory allocation failed.\n");
        exit(1);
    }

    rs__zmq__rsz_response__pack(&msg, buf);

#ifdef DEBUG
    fprintf(stderr, "Writing %d serialized bytes\n", *len);
#endif

    //** Free allocated space 
    rs_zmq_caplist_destroy(cap_set, rep->n_ele);
    rs_zmq_reqlist_destroy(req_set, rep->n_ele);

    return buf;
}

//********************************************************************
// rs_zmq_rep_deserialize - Deserialize response
//********************************************************************
void rs_zmq_rep_deserialize(rs_zmq_rep_t *rep, void *buf, int len)
{
    Rs__Zmq__RszResponse *msg;

    msg = rs__zmq__rsz_response__unpack(NULL, len, buf);
    if (msg == NULL) {
        fprintf(stderr, "error unpacking response\n");
        exit(1);
    }

#ifdef DEBUG
    printf("---------------------------------------------------------------------------\n");
    printf("Response received from zmq resource service\n");
    int index;
    for (index = 0; index < msg->n_cap; index++) {
        printf("\tResponse Read: (%s)\n", msg->cap[index]->read);
        printf("\tResponse Write: (%s)\n", msg->cap[index]->write);
        printf("\tResponse Manage: (%s)\n", msg->cap[index]->manage);
        printf("\tRequest rid index: %d\n", msg->req[index]->rid_index);
        printf("\tRequest size: %d\n", msg->req[index]->size);
        printf("\tRequest rid key: (%s)\n", msg->req[index]->rid_key);
    }
#endif
    rep->n_ele = msg->n_cap; 
    rep->req = rs_zmq_reqlist_deserialize(msg->req, msg->n_req);
    rep->caps = rs_zmq_caplist_deserialize(rep->ds, msg->cap, msg->n_cap);    
    printf("Deserialized response %d bytes\n", len);
    // Free the unpacked message
    rs__zmq__rsz_response__free_unpacked(msg, NULL);     
}

//****************************************************************************************
// rs_zmq_rep_send - Sends response through zmq
// Returns the length of the sent message
//****************************************************************************************
int rs_zmq_rep_send(rs_zmq_rep_t* rep, void *socket)
{
    //** Serialize response
    int len;
    void *buf = rs_zmq_rep_serialize(rep, &len);

    int rc;
    rc = rs_zmq_send(socket, buf, len);

    //** Destroy allocated space
    free(buf);
    return rc;
}

//****************************************************************************************
// rs_zmq_recvmsg - Receives data through zmq
// Allocates space for *buf
//****************************************************************************************
int rs_zmq_recv(void *socket, void **buf)
{
    zmq_msg_t msg;
    int rc = zmq_msg_init(&msg);
    assert(rc == 0);
  
    rc = zmq_recvmsg(socket, &msg, 0);
    assert(rc != 0); //** Message length shouldn't be 0
    if (rc == -1) return rc; //** Different actions for receving request and response
   
    *buf = malloc(rc); 
    if (buf == NULL) {
        fprintf(stderr, "Memory allocation failed.\n");
        exit(1);
    }
    memcpy(*buf, zmq_msg_data(&msg), rc);

    zmq_msg_close(&msg);

    return rc;    
}    

//****************************************************************************************
// rs_zmq_rep_recv - Receives response through zmq
// Returns the length of message
//****************************************************************************************
int rs_zmq_rep_recv(rs_zmq_rep_t* rep, void *socket)
{
    void *response;
    int rc = rs_zmq_recv(socket, &response);

    printf("Length of recvd response message %d bytes\n", rc);

    //** Deserializes request received from zmq client 
    rs_zmq_rep_deserialize(rep, response, rc);
    free(response);
    
    return rc;
}

//****************************************************************************************
// rs_zmq_rid_key_serialize - Serializes rid key 
//****************************************************************************************
void *rs_zmq_ridkey_serialize(rs_zmq_rid_key_t *keys, int *len)
{
    Rs__Zmq__RszRidKey pb_keys = RS__ZMQ__RSZ_RID_KEY__INIT;
    if (keys->rid_key != NULL)
	asprintf(&pb_keys.rid_key, "%s", keys->rid_key);
    if (keys->key != NULL)
	asprintf(&pb_keys.key, "%s", keys->key);

    *len = rs__zmq__rsz_rid_key__get_packed_size(&pb_keys);

    void *buf;
    buf = malloc(*len);
    if (buf == NULL) {
        fprintf(stderr, "Memory allocation failed.\n");
        exit(1);
    }

    rs__zmq__rsz_rid_key__pack(&pb_keys, buf);

#ifdef DEBUG
    fprintf(stderr, "Writing %d serialized bytes\n", *len);
#endif

    free(pb_keys.rid_key);
    free(pb_keys.key);
    return buf;
}

//***************************************************************************************
// rs_zmq_ridkey_deserialize - Deserializes rid key
//***************************************************************************************
void rs_zmq_ridkey_deserialize(rs_zmq_rid_key_t *keys, void *buf, int len)
{
    Rs__Zmq__RszRidKey *pb_keys;
    pb_keys = rs__zmq__rsz_rid_key__unpack(NULL, len, buf);
    if (pb_keys == NULL) {
	fprintf(stderr, "error unpacking incoming ridkey message\n");
    }

    if (pb_keys->rid_key[0] != '\0') {
	asprintf(&keys->rid_key, "%s", pb_keys->rid_key);
    }

    if (pb_keys->key[0] != '\0') {
	asprintf(&keys->key, "%s", pb_keys->key);
    }

    rs__zmq__rsz_rid_key__free_unpacked(pb_keys, NULL);
}

//**************************************************************************************
// rs_zmq_ridvalue_serialize - Serializes rid value
//**************************************************************************************
void *rs_zmq_ridvalue_serialize(rs_zmq_rid_value_t *value, int *len)
{
    Rs__Zmq__RszRidValue pb_ridvalue = RS__ZMQ__RSZ_RID_VALUE__INIT;

    if (value->rid_value != NULL)
        asprintf(&pb_ridvalue.rid_value, "%s", value->rid_value);

    *len = rs__zmq__rsz_rid_value__get_packed_size(&pb_ridvalue);

    void *buf;
    buf = malloc(*len);
    if (buf == NULL) {
	fprintf(stderr, "Memory allocation failed.\n");
	exit(1);
    }

    rs__zmq__rsz_rid_value__pack(&pb_ridvalue, buf);

#ifdef DEBUG
    fprintf(stderr, "Writing %d bytes serialized ridvalue message\n", *len);
#endif

    free(pb_ridvalue.rid_value);

    return buf;
}

//**************************************************************************************
// rs_zmq_ridvalue_deserializes - Deserializes rid value
//**************************************************************************************
void rs_zmq_ridvalue_deserialize(rs_zmq_rid_value_t *value, void *buf, int len)
{
    Rs__Zmq__RszRidValue *pb_ridvalue;
    pb_ridvalue = rs__zmq__rsz_rid_value__unpack(NULL, len, buf);

    if (pb_ridvalue == NULL) {
	fprintf(stderr, "error unpacking incoming ridvalue message.\n");
	exit(1);
    }

    if (pb_ridvalue->rid_value[0] != '\0')
	asprintf(&value->rid_value, "%s", pb_ridvalue->rid_value);

    rs__zmq__rsz_rid_value__free_unpacked(pb_ridvalue, NULL);
}

//****************************************************************************************
// rs_zmq_send - Sends rs data through zmq
//****************************************************************************************
int rs_zmq_send(void *socket, void *buf, int len)
{
    int rc;
    rc = zmq_send(socket, buf, len, 0);
    assert(rc == len);
    
    return rc;
}

//****************************************************************************************
// rs_zmq_ridkey_send - Sends ridkey through zmq
// Returns the length of the sent message
//****************************************************************************************
int rs_zmq_ridkey_send(rs_zmq_rid_key_t *keys, void *socket)
{
    //** Serialize keys 
    int len;
    void *buf = rs_zmq_ridkey_serialize(keys, &len);

    rs_zmq_send(socket, buf, len);

    //** Destroy allocated space
    free(buf);
    return len;
}

//****************************************************************************************
// rs_zmq_ridvalue_send - Sends ridvalue through zmq
// Returns the length of the sent message
//****************************************************************************************
int rs_zmq_ridvalue_send(rs_zmq_rid_value_t *value, void *socket)
{
    //** Serialize keys 
    int len;
    void *buf = rs_zmq_ridvalue_serialize(value, &len);

    rs_zmq_send(socket, buf, len);

    //** Destroy allocated space
    free(buf);
    return len;
}

//****************************************************************************************
// rs_zmq_ridkey_recv - Receives ridkey through zmq
// Returns the length of message
//****************************************************************************************
int rs_zmq_ridkey_recv(rs_zmq_rid_key_t *keys, void *socket)
{
    void *buf;
    int rc = rs_zmq_recv(socket, &buf);
    assert(rc != -1);
    printf("Length of recvd ridkey message %d bytes\n", rc);

    //** Deserializes keys received from zmq client 
    rs_zmq_ridkey_deserialize(keys, buf, rc);

    free(buf);

    return rc;
}

//****************************************************************************************
// rs_zmq_ridvalue_recv - Receives ridvalue through zmq
// Returns the length of message
//****************************************************************************************
int rs_zmq_ridvalue_recv(rs_zmq_rid_value_t *value, void *socket)
{
    void *buf;
    int rc = rs_zmq_recv(socket, &buf);
    assert(rc != -1);
    //** Deserializes keys received from zmq client 
    rs_zmq_ridvalue_deserialize(value, buf, rc);

    free(buf);

    return rc;
}

//********************************************************************************************
// rs_zmq_send_rid_value - Receives rid key and sends rid value
//********************************************************************************************

void rs_zmq_send_rid_value(resource_service_fn_t *rs, void *socket)
{
    rs_zmq_rid_key_t *keys;
    rs_zmq_rid_value_t *value;

    type_malloc_clear(keys, rs_zmq_rid_key_t, 1);
    type_malloc_clear(value, rs_zmq_rid_value_t, 1);

    int len = rs_zmq_ridkey_recv(keys, socket);
    printf("Received ridkey %s length %d\n", keys->rid_key, len);

    value->rid_value = rs_get_rid_value(rs, keys->rid_key, keys->key);

    len = rs_zmq_ridvalue_send(value, socket);
    printf("Sent ridvalue %s length %d\n", value->rid_value, len);

    free(keys->rid_key);
    free(keys->key);
    free(keys);
    free(value);
}
