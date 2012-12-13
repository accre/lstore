/*
Advanced Computing Center for Research and Education Proprietary License
Version 1.0 (September 2012)

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

//*************************************************************************
//*************************************************************************

#define _log_module_index 213

#include "rr_broker.h"

//*************************************************************************
// rrbroker_init - Initialize broker
//*************************************************************************

void _rrbroker_init(rrbroker_t *self)
{
    self->ctx = NULL;
    self->socket = NULL;
    self->pattern = NULL;
    self->endpoint = NULL;
    self->workers_ht = NULL;
    self->workers = NULL;
    self->waiting_requests = NULL;
    self->heartbeat_at = 0;
    self->finished = 0;
    self->heartbeat = HEARTBEAT_DFT;
    self->liveness = HEARTBEAT_LIVENESS;
}

//*************************************************************************
// rrbroker_new - Construct broker
//*************************************************************************

rrbroker_t *rrbroker_new()
{
    rrbroker_t *self;
    type_malloc_clear(self, rrbroker_t, 1);

    _rrbroker_init(self);

    self->ctx = zctx_new();
    assert(self->ctx);
    self->workers_ht = zhash_new();
    assert(self->workers_ht);
    self->workers = zlist_new();
    assert(self->workers);
    self->waiting_requests = zlist_new();
    assert(self->waiting_requests);
   
    return self;
}
//*************************************************************************
// rrbroker_requests_destroy - Destroy all the waiting requests
//*************************************************************************

void rrbroker_requests_destroy(rrbroker_t *self)
{
    zmsg_t *msg = (zmsg_t *)zlist_pop(self->waiting_requests);
    while(msg) {
	zmsg_destroy(&msg);
	msg = (zmsg_t *)zlist_pop(self->waiting_requests);
    }
}

//*************************************************************************
// rrbroker_destroy - Destroy broker
//*************************************************************************

void rrbroker_destroy(rrbroker_t **self_p)
{
    assert(self_p);
    if (*self_p) {
	rrbroker_t *self = *self_p;
	free(self->pattern);
	free(self->endpoint);
	zctx_destroy(&self->ctx);
	rrbroker_requests_destroy(self);	
	zlist_destroy(&self->waiting_requests);
   	zlist_destroy(&self->workers);
 	zhash_foreach(self->workers_ht, rrbroker_worker_erase, NULL);
	zhash_destroy(&self->workers_ht);	
	free(self);
	*self_p = NULL;
    }
}

//*************************************************************************
// rrworker_new - Construct worker
//*************************************************************************

rrworker_t *rrworker_new()
{
    rrworker_t *self;
    type_malloc_clear(self, rrworker_t, 1);

    self->ready = 0;
    self->broker = NULL;
    self->uuid_str = NULL;
    self->pending_requests = zhash_new();
    assert(self->pending_requests);
    self->expiry = zclock_time() + HEARTBEAT_EXPIRY;
    self->finished = 0;
   
    return self;
}

//*************************************************************************
// rrworker_destroy - Destroy rrworker
//*************************************************************************

void rrworker_destroy(rrworker_t **self_p)
{
    assert(self_p);
    if (*self_p) {
	rrworker_t *self = *self_p;
	free(self->uuid_str);
	zframe_destroy(&self->address);
 	//zhash_foreach(self->pending_requests, rrworker_request_erase, NULL); //** Pending requests should be already claimed back until then
	zhash_destroy(&self->pending_requests); //** Will call the registered free_fn for each item
	free(self);
        *self_p = NULL;
    }
}

//*************************************************************************
// rrbroker_worker_erase - Erase the worker  
//*************************************************************************

int rrbroker_worker_erase(const char *key, void *item, void *argument)
{
    rrworker_t *wrk = (rrworker_t *)item;
    rrworker_destroy(&wrk);
    return 0;
}

//*************************************************************************
// rrworker_request_erase - Erase the request
//*************************************************************************

int rrworker_request_erase(const char *key, void *item, void *argument)
{
    zmsg_t *msg = (zmsg_t *)item;
    zmsg_destroy(&msg);
    return 0;
}

//*************************************************************************
// rrworker_foreach_fn - Callback function for zhash_foreach method
// Put the pending requests on this worker back to broker's waiting list
//*************************************************************************

int rrworker_foreach_fn(const char *key, void *item, void *argument)
{
    rrworker_t *self = (rrworker_t *)argument;

    zlist_push(self->broker->waiting_requests, item);
    zhash_delete(self->pending_requests, key);

    return 0;
}

//*************************************************************************
// rrworker_delete - Delete current worker
//*************************************************************************

void rrworker_delete(rrworker_t *self)
{
    //** Claim pending requests first
    if (zhash_size(self->pending_requests)) 
	zhash_foreach(self->pending_requests, rrworker_foreach_fn, self);

    //** Remove it from broker 
    zlist_remove(self->broker->workers, self);
    zhash_delete(self->broker->workers_ht, self->uuid_str);
     
    self->broker->wrk_num--;
    printf("Lost a worker. Total %Zd workers.\n", self->broker->wrk_num);

    //** Destroy it 
    rrworker_destroy(&self);
}

//*************************************************************************
// rrbroker_purge - Disconnect the expired workers and resubmit the 
// requests on its pending list
//*************************************************************************

void rrbroker_purge(rrbroker_t *self)
{
    rrworker_t *worker = (rrworker_t *)zlist_first(self->workers);
    while(worker) {
   	if (zclock_time() < worker->expiry)  
	    break;		//** Worker is alive, we're done here
        //** Delete expired worker
	rrworker_delete(worker);

	worker = (rrworker_t *)zlist_first(self->workers); 
    }

    //** Redispatch the requests
    rrbroker_request_dispatch(self, NULL);
}

//*************************************************************************
// rrbroker_send_to_worker - Send msg to worker and puts it on its pending list
//*************************************************************************

void rrbroker_send_to_worker(rrworker_t *worker, char *command, zmsg_t *msg)
{
    zmsg_t *msg_copy = msg? zmsg_dup(msg):zmsg_new();
    assert(msg_copy);

    zmsg_pushstr(msg_copy, command);
    zmsg_pushstr(msg_copy, RR_WORKER);
    zmsg_wrap(msg_copy, zframe_dup(worker->address));
    zmsg_send(&msg_copy, worker->broker->socket);
   
    //** Insert message into worker's pending table 
    if (msg) {
	zframe_t *uuid = zmsg_first(msg);
	char *uuid_str = zframe_strdup(uuid);
	zhash_insert(worker->pending_requests, uuid_str, msg);		
	zhash_freefn(worker->pending_requests, uuid_str, NULL); //** Or leave it off
	free(uuid_str);
    }
}

//*************************************************************************
// rrbroker_next_worker - Get next worker ready for work
//*************************************************************************

rrworker_t *rrbroker_next_worker(rrbroker_t *self)
{
    rrworker_t *worker = (rrworker_t *)zlist_next(self->workers);
    while(worker) {
	if (worker->ready && (zclock_time() < worker->expiry))
	    break;	
	worker = (rrworker_t *)zlist_next(self->workers);
    } 

    return worker;
}

//*************************************************************************
// rrbroker_request_dispatch - Dispatch requests to waiting workers
//*************************************************************************

void rrbroker_request_dispatch(rrbroker_t *self, zmsg_t *request)
{
    //** Queue message 
    if (request)
        zlist_append(self->waiting_requests, request);
   
    while(zlist_size(self->waiting_requests) && zlist_size(self->workers)) {
	//** Get the next message and available worker
        rrworker_t *worker = rrbroker_next_worker(self); 
	if (!worker) 
	    break;  //** No worker is ready
	if (self->mode == SYNC_MODE) 
            worker->ready = 0; //** Worker on SYNC_MODE only process one request once
	zmsg_t *msg = zlist_pop(self->waiting_requests);
		
	//** Send request to this worker
        rrbroker_send_to_worker(worker, RRWRK_REQUEST, msg);
       
	//printf("Message sent to worker: %s\n", worker->uuid_str);
        //zmsg_dump(msg);
    }
}

//*************************************************************************
// rrbroker_client_msg - Processs requests from client
//*************************************************************************

void rrbroker_client_msg(rrbroker_t *self, zframe_t *address, zmsg_t *request)
{
    assert(zmsg_size(request) >= 1); //**CMD or CMD + BODY 
    zframe_t *cmd = zmsg_pop(request);

    if (zframe_streq(cmd, RRCLI_FINISHED)) {
	zmsg_t *msg = zmsg_new();
	zmsg_wrap(msg, zframe_dup(address));
	zmsg_send(&msg, self->socket);		
	zmsg_destroy(&request);
    } else if (zframe_streq(cmd, RRCLI_REQUEST)){ 
        //** Set message identity to client sender
    	zmsg_push(request, zframe_dup(address));
    
    	//** Set message UUID
    	uuid_t uuid; 
    	uuid_generate(uuid);
    	char *id_str = rr_uuid_str(uuid);
    	zmsg_pushstr(request, id_str);

    	//** Dispatch requests
    	rrbroker_request_dispatch(self, request);
    	free(id_str);
    }

    zframe_destroy(&cmd);
}

//************************************************************************
// rrbroker_worker_require - Locate a worker by identity, or creates a new
// worker if there is no worker already with the identity
//************************************************************************

rrworker_t *rrbroker_worker_require(rrbroker_t *self, char *identity, zframe_t *address)
{
    assert(identity);
    assert(address);

    rrworker_t *worker = (rrworker_t *)zhash_lookup(self->workers_ht, identity);

    if (worker == NULL) {
	worker = rrworker_new();
	worker->broker = self;
	worker->uuid_str = (char *)malloc(strlen(identity) + 1);
	strcpy(worker->uuid_str, identity);
	worker->address = zframe_dup(address);
	worker->ready = 1; //** New worker ready to work
	zhash_insert(self->workers_ht, identity, worker);
	zhash_freefn(self->workers_ht, identity, NULL);
	zlist_append(self->workers, worker);
    } 

    return worker;
}

//************************************************************************
// rrbroker_worker_live - Worker is still live. Put it at the back of the list. 
//************************************************************************

void rrbroker_worker_live(rrworker_t *self)
{
    self->expiry = zclock_time() + HEARTBEAT_EXPIRY;
    zlist_remove(self->broker->workers, self);
    zlist_append(self->broker->workers, self); 
}

//************************************************************************
// rrbroker_worker_ready - Worker is now waiting for work
//************************************************************************

void rrbroker_worker_ready(rrworker_t *self)
{
    rrbroker_worker_live(self);
    self->ready = 1;
    //** Dispatch requests 
    rrbroker_request_dispatch(self->broker, NULL);
}

//************************************************************************
// rrbroker_worker_msg - Process messages from worker
//************************************************************************

void rrbroker_worker_msg(rrbroker_t *self, zframe_t *address, zmsg_t *msg)
{
    assert(zmsg_size(msg) >= 1); //** COMMAND or COMMAND + MSG_UUID + SOURCE 

    zframe_t *command = zmsg_pop(msg);
    char *uuid_str = zframe_strdup(address);
    int worker_live = (zhash_lookup(self->workers_ht, uuid_str) != NULL);
    rrworker_t *worker = rrbroker_worker_require(self, uuid_str, address);

    if (zframe_streq(command, RRWRK_REPLY)) {
	if (worker_live) {
            assert(zmsg_size(msg) == 2); //** MSG_UUID + SOURCE
	    
	    //** Delete acknowledged message from pending list
	    zframe_t *msg_uuid = zmsg_pop(msg);
	    char *m_uuid_str = zframe_strdup(msg_uuid);
	    zmsg_t *request = zhash_lookup(worker->pending_requests, m_uuid_str);
	    if (request) {	    
	        worker->finished++;
	        self->finished++;
	    }
	    //assert(request);
	    zhash_delete(worker->pending_requests, m_uuid_str);
	    zmsg_destroy(&request);
	    free(m_uuid_str);
	    zframe_destroy(&msg_uuid);
  
	    //** This worker is now waiting for work
	    rrbroker_worker_ready(worker);
	} 
    } else if (zframe_streq(command, RRWRK_READY)) {
	if (worker_live) {
	    rrworker_delete(worker); //Not first command in session
	} else {
	    self->wrk_num++;
	    printf("B: Got a new worker. Total %Zd workers.\n", self->wrk_num);
	    rrbroker_request_dispatch(self, NULL);
	}		
    } else if (zframe_streq(command, RRWRK_HEARTBEAT)) {
        if (worker_live) {
	    if (self->mode == ASYNC_MODE) {
		rrbroker_worker_ready(worker);
	    } else {
	        rrbroker_worker_live(worker);
	    }
	} 
    } else if (zframe_streq(command, RRWRK_DISCONNECT)) {
        rrworker_delete(worker);
    } else {
        log_printf(0, "B: invalid worker command\n");
    }
    zframe_destroy(&command);
    zmsg_destroy(&msg);
    free(uuid_str);
}

//*************************************************************************
// rrbroker_start - Start rr broker
//*************************************************************************

void rrbroker_start(rrbroker_t *self)
{
    assert(self);
    rrbroker_print(self);
    
    //** Get and process messages forever or until interrupted
    while(!zctx_interrupted) {
	zmq_pollitem_t item = {self->socket, 0, ZMQ_POLLIN, 0};
	int rc = zmq_poll(&item, 1, self->heartbeat * ZMQ_POLL_MSEC);
	if (rc == -1)
	    break;	//** Interrrupted

	if (item.revents & ZMQ_POLLIN) {
	    zmsg_t *msg = zmsg_recv(self->socket);
	    if (!msg)
		break;	//** Interrupted

	    //** Can I replace the following three calls by zmsg_unwrap()?
	    zframe_t *sender = zmsg_pop(msg);
	    zframe_t *empty = zmsg_pop(msg); 
	    zframe_destroy(&empty); 
	    zframe_t *header = zmsg_pop(msg);

	    if (zframe_streq(header, RR_CLIENT)) {
		//** Process requests from client
		rrbroker_client_msg(self, sender, msg);
	    } else if (zframe_streq(header, RR_WORKER)) {
		//** Process data from worker
		rrbroker_worker_msg(self, sender, msg);	
	    } else {
		log_printf(0, "B: invalid message:");
		zmsg_dump(msg);
		zmsg_destroy(&msg);
	    }

	    zframe_destroy(&sender);
	    zframe_destroy(&header);
 	} 

	if (zclock_time() > self->heartbeat_at) {

  	    //** Disconnect and delete any expired workers
	    rrbroker_purge(self);	    //** Purge expired workers

	    rrbroker_print(self);
    
 	    //** Send heartbeats
	    rrworker_t *worker = (rrworker_t *)zlist_first(self->workers);
	    while(worker) {
		rrbroker_send_to_worker(worker, RRWRK_HEARTBEAT, NULL);
		worker = (rrworker_t *)zlist_next(self->workers);
	    }
	    self->heartbeat_at = zclock_time() + self->heartbeat;
	}	    
    }

    if (zctx_interrupted) 
	log_printf(0, "B: interrrupt received. Killing broker...\n");
}

//*****************************************************************************
// rrbroker_bind 
//*****************************************************************************

void rrbroker_bind(rrbroker_t *self)
{
    self->socket = zsocket_new(self->ctx, ZMQ_ROUTER);
    assert(self->socket);
    zsocket_set_sndbuf(self->socket, 100000000);
    zsocket_set_rcvbuf(self->socket, 100000000);
    zsocket_set_sndhwm(self->socket, 100000);
    zsocket_set_rcvhwm(self->socket, 100000);

    int rc = zsocket_bind(self->socket, "%s", self->endpoint);
    assert(rc != -1); 
}

//******************************************************************************
// _rrbroker_config_ppp - Config rrbroker to use ppp pattern
//******************************************************************************

void _rrbroker_config_ppp(rrbroker_t *self, inip_file_t *keyfile)
{
    self->endpoint = inip_get_string(keyfile, "pppbroker", "endpoint", NULL);
    assert(self->endpoint);
    self->liveness = inip_get_integer(keyfile, "pppbroker", "liveness", 3);
    self->heartbeat = inip_get_integer(keyfile, "pppbroker", "heartbeat", 2500);
    rr_set_mode_tm(keyfile, "pppbroker", &self->mode, NULL);

    rrbroker_bind(self);
}

//******************************************************************************
// rrbroker_load_config - Config the rr broker
//******************************************************************************

void rrbroker_load_config(rrbroker_t *self, char *fname)
{
    assert(self);

    inip_file_t *keyfile = inip_read(fname);
    assert(keyfile);

    self->pattern = inip_get_string(keyfile, "zsock", "pattern", NULL);
    assert(self->pattern);

    if (strcmp(self->pattern, "ppp") == 0) {
	_rrbroker_config_ppp(self, keyfile);
    } else {
	log_printf(0, "B: Unknown ZMQ Patterns: %s.\n", self->pattern);
    }

    inip_destroy(keyfile);
}

int callback(const char *key, void *item, void *argument)
{
    printf("\tKey:%s.\t Item: %p\n", key, item);
    return 0;
}

void rrbroker_print(rrbroker_t *self)
{
    printf("\n=============================================================================\n");
    printf("B: Waiting request list: %Zd requests.\n", zlist_size(self->waiting_requests));
    printf("B: Total finished requests: %" PRIu64 "\n", self->finished);
/*    printf("[\n");
    if (zlist_size(self->waiting_requests)) {
	zmsg_t *msg = (zmsg_t *)zlist_first(self->waiting_requests);
	while(msg) {
	    zmsg_dump(msg);
	    msg = (zmsg_t *)zlist_next(self->waiting_requests);
	}
    }
    printf("]\n");
*/ 
    printf("B: Worker list: %Zd workers\n", zlist_size(self->workers));
    printf("[\n");
    if (zlist_size(self->workers)) {
	rrworker_t *wrk = (rrworker_t *)zlist_first(self->workers);
	while(wrk) {
	    printf("\tW: UUID: %s. Item: %p\n", wrk->uuid_str, wrk);
	    printf("\tW: Pending request list: %Zd\n", zhash_size(wrk->pending_requests));
	    printf("\tW: Total finished requests: %" PRIu64 "\n", wrk->finished);
//	    zhash_foreach(wrk->pending_requests, callback, NULL);
	    wrk = (rrworker_t *)zlist_next(self->workers);
	}
    }
    printf("]\n");
/*
    printf("Worker hashtable: %Zd workers\n", zhash_size(self->workers_ht));
    printf("[\n");
    zhash_foreach(self->workers_ht, callback, NULL); 
    printf("]\n");*/
    printf("\n=============================================================================\n");
}

