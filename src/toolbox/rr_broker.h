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

#ifndef _RR_BROKER_H_
#define _RR_BROKER_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "rr_base.h"

//** Request and Response broker class
typedef struct {
    zctx_t *ctx;       		//** CZMQ context
    void *socket;      		//** Socket for clients and workers
    char *pattern;     		//** ZMQ pattern
    char *endpoint;    		//** Endpoint that server binds to
    uint64_t heartbeat_at;
    uint64_t finished;		//** Total processed requests so far
    int heartbeat;
    int mode;
    size_t liveness;
    size_t wrk_num;		//** Number of workers
    zhash_t *workers_ht;
    zlist_t *workers;  		//** Live workers have to be kept from oldest to most recent. Note that not every live worker is ready for working.
    zlist_t *waiting_requests;  //** Requests waiting for dispatching
} rrbroker_t;

//** Worker class defines a single worker, idle or active
typedef struct {
    rrbroker_t *broker;		//** Broker instance
//    uuid_t uuid;	  	//** UUID as a binary blob
    char *uuid_str; 		//** UUID as printable string
    zframe_t *address;    	//** Worker address
    uint64_t expiry;  	  	//** Expires at unless heartbeat
    int ready;		  	//** 1 ready for new task, 0 not
    uint64_t finished;		//** Total processed requests by this worker so far
    zhash_t *pending_requests; 	//** Requests waiting for acknowledgement by this worker
} rrworker_t;

rrbroker_t *rrbroker_new();
rrworker_t *rrworker_new();
void rrbroker_destroy(rrbroker_t **self_p);
void rrworker_destroy(rrworker_t **self_p);
int rrbroker_worker_erase(const char *key, void *item, void *argument);
int rrworker_request_erase(const char *key, void *item, void *argument);
int rrworker_foreach_fn(const char *key, void *item, void *argument);
void rrworker_delete(rrworker_t *self);
void rrbroker_purge(rrbroker_t *self);
void rrbroker_send_to_worker(rrworker_t *worker, char *command, zmsg_t *msg);
void rrbroker_request_dispatch(rrbroker_t *self, zmsg_t *request);
void rrbroker_client_msg(rrbroker_t *self, zframe_t *address, zmsg_t *request);
rrworker_t *rrbroker_worker_require(rrbroker_t *self, char *identity, zframe_t *address);
void rrbroker_worker_live(rrworker_t *self);
void rrbroker_worker_ready(rrworker_t *self);
void rrbroker_worker_msg(rrbroker_t *self, zframe_t *address, zmsg_t *msg);
void rrbroker_bind(rrbroker_t *self);
void rrbroker_load_config(rrbroker_t *self, char *fname);

void rrbroker_start(rrbroker_t *brk);
void rrbroker_print(rrbroker_t *brk);
#ifdef __cplusplus
}
#endif

#endif
