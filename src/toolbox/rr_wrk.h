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

#ifndef _RR_WRK_H_
#define _RR_WRK_H

#ifdef __cplusplus
extern "C" {
#endif

#include "rr_base.h"

//** User callback function to perform task
typedef rrtask_data_t *(wrk_task_fn)(rrtask_data_t *input);

//** Request and Response worker class
typedef struct {
    zctx_t *ctx;       		//** CZMQ context
    uuid_t uuid;
    char *uuid_str;
    void *worker;      		//** Socket to receive message from broker
    void *sender;      		//** Socket to send summary to sinker
    void *pipe;
    char *pattern;     		//** ZMQ pattern
    char *broker;
    char *sinker;
    uint64_t heartbeat_at;	//** When to send HEARBEAT
    uint64_t last_heartbeat;	//** When was last HEARBEAT received
    uint64_t total_received;
    uint64_t total_finished;
    size_t liveness;		//** How many attempts left
    int mode;          		//** Either SYNC_MODE or ASYNC_MODE
    int heartbeat;		//** Heartbeat delay, msecs
    int reconnect;		//** Reconnect delay, msecs
    zlist_t *data;		//** List of task data
    wrk_task_fn *cb;		//** Callback function
} rrwrk_t;

rrwrk_t *rrwrk_new();
void rrwrk_destroy(rrwrk_t **self_p);
int rrwrk_start(rrwrk_t *self, wrk_task_fn *cb);

void rrtask_manager_fn(void *args, zctx_t *ctx, void *pipe);

void rrwrk_load_config(rrwrk_t *self, char *fname);

void rrwrk_connect_to_broker(rrwrk_t *self);
void rrwrk_connect_to_sinker(rrwrk_t *self);

void rrwrk_send_to_broker(rrwrk_t *self, char *command, zmsg_t *msg);
void rrwrk_send_to_sinker(rrwrk_t *self, char *command, zmsg_t *msg);

void rrwrk_print(rrwrk_t *self);
#ifdef __cplusplus
}
#endif

#endif
