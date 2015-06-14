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
#define _log_module_index 212
#include "rr_wrk.h"

//*************************************************************************
// _rrwrk_init - Initialize rr worker
//*************************************************************************

void _rrwrk_init(rrwrk_t *self) {
    self->mode = SYNC_MODE;
    self->heartbeat = HEARTBEAT_DFT; //** msecs
    self->reconnect = RECONNECT_DFT; //** msecs
    self->liveness = HEARTBEAT_LIVENESS;
    self->heartbeat_at = 0; //** Is it appropriate to set this value?
    self->last_heartbeat = 0;
    self->total_received = 0;
    self->total_finished = 0;
    self->ctx = NULL;
    self->uuid_str = NULL;
    self->worker = NULL;
    self->sender = NULL;
    self->pipe = NULL;
    self->pattern = NULL;
    self->sinker = NULL;
    self->broker = NULL;
    self->data = NULL;
    self->cb = NULL;
}

//*************************************************************************
// rrwrk_new - Construct a request response worker.
//*************************************************************************

rrwrk_t *rrwrk_new() {
    rrwrk_t *wrk;
    type_malloc_clear(wrk, rrwrk_t, 1);

    _rrwrk_init(wrk);

    wrk->ctx = zctx_new();
    assert(wrk->ctx);
    wrk->data = zlist_new();
    assert(wrk->data);
    return wrk;
}

//************************************************************************
// rrwrk_data_destroy - Destroy work data
//************************************************************************

void rrwrk_data_destroy(rrwrk_t *self) {
    zmsg_t *msg = (zmsg_t *)zlist_pop(self->data);
    while(msg) {
        zmsg_destroy(&msg);
        msg = (zmsg_t *)zlist_pop(self->data);
    }
}

//************************************************************************
// rrwrk_destroy - Destroy rrwrk
//************************************************************************

void rrwrk_destroy(rrwrk_t **self_p) {
    assert(self_p);
    if (*self_p) {
        rrwrk_t *self = *self_p;
        zctx_destroy(&self->ctx); //** Also destroy all the sockets created in this context
        rrwrk_data_destroy(self);
        zlist_destroy(&self->data);
        free(self->pattern);
        free(self->sinker);
        free(self->broker);
        free(self->uuid_str);
        free(self);
        *self_p = NULL;
    }
}

//*************************************************************************
// rrtask_manager_fn - Function to perform the task
//*************************************************************************

void rrtask_manager_fn(void *args, zctx_t *ctx, void *pipe) {
    rrwrk_t *self = (rrwrk_t *)args;

    zstr_send(pipe, "READY");

    while (true) {
        zmq_pollitem_t item = {pipe, 0, ZMQ_POLLIN, 0};
        int rc = zmq_poll(&item, 1, -1);
        if (rc == -1 && errno == ETERM)
            break;    //** Context has been shut down

        zmsg_t *msg;
        if (item.revents & ZMQ_POLLIN) {

            //** Receives task input from pipe
            msg = zmsg_recv(pipe);

            zframe_t *data_in = zmsg_last(msg);

            rrtask_data_t *input = rrtask_data_new();
            rrtask_set_data(input, zframe_data(data_in), zframe_size(data_in));

            //** User callback function to perform the task
            rrtask_data_t *output = self->cb(input);
            zframe_t *data_out = zframe_new(output->data, output->len);

            //** Removes input data from message and destroy it
            zmsg_remove(msg, data_in);
            zframe_destroy(&data_in);

            //** Adds output data to the message and send it to pipe
            zmsg_add(msg, data_out);
            zmsg_send(&msg, pipe);

            //** Destroys task input and output data
            rrtask_data_destroy(&input);
            rrtask_data_destroy(&output);

            zmsg_destroy(&msg);
        }

        if (zctx_interrupted) {
            printf("I am destroying this message.\n");
            zmsg_destroy(&msg);
            break;
        }
    }
}

//*************************************************************************
// _rrwrk_create_socket - Create a socket for rr
//*************************************************************************

void *_rrwrk_create_socket(zctx_t *ctx, void *socket, int type) {
    if (socket) {
        zsocket_destroy(ctx, socket); //** Destroy existing socket
    }
    socket = zsocket_new(ctx, type);
    assert(socket);

    return socket;
}

//*************************************************************************
// _rrwrk_send - Send msg through socket. If no msg is provided, creates one internally.
//*************************************************************************

void _rrwrk_send(void *socket, char *command, zmsg_t *msg) {
    msg = msg ? zmsg_dup(msg):zmsg_new();

    zmsg_pushstr(msg, command);
    zmsg_pushstr(msg, RR_WORKER);
    zmsg_pushstr(msg, "");

    zmsg_send(&msg, socket);
}

//*************************************************************************
// rrwrk_send_to_sinker - Send summary to sinker.
//*************************************************************************

void rrwrk_send_to_sinker(rrwrk_t *self, char *command, zmsg_t *msg) {
    _rrwrk_send(self->sender, command, msg);
}

//*************************************************************************
// rrwrk_send_to_broker - Send message to broker.
//*************************************************************************

void rrwrk_send_to_broker(rrwrk_t *self, char *command, zmsg_t *msg) {
    _rrwrk_send(self->worker, command, msg);
}

//*************************************************************************
// rrwrk_connect_to_broker - Connect or reconnect to broker
//*************************************************************************

void rrwrk_connect_to_broker(rrwrk_t *self) {
    self->worker = _rrwrk_create_socket(self->ctx, self->worker, ZMQ_DEALER);

    //** Recreate uuid for each new connection
    if (self->uuid_str) {
        free(self->uuid_str);
        uuid_clear(self->uuid);
    }

    uuid_generate(self->uuid);
    self->uuid_str = rr_uuid_str(self->uuid);
    zsocket_set_identity(self->worker, self->uuid_str);

    zsocket_connect(self->worker, self->broker);

    //** Tell broker we are ready for work
    rrwrk_send_to_broker(self, RRWRK_READY, NULL);

    //** If liveness hits zero, queue is considered disconnected
    self->liveness = HEARTBEAT_LIVENESS;
    self->heartbeat_at = zclock_time() + self->heartbeat;
}

//*************************************************************************
// rrwrk_connect_to_sinker - Connect or reconnect to sinker
//*************************************************************************

void rrwrk_connect_to_sinker(rrwrk_t *self) {
    self->sender = _rrwrk_create_socket(self->ctx, self->sender, ZMQ_PUSH);
    zsocket_connect(self->sender, self->sinker);
}

//************************************************************************
// rrwrk_task_dispatch - Dispatch tasks to individual worker
//************************************************************************

void rrwrk_task_dispatch(rrwrk_t *self, zmsg_t *request) {
    if (request)
        zlist_append(self->data, request);
}


//************************************************************************
// rrwrk_start - Start working on taskes
//************************************************************************

int rrwrk_start(rrwrk_t *self, wrk_task_fn *cb) {
    self->heartbeat_at = zclock_time() + self->heartbeat;
    self->cb = cb;

    //** Start task thread and wait for synchronization signal
    self->pipe = zthread_fork(self->ctx, rrtask_manager_fn, (void *)self);
    assert(self->pipe);
    free(zstr_recv(self->pipe));
    //self->liveness = HEARTBEAT_LIVENESS; //** Don't do reconnect before the first connection established

    while(!zctx_interrupted) {
        zmq_pollitem_t items[] = {{self->worker, 0, ZMQ_POLLIN, 0}, {self->pipe, 0, ZMQ_POLLIN, 0}}; //** Be aware: this must be within while loop!!
        int rc = zmq_poll(items, 2, self->heartbeat * ZMQ_POLL_MSEC);
        if (rc == -1)
            break;	//** Interrupted

        if (items[0].revents & ZMQ_POLLIN) { //** Data from broker is ready
            zmsg_t *msg = zmsg_recv(self->worker);
            if (!msg)
                break; //** Interrupted. Need to do more research to confirm it
            self->liveness = HEARTBEAT_LIVENESS;
            self->last_heartbeat = zclock_time();

            //** Dont try to handle errors, just assert noisily
            assert(zmsg_size(msg) >= 3); //** empty + header + command + ...

            zframe_t *empty = zmsg_pop(msg);
            assert(zframe_streq(empty, ""));
            zframe_destroy(&empty);

            zframe_t *header = zmsg_pop(msg);
            assert(zframe_streq(header, RR_WORKER));
            zframe_destroy(&header);

            zframe_t *command = zmsg_pop(msg);
            if (zframe_streq(command, RRWRK_REQUEST)) {
                assert(zmsg_size(msg) == 3); //** UUID + SOURCE + INPUT DATA
                self->total_received++;
                zmq_pollitem_t item = {self->pipe, 0, ZMQ_POLLOUT, 0};
                int rc = zmq_poll(&item, 1, 0);
                assert(rc != -1);
                if (item.revents & ZMQ_POLLOUT) { //** Dispatch it if worker is ready
                    //** Send task to task manager
                    zmsg_send(&msg, self->pipe);
                } else { //** Otherwise put it on waiting list
                    zlist_push(self->data, zmsg_dup(msg));
                }
            } else if (zframe_streq(command, RRWRK_HEARTBEAT)) {
                ; //** Do nothing for heartbeat
            } else if (zframe_streq(command, RRWRK_DISCONNECT)) {
                rrwrk_connect_to_broker(self);
            } else {
                log_printf(0, "E: invalid input message\n");
            }

            zframe_destroy(&command);
            zmsg_destroy(&msg);
        } else if ((zclock_time() - self->heartbeat) > self->last_heartbeat) {
            if(--self->liveness == 0) {
                rrwrk_print(self);
                log_printf(0, "W: Disconnected from broker - retrying ...\n");
                rrwrk_print(self);
                zclock_sleep(self->reconnect);
                rrwrk_connect_to_broker(self);
            }
        }

        if (items[1].revents & ZMQ_POLLIN) { //** Data from pipe is ready
            zmsg_t *output = zmsg_recv(self->pipe);
            assert(zmsg_size(output) == 3); //** UUID + SOURCE + DATA

            self->total_finished++;

            zmsg_t *reply = zmsg_new();
            //** Adds UUID + SOURCE to reply message
            zframe_t *uuid = zframe_dup(zmsg_first(output));
            zframe_t *source = zframe_dup(zmsg_next(output));
            zmsg_add(reply, uuid);
            zmsg_add(reply, source);

            //** Sends reply to broker
            rrwrk_send_to_broker(self, RRWRK_REPLY, reply);

            //** Sends output to sinker
            //zmsg_send(&output, self->sender);
            rrwrk_send_to_sinker(self, RRWRK_OUTPUT, output);

            zmsg_destroy(&output);
            zmsg_destroy(&reply);
        }

        //** Dispatch task if any
        while (true) {
            zmq_pollitem_t pipe_write = {self->pipe, 0, ZMQ_POLLOUT, 0};
            zmq_poll(&pipe_write, 1, 0);
            if ((pipe_write.revents & ZMQ_POLLOUT) && (zlist_size(self->data))) {
                zmsg_t* data = (zmsg_t *)zlist_pop(self->data);
                zmsg_send(&data, self->pipe);
                printf("Dispatched one task.\n");
            } else
                break;
        }

        //** Send HEARTBEAT if it's time
        if (zclock_time() > self->heartbeat_at) {
            rrwrk_print(self);
            rrwrk_send_to_broker(self, RRWRK_HEARTBEAT, NULL);
            self->heartbeat_at = zclock_time() + self->heartbeat;
        }

    }

    if (zctx_interrupted)
        log_printf(0, "W: interrupt received. Killing worker...\n");

    return -1;
}

//**************************************************************************
// rrwrk_config_ppp - Config rrworker to use ppp pattern
//**************************************************************************

void _rrwrk_config_ppp(rrwrk_t *self, inip_file_t *keyfile) {
    assert(keyfile);
    self->sinker = inip_get_string(keyfile, "pppwrk", "sinker", NULL);
    assert(self->sinker);
    self->broker = inip_get_string(keyfile, "pppwrk", "broker", NULL);
    assert(self->broker);
    self->liveness = inip_get_integer(keyfile, "pppwrk", "liveness", HEARTBEAT_LIVENESS);
    self->heartbeat = inip_get_integer(keyfile, "pppwrk", "heartbeat", HEARTBEAT_DFT);
    self->reconnect = inip_get_integer(keyfile, "pppwrk", "reconnect", RECONNECT_DFT);

    rrwrk_connect_to_broker(self);
    rrwrk_connect_to_sinker(self);
}

//*************************************************************************
// rrwrk_load_config - Config the rr worker
//*************************************************************************

void rrwrk_load_config(rrwrk_t *self, char *fname) {
    assert(fname);
    inip_file_t *keyfile = inip_read(fname);
    self->pattern = inip_get_string(keyfile, "zsock", "pattern", NULL);
    assert(self->pattern); //** Need a more decent way to handle misconfiguration

    if (strcmp(self->pattern, "ppp") == 0) {
        _rrwrk_config_ppp(self, keyfile);
    } else {
        log_printf(0, "W: Unknown ZMQ Pattern: %s.\n", self->pattern);
        exit(EXIT_FAILURE);
    }

    inip_destroy(keyfile);
}

void rrwrk_print(rrwrk_t *self) {
    printf("\n=========================================================================\n");
    printf("W: live %Zd, hb %d\n", self->liveness, self->heartbeat);
    printf("W: waiting list %Zd\n", zlist_size(self->data));
    printf("W: received %Zd, finished %Zd\n", self->total_received, self->total_finished);
}
