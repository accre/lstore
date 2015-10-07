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

#include "rr_sinker.h"

//*************************************************************************
// rrsinker_new - Construct rrsinker class
//*************************************************************************

rrsinker_t *rrsinker_new()
{
    rrsinker_t *self;
    type_malloc_clear(self, rrsinker_t, 1);

    self->ctx = zctx_new();
    assert(self->ctx);
    self->sinker = zsocket_new(self->ctx, ZMQ_PULL);
    assert(self->sinker);
    self->cb = NULL;
    self->pattern = NULL;
    self->endpoint = NULL;

    return self;
}

//**************************************************************************
// rrsinker_destroy - Destroy rrsinker
//**************************************************************************

void rrsinker_destroy(rrsinker_t **self_p)
{
    assert(self_p);
    if (*self_p) {
        rrsinker_t *self = *self_p;
        free(self->pattern);
        free(self->endpoint);
        zctx_destroy(&self->ctx);
        free(self);
        self_p = NULL;
    }
}

//**************************************************************************
// rrsinker_task_manager
//**************************************************************************

void rrsinker_task_manager(void *args, zctx_t *ctx, void *pipe)
{
    rrsinker_t *self = (rrsinker_t *)args;
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
            self->cb(input);
            zmsg_destroy(&msg);
            rrtask_data_destroy(&input);
        }
    }
}

//**************************************************************************
// rrsinker_start - Start working on sinker task
//**************************************************************************

void rrsinker_start(rrsinker_t *self, sinker_task_fn *cb)
{
    self->cb = cb;
    self->pipe = zthread_fork(self->ctx, rrsinker_task_manager, (void *)self);
    assert(self->pipe);
    free(zstr_recv(self->pipe));

    while(!zctx_interrupted) {
        zmq_pollitem_t item = {self->sinker, 0, ZMQ_POLLIN, 0};
        int rc = zmq_poll(&item, 1, -1);
        if (rc == -1)
            break;	//** Interrupted

        if (item.revents & ZMQ_POLLIN) {
            zmsg_t *msg = zmsg_recv(self->sinker);
            if (!msg)
                break;
            zframe_t *empty = zmsg_pop(msg);
            zframe_destroy(&empty);
            zframe_t *header = zmsg_pop(msg);

            if (zframe_streq(header, RR_WORKER)) {
                zframe_t *command = zmsg_pop(msg);
                assert(zmsg_size(msg) >= 3); //** UUID + SOURCE + DATA
                if (zframe_streq(command, RRWRK_OUTPUT)) {
                    zmsg_send(&msg, self->pipe);
                }
                zframe_destroy(&command);
            } else {
                log_printf(0, "S: invalid message:");
                zmsg_dump(msg);
                zmsg_destroy(&msg);
            }
            zframe_destroy(&header);
            zmsg_destroy(&msg);
        }
    }
}

//*************************************************************************
// _rrsinker_config_ppp
//*************************************************************************

void _rrsinker_config_ppp(rrsinker_t *self, inip_file_t *keyfile)
{
    assert(keyfile);
    self->endpoint = inip_get_string(keyfile, "pppsinker", "endpoint", NULL);
    assert(self->endpoint);

    int rc = zsocket_bind(self->sinker, "%s", self->endpoint);
    assert(rc != -1);
}

//*************************************************************************
// rrsinker_load_config - Config the rr sinker
//*************************************************************************

void rrsinker_load_config(rrsinker_t *self, char *fname)
{
    assert(fname);
    inip_file_t *keyfile = inip_read(fname);
    self->pattern = inip_get_string(keyfile, "zsock", "pattern", NULL);
    assert(self->pattern); //** Need a more decent way to handle misconfiguration

    if (strcmp(self->pattern, "ppp") == 0) {
        _rrsinker_config_ppp(self, keyfile);
    } else {
        log_printf(0, "W: Unknown ZMQ Pattern: %s.\n", self->pattern);
        exit(EXIT_FAILURE);
    }

    inip_destroy(keyfile);
}

