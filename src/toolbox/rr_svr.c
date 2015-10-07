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

#include "rr_svr.h"

//*************************************************************************
// _rrsvr_init - Initialize rr server
//*************************************************************************

void _rrsvr_init(rrsvr_t *self)
{
    self->mode = SYNC_MODE;
    self->timeout = TIMEOUT_DFT;
    self->ctx = NULL;
    self->socket = NULL;
    self->pattern = NULL;
    self->endpoint = NULL;
    self->cli_identity = NULL;
}

//*************************************************************************
// rrsvr_new - Construct a request response server
//*************************************************************************

rrsvr_t *rrsvr_new()
{
    rrsvr_t *svr;
    type_malloc_clear(svr, rrsvr_t, 1);

    _rrsvr_init(svr);

    svr->ctx = zctx_new();
    if (!svr->ctx) {
        free(svr);
        return NULL;
    }

    return svr;
}

//************************************************************************
// rrsvr_destroy - Destroy rrsvr
//************************************************************************
void rrsvr_destroy(rrsvr_t **self_p)
{
    assert(self_p);
    if (*self_p) {
        rrsvr_t *self = *self_p;
        zctx_destroy(&self->ctx);
        free(self->pattern);
        free(self->endpoint);
        free(self->cli_identity);
        free(self);
        *self_p = NULL;
    }
}

//*************************************************************************
// _rrsvr_bind_lpp - Bind to an endpoint
//*************************************************************************

void _rrsvr_bind_lpp(rrsvr_t *self)
{
    self->socket = rr_create_socket(self->ctx, self->mode, ZMQ_REP, ZMQ_ROUTER);
    zsocket_bind(self->socket, "%s", self->endpoint);
}

//***********************************************************************
// rrsvr_construct_envelope - Construct the envelope
//***********************************************************************

void rrsvr_construct_envelope(rrsvr_t *self, zmsg_t *msg)
{
    zmsg_pushstr(msg, "%s", self->cli_identity);
}

//***********************************************************************
// rrsvr_send - Send len of bytes in buf through zmq
// OUTPUTS:
//    Returns the number of bytes in the sent message if successful. Otherwise, retruns -1.
//***********************************************************************

int rrsvr_send(rrsvr_t *self, void *buf, size_t len)
{
    zmsg_t *msg = zmsg_new();

    if (self->mode == ASYNC_MODE) {
        rrsvr_construct_envelope(self, msg);
    }

    zmsg_addmem(msg, buf, len);

    int rc = zmsg_send(&msg, self->socket);
    return rc;
}

//*************************************************************************
// rrsvr_recv - Receive data through zmq
// OUTPUTS:
//    Returns the number of bytes in the MESSAGE if successful. Note the value
//    can exceed the value of len parameter in case the message was truncated.
//    (nbytes < len ? nbytes : len) bytes are stored in the buf.  If failed, returns -1.
//*************************************************************************

int rrsvr_recv(rrsvr_t *self, void *buf, size_t len)
{
    int nbytes = 0;
    if (zsocket_poll(self->socket, self->timeout)) {
        zmsg_t *msg = zmsg_recv(self->socket);
        if (self->mode == ASYNC_MODE) { //** Retrieves envelope
            free(self->cli_identity);
            self->cli_identity = zmsg_popstr(msg);
        }

        nbytes = zmsg_content_size(msg);

        size_t to_copy = (size_t)nbytes < len ? (size_t)nbytes : len;
        zframe_t *frame = zmsg_pop(msg);
        memcpy (buf, zframe_data(frame), to_copy);
        zframe_destroy(&frame);
        zmsg_destroy(&msg);
    }

    return nbytes;
}

//**************************************************************************
// rrsvr_config_lpp - Config rrsvr to use lpp pattern
//**************************************************************************

void _rrsvr_config_lpp(rrsvr_t *self, inip_file_t *keyfile)
{
    assert(keyfile);

    rr_set_mode_tm(keyfile, "lppsvr", &self->mode, &self->timeout);
    self->endpoint = inip_get_string(keyfile, "lppsvr", "endpoint", NULL);
    assert(self->endpoint);

    _rrsvr_bind_lpp(self);
}

//*************************************************************************
// rrsvr_load_config - Config the rr server
//*************************************************************************

void rrsvr_load_config(rrsvr_t *self, char *fname)
{
    assert(fname);
    inip_file_t *keyfile = inip_read(fname);
    self->pattern = inip_get_string(keyfile, "zsock", "pattern", NULL);
    assert(self->pattern);

    if (strcmp(self->pattern, "lpp") == 0) {
        _rrsvr_config_lpp(self, keyfile);
    } else {
        log_printf(0, "Unknown ZMQ Pattern: %s.\n", self->pattern);
        exit(0);
    }

    inip_destroy(keyfile);
}
