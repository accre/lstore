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

#include "rr_cli.h"

//*************************************************************************
// _rrcli_init - Initialize rr client
//*************************************************************************

void _rrcli_init(rrcli_t *self)
{
    self->mode = SYNC_MODE;
    self->timeout = TIMEOUT_DFT; 
    self->retries = RETRIES_DFT;
    self->ctx = NULL;
    self->socket = NULL;
    self->pattern = NULL;
    self->server = NULL;
    self->broker = NULL;
    self->identity = NULL;
    self->single = NULL;
}

//*************************************************************************
// rrcli_new - Construct a request response client. 
//*************************************************************************

rrcli_t *rrcli_new()
{
    rrcli_t *cli;
    type_malloc_clear(cli, rrcli_t, 1);

    _rrcli_init(cli);

    cli->ctx = zctx_new();
    if (!cli->ctx) {
	free(cli);
	return NULL;	
    }

    return cli;       
}

//************************************************************************
// rrcli_close - Close rrcli
//************************************************************************

void rrcli_close(rrcli_t *self)
{
    zmsg_t *msg = zmsg_new();

    zmsg_pushstr(msg, RRCLI_FINISHED);
    zmsg_pushstr(msg, RR_CLIENT);
    if (self->mode == ASYNC_MODE)
        zmsg_pushstr(msg, "");

    zmsg_send(&msg, self->socket);
    zmsg_t *ack = zmsg_recv(self->socket);
    zmsg_destroy(&ack);
}

//************************************************************************
// rrcli_destroy - Destroy rrcli
//************************************************************************

void rrcli_destroy(rrcli_t **self_p) 
{
    assert(self_p);
    if (*self_p) {
	rrcli_t *self = *self_p;
	if (streq(self->pattern, "ppp"))
	    rrcli_close(self);
 	zctx_destroy(&self->ctx);
	zmsg_destroy(&self->single);	
	free(self->pattern);
	free(self->server);
	free(self->broker);
	free(self->identity);
	free(self);
	*self_p = NULL;
    }
}

//*************************************************************************
// _rrcli_create_socket - Create a socket for rrcli
//*************************************************************************

void _rrcli_create_socket(rrcli_t *self)
{
    if (self->socket) {
        zsocket_destroy(self->ctx, self->socket); //** Destroy existing socket 
    }
    
    self->socket = rr_create_socket(self->ctx, self->mode, ZMQ_REQ, ZMQ_DEALER);
    if (self->identity)
        zsocket_set_identity(self->socket, self->identity);
}

//*************************************************************************
// _rrcli_connect - Connect or reconnect to a server or broker 
//*************************************************************************

void _rrcli_connect(rrcli_t *self, char *endpoint)
{
    _rrcli_create_socket(self);
    zsocket_connect(self->socket, endpoint);
}

//***********************************************************************
// rrcli_send - Send len of bytes in buf through zmq
// OUTPUTS:
//    Return non-zero code on failure.
//***********************************************************************

int rrcli_send(rrcli_t *self, void *buf, size_t len)
{
    zmsg_t *msg = zmsg_new();
    zmsg_pushmem(msg, buf, len);

    if (streq(self->pattern, "ppp")) {
	zmsg_pushstr(msg, RRCLI_REQUEST);
	zmsg_pushstr(msg, RR_CLIENT);
	if (self->mode == ASYNC_MODE)
	    zmsg_pushstr(msg, "");
    }

    if (self->mode == SYNC_MODE) {
	if (self->single) {
	    zmsg_destroy(&self->single);
	}
        self->single = zmsg_dup(msg);
    }

    int rc = zmsg_send(&msg, self->socket);

    return rc;
}

//*************************************************************************
// _rrcli_retry_to_connect - Reconnect to the remote endpoint and resends message
//*************************************************************************

void _rrcli_retry_to_connect(rrcli_t *self)
{
    if (strcmp(self->pattern, "lpp") == 0) {
        _rrcli_connect(self, self->server);
    } else if (strcmp(self->pattern, "md") == 0) {
        _rrcli_connect(self, self->broker);
    } 

    zmsg_t *msg = zmsg_dup(self->single);
    zmsg_send(&msg, self->socket);
}

//*************************************************************************
// rrcli_recv - Receive data through zmq
// OUTPUTS:
//    Returns the number of bytes in the MESSAGE if successful. Note the value
//    can exceed the value of len parameter in case the message was truncated.
//    (nbytes < len ? nbytes : len) bytes are stored in the buf. If failed, returns -1.
//*************************************************************************

int rrcli_recv(rrcli_t *self, void *buf, size_t len)
{
    int retries = self->retries;
    int nbytes = -1;

    while (1) {
        if (zsocket_poll(self->socket, self->timeout)) {
            nbytes = zmq_recv(self->socket, buf, len, 0);
            assert(nbytes != -1);
            break;
        }

        if (self->mode == ASYNC_MODE) break;

        if (--retries < 0) break;

        //** Reconnection
        _rrcli_retry_to_connect(self); 
    }

    return nbytes;
}

//***********************************************************************
// _rrcli_config_base - Config common fields 
//***********************************************************************

void _rrcli_config_base(rrcli_t *self, inip_file_t *keyfile, char *section)
{
    assert(keyfile);
    assert(section);

    rr_set_mode_tm(keyfile, section, &self->mode, &self->timeout); 
    self->retries = inip_get_integer(keyfile, section, "retries", RETRIES_DFT);
    self->identity = inip_get_string(keyfile, section, "identity", NULL);
}

//**************************************************************************
// rrcli_config_lpp - Config rrcli to use lpp pattern
//**************************************************************************

void _rrcli_config_lpp(rrcli_t *self, inip_file_t *keyfile)
{
    assert(keyfile);
    
    _rrcli_config_base(self, keyfile, "lppcli");

    self->server = inip_get_string(keyfile, "lppcli", "server", NULL);
    assert(self->server);

    _rrcli_connect(self, self->server);
}

//**************************************************************************
// _rrcli_config - Config rrcli 
//**************************************************************************

void _rrcli_config(rrcli_t *self, inip_file_t *keyfile, char *pattern)
{
    assert(keyfile);

    _rrcli_config_base(self, keyfile, pattern);

    self->broker = inip_get_string(keyfile, pattern, "broker", NULL);
    assert(self->broker);

    _rrcli_connect(self, self->broker);
}

//**************************************************************************
// rrcli_config_md - Config rrcli to use md pattern 
//**************************************************************************

void _rrcli_config_md(rrcli_t *self, inip_file_t *keyfile)
{
    _rrcli_config(self, keyfile, "mdcli");
}

//**************************************************************************
// rrcli_config_ppp - Config rrcli to use ppp pattern 
//**************************************************************************

void _rrcli_config_ppp(rrcli_t *self, inip_file_t *keyfile)
{
    _rrcli_config(self, keyfile, "pppcli");
}

//*************************************************************************
// rrcli_load_config - Config the rr client
//*************************************************************************

void rrcli_load_config(rrcli_t *self, char *fname)
{
    assert(fname);
    inip_file_t *keyfile = inip_read(fname);
    self->pattern = inip_get_string(keyfile, "zsock", "pattern", NULL);
    assert(self->pattern); //** Need a more decent way to handle misconfiguration 

    if (strcmp(self->pattern, "lpp") == 0) {
	_rrcli_config_lpp(self, keyfile);
    } else if (strcmp(self->pattern, "md") == 0) {
	_rrcli_config_md(self, keyfile);
    } else if (strcmp(self->pattern, "ppp") == 0) {
	_rrcli_config_ppp(self, keyfile);
    } else {
	log_printf(0, "Unknown ZMQ Pattern: %s.\n", self->pattern);
 	exit(0);	
    } 

    inip_destroy(keyfile);
}


