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
// _rrsvr_init - Initializes rr server 
//*************************************************************************

void _rrsvr_init(rrsvr_t *self)
{
    self->mode = SYNC_MODE;
    self->timeout = TIMEOUT_DFT; 
    self->ctx = NULL;
    self->socket = NULL;
    self->pattern = NULL;
    self->endpoint = NULL;
}

//*************************************************************************
// rrsvr_new - Constructs a request response server 
//*************************************************************************

rrsvr_t *rrsvr_new()
{
    rrsvr_t *svr;
    type_malloc_clear(svr, rrsvr_t, 1);

    _rrsvr_init(svr);

    svr->ctx = zctx_new();

    return svr;       
}

//************************************************************************
// rrsvr_destroy - Destroys rrsvr
//************************************************************************
void rrsvr_destroy(rrsvr_t **self_p) 
{
    assert(self_p);
    if (*self_p) {
	rrsvr_t *self = *self_p;
 	zctx_destroy(&self->ctx);
	free(self->pattern);
	free(self->endpoint);
	free(self);
	*self_p = NULL;
    }
}

//*************************************************************************
// _rrsvr_bind_lpp - Binds to an endpoint 
//*************************************************************************

void _rrsvr_bind_lpp(rrsvr_t *self)
{
    self->socket = rr_create_socket(self->ctx, self->mode, ZMQ_REP, ZMQ_ROUTER);
    zsocket_bind(self->socket, "%s", self->endpoint);
}

//***********************************************************************
// rrsvr_send - Sends buf through zmq
// OUTPUTS:
//    Returns the number of bytes in the sent message if successful. Otherwise, retruns -1.
//***********************************************************************

int rrsvr_send(rrsvr_t *self, void *buf, size_t len)
{
    zmsg_t *msg = zmsg_new();
    zmsg_pushmem(msg, buf, len);

    int rc = zmsg_send(&msg, self->socket);
    return rc;
}

//*************************************************************************
// rrsvr_recv - Receives data through zmq
// OUTPUTS:
//    Returns the number of bytes in the MESSAGE if successful. Note the value
//    can exceed the value of len parameter in case the message was truncated.
//*************************************************************************

int rrsvr_recv(rrsvr_t *self, void *buf, size_t len)
{
    int nbytes = 0;
    if (zsocket_poll(self->socket, self->timeout)) {
        nbytes = zmq_recv(self->socket, buf, len, 0);
        assert(nbytes != -1);
    }

    return nbytes;
}

//**************************************************************************
// rrsvr_config_lpp - Configs rrsvr to use lpp pattern
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
// rrsvr_load_config - Configs the rr server 
//*************************************************************************

void rrsvr_load_config(rrsvr_t *self, char *fname)
{
/*    if (fname == NULL) {
	if (os_local_filetype("zsock.cfg") != 0) {
	    fname = zsock.cfg;
	} else if (os_local_filetype("~/zsock.cfg") != 0) {
	    fname = "~/zsock.cfg";
	} else if (os_local_filetype("/etc/zsock.cfg") != 0) {
	    fname = "/etc/zsock.cfg";
	}
    }
*/
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


