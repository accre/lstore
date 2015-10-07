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

#ifndef _ZSOCK_OP_H_
#define _ZSOCK_OP_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <apr_pools.h>
#include "host_portal.h"
#include "atomic_counter.h"
#include "zsock_config.h"

#define ZSOCK_READ 0
#define ZSOCK_WRITE 1

#define ZSOCK_OK 1

//op_status_t zsock_success_status = {OP_STATE_SUCCESS, ZSOCK_OK};
//op_status_t zsock_failure_status = {OP_STATE_FAILURE, 0};

typedef struct {
    tbuffer_t *buffer;
    zsock_off_t size;
    zsock_off_t boff;
    int n_iovec;   
} zsock_rw_buf_t;

typedef struct { //** Read/Write operation
    int rw_mode;
    int n_ops;
    int n_iovec_total;
    zsock_off_t size;
    zsock_rw_buf_t **rwbuf;
    zsock_rw_buf_t *bs_ptr; //** What is used for?
    zsock_rw_buf_t buf_single;
} zsock_op_rw_t;

typedef struct {
    zsock_context_t *zc;
    op_generic_t gop;
    op_data_t dop;
    union {
	zsock_op_rw_t rw_op;
    };
} zsock_op_t;

#define zsock_get_gop(a) &((a)->gop)
#define zsock_get_zop(a) (a)->op->priv
#define zsock_reset_zop(a) gop_reset(zsock_get_gop((a)))

zsock_op_t *new_zsock_op(zsock_context_t *zc);
void init_zsock_op(zsock_context_t *zc, zsock_op_t *op);
void _zsock_op_free(op_generic_t *gop, int mode);

void set_zsock_rw_op(zsock_op_t *op, char *hostname, int port, int rw_type, tbuffer_t *buffer, zsock_off_t boff, zsock_off_t len, int timeout); 
op_generic_t *new_zsock_rw_op(zsock_context_t *zc, char *hostname, int port,  int rw_type, tbuffer_t *buffer, zsock_off_t boff, zsock_off_t len, int timeout);

op_generic_t *new_zsock_read_op(zsock_context_t *zc, char *hostname, int port, tbuffer_t *buffer, zsock_off_t boff, zsock_off_t len, int timeout);
void set_zsock_read_op(zsock_op_t *op, char *hostname, int port, tbuffer_t *buffer, zsock_off_t boff, zsock_off_t len, int timeout); 

op_generic_t *new_zsock_write_op(zsock_context_t *zc, char *hostname, int port, tbuffer_t *buffer, zsock_off_t boff, zsock_off_t len, int timeout);
void set_zsock_write_op(zsock_op_t *op, char *hostname, int port, tbuffer_t *buffer, zsock_off_t boff, zsock_off_t len, int timeout); 

op_status_t write_send(op_generic_t *gop, NetStream_t *ns);
op_status_t read_recv(op_generic_t *gop, NetStream_t *ns);
op_status_t gop_write(NetStream_t *ns, op_generic_t *gop, tbuffer_t *buffer, zsock_off_t pos, zsock_off_t size);

#ifdef __cplusplus
}
#endif

#endif
