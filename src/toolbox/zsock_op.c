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

#include "zsock_op.h"

op_status_t zsock_success_status = {OP_STATE_SUCCESS, ZSOCK_OK};
op_status_t zsock_failure_status = {OP_STATE_FAILURE, 0};

//*************************************************************************
// gop_write - Send data
//*************************************************************************

op_status_t gop_write(NetStream_t *ns, op_generic_t *gop, tbuffer_t *buffer, zsock_off_t pos, zsock_off_t size) {
    int nbytes;
    op_status_t status;
    Net_timeout_t dt;

    set_net_timeout(&dt, 1, 0);

    nbytes = 0;
    nbytes = write_netstream(ns, buffer, pos, size, dt);

    if (nbytes == size) {
        status = zsock_success_status;
    } else {
        status = zsock_failure_status;
    }

    return(status);
}

//*************************************************************************
// gop_read - Receive data. timeout is set to be 1s
//*************************************************************************

op_status_t gop_read(NetStream_t *ns, op_generic_t *gop, tbuffer_t *buffer, zsock_off_t pos, zsock_off_t size) {
    op_status_t status;
    Net_timeout_t dt;

    set_net_timeout(&dt, 1, 0);

    int rc = read_netstream(ns, buffer, pos, size, dt);

    if (rc <= 0) {
        status = zsock_failure_status;
    } else {
        status = zsock_success_status;
    }

    return(status);
}

//*************************************************************************
// _zsock_op_free -  Free an op's space
//*************************************************************************

void _zsock_op_free(op_generic_t *gop, int mode) { //** Are these enough? or redudant?

    log_printf(0, "_zsock_op_free: mode=%d gid=%d gop=%p\n", mode, gop_id(gop), gop);

    if (gop->op->cmd.hostport != NULL) {
        free(gop->op->cmd.hostport);
        gop->op->cmd.hostport = NULL;
    }

//    zsock_op_t *zop;
//    zop = zsock_get_zop(gop);
//    free(zop->rw_op.rwbuf);

    gop_generic_free(gop, OP_FINALIZE);

    if (mode == OP_DESTROY) free(gop->free_ptr);
    log_printf(0, "_zsock_op_free:END\n");

}

//*************************************************************************
// init_zsock_op - Initialize an ZSOCK op
//*************************************************************************

void init_zsock_op(zsock_context_t *zc, zsock_op_t *op) {
    op_generic_t *gop;

    type_memclear(op, zsock_op_t, 1);

    gop = &(op->gop);
    gop_init(gop);
    gop->op = &(op->dop);
    gop->op->priv = op;
    gop->type = Q_TYPE_OPERATION; //** What is this field?
    op->zc = zc;
    op->dop.priv = op;
    op->dop.pc = zc->pc;
    gop->base.free = _zsock_op_free;
    gop->free_ptr = op;
    gop->base.pc = zc->pc;
    gop->base.status = op_error_status;
}

//*************************************************************************
// new_zsock_op -  Allocate space for a new op
//*************************************************************************

zsock_op_t *new_zsock_op(zsock_context_t *zc) {
    zsock_op_t *op;

    type_malloc(op, zsock_op_t, 1);

    atomic_inc(zc->n_ops);
    init_zsock_op(zc, op);

    return op;
}

//************************************************************************
// new_zsock_rw_op - Create a new IO operation
//************************************************************************

op_generic_t *new_zsock_rw_op(zsock_context_t *zc, char *hostname, int port, int rw_type, tbuffer_t *buffer, zsock_off_t boff, zsock_off_t len, int timeout) {
    zsock_op_t *op = new_zsock_op(zc);
    if (op == NULL) return NULL;

    set_zsock_rw_op(op, hostname, port, rw_type, buffer, boff, len, timeout);

    return(zsock_get_gop(op));
}

//************************************************************************
// set_zsock_rw_op - Generate a new IO operation
//************************************************************************

void set_zsock_rw_op(zsock_op_t *op, char *hostname, int port, int rw_type, tbuffer_t *buffer, zsock_off_t boff, zsock_off_t len, int timeout) {
    zsock_op_rw_t *cmd;
    zsock_rw_buf_t *rwbuf;
    char *hoststr;

    cmd = &(op->rw_op);

    op_generic_t *gop = zsock_get_gop(op);

    //** This is important
    op->dop.cmd.connect_context = &(op->zc->cc[rw_type]);
    asprintf(&hoststr, "%s" HP_HOSTPORT_SEPARATOR "%d", hostname, port);
    op->dop.cmd.hostport = strdup(hoststr);//"129.59.132.61|5001");

    cmd->size = len; //* This is the total size

    rwbuf = &(cmd->buf_single);
    cmd->bs_ptr = rwbuf;
    cmd->rwbuf = &(cmd->bs_ptr);
    cmd->n_ops = 1;
    cmd->n_iovec_total = 1;
    cmd->rw_mode = rw_type;

    rwbuf->n_iovec = 1;
    rwbuf->buffer = buffer;
    rwbuf->boff = boff;
    rwbuf->size = len;

    if (rw_type == ZSOCK_WRITE) {
        gop->op->cmd.send_command = NULL;
        gop->op->cmd.send_phase = write_send;
        gop->op->cmd.recv_phase = NULL;
        gop->op->cmd.on_submit = NULL;
        gop->op->cmd.before_exec = NULL;
    } else {
        gop->op->cmd.send_command = NULL;
        gop->op->cmd.send_phase = NULL;
        gop->op->cmd.recv_phase = read_recv;
        gop->op->cmd.on_submit = NULL;
        gop->op->cmd.before_exec = NULL;
    }

    free(hoststr);
}

//************************************************************************
// new_zsock_read_op - Create a new read operation
//************************************************************************

op_generic_t *new_zsock_read_op(zsock_context_t *zc, char *hostname, int port, tbuffer_t *buffer, zsock_off_t boff, zsock_off_t len, int timeout) {
    op_generic_t *op = new_zsock_rw_op(zc, hostname, port, ZSOCK_READ, buffer, boff, len, timeout);
    return op;
}

//************************************************************************
// set_zsock_read_ip - Generate a new read operation
//************************************************************************

void set_zsock_read_op(zsock_op_t *op, char *hostname, int port, tbuffer_t *buffer, zsock_off_t boff, zsock_off_t len, int timeout) {
    set_zsock_rw_op(op, hostname, port, ZSOCK_READ, buffer, boff, len, timeout);
}

//***********************************************************************
// new_zsock_write_op - Create a new write operation
//***********************************************************************

op_generic_t *new_zsock_write_op(zsock_context_t *zc, char *hostname, int port, tbuffer_t *buffer, zsock_off_t boff, zsock_off_t len, int timeout) {
    op_generic_t *op = new_zsock_rw_op(zc, hostname, port, ZSOCK_WRITE, buffer, boff, len, timeout);
    return op;
}

//***********************************************************************
// set_zsock_write_op - Generate a new write operation
//***********************************************************************

void set_zsock_write_op(zsock_op_t *op, char *hostname, int port, tbuffer_t *buffer, zsock_off_t boff, zsock_off_t len, int timeout) {
    set_zsock_rw_op(op, hostname, port, ZSOCK_WRITE, buffer, boff, len, timeout);
}

//***********************************************************************
// set_zsock_vec_write_op - Generates a new vec write operation
//***********************************************************************

//void set_zsock_vec_write_op(zsock_op_t *op, int n_iovec, zsock_iovec_t *iovec, tbuffer_t *buffer, zsock_off_t boff, zsock_off_t len, int timeout)
//{

//}

//***********************************************************************
// write_send - Execute write operation
//***********************************************************************

op_status_t write_send(op_generic_t *gop, NetStream_t *ns) {
    zsock_op_t *zop = zsock_get_zop(gop);
    zsock_op_rw_t *cmd = &(zop->rw_op);

    log_printf(0, "write_send: START gid:%d n_ops=%d\n", gop_id(gop), cmd->n_ops);

    int i;
    zsock_rw_buf_t *rwbuf;
    op_status_t err;
    for (i = 0; i < cmd->n_ops; i++) {
        rwbuf = cmd->rwbuf[i];
        log_printf(0, "gid=%d ns=%d i=%d size=" I64T "\n", gop_id(gop), ns_getid(ns), i, rwbuf->size);
        err = gop_write(ns, gop, rwbuf->buffer, rwbuf->boff, rwbuf->size);
        log_printf(0, "gid=%d ns=%d i=%d status=%d\n", gop_id(gop), ns_getid(ns), i, err.op_status);
        if (err.op_status != OP_STATE_SUCCESS) break;
    }

    log_printf(0, "write_send: END ns=%d status=%d\n", ns_getid(ns), err.op_status);
    return err;
}

//***********************************************************************
// read_recv - Execute read operation
//***********************************************************************

op_status_t read_recv(op_generic_t *gop, NetStream_t *ns) {
    zsock_op_t *zop = zsock_get_zop(gop);
    zsock_op_rw_t *cmd = &(zop->rw_op);

    log_printf(0, "read_recv: START gid:%d n_ops=%d\n", gop_id(gop), cmd->n_ops);

    int i;
    zsock_rw_buf_t *rwbuf;
    op_status_t err;
    for (i = 0; i < cmd->n_ops; i++) {
        rwbuf = cmd->rwbuf[i];
        log_printf(0, "gid=%d ns=%d i=%d size=" I64T "\n", gop_id(gop), ns_getid(ns), i, rwbuf->size);
        err = gop_read(ns, gop, rwbuf->buffer, rwbuf->boff, rwbuf->size);
        log_printf(0, "gid=%d ns=%d i=%d status=%d\n", gop_id(gop), ns_getid(ns), i, err.op_status);
        if (err.op_status != OP_STATE_SUCCESS) break;
    }

    log_printf(0, "read_recv: END ns=%d status=%d\n", ns_getid(ns), err.op_status);
    return err;
}
