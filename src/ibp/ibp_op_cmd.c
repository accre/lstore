/*
   Copyright 2016 Vanderbilt University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#define _log_module_index 132

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <tbx/assert_result.h>
#include "ibp.h"
#include <tbx/fmttypes.h>
#include <tbx/network.h>
#include <tbx/log.h>
#include "ibp_misc.h"
#include <tbx/dns_cache.h>
#include <tbx/type_malloc.h>
#include <tbx/append_printf.h>
#include <tbx/type_malloc.h>
#include <tbx/string_token.h>

#define ibp_set_status(v, opstat, errcode) (v).op_status = status; (v).error_code = errorcode

apr_time_t gop_get_end_time(op_generic_t *gop, int *state);
op_status_t gop_readline_with_timeout(tbx_ns_t *ns, char *buffer, int size, op_generic_t *gop);
op_status_t gop_write_block(tbx_ns_t *ns, op_generic_t *gop, tbx_tbuf_t *buffer, ibp_off_t pos, ibp_off_t size);
op_status_t gop_read_block(tbx_ns_t *ns, op_generic_t *gop, tbx_tbuf_t *buffer, ibp_off_t pos, ibp_off_t size);

op_status_t status_get_recv(op_generic_t *gop, tbx_ns_t *ns);
void _ibp_op_free(op_generic_t *op, int mode);

op_status_t vec_read_command(op_generic_t *gop, tbx_ns_t *ns);
op_status_t vec_write_command(op_generic_t *gop, tbx_ns_t *ns);

// In ibp_op.c
void set_hostport(char *hostport, int max_size, char *host, int port, ibp_connect_context_t *cc);
int process_inq(char *buffer, ibp_depotinfo_t *di);

op_status_t process_error(op_generic_t *gop, op_status_t *err, int status, double wait_time, char **bstate)
{
    int fin;
    apr_time_t sec, usec;

    if (status == IBP_E_OUT_OF_SOCKETS) {
        if (bstate != NULL) {
            wait_time = atof(tbx_stk_string_token(NULL, " ", bstate, &fin));
        }
        if (wait_time < 0) wait_time = 0;
        sec=wait_time;
        usec = (wait_time-sec)*1000000;
        gop->op->cmd.retry_wait = apr_time_make(sec, usec);

        log_printf(5, "gid=%d status=%d retry_wait=%lf (s,us)=(" TT "," TT ")\n", gop_id(gop), status, wait_time, sec, usec);
        *err = ibp_retry_status;
    } else if (status == IBP_OK) {
        err->op_status = OP_STATE_SUCCESS;
        err->error_code = IBP_OK;
    } else {
        err->op_status = OP_STATE_FAILURE;
        err->error_code = status;
    }

    log_printf(5, "gid=%d status=%d ibp_err=%d\n", gop_id(gop), err->op_status, err->error_code);

    return(*err);
}

op_status_t send_command(op_generic_t *gop, tbx_ns_t *ns, char *command)
{
    tbx_ns_timeout_t dt;
    tbx_ns_timeout_set(&dt, 5, 0);
    tbx_tbuf_t buf;
    op_status_t status;

    log_printf(5, "send_command: ns=%d gid=%d command=%s\n", tbx_ns_getid(ns), gop_id(gop), command);

    int len = strlen(command);
    tbx_tbuf_single(&buf, len, command);
    status = gop_write_block(ns, gop, &buf, 0, len);
    if (status.op_status !=  OP_STATE_SUCCESS) {
        log_printf(10, "send_command: Error=%d! ns=%d command=!%s!", status.op_status, tbx_ns_getid(ns), command);
        return(ibp_retry_status);
    }

    return(status);
}

op_status_t gop_readline_with_timeout(tbx_ns_t *ns, char *buffer, int size, op_generic_t *gop)
{
    int nbytes, n, nleft, pos;
    int err, state;
    apr_time_t end_time;
    op_status_t status;
    tbx_tbuf_t tbuf;

    log_printf(15, "readline_with_timeout: START ns=%d size=%d\n", tbx_ns_getid(ns), size);
    state = 0;
    nbytes = 0;
    err = 0;
    nleft = size;
    tbx_tbuf_single(&tbuf, size, buffer);
    pos = 0;
    end_time = gop_get_end_time(gop, &state);
    while ((err == 0) && (apr_time_now() <= end_time) && (nleft > 0)) {
        n = tbx_ns_readline_raw(ns, &tbuf, pos, nleft, global_dt, &err);
        nleft = nleft - n;
        nbytes = nbytes + n;
        pos = pos + nbytes;
        log_printf(15, "readline_with_timeout: nbytes=%d nleft=%d err=%d time=" TT " end_time=" TT " ns=%d buffer=%s\n", nbytes, nleft, err, apr_time_now(), end_time, tbx_ns_getid(ns), buffer);
        if (nleft > 0) end_time = gop_get_end_time(gop, &state);
    }

    if (err > 0) {
        err = IBP_OK;
        status = ibp_success_status;
        log_printf(15, "readline_with_timeout: END nbytes=%d command=%s\n", nbytes, buffer);
    } else {
        if (err == 0) {
            if (nbytes < size) {
                log_printf(15, "readline_with_timeout: END Client timeout time=" TT " end_time=" TT "ns=%d\n", apr_time_now(), end_time, tbx_ns_getid(ns));
            } else {
                log_printf(0, "readline_with_timeout:  END Out of sync issue!! nbytes=%d size=%d ns=%d\n", nbytes, size, tbx_ns_getid(ns));
                tbx_log_flush();
                err = 0; //** GEnerate a core dump
            }
            err = ERR_RETRY_DEADSOCKET;
            status = ibp_retry_status;
        } else {
            log_printf(15, "readline_with_timeout: END connection error=%d ns=%d\n", err, tbx_ns_getid(ns));
            err = ERR_RETRY_DEADSOCKET;
            status = ibp_retry_status;
        }
    }


    return(status);
}

op_status_t vec_read_command(op_generic_t *gop, tbx_ns_t *ns)
{
    ibp_op_t *op = ibp_get_iop(gop);
    int bufsize = 204800;
    char stackbuffer[bufsize];
    char *buffer = stackbuffer;
    int i, j, used;
    ibp_op_rw_t *cmd;
    ibp_rw_buf_t *rwbuf;
    op_status_t err;

    cmd = &(op->ops.rw_op);

    used = 0;

    //** Store the base command
    if (tbx_ns_chksum_is_valid(&(op->ncs)) == 0) {
        tbx_append_printf(buffer, &used, bufsize, "%d %d %s %s %d", IBPv040, IBP_VEC_READ, cmd->key, cmd->typekey, cmd->n_tbx_iovec_total);
    } else {
        tbx_append_printf(buffer, &used, bufsize, "%d %d %d " I64T " %s %s %d",
                      IBPv040, IBP_VEC_READ_CHKSUM, tbx_ns_chksum_type(&(op->ncs)), tbx_ns_chksum_blocksize(&(op->ncs)),
                      cmd->key, cmd->typekey, cmd->n_tbx_iovec_total);
    }

    //** Add the IO vec list
    for (j=0; j<cmd->n_ops; j++) {
        rwbuf = cmd->rwbuf[j];
        for (i=0; i<rwbuf->n_iovec; i++) {
            if (used >= (bufsize-100)) {
                bufsize = bufsize * 1.5;
                if (buffer == stackbuffer) {
                    buffer = (char *)malloc(bufsize);
                    memcpy(buffer, stackbuffer, used);
                } else {
                    buffer = (char *)realloc(buffer, bufsize);
                }
            }
            tbx_append_printf(buffer, &used, bufsize, " " I64T " " I64T, rwbuf->iovec[i].offset, rwbuf->iovec[i].len);
        }
    }

    //** Add the timeout
    tbx_append_printf(buffer, &used, bufsize, " %d\n", (int)apr_time_sec(gop->op->cmd.timeout));

    tbx_ns_chksum_write_set(ns, op->ncs);
    tbx_ns_chksum_write_disable(ns);

    err = send_command(gop, ns, buffer);
    if (err.op_status != OP_STATE_SUCCESS) {
        log_printf(10, "read_command: Error with send_command()! ns=%d\n", tbx_ns_getid(ns));
    }

    if (buffer != stackbuffer) free(buffer);

    return(err);
}

op_status_t read_command(op_generic_t *gop, tbx_ns_t *ns)
{
    ibp_op_t *op = ibp_get_iop(gop);
    char buffer[1024];
    op_status_t err;
    ibp_op_rw_t *cmd;

    cmd = &(op->ops.rw_op);

    if (tbx_ns_chksum_is_valid(&(op->ncs)) == 0) {
        snprintf(buffer, sizeof(buffer), "%d %d %s %s " I64T " " I64T " %d\n",
                 IBPv040, IBP_LOAD, cmd->key, cmd->typekey, cmd->buf_single.iovec[0].offset, cmd->buf_single.size, (int)apr_time_sec(gop->op->cmd.timeout));
    } else {
        snprintf(buffer, sizeof(buffer), "%d %d %d " I64T " %s %s " I64T " " I64T " %d\n",
                 IBPv040, IBP_LOAD_CHKSUM, tbx_ns_chksum_type(&(op->ncs)), tbx_ns_chksum_blocksize(&(op->ncs)),
                 cmd->key, cmd->typekey, cmd->buf_single.iovec[0].offset, cmd->buf_single.size, (int)apr_time_sec(gop->op->cmd.timeout));
    }

    tbx_ns_chksum_write_set(ns, op->ncs);
    tbx_ns_chksum_write_disable(ns);

    err = send_command(gop, ns, buffer);
    if (err.op_status != OP_STATE_SUCCESS) {
        log_printf(10, "read_command: Error with send_command()! ns=%d\n", tbx_ns_getid(ns));
    }

    return(err);
}

op_status_t gop_read_block(tbx_ns_t *ns, op_generic_t *gop, tbx_tbuf_t *buffer, ibp_off_t pos, ibp_off_t size)
{
    int nbytes, state, bpos, nleft;
    op_status_t status;
    tbx_ns_timeout_t dt;
    apr_time_t end_time;

    state = 0;
    tbx_ns_timeout_set(&dt, 1, 0);
    end_time = gop_get_end_time(gop, &state);

    nbytes = 0;
    bpos = pos;
    nleft = size;
    while ((nbytes != -1) && (nleft > 0) && (apr_time_now() < end_time)) {
        nbytes = tbx_ns_read(ns, buffer, bpos, nleft, dt);
        if (nbytes != -1) {
            bpos += nbytes;
            nleft -= nbytes;
        }

        end_time = gop_get_end_time(gop, &state);

    }

    if (nleft == 0) {
        status = ibp_success_status;
    } else if (apr_time_now() > end_time) {
        status = ibp_timeout_status;
        status.error_code = size - nleft;
    } else {
        status = ibp_retry_status;  //** Dead connection so retry
        status.error_code = size - nleft;
    }

    return(status);
}

op_status_t read_recv(op_generic_t *gop, tbx_ns_t *ns)
{
    ibp_op_t *op = ibp_get_iop(gop);
    ibp_off_t nbytes;
    int i, status, fin;
    op_status_t err;
    char buffer[1024];
    char *bstate;
    ibp_op_rw_t *cmd;
    double swait;
    ibp_rw_buf_t *rwbuf;

    cmd = &(op->ops.rw_op);

    //** Need to read the depot status info
    log_printf(15, "read_recv: ns=%d starting command size=" OT "\n", tbx_ns_getid(ns), cmd->size);

    tbx_ns_chksum_read_set(ns, op->ncs);

    err = gop_readline_with_timeout(ns, buffer, sizeof(buffer), gop);
    if (err.op_status != OP_STATE_SUCCESS) return(err);

    log_printf(15, "read_recv: after readline err = %d  ns=%d buffer=%s\n", err.op_status, tbx_ns_getid(ns), buffer);

    rwbuf = cmd->rwbuf[0];

    err = ibp_success_status;
    status = atoi(tbx_stk_string_token(buffer, " ", &bstate, &fin));
    swait = atof(tbx_stk_string_token(NULL, " ", &bstate, &fin));
    nbytes = swait;
    if ((status != IBP_OK) || (nbytes != cmd->size)) {
        log_printf(15, "read_recv: (read) ns=%d cap=%s offset[0]=" I64T " len[0]=" I64T " err=%d Error!  status=%d bytes=!%s!\n",
                   tbx_ns_getid(ns), cmd->cap, rwbuf->iovec[0].offset, rwbuf->size, err.op_status, status, buffer);

        process_error(gop, &err, status, swait, NULL);
        return(err);
    }


    //** Turn on chksumming if needed
    if (tbx_ns_chksum_is_valid(&(op->ncs)) == 1) {
        tbx_ns_chksum_reset(&(op->ncs));
        tbx_ns_chksum_read_set(ns, op->ncs);
        tbx_ns_chksum_read_enable(ns);
    }

    for (i=0; i<cmd->n_ops; i++) {
        rwbuf = cmd->rwbuf[i];
        err = gop_read_block(ns, gop, rwbuf->buffer, rwbuf->boff, rwbuf->size);
        log_printf(5, "gid=%d ns=%d i=%d size=" I64T "\n", gop_id(gop), tbx_ns_getid(ns), i, rwbuf->size);
        if (err.op_status != OP_STATE_SUCCESS) break;

        log_printf(15, "read_recv: ns=%d op_index=%d  size=" I64T " pos=" I64T " time=" TT "\n", tbx_ns_getid(ns), i,
                   rwbuf->size, rwbuf->boff, apr_time_now());
    }

    if (err.op_status == OP_STATE_SUCCESS) {  //** Call the next block routine to process the last chunk
        if (tbx_ns_chksum_is_valid(&(op->ncs)) == 1) {
            if (tbx_ns_chksum_read_flush(ns) != 0) {
                err.op_status = OP_STATE_FAILURE;
                err.error_code = IBP_E_CHKSUM;
            }
            tbx_ns_chksum_read_disable(ns);
        }
    } else {
        rwbuf = cmd->rwbuf[0];
        log_printf(0, "read_recv: (read) ns=%d cap=%s offset[0]=" I64T " len[0]=" I64T " got=%d Error!\n",
                   tbx_ns_getid(ns), cmd->cap, rwbuf->iovec[0].offset, rwbuf->size, err.error_code);
    }

    return(err);
}

op_status_t vec_write_command(op_generic_t *gop, tbx_ns_t *ns)
{
    ibp_op_t *op = ibp_get_iop(gop);
    int bufsize = 204800;
    char stackbuffer[bufsize];
    char *buffer = stackbuffer;
    op_status_t err;
    int i,j,used;
    ibp_op_rw_t *cmd;
    ibp_rw_buf_t *rwbuf;

    cmd = &(op->ops.rw_op);

    used = 0;

    //** Store base command
    if (tbx_ns_chksum_is_valid(&(op->ncs)) == 0) {
        tbx_append_printf(buffer, &used, bufsize, "%d %d %s %s %d",
                      IBPv040, IBP_VEC_WRITE, cmd->key, cmd->typekey, cmd->n_tbx_iovec_total);
    } else {
        tbx_append_printf(buffer, &used, bufsize, "%d %d %d %d %s %s %d",
                      IBPv040, IBP_VEC_WRITE_CHKSUM, tbx_ns_chksum_type(&(op->ncs)), tbx_ns_chksum_blocksize(&(op->ncs)),
                      cmd->key, cmd->typekey, cmd->n_tbx_iovec_total);
    }


    //** Add the IO vec list
    for (j=0; j<cmd->n_ops; j++) {
        rwbuf = cmd->rwbuf[j];
        for (i=0; i<rwbuf->n_iovec; i++) {
            if (used >= (bufsize-100)) {
                bufsize = bufsize * 1.5;
                if (buffer == stackbuffer) {
                    buffer = (char *)malloc(bufsize);
                    memcpy(buffer, stackbuffer, used);
                } else {
                    buffer = (char *)realloc(buffer, bufsize);
                }
            }
            tbx_append_printf(buffer, &used, bufsize, " " I64T " " I64T, rwbuf->iovec[i].offset, rwbuf->iovec[i].len);
        }
    }

    //** Add the timeout
    tbx_append_printf(buffer, &used, bufsize, " %d\n", (int)apr_time_sec(gop->op->cmd.timeout));

    tbx_ns_chksum_write_set(ns, op->ncs);
    tbx_ns_chksum_write_disable(ns);

    log_printf(1, "sending command ns=%d gid=%d, command=%s\n", tbx_ns_getid(ns), gop_id(gop), buffer);

    err = send_command(gop, ns, buffer);
    if (err.op_status != OP_STATE_SUCCESS) {
        log_printf(10, "write_command: Error with send_command()! ns=%d\n", tbx_ns_getid(ns));
    }

    if (buffer != stackbuffer) free(buffer);

    return(err);
}

op_status_t write_command(op_generic_t *gop, tbx_ns_t *ns)
{
    ibp_op_t *op = ibp_get_iop(gop);
    char buffer[1024];
    op_status_t err;
    ibp_op_rw_t *cmd;

    cmd = &(op->ops.rw_op);

    if (tbx_ns_chksum_is_valid(&(op->ncs)) == 0) {
        snprintf(buffer, sizeof(buffer), "%d %d %s %s " I64T " " I64T " %d\n",
                 IBPv040, IBP_WRITE, cmd->key, cmd->typekey, cmd->buf_single.iovec[0].offset, cmd->buf_single.size, (int)apr_time_sec(gop->op->cmd.timeout));
    } else {
        snprintf(buffer, sizeof(buffer), "%d %d %d " I64T " %s %s " I64T " " I64T " %d\n",
                 IBPv040, IBP_WRITE_CHKSUM, tbx_ns_chksum_type(&(op->ncs)), tbx_ns_chksum_blocksize(&(op->ncs)),
                 cmd->key, cmd->typekey, cmd->buf_single.iovec[0].offset, cmd->buf_single.size, (int)apr_time_sec(gop->op->cmd.timeout));
    }

    tbx_ns_chksum_write_set(ns, op->ncs);
    tbx_ns_chksum_write_disable(ns);

    log_printf(1, "sending command ns=%d gid=%d, command=%s\n", tbx_ns_getid(ns), gop_id(gop), buffer);

    err = send_command(gop, ns, buffer);
    if (err.op_status != OP_STATE_SUCCESS) {
        log_printf(10, "write_command: Error with send_command()! ns=%d\n", tbx_ns_getid(ns));
    }

    return(err);
}

op_status_t gop_write_block(tbx_ns_t *ns, op_generic_t *gop, tbx_tbuf_t *buffer, ibp_off_t pos, ibp_off_t size)
{
    int nbytes, state, bpos, nleft;
    op_status_t status;
    tbx_ns_timeout_t dt;
    apr_time_t end_time;

    state = 0;
    tbx_ns_timeout_set(&dt, 1, 0);
    end_time = gop_get_end_time(gop, &state);

    nbytes = 0;
    bpos = pos;
    nleft = size;
    while ((nbytes != -1) && (nleft > 0) && (apr_time_now() < end_time)) {
        nbytes = tbx_ns_write(ns, buffer, bpos, nleft, dt);
        if (nbytes != -1) {
            bpos += nbytes;
            nleft -= nbytes;
        }

        end_time = gop_get_end_time(gop, &state);
    }

    if (nleft == 0) {
        status = ibp_success_status;
    } else if (apr_time_now() > end_time) {
        log_printf(5, "gid=%d timeout! now=" TT " end=" TT " state=%d\n", gop_id(gop), apr_time_now(), end_time, state);
        status = ibp_timeout_status;
        status.error_code = size - nleft;
    } else {
        log_printf(5, "gid=%d timeout! RETRY\n", gop_id(gop));
        status = ibp_retry_status;  //** Dead connection so retry
        status.error_code = size - nleft;
    }

    return(status);
}

op_status_t write_send(op_generic_t *gop, tbx_ns_t *ns)
{
    ibp_op_t *iop = ibp_get_iop(gop);
    int i;
    op_status_t err;
    ibp_op_rw_t *cmd = &(iop->ops.rw_op);
    ibp_rw_buf_t *rwbuf;

    err = ibp_failure_status;

    //** Turn on chksumming if needed
    if (tbx_ns_chksum_is_valid(&(iop->ncs)) == 1) {
        log_printf(15, "write_send: ns=%d tbx_chksum_type=%d\n", tbx_ns_getid(ns), tbx_ns_chksum_type(&(iop->ncs)));
        tbx_ns_chksum_reset(&(iop->ncs));
        tbx_ns_chksum_write_set(ns, iop->ncs);
        tbx_ns_chksum_write_enable(ns);
    }

    log_printf(10, "write_send: gid=%d n_ops=%d\n", gop_id(gop), cmd->n_ops);

    for (i=0; i<cmd->n_ops; i++) {
        rwbuf = cmd->rwbuf[i];
        log_printf(5, "gid=%d ns=%d i=%d size=" I64T "\n", gop_id(gop), tbx_ns_getid(ns), i, rwbuf->size);
        err = gop_write_block(ns, gop, rwbuf->buffer, rwbuf->boff, rwbuf->size);
        log_printf(5, "gid=%d ns=%d i=%d status=%d\n", gop_id(gop), tbx_ns_getid(ns), i, err.op_status);
        if (err.op_status != OP_STATE_SUCCESS) {
            log_printf(1, "ERROR: cap=%s gid=%d ns=%d i=%d boff=" LU " size=" I64T " sent=%d\n", cmd->cap, gop_id(gop), tbx_ns_getid(ns), i, rwbuf->boff, rwbuf->size, err.error_code);
            break;
        }
    }

    log_printf(15, "write_send: END ns=%d status=%d\n", tbx_ns_getid(ns), err.op_status);

    if ((tbx_ns_chksum_is_valid(&(iop->ncs)) == 1) && (err.op_status == OP_STATE_SUCCESS)) {
        if (tbx_ns_chksum_write_flush(ns) != 0) {
            err.op_status = OP_STATE_FAILURE;
            err.error_code = IBP_E_CHKSUM;
        }
        tbx_ns_chksum_write_disable(ns);
    }

    return(err);
}

op_status_t write_recv(op_generic_t *gop, tbx_ns_t *ns)
{
    ibp_op_t *op = ibp_get_iop(gop);
    char buffer[1024];
    op_status_t err;
    int status, fin;
    ibp_off_t nbytes;
    ibp_op_rw_t *cmd;
    char *bstate;

    log_printf(15, "write_recv: Start!!! ns=%d\n", tbx_ns_getid(ns));

    cmd = &(op->ops.rw_op);

    tbx_ns_chksum_read_disable(ns);

    err = gop_readline_with_timeout(ns, buffer, sizeof(buffer), gop);
    if (err.op_status != OP_STATE_SUCCESS) return(err);

    status = atoi(tbx_stk_string_token(buffer, " ", &bstate, &fin));
    if (status != IBP_OK) {
        log_printf(15, "write_recv: ns=%d id=%d cap=%s n_ops=%d  Error!  status=%s\n",
                   tbx_ns_getid(ns), gop_get_id(gop), cmd->cap, cmd->n_ops, buffer);
        process_error(gop, &err, status, -1, &bstate);
    } else {
        err = gop_readline_with_timeout(ns, buffer, sizeof(buffer), gop);
        if (err.op_status == OP_STATE_SUCCESS) {
            log_printf(15, "write_recv: ns=%d cap=%s gid=%d n_ops=%d status/nbytes=%s\n",
                       tbx_ns_getid(ns), cmd->cap, gop_id(gop), cmd->n_ops, buffer);
            nbytes = -1;
            status = atoi(tbx_stk_string_token(buffer, " ", &bstate, &fin));
            sscanf(tbx_stk_string_token(NULL, " ", &bstate, &fin), I64T, &nbytes);
            if ((nbytes != cmd->size) || (status != IBP_OK)) {
                log_printf(1, "write_recv: ns=%d cap=%s gid=%d n_ops=%d Error! status/nbytes=%s\n",
                           tbx_ns_getid(ns), cmd->cap, gop_id(gop), cmd->n_ops, buffer);
                err.op_status = OP_STATE_FAILURE;
                err.error_code = status;
            } else {
                err = ibp_success_status;
            }
        } else {
            log_printf(1, "write_recv: ns=%d cap=%s gid=%d n_ops=%d Error with readline! buffer=%s\n",
                       tbx_ns_getid(ns), cmd->cap, gop_id(gop), cmd->n_ops, buffer);
            return(err);
        }
    }

    return(err);
}

op_status_t append_command(op_generic_t *gop, tbx_ns_t *ns)
{
    ibp_op_t *op = ibp_get_iop(gop);
    char buffer[1024];
    op_status_t err;
    ibp_op_rw_t *cmd;

    cmd = &(op->ops.rw_op);

    if (tbx_ns_chksum_is_valid(&(op->ncs)) == 0) {
        snprintf(buffer, sizeof(buffer), "%d %d %s %s " I64T " %d\n",
                 IBPv040, IBP_STORE, cmd->key, cmd->typekey, cmd->size, (int)apr_time_sec(gop->op->cmd.timeout));
    } else {
        snprintf(buffer, sizeof(buffer), "%d %d %d " I64T " %s %s " I64T " %d\n",
                 IBPv040, IBP_STORE_CHKSUM, tbx_ns_chksum_type(&(op->ncs)), tbx_ns_chksum_blocksize(&(op->ncs)),
                 cmd->key, cmd->typekey, cmd->size, (int)apr_time_sec(gop->op->cmd.timeout));
    }

    tbx_ns_chksum_write_set(ns, op->ncs);
    tbx_ns_chksum_write_disable(ns);

    err = send_command(gop, ns, buffer);
    if (err.op_status != OP_STATE_SUCCESS) {
        log_printf(10, "append_command: Error with send_command()! ns=%d\n", tbx_ns_getid(ns));
    }

    return(err);
}

op_status_t validate_chksum_command(op_generic_t *gop, tbx_ns_t *ns)
{
    ibp_op_t *op = ibp_get_iop(gop);
    char buffer[1024];
    op_status_t err;
    ibp_op_validate_chksum_t *cmd = &(op->ops.validate_op);

    snprintf(buffer, sizeof(buffer), "%d %d %s %s %d %d\n",
             IBPv040, IBP_VALIDATE_CHKSUM, cmd->key, cmd->typekey, cmd->correct_errors,
             (int)apr_time_sec(gop->op->cmd.timeout));

    tbx_ns_chksum_write_clear(ns);

    err = send_command(gop, ns, buffer);
    if (err.op_status != OP_STATE_SUCCESS) {
        log_printf(10, "validate_chksum_command: Error with send_command()! ns=%d\n", tbx_ns_getid(ns));
    }

    return(err);
}

op_status_t validate_chksum_recv(op_generic_t *gop, tbx_ns_t *ns)
{
    ibp_op_t *op = ibp_get_iop(gop);
    char buffer[1024];
    op_status_t err;
    int status, fin;
    ibp_off_t nerrors;
    ibp_op_validate_chksum_t *cmd = &(op->ops.validate_op);
    char *bstate;
    double swait;

    log_printf(15, "validate_chksum_recv: Start!!! ns=%d\n", tbx_ns_getid(ns));

    tbx_ns_chksum_read_disable(ns);

    err = gop_readline_with_timeout(ns, buffer, sizeof(buffer), gop);
    if (err.op_status != OP_STATE_SUCCESS) return(err);

    log_printf(15, "validate_chksum_recv: ns=%d cap=%s status/n_errors=%s\n",
               tbx_ns_getid(ns), cmd->cap, buffer);

    //** Get the status and number of bad blocks(if available)
    status = atoi(tbx_stk_string_token(buffer, " ", &bstate, &fin));
    sscanf(tbx_stk_string_token(NULL, " ", &bstate, &fin), "%lf", &swait);
    nerrors = swait;
    *cmd->n_bad_blocks = nerrors;

    return(process_error(gop, &err, status, swait, NULL));
}

op_status_t get_chksum_command(op_generic_t *gop, tbx_ns_t *ns)
{
    ibp_op_t *op = ibp_get_iop(gop);
    char buffer[1024];
    op_status_t err;
    ibp_op_get_chksum_t *cmd = &(op->ops.get_chksum_op);

    snprintf(buffer, sizeof(buffer), "%d %d %s %s %d %d\n",
             IBPv040, IBP_GET_CHKSUM, cmd->key, cmd->typekey, cmd->chksum_info_only,
             (int)apr_time_sec(gop->op->cmd.timeout));

    tbx_ns_chksum_write_clear(ns);

    err = send_command(gop, ns, buffer);
    if (err.op_status != OP_STATE_SUCCESS) {
        log_printf(10, "get_chksum_command: Error with send_command()! ns=%d\n", tbx_ns_getid(ns));
    }

    return(err);
}

op_status_t get_chksum_recv(op_generic_t *gop, tbx_ns_t *ns)
{
    ibp_op_t *op = ibp_get_iop(gop);
    char buffer[1024];
    op_status_t err;
    int status, fin;
    ibp_op_get_chksum_t *cmd = &(op->ops.get_chksum_op);
    char *bstate;
    tbx_tbuf_t buf;

    log_printf(15, "get_chksum_recv: Start!!! ns=%d\n", tbx_ns_getid(ns));

    tbx_ns_chksum_read_disable(ns);

    err = gop_readline_with_timeout(ns, buffer, sizeof(buffer), gop);
    if (err.op_status != OP_STATE_SUCCESS) return(err);

    log_printf(15, "get_chksum_recv: ns=%d cap=%s status string=%s\n",
               tbx_ns_getid(ns), cmd->cap, buffer);

    //** Get the status
    status = atoi(tbx_stk_string_token(buffer, " ", &bstate, &fin));
    if (status != IBP_OK) {
        log_printf(15, "get_chksum_recv: ns=%d cap=%s status error=%d!\n",  tbx_ns_getid(ns), cmd->cap, status);
        return(process_error(gop, &err, status, -1, &bstate));
    }

    //** Now get the chksum type
    *cmd->cs_type = CHKSUM_NONE;
    sscanf(tbx_stk_string_token(NULL, " ", &bstate, &fin), "%d", cmd->cs_type);

    //    status cs_type cs_size block_size nblocks nbytes\n
    //    ...nbytes_of_chksum...

    //** Now parse the rest of the chksum info line
    sscanf(tbx_stk_string_token(NULL, " ", &bstate, &fin), "%d", cmd->cs_size);
    sscanf(tbx_stk_string_token(NULL, " ", &bstate, &fin), I64T, cmd->blocksize);
    sscanf(tbx_stk_string_token(NULL, " ", &bstate, &fin), I64T, cmd->nblocks);
    sscanf(tbx_stk_string_token(NULL, " ", &bstate, &fin), I64T, cmd->n_chksumbytes);

    if (cmd->chksum_info_only == 1) {  //** Only wanted the chksum info so return
        return(ibp_success_status);
    }

    //** Check and make sure the buffer is large enough
    if (*cmd->n_chksumbytes > cmd->bufsize) {
        log_printf(15, "get_chksum_recv: ns=%d cap=%s buffer too small!  bufsize=" I64T " need= " I64T "\n",  tbx_ns_getid(ns), cmd->cap, cmd->bufsize, *cmd->n_chksumbytes);
        tbx_ns_close(ns);
        _op_set_status(err, OP_STATE_FAILURE, IBP_E_WOULD_EXCEED_LIMIT);
        return(err);
    }

    //** Finally read in the chksum
    tbx_tbuf_single(&buf, *cmd->n_chksumbytes, cmd->buffer);
    err = gop_read_block(ns, gop, &buf, 0, *cmd->n_chksumbytes);

    return(err);
}

op_status_t allocate_command(op_generic_t *gop, tbx_ns_t *ns)
{
    ibp_op_t *op = ibp_get_iop(gop);
    char buffer[1024];
    op_status_t err;
    ibp_op_alloc_t *cmd = &(op->ops.alloc_op);

    log_printf(10, "allocate_command: cs_type=%d\n", cmd->disk_chksum_type);

    if (cmd->disk_chksum_type == CHKSUM_DEFAULT) {  //** Normal allocation
        snprintf(buffer, sizeof(buffer), "%d %d %s %d %d %d " I64T " %d\n",
                 IBPv040, IBP_ALLOCATE, cmd->depot->rid.name, cmd->attr->reliability, cmd->attr->type,
                 cmd->duration, cmd->size, (int)apr_time_sec(gop->op->cmd.timeout));
    } else if ((tbx_chksum_type_valid(cmd->disk_chksum_type) == 1) || (cmd->disk_chksum_type == CHKSUM_NONE)) {
        snprintf(buffer, sizeof(buffer), "%d %d %d " I64T " %s %d %d %d " I64T " %d\n",
                 IBPv040, IBP_ALLOCATE_CHKSUM, cmd->disk_chksum_type, cmd->disk_blocksize, cmd->depot->rid.name, cmd->attr->reliability, cmd->attr->type,
                 cmd->duration, cmd->size, (int)apr_time_sec(gop->op->cmd.timeout));
    } else {
        log_printf(10, "allocate_command: Invalid chksum type! type=%d ns=%d\n", cmd->disk_chksum_type, tbx_ns_getid(ns));
        _op_set_status(err, OP_STATE_FAILURE, IBP_E_CHKSUM_TYPE);
        return(err);
    }

    tbx_ns_chksum_write_clear(ns);

    err = send_command(gop, ns, buffer);
    if (err.op_status != OP_STATE_SUCCESS) {
        log_printf(10, "allocate_command: Error with send_command()! ns=%d\n", tbx_ns_getid(ns));
    }

    return(err);
}

op_status_t split_allocate_command(op_generic_t *gop, tbx_ns_t *ns)
{
    ibp_op_t *op = ibp_get_iop(gop);
    char buffer[1024];
    op_status_t err;
    ibp_op_alloc_t *cmd = &(op->ops.alloc_op);

    if (cmd->disk_chksum_type == CHKSUM_DEFAULT) {  //** Normal split allocation
        snprintf(buffer, sizeof(buffer), "%d %d %s %s %d %d %d " I64T " %d\n",
                 IBPv040, IBP_SPLIT_ALLOCATE, cmd->key, cmd->typekey, cmd->attr->reliability, cmd->attr->type,
                 cmd->duration, cmd->size, (int)apr_time_sec(gop->op->cmd.timeout));
    } else if ((tbx_chksum_type_valid(cmd->disk_chksum_type) == 1) || (cmd->disk_chksum_type == CHKSUM_NONE)) {
        snprintf(buffer, sizeof(buffer), "%d %d %d " I64T " %s %s %d %d %d " I64T " %d\n",
                 IBPv040, IBP_SPLIT_ALLOCATE_CHKSUM, cmd->disk_chksum_type, cmd->disk_blocksize, cmd->key, cmd->typekey, cmd->attr->reliability, cmd->attr->type,
                 cmd->duration, cmd->size, (int)apr_time_sec(gop->op->cmd.timeout));
    } else {
        log_printf(10, "split_allocate_command: Invalid chksum type! type=%d ns=%d\n", cmd->disk_chksum_type, tbx_ns_getid(ns));
        _op_set_status(err, OP_STATE_FAILURE, IBP_E_CHKSUM_TYPE);
        return(err);
    }

    tbx_ns_chksum_write_clear(ns);

    err = send_command(gop, ns, buffer);
    if (err.op_status != OP_STATE_SUCCESS) {
        log_printf(10, "split_allocate_command: Error with send_command()! ns=%d\n", tbx_ns_getid(ns));
    }

    return(err);
}

op_status_t allocate_recv(op_generic_t *gop, tbx_ns_t *ns)
{
    ibp_op_t *op = ibp_get_iop(gop);
    int status, fin;
    char buffer[1025];
    char rcap[1025], wcap[1025], mcap[1025];
    char *bstate;
    op_status_t err;
    ibp_op_alloc_t *cmd = &(op->ops.alloc_op);

    //** Need to read the depot status info
    log_printf(15, "allocate_recv: ns=%d Start\n", tbx_ns_getid(ns));

    tbx_ns_chksum_read_clear(ns);

    err = gop_readline_with_timeout(ns, buffer, sizeof(buffer), gop);
    if (err.op_status != OP_STATE_SUCCESS) return(err);

    log_printf(15, "allocate_recv: after readline ns=%d buffer=%s\n", tbx_ns_getid(ns), buffer);

    sscanf(tbx_stk_string_token(buffer, " ", &bstate, &fin), "%d", &status);
    if (status != IBP_OK) {
        log_printf(1, "alloc_recv: ns=%d Error! status=%d bstate=%s\n", tbx_ns_getid(ns), status, bstate);
        return(process_error(gop, &err, status, -1, &bstate));
    }

    rcap[0] = '\0';
    wcap[0] = '\0';
    mcap[0] = '\0';
    strncpy(rcap, tbx_stk_string_token(NULL, " ", &bstate, &fin), sizeof(rcap)-1);
    rcap[sizeof(rcap)-1] = '\0';
    strncpy(wcap, tbx_stk_string_token(NULL, " ", &bstate, &fin), sizeof(rcap)-1);
    wcap[sizeof(wcap)-1] = '\0';
    strncpy(mcap, tbx_stk_string_token(NULL, " ", &bstate, &fin), sizeof(rcap)-1);
    mcap[sizeof(mcap)-1] = '\0';

    if ((strlen(rcap) == 0) || (strlen(wcap) == 0) || (strlen(mcap) == 0)) {
        log_printf(0, "alloc_recv: ns=%d Error reading caps!  buffer=%s\n", tbx_ns_getid(ns), buffer);
        if (sscanf(buffer, "%d", &status) != 1) {
            log_printf(1, "alloc_recv: ns=%d Can't read status!\n", tbx_ns_getid(ns));
            _op_set_status(err, OP_STATE_FAILURE, IBP_E_GENERIC);
            return(err);
        } else {
            _op_set_status(err, OP_STATE_FAILURE, status);
            return(err);
        }
    }

    cmd->caps->readCap = strdup(rcap);
    cmd->caps->writeCap = strdup(wcap);
    cmd->caps->manageCap = strdup(mcap);

    log_printf(15, "alloc_recv: ns=%d rcap=%s wcap=%s mcap=%s\n", tbx_ns_getid(ns),
               cmd->caps->readCap, cmd->caps->writeCap, cmd->caps->manageCap);

    return(ibp_success_status);
}

op_status_t rename_command(op_generic_t *gop, tbx_ns_t *ns)
{
    ibp_op_t *op = ibp_get_iop(gop);
    char buffer[1024];
    op_status_t err;
    ibp_op_alloc_t *cmd = &(op->ops.alloc_op);

    snprintf(buffer, sizeof(buffer), "%d %d %s %s %d\n",
             IBPv040, IBP_RENAME, cmd->key, cmd->typekey, (int)apr_time_sec(gop->op->cmd.timeout));

    tbx_ns_chksum_write_clear(ns);

    err = send_command(gop, ns, buffer);
    if (err.op_status != OP_STATE_SUCCESS) {
        log_printf(10, "rename_command: Error with send_command()! ns=%d\n", tbx_ns_getid(ns));
    }

    return(err);
}

op_status_t merge_command(op_generic_t *gop, tbx_ns_t *ns)
{
    ibp_op_t *op = ibp_get_iop(gop);
    char buffer[1024];
    op_status_t err;
    ibp_op_merge_alloc_t *cmd = &(op->ops.merge_op);

    snprintf(buffer, sizeof(buffer), "%d %d %s %s %s %s %d\n",
             IBPv040, IBP_MERGE_ALLOCATE, cmd->mkey, cmd->mtypekey, cmd->ckey, cmd->ctypekey, (int)apr_time_sec(gop->op->cmd.timeout));

    tbx_ns_chksum_write_clear(ns);

    err = send_command(gop, ns, buffer);
    if (err.op_status != OP_STATE_SUCCESS) {
        log_printf(10, "merge_command: Error with send_command()! ns=%d\n", tbx_ns_getid(ns));
    }

    return(err);
}

op_status_t alias_allocate_command(op_generic_t *gop, tbx_ns_t *ns)
{
    ibp_op_t *op = ibp_get_iop(gop);
    char buffer[1024];
    op_status_t err;
    ibp_op_alloc_t *cmd = &(op->ops.alloc_op);

    snprintf(buffer, sizeof(buffer), "%d %d %s %s " I64T " " I64T " %d %d\n",
             IBPv040, IBP_ALIAS_ALLOCATE, cmd->key, cmd->typekey, cmd->offset, cmd->size, cmd->duration, (int)apr_time_sec(gop->op->cmd.timeout));

    tbx_ns_chksum_write_clear(ns);

    err = send_command(gop, ns, buffer);
    if (err.op_status != OP_STATE_SUCCESS) {
        log_printf(10, "alias_allocate_command: Error with send_command()! ns=%d\n", tbx_ns_getid(ns));
    }

    return(err);
}

op_status_t modify_count_command(op_generic_t *gop, tbx_ns_t *ns)
{
    ibp_op_t *op = ibp_get_iop(gop);
    char buffer[1024];
    op_status_t err;
    ibp_op_probe_t *cmd;

    cmd = &(op->ops.probe_op);

    snprintf(buffer, sizeof(buffer), "%d %d %s %s %d %d %d\n",
             IBPv040, cmd->cmd, cmd->key, cmd->typekey, cmd->mode, cmd->captype, (int)apr_time_sec(gop->op->cmd.timeout));

    err = send_command(gop, ns, buffer);
    if (err.op_status != OP_STATE_SUCCESS) {
        log_printf(10, "modify_count_command: Error with send_command()! ns=%d\n", tbx_ns_getid(ns));
    }

    return(err);
}

op_status_t alias_modify_count_command(op_generic_t *gop, tbx_ns_t *ns)
{
    ibp_op_t *op = ibp_get_iop(gop);
    char buffer[1024];
    op_status_t err;
    ibp_op_probe_t *cmd;

    cmd = &(op->ops.probe_op);

    snprintf(buffer, sizeof(buffer), "%d %d %s %s %d %d %s %s %d\n",
             IBPv040, cmd->cmd, cmd->key, cmd->typekey, cmd->mode, cmd->captype, cmd->mkey, cmd->mtypekey, (int)apr_time_sec(gop->op->cmd.timeout));

    tbx_ns_chksum_write_clear(ns);

    err = send_command(gop, ns, buffer);
    if (err.op_status != OP_STATE_SUCCESS) {
        log_printf(10, "modify_count_command: Error with send_command()! ns=%d\n", tbx_ns_getid(ns));
    }

    return(err);
}

op_status_t status_get_recv(op_generic_t *gop, tbx_ns_t *ns)
{
    int status, fin;
    char buffer[1025];
    char *bstate;
    op_status_t err;

    //** Need to read the depot status info
    log_printf(15, "status_get_recv: ns=%d Start", tbx_ns_getid(ns));

    tbx_ns_chksum_read_clear(ns);

    err = gop_readline_with_timeout(ns, buffer, sizeof(buffer), gop);
    if (err.op_status != OP_STATE_SUCCESS) return(err);

    log_printf(15, "status_get_recv: after readline ns=%d buffer=%s\n", tbx_ns_getid(ns), buffer);

    status = atoi(tbx_stk_string_token(buffer, " ", &bstate, &fin));

    return(process_error(gop, &err, status, -1, &bstate));
}

op_status_t modify_alloc_command(op_generic_t *gop, tbx_ns_t *ns)
{
    ibp_op_t *op = ibp_get_iop(gop);
    char buffer[1024];
    op_status_t err;
    int atime;
    ibp_op_modify_alloc_t *cmd;

    cmd = &(op->ops.mod_alloc_op);

    atime = cmd->duration - time(NULL); //** This is in sec NOT APR time
    if (atime < 0) atime = cmd->duration;

    snprintf(buffer, sizeof(buffer), "%d %d %s %s %d %d " I64T " %d %d %d\n",
             IBPv040, IBP_MANAGE, cmd->key, cmd->typekey, IBP_CHNG, IBP_MANAGECAP, cmd->size, atime,
             cmd->reliability, (int)apr_time_sec(gop->op->cmd.timeout));

    tbx_ns_chksum_write_clear(ns);

    err = send_command(gop, ns, buffer);
    if (err.op_status != OP_STATE_SUCCESS) {
        log_printf(10, "modify_count_command: Error with send_command()! ns=%d\n", tbx_ns_getid(ns));
    }

    return(err);
}

op_status_t alias_modify_alloc_command(op_generic_t *gop, tbx_ns_t *ns)
{
    ibp_op_t *op = ibp_get_iop(gop);
    char buffer[1024];
    op_status_t err;
    int atime;
    ibp_op_modify_alloc_t *cmd;

    cmd = &(op->ops.mod_alloc_op);

    atime = cmd->duration - time(NULL); //** This is in sec NOT APR time
    if (atime < 0) atime = cmd->duration;

    snprintf(buffer, sizeof(buffer), "%d %d %s %s %d " I64T " " I64T " %d %s %s %d\n",
             IBPv040, IBP_ALIAS_MANAGE, cmd->key, cmd->typekey, IBP_CHNG, cmd->offset,  cmd->size, atime,
             cmd->mkey, cmd->mtypekey, (int)apr_time_sec(gop->op->cmd.timeout));

    tbx_ns_chksum_write_clear(ns);

    err = send_command(gop, ns, buffer);
    if (err.op_status != OP_STATE_SUCCESS) {
        log_printf(10, "alias_modify_count_command: Error with send_command()! ns=%d\n", tbx_ns_getid(ns));
    }

    return(err);
}

op_status_t truncate_command(op_generic_t *gop, tbx_ns_t *ns)
{
    ibp_op_t *op = ibp_get_iop(gop);
    char buffer[1024];
    op_status_t err;
    ibp_op_modify_alloc_t *cmd;

    cmd = &(op->ops.mod_alloc_op);

    snprintf(buffer, sizeof(buffer), "%d %d %s %s %d " I64T " %d\n",
             IBPv040, IBP_MANAGE, cmd->key, cmd->typekey, IBP_TRUNCATE, cmd->size, (int)apr_time_sec(gop->op->cmd.timeout));

    tbx_ns_chksum_write_clear(ns);

    err = send_command(gop, ns, buffer);
    if (err.op_status != OP_STATE_SUCCESS) {
        log_printf(10, "truncate_command: Error with send_command()! ns=%d\n", tbx_ns_getid(ns));
    }

    return(err);
}

op_status_t probe_command(op_generic_t *gop, tbx_ns_t *ns)
{
    ibp_op_t *op = ibp_get_iop(gop);
    char buffer[1024];
    op_status_t err;
    ibp_op_probe_t *cmd;

    cmd = &(op->ops.probe_op);

    snprintf(buffer, sizeof(buffer), "%d %d %s %s %d %d 0 0 0 %d \n",
             IBPv040, IBP_MANAGE, cmd->key, cmd->typekey, IBP_PROBE, IBP_MANAGECAP, (int)apr_time_sec(gop->op->cmd.timeout));

    tbx_ns_chksum_write_clear(ns);

    err = send_command(gop, ns, buffer);
    if (err.op_status != OP_STATE_SUCCESS) {
        log_printf(10, "probe_command: Error with send_command()! ns=%d\n", tbx_ns_getid(ns));
    }

    return(err);
}

op_status_t probe_recv(op_generic_t *gop, tbx_ns_t *ns)
{
    ibp_op_t *op = ibp_get_iop(gop);
    int status, fin;
    char buffer[1025];
    op_status_t err;
    char *bstate;
    ibp_capstatus_t *p;

    //** Need to read the depot status info
    log_printf(15, "probe_recv: ns=%d Start", tbx_ns_getid(ns));

    tbx_ns_chksum_read_clear(ns);

    err = gop_readline_with_timeout(ns, buffer, sizeof(buffer), gop);
    if (err.op_status != OP_STATE_SUCCESS) return(err);

    log_printf(15, "probe_recv: after readline ns=%d buffer=%s\n", tbx_ns_getid(ns), buffer);

    status = atoi(tbx_stk_string_token(buffer, " ", &bstate, &fin));
    if ((status == IBP_OK) && (fin == 0)) {
        p = op->ops.probe_op.probe;
        log_printf(15, "probe_recv: p=%p QWERT\n", p);

        p->readRefCount = atoi(tbx_stk_string_token(NULL, " ", &bstate, &fin));
        p->writeRefCount = atoi(tbx_stk_string_token(NULL, " ", &bstate, &fin));
        sscanf(tbx_stk_string_token(NULL, " ", &bstate, &fin), I64T, &(p->currentSize));
        sscanf(tbx_stk_string_token(NULL, " ", &bstate, &fin), I64T, &(p->maxSize));
        p->attrib.duration = atol(tbx_stk_string_token(NULL, " ", &bstate, &fin)) + time(NULL); //** This is in sec NOT APR time
        p->attrib.reliability = atoi(tbx_stk_string_token(NULL, " ", &bstate, &fin));
        p->attrib.type = atoi(tbx_stk_string_token(NULL, " ", &bstate, &fin));
    } else {
        process_error(gop, &err, status, -1, &bstate);
    }

    return(err);
}

op_status_t alias_probe_command(op_generic_t *gop, tbx_ns_t *ns)
{
    ibp_op_t *op = ibp_get_iop(gop);
    char buffer[1024];
    op_status_t err;
    ibp_op_probe_t *cmd;

    cmd = &(op->ops.probe_op);

    snprintf(buffer, sizeof(buffer), "%d %d %s %s %d %d %d \n",
             IBPv040, IBP_ALIAS_MANAGE, cmd->key, cmd->typekey, IBP_PROBE, IBP_MANAGECAP, (int)apr_time_sec(gop->op->cmd.timeout));

    tbx_ns_chksum_write_clear(ns);

    err = send_command(gop, ns, buffer);
    if (err.op_status != OP_STATE_SUCCESS) {
        log_printf(10, "alias_probe_command: Error with send_command()! ns=%d\n", tbx_ns_getid(ns));
    }

    return(err);
}

op_status_t alias_probe_recv(op_generic_t *gop, tbx_ns_t *ns)
{
    ibp_op_t *op = ibp_get_iop(gop);
    int status, fin;
    char buffer[1025];
    op_status_t err;
    char *bstate;
    ibp_alias_capstatus_t *p;

    //** Need to read the depot status info
    log_printf(15, "alias_probe_recv: ns=%d Start", tbx_ns_getid(ns));

    tbx_ns_chksum_read_clear(ns);

    err = gop_readline_with_timeout(ns, buffer, sizeof(buffer), gop);
    if (err.op_status != OP_STATE_SUCCESS) return(err);

    log_printf(15, "alias_probe_recv: after readline ns=%d buffer=%s\n", tbx_ns_getid(ns), buffer);

    status = atoi(tbx_stk_string_token(buffer, " ", &bstate, &fin));
    if ((status == IBP_OK) && (fin == 0)) {
        p = op->ops.probe_op.alias_probe;
        p->read_refcount = atoi(tbx_stk_string_token(NULL, " ", &bstate, &fin));
        p->write_refcount = atoi(tbx_stk_string_token(NULL, " ", &bstate, &fin));
        p->offset = atol(tbx_stk_string_token(NULL, " ", &bstate, &fin));
        p->size = atol(tbx_stk_string_token(NULL, " ", &bstate, &fin));
        p->duration = atol(tbx_stk_string_token(NULL, " ", &bstate, &fin)) + time(NULL); //** This is in sec NOT APR time
    } else {
        process_error(gop, &err, status, -1, &bstate);
    }

    return(err);
}

op_status_t copyappend_command(op_generic_t *gop, tbx_ns_t *ns)
{
    ibp_op_t *op = ibp_get_iop(gop);
    char buffer[1024];
    op_status_t err;
    ibp_op_copy_t *cmd;

    cmd = &(op->ops.copy_op);

    if (tbx_ns_chksum_is_valid(&(op->ncs)) == 0) {
        snprintf(buffer, sizeof(buffer), "%d %d %s %s %s %s " I64T " " I64T " %d %d %d\n",
                 IBPv040, cmd->ibp_command, cmd->path, cmd->src_key, cmd->destcap, cmd->src_typekey, cmd->src_offset, cmd->len,
                 (int)apr_time_sec(gop->op->cmd.timeout), cmd->dest_timeout, cmd->dest_client_timeout);
    } else {
        snprintf(buffer, sizeof(buffer), "%d %d %d " I64T " %s %s %s %s " I64T " " I64T " %d %d %d\n",
                 IBPv040, cmd->ibp_command, tbx_ns_chksum_type(&(op->ncs)), tbx_ns_chksum_blocksize(&(op->ncs)),
                 cmd->path, cmd->src_key, cmd->destcap, cmd->src_typekey, cmd->src_offset, cmd->len,
                 (int)apr_time_sec(gop->op->cmd.timeout), cmd->dest_timeout, cmd->dest_client_timeout);
    }

    tbx_ns_chksum_write_clear(ns);

    err = send_command(gop, ns, buffer);
    if (err.op_status != OP_STATE_SUCCESS) {
        log_printf(10, "copyappend_command: Error with send_command()! ns=%d\n", tbx_ns_getid(ns));
    }

    return(err);
}

op_status_t copy_recv(op_generic_t *gop, tbx_ns_t *ns)
{
    ibp_op_t *op = ibp_get_iop(gop);
    int status, fin;
    char buffer[1025];
    op_status_t err;
    ibp_off_t nbytes;
    char *bstate;
    ibp_op_copy_t *cmd;
    double swait;

    cmd = &(op->ops.copy_op);

    //** Need to read the depot status info
    log_printf(15, "copy_recv: ns=%d Start\n", tbx_ns_getid(ns));

    tbx_ns_chksum_read_clear(ns);

    err = gop_readline_with_timeout(ns, buffer, sizeof(buffer), gop);
    if (err.op_status != OP_STATE_SUCCESS) return(err);

    log_printf(15, "copy_recv: after readline ns=%d buffer=%s\n", tbx_ns_getid(ns), buffer);

    status = atoi(tbx_stk_string_token(buffer, " ", &bstate, &fin));
    swait = atof(tbx_stk_string_token(NULL, " ", &bstate, &fin));
    nbytes = swait;
    if ((status != IBP_OK) || (nbytes != cmd->len)) {
        log_printf(0, "copy_recv: (read) ns=%d srccap=%s destcap=%s offset=" I64T " len=" I64T " err=%d Error!  status/nbytes=!%s!\n",
                   tbx_ns_getid(ns), cmd->srccap, cmd->destcap, cmd->src_offset, cmd->len, err.op_status, buffer);
        process_error(gop, &err, status, swait, NULL);
    }

    return(err);
}

op_status_t pushpull_command(op_generic_t *gop, tbx_ns_t *ns)
{
    ibp_op_t *op = ibp_get_iop(gop);
    char buffer[1024];
    op_status_t err;
    ibp_op_copy_t *cmd;

    cmd = &(op->ops.copy_op);

    if (tbx_ns_chksum_is_valid(&(op->ncs)) == 0) {
        snprintf(buffer, sizeof(buffer), "%d %d %d %s %s %s %s " I64T " " I64T " " I64T " %d %d %d\n",
                 IBPv040, cmd->ibp_command, cmd->ctype, cmd->path, cmd->src_key, cmd->destcap, cmd->src_typekey,
                 cmd->src_offset, cmd->dest_offset, cmd->len,
                 (int)apr_time_sec(gop->op->cmd.timeout), cmd->dest_timeout, cmd->dest_client_timeout);
    } else {
        snprintf(buffer, sizeof(buffer), "%d %d %d " I64T " %d %s %s %s %s " I64T " " I64T " " I64T " %d %d %d\n",
                 IBPv040, cmd->ibp_command, tbx_ns_chksum_type(&(op->ncs)), tbx_ns_chksum_blocksize(&(op->ncs)),
                 cmd->ctype, cmd->path, cmd->src_key, cmd->destcap, cmd->src_typekey,
                 cmd->src_offset, cmd->dest_offset, cmd->len,
                 (int)apr_time_sec(gop->op->cmd.timeout), cmd->dest_timeout, cmd->dest_client_timeout);
    }

    tbx_ns_chksum_write_clear(ns);

    err = send_command(gop, ns, buffer);
    if (err.op_status != OP_STATE_SUCCESS) {
        log_printf(10, "copyappend_command: Error with send_command()! ns=%d\n", tbx_ns_getid(ns));
    }

    return(err);
}

op_status_t depot_modify_command(op_generic_t *gop, tbx_ns_t *ns)
{
    ibp_op_t *op = ibp_get_iop(gop);
    char buffer[1024];
    op_status_t err;
    ibp_op_depot_modify_t *cmd;

    cmd = &(op->ops.depot_modify_op);

    snprintf(buffer, sizeof(buffer), "%d %d %s %d %s %d\n " I64T " " I64T " " TT "\n",
             IBPv040, IBP_STATUS, cmd->depot->rid.name, IBP_ST_CHANGE, cmd->password, (int)apr_time_sec(gop->op->cmd.timeout),
             cmd->max_hard, cmd->max_soft, cmd->max_duration);

    tbx_ns_chksum_write_clear(ns);

    err = send_command(gop, ns, buffer);
    if (err.op_status != OP_STATE_SUCCESS) {
        log_printf(10, "modify_depot_command: Error with send_command()! ns=%d\n", tbx_ns_getid(ns));
    }

    return(err);
}

op_status_t depot_inq_command(op_generic_t *gop, tbx_ns_t *ns)
{
    ibp_op_t *op = ibp_get_iop(gop);
    char buffer[1024];
    op_status_t err;
    ibp_op_depot_inq_t *cmd;

    cmd = &(op->ops.depot_inq_op);

    tbx_ns_chksum_write_clear(ns);

    snprintf(buffer, sizeof(buffer), "%d %d %s %d %s %d\n",
             IBPv040, IBP_STATUS, cmd->depot->rid.name, IBP_ST_INQ, cmd->password, (int)apr_time_sec(gop->op->cmd.timeout));

    err = send_command(gop, ns, buffer);
    if (err.op_status != OP_STATE_SUCCESS) {
        log_printf(10, "depot_inq_command: Error with send_command()! ns=%d\n", tbx_ns_getid(ns));
    }

    return(err);
}

op_status_t depot_inq_recv(op_generic_t *gop, tbx_ns_t *ns)
{
    ibp_op_t *op = ibp_get_iop(gop);
    char buffer[1024];
    op_status_t err;
    int nbytes, status, fin;
    char *bstate;
    ibp_op_depot_inq_t *cmd;

    cmd = &(op->ops.depot_inq_op);

    tbx_ns_chksum_read_clear(ns);

    err = gop_readline_with_timeout(ns, buffer, sizeof(buffer), gop);
    if (err.op_status != OP_STATE_SUCCESS) return(err);

    log_printf(15, "depot_inq_recv: after readline ns=%d buffer=%s err=%d\n", tbx_ns_getid(ns), buffer, err.op_status);

    status = atoi(tbx_stk_string_token(buffer, " ", &bstate, &fin));
    if ((status == IBP_OK) && (fin == 0)) {
        nbytes = atoi(tbx_stk_string_token(NULL, " ", &bstate, &fin));

        if (nbytes <= 0) {
            return(ibp_error_status);
        }
        if (sizeof(buffer) < nbytes) {
            return(ibp_error_status);
        }

        //** Read the next line.  I ignore the size....
        err = gop_readline_with_timeout(ns, buffer, sizeof(buffer), gop);
        if (err.op_status != OP_STATE_SUCCESS) return(err);

        status = process_inq(buffer, cmd->di);
        if (status == IBP_OK) {
            err = ibp_success_status;
        } else {
            _op_set_status(err, OP_STATE_FAILURE, status);
        }
    } else {
        process_error(gop, &err, status, -1, NULL);
    }

    return(err);
}

op_status_t depot_version_command(op_generic_t *gop, tbx_ns_t *ns)
{
    char buffer[1024];
    op_status_t err;

    snprintf(buffer, sizeof(buffer), "%d %d %d %d\n",IBPv040, IBP_STATUS, IBP_ST_VERSION, (int)apr_time_sec(gop->op->cmd.timeout));

    tbx_ns_chksum_write_clear(ns);

    err = send_command(gop, ns, buffer);
    if (err.op_status != OP_STATE_SUCCESS) {
        log_printf(10, "depot_version_command: Error with send_command()! ns=%d\n", tbx_ns_getid(ns));
    }

    return(err);
}

op_status_t depot_version_recv(op_generic_t *gop, tbx_ns_t *ns)
{
    ibp_op_t *op = ibp_get_iop(gop);
    char buffer[1024], *bstate;
    op_status_t err;
    int pos, nmax, status, fin;
    ibp_op_version_t *cmd;

    cmd = &(op->ops.ver_op);

    err = ibp_success_status;
    pos = 0;
    cmd->buffer[0] = '\0';

    tbx_ns_chksum_read_clear(ns);

    status = IBP_E_GENERIC;

    err = gop_readline_with_timeout(ns, buffer, sizeof(buffer), gop);
    if (err.op_status == OP_STATE_SUCCESS) status = atoi(tbx_stk_string_token(buffer, " ", &bstate, &fin));

    if (status == IBP_OK) {
        err = gop_readline_with_timeout(ns, buffer, sizeof(buffer), gop);

        while (err.op_status == OP_STATE_SUCCESS)  {
            if (strcmp("END", buffer) == 0) {  //** Got the end so exit
                return(err);
            }

            //** Copy what we can **
            nmax = cmd->buffer_size - pos - 2;
            strncat(cmd->buffer, buffer, nmax);
            strcat(cmd->buffer, "\n");
            if (strlen(buffer) + pos > cmd->buffer_size) {  //** Exit if we are out of space
                _op_set_status(err, OP_STATE_FAILURE, IBP_E_WOULD_EXCEED_LIMIT);
                return(err);
            }

            err = gop_readline_with_timeout(ns, buffer, sizeof(buffer), gop);
        }
    } else {
        process_error(gop, &err, status, -1, &bstate);
    }

    return(err);
}

op_status_t query_res_command(op_generic_t *gop, tbx_ns_t *ns)
{
    char buffer[1024];
    op_status_t err;

    snprintf(buffer, sizeof(buffer), "%d %d %d %d\n",IBPv040, IBP_STATUS, IBP_ST_RES, (int)apr_time_sec(gop->op->cmd.timeout));

    tbx_ns_chksum_write_clear(ns);

    err = send_command(gop, ns, buffer);
    if (err.op_status != OP_STATE_SUCCESS) {
        log_printf(10, "query_res_command: Error with send_command()! ns=%d\n", tbx_ns_getid(ns));
    }

    return(err);
}

op_status_t query_res_recv(op_generic_t *gop, tbx_ns_t *ns)
{
    ibp_op_t *op = ibp_get_iop(gop);
    char buffer[1024];
    op_status_t err;
    int fin, n, i, status;
    char *p, *bstate;
    ibp_op_rid_inq_t *cmd = &(op->ops.rid_op);

    err = ibp_success_status;

    tbx_ns_chksum_read_clear(ns);

    err = gop_readline_with_timeout(ns, buffer, sizeof(buffer), gop);
    if (err.op_status != OP_STATE_SUCCESS) return(err);

    //** check to make sure the depot supports the command
    status = atoi(tbx_stk_string_token(buffer, " ", &bstate, &fin));
    if (err.op_status != OP_STATE_SUCCESS) return(process_error(gop, &err, status, -1, &bstate));

    //** Ok now we just need to process the line **
    tbx_stack_t *list = tbx_stack_new();
    p = tbx_stk_string_token(NULL, " ", &bstate, &fin);
    while (fin == 0) {
        tbx_stack_push(list, p);
        p = tbx_stk_string_token(NULL, " ", &bstate, &fin);
    }

    n = tbx_stack_count(list);
    ridlist_init(cmd->rlist, n);
    tbx_stack_move_to_bottom(list);
    for (i=0; i<n; i++) {
        p = tbx_stack_get_current_data(list);
        cmd->rlist->rl[i] = ibp_str2rid(p);
        tbx_stack_move_up(list);
    }

    tbx_stack_free(list, 0);

    return(err);
}
