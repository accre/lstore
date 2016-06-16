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

#include <apr_time.h>
#include <gop/types.h>
#include <stdio.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/dns_cache.h>
#include <tbx/log.h>
#include <tbx/network.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>
#include <time.h>
#include <gop/gop.h>
#include <gop/host_portal.h>
#include <tbx/atomic_counter.h>
#include <tbx/chksum.h>
#include <tbx/transfer_buffer.h>

#include "ibp_misc.h"
#include "ibp_op.h"
#include "ibp_protocol.h"
#include "ibp_types.h"

#define ibp_set_status(v, opstat, errcode) (v).op_status = status; (v).error_code = errorcode

tbx_ns_timeout_t global_dt = 1*1000000;
apr_time_t gop_get_end_time(op_generic_t *gop, int *state);
void _ibp_op_free(op_generic_t *op, int mode);

op_status_t ibp_success_status = {OP_STATE_SUCCESS, IBP_OK};
op_status_t ibp_failure_status = {OP_STATE_FAILURE, 0};
op_status_t ibp_retry_status = {OP_STATE_RETRY, ERR_RETRY_DEADSOCKET};
op_status_t ibp_dead_status = {OP_STATE_DEAD, ERR_RETRY_DEADSOCKET};
op_status_t ibp_timeout_status = {OP_STATE_TIMEOUT, IBP_E_CLIENT_TIMEOUT};
op_status_t ibp_invalid_host_status = {OP_STATE_INVALID_HOST, IBP_E_INVALID_HOST};
op_status_t ibp_cant_connect_status = {OP_STATE_CANT_CONNECT, IBP_E_CANT_CONNECT};
op_status_t ibp_error_status = {OP_STATE_ERROR, 0};


// GOP handlers
op_status_t gop_read_block(tbx_ns_t *ns, op_generic_t *gop, tbx_tbuf_t *buffer, ibp_off_t pos, ibp_off_t size);
op_status_t gop_readline_with_timeout(tbx_ns_t *ns, char *buffer, int size, op_generic_t *gop);
op_status_t gop_write_block(tbx_ns_t *ns, op_generic_t *gop, tbx_tbuf_t *buffer, ibp_off_t pos, ibp_off_t size);

// Operation Commands
op_status_t alias_allocate_command(op_generic_t *gop, tbx_ns_t *ns);
op_status_t alias_modify_alloc_command(op_generic_t *gop, tbx_ns_t *ns);
op_status_t alias_modify_count_command(op_generic_t *gop, tbx_ns_t *ns);
op_status_t alias_probe_command(op_generic_t *gop, tbx_ns_t *ns);
op_status_t alias_probe_recv(op_generic_t *gop, tbx_ns_t *ns);
op_status_t allocate_command(op_generic_t *gop, tbx_ns_t *ns);
op_status_t allocate_recv(op_generic_t *gop, tbx_ns_t *ns);
op_status_t append_command(op_generic_t *gop, tbx_ns_t *ns);
op_status_t copy_recv(op_generic_t *gop, tbx_ns_t *ns);
op_status_t copyappend_command(op_generic_t *gop, tbx_ns_t *ns);
op_status_t depot_inq_command(op_generic_t *gop, tbx_ns_t *ns);
op_status_t depot_inq_recv(op_generic_t *gop, tbx_ns_t *ns);
op_status_t depot_modify_command(op_generic_t *gop, tbx_ns_t *ns);
op_status_t depot_version_command(op_generic_t *gop, tbx_ns_t *ns);
op_status_t depot_version_recv(op_generic_t *gop, tbx_ns_t *ns);
op_status_t get_chksum_command(op_generic_t *gop, tbx_ns_t *ns);
op_status_t get_chksum_recv(op_generic_t *gop, tbx_ns_t *ns);
op_status_t merge_command(op_generic_t *gop, tbx_ns_t *ns);
op_status_t modify_alloc_command(op_generic_t *gop, tbx_ns_t *ns);
op_status_t modify_count_command(op_generic_t *gop, tbx_ns_t *ns);
op_status_t probe_command(op_generic_t *gop, tbx_ns_t *ns);
op_status_t probe_recv(op_generic_t *gop, tbx_ns_t *ns);
op_status_t pushpull_command(op_generic_t *gop, tbx_ns_t *ns);
op_status_t query_res_command(op_generic_t *gop, tbx_ns_t *ns);
op_status_t query_res_recv(op_generic_t *gop, tbx_ns_t *ns);
op_status_t read_command(op_generic_t *gop, tbx_ns_t *ns);
op_status_t read_recv(op_generic_t *gop, tbx_ns_t *ns);
op_status_t rename_command(op_generic_t *gop, tbx_ns_t *ns);
op_status_t split_allocate_command(op_generic_t *gop, tbx_ns_t *ns);
op_status_t status_get_recv(op_generic_t *gop, tbx_ns_t *ns);
op_status_t truncate_command(op_generic_t *gop, tbx_ns_t *ns);
op_status_t validate_chksum_command(op_generic_t *gop, tbx_ns_t *ns);
op_status_t validate_chksum_recv(op_generic_t *gop, tbx_ns_t *ns);
op_status_t vec_read_command(op_generic_t *gop, tbx_ns_t *ns);
op_status_t vec_write_command(op_generic_t *gop, tbx_ns_t *ns);
op_status_t write_command(op_generic_t *gop, tbx_ns_t *ns);
op_status_t write_recv(op_generic_t *gop, tbx_ns_t *ns);
op_status_t write_send(op_generic_t *gop, tbx_ns_t *ns);

//
// Misc tools
//
int process_inq(char *buffer, ibp_depotinfo_t *di)
{
    char *bstate, *bstate2, *p, *key, *d;
    int err;

    memset(di, 0, sizeof(ibp_depotinfo_t));

    p = tbx_stk_string_token(buffer, " ", &bstate, &err);
    while (err == 0) {
        key = tbx_stk_string_token(p, ":", &bstate2, &err);
        d = tbx_stk_string_token(NULL, ":", &bstate2, &err);

        if (strcmp(key, ST_VERSION) == 0) {
            di->majorVersion = atof(d);
            di->minorVersion = atof(tbx_stk_string_token(NULL, ":", &bstate2, &err));
        } else if (strcmp(key, ST_DATAMOVERTYPE) == 0) {
            //*** I just skip this.  IS it used??? ***
        } else if (strcmp(key, ST_RESOURCEID) == 0) {
            di->rid = atol(d);
        } else if (strcmp(key, ST_RESOURCETYPE) == 0) {
            di->type = atol(d);
        } else if (strcmp(key, ST_CONFIG_TOTAL_SZ) == 0) {
            di->TotalConfigured = atoll(d);
        } else if (strcmp(key, ST_SERVED_TOTAL_SZ) == 0) {
            di->TotalServed = atoll(d);
        } else if (strcmp(key, ST_USED_TOTAL_SZ) == 0) {
            di->TotalUsed = atoll(d);
        } else if (strcmp(key, ST_USED_HARD_SZ) == 0) {
            di->HardUsed = atoll(d);
        } else if (strcmp(key, ST_SERVED_HARD_SZ) == 0) {
            di->HardServed = atoll(d);
        } else if (strcmp(key, ST_CONFIG_HARD_SZ) == 0) {
            di->HardConfigured = atoll(d);
        } else if (strcmp(key, ST_ALLOC_TOTAL_SZ) == 0) {
            di->SoftAllocable = atoll(d);  //** I have no idea what field this maps to....
        } else if (strcmp(key, ST_ALLOC_HARD_SZ) == 0) {
            di->HardAllocable = atoll(d);
        } else if (strcmp(key, ST_DURATION) == 0) {
            di->Duration = atoll(d);
        } else if (strcmp(key, "RE") == 0) {
            err = 1;
        } else {
            log_printf(1, "process_inq:  Unknown tag:%s key=%s data=%s\n", p, key, d);
        }

        p = tbx_stk_string_token(NULL, " ", &bstate, &err);
    }

    return(IBP_OK);
}

void set_hostport(char *hostport, int max_size, char *host, int port, ibp_connect_context_t *cc)
{
    char in_addr[DNS_ADDR_MAX];
    char ip[64];
    int type, i;

    type = (cc == NULL) ? NS_TYPE_SOCK : cc->type;

    i = 0;
    while ((host[i] != 0) && (host[i] != '#')) i++;
    if (host[i] == '#') {
        host[i] = 0;
        i=-i;
    }

    if (tbx_dnsc_lookup(host, in_addr, ip) != 0) {
        if (i<0) host[-i] = '#';
        log_printf(1, "set_hostport:  Failed to lookup host: %s\n", host);
        hostport[max_size-1] = '\0';
        snprintf(hostport, max_size-1, "%s:%d:%d:0", host, port, type);
        return;
    }
    if (i<0) host[-i] = '#';

    ip[63] = '\0';

    hostport[max_size-1] = '\0';
    snprintf(hostport, max_size-1, "%s" HP_HOSTPORT_SEPARATOR "%d" HP_HOSTPORT_SEPARATOR "%d" HP_HOSTPORT_SEPARATOR "0",
                 host, port, type);
}

char *change_hostport_cc(char *old_hostport, ibp_connect_context_t *cc)
{
    char host[MAX_HOST_SIZE], new_hostport[MAX_HOST_SIZE];
    char *hp2 = strdup(old_hostport);
    char *bstate;
    int fin, port;

    strncpy(host, tbx_stk_string_token(hp2, HP_HOSTPORT_SEPARATOR, &bstate, &fin), sizeof(host)-1);
    host[sizeof(host)-1] = '\0';
    port = atoi(bstate);

    set_hostport(new_hostport, sizeof(new_hostport), host, port, cc);

    free(hp2);
    return(strdup(new_hostport));
}

apr_time_t gop_get_end_time(op_generic_t *gop, int *state)
{
    apr_time_t end_time;
    command_op_t *cmd = &(gop->op->cmd);

    if (*state == 0) {
        *state = tbx_atomic_get(cmd->on_top);
        if (*state == 0) {
            end_time = apr_time_now() + apr_time_make(10,0);  //** Default to 10 secs while percolating to the top
        } else {  //** We're on top so use the official end time
            end_time = cmd->end_time;
        }
    } else {
        end_time = cmd->end_time;  //** This won't change after we're on top so no need to lock
    }
    return(end_time);
}

//
// General ibp_op_t functions
//

void ibp_op_init(ibp_context_t *ic, ibp_op_t *op)
{
    op_generic_t *gop;

    //** Clear it
    tbx_type_memclear(op, ibp_op_t, 1);

    //** Now munge the pointers
    gop = &(op->gop);
    gop_init(gop);
    gop->op = &(op->dop);
    gop->op->priv = op;
    gop->type = Q_TYPE_OPERATION;
    op->ic = ic;
    op->dop.priv = op;
    op->dop.pc = ic->pc; //**IS this needed?????
    gop->base.free = _ibp_op_free;
    gop->free_ptr = op;
    gop->base.pc = ic->pc;
    gop->base.status = gop_error_status;
}

ibp_op_t *new_ibp_op(ibp_context_t *ic)
{
    ibp_op_t *op;

    //** Make the struct and clear it
    tbx_type_malloc(op, ibp_op_t, 1);

    tbx_atomic_inc(ic->n_ops);
    ibp_op_init(ic, op);

    return(op);
}

void init_ibp_base_op(ibp_op_t *iop, char *logstr, int timeout_sec, int workload, char *hostport,
                      int cmp_size, int primary_cmd, int sub_cmd)
{
    command_op_t *cmd = &(iop->dop.cmd);
    apr_time_t dt;

    iop->primary_cmd = primary_cmd;
    iop->sub_cmd = sub_cmd;
    if (iop->ic->transfer_rate > 0) {
        dt = (double)workload / iop->ic->transfer_rate;
        if (dt < timeout_sec) dt = timeout_sec;
    } else {
        dt = timeout_sec;
    }

    cmd->timeout = apr_time_make(dt, 0);
    cmd->retry_count = iop->ic->max_retry;
    cmd->workload = workload;
    cmd->hostport = hostport;
    cmd->cmp_size = cmp_size;
    cmd->send_command = NULL;
    cmd->send_phase = NULL;
    cmd->recv_phase = NULL;
    cmd->on_submit = NULL;
    cmd->before_exec = NULL;
    cmd->destroy_command = NULL;

    cmd->coalesced_ops = NULL;

    cmd->connect_context = &(iop->ic->cc[primary_cmd]);
    tbx_ns_chksum_init(&(iop->ncs));
}

void ibp_op_ncs_set(op_generic_t *gop, tbx_ns_chksum_t *ncs)
{
    if ( ncs == NULL) return;

    ibp_op_t *op = ibp_get_iop(gop);
    op->ncs = *ncs;
}

int ibp_cc_type(ibp_connect_context_t *cc)
{
    if ( cc == NULL) return(NS_TYPE_UNKNOWN);

    return(cc->type);
}

void ibp_op_cc_set(op_generic_t *gop, ibp_connect_context_t *cc)
{
    ibp_op_t *op;
    char *orig;

    if (cc == NULL) return;

    op = ibp_get_iop(gop);

    op->dop.cmd.connect_context = cc;

    orig = op->dop.cmd.hostport;
    if (orig == NULL) return;

    op->dop.cmd.hostport = change_hostport_cc(orig, cc);
    free(orig);
}

int ibp_tbx_chksum_set(tbx_ns_chksum_t *ncs, tbx_chksum_t *cs, int blocksize)
{
    if (cs == NULL) {
        tbx_ns_chksum_clear(ncs);
    } else {
        if (blocksize == 0) blocksize = IBP_CHKSUM_BLOCKSIZE;
        tbx_ns_chksum_set(ncs, cs, blocksize);
        tbx_ns_chksum_enable(ncs);
    }

    return(0);
}

//
// Factories for ibp_op_t structs corresponding to different IBP client
// operations.
//
// TODO: Sort this alphabetically.
//
op_generic_t *ibp_read_op(ibp_context_t *ic, ibp_cap_t *cap, ibp_off_t offset, tbx_tbuf_t *buffer, ibp_off_t boff, ibp_off_t len, int timeout)
{
    op_generic_t *op = ibp_rw_op(ic, IBP_READ, cap, offset, buffer, boff, len, timeout);
    return(op);
}

op_generic_t *ibp_vec_read_op(ibp_context_t *ic, ibp_cap_t *cap, int n_vec, ibp_tbx_iovec_t *vec, tbx_tbuf_t *buffer, ibp_off_t boff, ibp_off_t len, int timeout)
{
    ibp_op_t *op = new_ibp_op(ic);
    if (op == NULL) return(NULL);

    op_generic_t *gop = ibp_get_gop(op);

    set_ibp_rw_op(op, IBP_READ, cap, 0, buffer, boff, len, timeout);
    op->ops.rw_op.n_ops = 1;
    op->ops.rw_op.n_tbx_iovec_total = n_vec;
    op->ops.rw_op.buf_single.n_iovec = n_vec;
    op->ops.rw_op.buf_single.iovec = vec;

    gop->op->cmd.send_command = vec_read_command;

    return(ibp_get_gop(op));
}

op_generic_t *ibp_write_op(ibp_context_t *ic, ibp_cap_t *cap, ibp_off_t offset, tbx_tbuf_t *buffer, ibp_off_t bpos, ibp_off_t len, int timeout)
{
    op_generic_t *gop = ibp_rw_op(ic, IBP_WRITE, cap, offset, buffer, bpos, len, timeout);
    return(gop);
}

op_generic_t *ibp_vec_write_op(ibp_context_t *ic, ibp_cap_t *cap, int n_iovec, ibp_tbx_iovec_t *iovec, tbx_tbuf_t *buffer, ibp_off_t bpos, ibp_off_t len, int timeout)
{
    ibp_op_t *op = new_ibp_op(ic);
    if (op == NULL) return(NULL);
    op_generic_t *gop = ibp_get_gop(op);

    set_ibp_rw_op(op, IBP_WRITE, cap, 0, buffer, bpos, len, timeout);
    op->ops.rw_op.n_ops = 1;
    op->ops.rw_op.n_tbx_iovec_total = n_iovec;
    op->ops.rw_op.buf_single.n_iovec = n_iovec;
    op->ops.rw_op.buf_single.iovec = iovec;

    gop->op->cmd.send_command = vec_write_command;

    return(ibp_get_gop(op));
}

op_generic_t *ibp_append_op(ibp_context_t *ic, ibp_cap_t *cap, tbx_tbuf_t *buffer, ibp_off_t bpos, ibp_off_t len, int timeout)
{
    op_generic_t *gop = ibp_rw_op(ic, IBP_STORE, cap, 0, buffer, bpos, len, timeout);
    if (gop == NULL) return(NULL);

    gop->op->cmd.send_command = append_command;
    return(gop);
}

void set_ibp_rw_op(ibp_op_t *op, int rw_type, ibp_cap_t *cap, ibp_off_t offset, tbx_tbuf_t *buffer, ibp_off_t bpos, ibp_off_t len, int timeout)
{
    char hoststr[MAX_HOST_SIZE];
    int port;
    char host[MAX_HOST_SIZE];
    ibp_op_rw_t *cmd;
    ibp_rw_buf_t *rwbuf;

    cmd = &(op->ops.rw_op);

    init_ibp_base_op(op, "rw", timeout, op->ic->rw_new_command + len, NULL, len, rw_type, IBP_NOP);
    op_generic_t *gop = ibp_get_gop(op);


    parse_cap(op->ic, cap, host, &port, cmd->key, cmd->typekey);
    set_hostport(hoststr, sizeof(hoststr), host, port, &(op->ic->cc[rw_type]));
    op->dop.cmd.hostport = strdup(hoststr);

    cmd->cap = cap;
    cmd->size = len; //** This is the total size

    rwbuf = &(cmd->buf_single);
    cmd->bs_ptr = rwbuf;
    cmd->rwbuf = &(cmd->bs_ptr);
    cmd->n_ops = 1;
    cmd->n_tbx_iovec_total = 1;
    cmd->rw_mode = rw_type;

    rwbuf->iovec = &(rwbuf->iovec_single);

    rwbuf->n_iovec = 1;
    rwbuf->iovec->offset = offset;
    rwbuf->iovec->len = len;
    rwbuf->buffer = buffer;
    rwbuf->boff = bpos;
    rwbuf->size = len;

    if (rw_type == IBP_WRITE) {
        gop->op->cmd.send_command = write_command;
        gop->op->cmd.send_phase = write_send;
        gop->op->cmd.recv_phase = write_recv;
        gop->op->cmd.on_submit = ibp_rw_submit_coalesce;
        gop->op->cmd.before_exec = ibp_rw_coalesce;
    } else {
        gop->op->cmd.send_command = read_command;
        gop->op->cmd.send_phase = NULL;
        gop->op->cmd.recv_phase = read_recv;
        gop->op->cmd.on_submit = ibp_rw_submit_coalesce;
        gop->op->cmd.before_exec = ibp_rw_coalesce;
    }

    op->ncs = op->ic->ncs;  //** Copy the default network chksum
}

op_generic_t *ibp_rw_op(ibp_context_t *ic, int rw_type, ibp_cap_t *cap, ibp_off_t offset, tbx_tbuf_t *buffer, ibp_off_t bpos, ibp_off_t len, int timeout)
{
    ibp_op_t *op = new_ibp_op(ic);
    if (op == NULL) return(NULL);

    set_ibp_rw_op(op, rw_type, cap, offset, buffer, bpos, len, timeout);

    return(ibp_get_gop(op));
}

op_generic_t *ibp_validate_chksum_op(ibp_context_t *ic, ibp_cap_t *mcap, int correct_errors, int *n_bad_blocks,
        int timeout)
{
    ibp_op_t *op = new_ibp_op(ic);
    char hoststr[MAX_HOST_SIZE];
    char host[MAX_HOST_SIZE];
    ibp_op_validate_chksum_t *cmd;
    int port;

    init_ibp_base_op(op, "validate_chksum", timeout, op->ic->other_new_command, NULL, 1, IBP_VALIDATE_CHKSUM, IBP_NOP);
    op_generic_t *gop = ibp_get_gop(op);

    cmd = &(op->ops.validate_op);

    parse_cap(op->ic, mcap, host, &port, cmd->key, cmd->typekey);
    set_hostport(hoststr, sizeof(hoststr), host, port, &(op->ic->cc[IBP_VALIDATE_CHKSUM]));
    op->dop.cmd.hostport = strdup(hoststr);

    cmd = &(op->ops.validate_op);
    cmd->correct_errors = correct_errors;
    cmd->n_bad_blocks = n_bad_blocks;

    gop->op->cmd.send_command = validate_chksum_command;
    gop->op->cmd.send_phase = NULL;
    gop->op->cmd.recv_phase = validate_chksum_recv;

    return(ibp_get_gop(op));
}

op_generic_t *new_ibp_get_chksum_op(ibp_context_t *ic, ibp_cap_t *mcap,
        int chksum_info_only, int *cs_type, int *cs_size, ibp_off_t *blocksize,
        ibp_off_t *nblocks, ibp_off_t *nbytes, char *buffer, ibp_off_t bufsize,
        int timeout)
{
    ibp_op_t *op = new_ibp_op(ic);
    
    char hoststr[MAX_HOST_SIZE];
    char host[MAX_HOST_SIZE];
    ibp_op_get_chksum_t *cmd;
    int port;

    init_ibp_base_op(op, "get_chksum", timeout, op->ic->other_new_command, NULL, 1, IBP_VALIDATE_CHKSUM, IBP_NOP);
    op_generic_t *gop = ibp_get_gop(op);

    cmd = &(op->ops.get_chksum_op);

    parse_cap(op->ic, mcap, host, &port, cmd->key, cmd->typekey);
    set_hostport(hoststr, sizeof(hoststr), host, port, &(op->ic->cc[IBP_VALIDATE_CHKSUM]));
    op->dop.cmd.hostport = strdup(hoststr);

    cmd->cap = mcap;
    cmd = &(op->ops.get_chksum_op);
    cmd->chksum_info_only = chksum_info_only;
    cmd->cs_type = cs_type;
    cmd->cs_size = cs_size;
    cmd->blocksize = blocksize;
    cmd->nblocks = nblocks;
    cmd->n_chksumbytes = nbytes;

    gop->op->cmd.send_command = get_chksum_command;
    gop->op->cmd.send_phase = NULL;
    gop->op->cmd.recv_phase = get_chksum_recv;

    return(ibp_get_gop(op));
}

op_generic_t *ibp_alloc_op(ibp_context_t *ic, ibp_capset_t *caps, ibp_off_t size, ibp_depot_t *depot, ibp_attributes_t *attr,
                               int disk_cs_type, ibp_off_t disk_blocksize, int timeout)
{
    ibp_op_t *op = new_ibp_op(ic);

    char hoststr[MAX_HOST_SIZE];
    char pchost[MAX_HOST_SIZE];
    ibp_op_alloc_t *cmd;

    ibppc_form_host(op->ic, pchost, sizeof(pchost), depot->host, depot->rid);
    set_hostport(hoststr, sizeof(hoststr), pchost, depot->port, &(op->ic->cc[IBP_ALLOCATE]));

    init_ibp_base_op(op, "alloc", timeout, op->ic->other_new_command, strdup(hoststr), 1, IBP_ALLOCATE, IBP_NOP);

    cmd = &(op->ops.alloc_op);
    cmd->caps = caps;
    cmd->depot = depot;
    cmd->attr = attr;

    cmd->duration = cmd->attr->duration - time(NULL);  //** This is in sec NOT APR time
    if (cmd->duration < 0) cmd->duration = cmd->attr->duration;

    cmd->size = size;
    cmd->disk_chksum_type = disk_cs_type;
    cmd->disk_blocksize = disk_blocksize;

    op_generic_t *gop = ibp_get_gop(op);
    gop->op->cmd.send_command = allocate_command;
    gop->op->cmd.send_phase = NULL;
    gop->op->cmd.recv_phase = allocate_recv;
    
    return(ibp_get_gop(op));
}

op_generic_t *new_ibp_split_alloc_op(ibp_context_t *ic, ibp_cap_t *mcap, ibp_capset_t *caps, ibp_off_t size,
                                     ibp_attributes_t *attr, int disk_cs_type, ibp_off_t disk_blocksize, int timeout)
{
    ibp_op_t *op = new_ibp_op(ic);

    char hoststr[MAX_HOST_SIZE];
    char host[MAX_HOST_SIZE];
    ibp_op_alloc_t *cmd;
    int port;

    init_ibp_base_op(op, "split_allocate", timeout, op->ic->other_new_command, NULL, 1, IBP_SPLIT_ALLOCATE, IBP_NOP);

    cmd = &(op->ops.alloc_op);

    parse_cap(op->ic, mcap, host, &port, cmd->key, cmd->typekey);
    set_hostport(hoststr, sizeof(hoststr), host, port, &(op->ic->cc[IBP_SPLIT_ALLOCATE]));
    op->dop.cmd.hostport = strdup(hoststr);

    cmd = &(op->ops.alloc_op);
    cmd->caps = caps;
    cmd->attr = attr;

    cmd->duration = cmd->attr->duration - time(NULL);  //** This is in sec NOT APR time
    if (cmd->duration < 0) cmd->duration = cmd->attr->duration;

    cmd->size = size;
    cmd->disk_chksum_type = disk_cs_type;
    cmd->disk_blocksize = disk_blocksize;

    op_generic_t *gop = ibp_get_gop(op);
    gop->op->cmd.send_command = split_allocate_command;
    gop->op->cmd.send_phase = NULL;
    gop->op->cmd.recv_phase = allocate_recv;

    return(ibp_get_gop(op));
}

op_generic_t *new_ibp_rename_op(ibp_context_t *ic, ibp_capset_t *caps, ibp_cap_t *mcap, int timeout)
{
    ibp_op_t *op = new_ibp_op(ic);
    char hoststr[MAX_HOST_SIZE];
    char host[MAX_HOST_SIZE];
    ibp_op_alloc_t *cmd;
    int port;

    log_printf(15, "set_ibp_rename_op: start. ic=%p\n", op->ic);

    init_ibp_base_op(op, "rename", timeout, op->ic->other_new_command, NULL, 1, IBP_RENAME, IBP_NOP);

    cmd = &(op->ops.alloc_op);

    parse_cap(op->ic, mcap, host, &port, cmd->key, cmd->typekey);
    set_hostport(hoststr, sizeof(hoststr), host, port, &(op->ic->cc[IBP_RENAME]));
    op->dop.cmd.hostport = strdup(hoststr);

    cmd->caps = caps;

    op_generic_t *gop = ibp_get_gop(op);
    gop->op->cmd.send_command = rename_command;
    gop->op->cmd.send_phase = NULL;
    gop->op->cmd.recv_phase = allocate_recv;

    return(ibp_get_gop(op));
}

op_generic_t *new_ibp_merge_alloc_op(ibp_context_t *ic, ibp_cap_t *mcap, ibp_cap_t *ccap, int timeout)
{
    ibp_op_t *op = new_ibp_op(ic);
    char hoststr[MAX_HOST_SIZE];
    char host[MAX_HOST_SIZE];
    char chost[MAX_HOST_SIZE];
    ibp_op_merge_alloc_t *cmd;
    int port, cport;

    log_printf(15, "set_ibp_merge_op: start. ic=%p\n", op->ic);

    init_ibp_base_op(op, "rename", timeout, op->ic->other_new_command, NULL, 1, IBP_RENAME, IBP_NOP);

    cmd = &(op->ops.merge_op);

    parse_cap(op->ic, mcap, host, &port, cmd->mkey, cmd->mtypekey);
    set_hostport(hoststr, sizeof(hoststr), host, port, &(op->ic->cc[IBP_MERGE_ALLOCATE]));
    op->dop.cmd.hostport = strdup(hoststr);

    parse_cap(op->ic, ccap, chost, &cport, cmd->ckey, cmd->ctypekey);

    op_generic_t *gop = ibp_get_gop(op);
    gop->op->cmd.send_command = merge_command;
    gop->op->cmd.send_phase = NULL;
    gop->op->cmd.recv_phase = status_get_recv;

    return(ibp_get_gop(op));
}

op_generic_t *ibp_alias_alloc_op(ibp_context_t *ic, ibp_capset_t *caps, ibp_cap_t *mcap, ibp_off_t offset, ibp_off_t size,
                                     int duration, int timeout)
{
    ibp_op_t *op = new_ibp_op(ic);
    char hoststr[MAX_HOST_SIZE];
    char host[MAX_HOST_SIZE];
    ibp_op_alloc_t *cmd;
    int port;

    log_printf(15, "set_ibp_alias_alloc_op: start. ic=%p\n", op->ic);

    init_ibp_base_op(op, "rename", timeout, op->ic->other_new_command, NULL, 1, IBP_ALIAS_ALLOCATE, IBP_NOP);

    cmd = &(op->ops.alloc_op);

    parse_cap(op->ic, mcap, host, &port, cmd->key, cmd->typekey);
    set_hostport(hoststr, sizeof(hoststr), host, port, &(op->ic->cc[IBP_ALIAS_ALLOCATE]));
    op->dop.cmd.hostport = strdup(hoststr);

    cmd->offset = offset;
    cmd->size = size;
    if (duration == 0) {
        cmd->duration = 0;
    } else {
        cmd->duration = duration - time(NULL); //** This is in sec NOT APR time
    }


    cmd->caps = caps;

    op_generic_t *gop = ibp_get_gop(op);
    gop->op->cmd.send_command = alias_allocate_command;
    gop->op->cmd.send_phase = NULL;
    gop->op->cmd.recv_phase = allocate_recv;

    return(ibp_get_gop(op));
}

void set_ibp_generic_modify_count_op(int command, ibp_op_t *op, ibp_cap_t *cap, ibp_cap_t *mcap, int mode, int captype, int timeout)
{
    char hoststr[MAX_HOST_SIZE];
    int port;
    char host[MAX_HOST_SIZE];
    ibp_op_probe_t *cmd;

    if ((command != IBP_MANAGE) && (command != IBP_ALIAS_MANAGE)) {
        log_printf(0, "set_ibp_generic_modify_count_op: Invalid command! should be IBP_MANAGE or IBP_ALIAS_MANAGE.  Got %d\n", command);
        return;
    }
    if ((mode != IBP_INCR) && (mode != IBP_DECR)) {
        log_printf(0, "ibp_modify_count_op: Invalid mode! should be IBP_INCR or IBP_DECR\n");
        return;
    }
    if ((captype != IBP_WRITECAP) && (captype != IBP_READCAP)) {
        log_printf(0, "ibp_modify_count_op: Invalid captype! should be IBP_READCAP or IBP_WRITECAP\n");
        return;
    }

    init_ibp_base_op(op, "modify_count", timeout, op->ic->other_new_command, NULL, 1, command, mode);

    cmd = &(op->ops.probe_op);

    parse_cap(op->ic, cap, host, &port, cmd->key, cmd->typekey);
    set_hostport(hoststr, sizeof(hoststr), host, port, &(op->ic->cc[command]));
    op->dop.cmd.hostport = strdup(hoststr);

    if (command == IBP_ALIAS_MANAGE) parse_cap(op->ic, mcap, host, &port, cmd->mkey, cmd->mtypekey);

    cmd->cmd = command;
    cmd->cap = cap;
    cmd->mode = mode;
    cmd->captype = captype;

    op_generic_t *gop = ibp_get_gop(op);
    gop->op->cmd.send_command = modify_count_command;
    if (command == IBP_ALIAS_MANAGE) gop->op->cmd.send_command = alias_modify_count_command;
    gop->op->cmd.send_phase = NULL;
    gop->op->cmd.recv_phase = status_get_recv;
}

op_generic_t *ibp_modify_count_op(ibp_context_t *ic, ibp_cap_t *cap, int mode, int captype, int timeout)
{
    ibp_op_t *op = new_ibp_op(ic);

    set_ibp_generic_modify_count_op(IBP_MANAGE, op, cap, NULL, mode, captype, timeout);

    return(ibp_get_gop(op));
}

op_generic_t *new_ibp_alias_modify_count_op(ibp_context_t *ic, ibp_cap_t *cap, ibp_cap_t *mcap, int mode, int captype, int timeout)
{
    ibp_op_t *op = new_ibp_op(ic);

    set_ibp_generic_modify_count_op(IBP_ALIAS_MANAGE, op, cap, mcap, mode, captype, timeout);

    return(ibp_get_gop(op));
}

op_generic_t *ibp_modify_alloc_op(ibp_context_t *ic, ibp_cap_t *cap, ibp_off_t size, int duration, int reliability, int timeout)
{
    ibp_op_t *op = new_ibp_op(ic);
    char hoststr[MAX_HOST_SIZE];
    int port;
    char host[MAX_HOST_SIZE];
    ibp_op_modify_alloc_t *cmd;

    init_ibp_base_op(op, "modify_alloc", timeout, op->ic->other_new_command, NULL, 1, IBP_MANAGE, IBP_CHNG);

    cmd = &(op->ops.mod_alloc_op);

    parse_cap(op->ic, cap, host, &port, cmd->key, cmd->typekey);
    set_hostport(hoststr, sizeof(hoststr), host, port, &(op->ic->cc[IBP_MANAGE]));
    op->dop.cmd.hostport = strdup(hoststr);

    cmd->cap = cap;
    cmd->size = size;
    cmd->duration = duration;
    cmd->reliability = reliability;

    op_generic_t *gop = ibp_get_gop(op);
    gop->op->cmd.send_command = modify_alloc_command;
    gop->op->cmd.send_phase = NULL;
    gop->op->cmd.recv_phase = status_get_recv;

    return(ibp_get_gop(op));
}

op_generic_t *new_ibp_alias_modify_alloc_op(ibp_context_t *ic, ibp_cap_t *cap, ibp_cap_t *mcap, ibp_off_t offset, ibp_off_t size, int duration, int timeout)
{
    ibp_op_t *op = new_ibp_op(ic);
    char hoststr[MAX_HOST_SIZE];
    int port;
    char host[MAX_HOST_SIZE];
    ibp_op_modify_alloc_t *cmd;

    init_ibp_base_op(op, "alias_modify_alloc", timeout, op->ic->other_new_command, NULL, 1, IBP_ALIAS_MANAGE, IBP_CHNG);

    cmd = &(op->ops.mod_alloc_op);

    parse_cap(op->ic, cap, host, &port, cmd->key, cmd->typekey);
    set_hostport(hoststr, sizeof(hoststr), host, port, &(op->ic->cc[IBP_ALIAS_MANAGE]));
    op->dop.cmd.hostport = strdup(hoststr);

    parse_cap(op->ic, mcap, host, &port, cmd->mkey, cmd->mtypekey);

    cmd->cap = cap;
    cmd->offset = offset;
    cmd->size = size;
    cmd->duration = duration;

    op_generic_t *gop = ibp_get_gop(op);
    gop->op->cmd.send_command = alias_modify_alloc_command;
    gop->op->cmd.send_phase = NULL;
    gop->op->cmd.recv_phase = status_get_recv;

    return(ibp_get_gop(op));
}

op_generic_t *ibp_truncate_op(ibp_context_t *ic, ibp_cap_t *cap, ibp_off_t size, int timeout)
{
    ibp_op_t *op = new_ibp_op(ic);
    char hoststr[MAX_HOST_SIZE];
    int port;
    char host[MAX_HOST_SIZE];
    ibp_op_modify_alloc_t *cmd;

    init_ibp_base_op(op, "truncate_alloc", timeout, op->ic->other_new_command, NULL, 1, IBP_MANAGE, IBP_CHNG);

    cmd = &(op->ops.mod_alloc_op);

    parse_cap(op->ic, cap, host, &port, cmd->key, cmd->typekey);
    set_hostport(hoststr, sizeof(hoststr), host, port, &(op->ic->cc[IBP_MANAGE]));
    op->dop.cmd.hostport = strdup(hoststr);

    cmd->cap = cap;
    cmd->size = size;

    op_generic_t *gop = ibp_get_gop(op);
    gop->op->cmd.send_command = truncate_command;
    gop->op->cmd.send_phase = NULL;
    gop->op->cmd.recv_phase = status_get_recv;

    return(ibp_get_gop(op));
}

op_generic_t *ibp_remove_op(ibp_context_t *ic, ibp_cap_t *cap, int timeout)
{
    return(ibp_modify_count_op(ic, cap, IBP_DECR, IBP_READCAP, timeout));
}

op_generic_t *ibp_alias_remove_op(ibp_context_t *ic, ibp_cap_t *cap, ibp_cap_t *mcap, int timeout)
{
    return(new_ibp_alias_modify_count_op(ic, cap, mcap, IBP_DECR, IBP_READCAP, timeout));
}

op_generic_t *ibp_probe_op(ibp_context_t *ic, ibp_cap_t *cap, ibp_capstatus_t *probe, int timeout)
{
    ibp_op_t *op = new_ibp_op(ic);
    char hoststr[MAX_HOST_SIZE];
    int port;
    char host[MAX_HOST_SIZE];
    ibp_op_probe_t *cmd;

    log_printf(15, " cctype=%d\n", op->ic->cc[IBP_MANAGE].type);

    init_ibp_base_op(op, "probe", timeout, op->ic->other_new_command, NULL, 1, IBP_MANAGE, IBP_PROBE);

    log_printf(15, "AFTER cctype=%d\n", op->ic->cc[IBP_MANAGE].type);
    tbx_log_flush();

    cmd = &(op->ops.probe_op);

    parse_cap(op->ic, cap, host, &port, cmd->key, cmd->typekey);
    set_hostport(hoststr, sizeof(hoststr), host, port, &(op->ic->cc[IBP_MANAGE]));
    op->dop.cmd.hostport = strdup(hoststr);

    log_printf(15, "set_ibp_probe_op: p=%p QWERT\n", probe);
    cmd->cap = cap;
    cmd->probe = probe;

    op_generic_t *gop = ibp_get_gop(op);
    gop->op->cmd.send_command = probe_command;
    gop->op->cmd.send_phase = NULL;
    gop->op->cmd.recv_phase = probe_recv;

    return(ibp_get_gop(op));
}

op_generic_t *new_ibp_alias_probe_op(ibp_context_t *ic, ibp_cap_t *cap, ibp_alias_capstatus_t *probe, int timeout)
{
    ibp_op_t *op = new_ibp_op(ic);
    char hoststr[MAX_HOST_SIZE];
    int port;
    char host[MAX_HOST_SIZE];
    ibp_op_probe_t *cmd;

    init_ibp_base_op(op, "alias_probe", timeout, op->ic->other_new_command, NULL, 1, IBP_ALIAS_MANAGE, IBP_PROBE);

    cmd = &(op->ops.probe_op);

    parse_cap(op->ic, cap, host, &port, cmd->key, cmd->typekey);
    set_hostport(hoststr, sizeof(hoststr), host, port, &(op->ic->cc[IBP_ALIAS_MANAGE]));
    op->dop.cmd.hostport = strdup(hoststr);

    cmd->cap = cap;
    cmd->alias_probe = probe;

    op_generic_t *gop = ibp_get_gop(op);
    gop->op->cmd.send_command = alias_probe_command;
    gop->op->cmd.send_phase = NULL;
    gop->op->cmd.recv_phase = alias_probe_recv;

    return(ibp_get_gop(op));
}

op_generic_t *ibp_copyappend_op(ibp_context_t *ic, int ns_type, char *path, ibp_cap_t *srccap, ibp_cap_t *destcap, ibp_off_t src_offset, ibp_off_t size,
                                    int src_timeout, int  dest_timeout, int dest_client_timeout)
{
    ibp_op_t *op = new_ibp_op(ic);
    if (op == NULL) return(NULL);

    char hoststr[MAX_HOST_SIZE];
    int port;
    char host[MAX_HOST_SIZE];
    ibp_op_copy_t *cmd;

    init_ibp_base_op(op, "copyappend", src_timeout, op->ic->rw_new_command + size, NULL, size, IBP_SEND, IBP_NOP);

    cmd = &(op->ops.copy_op);

    parse_cap(op->ic, srccap, host, &port, cmd->src_key, cmd->src_typekey);
    set_hostport(hoststr, sizeof(hoststr), host, port, &(op->ic->cc[IBP_SEND]));
    op->dop.cmd.hostport = strdup(hoststr);

    cmd->ibp_command = IBP_SEND;
    if (ns_type == NS_TYPE_PHOEBUS) {
        cmd->ibp_command = IBP_PHOEBUS_SEND;
        cmd->path = path;
        if (cmd->path == NULL) cmd->path = "auto";  //** If NULL default to auto
    } else {    //** All other ns types don't use the path
        cmd->path = "\0";
    }

    //** Want chksumming so tweak the command
    if (tbx_ns_chksum_is_valid(&(op->ncs)) == 1) {
        if (cmd->ibp_command == IBP_SEND) {
            cmd->ibp_command = IBP_SEND_CHKSUM;
        } else {
            cmd->ibp_command = IBP_PHOEBUS_SEND_CHKSUM;
        }
    }

    cmd->srccap = srccap;
    cmd->destcap = destcap;
    cmd->len = size;
    cmd->src_offset = src_offset;
    cmd->dest_timeout = dest_timeout;
    cmd->dest_client_timeout = dest_client_timeout;

    op_generic_t *gop = ibp_get_gop(op);
    gop->op->cmd.send_command = copyappend_command;
    gop->op->cmd.send_phase = NULL;
    gop->op->cmd.recv_phase = copy_recv;

    return(ibp_get_gop(op));
}

op_generic_t *ibp_copy_op(ibp_context_t *ic, int mode, int ns_type,
                              char *path, ibp_cap_t *srccap, ibp_cap_t *destcap,
                              ibp_off_t src_offset, ibp_off_t dest_offset,
                              ibp_off_t size, int src_timeout,
                              int dest_timeout, int dest_client_timeout)
{
    ibp_op_t *op = new_ibp_op(ic);
    if (op == NULL) return(NULL);
    
    char hoststr[MAX_HOST_SIZE];
    int port;
    char host[MAX_HOST_SIZE];
    ibp_op_copy_t *cmd;

    init_ibp_base_op(op, "copy", src_timeout, op->ic->rw_new_command + size, NULL, size, IBP_SEND, IBP_NOP);

    cmd = &(op->ops.copy_op);

    parse_cap(op->ic, srccap, host, &port, cmd->src_key, cmd->src_typekey);
    set_hostport(hoststr, sizeof(hoststr), host, port, &(op->ic->cc[IBP_SEND]));
    op->dop.cmd.hostport = strdup(hoststr);

    cmd->ibp_command = mode;
    if (ns_type == NS_TYPE_PHOEBUS) {
        cmd->ctype = IBP_PHOEBUS;
        cmd->path = path;
        if (cmd->path == NULL) cmd->path = "auto";  //** If NULL default to auto
    } else {    //** All other ns types don't use the path
        cmd->ctype = IBP_TCP;
        cmd->path = "\0";
    }

    //** Want chksumming so tweak the command
    if (tbx_ns_chksum_is_valid(&(op->ncs)) == 1) {
        if (cmd->ibp_command == IBP_PUSH) {
            cmd->ibp_command = IBP_PUSH_CHKSUM;
        } else {
            cmd->ibp_command = IBP_PULL_CHKSUM;
        }
    }

    cmd->srccap = srccap;
    cmd->destcap = destcap;
    cmd->len = size;
    cmd->src_offset = src_offset;
    cmd->dest_offset = dest_offset;
    cmd->dest_timeout = dest_timeout;
    cmd->dest_client_timeout = dest_client_timeout;

    op_generic_t *gop = ibp_get_gop(op);
    gop->op->cmd.send_command = pushpull_command;
    gop->op->cmd.send_phase = NULL;
    gop->op->cmd.recv_phase = copy_recv;

    return(ibp_get_gop(op));
}

op_generic_t *new_ibp_depot_modify_op(ibp_context_t *ic, ibp_depot_t *depot, char *password, ibp_off_t hard, ibp_off_t soft,
                                      int duration, int timeout)
{
    ibp_op_t *op = new_ibp_op(ic);
    if (op == NULL) return(NULL);

    ibp_op_depot_modify_t *cmd = &(op->ops.depot_modify_op);

    init_ibp_base_op(op, "depot_modify", timeout, op->ic->other_new_command, NULL,
                     op->ic->other_new_command, IBP_STATUS, IBP_ST_CHANGE);

    cmd->depot = depot;
    cmd->password = password;
    cmd->max_hard = hard;
    cmd->max_soft = soft;
    cmd->max_duration = duration;

    op_generic_t *gop = ibp_get_gop(op);
    gop->op->cmd.send_command = depot_modify_command;
    gop->op->cmd.send_phase = NULL;
    gop->op->cmd.recv_phase = status_get_recv;

    return(ibp_get_gop(op));
}

op_generic_t *ibp_depot_inq_op(ibp_context_t *ic, ibp_depot_t *depot, char *password, ibp_depotinfo_t *di, int timeout)
{
    ibp_op_t *op = new_ibp_op(ic);
    if (op == NULL) return(NULL);

    char hoststr[MAX_HOST_SIZE];
    char pchost[MAX_HOST_SIZE];
    ibp_op_depot_inq_t *cmd = &(op->ops.depot_inq_op);

    ibppc_form_host(op->ic, pchost, sizeof(pchost), depot->host, depot->rid);
    set_hostport(hoststr, sizeof(hoststr), pchost, depot->port, &(op->ic->cc[IBP_STATUS]));

    init_ibp_base_op(op, "depot_inq", timeout, op->ic->other_new_command, strdup(hoststr),
                     op->ic->other_new_command, IBP_STATUS, IBP_ST_INQ);

    cmd->depot = depot;
    cmd->password = password;
    cmd->di = di;

    op_generic_t *gop = ibp_get_gop(op);
    gop->op->cmd.send_command = depot_inq_command;
    gop->op->cmd.send_phase = NULL;
    gop->op->cmd.recv_phase = depot_inq_recv;

    return(ibp_get_gop(op));
}

op_generic_t *ibp_version_op(ibp_context_t *ic, ibp_depot_t *depot, char *buffer, int buffer_size, int timeout)
{
    ibp_op_t *op = new_ibp_op(ic);
    if (op == NULL) return(NULL);

    char hoststr[MAX_HOST_SIZE];
    char pchoststr[MAX_HOST_SIZE];
    ibp_op_version_t *cmd = &(op->ops.ver_op);

    ibppc_form_host(op->ic, pchoststr, sizeof(pchoststr), depot->host, depot->rid);
    set_hostport(hoststr, sizeof(hoststr), pchoststr, depot->port, &(op->ic->cc[IBP_STATUS]));

    init_ibp_base_op(op, "depot_version", timeout, op->ic->other_new_command, strdup(hoststr),
                     op->ic->other_new_command, IBP_STATUS, IBP_ST_VERSION);

    cmd->depot = depot;
    cmd->buffer = buffer;
    cmd->buffer_size = buffer_size;

    op_generic_t *gop = ibp_get_gop(op);
    gop->op->cmd.send_command = depot_version_command;
    gop->op->cmd.send_phase = NULL;
    gop->op->cmd.recv_phase = depot_version_recv;

    return(ibp_get_gop(op));
}

op_generic_t *ibp_query_resources_op(ibp_context_t *ic, ibp_depot_t *depot, ibp_ridlist_t *rlist, int timeout)
{
    ibp_op_t *op = new_ibp_op(ic);
    if (op == NULL) return(NULL);
    
    char hoststr[MAX_HOST_SIZE];
    char pchoststr[MAX_HOST_SIZE];
    ibp_op_rid_inq_t *cmd = &(op->ops.rid_op);

    ibppc_form_host(op->ic, pchoststr, sizeof(pchoststr), depot->host, depot->rid);
    set_hostport(hoststr, sizeof(hoststr), pchoststr, depot->port, &(op->ic->cc[IBP_STATUS]));

    init_ibp_base_op(op, "query_resources", timeout, op->ic->other_new_command, strdup(hoststr),
                        op->ic->other_new_command, IBP_STATUS, IBP_ST_RES);

    cmd->depot = depot;
    cmd->rlist = rlist;

    op_generic_t *gop = ibp_get_gop(op);
    gop->op->cmd.send_command = query_res_command;
    gop->op->cmd.send_phase = NULL;
    gop->op->cmd.recv_phase = query_res_recv;
    
    return(ibp_get_gop(op));
}
