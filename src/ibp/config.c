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

#define _log_module_index 129

#include <apr_pools.h>
#include <apr_thread_mutex.h>
#include <apr_thread_proc.h>
#include <apr_time.h>
#include <assert.h>
#include <gop/gop.h>
#include <gop/hp.h>
#include <gop/opque.h>
#include <gop/portal.h>
#include <gop/types.h>
#include <ibp/protocol.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <tbx/atomic_counter.h>
#include <tbx/dns_cache.h>
#include <tbx/fmttypes.h>
#include <tbx/iniparse.h>
#include <tbx/list.h>
#include <tbx/log.h>
#include <tbx/net_sock.h>
#include <tbx/network.h>
#include <tbx/pigeon_coop.h>
#include <tbx/stack.h>
#include <tbx/type_malloc.h>

#include "misc.h"
#include "op.h"
#include "types.h"

extern apr_thread_once_t *_err_once;

//** These are in bip_op.c
gop_op_status_t vec_read_command(gop_op_generic_t *gop, tbx_ns_t *ns);
gop_op_status_t vec_write_command(gop_op_generic_t *gop, tbx_ns_t *ns);

void *_ibp_dup_connect_context(void *connect_context);
void _ibp_destroy_connect_context(void *connect_context);
int _ibp_connect(tbx_ns_t *ns, void *connect_context, char *host, int port, tbx_ns_timeout_t timeout);

void _ibp_op_free(gop_op_generic_t *op, int mode);
void _ibp_submit_op(void *arg, gop_op_generic_t *op);

static gop_portal_fn_t _ibp_base_portal = {
    .dup_connect_context = _ibp_dup_connect_context,
    .destroy_connect_context = _ibp_destroy_connect_context,
    .connect = _ibp_connect,
    .close_connection = tbx_ns_close,
    .sort_tasks = gop_default_sort_ops,
    .submit = _ibp_submit_op,
    .sync_exec = NULL
};

int _ibp_context_count = 0;

typedef struct {
    tbx_stack_t stack;
} rwc_gop_stack_t;

typedef struct {
    tbx_stack_t list_stack;
    tbx_pch_t pch;
} rw_coalesce_t;

//*************************************************************
// rwc_gop_stack_new - Creates a new rwc_stack_t set
//*************************************************************

void *rwc_gop_stack_new(void *arg, int size)
{
    rwc_gop_stack_t *shelf;
    int i;

    tbx_type_malloc_clear(shelf, rwc_gop_stack_t, size);

    for (i=0; i<size; i++) {
        tbx_stack_init(&(shelf[i].stack));
    }

    return((void *)shelf);
}

//*************************************************************
// rwc_gop_stack_free - Destroys a rwc_stack_t set
//*************************************************************

void rwc_gop_stack_free(void *arg, int size, void *data)
{
    rwc_gop_stack_t *shelf = (rwc_gop_stack_t *)data;
    int i;

    for (i=0; i<size; i++) {
        tbx_stack_empty(&(shelf[i].stack), 0);
    }

    free(shelf);
    return;
}

//*************************************************************
// rwc_stacks_new - Creates a new rw_coalesce_t set
//*************************************************************

void *rwc_stacks_new(void *arg, int size)
{
    rw_coalesce_t *shelf;
    int i;

    tbx_type_malloc_clear(shelf, rw_coalesce_t, size);

    for (i=0; i<size; i++) {
        tbx_stack_init(&(shelf[i].list_stack));
    }

    return((void *)shelf);
}

//*************************************************************
// rwc_stacks_free - Destroys a rw_coalesce_t set
//*************************************************************

void rwc_stacks_free(void *arg, int size, void *data)
{
    rw_coalesce_t *shelf = (rw_coalesce_t *)data;
    int i;

    for (i=0; i<size; i++) {
        tbx_stack_empty(&(shelf[i].list_stack), 0);
    }

    free(shelf);
    return;
}

//*************************************************************
// ibp_rw_submit_coalesce - Registers the RW op so it can
//   be combined with other similar ops
//*************************************************************

int ibp_rw_submit_coalesce(tbx_stack_t *stack, tbx_stack_ele_t *ele)
{
    gop_op_generic_t *gop = (gop_op_generic_t *)tbx_stack_ele_get_data(ele);
    ibp_op_t *iop = ibp_get_iop(gop);
    ibp_context_t *ic = iop->ic;
    ibp_op_rw_t *cmd = &(iop->ops.rw_op);
    rw_coalesce_t *rwc;
    tbx_pch_t pch;

    apr_thread_mutex_lock(ic->lock);

    if (ic->coalesce_enable == 0) {
        apr_thread_mutex_unlock(ic->lock);
        return(0);
    }

    //** If already coalesced return
    log_printf(15, "gid=%d coalesced_ops=%p\n", gop_id(gop), gop->op->cmd.coalesced_ops);

    if (gop->op->cmd.coalesced_ops != NULL) {
        apr_thread_mutex_unlock(ic->lock);
        return(0);
    }

    rwc = (rw_coalesce_t *)tbx_list_search(ic->coalesced_ops, cmd->cap);
    if (rwc == NULL) {  //** First time so add a stack
        pch = tbx_pch_reserve(ic->coalesced_stacks);
        rwc = (rw_coalesce_t *)tbx_pch_data(&pch);
        rwc->pch = pch;
        tbx_list_insert(ic->coalesced_ops, cmd->cap, rwc);
//    rwc->hp_stack = stack;
    }

    tbx_stack_move_to_bottom(&(rwc->list_stack));
    tbx_stack_insert_below(&(rwc->list_stack), ele);
    iop->hp_parent = stack;

    log_printf(15, "ibp_rw_submit_coalesce: gid=%d cap=%s count=%d\n", gop_id(gop), cmd->cap, tbx_stack_count(&(rwc->list_stack)));

    apr_thread_mutex_unlock(ic->lock);

    return(0);
}

//*************************************************************
// ibp_rw_coalesce - Coalesces read or write op with other pending ops
//*************************************************************

int ibp_rw_coalesce(gop_op_generic_t *gop1)
{
    ibp_op_t *iop1 = ibp_get_iop(gop1);
    ibp_op_t *iop2;
    ibp_context_t *ic = iop1->ic;
    ibp_op_rw_t *cmd1 = &(iop1->ops.rw_op);
    ibp_op_rw_t *cmd2;
    gop_op_generic_t *gop2;
    ibp_rw_buf_t **rwbuf;
    rw_coalesce_t *rwc;
    tbx_stack_ele_t *ele;
    tbx_stack_t *cstack;
    int64_t workload;
    int n, iov_sum, found_myself;
    rwc_gop_stack_t *rwcg;
    tbx_pch_t pch;
    tbx_stack_t *my_hp = iop1->hp_parent;;

    apr_thread_mutex_lock(ic->lock);

    if ((ic->coalesce_enable == 0) || (gop1->op->cmd.coalesced_ops != NULL)) {
        apr_thread_mutex_unlock(ic->lock);
        return(0);
    }

    rwc = (rw_coalesce_t *)tbx_list_search(ic->coalesced_ops, cmd1->cap);

    if (rwc == NULL) { //** Nothing to do so exit;
        apr_thread_mutex_unlock(ic->lock);
        return(0);
    }

    if (tbx_stack_count(&(rwc->list_stack)) == 1) { //** Nothing to do so exit
        ele = (tbx_stack_ele_t *)tbx_stack_pop(&(rwc->list_stack));  //** The top most task should be me
        gop2 = (gop_op_generic_t *)tbx_stack_ele_get_data(ele);
        if (gop2 != gop1) {
            log_printf(0, "ERROR! top stack element is not me! gid1=%d gid2=%d\n", gop_id(gop1), gop_id(gop2));
            tbx_log_flush();
            abort();
        }

        tbx_list_remove(ic->coalesced_ops, cmd1->cap, NULL);
        tbx_pch_release(ic->coalesced_stacks, &(rwc->pch));

        apr_thread_mutex_unlock(ic->lock);
        return(0);
    }

    log_printf(15, "ibp_rw_coalesce: gid=%d cap=%s count=%d\n", gop_id(gop1), cmd1->cap, tbx_stack_count(&(rwc->list_stack)));

    workload = 0;
    n = tbx_stack_count(&(rwc->list_stack))+1;
    tbx_type_malloc(rwbuf, ibp_rw_buf_t *, n);
    pch = tbx_pch_reserve(ic->coalesced_gop_stacks);
    rwcg = (rwc_gop_stack_t *)tbx_pch_data(&pch);
    cmd1->rwcg_pch = pch;
    cstack = &(rwcg->stack);
    gop1->op->cmd.coalesced_ops = cstack;
    cmd1->rwbuf = rwbuf;

    //** PAtch over the send command function
    if (cmd1->rw_mode == IBP_READ) {
        gop1->op->cmd.send_command = vec_read_command;
    } else {
        gop1->op->cmd.send_command = vec_write_command;
    }

    n = 0;
    iov_sum = 0;
    found_myself = 0;
    tbx_stack_move_to_top(&(rwc->list_stack));
    ele = (tbx_stack_ele_t *)tbx_stack_get_current_data(&(rwc->list_stack));
    do {
        gop2 = (gop_op_generic_t *)tbx_stack_ele_get_data(ele);
        iop2 = ibp_get_iop(gop2);
        cmd2 = &(iop2->ops.rw_op);

        //** Unlink the element if needed
        if (gop2 != gop1) {
            if (iop2->hp_parent == my_hp) {
                tbx_stack_move_to_ptr(iop2->hp_parent, ele);
                tbx_stack_unlink_current(iop2->hp_parent, 0);
                tbx_stack_link_push(cstack, ele);
            }
        } else {
            found_myself = 1;
        }


        //** Can only coalesce ops on the same hoststr
        if (iop2->hp_parent == my_hp) {
            log_printf(15, "ibp_rw_coalesce: gop[%d]->gid=%d n_iov=%d io_total=%d\n", n, gop_id(gop2), cmd2->n_tbx_iovec_total, iov_sum);
            rwbuf[n] = &(cmd2->buf_single);
            iov_sum += cmd2->n_tbx_iovec_total;
            workload += cmd2->size;
            n++;

            tbx_stack_delete_current(&(rwc->list_stack), 0, 0);  //** Remove it from the stack and move down to the next
            ele = NULL;
            if (workload < ic->max_coalesce) {
                ele = (tbx_stack_ele_t *)tbx_stack_get_current_data(&(rwc->list_stack));
            }
        } else {
            log_printf(15, "SKIPPING: gop[-]->gid=%d n_iov=%d io_total=%d\n",
                            gop_id(gop2), cmd2->n_tbx_iovec_total, iov_sum);
            tbx_stack_move_down(&(rwc->list_stack));
            ele = (tbx_stack_ele_t *)tbx_stack_get_current_data(&(rwc->list_stack));
        }
    } while ((ele != NULL) && (workload < ic->max_coalesce) && (iov_sum < 2000));

    if (found_myself == 0) {  //** Oops! Hit the max_coalesce workdload or size so Got to scan the list for myself
        tbx_stack_move_to_top(&(rwc->list_stack));
        while ((ele = (tbx_stack_ele_t *)tbx_stack_get_current_data(&(rwc->list_stack))) != NULL) {
            gop2 = (gop_op_generic_t *)tbx_stack_ele_get_data(ele);
            if (gop2 == gop1) {
                iop2 = ibp_get_iop(gop2);
                cmd2 = &(iop2->ops.rw_op);
                rwbuf[n] = &(cmd2->buf_single);
                iov_sum += cmd2->n_tbx_iovec_total;
                workload += cmd2->size;
                n++;

                tbx_stack_delete_current(&(rwc->list_stack), 0, 0);
                found_myself = 1;
                break;
            }

            tbx_stack_move_down(&(rwc->list_stack));
        }

        if (found_myself == 0) {
            log_printf(0, "ERROR! Scanned entire list and couldnt find myself! gid1=%d\n", gop_id(gop1));
            tbx_log_flush();
            abort();
        }
    }

    if (n > 0) log_printf(1, " Coalescing %d ops totaling " I64T " bytes  iov_sum=%d\n", n, workload, iov_sum);
    if (tbx_stack_count(&(rwc->list_stack)) > 0) log_printf(1, "%d ops left on stack to coalesce\n", tbx_stack_count(&(rwc->list_stack)));

    cmd1->n_ops = n;
    cmd1->n_tbx_iovec_total = iov_sum;
    cmd1->size = workload;
    gop1->op->cmd.workload = workload + ic->rw_new_command;

    if (tbx_stack_count(&(rwc->list_stack)) == 0) {  //** Nothing left so free it
        tbx_list_remove(ic->coalesced_ops, cmd1->cap, NULL);
        tbx_pch_release(ic->coalesced_stacks, &(rwc->pch));
    }

    apr_thread_mutex_unlock(ic->lock);

    return(0);
}


//*************************************************************


gop_portal_fn_t default_ibp_oplist_imp()
{
    return(_ibp_base_portal);
}


//*************************************************************

void _ibp_op_free(gop_op_generic_t *gop, int mode)
{
    ibp_op_t *iop;

    log_printf(15, "_ibp_op_free: mode=%d gid=%d gop=%p\n", mode, gop_id(gop), gop);
    tbx_log_flush();

    if (gop->op->cmd.hostport != NULL) {
        free(gop->op->cmd.hostport);
        gop->op->cmd.hostport = NULL;
    }

    if (gop->op->cmd.coalesced_ops != NULL) {  //** Coalesced R/W op so free the stack
        log_printf(15, "gid=%d Freeing rwbuf\n", gop_id(gop));
        iop = ibp_get_iop(gop);
        tbx_pch_release(iop->ic->coalesced_gop_stacks, &(iop->ops.rw_op.rwcg_pch));
        free(iop->ops.rw_op.rwbuf);
        gop->op->cmd.coalesced_ops = NULL;
    }
    gop_generic_free(gop, OP_FINALIZE);  //** I free the actual op

    if (mode == OP_DESTROY) free(gop->free_ptr);
    log_printf(15, "_ibp_op_free: END\n");
    tbx_log_flush();

}

//*************************************************************

void _ibp_submit_op(void *arg, gop_op_generic_t *gop)
{
    gop_portal_context_t *pc = gop->base.pc;

    log_printf(15, "_ibp_submit_op: hpc=%p hpc->table=%p gop=%p gid=%d\n", pc, pc->table, gop, gop_id(gop));

    if (gop->base.execution_mode == OP_EXEC_DIRECT) {
        gop_hp_direct_submit(pc, gop);
    } else {
        gop_hp_que_op_submit(pc, gop);
    }
}

//********************************************************************
// _ibp_dup_connect_context - Copies an IBP connect_context structure
//********************************************************************

void *_ibp_dup_connect_context(void *connect_context)
{
    ibp_connect_context_t *cc = (ibp_connect_context_t *)connect_context;
    ibp_connect_context_t *ccdup;

    if (cc == NULL) return(NULL);

    ccdup = (ibp_connect_context_t *)malloc(sizeof(ibp_connect_context_t));
   FATAL_UNLESS(ccdup != NULL);

    *ccdup = *cc;

    return((void *)ccdup);
}

//********************************************************************
// _ibp_destroy_connect_context - Frees/Destroys a IBP connect_context structure
//********************************************************************

void _ibp_destroy_connect_context(void *connect_context)
{
    if (connect_context == NULL) return;

    free(connect_context);

    return;
}


//**********************************************************
// _ibp_connect - Makes an IBP connection to a remote host
//     If connect_context == NULL then a standard socket based
//     connection is made.
//**********************************************************

int _ibp_connect(tbx_ns_t *ns, void *connect_context, char *host, int port, tbx_ns_timeout_t timeout)
{
    ibp_connect_context_t *cc = (ibp_connect_context_t *)connect_context;
    int i, n;

    int to = timeout;
    log_printf(0, "HOST host=%s to=%d\n", host, to);

    if (cc != NULL) {
        switch(cc->type) {
        case NS_TYPE_SOCK:
            tbx_ns_sock_config(ns, cc->tcpsize);
            break;
        default:
            log_printf(0, "_ibp__connect: Invalid type=%d Exiting!\n", cc->type);
            return(1);
        }
    } else {
        tbx_ns_sock_config(ns, 0);
    }

    //** See if we have an RID if so peel it off
    i = 0;
    while ((host[i] != 0) && (host[i] != '#')) i++;

    if (host[i] == '#') {
        host[i] = 0;
        i=-i;
    }
    n = tbx_ns_connect(ns, host, port, timeout);
    if (i<0) host[-i] = '#';

    return(n);
}


//**********************************************************
// set/unset routines for options
//**********************************************************

int ibp_context_chksum_set(ibp_context_t *ic, tbx_ns_chksum_t *ncs)
{
    tbx_ns_chksum_clear(&(ic->ncs));
    if (ncs != NULL) ic->ncs = *ncs;
    return(0);
}
void ibp_context_chksum_get(ibp_context_t *ic, tbx_ns_chksum_t *ncs)
{
    *ncs = ic->ncs;
};

void ibp_context_abort_attempts_set(ibp_context_t *ic, int n)
{
    ic->abort_conn_attempts = n;
}
int  ibp_context_abort_attempts_get(ibp_context_t *ic)
{
    return(ic->abort_conn_attempts);
}
void ibp_tcpsize_set(ibp_context_t *ic, int n)
{
    ic->tcpsize = n;
}
int  ibp_context_tcpsize_get(ibp_context_t *ic)
{
    return(ic->tcpsize);
}
void ibp_context_min_depot_threads_set(ibp_context_t *ic, int n)
{
    ic->min_threads = n;
    ic->pc->min_threads = n;
    gop_change_all_hportal_conn(ic->pc, ic->min_threads, ic->max_threads, ic->dt_connect);
}
int  ibp_context_min_depot_threads_get(ibp_context_t *ic)
{
    return(ic->min_threads);
}
void ibp_context_max_depot_threads_set(ibp_context_t *ic, int n)
{
    ic->max_threads = n;
    ic->pc->max_threads = n;
    gop_change_all_hportal_conn(ic->pc, ic->min_threads, ic->max_threads, ic->dt_connect);
}
int  ibp_context_max_depot_threads_get(ibp_context_t *ic)
{
    return(ic->max_threads);
}
void ibp_context_max_connections_set(ibp_context_t *ic, int n)
{
    ic->max_connections = n;
    ic->pc->max_connections = n;
}
int  ibp_context_max_connections_get(ibp_context_t *ic)
{
    return(ic->max_connections);
}
void ibp_context_command_weight_set(ibp_context_t *ic, int n)
{
    ic->other_new_command = n;
}
int  ibp_context_command_weight_get(ibp_context_t *ic)
{
    return(ic->other_new_command);
}
void ibp_context_max_retry_set_wait(ibp_context_t *ic, int n)
{
    ic->max_wait = n;
    ic->pc->max_wait = n;
}
int  ibp_context_max_retry_get_wait(ibp_context_t *ic)
{
    return(ic->max_wait);
}
void ibp_context_max_thread_workload_set(ibp_context_t *ic, int64_t n)
{
    ic->max_workload = n;
    ic->pc->max_workload = n;
}
int64_t  ibp_context_max_thread_workload_get(ibp_context_t *ic)
{
    return(ic->max_workload);
}
void ibp_context_max_coalesce_workload_set(ibp_context_t *ic, int64_t n)
{
    ic->max_coalesce = n;
}
int64_t  ibp_context_max_coalesce_workload_get(ibp_context_t *ic)
{
    return(ic->max_coalesce);
}
void ibp_context_wait_stable_time_set(ibp_context_t *ic, int n)
{
    ic->wait_stable_time = n;
    ic->pc->wait_stable_time = n;
}
int  ibp_context_wait_stable_time_get(ibp_context_t *ic)
{
    return(ic->wait_stable_time);
}
void ibp_context_check_interval_set(ibp_context_t *ic, int n)
{
    ic->check_connection_interval = n;
    ic->pc->check_connection_interval = n;
}
int  ibp_context_check_interval_get(ibp_context_t *ic)
{
    return(ic->check_connection_interval);
}
void ibp_context_max_retry_set(ibp_context_t *ic, int n)
{
    ic->max_retry = n;
    ic->pc->max_retry = n;
}
int  ibp_context_max_retry_get(ibp_context_t *ic)
{
    return(ic->max_retry);
}
void ibp_context_transfer_rate_set(ibp_context_t *ic, double rate)
{
    ic->transfer_rate = rate;
}
double  ibp_context_transfer_rate_get(ibp_context_t *ic)
{
    return(ic->transfer_rate);
}
void ibp_context_connection_mode_set(ibp_context_t *ic, int mode)
{
    ic->connection_mode = mode;
}
int  ibp_context_connection_mode_get(ibp_context_t *ic)
{
    return(ic->connection_mode);
}

//**********************************************************
// set_ibp_config - Sets the ibp config options
//**********************************************************

void copy_ibp_config(ibp_context_t *cfg)
{

    cfg->pc->min_idle = cfg->min_idle;
    cfg->pc->min_threads = cfg->min_threads;
    cfg->pc->max_threads = cfg->max_threads;
    cfg->pc->max_connections = cfg->max_connections;
    cfg->pc->max_workload = cfg->max_workload;
    cfg->pc->max_wait = cfg->max_wait;
    cfg->pc->dt_connect = cfg->dt_connect;
    cfg->pc->wait_stable_time = cfg->wait_stable_time;
    cfg->pc->abort_conn_attempts = cfg->abort_conn_attempts;
    cfg->pc->check_connection_interval = cfg->check_connection_interval;
    cfg->pc->max_retry = cfg->max_retry;
}

//**********************************************************
// Set the default CC's for read or write
//**********************************************************

void ibp_read_cc_set(ibp_context_t *ic, ibp_connect_context_t *cc)
{

    ic->cc[IBP_LOAD] = *cc;
    ic->cc[IBP_SEND] = *cc;
    ic->cc[IBP_PHOEBUS_SEND] = *cc;
}

//**********************************************************

void ibp_write_cc_set(ibp_context_t *ic, ibp_connect_context_t *cc)
{
    ic->cc[IBP_WRITE] = *cc;
    ic->cc[IBP_STORE] = *cc;
}

//**********************************************************
// cc_load - Stores a CC from the given keyfile
//**********************************************************

void cc_load(tbx_inip_file_t *kf, char *name, ibp_connect_context_t *cc)
{
    char *type = tbx_inip_get_string(kf, "ibp_connect", name, NULL);

    if (type == NULL) return;

    if (strcmp(type, "socket") == 0) {
        cc->type = NS_TYPE_SOCK;
    } else if (strcmp(type, "phoebus") == 0) {
        cc->type = NS_TYPE_PHOEBUS;
    } else if (strcmp(type, "ssl1") == 0) {
        cc->type = NS_TYPE_1_SSL;
    } else if (strcmp(type, "ssl2") == 0) {
        cc->type = NS_TYPE_2_SSL;
    } else {
        log_printf(0, "cc_load: Invalid CC type! command: %s type: %s\n", name, type);
    }
}

//**********************************************************
// ibp_cc_table_load - Loads the default connect_context for commands
//**********************************************************

void ibp_cc_load(tbx_inip_file_t *kf, ibp_context_t *cfg)
{
    int i;
    ibp_connect_context_t cc;

    //** Set everything to the default **
    cc.type = NS_TYPE_SOCK;
    cc.tcpsize = 0;
    cc_load(kf, "default", &cc);
    for (i=0; i<=IBP_MAX_NUM_CMDS; i++) cfg->cc[i] = cc;

    //** Now load the individual commands if they exist
    cc_load(kf, "ibp_allocate", &(cfg->cc[IBP_ALLOCATE]));
    cc_load(kf, "ibp_store", &(cfg->cc[IBP_STORE]));
    cc_load(kf, "ibp_status", &(cfg->cc[IBP_STATUS]));
    cc_load(kf, "ibp_send", &(cfg->cc[IBP_SEND]));
    cc_load(kf, "ibp_load", &(cfg->cc[IBP_LOAD]));
    cc_load(kf, "ibp_manage", &(cfg->cc[IBP_MANAGE]));
    cc_load(kf, "ibp_write", &(cfg->cc[IBP_WRITE]));
    cc_load(kf, "ibp_proxy_allocate", &(cfg->cc[IBP_PROXY_ALLOCATE]));
    cc_load(kf, "ibp_proxy_manage", &(cfg->cc[IBP_PROXY_MANAGE]));
    cc_load(kf, "ibp_rename", &(cfg->cc[IBP_RENAME]));
    cc_load(kf, "ibp_phoebus_send", &(cfg->cc[IBP_PHOEBUS_SEND]));
    cc_load(kf, "ibp_push", &(cfg->cc[IBP_PUSH]));
    cc_load(kf, "ibp_pull", &(cfg->cc[IBP_PULL]));
}


//**********************************************************
// ibp_config_load - Loads the ibp client config
//**********************************************************

int ibp_config_load(ibp_context_t *ic, tbx_inip_file_t *keyfile, char *section)
{
    apr_time_t t = 0;

    if (section == NULL) section = "ibp";

    ic->abort_conn_attempts = tbx_inip_get_integer(keyfile, section, "abort_attempts", ic->abort_conn_attempts);
    t = tbx_inip_get_integer(keyfile, section, "min_idle", 0);
    if (t != 0) ic->min_idle = t;
    ic->tcpsize = tbx_inip_get_integer(keyfile, section, "tcpsize", ic->tcpsize);
    ic->min_threads = tbx_inip_get_integer(keyfile, section, "min_depot_threads", ic->min_threads);
    ic->max_threads = tbx_inip_get_integer(keyfile, section, "max_depot_threads", ic->max_threads);
    ic->dt_connect = tbx_inip_get_integer(keyfile, section, "dt_connect_us", ic->dt_connect);
    ic->max_connections = tbx_inip_get_integer(keyfile, section, "max_connections", ic->max_connections);
    ic->rw_new_command = tbx_inip_get_integer(keyfile, section, "rw_command_weight", ic->rw_new_command);
    ic->other_new_command = tbx_inip_get_integer(keyfile, section, "other_command_weight", ic->other_new_command);
    ic->max_workload = tbx_inip_get_integer(keyfile, section, "max_thread_workload", ic->max_workload);
    ic->max_coalesce = tbx_inip_get_integer(keyfile, section, "max_coalesce_workload", ic->max_coalesce);
    ic->coalesce_enable = tbx_inip_get_integer(keyfile, section, "coalesce_enable", ic->coalesce_enable);
    ic->max_wait = tbx_inip_get_integer(keyfile, section, "max_wait", ic->max_wait);
    ic->wait_stable_time = tbx_inip_get_integer(keyfile, section, "wait_stable_time", ic->wait_stable_time);
    ic->check_connection_interval = tbx_inip_get_integer(keyfile, section, "check_interval", ic->check_connection_interval);
    ic->max_retry = tbx_inip_get_integer(keyfile, section, "max_retry", ic->max_retry);
    ic->connection_mode = tbx_inip_get_integer(keyfile, section, "connection_mode", ic->connection_mode);
    ic->transfer_rate = tbx_inip_get_double(keyfile, section, "transfer_rate", ic->transfer_rate);
    ic->rr_size = tbx_inip_get_integer(keyfile, section, "rr_size", ic->rr_size);

    ibp_cc_load(keyfile, ic);

    copy_ibp_config(ic);

    log_printf(1, "section=%s cmode=%d min_depot_threads=%d max_depot_threads=%d max_connections=%d max_thread_workload=%" PRId64 " coalesce_enable=%d dt_connect=" TT "\n", section, ic->connection_mode, ic->min_threads, ic->max_threads, ic->max_connections, ic->max_workload, ic->coalesce_enable, ((apr_time_t) ic->dt_connect));

    return(0);
}

//**********************************************************
// ibp_config_load_file - Loads the ibp client config from a text file
//**********************************************************

int ibp_config_load_file(ibp_context_t *ic, char *fname, char *section)
{
    tbx_inip_file_t *keyfile;
    int err;

    //* Load the config file
    keyfile = tbx_inip_file_read(fname);
    if (keyfile == NULL) {
        log_printf(0, "Error parsing config file! file=%s\n", fname);
        return(-1);
    }

    err = ibp_config_load(ic, keyfile, section);

    tbx_inip_destroy(keyfile);

    return(err);
}


//**********************************************************
// default_ibp_config - Sets the default ibp config options
//**********************************************************

void default_ibp_config(ibp_context_t *ic)
{
    int i;

    tbx_ns_chksum_clear(&(ic->ncs));

    ic->tcpsize = 0;
    ic->min_idle = apr_time_make(30, 0);
    ic->min_threads = 1;
    ic->max_threads = 4;
    ic->max_connections = 128;
    ic->rw_new_command = 10*1024;
    ic->other_new_command = 100*1024;
    ic->max_workload = 10*1024*1024;
    ic->max_coalesce = ic->max_workload;
    ic->coalesce_enable = 1;
    ic->max_wait = 30;
    ic->dt_connect = apr_time_from_sec(5);
    ic->wait_stable_time = 15;
    ic->abort_conn_attempts = 4;
    ic->check_connection_interval = 2;
    ic->max_retry = 2;
    ic->transfer_rate = 0;
    ic->rr_size = 4;
    ic->connection_mode = IBP_CMODE_HOST;

    for (i=0; i<=IBP_MAX_NUM_CMDS; i++) {
        ic->cc[i].type = NS_TYPE_SOCK;
    }

    copy_ibp_config(ic);
}


//**********************************************************
//  ibp_context_create - Creates an IBP context
//**********************************************************

ibp_context_t *ibp_context_create()
{
    ibp_context_t *ic = (ibp_context_t *)malloc(sizeof(ibp_context_t));
   FATAL_UNLESS(ic != NULL);
    memset(ic, 0, sizeof(ibp_context_t));

    if (_ibp_context_count == 0) {
        tbx_dnsc_startup();

        ibp_configure_signals();
    }

    ic->pc = gop_hp_context_create(&_ibp_base_portal);

    default_ibp_config(ic);

    apr_pool_create(&(ic->mpool), NULL);

    if (_ibp_context_count == 0) {
        ibp_errno_init();
    }

    _ibp_context_count++;

    tbx_atomic_set(ic->n_ops, 0);

    ic->coalesced_stacks = tbx_pc_new("ibp_coalesced_stacks", 50, sizeof(rw_coalesce_t), NULL, rwc_stacks_new, rwc_stacks_free);
    ic->coalesced_gop_stacks = tbx_pc_new("ibp_coalesced_gop_stacks", 50, sizeof(rwc_gop_stack_t), NULL, rwc_gop_stack_new, rwc_gop_stack_free);
    ic->coalesced_ops = tbx_list_create(0, &tbx_list_string_compare, tbx_list_string_dup, tbx_list_simple_free, tbx_list_no_data_free);
    apr_thread_mutex_create(&(ic->lock), APR_THREAD_MUTEX_DEFAULT, ic->mpool);

    return(ic);
}


//**********************************************************
//  ibp_context_destroy - Shuts down the IBP subsystem
//**********************************************************

void ibp_context_destroy(ibp_context_t *ic)
{
    log_printf(15, "ibp_context_destroy: Shutting down! count=%d\n", _ibp_context_count);

    gop_hp_shutdown(ic->pc);
    gop_hp_context_destroy(ic->pc);

    tbx_pc_destroy(ic->coalesced_stacks);
    tbx_pc_destroy(ic->coalesced_gop_stacks);
    tbx_list_destroy(ic->coalesced_ops);

    apr_thread_mutex_destroy(ic->lock);

    apr_pool_destroy(ic->mpool);

    _ibp_context_count--;
    if (_ibp_context_count == 0) {
        tbx_dnsc_shutdown();
    }

    free(ic);
}
