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

//*********************************************************************
//*********************************************************************

#define _log_module_index 212
#include "zsock_config.h"

static portal_fn_t _zsock_base_portal = {
    .dup_connect_context = _zsock_dup_connect_context,
    .destroy_connect_context = _zsock_destroy_connect_context,
    .connect = _zsock_connect,
    .close_connection = close_netstream,
    .sort_tasks = default_sort_ops,
    .submit = _zsock_submit_op
};

int _zsock_context_count = 0; //** Is it necessary to have this var?
inip_element_t *_find_group_key(inip_file_t *inip, const char *group_name, const char *key);

//**********************************************************************
// _zsock_dup_connect_connect - Copy an ZMQ connect_context structure
//**********************************************************************

void *_zsock_dup_connect_context(void *connect_context)
{
    zsock_connect_context_t *cc = (zsock_connect_context_t *)connect_context;
    zsock_connect_context_t *ccdup;

    if (cc == NULL) return NULL;
    ccdup = (zsock_connect_context_t *)malloc(sizeof(zsock_connect_context_t));
    assert(ccdup != NULL);

    *ccdup = *cc;

    return (void*)ccdup;
}


//*********************************************************************
//_zsock_destroy_connect_context - Destroy an ZMQ connect_context structure
//*********************************************************************

void _zsock_destroy_connect_context(void *connect_context)
{
    if (connect_context != NULL) {
        zsock_connect_context_t * cc = (zsock_connect_context_t *)connect_context;
        free(cc);
    }
}

//**********************************************************************
// _zsock_connect - Make a ZMQ connection to remote host
//**********************************************************************

int _zsock_connect(NetStream_t *ns, void *connect_context, char *host, int port, Net_timeout_t timeout)
{
    zsock_connect_context_t *cc = (zsock_connect_context_t *)connect_context;

    if (cc != NULL) {
        switch(cc->conn_type) {
        case NS_TYPE_ZSOCK:
            ns_config_zsock(ns, cc->sock_type, cc->prtcl, (zsocket_opt_t *)cc->arg);
            break;
        default:
            log_printf(0, "_zsock_connect: Invalid conn type=%d Exiting!\n", cc->conn_type);
        }
    } else {
        ns_config_zsock(ns, cc->sock_type, cc->prtcl, (zsocket_opt_t *)cc->arg);
    }

    int rc = net_connect(ns, host, port, timeout);

    return rc;
}

//***********************************************************************
// _zsock_submit_op - Submit a zsock OP
//***********************************************************************

void _zsock_submit_op(void *arg, op_generic_t *gop)
{
    portal_context_t *pc = gop->base.pc;

    log_printf(15, "_zsock_submit_op: hpc=%p hpc->table=%p gop=%p gid=%d\n", pc, pc->table, gop, gop_id(gop));

    if (gop->base.execution_mode == OP_EXEC_DIRECT) {
        submit_hp_direct_op(pc, gop);
    } else {
        submit_hp_que_op(pc, gop);
    }
}

//************************************************************************
// default_zsock_config
//************************************************************************

void default_zsock_config(zsock_context_t *zc)
{
    zc->min_idle = apr_time_make(30, 0);        //** Connection minimum idle time before disconnecting
    zc->min_threads = 1;     			//** Min and max threads allowed
    zc->max_threads = 4;     			//** Max number of simultaneous connection
    zc->max_connections = 128; 			//** Max number of connections across all connections
    zc->max_wait = 30;         			//** Max time to wait and retry a connection
    zc->wait_stable_time = 15; 			//** Time to wait before opening a new connection for a heavily loaded server
    zc->abort_conn_attempts = 4; 		//** If this many failed connection requests occur in a row we abort
    zc->check_connection_interval = 2;  	//** # of secs to wait between checks if we need more connections
    zc->max_retry = 2;

    int i;

    for (i = 0; i < ZSOCK_MAX_NUM_CMDS; i++) {
        zc->cc[i].conn_type = NS_TYPE_ZSOCK;
    }
}

//************************************************************************
// zsock_create_context - Create a zsock context
//************************************************************************

zsock_context_t *zsock_create_context()
{
    zsock_context_t *zc;
    type_malloc_clear(zc, zsock_context_t, 1);

    assert(apr_wrapper_start() == APR_SUCCESS);

    zc->pc = create_hportal_context(&_zsock_base_portal);

    default_zsock_config(zc);

    apr_pool_create(&(zc->mpool), NULL);
    apr_thread_mutex_create(&(zc->lock), APR_THREAD_MUTEX_DEFAULT, zc->mpool);

    if (_zsock_context_count == 0) {
        dns_cache_init(100);
        init_opque_system();
    }

    _zsock_context_count++;

    atomic_set(zc->n_ops, 0);

    return zc;
}

//************************************************************************
// zsock_destroy_context - Destroy zsock context
//************************************************************************

void zsock_destroy_context(zsock_context_t *zc)
{
    log_printf(0, "zsock_destroy_context: Shutting down! count=%d\n", _zsock_context_count);

    shutdown_hportal(zc->pc);

    destroy_hportal_context(zc->pc);

    apr_thread_mutex_destroy(zc->lock);

    apr_pool_destroy(zc->mpool);

    _zsock_context_count--;

    if (_zsock_context_count == 0) {
        finalize_dns_cache();
        destroy_opque_system();
    }

    apr_wrapper_stop();

    //** Temp put here for testing
    //** Reason is that _zsock_destroy_connect_context only called once for one portal, not ZSOCK_MAX_NUM_CMDS times
    int i;

    for (i = 0; i < ZSOCK_MAX_NUM_CMDS; i++) {
        free(zc->cc[i].prtcl);
        zsock_option_destroy((zsocket_opt_t *)zc->cc[i].arg);
    }

    free(zc);
}

//************************************************************************
// zsock_get_type - Get the zmq socket type
//************************************************************************

int zsock_get_type(char *type)
{
    type = string_trim(type);
    if (strcmp(type, "ZMQ_REQ") == 0) {
        return ZMQ_REQ;
    } else if (strcmp(type, "ZMQ_REP") == 0) {
        return ZMQ_REP;
    } else if (strcmp(type, "ZMQ_DEALER") == 0) {
        return ZMQ_DEALER;
    } else if (strcmp(type, "ZMQ_ROUTER") == 0) {
        return ZMQ_ROUTER;
    } else if (strcmp(type, "ZMQ_PUB") == 0) {
        return ZMQ_PUB;
    } else if (strcmp(type, "ZMQ_SUB") == 0) {
        return ZMQ_SUB;
    } else if (strcmp(type, "ZMQ_PUSH") == 0) {
        return ZMQ_PUSH;
    } else if (strcmp(type, "ZMQ_PULL") == 0) {
        return ZMQ_PULL;
    } else if (strcmp(type, "ZMQ_PAIR") == 0) {
        return ZMQ_PAIR;
    } else {
        return -1;
    }
}

//************************************************************************
// zsock_option_load - Load the socket option for connection
//************************************************************************

void zsock_option_load(inip_file_t *kf, const char *group, zsocket_opt_t *option)
{
    if (_find_group_key(kf, group, "rate")) {
        set_flag(option->flag, RATE);
        option->rate = inip_get_integer(kf, group, "rate", 100);
    }

    if (_find_group_key(kf, group, "multicast_hops")) {
        set_flag(option->flag, MULTICAST_HOPS);
        option->rate = inip_get_integer(kf, group, "multicast_hops", 1);
    }

    if (_find_group_key(kf, group, "router_behavior")) {
        set_flag(option->flag, ROUTER_BEHAVIOR);
        option->router_behavior = inip_get_integer(kf, group, "router_behavior", 0);
    }

    if (_find_group_key(kf, group, "sndhwm")) {
        set_flag(option->flag, SNDHWM);
        option->sndhwm = inip_get_integer(kf, group, "sndhwm", 1000);
    }

    if (_find_group_key(kf, group, "rcvhwm")) {
        set_flag(option->flag, RCVHWM);
        option->rcvhwm = inip_get_integer(kf, group, "rcvhwm", 1000);
    }

    if (_find_group_key(kf, group, "affinity")) {
        set_flag(option->flag, AFFINITY);
        option->affinity = inip_get_integer(kf, group, "affinity", 0);
    }

    if (_find_group_key(kf, group, "recovery_ivl")) {
        set_flag(option->flag, RECOVERY_IVL);
        option->recovery_ivl = inip_get_integer(kf, group, "recovery_ivl", 1);
    }

    if (_find_group_key(kf, group, "sndbuf")) {
        set_flag(option->flag, SNDBUF);
        option->sndbuf = inip_get_integer(kf, group, "sndbuf", 0);
    }

    if (_find_group_key(kf, group, "rcvbuf")) {
        set_flag(option->flag, RCVBUF);
        option->rcvbuf = inip_get_integer(kf, group, "rcvbuf", 0);
    }

    if (_find_group_key(kf, group, "reconnect_ivl")) {
        set_flag(option->flag, RECONNECT_IVL);
        option->reconnect_ivl = inip_get_integer(kf, group, "reconnect_ivl", 100);
    }

    if (_find_group_key(kf, group, "reconnect_ivl_max")) {
        set_flag(option->flag, RECONNECT_IVL_MAX);
        option->reconnect_ivl = inip_get_integer(kf, group, "reconnect_ivl_max", 0);
    }

    if (_find_group_key(kf, group, "backlog")) {
        set_flag(option->flag, BACKLOG);
        option->backlog = inip_get_integer(kf, group, "backlog", 100);
    }

    if (_find_group_key(kf, group, "maxmsgsize")) {
        set_flag(option->flag, MAXMSGSIZE);
        option->maxmsgsize = inip_get_integer(kf, group, "maxmsgsize", -1);
    }

    if (_find_group_key(kf, group, "rcvtimeo")) {
        set_flag(option->flag, RCVTIMEO);
        option->rcvtimeo = inip_get_integer(kf, group, "rcvtimeo", -1);
    }

    if (_find_group_key(kf, group, "sndtimeo")) {
        set_flag(option->flag, SNDTIMEO);
        option->sndtimeo = inip_get_integer(kf, group, "sndtimeo", -1);
    }

    if (_find_group_key(kf, group, "ipv4only")) {
        set_flag(option->flag, IPV4ONLY);
        option->ipv4only = inip_get_integer(kf, group, "ipv4only", 1);
    }

    if (_find_group_key(kf, group, "hwm")) {
        set_flag(option->flag, HWM);
        option->hwm = inip_get_integer(kf, group, "hwm", 1);
    }

    if (_find_group_key(kf, group, "sub_num")) {
        set_flag(option->flag, SUBSCRIBE);
        option->sub_num = inip_get_integer(kf, group, "sub_num", 0);
        char *sub_list = inip_get_string(kf, group, "subscribe", NULL);
        option->subscribe = (char**) malloc(option->sub_num * sizeof(char *));
        option->subscribe[0] = strdup(strtok(sub_list, "|"));
        int i;
        for (i = 1; i < option->sub_num; i++) {
            option->subscribe[i] = strdup(strtok(NULL, "|"));
        }
        free(sub_list);
    }

    if (_find_group_key(kf, group, "unsub_num")) {
        set_flag(option->flag, UNSUBSCRIBE);
        option->unsub_num = inip_get_integer(kf, group, "unsub_num", 0);
        char *unsub_list = inip_get_string(kf, group, "unsubscribe", NULL);
        option->unsubscribe = (char**) malloc(option->unsub_num * sizeof(char *));
        option->unsubscribe[0] = strdup(strtok(unsub_list, "|"));
        int i;
        for (i = 1; i < option->unsub_num; i++) {
            option->unsubscribe[i] = strdup(strtok(NULL, "|"));
        }
        free(unsub_list);
    }

    if (_find_group_key(kf, group, "identity")) {
        set_flag(option->flag, IDENTITY);
        option->identity = inip_get_string(kf, group, "identity", NULL);
    }
}

//************************************************************************
// zsock_cc_load - Load the default connect_context for commands
//************************************************************************

void zsock_cc_load(inip_file_t *kf, zsock_context_t *cfg)
{
    int i;

    for (i = 0; i < ZSOCK_MAX_NUM_CMDS; i++) {
        cfg->cc[i].conn_type = NS_TYPE_ZSOCK;

        char *type = inip_get_string(kf, "zsock_connect", "sock_type", NULL);
        cfg->cc[i].sock_type = zsock_get_type(type);
        assert(cfg->cc[i].sock_type != -1);
        free(type);

        cfg->cc[i].prtcl = inip_get_string(kf, "zsock_connect", "protocol", NULL);

        //** Handle socket options
        zsocket_opt_t *option = zsock_option_create();

        zsock_option_load(kf, "zsock_connect", option);

        cfg->cc[i].arg = (void *)option;
    }
}

//************************************************************************
// copy_zsock_config - Set the zsock config options
//************************************************************************

void copy_zsock_config(zsock_context_t *cfg)
{
    cfg->pc->max_workload = cfg->max_workload;
    cfg->pc->min_idle =  cfg->min_idle;
    cfg->pc->min_threads = cfg->min_threads;
    cfg->pc->max_threads = cfg->max_threads;
    cfg->pc->max_connections = cfg->max_connections;
    cfg->pc->max_wait = cfg->max_wait;
    cfg->pc->wait_stable_time = cfg->wait_stable_time;
    cfg->pc->abort_conn_attempts = cfg->abort_conn_attempts;
    cfg->pc->check_connection_interval = cfg->check_connection_interval;
    cfg->pc->max_retry = cfg->max_retry;
}

//************************************************************************
// zsock_load_config - Load the zsock config
//************************************************************************

int zsock_load_config(zsock_context_t *zc, char *fname, char *section)
{
    inip_file_t *keyfile;

    if (section == NULL) section = "zsock";

    //** Load the config file
    keyfile = inip_read(fname);
    if (keyfile == NULL) {
        log_printf(0, "ibp_load_config:  Error parsing config file! file=%s\n", fname);
        return(-1);
    }

    zc->max_workload = inip_get_integer(keyfile, section, "max_thread_workload", zc->max_workload);
    zc->max_retry = inip_get_integer(keyfile, section, "max_retry", zc->max_retry);
    zc->min_threads = inip_get_integer(keyfile, section, "min_threads", zc->min_threads);
    zc->max_threads = inip_get_integer(keyfile, section, "max_threads", zc->max_threads);
    zc->max_connections = inip_get_integer(keyfile, section, "max_connections", zc->max_connections);
    zc->max_wait = inip_get_integer(keyfile, section, "max_wait", zc->max_wait);
    zc->wait_stable_time = inip_get_integer(keyfile, section, "wait_stable_time", zc->wait_stable_time);
    zc->check_connection_interval = inip_get_integer(keyfile, section, "check_interval", zc->check_connection_interval);

    copy_zsock_config(zc);

    zsock_cc_load(keyfile, zc);

    inip_destroy(keyfile);

    return 0;
}
