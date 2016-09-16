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
//*************************************************************
// ibp_op.h - Header defining I/O structs and operations
//*************************************************************

#ifndef __IBP_OP_H_
#define __IBP_OP_H_

#include <apr_pools.h>
#include <apr_thread_mutex.h>
#include <apr_time.h>
#include <gop/gop.h>
#include <gop/hp.h>
#include <gop/types.h>
#include <ibp/op.h>
#include <ibp/protocol.h>
#include <ibp/visibility.h>
#include <stdint.h>
#include <tbx/atomic_counter.h>
#include <tbx/iniparse.h>
#include <tbx/list.h>
#include <tbx/network.h>
#include <tbx/stack.h>
#include <tbx/transfer_buffer.h>

#include "types.h"

#ifdef __cplusplus
extern "C" {
#endif



#define ERR_RETRY_DEADSOCKET 0 //** Used as another IBP error
#define IBP_READ IBP_LOAD
#define IBP_ST_STATS   4     //** Get depot stats
#define IBP_ST_VERSION 5     //** This is for the get_version() command
#define IBP_ST_RES   3         //** Used to get the list or resources from the depot
#define MAX_HOST_SIZE 1024
#define IBP_CHKSUM_BLOCKSIZE 65536

#define IBP_CMODE_HOST 0
#define IBP_CMODE_RID  1
#define IBP_CMODE_ROUND_ROBIN  2

struct ibp_context_t {
    int tcpsize;         //** TCP R/W buffer size.  If 0 then OS default is used
    int min_idle;        //** Connection minimum idle time before disconnecting
    int min_threads;     //** Min and max threads allowed to a depot
    int max_threads;     //** Max number of simultaneous connection to a depot
    int max_connections; //** Max number of connections across all connections
    apr_time_t dt_connect;  //** How long to wait when making a new connection
    int rw_new_command;     //** byte "cost" of just the command portion excluding any data transfer for a Read/Write command
    int other_new_command;     //** byte "cost" of the non-R/W commands
    int coalesce_enable; //** Enable R/W coaleascing
    int64_t max_workload;    //** Max workload allowed in a given connection
    int64_t max_coalesce;    //** MAx amount of data that can be coalesced
    int max_wait;         //** Max time to wait and retry a connection
    int wait_stable_time; //** Time to wait before opening a new connection for a heavily loaded depot
    int abort_conn_attempts; //** If this many failed connection requests occur in a row we abort
    int check_connection_interval;  //**# of secs to wait between checks if we need more connections to a depot
    int max_retry;        //** Max number of times to retry a command before failing.. only for dead socket retries
    int coalesce_ops;     //** If 1 then Read and Write ops for the same allocation are coalesced
    int connection_mode;  //** Connection mode
    int rr_size;          //** Round robin connection count. Only used ir cmode = RR
    double transfer_rate; //** Transfer rate in bytes/sec used for calculating timeouts.  Set to 0 to disable function
    tbx_atomic_unit32_t rr_count; //** RR counter
    ibp_connect_context_t cc[IBP_MAX_NUM_CMDS+1];  //** Default connection contexts for EACH command
    tbx_ns_chksum_t ncs;
    gop_portal_context_t *pc;
    tbx_pc_t *coalesced_stacks;
    tbx_pc_t *coalesced_gop_stacks;
    tbx_list_t   *coalesced_ops;  //** Ops available for coalescing go here
    apr_thread_mutex_t *lock;
    apr_pool_t *mpool;
    tbx_atomic_unit32_t n_ops;
};



//
// Status/error codes
//
extern gop_op_status_t ibp_success_status;
extern gop_op_status_t ibp_failure_status;
extern gop_op_status_t ibp_retry_status;
extern gop_op_status_t ibp_dead_status;
extern gop_op_status_t ibp_timeout_status;
extern gop_op_status_t ibp_invalid_host_status;
extern gop_op_status_t ibp_cant_connect_status;
extern gop_op_status_t ibp_error_status;

//
// Helper macros
//
#define IBP_OP(NAME, ...) IBP_API gop_op_generic_t * NAME(ibp_context_t *ic, \
                                                      ibp_cap_t *cap, \
                                                      int timeout, \
                                                      __VA_ARGS__);

#define ibp_get_gop(a) &((a)->gop)
#define ibp_reset_iop(a) gop_reset(ibp_get_gop((a)))

//** ibp_op.c **
//void ibp_op_callback_append(gop_op_generic_t *gop, gop_callback_t *cb);

ibp_op_t *new_ibp_op(ibp_context_t *ic);

void init_ibp_base_op(ibp_op_t *op, char *logstr, int timeout, int workload, char *hostport, int cmp_size, int primary_cmd, int sub_cmd);
void set_ibp_rw_gop(ibp_op_t *op, int rw_type, ibp_cap_t *cap, ibp_off_t offset, tbx_tbuf_t *buffer, ibp_off_t boff, ibp_off_t len, int timeout);
void set_ibp_truncate_gop(ibp_op_t *op, ibp_cap_t *cap, ibp_off_t size, int timeout);
void free_ibp_op(ibp_op_t *iop);
void finalize_ibp_op(ibp_op_t *iop);
int ibp_op_status(ibp_op_t *op);
int ibp_op_id(ibp_op_t *op);

//** IBP_VALDIATE_CHKSUM

//** IBP_GET_CHKSUM
gop_op_generic_t *ibp_get_chksum_gop(ibp_context_t *ic, ibp_cap_t *mcap, int chksum_info_only,
        int *cs_type, int *cs_size, ibp_off_t *blocksize, ibp_off_t *nblocks, ibp_off_t *n_chksumbytes, char *buffer, ibp_off_t bufsize,
        int timeout);

//** ibp_config.c **
int ibp_rw_submit_coalesce(tbx_stack_t *stack, tbx_stack_ele_t *ele);
int ibp_rw_coalesce(gop_op_generic_t *gop);
void ibp_get_chksum(ibp_context_t *ic, tbx_ns_chksum_t *ncs);
void ibp_set_abort_attempts(ibp_context_t *ic, int n);
int  ibp_get_abort_attempts(ibp_context_t *ic);
IBP_API int  ibp_tcpsize_get(ibp_context_t *ic);
void ibp_set_min_depot_threads(ibp_context_t *ic, int n);
int  ibp_get_min_depot_threads(ibp_context_t *ic);
IBP_API int  ibp_max_depot_threads_get(ibp_context_t *ic);
void ibp_set_max_connections(ibp_context_t *ic, int n);
int  ibp_get_max_connections(ibp_context_t *ic);
void ibp_set_command_weight(ibp_context_t *ic, int n);
int  ibp_get_command_weight(ibp_context_t *ic);
void ibp_set_max_thread_workload(ibp_context_t *ic, int64_t n);
int64_t  ibp_get_max_thread_workload(ibp_context_t *ic);
void ibp_set_max_coalesce_workload(ibp_context_t *ic, int64_t n);
int64_t  ibp_get_max_coalesce_workload(ibp_context_t *ic);
void ibp_set_wait_stable_time(ibp_context_t *ic, int n);
int  ibp_get_wait_stable_time(ibp_context_t *ic);
void ibp_set_check_interval(ibp_context_t *ic, int n);
int  ibp_get_check_interval(ibp_context_t *ic);
void ibp_set_max_retry(ibp_context_t *ic, int n);
int  ibp_get_max_retry(ibp_context_t *ic);
void ibp_set_transfer_rate(ibp_context_t *ic, double rate);
double ibp_get_transfer_rate(ibp_context_t *ic);

//void set_ibp_config(ibp_config_t *cfg);
void default_ibp_config(ibp_context_t *ic);

void destroy_ibp_sync_context();

//**** ibp_version.c *******

//******* ibp_errno.c ********
void ibp_errno_init();

#ifdef __cplusplus
}
#endif


#endif


