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
    apr_time_t dt_connect;  //** How long to wait when making a new connection
    int rw_new_command;     //** byte "cost" of just the command portion excluding any data transfer for a Read/Write command
    int other_new_command;     //** byte "cost" of the non-R/W commands
    int coalesce_enable; //** Enable R/W coaleascing
    int64_t max_coalesce;    //** MAx amount of data that can be coalesced
    int max_retry;        //** Max number of times to retry a command before failing.. only for dead socket retries
    int coalesce_ops;     //** If 1 then Read and Write ops for the same allocation are coalesced
    int connection_mode;  //** Connection mode
    int rr_size;          //** Round robin connection count. Only used ir cmode = RR
    double transfer_rate; //** Transfer rate in bytes/sec used for calculating timeouts.  Set to 0 to disable function
    tbx_atomic_int_t rr_count; //** RR counter
    ibp_connect_context_t cc[IBP_MAX_NUM_CMDS+1];  //** Default connection contexts for EACH command
    tbx_ns_chksum_t ncs;
    gop_portal_context_t *pc;
    tbx_pc_t *coalesced_stacks;
    tbx_pc_t *coalesced_gop_stacks;
    tbx_list_t   *coalesced_ops;  //** Ops available for coalescing go here
    apr_thread_mutex_t *lock;
    apr_pool_t *mpool;
    tbx_atomic_int_t n_ops;
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
gop_op_generic_t *ibp_context_chksum_get_gop(ibp_context_t *ic, ibp_cap_t *mcap, int chksum_info_only,
        int *cs_type, int *cs_size, ibp_off_t *blocksize, ibp_off_t *nblocks, ibp_off_t *n_chksumbytes, char *buffer, ibp_off_t bufsize,
        int timeout);

//** ibp_config.c **
int ibp_rw_submit_coalesce(tbx_stack_t *stack, tbx_stack_ele_t *ele);
int ibp_rw_coalesce(gop_op_generic_t *gop);

//void set_ibp_config(ibp_config_t *cfg);
void default_ibp_config(ibp_context_t *ic);

void destroy_ibp_sync_context();

//**** ibp_version.c *******

//******* ibp_errno.c ********
void ibp_errno_init();

struct ibp_op_validate_chksum_t {    //** IBP_VALIDATE_CHKSUM
    ibp_cap_t *cap;
    char       key[MAX_KEY_SIZE];
    char       typekey[MAX_KEY_SIZE];
    int correct_errors;
    int *n_bad_blocks;
};

struct ibp_op_get_chksum_t {   //** IBP_GET_CHKSUM
    ibp_cap_t *cap;
    char       key[MAX_KEY_SIZE];
    char       typekey[MAX_KEY_SIZE];
    int chksum_info_only;
    ibp_off_t bufsize;
    char *buffer;
    int *cs_type;
    int *cs_size;
    ibp_off_t *blocksize;
    ibp_off_t *nblocks;
    ibp_off_t *n_chksumbytes;
};

struct ibp_rw_buf_t {
    ibp_tbx_iovec_t *iovec;
    tbx_tbuf_t *buffer;
    ibp_off_t size;
    ibp_off_t boff;
    int n_iovec;
    ibp_tbx_iovec_t iovec_single;
};

struct ibp_op_rw_t {  //** Read/Write operation
    ibp_cap_t *cap;
    char       key[MAX_KEY_SIZE];
    char       typekey[MAX_KEY_SIZE];
    int rw_mode;
    int n_ops;
    int n_tbx_iovec_total;
    ibp_off_t size;
    ibp_rw_buf_t **rwbuf;
    ibp_rw_buf_t *bs_ptr;
    tbx_pch_t rwcg_pch;
    ibp_rw_buf_t buf_single;
};

struct ibp_op_merge_alloc_t { //** MERGE allocoation op
    char mkey[MAX_KEY_SIZE];      //** Master key
    char mtypekey[MAX_KEY_SIZE];
    char ckey[MAX_KEY_SIZE];      //** Child key
    char ctypekey[MAX_KEY_SIZE];
};

struct ibp_op_alloc_t {  //**Allocate operation
    ibp_off_t size;
    ibp_off_t offset;                //** ibp_proxy_allocate
    int   duration;               //** ibp_proxy_allocate
    int   disk_chksum_type;            //** ibp_*ALLOCATE_CHKSUM
    ibp_off_t  disk_blocksize;          //** IBP_*ALLOCATE_CHKSUM
    char       key[MAX_KEY_SIZE];      //** ibp_rename/proxy_allocate
    char       typekey[MAX_KEY_SIZE];  //** ibp_rename/proxy_allocate
    ibp_cap_t *mcap;         //** This is just used for ibp_rename/ibp_split_allocate
    ibp_capset_t *caps;
    ibp_depot_t *depot;
    ibp_attributes_t *attr;
};

struct ibp_op_probe_t {  //** modify count and PROBE  operation
    int       cmd;    //** IBP_MANAGE or IBP_PROXY_MANAGE
    ibp_cap_t *cap;
    char       mkey[MAX_KEY_SIZE];     //** USed for PROXY_MANAGE
    char       mtypekey[MAX_KEY_SIZE]; //** USed for PROXY_MANAGE
    char       key[MAX_KEY_SIZE];
    char       typekey[MAX_KEY_SIZE];
    int        mode;
    int        captype;
    ibp_capstatus_t *probe;
    ibp_proxy_capstatus_t *proxy_probe;
};

struct ibp_op_modify_alloc_t {  //** modify Allocation operation
    ibp_cap_t *cap;
    char       mkey[MAX_KEY_SIZE];     //** USed for PROXY_MANAGE
    char       mtypekey[MAX_KEY_SIZE]; //** USed for PROXY_MANAGE
    char       key[MAX_KEY_SIZE];
    char       typekey[MAX_KEY_SIZE];
    ibp_off_t     offset;    //** IBP_PROXY_MANAGE
    ibp_off_t     size;
    int        duration;
    int        reliability;
};
struct ibp_op_copy_t {  //** depot depot copy operations
    char      *path;       //** Phoebus path or NULL for default
    ibp_cap_t *srccap;
    ibp_cap_t *destcap;
    char       src_key[MAX_KEY_SIZE];
    char       src_typekey[MAX_KEY_SIZE];
    ibp_off_t  src_offset;
    ibp_off_t  dest_offset;
    ibp_off_t  len;
    int        dest_timeout;
    int        dest_client_timeout;
    int        ibp_command;
    int        ctype;
};

struct ibp_op_depot_modify_t {  //** Modify a depot/RID settings
    ibp_depot_t *depot;
    char *password;
    ibp_off_t max_hard;
    ibp_off_t max_soft;
    apr_time_t max_duration;
};

struct ibp_op_depot_inq_t {  //** Modify a depot/RID settings
    ibp_depot_t *depot;
    char *password;
    ibp_depotinfo_t *di;
};

struct ibp_op_version_t {  //** Get the depot version information
    ibp_depot_t *depot;
    char *buffer;
    int buffer_size;
};

struct ibp_op_rid_inq_t {  //** Get a list of RID's for a depot
    ibp_depot_t *depot;
    ibp_ridlist_t *rlist;
};

struct ibp_op_t { //** Individual IO operation
    ibp_context_t *ic;
    gop_op_generic_t gop;
    gop_op_data_t dop;
    tbx_stack_t *hp_parent;  //** Only used for RW coalescing
    int primary_cmd;//** Primary sync IBP command family
    int sub_cmd;    //** sub command, if applicable
    tbx_ns_chksum_t ncs;  //** chksum associated with the command
    union {         //** Holds the individual commands options
        ibp_op_validate_chksum_t validate_op;
        ibp_op_get_chksum_t      get_chksum_op;
        ibp_op_alloc_t  alloc_op;
        ibp_op_merge_alloc_t  merge_op;
        ibp_op_probe_t  probe_op;
        ibp_op_rw_t     rw_op;
        ibp_op_copy_t   copy_op;
        ibp_op_depot_modify_t depot_modify_op;
        ibp_op_depot_inq_t depot_inq_op;
        ibp_op_modify_alloc_t mod_alloc_op;
        ibp_op_rid_inq_t   rid_op;
        ibp_op_version_t   ver_op;
    } ops;
};


#ifdef __cplusplus
}
#endif


#endif


