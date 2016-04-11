/*
Advanced Computing Center for Research and Education Proprietary License
Version 1.0 (April 2006)

Copyright (c) 2006, Advanced Computing Center for Research and Education,
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

//*************************************************************
// ibp_op.h - Header defining I/O structs and operations
//*************************************************************

#ifndef __IBP_OP_H_
#define __IBP_OP_H_

#include <apr_time.h>
#include <apr_pools.h>
#include "stack.h"
#include "network.h"
#include "opque.h"
#include "host_portal.h"
#include "ibp_types.h"
#include "transfer_buffer.h"
#include "pigeon_coop.h"
#include "list.h"
#include "atomic_counter.h"
#include "iniparse.h"

#ifdef __cplusplus
extern "C" {
#endif

#define ERR_RETRY_DEADSOCKET 0 //** Used as another IBP error
#define IBP_READ IBP_LOAD
#define IBP_ST_STATS   4     //** Get depot stats
#define IBP_ST_VERSION 5     //** This is for the get_version() command
#define IBP_ST_RES   3         //** Used to get the list or resources from the depot
#define MAX_KEY_SIZE 256
#define MAX_HOST_SIZE 1024
#define IBP_CHKSUM_BLOCKSIZE 65536
 
#define IBP_CMODE_HOST 0
#define IBP_CMODE_RID  1
#define IBP_CMODE_ROUND_ROBIN  2
 
 
//typedef int64_t ibp_off_t;
 
typedef struct {
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
atomic_int_t rr_count; //** RR counter
ibp_connect_context_t cc[IBP_MAX_NUM_CMDS+1];  //** Default connection contexts for EACH command
ns_chksum_t ncs;
portal_context_t *pc;
pigeon_coop_t *coalesced_stacks;
pigeon_coop_t *coalesced_gop_stacks;
list_t   *coalesced_ops;  //** Ops available for coalescing go here
apr_thread_mutex_t *lock;
apr_pool_t *mpool;
atomic_int_t n_ops;
} ibp_context_t;
 
 
//extern Hportal_context_t *_hpc_config;
//extern ibp_config_t *_ibp_config;
 
typedef struct {    //** IBP_VALIDATE_CHKSUM
ibp_cap_t *cap;
char       key[MAX_KEY_SIZE];
char       typekey[MAX_KEY_SIZE];
int correct_errors;
int *n_bad_blocks;
} ibp_op_validate_chksum_t;
 
typedef struct {   //** IBP_GET_CHKSUM
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
} ibp_op_get_chksum_t;
 
typedef struct {
ibp_iovec_t *iovec;
tbuffer_t *buffer;
ibp_off_t size;
ibp_off_t boff;
int n_iovec;
ibp_iovec_t iovec_single;
} ibp_rw_buf_t;
 
typedef struct {  //** Read/Write operation 
ibp_cap_t *cap;
char       key[MAX_KEY_SIZE];
char       typekey[MAX_KEY_SIZE];
//   char *buf;
//   ibp_off_t offset;
//   ibp_off_t size;
//   ibp_off_t boff;
//   ibp_iovec_t *iovec;
//   int   n_iovec;
//   tbuffer_t *buffer;
int rw_mode;
int n_ops;
int n_iovec_total;
ibp_off_t size;
ibp_rw_buf_t **rwbuf;
ibp_rw_buf_t *bs_ptr;
pigeon_coop_hole_t rwcg_pch;
ibp_rw_buf_t buf_single;
} ibp_op_rw_t;
 
typedef struct { //** MERGE allocoation op
char mkey[MAX_KEY_SIZE];      //** Master key
char mtypekey[MAX_KEY_SIZE];
char ckey[MAX_KEY_SIZE];      //** Child key
char ctypekey[MAX_KEY_SIZE];
} ibp_op_merge_alloc_t;
 
typedef struct {  //**Allocate operation
ibp_off_t size;
ibp_off_t offset;                //** ibp_alias_allocate
int   duration;               //** ibp_alias_allocate
int   disk_chksum_type;            //** ibp_*ALLOCATE_CHKSUM
ibp_off_t  disk_blocksize;          //** IBP_*ALLOCATE_CHKSUM
char       key[MAX_KEY_SIZE];      //** ibp_rename/alias_allocate
char       typekey[MAX_KEY_SIZE];  //** ibp_rename/alias_allocate
ibp_cap_t *mcap;         //** This is just used for ibp_rename/ibp_split_allocate
ibp_capset_t *caps;
ibp_depot_t *depot;
ibp_attributes_t *attr;
} ibp_op_alloc_t;
 
typedef struct {  //** modify count and PROBE  operation
int       cmd;    //** IBP_MANAGE or IBP_ALIAS_MANAGE
ibp_cap_t *cap;
char       mkey[MAX_KEY_SIZE];     //** USed for ALIAS_MANAGE
char       mtypekey[MAX_KEY_SIZE]; //** USed for ALIAS_MANAGE
char       key[MAX_KEY_SIZE];
char       typekey[MAX_KEY_SIZE];
int        mode;
int        captype;
ibp_capstatus_t *probe;
ibp_alias_capstatus_t *alias_probe;
} ibp_op_probe_t;
 
typedef struct {  //** modify Allocation operation
ibp_cap_t *cap;
char       mkey[MAX_KEY_SIZE];     //** USed for ALIAS_MANAGE
char       mtypekey[MAX_KEY_SIZE]; //** USed for ALIAS_MANAGE
char       key[MAX_KEY_SIZE];
char       typekey[MAX_KEY_SIZE];
ibp_off_t     offset;    //** IBP_ALIAS_MANAGE
ibp_off_t     size;
int        duration;
int        reliability;
} ibp_op_modify_alloc_t;
 
typedef struct {  //** depot depot copy operations
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
} ibp_op_copy_t;
 
typedef struct {  //** Modify a depot/RID settings
ibp_depot_t *depot;
char *password;
ibp_off_t max_hard;
ibp_off_t max_soft;
apr_time_t max_duration;
} ibp_op_depot_modify_t;
 
typedef struct {  //** Modify a depot/RID settings
ibp_depot_t *depot;
char *password;
ibp_depotinfo_t *di;
} ibp_op_depot_inq_t;
 
typedef struct {  //** Get the depot version information
ibp_depot_t *depot;
char *buffer;
int buffer_size;
} ibp_op_version_t;
 
typedef struct {  //** Get a list of RID's for a depot
ibp_depot_t *depot;
ibp_ridlist_t *rlist;
} ibp_op_rid_inq_t;
 
typedef struct _ibp_op_s { //** Individual IO operation
ibp_context_t *ic;
op_generic_t gop;
op_data_t dop;
Stack_t *hp_parent;  //** Only used for RW coalescing
int primary_cmd;//** Primary sync IBP command family
int sub_cmd;    //** sub command, if applicable
ns_chksum_t ncs;  //** chksum associated with the command
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
};
 
} ibp_op_t;
 
 
#define ibp_get_gop(a) &((a)->gop)
#define ibp_get_iop(a) (a)->op->priv
#define ibp_reset_iop(a) gop_reset(ibp_get_gop((a)))
 
//** ibp_op.c **
void ibp_op_set_cc(op_generic_t *gop, ibp_connect_context_t *cc);
int ibp_cc_type(ibp_connect_context_t *cc);
void ibp_op_set_ncs(op_generic_t *gop, ns_chksum_t *ncs);
//void ibp_op_callback_append(op_generic_t *gop, callback_t *cb);
 
void init_ibp_op(ibp_context_t *ic, ibp_op_t *op);
ibp_op_t *new_ibp_op(ibp_context_t *ic);
void init_ibp_base_op(ibp_op_t *op, char *logstr, int timeout, int workload, char *hostport,
int cmp_size, int primary_cmd, int sub_cmd);
 
void set_ibp_rw_op(ibp_op_t *op, int rw_type, ibp_cap_t *cap, ibp_off_t offset, tbuffer_t *buffer, ibp_off_t boff, ibp_off_t len, int timeout);
op_generic_t *new_ibp_rw_op(ibp_context_t *ic, int rw_type, ibp_cap_t *cap, ibp_off_t offset, tbuffer_t *buffer, ibp_off_t boff, ibp_off_t len, int timeout);
 
op_generic_t *new_ibp_read_op(ibp_context_t *ic, ibp_cap_t *cap, ibp_off_t offset, tbuffer_t *buffer, ibp_off_t boff, ibp_off_t len, int timeout);
void set_ibp_read_op(ibp_op_t *op, ibp_cap_t *cap, ibp_off_t offset, tbuffer_t *buffer, ibp_off_t boff, ibp_off_t len, int timeout);
 
void set_ibp_vec_read_op(ibp_op_t *op, ibp_cap_t *cap, int n_vec, ibp_iovec_t *vec, tbuffer_t *buffer, ibp_off_t boff, ibp_off_t len, int timeout);
op_generic_t *new_ibp_vec_read_op(ibp_context_t *ic, ibp_cap_t *cap, int n_vec, ibp_iovec_t *vec, tbuffer_t *buffer, ibp_off_t boff, ibp_off_t len, int timeout);
 
void set_ibp_vec_write_op(ibp_op_t *op, ibp_cap_t *cap, int n_iovec, ibp_iovec_t *iovec, tbuffer_t *buffer, ibp_off_t boff, ibp_off_t len, int timeout);
op_generic_t *new_ibp_vec_write_op(ibp_context_t *ic, ibp_cap_t *cap, int n_iovec, ibp_iovec_t *iovec, tbuffer_t *buffer, ibp_off_t boff, ibp_off_t len, int timeout);
 
op_generic_t *new_ibp_write_op(ibp_context_t *ic, ibp_cap_t *cap, ibp_off_t offset, tbuffer_t *buffer, ibp_off_t boff, ibp_off_t len, int timeout);
void set_ibp_write_op(ibp_op_t *op, ibp_cap_t *cap, ibp_off_t offset, tbuffer_t *buffer, ibp_off_t boff, ibp_off_t len, int timeout);
op_generic_t *new_ibp_append_op(ibp_context_t *ic, ibp_cap_t *cap, tbuffer_t *buffer, ibp_off_t boff, ibp_off_t len, int timeout);
void set_ibp_append_op(ibp_op_t *op, ibp_cap_t *cap, tbuffer_t *buffer, ibp_off_t boff, ibp_off_t len, int timeout);
 
op_generic_t *new_ibp_copyappend_op(ibp_context_t *ic, int ns_type, char *path, ibp_cap_t *srccap, ibp_cap_t *destcap, ibp_off_t src_offset, ibp_off_t size,
int src_timeout, int  dest_timeout, int dest_client_timeout);
void set_ibp_copyappend_op(ibp_op_t *op, int ns_type, char *path, ibp_cap_t *srccap, ibp_cap_t *destcap, ibp_off_t src_offset, ibp_off_t size,
int src_timeout, int  dest_timeout, int dest_client_timeout);
void set_ibp_copy_op(ibp_op_t *op, int mode, int ns_type, char *path, ibp_cap_t *srccap, ibp_cap_t *destcap,
ibp_off_t src_offset, ibp_off_t dest_offset, ibp_off_t size, int src_timeout, int  dest_timeout,
int dest_client_timeout);
op_generic_t *new_ibp_copy_op(ibp_context_t *ic, int mode, int ns_type, char *path, ibp_cap_t *srccap, ibp_cap_t *destcap,
ibp_off_t src_offset, ibp_off_t dest_offset, ibp_off_t size, int src_timeout,
int  dest_timeout, int dest_client_timeout);
 
op_generic_t *new_ibp_alloc_op(ibp_context_t *ic, ibp_capset_t *caps, ibp_off_t size, ibp_depot_t *depot, ibp_attributes_t *attr,
int disk_cs_type, ibp_off_t disk_blocksize, int timeout);
void set_ibp_alloc_op(ibp_op_t *op, ibp_capset_t *caps, ibp_off_t size, ibp_depot_t *depot, ibp_attributes_t *attr,
int disk_cs_type, ibp_off_t disk_blocksize, int timeout);
void set_ibp_split_alloc_op(ibp_op_t *op, ibp_cap_t *mcap, ibp_capset_t *caps, ibp_off_t size,
ibp_attributes_t *attr, int disk_cs_type, ibp_off_t disk_blocksize, int timeout);
op_generic_t *new_ibp_merge_alloc_op(ibp_context_t *ic, ibp_cap_t *mcap, ibp_cap_t *ccap,
int timeout);
void set_ibp_merge_alloc_op(ibp_op_t *op, ibp_cap_t *mcap, ibp_cap_t *ccap, int timeout);
op_generic_t *new_ibp_split_alloc_op(ibp_context_t *ic, ibp_cap_t *mcap, ibp_capset_t *caps, ibp_off_t size,
ibp_attributes_t *attr, int disk_cs_type, ibp_off_t disk_blocksize, int timeout);
void set_ibp_alias_alloc_op(ibp_op_t *op, ibp_capset_t *caps, ibp_cap_t *mcap, ibp_off_t offset, ibp_off_t size,
int duration, int timeout);
op_generic_t *new_ibp_alias_alloc_op(ibp_context_t *ic, ibp_capset_t *caps, ibp_cap_t *mcap, ibp_off_t offset, ibp_off_t size,
int duration, int timeout);
op_generic_t *new_ibp_rename_op(ibp_context_t *ic, ibp_capset_t *caps, ibp_cap_t *mcap, int timeout);
void set_ibp_rename_op(ibp_op_t *op, ibp_capset_t *caps, ibp_cap_t *mcap, int timeout);
op_generic_t *new_ibp_remove_op(ibp_context_t *ic, ibp_cap_t *cap, int timeout);
void set_ibp_remove_op(ibp_op_t *op, ibp_cap_t *cap, int timeout);
void set_ibp_alias_remove_op(ibp_op_t *op, ibp_cap_t *cap, ibp_cap_t *mcap, int timeout);
op_generic_t *new_ibp_alias_remove_op(ibp_context_t *ic, ibp_cap_t *cap, ibp_cap_t *mcap, int timeout);
op_generic_t *new_ibp_modify_count_op(ibp_context_t *ic, ibp_cap_t *cap, int mode, int captype, int timeout);
void set_ibp_modify_count_op(ibp_op_t *op, ibp_cap_t *cap, int mode, int captype, int timeout);
void set_ibp_alias_modify_count_op(ibp_op_t *op, ibp_cap_t *cap, ibp_cap_t *mcap, int mode, int captype, int timeout);
op_generic_t *new_ibp_alias_modify_count_op(ibp_context_t *ic, ibp_cap_t *cap, ibp_cap_t *mcap, int mode, int captype, int timeout);
void set_ibp_modify_alloc_op(ibp_op_t *op, ibp_cap_t *cap, ibp_off_t size, int duration, int reliability, int timeout);
op_generic_t *new_ibp_modify_alloc_op(ibp_context_t *ic, ibp_cap_t *cap, ibp_off_t size, int duration, int reliability, int timeout);
 
op_generic_t *new_ibp_truncate_op(ibp_context_t *ic, ibp_cap_t *cap, ibp_off_t size, int timeout);
void set_ibp_truncate_op(ibp_op_t *op, ibp_cap_t *cap, ibp_off_t size, int timeout);
 
op_generic_t *new_ibp_probe_op(ibp_context_t *ic, ibp_cap_t *cap, ibp_capstatus_t *probe, int timeout);
void set_ibp_probe_op(ibp_op_t *op, ibp_cap_t *cap, ibp_capstatus_t *probe, int timeout);
void set_ibp_alias_probe_op(ibp_op_t *op, ibp_cap_t *cap, ibp_alias_capstatus_t *probe, int timeout);
op_generic_t *new_ibp_alias_probe_op(ibp_context_t *ic, ibp_cap_t *cap, ibp_alias_capstatus_t *probe, int timeout);
void set_ibp_depot_modify_op(ibp_op_t *op, ibp_depot_t *depot, char *password, ibp_off_t hard, ibp_off_t soft,
int duration, int timeout);
op_generic_t *new_ibp_depot_modify_op(ibp_context_t *ic, ibp_depot_t *depot, char *password, ibp_off_t hard, ibp_off_t soft,
int duration, int timeout);
void set_ibp_alias_modify_alloc_op(ibp_op_t *op, ibp_cap_t *cap, ibp_cap_t *mcap, ibp_off_t offset, ibp_off_t size, int duration,
int timeout);
op_generic_t *new_ibp_alias_modify_alloc_op(ibp_context_t *ic, ibp_cap_t *cap, ibp_cap_t *mcap, ibp_off_t offset, ibp_off_t size, int duration,
int timeout);
 
void set_ibp_depot_inq_op(ibp_op_t *op, ibp_depot_t *depot, char *password, ibp_depotinfo_t *di, int timeout);
op_generic_t *new_ibp_depot_inq_op(ibp_context_t *ic, ibp_depot_t *depot, char *password, ibp_depotinfo_t *di, int timeout);
void set_ibp_version_op(ibp_op_t *op, ibp_depot_t *depot, char *buffer, int buffer_size, int timeout);
op_generic_t *new_ibp_version_op(ibp_context_t *ic, ibp_depot_t *depot, char *buffer, int buffer_size, int timeout);
void set_ibp_query_resources_op(ibp_op_t *op, ibp_depot_t *depot, ibp_ridlist_t *rlist, int timeout);
op_generic_t *new_ibp_query_resources_op(ibp_context_t *ic, ibp_depot_t *depot, ibp_ridlist_t *rlist, int timeout);
 
void free_ibp_op(ibp_op_t *iop);
void finalize_ibp_op(ibp_op_t *iop);
int ibp_op_status(ibp_op_t *op);
int ibp_op_id(ibp_op_t *op);
 
//** IBP_VALDIATE_CHKSUM
void set_ibp_validate_chksum_op(ibp_op_t *op, ibp_cap_t *mcap, int correct_errors, int *n_bad_blocks, int timeout);
op_generic_t *new_ibp_validate_chksum_op(ibp_context_t *ic, ibp_cap_t *mcap, int correct_errors, int *n_bad_blocks, int timeout);
 
//** IBP_GET_CHKSUM
void set_ibp_get_chksum_op(ibp_op_t *op, ibp_cap_t *mcap, int chksum_info_only,
int *cs_type, int *cs_size, ibp_off_t *blocksize, ibp_off_t *nblocks, ibp_off_t *n_chksumbytes, char *buffer, ibp_off_t bufsize,
int timeout);
op_generic_t *new_ibp_get_chksum_op(ibp_context_t *ic, ibp_cap_t *mcap, int chksum_info_only,
int *cs_type, int *cs_size, ibp_off_t *blocksize, ibp_off_t *nblocks, ibp_off_t *n_chksumbytes, char *buffer, ibp_off_t bufsize,
int timeout);
 
 
 
//** ibp_config.c **
int ibp_rw_submit_coalesce(Stack_t *stack, Stack_ele_t *ele);
int ibp_rw_coalesce(op_generic_t *gop);
int ibp_set_chksum(ibp_context_t *ic, ns_chksum_t *ncs);
void ibp_get_chksum(ibp_context_t *ic, ns_chksum_t *ncs);
void ibp_set_abort_attempts(ibp_context_t *ic, int n);
int  ibp_get_abort_attempts(ibp_context_t *ic);
void ibp_set_tcpsize(ibp_context_t *ic, int n);
int  ibp_get_tcpsize(ibp_context_t *ic);
void ibp_set_min_depot_threads(ibp_context_t *ic, int n);
int  ibp_get_min_depot_threads(ibp_context_t *ic);
void ibp_set_max_depot_threads(ibp_context_t *ic, int n);
int  ibp_get_max_depot_threads(ibp_context_t *ic);
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
void ibp_set_read_cc(ibp_context_t *ic, ibp_connect_context_t *cc);
void ibp_set_write_cc(ibp_context_t *ic, ibp_connect_context_t *cc);
void ibp_set_transfer_rate(ibp_context_t *ic, double rate);
double ibp_get_transfer_rate(ibp_context_t *ic);
 
int ibp_load_config(ibp_context_t *ic, inip_file_t *ifd, char *section);
int ibp_load_config_file(ibp_context_t *ic, char *fname, char *section);
//void set_ibp_config(ibp_config_t *cfg);
void default_ibp_config(ibp_context_t *ic);
ibp_context_t *ibp_create_context();
void ibp_destroy_context(ibp_context_t *ic);
 
//*** ibp_sync.c ***
int ibp_sync_command(ibp_op_t *op);
unsigned long int IBP_phoebus_copy(char *path, ibp_cap_t *srccap, ibp_cap_t *destcap, ibp_timer_t  *src_timer, ibp_timer_t *dest_timer,
ibp_off_t size, ibp_off_t offset);
void destroy_ibp_sync_context();
void set_ibp_sync_context(ibp_context_t *ic)
;
//**** ibp_version.c *******
char *ibp_version();
 
//******* ibp_errno.c ********
void ibp_errno_init();
 
#ifdef __cplusplus
}
#endif
 
 
#endif
 
 