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

/** \file
* Autogenerated public API
*/

#ifndef ACCRE_LIO_EX3_ABSTRACT_H_INCLUDED
#define ACCRE_LIO_EX3_ABSTRACT_H_INCLUDED

#include <apr_hash.h>
#include <apr_thread_cond.h>
#include <gop/tp.h>
#include <lio/lio_visibility.h>
#include <lio/ds.h>
#include <lio/service_manager.h>
#include <lio/ex3_types.h>
#include <lio/ex3_header.h>
#include <lio/rs.h>
#include <tbx/log.h>

#ifdef __cplusplus
extern "C" {
#endif

// Typedefs
typedef struct exnode_t exnode_t;
typedef struct inspect_args_t inspect_args_t;
typedef struct rid_inspect_tweak_t rid_inspect_tweak_t;
typedef struct segment_errors_t segment_errors_t;
typedef struct segment_fn_t segment_fn_t;
typedef struct segment_rw_hints_t segment_rw_hints_t;
typedef struct segment_t segment_t;
typedef segment_t *(segment_create_t)(void *arg);

// Functions
LIO_API op_generic_t *lio_exnode_clone(thread_pool_context_t *tpc, exnode_t *ex, data_attr_t *da, exnode_t **clone_ex, void *arg, int mode, int timeout);
LIO_API exnode_t *lio_exnode_create();
LIO_API segment_t *lio_exnode_default_get(exnode_t *ex);
LIO_API int lio_exnode_deserialize(exnode_t *ex, exnode_exchange_t *exp, service_manager_t *ess);
LIO_API void lio_exnode_destroy(exnode_t *ex);
LIO_API exnode_exchange_t *lio_exnode_exchange_create(int type);
LIO_API void lio_exnode_exchange_destroy(exnode_exchange_t *exp);
LIO_API exnode_exchange_t *lio_exnode_exchange_load_file(char *fname);
LIO_API exnode_exchange_t *lio_exnode_exchange_text_parse(char *text);
LIO_API int lio_exnode_serialize(exnode_t *ex, exnode_exchange_t *exp);
LIO_API op_generic_t *lio_segment_copy(thread_pool_context_t *tpc, data_attr_t *da, segment_rw_hints_t *rw_hints, segment_t *src_seg, segment_t *dest_seg, ex_off_t src_offset, ex_off_t dest_offset, ex_off_t len, ex_off_t bufsize, char *buffer, int do_truncate, int timoeut);
LIO_API int lio_view_insert(exnode_t *ex, segment_t *view);

// Preprocessor constants
#define EX_TEXT             0
#define EX_PROTOCOL_BUFFERS 1

#define CLONE_STRUCTURE       0
#define CLONE_STRUCT_AND_DATA 1

#define INSPECT_QUICK_CHECK   1
#define INSPECT_SCAN_CHECK    2
#define INSPECT_FULL_CHECK    3
#define INSPECT_QUICK_REPAIR  4
#define INSPECT_SCAN_REPAIR   5
#define INSPECT_FULL_REPAIR   6
#define INSPECT_SOFT_ERRORS   7
#define INSPECT_HARD_ERRORS   8
#define INSPECT_MIGRATE       9
#define INSPECT_WRITE_ERRORS 10

#define INSPECT_FORCE_REPAIR          128   //** Make the repair even if it leads to data loss
#define INSPECT_COMMAND_BITS 15

#define SEG_SM_CREATE "segment_create"

#define INSPECT_RESULT_FULL_CHECK      512    //** Full byte-level check performed
#define INSPECT_RESULT_SOFT_ERROR     1024   //** Soft errors found
#define INSPECT_RESULT_HARD_ERROR     2048   //** Hard errors found

#define INSPECT_SOFT_ERROR_FAIL       256   //** Treat soft errors as hard
#define INSPECT_FORCE_RECONSTRUCTION  512   //** Don't use depot-depot copies for data movement.  Instead use reconstruction
#define INSPECT_FAIL_ON_ERROR        1024   //** Kick out if an unrecoverable error is hit
#define INSPECT_FIX_READ_ERROR       2048   //** Treat read errors as bad blocks for repair
#define INSPECT_FIX_WRITE_ERROR      4096   //** Treat write errors as bad blocks for repair


// Preprocessor macros
#define segment_flush(s, da, lo, hi, to) (s)->fn.flush(s, da, lo, hi, to)
#define segment_id(s) (s)->header.id
#define segment_inspect(s, da, fd, mode, bsize, query, to) (s)->fn.inspect(s, da, fd, mode, bsize, query, to)
#define segment_read(s, da, hints, n_iov, iov, tbuf, boff, to) (s)->fn.read(s, da, hints, n_iov, iov, tbuf, boff, to)
#define segment_signature(s, buffer, used, bufsize) (s)->fn.signature(s, buffer, used, bufsize)
#define segment_size(s) (s)->fn.size(s)
#define segment_truncate(s, da, new_size, to) (s)->fn.truncate(s, da, new_size, to)
#define segment_write(s, da, hints, n_iov, iov, tbuf, boff, to) (s)->fn.write(s, da, hints, n_iov, iov, tbuf, boff, to)

// Exported types. To be obscured

struct segment_fn_t {
    op_generic_t *(*read)(segment_t *seg, data_attr_t *da, segment_rw_hints_t *hints, int n_iov, ex_tbx_iovec_t *iov, tbx_tbuf_t *buffer, ex_off_t boff, int timeout);
    op_generic_t *(*write)(segment_t *seg, data_attr_t *da, segment_rw_hints_t *hints, int n_iov, ex_tbx_iovec_t *iov, tbx_tbuf_t *buffer, ex_off_t boff, int timeout);
    op_generic_t *(*inspect)(segment_t *seg, data_attr_t *da, tbx_log_fd_t *fd, int mode, ex_off_t buffer_size, inspect_args_t *args, int timeout);
    op_generic_t *(*truncate)(segment_t *seg, data_attr_t *da, ex_off_t new_size, int timeout);
    op_generic_t *(*remove)(segment_t *seg, data_attr_t *da, int timeout);
    op_generic_t *(*flush)(segment_t *seg, data_attr_t *da, ex_off_t lo, ex_off_t hi, int timeout);
    op_generic_t *(*clone)(segment_t *seg, data_attr_t *da, segment_t **clone, int mode, void *attr, int timeout);
    int (*signature)(segment_t *seg, char *buffer, int *used, int bufsize);
    ex_off_t (*block_size)(segment_t *seg);
    ex_off_t (*size)(segment_t *seg);
    int (*serialize)(segment_t *seg, exnode_exchange_t *exp);
    int (*deserialize)(segment_t *seg, ex_id_t id, exnode_exchange_t *exp);
    void (*destroy)(segment_t *seg);
};

typedef void segment_priv_t;
struct segment_t {
    ex_header_t header;
    tbx_atomic_unit32_t ref_count;
    segment_priv_t *priv;
    service_manager_t *ess;
    segment_fn_t fn;
    apr_thread_mutex_t *lock;
    apr_thread_cond_t *cond;
    apr_pool_t *mpool;
};

struct rid_inspect_tweak_t {
    rid_change_entry_t *rid;
    apr_hash_t *pick_pool;
};

struct segment_errors_t {
    int soft;
    int hard;
    int write;
};

struct inspect_args_t {
    rs_query_t *query;   //** Generic extra query
    opque_t *qs;         //** Cleanup Que on success
    opque_t *qf;         //** Cleanup Que for failure
    apr_hash_t *rid_changes;  //** List of RID space changes
    apr_thread_mutex_t *rid_lock;     //** Lock for manipulating the rid_changes table
    int n_dev_rows;
    int dev_row_replaced[128];
};

#ifdef __cplusplus
}
#endif

#endif /* ^ ACCRE_LIO_EX3_ABSTRACT_H_INCLUDED ^ */ 
