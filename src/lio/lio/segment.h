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

#ifndef ACCRE_LIO_SEGMENT_H_INCLUDED
#define ACCRE_LIO_SEGMENT_H_INCLUDED

#include <gop/opque.h>
#include <lio/cache.h>
#include <lio/ex3.h>
#include <lio/visibility.h>
#include <lio/rs.h>
#include <tbx/object.h>

#ifdef __cplusplus
extern "C" {
#endif

// Declare block size type flags
typedef enum lio_segment_block_type_t lio_segment_block_type_t;
enum lio_segemtn_block_type_t {
    LIO_SEGMENT_BLOCK_MIN     = 0,   // Minimum allowed block size
    LIO_SEGMENT_BLOCK_NATURAL = 1    // Natural block size for a client write
};

// Typedefs
typedef struct lio_segment_vtable_t lio_segment_vtable_t;
typedef gop_op_generic_t *(*lio_segment_read_fn_t)(lio_segment_t *seg, data_attr_t *da, lio_segment_rw_hints_t *hints, int n_iov, ex_tbx_iovec_t *iov, tbx_tbuf_t *buffer, ex_off_t boff, int timeout);
typedef gop_op_generic_t *(*lio_segment_write_fn_t)(lio_segment_t *seg, data_attr_t *da, lio_segment_rw_hints_t *hints, int n_iov, ex_tbx_iovec_t *iov, tbx_tbuf_t *buffer, ex_off_t boff, int timeout);
typedef gop_op_generic_t *(*lio_segment_inspect_fn_t)(lio_segment_t *seg, data_attr_t *da, tbx_log_fd_t *fd, int mode, ex_off_t buffer_size, lio_inspect_args_t *args, int timeout);
typedef gop_op_generic_t *(*lio_segment_truncate_fn_t)(lio_segment_t *seg, data_attr_t *da, ex_off_t new_size, int timeout);
typedef gop_op_generic_t *(*lio_segment_remove_fn_t)(lio_segment_t *seg, data_attr_t *da, int timeout);
typedef gop_op_generic_t *(*lio_segment_flush_fn_t)(lio_segment_t *seg, data_attr_t *da, ex_off_t lo, ex_off_t hi, int timeout);
typedef gop_op_generic_t *(*lio_segment_clone_fn_t)(lio_segment_t *seg, data_attr_t *da, lio_segment_t **clone, int mode, void *attr, int timeout);
typedef int (*lio_segment_signature_fn_t)(lio_segment_t *seg, char *buffer, int *used, int bufsize);
typedef ex_off_t (*lio_segment_block_size_fn_t)(lio_segment_t *seg, int block_type);
typedef ex_off_t (*lio_segment_size_fn_t)(lio_segment_t *seg);
typedef int (*lio_segment_serialize_fn_t)(lio_segment_t *seg, lio_exnode_exchange_t *exp);
typedef int (*lio_segment_deserialize_fn_t)(lio_segment_t *seg, ex_id_t id, lio_exnode_exchange_t *exp);
typedef void (*lio_segment_destroy_fn_t)(lio_segment_t *seg);
// FIXME: leaky
typedef struct lio_seglog_priv_t lio_seglog_priv_t;
typedef struct lio_slog_range_t lio_slog_range_t;
typedef struct lio_seglun_priv_t lio_seglun_priv_t;

// Functions
// FIXME: leaky
LIO_API int lio_cache_stats_get(lio_cache_t *c, lio_cache_stats_get_t *cs);
LIO_API int lio_cache_stats_get_print(lio_cache_stats_get_t *cs, char *buffer, int *used, int nmax);
LIO_API lio_cache_stats_get_t segment_lio_cache_stats_get(lio_segment_t *seg);
LIO_API gop_op_generic_t *lio_segment_linear_make_gop(lio_segment_t *seg, data_attr_t *da, rs_query_t *rsq, int n_rid, ex_off_t block_size, ex_off_t total_size, int timeout);
LIO_API gop_op_generic_t *lio_slog_merge_with_base_gop(lio_segment_t *seg, data_attr_t *da, ex_off_t bufsize, char *buffer, int truncate_old_log, int timeout);  //** Merges the current log with the base

// Preprocessor constants
// FIXME: leaky
#define SEGMENT_TYPE_LINEAR "linear"

// Preprocessor macros
#define segment_flush(s, da, lo, hi, to) ((lio_segment_vtable_t *)(s)->obj.vtable)->flush(s, da, lo, hi, to)
#define segment_id(s) (s)->header.id
#define segment_inspect(s, da, fd, mode, bsize, query, to) ((lio_segment_vtable_t *)(s)->obj.vtable)->inspect(s, da, fd, mode, bsize, query, to)
#define segment_read(s, da, hints, n_iov, iov, tbuf, boff, to) ((lio_segment_vtable_t *)(s)->obj.vtable)->read(s, da, hints, n_iov, iov, tbuf, boff, to)
#define segment_signature(s, buffer, used, bufsize) ((lio_segment_vtable_t *)(s)->obj.vtable)->signature(s, buffer, used, bufsize)
#define segment_size(s) ((lio_segment_vtable_t *)(s)->obj.vtable)->size(s)
#define lio_segment_truncate(s, da, new_size, to) ((lio_segment_vtable_t *)(s)->obj.vtable)->truncate(s, da, new_size, to)
#define segment_write(s, da, hints, n_iov, iov, tbuf, boff, to) ((lio_segment_vtable_t *)(s)->obj.vtable)->write(s, da, hints, n_iov, iov, tbuf, boff, to)
#define segment_block_size(s, btype) ((lio_segment_vtable_t *)(s)->obj.vtable)->block_size(s, btype)

// Exported types. To be obscured
struct lio_segment_vtable_t {
    tbx_vtable_t base;
    lio_segment_read_fn_t read;
    lio_segment_write_fn_t write;
    lio_segment_inspect_fn_t inspect;
    lio_segment_truncate_fn_t truncate;
    lio_segment_remove_fn_t remove;
    lio_segment_flush_fn_t flush;
    lio_segment_clone_fn_t clone;
    lio_segment_signature_fn_t signature;
    lio_segment_block_size_fn_t block_size;
    lio_segment_size_fn_t size;
    lio_segment_serialize_fn_t serialize;
    lio_segment_deserialize_fn_t deserialize;
};

struct lio_segment_t {
    tbx_obj_t obj;
    lio_ex_header_t header;
    segment_priv_t *priv;
    lio_service_manager_t *ess;
    apr_thread_mutex_t *lock;
    apr_thread_cond_t *cond;
    apr_pool_t *mpool;
};

struct lio_segment_errors_t {
    int soft;
    int hard;
    int write;
};

#ifdef __cplusplus
}
#endif

#endif /* ^ ACCRE_LIO_SEGMENT_CACHE_H_INCLUDED ^ */ 
