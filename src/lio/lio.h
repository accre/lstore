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

//***********************************************************************
// Generic LIO functionality
//***********************************************************************

#ifndef _LIO_ABSTRACT_H_
#define _LIO_ABSTRACT_H_

#include <gop/mq.h>
#include <lio/lio.h>
#include <lio/visibility.h>
#include <sys/stat.h>
#include <tbx/log.h>
#include <zlib.h>

#include "blacklist.h"

#ifdef __cplusplus
extern "C" {
#endif

void lio_open_files_info_fn(void *arg, FILE *fd);

extern char *_lio_stat_keys[];
#define  _lio_stat_key_size 7

extern char *_lio_exe_name;  //** Executable name

struct lio_unified_object_iter_t {
    lio_path_tuple_t tuple;
    os_object_iter_t *oit;
    local_object_iter_t *lit;
};

#define LIO_WRITE_MODE     2
#define LIO_TRUNCATE_MODE  4
#define LIO_CREATE_MODE    8
#define LIO_APPEND_MODE   16
#define LIO_EXCL_MODE     32
#define LIO_RW_MODE       (LIO_READ_MODE|LIO_WRITE_MODE)

#define lio_lock(s) apr_thread_mutex_lock((s)->lock)
#define lio_unlock(s) apr_thread_mutex_unlock((s)->lock)

struct wq_context_s;
typedef struct wq_context_s wq_context_t;

struct lio_file_handle_t {  //** Shared file handle
    lio_exnode_t *ex;
    lio_segment_t *seg;
    lio_config_t *lc;
    wq_context_t *wq_ctx;
    char *fname;  //** This is just used for dumping info and represents the name used for adding the FH
    ex_id_t vid;
    int ref_count;
    int remove_on_close;
    ex_off_t readahead_end;
    tbx_atomic_unit32_t modified;
    tbx_list_t *write_table;
};

typedef struct {
    ex_off_t offset;
    ex_off_t len;
    uLong adler32;
} lfs_adler32_t;

typedef struct {
    lio_fd_t *fd;
    int n_iov;
    ex_tbx_iovec_t *iov;
    tbx_tbuf_t *buffer;
    lio_segment_rw_hints_t *rw_hints;
    ex_tbx_iovec_t iov_dummy;
    tbx_tbuf_t buffer_dummy;
    ex_off_t boff;
} lio_rw_op_t;

typedef gop_op_generic_t *(lio_rw_ex_gop_t)(lio_rw_op_t *op);

struct lio_fd_t {  //** Individual file descriptor
    lio_config_t *lc;
    lio_file_handle_t *fh;  //** Shared handle
    wq_context_t *wq_ctx;
    tbx_stack_t *wq;
    lio_creds_t *creds;
    char *path;
    lio_rw_ex_gop_t *read_gop;
    lio_rw_ex_gop_t *write_gop;
    int mode;         //** R/W mode
    ex_off_t curr_offset;
};

wq_context_t *wq_context_create(lio_fd_t *fd, int max_tasks);
void wq_context_destroy(wq_context_t *ctx);
void wq_backend_shutdown(wq_context_t *ctx);
gop_op_generic_t *wq_op_new(wq_context_t *ctx, lio_rw_op_t *rw_op, int rw_mode);

extern tbx_sl_compare_t ex_id_compare;

mode_t ftype_lio2posix(int ftype);
void _lio_parse_stat_vals(char *fname, struct stat *stat, char **val, int *v_size, char *mount_prefix, char **flink);

int lio_update_error_counts(lio_config_t *lc, lio_creds_t *creds, char *path, lio_segment_t *seg, int mode);
int lio_update_exnode_attrs(lio_config_t *lc, lio_creds_t *creds, lio_exnode_t *ex, lio_segment_t *seg, char *fname, lio_segment_errors_t *serr);

void lio_set_timestamp(char *id, char **val, int *v_size);
void lio_get_timestamp(char *val, int *timestamp, char **id);
gop_op_status_t cp_lio2lio(lio_cp_file_t *cp);
gop_op_status_t cp_local2lio(lio_cp_file_t *cp);
gop_op_status_t cp_lio2local(lio_cp_file_t *cp);
int lio_cp_create_dir(tbx_list_t *table, lio_path_tuple_t tuple);

void lc_object_remove_unused(int remove_all_unused);
lio_path_tuple_t lio_path_auto_fuse_convert(lio_path_tuple_t *ltuple);
int lio_parse_path(char *startpath, char **user, char **service, char **path);
lio_fn_t *lio_core_create();
void lio_core_destroy(lio_config_t *lio);
lio_config_t *lio_create(char *fname, char *section, char *user, char *exe_name);
void lio_destroy(lio_config_t *lio);
const char *lio_client_version();

void lio_wq_shutdown();
void lio_wq_startup(int n_parallel);

#ifdef __cplusplus
}
#endif

#endif
