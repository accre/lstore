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

#include "blacklist.h"

#ifdef __cplusplus
extern "C" {
#endif


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
#define LIO_RW_MODE        (LIO_READ_MODE|LIO_WRITE_MODE)


#define lio_lock(s) apr_thread_mutex_lock((s)->lock)
#define lio_unlock(s) apr_thread_mutex_unlock((s)->lock)

struct lio_file_handle_t {  //** Shared file handle
    lio_exnode_t *ex;
    lio_segment_t *seg;
    lio_config_t *lc;
    ex_id_t vid;
    int ref_count;
    int remove_on_close;
    ex_off_t readahead_end;
    tbx_atomic_unit32_t modified;
    tbx_list_t *write_table;
};


struct lio_fd_t {  //** Individual file descriptor
    lio_config_t *lc;
    lio_file_handle_t *fh;  //** Shared handle
    lio_creds_t *creds;
    char *path;
    int mode;         //** R/W mode
    ex_off_t curr_offset;
};

extern tbx_sl_compare_t ex_id_compare;


gop_op_generic_t *gop_lio_abort_regex_object_set_multiple_attrs(lio_config_t *lc, gop_op_generic_t *gop);
gop_op_generic_t *gop_lio_symlink_object(lio_config_t *lc, lio_creds_t *creds, char *src_path, char *dest_path, char *id);


//NOT NEEDED NOW???? gop_op_generic_t *gop_lio_abort_open_object(lio_config_t *lc, gop_op_generic_t *gop);

gop_op_generic_t *gop_lio_read(lio_fd_t *fd, char *buf, ex_off_t size, ex_off_t off, lio_segment_rw_hints_t *rw_hints);
gop_op_generic_t *gop_lio_readv(lio_fd_t *fd, tbx_iovec_t *iov, int n_iov, ex_off_t size, ex_off_t off, lio_segment_rw_hints_t *rw_hints);
gop_op_generic_t *gop_lio_read_ex(lio_fd_t *fd, int n_iov, ex_tbx_iovec_t *iov, tbx_tbuf_t *buffer, ex_off_t boff, lio_segment_rw_hints_t *rw_hints);
gop_op_generic_t *gop_lio_write(lio_fd_t *fd, char *buf, ex_off_t size, off_t off, lio_segment_rw_hints_t *rw_hints);
gop_op_generic_t *gop_lio_writev(lio_fd_t *fd, tbx_iovec_t *iov, int n_iov, ex_off_t size, off_t off, lio_segment_rw_hints_t *rw_hints);
gop_op_generic_t *gop_lio_write_ex(lio_fd_t *fd, int n_iov, ex_tbx_iovec_t *iov, tbx_tbuf_t *buffer, ex_off_t boff, lio_segment_rw_hints_t *rw_hints);

int lio_read(lio_fd_t *fd, char *buf, ex_off_t size, off_t off, lio_segment_rw_hints_t *rw_hints);
int lio_readv(lio_fd_t *fd, tbx_iovec_t *iov, int n_iov, ex_off_t size, off_t off, lio_segment_rw_hints_t *rw_hints);
int lio_read_ex(lio_fd_t *fd, int n_iov, ex_tbx_iovec_t *iov, tbx_tbuf_t *buffer, ex_off_t boff, lio_segment_rw_hints_t *rw_hints);
int lio_write(lio_fd_t *fd, char *buf, ex_off_t size, off_t off, lio_segment_rw_hints_t *rw_hints);
int lio_writev(lio_fd_t *fd, tbx_iovec_t *iov, int n_iov, ex_off_t size, off_t off, lio_segment_rw_hints_t *rw_hints);
int lio_write_ex(lio_fd_t *fd, int n_iov, ex_tbx_iovec_t *iov, tbx_tbuf_t *buffer, ex_off_t boff, lio_segment_rw_hints_t *rw_hints);

mode_t ftype_lio2posix(int ftype);
void _lio_parse_stat_vals(char *fname, struct stat *stat, char **val, int *v_size, char *mount_prefix, char **flink);

ex_off_t lio_seek(lio_fd_t *fd, ex_off_t offset, int whence);
ex_off_t lio_tell(lio_fd_t *fd);
ex_off_t lio_size(lio_fd_t *fd);
gop_op_generic_t *gop_lio_truncate(lio_fd_t *fd, ex_off_t new_size);
// NOT IMPLEMENTED gop_op_generic_t *gop_lio_stat(lio_t *lc, const char *fname, struct stat *stat);

gop_op_generic_t *gop_lio_cp_lio2lio(lio_fd_t *sfd, lio_fd_t *dfd, ex_off_t bufsize, char *buffer, int hints, lio_segment_rw_hints_t *rw_hints);

//gop_op_generic_t *gop_lio_symlink_attr(lio_config_t *lc, lio_creds_t *creds, char *src_path, char *key_src, const char *path_dest, char *key_dest);
//gop_op_generic_t *gop_lio_symlink_multiple_attrs(lio_config_t *lc, lio_creds_t *creds, char **src_path, char **key_src, const char *path_dest, char **key_dest, int n);

//gop_op_generic_t *gop_lio_move_attr(lio_config_t *lc, lio_creds_t *creds, const char *path, char *id, char *key_old, char *key_new);
//gop_op_generic_t *gop_lio_copy_attr(lio_config_t *lc, lio_creds_t *creds, const char *path_src, char *id, char *key_src, const char *path_dest, char *key_dest);
gop_op_generic_t *gop_lio_get_multiple_attrs(lio_config_t *lc, lio_creds_t *creds, const char *path, char *id, char **key, void **val, int *v_size, int n);
gop_op_generic_t *gop_lio_multiple_setattr_op(lio_config_t *lc, lio_creds_t *creds, const char *path, char *id, char **key, void **val, int *v_size, int n);
//gop_op_generic_t *gop_lio_move_multiple_attrs(lio_config_t *lc, lio_creds_t *creds, const char *char *id, path, char **key_old, char **key_new, int n);
//gop_op_generic_t *gop_lio_copy_multiple_attrs(lio_config_t *lc, lio_creds_t *creds, const char *path_src, char *id, char **key_src, const char *path_dest, char **key_dest, int n);
int lio_get_multiple_attrs(lio_config_t *lc, lio_creds_t *creds, const char *path, char *id, char **key, void **val, int *v_size, int n);

os_attr_iter_t *lio_create_attr_iter(lio_config_t *lc, lio_creds_t *creds, const char *path, lio_os_regex_table_t *attr, int v_max);
void lio_destroy_attr_iter(lio_config_t *lc, os_attr_iter_t *it);

int lio_update_error_counts(lio_config_t *lc, lio_creds_t *creds, char *path, lio_segment_t *seg, int mode);
int lio_update_exnode_attrs(lio_config_t *lc, lio_creds_t *creds, lio_exnode_t *ex, lio_segment_t *seg, char *fname, lio_segment_errors_t *serr);



//-----
gop_op_generic_t *lioc_create_object(lio_config_t *lc, lio_creds_t *creds, char *path, int type, char *ex, char *id);
gop_op_generic_t *lioc_remove_object(lio_config_t *lc, lio_creds_t *creds, char *path, char *ex_optional, int ftype_optional);
gop_op_generic_t *lioc_remove_regex_object(lio_config_t *lc, lio_creds_t *creds, lio_os_regex_table_t *rpath, lio_os_regex_table_t *robj, int obj_types, int recurse_depth, int np);


void lio_set_timestamp(char *id, char **val, int *v_size);
void lio_get_timestamp(char *val, int *timestamp, char **id);
int lioc_set_multiple_attrs(lio_config_t *lc, lio_creds_t *creds, char *path, char *id, char **key, void **val, int *v_size, int n);
int lioc_get_multiple_attrs(lio_config_t *lc, lio_creds_t *creds, char *path, char *id, char **key, void **val, int *v_size, int n_keys);
int lioc_encode_error_counts(lio_segment_errors_t *serr, char **key, char **val, char *buf, int *v_size, int mode);
void lioc_get_error_counts(lio_config_t *lc, lio_segment_t *seg, lio_segment_errors_t *serr);
int lioc_update_error_counts(lio_config_t *lc, lio_creds_t *creds, char *path, lio_segment_t *seg, int mode);
int lioc_update_exnode_attrs(lio_config_t *lc, lio_creds_t *creds, lio_exnode_t *ex, lio_segment_t *seg, char *fname, lio_segment_errors_t *serr);
gop_op_generic_t *lioc_remove_object(lio_config_t *lc, lio_creds_t *creds, char *path, char *ex_optional, int ftype_optional);
gop_op_status_t cp_lio2lio(lio_cp_file_t *cp);
gop_op_status_t cp_local2lio(lio_cp_file_t *cp);
gop_op_status_t cp_lio2local(lio_cp_file_t *cp);
int lio_cp_create_dir(tbx_list_t *table, lio_path_tuple_t tuple);

void lc_object_remove_unused(int remove_all_unused);
lio_path_tuple_t lio_path_auto_fuse_convert(lio_path_tuple_t *tuple);
int lio_parse_path(char *startpath, char **user, char **service, char **path);
lio_fn_t *lio_core_create();
void lio_core_destroy(lio_config_t *lio);
lio_config_t *lio_create(char *fname, char *section, char *user, char *exe_name);
void lio_destroy(lio_config_t *lio);
const char *lio_client_version();


#ifdef __cplusplus
}
#endif

#endif
