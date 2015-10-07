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

//***********************************************************************
// Generic LIO functionality
//***********************************************************************

#include "exnode.h"
#include "blacklist.h"
#include "mq_portal.h"
#include "log.h"

#ifndef _LIO_ABSTRACT_H_
#define _LIO_ABSTRACT_H_

#ifdef __cplusplus
extern "C" {
#endif

#define LIO_FSCK_FINISHED           -1
#define LIO_FSCK_GOOD                0
#define LIO_FSCK_MISSING_OWNER       1
#define LIO_FSCK_MISSING_EXNODE      2
#define LIO_FSCK_MISSING_EXNODE_SIZE 4
#define LIO_FSCK_MISSING_INODE       8
#define LIO_FSCK_MISSING            16

#define LIO_FSCK_MANUAL      0
#define LIO_FSCK_PARENT      1
#define LIO_FSCK_DELETE      2
#define LIO_FSCK_USER        4
#define LIO_FSCK_SIZE_REPAIR 8

extern char *_lio_stat_keys[];
#define  _lio_stat_key_size 7

typedef struct lio_config_s lio_config_t;
typedef struct lio_fn_s lio_fn_t;
typedef struct lio_fsck_iter_s lio_fsck_iter_t;

extern FILE *_lio_ifd;  //** Default information log device
extern char *_lio_exe_name;  //** Executable name

struct lio_config_s {
    data_service_fn_t *ds;
    object_service_fn_t *os;
    resource_service_fn_t *rs;
    thread_pool_context_t *tpc_unlimited;
    mq_context_t *mqc;
    service_manager_t *ess;
    service_manager_t *ess_nocache;  //** Copy of ess but missing cache.  Kind of a kludge...
    Stack_t *plugin_stack;
    cache_t *cache;
    data_attr_t *da;
    inip_file_t *ifd;
    list_t *open_index;
    creds_t *creds;
    apr_thread_mutex_t *lock;
    apr_pool_t *mpool;
    char *cfg_name;
    char *section_name;
    char *ds_section;
    char *mq_section;
    char *os_section;
    char *rs_section;
    char *tpc_unlimited_section;
    char *creds_name;
    char *exe_name;
    blacklist_t *blacklist;
    ex_off_t readahead;
    ex_off_t readahead_trigger;
    int calc_adler32;
    int timeout;
    int max_attr;
    int anonymous_creation;
    int auto_translate;
    int ref_cnt;
};

typedef struct {
    creds_t *creds;
    lio_config_t *lc;
    char *path;
    int is_lio;
} lio_path_tuple_t;

typedef struct {
    lio_path_tuple_t tuple;
    os_object_iter_t *oit;
    local_object_iter_t *lit;
} unified_object_iter_t;

typedef struct {
    segment_rw_hints_t *rw_hints;
    lio_path_tuple_t src_tuple;
    lio_path_tuple_t dest_tuple;
    ex_off_t bufsize;
    int slow;
} lio_cp_file_t;

typedef struct {
    lio_path_tuple_t src_tuple;
    lio_path_tuple_t dest_tuple;
    os_regex_table_t *path_regex;
    os_regex_table_t *obj_regex;
    int recurse_depth;
    int dest_type;
    int obj_types;
    int max_spawn;
    int slow;
    ex_off_t bufsize;
} lio_cp_path_t;

extern lio_config_t *lio_gc;
extern info_fd_t *lio_ifd;
extern int lio_parallel_task_count;

#define LIO_READ_MODE      1
#define LIO_WRITE_MODE     2
#define LIO_TRUNCATE_MODE  4
#define LIO_CREATE_MODE    8
#define LIO_APPEND_MODE   16
#define LIO_RW_MODE        (LIO_READ_MODE|LIO_WRITE_MODE)

#define LIO_COPY_DIRECT   0
#define LIO_COPY_INDIRECT 1

#define lio_lock(s) apr_thread_mutex_lock((s)->lock)
#define lio_unlock(s) apr_thread_mutex_unlock((s)->lock)

struct lio_file_handle_s {  //** Shared file handle
    exnode_t *ex;
    segment_t *seg;
    lio_config_t *lc;
    ex_id_t vid;
    int ref_count;
    int remove_on_close;
    ex_off_t readahead_end;
    atomic_int_t modified;
    list_t *write_table;
};

typedef struct lio_file_handle_s lio_file_handle_t;

typedef struct {  //** Individual file descriptor
    lio_config_t *lc;
    lio_file_handle_t *fh;  //** Shared handle
    creds_t *creds;
    char *path;
    int mode;         //** R/W mode
    ex_off_t curr_offset;
} lio_fd_t;

extern skiplist_compare_t ex_id_compare;

op_generic_t *gop_lio_exists(lio_config_t *lc, creds_t *creds, char *path);
int lio_exists(lio_config_t *lc, creds_t *creds, char *path);
op_generic_t *gop_lio_create_object(lio_config_t *lc, creds_t *creds, char *path, int type, char *ex, char *id);
op_generic_t *gop_lio_remove_object(lio_config_t *lc, creds_t *creds, char *path, char *ex_optional, int ftype_optional);
op_generic_t *gop_lio_remove_regex_object(lio_config_t *lc, creds_t *creds, os_regex_table_t *path, os_regex_table_t *object_regex, int obj_types, int recurse_depth, int np);

op_generic_t *gop_lio_regex_object_set_multiple_attrs(lio_config_t *lc, creds_t *creds, char *id, os_regex_table_t *path, os_regex_table_t *object_regex, int object_types, int recurse_depth, char **key, void **val, int *v_size, int n);
op_generic_t *gop_lio_abort_regex_object_set_multiple_attrs(lio_config_t *lc, op_generic_t *gop);
op_generic_t *gop_lio_move_object(lio_config_t *lc, creds_t *creds, char *src_path, char *dest_path);
op_generic_t *gop_lio_symlink_object(lio_config_t *lc, creds_t *creds, char *src_path, char *dest_path, char *id);
op_generic_t *gop_lio_hardlink_object(lio_config_t *lc, creds_t *creds, char *src_path, char *dest_path, char *id);
op_generic_t *gop_lio_link_object(lio_config_t *lc, creds_t *creds, int symlink, char *src_path, char *dest_path, char *id);

os_object_iter_t *lio_create_object_iter(lio_config_t *lc, creds_t *creds, os_regex_table_t *path, os_regex_table_t *obj_regex, int object_types, os_regex_table_t *attr, int recurse_dpeth, os_attr_iter_t **it, int v_max);
os_object_iter_t *lio_create_object_iter_alist(lio_config_t *lc, creds_t *creds, os_regex_table_t *path, os_regex_table_t *obj_regex, int object_types, int recurse_depth, char **key, void **val, int *v_size, int n_keys);
int lio_next_object(lio_config_t *lc, os_object_iter_t *it, char **fname, int *prefix_len);
void lio_destroy_object_iter(lio_config_t *lc, os_object_iter_t *it);

int lio_fopen_flags(char *sflags);
op_generic_t *gop_lio_open_object(lio_config_t *lc, creds_t *creds, char *path, int mode, char *id, lio_fd_t **fd, int max_wait);
op_generic_t *gop_lio_close_object(lio_fd_t *fd);
//NOT NEEDED NOW???? op_generic_t *gop_lio_abort_open_object(lio_config_t *lc, op_generic_t *gop);

op_generic_t *gop_lio_read(lio_fd_t *fd, char *buf, ex_off_t size, ex_off_t off, segment_rw_hints_t *rw_hints);
op_generic_t *gop_lio_readv(lio_fd_t *fd, iovec_t *iov, int n_iov, ex_off_t size, ex_off_t off, segment_rw_hints_t *rw_hints);
op_generic_t *gop_lio_read_ex(lio_fd_t *fd, int n_iov, ex_iovec_t *iov, tbuffer_t *buffer, ex_off_t boff, segment_rw_hints_t *rw_hints);
op_generic_t *gop_lio_write(lio_fd_t *fd, char *buf, ex_off_t size, off_t off, segment_rw_hints_t *rw_hints);
op_generic_t *gop_lio_writev(lio_fd_t *fd, iovec_t *iov, int n_iov, ex_off_t size, off_t off, segment_rw_hints_t *rw_hints);
op_generic_t *gop_lio_write_ex(lio_fd_t *fd, int n_iov, ex_iovec_t *iov, tbuffer_t *buffer, ex_off_t boff, segment_rw_hints_t *rw_hints);

int lio_read(lio_fd_t *fd, char *buf, ex_off_t size, off_t off, segment_rw_hints_t *rw_hints);
int lio_readv(lio_fd_t *fd, iovec_t *iov, int n_iov, ex_off_t size, off_t off, segment_rw_hints_t *rw_hints);
int lio_read_ex(lio_fd_t *fd, int n_iov, ex_iovec_t *iov, tbuffer_t *buffer, ex_off_t boff, segment_rw_hints_t *rw_hints);
int lio_write(lio_fd_t *fd, char *buf, ex_off_t size, off_t off, segment_rw_hints_t *rw_hints);
int lio_writev(lio_fd_t *fd, iovec_t *iov, int n_iov, ex_off_t size, off_t off, segment_rw_hints_t *rw_hints);
int lio_write_ex(lio_fd_t *fd, int n_iov, ex_iovec_t *iov, tbuffer_t *buffer, ex_off_t boff, segment_rw_hints_t *rw_hints);

mode_t ftype_lio2posix(int ftype);
void _lio_parse_stat_vals(char *fname, struct stat *stat, char **val, int *v_size, char *mount_prefix, char **flink);
int lio_stat(lio_config_t *lc, creds_t *creds, char *fname, struct stat *stat, char *mount_prefix, char **readlink);

ex_off_t lio_seek(lio_fd_t *fd, ex_off_t offset, int whence);
ex_off_t lio_tell(lio_fd_t *fd);
ex_off_t lio_size(lio_fd_t *fd);
op_generic_t *gop_lio_truncate(lio_fd_t *fd, ex_off_t new_size);
// NOT IMPLEMENTED op_generic_t *gop_lio_stat(lio_t *lc, const char *fname, struct stat *stat);

op_generic_t *gop_lio_cp_local2lio(FILE *sfd, lio_fd_t *dfd, ex_off_t bufsize, char *buffer, segment_rw_hints_t *rw_hints);
op_generic_t *gop_lio_cp_lio2local(lio_fd_t *sfd, FILE *dfd, ex_off_t bufsize, char *buffer, segment_rw_hints_t *rw_hints);
op_generic_t *gop_lio_cp_lio2lio(lio_fd_t *sfd, lio_fd_t *dfd, ex_off_t bufsize, char *buffer, int hints, segment_rw_hints_t *rw_hints);

//op_generic_t *gop_lio_symlink_attr(lio_config_t *lc, creds_t *creds, char *src_path, char *key_src, const char *path_dest, char *key_dest);
//op_generic_t *gop_lio_symlink_multiple_attrs(lio_config_t *lc, creds_t *creds, char **src_path, char **key_src, const char *path_dest, char **key_dest, int n);

op_generic_t *gop_lio_get_attr(lio_config_t *lc, creds_t *creds, const char *path, char *id, char *key, void **val, int *v_size);
op_generic_t *gop_lio_set_attr(lio_config_t *lc, creds_t *creds, const char *path, char *id, char *key, void *val, int v_size);
//op_generic_t *gop_lio_move_attr(lio_config_t *lc, creds_t *creds, const char *path, char *id, char *key_old, char *key_new);
//op_generic_t *gop_lio_copy_attr(lio_config_t *lc, creds_t *creds, const char *path_src, char *id, char *key_src, const char *path_dest, char *key_dest);
op_generic_t *gop_lio_get_multiple_attrs(lio_config_t *lc, creds_t *creds, const char *path, char *id, char **key, void **val, int *v_size, int n);
op_generic_t *gop_lio_set_multiple_attrs(lio_config_t *lc, creds_t *creds, const char *path, char *id, char **key, void **val, int *v_size, int n);
//op_generic_t *gop_lio_move_multiple_attrs(lio_config_t *lc, creds_t *creds, const char *char *id, path, char **key_old, char **key_new, int n);
//op_generic_t *gop_lio_copy_multiple_attrs(lio_config_t *lc, creds_t *creds, const char *path_src, char *id, char **key_src, const char *path_dest, char **key_dest, int n);
int lio_get_attr(lio_config_t *lc, creds_t *creds, const char *path, char *id, char *key, void **val, int *v_size);
int lio_set_attr(lio_config_t *lc, creds_t *creds, const char *path, char *id, char *key, void *val, int v_size);
int lio_get_multiple_attrs(lio_config_t *lc, creds_t *creds, const char *path, char *id, char **key, void **val, int *v_size, int n);
int lio_set_multiple_attrs(lio_config_t *lc, creds_t *creds, const char *path, char *id, char **key, void **val, int *v_size, int n);

os_attr_iter_t *lio_create_attr_iter(lio_config_t *lc, creds_t *creds, const char *path, os_regex_table_t *attr, int v_max);
int lio_next_attr(lio_config_t *lc, os_attr_iter_t *it, char **key, void **val, int *v_size);
void lio_destroy_attr_iter(lio_config_t *lc, os_attr_iter_t *it);

int lio_encode_error_counts(segment_errors_t *serr, char **key, char **val, char *buf, int *v_size, int mode);
void lio_get_error_counts(lio_config_t *lc, segment_t *seg, segment_errors_t *serr);
int lio_update_error_counts(lio_config_t *lc, creds_t *creds, char *path, segment_t *seg, int mode);
int lio_update_exnode_attrs(lio_config_t *lc, creds_t *creds, exnode_t *ex, segment_t *seg, char *fname, segment_errors_t *serr);

int lio_next_fsck(lio_config_t *lc, lio_fsck_iter_t *oit, char **bad_fname, int *bad_atype);
lio_fsck_iter_t *lio_create_fsck_iter(lio_config_t *lc, creds_t *creds, char *path, int owner_mode, char *owner, int exnode_mode);
void lio_destroy_fsck_iter(lio_config_t *lc, lio_fsck_iter_t *oit);
ex_off_t lio_fsck_visited_count(lio_config_t *lc, lio_fsck_iter_t *oit);
op_generic_t *lio_fsck_object(lio_config_t *lc, creds_t *creds, char *fname, int ftype, int owner_mode, char *owner, int exnode_mode);


//-----
op_generic_t *lioc_create_object(lio_config_t *lc, creds_t *creds, char *path, int type, char *ex, char *id);
op_generic_t *lioc_remove_object(lio_config_t *lc, creds_t *creds, char *path, char *ex_optional, int ftype_optional);
op_generic_t *lioc_remove_regex_object(lio_config_t *lc, creds_t *creds, os_regex_table_t *rpath, os_regex_table_t *robj, int obj_types, int recurse_depth, int np);


void lio_set_timestamp(char *id, char **val, int *v_size);
void lio_get_timestamp(char *val, int *timestamp, char **id);
int lioc_exists(lio_config_t *lc, creds_t *creds, char *path);
int lioc_set_multiple_attrs(lio_config_t *lc, creds_t *creds, char *path, char *id, char **key, void **val, int *v_size, int n);
int lioc_set_attr(lio_config_t *lc, creds_t *creds, char *path, char *id, char *key, void *val, int v_size);
int lioc_get_multiple_attrs(lio_config_t *lc, creds_t *creds, char *path, char *id, char **key, void **val, int *v_size, int n_keys);
int lioc_get_attr(lio_config_t *lc, creds_t *creds, char *path, char *id, char *key, void **val, int *v_size);
int lioc_encode_error_counts(segment_errors_t *serr, char **key, char **val, char *buf, int *v_size, int mode);
void lioc_get_error_counts(lio_config_t *lc, segment_t *seg, segment_errors_t *serr);
int lioc_update_error_counts(lio_config_t *lc, creds_t *creds, char *path, segment_t *seg, int mode);
int lioc_update_exnode_attrs(lio_config_t *lc, creds_t *creds, exnode_t *ex, segment_t *seg, char *fname, segment_errors_t *serr);
op_generic_t *lioc_remove_object(lio_config_t *lc, creds_t *creds, char *path, char *ex_optional, int ftype_optional);
unified_object_iter_t *unified_create_object_iter(lio_path_tuple_t tuple, os_regex_table_t *path_regex, os_regex_table_t *obj_regex, int obj_types, int rd);
void unified_destroy_object_iter(unified_object_iter_t *it);
int unified_next_object(unified_object_iter_t *it, char **fname, int *prefix_len);
op_status_t cp_lio2lio(lio_cp_file_t *cp);
op_status_t cp_local2lio(lio_cp_file_t *cp);
op_status_t cp_lio2local(lio_cp_file_t *cp);
op_status_t lio_cp_file_fn(void *arg, int id);
int lio_cp_create_dir(list_t *table, lio_path_tuple_t tuple);
op_status_t lio_cp_path_fn(void *arg, int id);
op_generic_t *lioc_truncate(lio_path_tuple_t *tuple, ex_off_t new_size);

void lc_object_remove_unused(int remove_all_unused);
void lio_path_release(lio_path_tuple_t *tuple);
void lio_path_local_make_absolute(lio_path_tuple_t *tuple);
int lio_path_wildcard_auto_append(lio_path_tuple_t *tuple);
lio_path_tuple_t lio_path_auto_fuse_convert(lio_path_tuple_t *tuple);
lio_path_tuple_t lio_path_resolve_base(char *startpath);
lio_path_tuple_t lio_path_resolve(int auto_fuse_convert, char *startpath);
int lio_parse_path(char *startpath, char **user, char **service, char **path);
lio_fn_t *lio_core_create();
void lio_core_destroy(lio_config_t *lio);
void lio_print_path_options(FILE *fd);
int lio_parse_path_options(int *argc, char **argv, int auto_mode, lio_path_tuple_t *tuple, os_regex_table_t **rp, os_regex_table_t **ro);
void lio_print_options(FILE *fd);
int lio_init(int *argc, char ***argv);
int lio_shutdown();
lio_config_t *lio_create(char *fname, char *section, char *user, char *exe_name);
void lio_destroy(lio_config_t *lio);
const char *lio_client_version();


#ifdef __cplusplus
}
#endif

#endif

