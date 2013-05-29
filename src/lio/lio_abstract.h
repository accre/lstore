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

typedef struct lio_config_s lio_config_t;
typedef struct lio_fn_s lio_fn_t;
typedef void lio_fsck_iter_t;
typedef void lio_fd_t;

struct lio_fn_s {
  void *priv;
  char *type;
  void (*destroy_service)(lio_config_t *lc);

  lio_fsck_iter_t *(*create_fsck_iter)(lio_config_t *lc, creds_t *creds, char *path, int owner_mode, char *owner, int exnode_mode);
  void (*destroy_fsck_iter)(lio_config_t *lc, lio_fsck_iter_t *it);
  int (*next_fsck)(lio_config_t *lc, lio_fsck_iter_t *it, char **fname, int *ftype);
  op_generic_t *(*fsck_object)(lio_config_t *lc, creds_t *creds, char *fname, int ftype, int owner_mode, char *owner, int exnode_mode);
  ex_off_t (*fsck_visited_count)(lio_config_t *lc, lio_fsck_iter_t *it);
  op_generic_t *(*create_object)(lio_config_t *lc, creds_t *creds, char *path, int type, char *ex, char *id);
  op_generic_t *(*remove_object)(lio_config_t *lc, creds_t *creds, char *path, char *ex_optional, int ftype_optional);
  op_generic_t *(*remove_regex_object)(lio_config_t *lc, creds_t *creds, os_regex_table_t *path, os_regex_table_t *object_regex, int obj_types, int recurse_depth, int np);
  op_generic_t *(*move_object)(lio_config_t *lc, creds_t *creds, char *src_path, char *dest_path);
  op_generic_t *(*link_object)(lio_config_t *lc, creds_t *creds, int symlink, char *src_path, char *dest_path, char *id);

//  op_generic_t *(*open_io)(lio_config_t *lc, creds_t *creds, char *path, int mode, char *id, lio_fd_t **fd, int max_wait);
//  op_generic_t *(*close_io)(lio_config_t *lc, lio_fd_t *fd);
//  op_generic_t *(*abort_open_io)(lio_config_t *lc, op_generic_t *gop);
};

//#define lio_core_destroy(lc) lc->lio->destroy_service(lc)
#define lio_create_fsck_iter(lc, c, path, owner_mode, owner, exnode_mode) (lc)->lio->create_fsck_iter(lc, c, path, owner_mode, owner, exnode_mode)
#define lio_destroy_fsck_iter(lc, it) (lc)->lio->destroy_fsck_iter(lc, it)
#define lio_fsck_visited_count(lc, it) (lc)->lio->fsck_visited_count(lc, it)
#define lio_next_fsck(lc, it, fname, atype) (lc)->lio->next_fsck(lc, it, fname, atype)
#define lio_fsck_object(lc, c, fname, ftype, owner_mode, owner, exnode_mode) (lc)->lio->fsck_object(lc, c, fname, ftype, owner_mode, owner, exnode_mode)

#define lio_create_object(lc, c, path, type, ex, id) (lc)->lio->create_object(lc, c, path, type, ex, id)
#define lio_remove_object(lc, c, path, ex_opt, ftype_opt) (lc)->lio->remove_object(lc, c, path, ex_opt, ftype_opt)
#define lio_remove_regex_object(lc, c, path, obj_regex, obj_types, depth, np) (lc)->lio->remove_regex_object(lc, c, path, obj_regex, obj_types, depth, np)
#define lio_move_object(lc, c, src_path, dest_path) (lc)->lio->move_object(lc, c, src_path, dest_path)
#define lio_link_object(lc, c, symlink, src_path, dest_path, id) (lc)->lio->link_object(lc, c, symlink, src_path, dest_path, id)

//#define lio_open_io(lio, c, path, mode, id, fd, max_wait) (lc)->lio->open_io(lc, c, path, mode, id, fd, max_wait)
//#define lio_close_io(lio, fd) (lc)->lio->close_io(lc, fd)
//#define lio_abort_open_io(lio, gop) (lc)->lio->abort_open_io(lc, gop)

struct lio_config_s {
  data_service_fn_t *ds;
  object_service_fn_t *os;
  resource_service_fn_t *rs;
  thread_pool_context_t *tpc_unlimited;
  thread_pool_context_t *tpc_cpu;
  mq_context_t *mqc;
  service_manager_t *ess;
  service_manager_t *ess_nocache;  //** Copy of ess but missing cache.  Kind of a kludge...
  Stack_t *plugin_stack;
  lio_fn_t *lio;
  cache_t *cache;
  data_attr_t *da;
  inip_file_t *ifd;
  creds_t *creds;
  char *cfg_name;
  char *section_name;
  char *ds_section;
  char *mq_section;
  char *os_section;
  char *rs_section;
  char *tpc_cpu_section;
  char *tpc_unlimited_section;
  char *creds_name;
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

extern lio_config_t *lio_gc;
extern info_fd_t *lio_ifd;
extern int lio_parallel_task_count;

void lio_set_timestamp(char *id, char **val, int *v_size);
void lio_get_timestamp(char *val, int *timestamp, char **id);
int lioc_exists(lio_config_t *lc, creds_t *creds, char *path);
int lioc_set_multiple_attrs(lio_config_t *lc, creds_t *creds, char *path, char *id, char **key, void **val, int *v_size, int n);
int lioc_set_attr(lio_config_t *lc, creds_t *creds, char *path, char *id, char *key, void *val, int v_size);
int lioc_get_multiple_attrs(lio_config_t *lc, creds_t *creds, char *path, char *id, char **key, void **val, int *v_size, int n_keys);
int lioc_get_attr(lio_config_t *lc, creds_t *creds, char *path, char *id, char *key, void **val, int *v_size);
void lioc_get_error_counts(lio_config_t *lc, segment_t *seg, int *hard_errors, int *soft_errors);
int lioc_update_error_counts(lio_config_t *lc, creds_t *creds, char *path, segment_t *seg);
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
lio_config_t *lio_create(char *fname, char *section, char *user);
void lio_destroy(lio_config_t *lio);
const char *lio_client_version();


#ifdef __cplusplus
}
#endif

#endif

