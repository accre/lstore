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
// Generic object service
//***********************************************************************

#include <sys/types.h>
#include <regex.h>
#include "ex3_types.h"
#include "opque.h"
#include "thread_pool.h"
#include "authn_abstract.h"
#include "service_manager.h"

#ifndef _OBJECT_SERVICE_H_
#define _OBJECT_SERVICE_H_

#ifdef __cplusplus
extern "C" {
#endif

#define OS_PATH_MAX  32768    //** Max path length

#define OS_FSCK_FINISHED  0   //** FSCK scan is finished

#define OS_FSCK_GOOD            0 //** Nothing wrong with the object
#define OS_FSCK_MISSING_ATTR    1 //** Missing the object attributes
#define OS_FSCK_MISSING_OBJECT  2 //** Missing file entry

#define OS_FSCK_MANUAL    0   //** Manual resolution via fsck_resolve() or user control
#define OS_FSCK_REMOVE    1   //** Removes the problem object
#define OS_FSCK_REPAIR    2   //** Repairs the problem object

#define OS_OBJECT_FILE         1  //** File object or attribute
#define OS_OBJECT_DIR          2  //** Directory object
#define OS_OBJECT_SYMLINK      4  //** A symlinked object or attribute
#define OS_OBJECT_HARDLINK     8  //** A hard linked object
#define OS_OBJECT_BROKEN_LINK 16  //** Signifies a broken link
#define OS_OBJECT_VIRTUAL     32  //** A virtual attribute
#define OS_OBJECT_ANY         63

#define OS_MODE_READ_IMMEDIATE  0
#define OS_MODE_WRITE_IMMEDIATE 1
#define OS_MODE_READ_BLOCKING   2
#define OS_MODE_WRITE_BLOCKING  3

#define OS_VATTR_NORMAL  0   //** Normal virtual attribute.  Works the same as a non-virtual attribute.
#define OS_VATTR_PREFIX  1   //** Routine is called  whenever the VA prefix matches the attr.  Does not show up in iterators.

#define OS_CREDS_INI_TYPE 0  //** Load creds from file

typedef struct os_authz_s os_authz_t;

typedef struct {
  int q_mode;
  char *attr;
} os_attr_list_t;

typedef void os_fd_t;
typedef void os_attr_iter_t;
typedef void os_object_iter_t;
typedef void os_fsck_iter_t;

typedef struct {
  char *expression;
  int fixed;
  int fixed_prefix;
  regex_t compiled;
} os_regex_entry_t;

typedef struct {
  int n;
  os_regex_entry_t *regex_entry;
} os_regex_table_t;

struct object_service_fn_s;
typedef struct object_service_fn_s object_service_fn_t;
typedef struct os_virtual_attr_s os_virtual_attr_t;

struct object_service_fn_s {
  void *priv;
  char *type;
  void (*destroy_service)(object_service_fn_t *os);

  os_fsck_iter_t *(*create_fsck_iter)(object_service_fn_t *os, creds_t *creds, char *path, int mode);
  void (*destroy_fsck_iter)(object_service_fn_t *os, os_fsck_iter_t *it);
  int (*next_fsck)(object_service_fn_t *os, os_fsck_iter_t *it, char **fname, int *ftype);
  op_generic_t *(*fsck_object)(object_service_fn_t *os, creds_t *creds, char *fname, int ftype, int resolution);

  creds_t *(*cred_init)(object_service_fn_t *os, int type, void **arg);
  void (*cred_destroy)(object_service_fn_t *os, creds_t *creds);

  op_generic_t *(*exists)(object_service_fn_t *os, creds_t *creds, char *path);
  op_generic_t *(*create_object)(object_service_fn_t *os, creds_t *creds, char *path, int type, char *id);
  op_generic_t *(*remove_object)(object_service_fn_t *os, creds_t *creds, char *path);
  op_generic_t *(*remove_regex_object)(object_service_fn_t *os, creds_t *creds, os_regex_table_t *path, os_regex_table_t *object_regex, int obj_types, int recurse_depth);
  op_generic_t *(*regex_object_set_multiple_attrs)(object_service_fn_t *os, creds_t *creds, char *id, os_regex_table_t *path, os_regex_table_t *object_regex, int object_types, int recurse_depth, char **key, void **val, int *v_size, int n);
  op_generic_t *(*move_object)(object_service_fn_t *os, creds_t *creds, char *src_path, char *dest_path);
  op_generic_t *(*symlink_object)(object_service_fn_t *os, creds_t *creds, char *src_path, char *dest_path, char *id);
  op_generic_t *(*hardlink_object)(object_service_fn_t *os, creds_t *creds, char *src_path, char *dest_path, char *id);

  os_object_iter_t *(*create_object_iter)(object_service_fn_t *os, creds_t *creds, os_regex_table_t *path, os_regex_table_t *obj_regex, int object_types, os_regex_table_t *attr, int recurse_dpeth, os_attr_iter_t **it, int v_max);
  os_object_iter_t *(*create_object_iter_alist)(object_service_fn_t *os, creds_t *creds, os_regex_table_t *path, os_regex_table_t *obj_regex, int object_types, int recurse_depth, char **key, void **val, int *v_size, int n_keys);
  int (*next_object)(os_object_iter_t *it, char **fname, int *prefix_len);
  void (*destroy_object_iter)(os_object_iter_t *it);

  op_generic_t *(*open_object)(object_service_fn_t *os, creds_t *creds, char *path, int mode, char *id, os_fd_t **fd, int max_wait);
  op_generic_t *(*close_object)(object_service_fn_t *os, os_fd_t *fd);
  op_generic_t *(*abort_open_object)(object_service_fn_t *os, op_generic_t *gop);

  op_generic_t *(*symlink_attr)(object_service_fn_t *os, creds_t *creds, char *src_path, char *key_src, os_fd_t *fd_dest, char *key_dest);
  op_generic_t *(*symlink_multiple_attrs)(object_service_fn_t *os, creds_t *creds, char **src_path, char **key_src, os_fd_t *fd_dest, char **key_dest, int n);

  op_generic_t *(*get_attr)(object_service_fn_t *os, creds_t *creds, os_fd_t *fd, char *key, void **val, int *v_size);
  op_generic_t *(*set_attr)(object_service_fn_t *os, creds_t *creds, os_fd_t *fd, char *key, void *val, int v_size);
  op_generic_t *(*move_attr)(object_service_fn_t *os, creds_t *creds, os_fd_t *fd, char *key_old, char *key_new);
  op_generic_t *(*copy_attr)(object_service_fn_t *os, creds_t *creds, os_fd_t *fd_src, char *key_src, os_fd_t *fd_dest, char *key_dest);
  op_generic_t *(*get_multiple_attrs)(object_service_fn_t *os, creds_t *creds, os_fd_t *fd, char **key, void **val, int *v_size, int n);
  op_generic_t *(*set_multiple_attrs)(object_service_fn_t *os, creds_t *creds, os_fd_t *fd, char **key, void **val, int *v_size, int n);
  op_generic_t *(*move_multiple_attrs)(object_service_fn_t *os, creds_t *creds, os_fd_t *fd, char **key_old, char **key_new, int n);
  op_generic_t *(*copy_multiple_attrs)(object_service_fn_t *os, creds_t *creds, os_fd_t *fd_src, char **key_src, os_fd_t *fd_dest, char **key_dest, int n);
  os_attr_iter_t *(*create_attr_iter)(object_service_fn_t *os, creds_t *creds, os_fd_t *fd, os_regex_table_t *attr, int v_max);
  int (*next_attr)(os_attr_iter_t *it, char **key, void **val, int *v_size);

  int (*add_virtual_attr)(os_virtual_attr_t *va, char *key, int type);
  void (*destroy_attr_iter)(os_attr_iter_t *it);
};


typedef object_service_fn_t *(os_create_t)(service_manager_t *authn_sm, service_manager_t *osaz_sm, thread_pool_context_t *tpc_cpu, thread_pool_context_t *tpc_unlimited, char *fname, char *section);

#define os_type(os) (os)->type
#define os_destroy_service(os) (os)->destroy_service(os)
#define os_cred_init(os, type, args) (os)->cred_init(os, type, args)
#define os_cred_destroy(os, c) (os)->cred_destroy(os, c)

#define os_create_fsck_iter(os, c, path, mode) (os)->create_fsck_iter(os, c, path, mode)
#define os_destroy_fsck_iter(os, it) (os)->destroy_fsck_iter(os, it)
#define os_next_fsck(os, it, fname, atype) (os)->next_fsck(os, it, fname, atype)
#define os_fsck_object(os, c, fname, ftype, mode) (os)->fsck_object(os, c, fname, ftype, mode)

#define os_exists(os, c, path) (os)->exists(os, c, path)
#define os_create_object(os, c, path, type, id) (os)->create_object(os, c, path, type, id)
#define os_remove_object(os, c, path) (os)->remove_object(os, c, path)
#define os_remove_regex_object(os, c, path, obj_regex, obj_types, depth) (os)->remove_regex_object(os, c, path, obj_regex, obj_types, depth)
#define os_regex_object_set_multiple_attrs(os, c, id, path, obj_regex, otypes, depth, key, val, v_size, n) (os)->regex_object_set_multiple_attrs(os, c, id, path, obj_regex, otypes, depth, key, val, v_size, n)
#define os_move_object(os, c, src_path, dest_path) (os)->move_object(os, c, src_path, dest_path)
#define os_symlink_object(os, c, src_path, dest_path, id) (os)->symlink_object(os, c, src_path, dest_path, id)
#define os_hardlink_object(os, c, src_path, dest_path, id) (os)->hardlink_object(os, c, src_path, dest_path, id)

#define os_create_object_iter(os, c, path, obj_regex, otypes, attr, depth, it_attr, v_max) (os)->create_object_iter(os, c, path, obj_regex, otypes, attr, depth, it_attr, v_max)
#define os_create_object_iter_alist(os, c, path, obj_regex, otypes, depth, key, val, v_size, n_keys) (os)->create_object_iter_alist(os, c, path, obj_regex, otypes, depth, key, val, v_size, n_keys)
#define os_next_object(os, it, fname, plen) (os)->next_object(it, fname, plen)
#define os_destroy_object_iter(os, it) (os)->destroy_object_iter(it)

#define os_open_object(os, c, path, mode, id, fd, max_wait) (os)->open_object(os, c, path, mode, id, fd, max_wait)
#define os_close_object(os, fd) (os)->close_object(os, fd)
#define os_abort_open_object(os, gop) (os)->abort_open_object(os, gop)

#define os_symlink_attr(os, c, src_path, key_src, fd_dest, key_dest) (os)->symlink_attr(os, c, src_path, key_src, fd_dest, key_dest)
#define os_symlink_multiple_attrs(os, c, src_path, key_src, fd_dest, key_dest, n) (os)->symlink_multiple_attrs(os, c, src_path, key_src, fd_dest, key_dest, n)

#define os_get_attr(os, c, fd, key, val, v_size) (os)->get_attr(os, c, fd, key, val, v_size)
#define os_set_attr(os, c, fd, key, val, v_size) (os)->set_attr(os, c, fd, key, val, v_size)
#define os_move_attr(os, c, fd, key_old, key_new) (os)->move_attr(os, c, fd, key_old, key_new)
#define os_copy_attr(os, c, fd_src, key_src, fd_dest, key_dest) (os)->copy_attr(os, c, fd_src, key_src, fd_dest, key_dest)
#define os_get_multiple_attrs(os, c, fd, keys, vals, v_sizes, n) (os)->get_multiple_attrs(os, c, fd, keys, vals, v_sizes, n)
#define os_set_multiple_attrs(os, c, fd, keys, vals, v_sizes, n) (os)->set_multiple_attrs(os, c, fd, keys, vals, v_sizes, n)
#define os_move_multiple_attrs(os, c, fd, key_old, key_new, n) (os)->move_multiple_attrs(os, c, fd, key_old, key_new, n)
#define os_copy_multiple_attrs(os, c, fd_src, key_src, fd_dest, key_dest, n) (os)->copy_multiple_attrs(os, c, fd_src, key_src, fd_dest, key_dest, n)
#define os_create_attr_iter(os, c, fd, attr, v_max) (os)->create_attr_iter(os, c, fd, attr, v_max)
#define os_next_attr(os, it, key, val, vsize) (os)->next_attr(it, key, val, vsize)
#define os_destroy_attr_iter(os, it) (os)->destroy_attr_iter(it)
#define os_destroy(os) (os)->destroy_service(os)


int os_local_filetype(char *path);
int os_regex_is_fixed(os_regex_table_t *regex);
void os_path_split(char *path, char **dir, char **file);
os_regex_table_t *os_regex_table_create(int n);
void os_regex_table_destroy(os_regex_table_t *table);
os_regex_table_t *os_path_glob2regex(char *path);
char *os_glob2regex(char *glob);
os_regex_table_t *os_regex2table(char *regex);


struct os_authz_s {
  void *priv;
  int (*object_create)(os_authz_t *osa, creds_t *c, char *path);
  int (*object_remove)(os_authz_t *osa, creds_t *c, char *path);
  int (*object_access)(os_authz_t *osa, creds_t *c, char *path, int mode);
  int (*attr_create)(os_authz_t *osa, creds_t *c, char *path, char *key);
  int (*attr_remove)(os_authz_t *osa, creds_t *c, char *path, char *key);
  int (*attr_access)(os_authz_t *osa, creds_t *c, char *path, char *key, int mode);
  void (*destroy)(os_authz_t *osa);
};


typedef os_authz_t *(osaz_create_t)(char *fname, char *section, object_service_fn_t *os);

#define osaz_object_create(osa, c, path) (osa)->object_create(osa, c, path)
#define osaz_object_remove(osa, c, path) (osa)->object_remove(osa, c, path)
#define osaz_object_access(osa, c, path, mode) (osa)->object_access(osa, c, path, mode)
#define osaz_attr_create(osa, c, path, key) (osa)->attr_create(osa, c, path, key)
#define osaz_attr_remove(osa, c, path, key) (osa)->attr_remove(osa, c, path, key)
#define osaz_attr_access(osa, c, path, key, mode) (osa)->attr_access(osa, c, path, key, mode)
#define osaz_destroy(osa) (osa)->destroy(osa)

//int install_os_authz_service(char *type, os_authz_t *(*osa_create)(char *fname, object_service_fn_t *os));
//os_authz_t *create_os_authz_service(char *type, char *fname, object_service_fn_t *os);


struct os_virtual_attr_s {
  char *attribute;
  void *priv;
  int (*get)(os_virtual_attr_t *va, object_service_fn_t *os, creds_t *creds, os_fd_t *fd, char *key, void **val, int *v_size, int *atype);
  int (*set)(os_virtual_attr_t *va, object_service_fn_t *os, creds_t *creds, os_fd_t *fd, char *key, void *val, int v_size, int *atype);
  int (*get_link)(os_virtual_attr_t *va, object_service_fn_t *os, creds_t *creds, os_fd_t *fd, char *key, void **val, int *v_size, int *atype);
};

#ifdef __cplusplus
}
#endif

#endif

