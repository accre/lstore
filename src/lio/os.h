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
// Generic object service
//***********************************************************************

#ifndef _OBJECT_SERVICE_H_
#define _OBJECT_SERVICE_H_

#include <gop/opque.h>
#include <gop/tp.h>
#include <lio/os.h>
#include <lio/visibility.h>
#include <regex.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "authn.h"
#include "ex3/types.h"
#include "service_manager.h"

#ifdef __cplusplus
extern "C" {
#endif

#define OS_AVAILABLE "os_available"
#define OSAZ_AVAILABLE "osaz_available"

#define OS_MODE_WRITE_IMMEDIATE 1
#define OS_MODE_READ_BLOCKING   2
#define OS_MODE_WRITE_BLOCKING  3

#define OS_VATTR_NORMAL  0   //** Normal virtual attribute.  Works the same as a non-virtual attribute.
#define OS_VATTR_PREFIX  1   //** Routine is called  whenever the VA prefix matches the attr.  Does not show up in iterators.

#define OS_CREDS_INI_TYPE 0  //** Load creds from file

struct lio_os_attr_list_t {
    int q_mode;
    char *attr;
};

#define os_type(os) (os)->type
#define os_destroy_service(os) (os)->destroy_service(os)
#define os_cred_init(os, type, args) (os)->cred_init(os, type, args)
#define os_cred_destroy(os, c) (os)->cred_destroy(os, c)

#define os_exists(os, c, path) (os)->exists(os, c, path)
#define os_create_object(os, c, path, type, id) (os)->create_object(os, c, path, type, id)
#define os_remove_object(os, c, path) (os)->remove_object(os, c, path)
#define os_remove_regex_object(os, c, path, obj_regex, obj_types, depth) (os)->remove_regex_object(os, c, path, obj_regex, obj_types, depth)
#define os_abort_remove_regex_object(os, gop) (os)->abort_remove_regex_object(os, gop)
#define os_regex_object_set_multiple_attrs(os, c, id, path, obj_regex, otypes, depth, key, val, v_size, n) (os)->regex_object_set_multiple_attrs(os, c, id, path, obj_regex, otypes, depth, key, val, v_size, n)
#define os_abort_regex_object_set_multiple_attrs(os, gop) (os)->abort_regex_object_set_multiple_attrs(os, gop)
#define os_move_object(os, c, src_path, dest_path) (os)->move_object(os, c, src_path, dest_path)
#define os_symlink_object(os, c, src_path, dest_path, id) (os)->symlink_object(os, c, src_path, dest_path, id)
#define os_hardlink_object(os, c, src_path, dest_path, id) (os)->hardlink_object(os, c, src_path, dest_path, id)

#define os_create_object_iter(os, c, path, obj_regex, otypes, attr, depth, it_attr, v_max) (os)->create_object_iter(os, c, path, obj_regex, otypes, attr, depth, it_attr, v_max)
#define os_create_object_iter_alist(os, c, path, obj_regex, otypes, depth, key, val, v_size, n_keys) (os)->create_object_iter_alist(os, c, path, obj_regex, otypes, depth, key, val, v_size, n_keys)
#define os_next_object(os, it, fname, plen) (os)->next_object(it, fname, plen)
#define os_destroy_object_iter(os, it) (os)->destroy_object_iter(it)

#define os_abort_open_object(os, gop) (os)->abort_open_object(os, gop)

#define os_symlink_attr(os, c, src_path, key_src, fd_dest, key_dest) (os)->symlink_attr(os, c, src_path, key_src, fd_dest, key_dest)

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

lio_os_regex_table_t *os_regex_table_create(int n);
int os_regex_table_pack(lio_os_regex_table_t *regex, unsigned char *buffer, int bufsize);
lio_os_regex_table_t *os_regex_table_unpack(unsigned char *buffer, int bufsize, int *used);

struct lio_os_authz_t {
    void *priv;
    int (*object_create)(lio_os_authz_t *osa, lio_creds_t *c, char *path);
    int (*object_remove)(lio_os_authz_t *osa, lio_creds_t *c, char *path);
    int (*object_access)(lio_os_authz_t *osa, lio_creds_t *c, char *path, int mode);
    int (*attr_create)(lio_os_authz_t *osa, lio_creds_t *c, char *path, char *key);
    int (*attr_remove)(lio_os_authz_t *osa, lio_creds_t *c, char *path, char *key);
    int (*attr_access)(lio_os_authz_t *osa, lio_creds_t *c, char *path, char *key, int mode);
    void (*destroy)(lio_os_authz_t *osa);
};

typedef lio_object_service_fn_t *(os_create_t)(lio_service_manager_t *ess, tbx_inip_file_t *ifd, char *section);
typedef lio_os_authz_t *(osaz_create_t)(lio_service_manager_t *ess, tbx_inip_file_t *ifd, char *section, lio_object_service_fn_t *os);

#define osaz_object_create(osa, c, path) (osa)->object_create(osa, c, path)
#define osaz_object_remove(osa, c, path) (osa)->object_remove(osa, c, path)
#define osaz_object_access(osa, c, path, mode) (osa)->object_access(osa, c, path, mode)
#define osaz_attr_create(osa, c, path, key) (osa)->attr_create(osa, c, path, key)
#define osaz_attr_remove(osa, c, path, key) (osa)->attr_remove(osa, c, path, key)
#define osaz_attr_access(osa, c, path, key, mode) (osa)->attr_access(osa, c, path, key, mode)
#define osaz_destroy(osa) (osa)->destroy(osa)

struct lio_os_virtual_attr_t {
    char *attribute;
    void *priv;
    int (*get)(lio_os_virtual_attr_t *va, lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *fd, char *key, void **val, int *v_size, int *atype);
    int (*set)(lio_os_virtual_attr_t *va, lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *fd, char *key, void *val, int v_size, int *atype);
    int (*get_link)(lio_os_virtual_attr_t *va, lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *fd, char *key, void **val, int *v_size, int *atype);
};

int os_local_filetype_stat(char *path, struct stat *stat_link, struct stat *stat_object);

#ifdef __cplusplus
}
#endif

#endif
