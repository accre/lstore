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

#ifndef _OBJECT_SERVICE_H_
#define _OBJECT_SERVICE_H_

#ifdef __cplusplus
extern "C" {
#endif

#define OS_OBJECT_FILE 0
#define OS_OBJECT_DIR  1
#define OS_OBJECT_LINK 2

typedef struct {
  int q_mode;
  char *attr;
} os_attr_list_t;

typedef void os_creds_t;

typedef void os_fd_t;
typedef void os_attr_iter_t;
typedef void os_object_iter_t;

typedef struct {
  char *expression;
  regex_t compiled;
} os_regex_entry_t;

typedef struct {
  int n;
  os_regex_entry_t *regex_entry;
} os_regex_table_t;


//typedef struct {
//  char *key;
//  void *value;
//  int len;
//} os_attr_t;

struct object_service_fn_s;
typedef struct object_service_fn_s object_service_fn_t;

struct object_service_fn_s {
  void *priv;
  char *type;
  void (*destroy_service)(object_service_fn_t *os);

  os_creds_t *(*login)(object_service_fn_t *os, char *userid, int type, void *arg);
  void (*logout)(object_service_fn_t *os, os_creds_t *creds);

  op_generic_t *(*create_object)(object_service_fn_t *os, os_creds_t *creds, char *path, int type);
  op_generic_t *(*remove_object)(object_service_fn_t *os, os_creds_t *creds, char *path);
  op_generic_t *(*remove_regex_object)(object_service_fn_t *os, os_creds_t *creds, os_regex_table_t *path, int recurse_depth);
  op_generic_t *(*move_object)(object_service_fn_t *os, os_creds_t *creds, char *src_path, char *dest_path);
  op_generic_t *(*link_object)(object_service_fn_t *os, os_creds_t *creds, char *src_path, char *dest_path);

  os_object_iter_t *(*create_object_iter)(object_service_fn_t *os, os_creds_t *creds, os_regex_table_t *path, os_regex_table_t *attr, int recurse_depth, int v_max);
  int (*next_object)(os_object_iter_t *it, char **fname, os_attr_iter_t **it_attr);
  void (*destroy_object_iter)(os_object_iter_t *it);
  
  op_generic_t *(*open_object)(object_service_fn_t *os, os_creds_t *creds, char *path, os_fd_t **fd);
  op_generic_t *(*close_object)(object_service_fn_t *os, os_fd_t *fd);

  op_generic_t *(*get_attr)(object_service_fn_t *os, os_creds_t *creds, os_fd_t *fd, char *key, void **val, int *v_size);
  op_generic_t *(*set_attr)(object_service_fn_t *os, os_creds_t *creds, os_fd_t *fd, char *key, void *val, int v_size);
  op_generic_t *(*get_multiple_attrs)(object_service_fn_t *os, os_creds_t *creds, os_fd_t *fd, char **key, void **val, int *v_size, int n);
  op_generic_t *(*set_multiple_attrs)(object_service_fn_t *os, os_creds_t *creds, os_fd_t *fd, char **key, void **val, int *v_size, int n);
  os_attr_iter_t *(*create_attr_iter)(object_service_fn_t *os, os_creds_t *creds, os_fd_t *fd, os_regex_table_t *attr, int v_max);
  int (*next_attr)(os_attr_iter_t *it, char **key, void **val, int *v_size);
  void (*destroy_attr_iter)(os_attr_iter_t *it);
};

#define os_type(os) (os)->type
#define os_destroy_service(os) (os)->destroy_service(os)
#define os_login(os, userid, type, arg) (os)->(os, userid, type, arg)
#define os_logout(os, c) (os)->logout(os, c)

#define os_create_object(os, c, path, type) (os)->create_object(os, c, path, type)
#define os_remove_object(os, c, path) (os)->remove_object(os, c, path)
#define os_remove_regex_object(os, c, path, depth) (os)->remove_regex_object(os, c, path, depth)
#define os_move_object(os, c, src_path, dest_path) (os)->move_object(os, c, src_path, dest_path)
#define os_link_object(os, c, src_path, dest_path) (os)->link_object(os, c, src_path, dest_path)

#define os_create_object_iter(os, c, path, attr, depth) (os)->create_pbject_iter(os, c, path, attr, depth, v_max)
#define os_next_object(os, it) (os)->next_object(it)
#define os_destroy_object_iter(os, it) (os)->destroy_object_iter(it)

#define os_open_object(os, c, path, fd) (os)->open_object(os, c, path, fd)
#define os_close_object(os, c, fd) (os)->close_object(os, c, fd)

#define os_get_attr(os, c, fd, key, val, v_size) (os)->get_attr(os, c, fd, key, val, v_size)
#define os_set_attr(os, c, fd, key, val, v_size) (os)->set_attr(os, c, fd, key, val, v_size)
#define os_get_multiple_attrs(os, c, fd, keys, vals, v_sizes, n) (os)->get_multiple_attrs(os, c, fd, keys, vals, v_sizes, n)
#define os_set_multiple_attrs(os, c, fd, keys, vals, v_sizes, n) (os)->set_multiple_attrs(os, c, fd, keys, vals, v_sizes, n)
#define os_create_attr_iter(os, c, fd, attr) (os)->create_attr_iter(os, c, fd, attr)
#define os_next_attr(os, it) (os)->next_attr(it)
#define os_destroy_attr_iter(os, it) (os)->destroy_attr_iter(it)
#define os_destroy(os) (os)->destroy_service(os)

int install_object_service(char *type, object_service_fn_t *(*create)(void *arg, char *fname), void *arg);
object_service_fn_t *create_object_service(char *type, char *fname);

os_regex_table_t *os_regex_table_crete(int n);
void os_regex_table_destroy(os_regex_table_t *table);
os_regex_table_t *os_path_glob2regex(char *path);
char *os_glob2regex(char *glob);



#ifdef __cplusplus
}
#endif

#endif

