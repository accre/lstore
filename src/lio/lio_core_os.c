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

#define _log_module_index 189

#include "type_malloc.h"
#include "lio.h"
#include "log.h"
#include "string_token.h"

int ex_id_compare_fn(void *arg, skiplist_key_t *a, skiplist_key_t *b);
skiplist_compare_t ex_id_compare = {.fn=ex_id_compare_fn, .arg=NULL };

//***********************************************************************
// Core LFS functionality
//***********************************************************************

//************************************************************************
//  ex_id_compare_fn  - ID comparison function
//************************************************************************

int ex_id_compare_fn(void *arg, skiplist_key_t *a, skiplist_key_t *b)
{
  ex_id_t *al = (ex_id_t *)a;
  ex_id_t *bl = (ex_id_t *)b;

//log_printf(15, "a=" XIDT " b=" XIDT "\n", *al, *bl);
  if (*al<*bl) {
     return(-1);
  } else if (*al == *bl) {
     return(0);
  }

  return(1);
}

//***********************************************************************
// gop_lio_exists - Returns the filetype of the object or 0 if it
//   doesn't exist
//***********************************************************************

op_generic_t *gop_lio_exists(lio_config_t *lc, creds_t *creds, char *path)
{
  return(os_exists(lc->os, creds, path));
}

//***********************************************************************
// lio_exists - Returns the filetype of the object or 0 if it
//   doesn't exist
//***********************************************************************

int lio_exists(lio_config_t *lc, creds_t *creds, char *path)
{
  op_status_t status;

  status = gop_sync_exec_status(os_exists(lc->os, creds, path));
  return(status.error_code);
}

//*************************************************************************
//  gop_lio_create_object - Generate a create object task
//*************************************************************************

op_generic_t *gop_lio_create_object(lio_config_t *lc, creds_t *creds, char *path, int type, char *id)
{
  return(lioc_create_object(lc, creds, path, type, NULL, id));
}

//*************************************************************************
// gop_lio_remove_object
//*************************************************************************

op_generic_t *gop_lio_remove_object(lio_config_t *lc, creds_t *creds, char *path)
{
  return(lioc_remove_object(lc, creds, path, NULL, OS_OBJECT_ANY));
}

//*************************************************************************
// gop_lio_remove_regex_object
//*************************************************************************

op_generic_t *gop_lio_remove_regex_object(lio_config_t *lc, creds_t *creds, os_regex_table_t *rpath, os_regex_table_t *object_regex, int obj_types, int recurse_depth, int np)
{
  return(lioc_remove_regex_object(lc, creds, rpath, object_regex, obj_types, recurse_depth, np));

}

//*************************************************************************
// gop_lio_regex_object_set_multiple_attrs - Sets multiple object attributes
//*************************************************************************

op_generic_t *gop_lio_regex_object_set_multiple_attrs(lio_config_t *lc, creds_t *creds, char *id, os_regex_table_t *path, os_regex_table_t *object_regex, int object_types, int recurse_depth, char **key, void **val, int *v_size, int n)
{
  return(os_regex_object_set_multiple_attrs(lc->os, creds, id, path, object_regex, object_types, recurse_depth, key, val, v_size, n));
}

//*************************************************************************
// gop_lio_abort_regex_object_set_multiple_attrs - Aborts an ongoing set attr call
//*************************************************************************

op_generic_t *gop_lio_abort_regex_object_set_multiple_attrs(lio_config_t *lc, op_generic_t *gop)
{
   return(os_abort_regex_object_set_multiple_attrs(lc->os, gop));
}

//*************************************************************************
// gop_lio_move_object - Renames an object
//*************************************************************************

op_generic_t *gop_lio_move_object(lio_config_t *lc, creds_t *creds, char *src_path, char *dest_path)
{
  return(os_move_object(lc->os, creds, src_path, dest_path));
}


//*************************************************************************
//  gop_lio_symlink_object - Create a symbolic link to another object
//*************************************************************************

op_generic_t *gop_lio_symlink_object(lio_config_t *lc, creds_t *creds, char *src_path, char *dest_path, char *id)
{
  return(os_symlink_object(lc->os, creds, src_path, dest_path, id));
}


//*************************************************************************
//  gop_lio_hardlink_object - Create a hard link to another object
//*************************************************************************

op_generic_t *gop_lio_hardlink_object(lio_config_t *lc, creds_t *creds, char *src_path, char *dest_path, char *id)
{
  return(os_hardlink_object(lc->os, creds, src_path, dest_path, id));
}


//*************************************************************************
// lio_create_object_iter - Creates an object iterator using a regex for the attribute list
//*************************************************************************

os_object_iter_t *lio_create_object_iter(lio_config_t *lc, creds_t *creds, os_regex_table_t *path, os_regex_table_t *obj_regex, int object_types, os_regex_table_t *attr, int recurse_dpeth, os_attr_iter_t **it, int v_max)
{
   return(os_create_object_iter(lc->os, creds, path, obj_regex, object_types, attr, recurse_dpeth, it, v_max));
}

//*************************************************************************
// lio_create_object_iter_alist - Creates an object iterator using a fixed attribute list
//*************************************************************************

os_object_iter_t *lio_create_object_iter_alist(lio_config_t *lc, creds_t *creds, os_regex_table_t *path, os_regex_table_t *obj_regex, int object_types, int recurse_depth, char **key, void **val, int *v_size, int n_keys)
{
   return(os_create_object_iter_alist(lc->os, creds, path, obj_regex, object_types, recurse_depth, key, val, v_size, n_keys));
}


//*************************************************************************
// lio_next_object - Returns the next iterator object
//*************************************************************************

int lio_next_object(lio_config_t *lc, os_object_iter_t *it, char **fname, int *prefix_len)
{
  return(os_next_object(lc->os, it, fname, prefix_len));
}


//*************************************************************************
// lio_destroy_object_iter - Destroy's an object iterator
//*************************************************************************

void lio_destroy_object_iter(lio_config_t *lc, os_object_iter_t *it)
{
  os_destroy_object_iter(lc->os, it);
}


//***********************************************************************
// lio_*_attrs - Get/Set LIO attribute routines
//***********************************************************************

typedef struct {
  lio_config_t *lc;
  creds_t *creds;
  const char *path;
  char *id;
  char **mkeys;
  void **mvals;
  int *mv_size;
  char *skey;
  void *sval;
  int *sv_size;
  int n_keys;
} lio_attrs_op_t;

//***********************************************************************
// lio_get_multiple_attrs
//***********************************************************************

int lio_get_multiple_attrs(lio_config_t *lc, creds_t *creds, const char *path, char *id, char **key, void **val, int *v_size, int n_keys)
{
  int err, serr;
  os_fd_t *fd;

  err = gop_sync_exec(os_open_object(lc->os, creds, (char *)path, OS_MODE_READ_IMMEDIATE, id, &fd, lc->timeout));
  if (err != OP_STATE_SUCCESS) {
     log_printf(1, "ERROR opening object=%s\n", path);
     return(err);
  }

  //** IF the attribute doesn't exist *val == NULL an *v_size = 0
  serr = gop_sync_exec(os_get_multiple_attrs(lc->os, creds, fd, key, val, v_size, n_keys));

  //** Close the parent
  err = gop_sync_exec(os_close_object(lc->os, fd));
  if (err != OP_STATE_SUCCESS) {
     log_printf(1, "ERROR closing object=%s\n", path);
  }

  if (serr != OP_STATE_SUCCESS) {
      log_printf(1, "ERROR getting attributes object=%s\n", path);
      err = OP_STATE_FAILURE;
  }

  return(err);
}

//***********************************************************************

op_status_t lio_get_multiple_attrs_fn(void *arg, int id)
{
   lio_attrs_op_t *op = (lio_attrs_op_t *)arg;
   op_status_t status;
   int err;

   err = lio_get_multiple_attrs(op->lc, op->creds, (char *)op->path, op->id, op->mkeys, op->mvals, op->mv_size, op->n_keys);
   status.error_code = err;
   status.op_status = (err == 0) ? OP_STATE_SUCCESS : OP_STATE_FAILURE;
   return(status);
}

//***********************************************************************

op_generic_t *gop_lio_get_multiple_attrs(lio_config_t *lc, creds_t *creds, const char *path, char *id, char **key, void **val, int *v_size, int n_keys)
{
  lio_attrs_op_t *op;
  type_malloc_clear(op, lio_attrs_op_t, 1);

  op->lc = lc;
  op->creds = creds;
  op->path = path;
  op->id = id;
  op->mkeys = key;
  op->mvals = val;
  op->mv_size = v_size;
  op->n_keys = n_keys;

  return(new_thread_pool_op(lc->tpc_unlimited, NULL, lio_get_multiple_attrs_fn, (void *)op, free, 1));
}

//***********************************************************************
// lio_get_attr - Returns an attribute
//***********************************************************************

int lio_get_attr(lio_config_t *lc, creds_t *creds, const char *path, char *id, char *key, void **val, int *v_size)
{
  int err, serr;
  os_fd_t *fd;

  err = gop_sync_exec(os_open_object(lc->os, creds, (char *)path, OS_MODE_READ_IMMEDIATE, id, &fd, lc->timeout));
  if (err != OP_STATE_SUCCESS) {
     log_printf(1, "ERROR opening object=%s\n", path);
     return(err);
  }

  //** IF the attribute doesn't exist *val == NULL an *v_size = 0
  serr = gop_sync_exec(os_get_attr(lc->os, creds, fd, key, val, v_size));

  //** Close the parent
  err = gop_sync_exec(os_close_object(lc->os, fd));
  if (err != OP_STATE_SUCCESS) {
     log_printf(1, "ERROR closing object=%s\n", path);
  }

  if (serr != OP_STATE_SUCCESS) {
      log_printf(1, "ERROR getting attribute object=%s\n", path);
      err = OP_STATE_FAILURE;
  }

  return(err);
}

//***********************************************************************

op_status_t lio_get_attr_fn(void *arg, int id)
{
   lio_attrs_op_t *op = (lio_attrs_op_t *)arg;
   op_status_t status;
   int err;

   err = lio_get_attr(op->lc, op->creds, op->path, op->id, op->skey, op->sval, op->sv_size);
   status.error_code = 0;
   status.op_status = err;
   return(status);
}

//***********************************************************************

op_generic_t *gop_lio_get_attr(lio_config_t *lc, creds_t *creds, const char *path, char *id, char *key, void **val, int *v_size)
{
  lio_attrs_op_t *op;
  type_malloc_clear(op, lio_attrs_op_t, 1);

  op->lc = lc;
  op->creds = creds;
  op->path = path;
  op->id = id;
  op->skey = key;
  op->sval = val;
  op->sv_size = v_size;

  return(new_thread_pool_op(lc->tpc_unlimited, NULL, lio_get_attr_fn, (void *)op, free, 1));
}

//***********************************************************************
// lio_set_multiple_attrs_real - Returns an attribute
//***********************************************************************

int lio_set_multiple_attrs_real(lio_config_t *lc, creds_t *creds, const char *path, char *id, char **key, void **val, int *v_size, int n)
{
  int err, serr;
  os_fd_t *fd;

  err = gop_sync_exec(os_open_object(lc->os, creds, (char *)path, OS_MODE_READ_IMMEDIATE, id, &fd, lc->timeout));
  if (err != OP_STATE_SUCCESS) {
     log_printf(1, "ERROR opening object=%s\n", path);
     return(err);
  }

  serr = gop_sync_exec(os_set_multiple_attrs(lc->os, creds, fd, key, val, v_size, n));

  //** Close the parent
  err = gop_sync_exec(os_close_object(lc->os, fd));
  if (err != OP_STATE_SUCCESS) {
     log_printf(1, "ERROR closing object=%s\n", path);
  }

  if (serr != OP_STATE_SUCCESS) {
      log_printf(1, "ERROR setting attributes object=%s\n", path);
      err = OP_STATE_FAILURE;
  }

  return(err);
}

//***********************************************************************
// lio_set_multiple_attrs - Returns an attribute
//***********************************************************************

int lio_set_multiple_attrs(lio_config_t *lc, creds_t *creds, const char *path, char *id, char **key, void **val, int *v_size, int n)
{
  int err;

  err = lio_set_multiple_attrs_real(lc, creds, path, id, key, val, v_size, n);
  if (err != OP_STATE_SUCCESS) {  //** Got an error
     sleep(1);  //** Wait a bit before retrying
     err = lio_set_multiple_attrs_real(lc, creds, path, id, key, val, v_size, n);
  }

  return(err);
}

//***********************************************************************

op_status_t lio_set_multiple_attrs_fn(void *arg, int id)
{
   lio_attrs_op_t *op = (lio_attrs_op_t *)arg;
   op_status_t status;
   int err;

   err = lio_set_multiple_attrs(op->lc, op->creds, op->path, op->id, op->mkeys, op->mvals, op->mv_size, op->n_keys);
   status.error_code = err;
   status.op_status = (err == 0) ? OP_STATE_SUCCESS : OP_STATE_FAILURE;
   return(status);
}

//***********************************************************************

op_generic_t *gop_lio_set_multiple_attrs(lio_config_t *lc, creds_t *creds, const char *path, char *id, char **key, void **val, int *v_size, int n_keys)
{
  lio_attrs_op_t *op;
  type_malloc_clear(op, lio_attrs_op_t, 1);

  op->lc = lc;
  op->creds = creds;
  op->path = path;
  op->id = id;
  op->mkeys = key;
  op->mvals = val;
  op->mv_size = v_size;
  op->n_keys = n_keys;

  return(new_thread_pool_op(lc->tpc_unlimited, NULL, lio_set_multiple_attrs_fn, (void *)op, free, 1));
}

//***********************************************************************
// lio_set_attr_real - Sets an attribute
//***********************************************************************

int lio_set_attr_real(lio_config_t *lc, creds_t *creds, const char *path, char *id, char *key, void *val, int v_size)
{
  int err, serr;
  os_fd_t *fd;

  err = gop_sync_exec(os_open_object(lc->os, creds, (char *)path, OS_MODE_READ_IMMEDIATE, id, &fd, lc->timeout));
  if (err != OP_STATE_SUCCESS) {
     log_printf(1, "ERROR opening object=%s\n", path);
     return(err);
  }

  serr = gop_sync_exec(os_set_attr(lc->os, creds, fd, key, val, v_size));

  //** Close the parent
  err = gop_sync_exec(os_close_object(lc->os, fd));
  if (err != OP_STATE_SUCCESS) {
     log_printf(1, "ERROR closing object=%s\n", path);
  }

  if (serr != OP_STATE_SUCCESS) {
      log_printf(1, "ERROR setting attribute object=%s\n", path);
      err = OP_STATE_FAILURE;
  }

  return(err);
}

//***********************************************************************
// lio_set_attr - Sets a single attribute
//***********************************************************************

int lio_set_attr(lio_config_t *lc, creds_t *creds, const char *path, char *id, char *key, void *val, int v_size)
{
  int err;

  err = lio_set_attr_real(lc, creds, path, id, key, val, v_size);
  if (err != OP_STATE_SUCCESS) {  //** Got an error
     sleep(1);  //** Wait a bit before retrying
     err = lio_set_attr_real(lc, creds, path, id, key, val, v_size);
  }

  return(err);
}

//***********************************************************************

op_status_t lio_set_attr_fn(void *arg, int id)
{
   lio_attrs_op_t *op = (lio_attrs_op_t *)arg;
   op_status_t status;

   status.op_status = lio_set_attr(op->lc, op->creds, op->path, op->id, op->skey, op->sval, op->n_keys); //** NOTE: n_keys = v_size
   status.error_code = 0;
   return(status);
}

//***********************************************************************

op_generic_t *gop_lio_set_attr(lio_config_t *lc, creds_t *creds, const char *path, char *id, char *key, void *val, int v_size)
{
  lio_attrs_op_t *op;
  type_malloc_clear(op, lio_attrs_op_t, 1);

  op->lc = lc;
  op->creds = creds;
  op->path = path;
  op->id = id;
  op->skey = key;
  op->sval = val;
  op->n_keys = v_size;  //** Double use for the vaiable

  return(new_thread_pool_op(lc->tpc_unlimited, NULL, lio_set_attr_fn, (void *)op, free, 1));
}

//***********************************************************************
// lio_create_attr_iter - Creates an attribute iterator
//***********************************************************************

os_attr_iter_t *lio_create_attr_iter(lio_config_t *lc, creds_t *creds, const char *path, os_regex_table_t *attr, int v_max)
{
  return(os_create_attr_iter(lc->os, creds, (char *)path, attr, v_max));
}

//***********************************************************************
// lio_next_attr - Returns the next attribute from the iterator
//***********************************************************************

int lio_next_attr(lio_config_t *lc, os_attr_iter_t *it, char **key, void **val, int *v_size)
{
  return(os_next_attr(lc->os, it, key, val, v_size));
}

//***********************************************************************
// lio_destroy_attr_iter - Destroy the attribute iterator
//***********************************************************************

void lio_destroy_attr_iter(lio_config_t *lc, os_attr_iter_t *it)
{
  os_destroy_attr_iter(lc->os, it);
}
