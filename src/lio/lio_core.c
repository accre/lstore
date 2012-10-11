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

//***********************************************************************
// Core LIO functionality
//***********************************************************************


#define  _n_lioc_file_keys 7
#define  _n_lioc_dir_keys 6
#define _n_lioc_create_keys 7
#define _n_fsck_keys 4

static char *_lioc_create_keys[] = { "system.owner", "os.timestamp.system.create", "os.timestamp.system.modify_data", "os.timestamp.system.modify_attr", "system.inode", "system.exnode", "system.exnode.size"};
static char *_fsck_keys[] = { "system.owner", "system.inode", "system.exnode", "system.exnode.size" };

typedef struct {
  lio_config_t *lc;
  creds_t *creds;
  char *src_path;
  char *dest_path;
  char *id;
  char *ex;
  int type;
} lioc_mk_mv_rm_t;

typedef struct {
  lio_config_t *lc;
  creds_t *creds;
  os_regex_table_t *rpath;
  os_regex_table_t *robj;
  int recurse_depth;
  int obj_types;
  int np;
} lioc_remove_regex_t;

typedef struct {
  char *fname;
  char *val[_n_fsck_keys];
  int v_size[_n_fsck_keys];
  int ftype;
} lioc_fsck_task_t;

typedef struct {
  lio_config_t *lc;
  creds_t *creds;
  char *path;
  os_regex_table_t *regex;
  os_object_iter_t *it;
  int owner_mode;
  int exnode_mode;
  char *owner;
  char *val[_n_fsck_keys];
  int v_size[_n_fsck_keys];
  lioc_fsck_task_t *task;
  opque_t *q;
  int n;
  int firsttime;
} lioc_fsck_iter_t;

typedef struct {
  lio_config_t *lc;
  creds_t *creds;
  char *path;
  char **val;
  int *v_size;
  int ftype;
  int full;
  int owner_mode;
  int exnode_mode;
  char *owner;
} lioc_fsck_check_t;


//***********************************************************************
// lioc_free_mk_mv_rm
//***********************************************************************

void lioc_free_mk_mv_rm(void *arg)
{
  lioc_mk_mv_rm_t *op = (lioc_mk_mv_rm_t *)arg;

  if (op->src_path != NULL) free(op->src_path);
  if (op->dest_path != NULL) free(op->dest_path);
  if (op->id != NULL) free(op->id);
  if (op->ex != NULL) free(op->ex);

  free(op);
}

//***********************************************************************
// lioc_exists - Returns the filetype of the object or 0 if it
//   doesn't exist
//***********************************************************************

int lioc_exists(lio_config_t *lc, creds_t *creds, char *path)
{
  int ftype;
  op_generic_t *gop;
  op_status_t status;

  gop = os_exists(lc->os, creds, path);
  gop_waitall(gop);
  status = gop_get_status(gop);
  ftype = status.error_code;
  gop_free(gop, OP_DESTROY);

  return(ftype);
}

//***********************************************************************
//  lio_parse_path - Parses a path ofthe form: user@service:/my/path
//        The user and service are optional
//
//  Returns 1 if @: are encountered and 0 otherwise
//***********************************************************************

int lio_parse_path(char *startpath, char **user, char **service, char **path)
{
  int i, j, found, n, ptype;

  *user = *service = *path = NULL;
  n = strlen(startpath);
  ptype = 0;
  found = -1;
  for (i=0; i<n; i++) {
     if (startpath[i] == '@') { found = i; ptype = 1; break; }
  }

  if (found == -1) {*path = strdup(startpath); return(ptype); }

  if (found > 0) { //** Got a valid user
    *user = strndup(startpath, found);
  }

  j = found+1;
  found = -1;
  for (i=j; i<n; i++) {
     if (startpath[i] == ':') { found = i; break; }
  }

  if (found == -1) {  //**No path.  Just a service
     if (j < n) {
       *service = strdup(&(startpath[j]));
     }
     return(ptype);
  }

  i = found - j;
  *service = (i == 0) ? NULL : strndup(&(startpath[j]), i);

  //** Everything else is the path
  j = found + 1;
  if (found < n) {
     *path = strdup(&(startpath[j]));
  }

  return(ptype);
}

//***********************************************************************
// lio_set_timestamp - Sets the timestamp val/size for a attr put
//***********************************************************************

void lio_set_timestamp(char *id, char **val, int *v_size)
{
  *val = id;
  *v_size = (id == NULL) ? 0 : strlen(id);
  return;
}

//***********************************************************************
// lio_get_timestamp - Splits the timestamp ts/id field
//***********************************************************************

void lio_get_timestamp(char *val, int *timestamp, char **id)
{
  char *bstate;
  int fin;

  *timestamp = 0;
  sscanf(string_token(val, "|", &bstate, &fin), "%d", timestamp);
  if (id != NULL) *id = string_token(NULL, "|", &bstate, &fin);
  return;
}

//***********************************************************************
// lioc_get_multiple_attrs - Returns Multiple attributes
//***********************************************************************

int lioc_get_multiple_attrs(lio_config_t *lc, creds_t *creds, char *path, char *id, char **key, void **val, int *v_size, int n_keys)
{
  int err;
  os_fd_t *fd;

  err = gop_sync_exec(os_open_object(lc->os, creds, path, OS_MODE_READ_IMMEDIATE, id, &fd, lc->timeout));
  if (err != OP_STATE_SUCCESS) {
     log_printf(15, "ERROR opening object=%s\n", path);
     return(err);
  }

  //** IF the attribute doesn't exist *val == NULL an *v_size = 0
  gop_sync_exec(os_get_multiple_attrs(lc->os, creds, fd, key, val, v_size, n_keys));

  //** Close the parent
  err = gop_sync_exec(os_close_object(lc->os, fd));
  if (err != OP_STATE_SUCCESS) {
     log_printf(15, "ERROR closing object=%s\n", path);
  }

  return(err);
}

//***********************************************************************
// lioc_get_attr - Returns an attribute
//***********************************************************************

int lioc_get_attr(lio_config_t *lc, creds_t *creds, char *path, char *id, char *key, void **val, int *v_size)
{
  int err;
  os_fd_t *fd;

  err = gop_sync_exec(os_open_object(lc->os, creds, path, OS_MODE_READ_IMMEDIATE, id, &fd, lc->timeout));
  if (err != OP_STATE_SUCCESS) {
     log_printf(15, "ERROR opening object=%s\n", path);
     return(err);
  }

  //** IF the attribute doesn't exist *val == NULL an *v_size = 0
  gop_sync_exec(os_get_attr(lc->os, creds, fd, key, val, v_size));

  //** Close the parent
  err = gop_sync_exec(os_close_object(lc->os, fd));
  if (err != OP_STATE_SUCCESS) {
     log_printf(15, "ERROR closing object=%s\n", path);
  }

  return(err);
}

//***********************************************************************
// lioc_set_multiple_attrs - Returns an attribute
//***********************************************************************

int lioc_set_multiple_attrs(lio_config_t *lc, creds_t *creds, char *path, char *id, char **key, void **val, int *v_size, int n)
{
  int err;
  os_fd_t *fd;

  err = gop_sync_exec(os_open_object(lc->os, creds, path, OS_MODE_READ_IMMEDIATE, id, &fd, lc->timeout));
  if (err != OP_STATE_SUCCESS) {
     log_printf(15, "ERROR opening object=%s\n", path);
     return(err);
  }

  gop_sync_exec(os_set_multiple_attrs(lc->os, creds, fd, key, val, v_size, n));

  //** Close the parent
  err = gop_sync_exec(os_close_object(lc->os, fd));
  if (err != OP_STATE_SUCCESS) {
     log_printf(15, "ERROR closing object=%s\n", path);
  }

  return(err);
}

//***********************************************************************
// lioc_set_attr - Returns an attribute
//***********************************************************************

int lioc_set_attr(lio_config_t *lc, creds_t *creds, char *path, char *id, char *key, void *val, int v_size)
{
  int err;
  os_fd_t *fd;

  err = gop_sync_exec(os_open_object(lc->os, creds, path, OS_MODE_READ_IMMEDIATE, id, &fd, lc->timeout));
  if (err != OP_STATE_SUCCESS) {
     log_printf(15, "ERROR opening object=%s\n", path);
     return(err);
  }

  gop_sync_exec(os_set_attr(lc->os, creds, fd, key, val, v_size));

  //** Close the parent
  err = gop_sync_exec(os_close_object(lc->os, fd));
  if (err != OP_STATE_SUCCESS) {
     log_printf(15, "ERROR closing object=%s\n", path);
  }

  return(err);
}


//***********************************************************************
// lioc_remove_object - Removes an object
//***********************************************************************

op_status_t lioc_remove_object_fn(void *arg, int id)
{
  lioc_mk_mv_rm_t *op = (lioc_mk_mv_rm_t *)arg;
  char *ex_data, *val[2];
  char *hkeys[] = { "os.link_count", "system.exnode" };
  exnode_exchange_t *exp;
  exnode_t *ex;
  int err, v_size, ex_remove, vs[2], n;
  op_status_t status = op_success_status;

  //** First remove and data associated with the object
  v_size = -op->lc->max_attr;

  //** If no object type need to retrieve it
  if (op->type == 0) op->type = lioc_exists(op->lc, op->creds, op->src_path);

  ex_remove = 0;
  if ((op->type & OS_OBJECT_HARDLINK) > 0) { //** Got a hard link so check if we do a data removal
     val[0] = val[1] = NULL; vs[0] = vs[1] = -op->lc->max_attr;
     lioc_get_multiple_attrs(op->lc, op->creds, op->src_path, op->id, hkeys, (void **)val, vs, 2);

     if (val[0] == NULL) {
        log_printf(15, "Missing link count for fname=%s\n", op->src_path);
        if (val[1] != NULL) free(val[1]);
        return(op_failure_status);
     }

     n = 100;
     sscanf(val[0], "%d", &n);
     free(val[0]);
     if (n <= 1) {
        ex_remove = 1;
        if (op->ex == NULL) {
           op->ex = val[1];
        } else {
           if (val[1] != NULL) free(val[1]);
        }
     } else {
       if (val[1] != NULL) free(val[1]);
     }
  } else if ((op->type & (OS_OBJECT_SYMLINK|OS_OBJECT_DIR)) == 0) {
    ex_remove = 1;
  }

  ex_data = op->ex;
  if ((op->ex == NULL) && (ex_remove == 1)) {
     lioc_get_attr(op->lc, op->creds, op->src_path, op->id, "system.exnode", (void **)&ex_data, &v_size);
  }

  //** Load the exnode and remove it if needed.
  //** Only done for normal files.  No links or dirs
  if ((ex_remove == 1) && (ex_data != NULL)) {
     //** Deserialize it
     exp = exnode_exchange_create(EX_TEXT);
     exp->text = ex_data;
     ex = exnode_create();
     exnode_deserialize(ex, exp, op->lc->ess);

     //** Execute the remove operation
     err = gop_sync_exec(exnode_remove(op->lc->tpc_unlimited, ex, op->lc->da, op->lc->timeout));
     if (err != OP_STATE_SUCCESS) {
        log_printf(15, "ERROR removing data for object fname=%s\n", op->src_path);
        status = op_failure_status;
     }

     //** Clean up
     if (op->ex != NULL) exp->text = NULL;  //** The inital exnode is free() by the TP op
     exnode_exchange_destroy(exp);
     exnode_destroy(ex);
  }

  //** Now we can remove the OS entry
  err = gop_sync_exec(os_remove_object(op->lc->os, op->creds, op->src_path));
  if (err != OP_STATE_SUCCESS) {
     log_printf(0, "ERROR: removing file: %s err=%d\n", op->src_path, err);
     status = op_failure_status;
   }


  return(status);
}

//***********************************************************************
// lc_remove_object - Generates an object removal
//***********************************************************************

op_generic_t *lioc_remove_object(lio_config_t *lc, creds_t *creds, char *path, char *ex_optional, int ftype_optional)
{
  lioc_mk_mv_rm_t *op;

  type_malloc_clear(op, lioc_mk_mv_rm_t, 1);

  op->lc = lc;
  op->creds = creds;
  op->src_path = strdup(path);
  op->ex = ex_optional;
  op->type = ftype_optional;
  return(new_thread_pool_op(lc->tpc_unlimited, NULL, lioc_remove_object_fn, (void *)op, lioc_free_mk_mv_rm, 1));
}

//***********************************************************************
// lioc_remove_regex_object - Removes objects using regex's
//***********************************************************************

op_status_t lioc_remove_regex_object_fn(void *arg, int id)
{
  lioc_remove_regex_t *op = (lioc_remove_regex_t *)arg;
  os_object_iter_t *it;
  opque_t *q;
  op_generic_t *gop;
  int n, nfailed, atype, prefix_len;
  char *ex, *fname;
  char *key[1];
  int v_size[1];
  op_status_t status2;
  op_status_t status = op_success_status;

  key[0] = "system.exnode";
  ex = NULL;
  v_size[0] = -op->lc->max_attr;
  it = os_create_object_iter_alist(op->lc->os, op->creds, op->rpath, op->robj, op->obj_types, op->recurse_depth, key, (void **)&ex, v_size, 1);
  if (it == NULL) {
      log_printf(0, "ERROR: Failed with object_iter creation\n");
      return(op_failure_status);
   }

  //** Cycle through removing the objects
  q = new_opque();
  n = 0;
  nfailed = 0;
  while ((atype = os_next_object(op->lc->os, it, &fname, &prefix_len)) > 0) {

     //** If it's a directory so we need to flush all existing rm's first
     //** Otherwire the rmdir will see pending files
     if ((atype & OS_OBJECT_DIR) > 0) {
        opque_waitall(q);
     }

     gop = lioc_remove_object(op->lc, op->creds, fname, ex, atype);
     ex = NULL;  //** Freed in lioc_remove_object
     free(fname);
     opque_add(q, gop);

     if (opque_tasks_left(q) > op->np) {
        gop = opque_waitany(q);
        status2 = gop_get_status(gop);
        if (status2.op_status != OP_STATE_SUCCESS) { printf("Failed with gid=%d\n", gop_id(gop)); nfailed++; }
        gop_free(gop, OP_DESTROY);
     }

     n++;
  }

  os_destroy_object_iter(op->lc->os, it);

  opque_waitall(q);
  nfailed += opque_tasks_failed(q);
  opque_free(q, OP_DESTROY);

  status.op_status = (nfailed > 0) ? OP_STATE_FAILURE : OP_STATE_SUCCESS;
  status.error_code = n;
  return(status);
}

//***********************************************************************
// lc_remove_regex_object - Generates an object removal op
//***********************************************************************

op_generic_t *lioc_remove_regex_object(lio_config_t *lc, creds_t *creds, os_regex_table_t *rpath, os_regex_table_t *robj, int obj_types, int recurse_depth, int np)
{
  lioc_remove_regex_t *op;

  type_malloc_clear(op, lioc_remove_regex_t, 1);

  op->lc = lc;
  op->creds = creds;
  op->rpath = rpath;
  op->robj = robj;
  op->obj_types = obj_types;
  op->recurse_depth = recurse_depth;
  op->np = np;

  return(new_thread_pool_op(lc->tpc_unlimited, NULL, lioc_remove_regex_object_fn, (void *)op, free, 1));
}


//***********************************************************************
// lioc_create_object_fn - Does the actual object creation
//***********************************************************************

op_status_t lioc_create_object_fn(void *arg, int id)
{
  lioc_mk_mv_rm_t *op = (lioc_mk_mv_rm_t *)arg;
  os_fd_t *fd;
  exnode_abstract_set_t my_ess;
  char *dir, *fname;
  exnode_exchange_t *exp;
  exnode_t *ex, *cex;
  ex_id_t ino;
  char inode[32];
  char *val[_n_lioc_create_keys];
  op_status_t status;
  int v_size[_n_lioc_create_keys];
  int err;
  int ex_key = 5;

  status = op_success_status;

  val[ex_key] = NULL;

log_printf(15, "START op->ex=%p !!!!!!!!!\n fname=%s\n",  op->ex, op->src_path);

  //** Make the base object
  err = gop_sync_exec(os_create_object(op->lc->os, op->creds, op->src_path, op->type, op->id));
  if (err != OP_STATE_SUCCESS) {
     log_printf(15, "ERROR creating object fname=%s\n", op->src_path);
     status = op_failure_status;
     goto fail_bad;
  }

  //** Get the parent exnode to dup
  if (op->ex == NULL) {
     os_path_split(op->src_path, &dir, &fname);
log_printf(15, "dir=%s\n fname=%s\n", dir, fname);

     err = gop_sync_exec(os_open_object(op->lc->os, op->creds, dir, OS_MODE_READ_IMMEDIATE, op->id, &fd, op->lc->timeout));
     if (err != OP_STATE_SUCCESS) {
        log_printf(15, "ERROR opening parent=%s\n", dir);
        free(dir);
        status = op_failure_status;
        goto fail;
     }
     free(fname);

     v_size[0] = -op->lc->max_attr;
     err = gop_sync_exec(os_get_attr(op->lc->os, op->creds, fd, "system.exnode", (void **)&(val[ex_key]), &(v_size[0])));
     if (err != OP_STATE_SUCCESS) {
        log_printf(15, "ERROR opening parent=%s\n", dir);
        free(dir);
        status = op_failure_status;
        goto fail;
     }

     //** Close the parent
     err = gop_sync_exec(os_close_object(op->lc->os, fd));
     if (err != OP_STATE_SUCCESS) {
        log_printf(15, "ERROR closing parent fname=%s\n", dir);
        free(dir);
        status = op_failure_status;
        goto fail;
     }

     free(dir);
  } else {
    val[ex_key] = op->ex;
  }

  //** For a directory we can just copy the exnode.  For a file we have to
  //** Clone it to get unique IDs
  if ((op->type & OS_OBJECT_DIR) == 0) {
     //** If this has a caching segment we need to disable it from being adding
     //** to the global cache table cause there could be multiple copies of the
     //** same segment being serialized/deserialized.
     my_ess = *(op->lc->ess);
     my_ess.cache = NULL;

     //** Deserialize it
     exp = exnode_exchange_create(EX_TEXT);
     exp->text = val[ex_key];
     ex = exnode_create();
     exnode_deserialize(ex, exp, &my_ess);
     free(val[ex_key]);
     exp->text = NULL;

     //** Execute the clone operation
     err = gop_sync_exec(exnode_clone(op->lc->tpc_unlimited, ex, op->lc->da, &cex, NULL, CLONE_STRUCTURE, op->lc->timeout));
     if (err != OP_STATE_SUCCESS) {
        log_printf(15, "ERROR closing parent fname=%s\n", dir);
        status = op_failure_status;
        goto fail;
     }

     //** Serialize it for storage
     exnode_serialize(cex, exp);
     val[ex_key] = exp->text;
     exp->text = NULL;
     exnode_exchange_destroy(exp);
     exnode_destroy(ex);
     exnode_destroy(cex);
  }


  //** Open the object so I can add the required attributes
  err = gop_sync_exec(os_open_object(op->lc->os, op->creds, op->src_path, OS_MODE_WRITE_IMMEDIATE, op->id, &fd, op->lc->timeout));
  if (err != OP_STATE_SUCCESS) {
     log_printf(15, "ERROR opening object fname=%s\n", op->src_path);
     status = op_failure_status;
     free(val[1]);
     goto fail;
  }

  //** Now add the required attributes
  val[0] = an_cred_get_id(op->creds); v_size[0] = strlen(val[0]);
  val[1] = op->id;  v_size[1] = (op->id == NULL) ? 0 : strlen(op->id);
  val[2] = op->id; v_size[2] = v_size[1];
  val[3] = op->id; v_size[3] = v_size[1];
  ino = 0; generate_ex_id(&ino);  snprintf(inode, 32, XIDT, ino); val[4] = inode;  v_size[4] = strlen(inode);
  v_size[ex_key] = strlen(val[ex_key]);
  val[6] = "0";  v_size[6] = 1;

log_printf(15, "NEW ino=%s exnode=%s\n", val[4], val[ex_key]); flush_log();

  err = gop_sync_exec(os_set_multiple_attrs(op->lc->os, op->creds, fd, _lioc_create_keys, (void **)val, v_size, (op->type & OS_OBJECT_FILE) ? _n_lioc_file_keys : _n_lioc_dir_keys));
  if (err != OP_STATE_SUCCESS) {
     log_printf(15, "ERROR setting default attr fname=%s\n", op->src_path);
     status = op_failure_status;
  }


  //** Close the file
  err = gop_sync_exec(os_close_object(op->lc->os, fd));
  if (err != OP_STATE_SUCCESS) {
     log_printf(15, "ERROR closing object fname=%s\n", op->src_path);
     status = op_failure_status;
  }

fail:
  if (status.op_status != OP_STATE_SUCCESS) gop_sync_exec(os_remove_object(op->lc->os, op->creds, op->src_path));

fail_bad:
  if (val[ex_key] != NULL) free(val[ex_key]);

  return(status);
}


//***********************************************************************
// lc_create_object - Generates an object creation task
//***********************************************************************

op_generic_t *lioc_create_object(lio_config_t *lc, creds_t *creds, char *path, int type, char *ex, char *id)
{
  lioc_mk_mv_rm_t *op;

  type_malloc_clear(op, lioc_mk_mv_rm_t, 1);

  op->lc = lc;
  op->creds = creds;
  op->src_path = strdup(path);
  op->type = type;
  op->id = (id != NULL) ? strdup(id) : NULL;
  op->ex = (ex != NULL) ? strdup(ex) : NULL;
  return(new_thread_pool_op(lc->tpc_unlimited, NULL, lioc_create_object_fn, (void *)op, lioc_free_mk_mv_rm, 1));
}


//***********************************************************************
// lioc_link_object_fn - Does the actual object creation
//***********************************************************************

op_status_t lioc_link_object_fn(void *arg, int id)
{
  lioc_mk_mv_rm_t *op = (lioc_mk_mv_rm_t *)arg;
  os_fd_t *dfd;
  opque_t *q;
  int err;
  ex_id_t ino;
  char inode[32];
  op_status_t status;
  char *lkeys[] = {"system.exnode", "system.exnode.size"};
  char *spath[2];
  char *vkeys[] = {"system.owner", "system.inode", "os.timestamp.system.create", "os.timestamp.system.modify_data", "os.timestamp.system.modify_attr"};
  char *val[5];
  int vsize[5];

  //** Link the base object
  if (op->type == 1) { //** Symlink
     err = gop_sync_exec(os_symlink_object(op->lc->os, op->creds, op->src_path, op->dest_path, op->id));
  } else {
     err = gop_sync_exec(os_hardlink_object(op->lc->os, op->creds, op->src_path, op->dest_path, op->id));
  }
  if (err != OP_STATE_SUCCESS) {
     log_printf(15, "ERROR linking base object sfname=%s dfname=%s\n", op->src_path, op->dest_path);
     status = op_failure_status;
     goto finished;
  }

  if (op->type == 0) {  //** HArd link so exit
     status = op_success_status;
     goto finished;
  }

  q = new_opque();

  //** Open the Destination object
  opque_add(q, os_open_object(op->lc->os, op->creds, op->dest_path, OS_MODE_READ_IMMEDIATE, op->id, &dfd, op->lc->timeout));
  err = opque_waitall(q);
  if (err != OP_STATE_SUCCESS) {
     log_printf(15, "ERROR opening src(%s) or dest(%s) file\n", op->src_path, op->dest_path);
     status = op_failure_status;
     goto open_fail;
  }

  //** Now link the exnode and size
  spath[0] = op->src_path; spath[1] = op->src_path;
  opque_add(q, os_symlink_multiple_attrs(op->lc->os, op->creds, spath, lkeys, dfd, lkeys, 2));

  //** Store the owner, inode, and dates
  val[0] = an_cred_get_id(op->creds);  vsize[0] = strlen(val[0]);
  ino = 0; generate_ex_id(&ino);  snprintf(inode, 32, XIDT, ino); val[1] = inode;  vsize[1] = strlen(inode);
  val[2] = op->id; vsize[2] = (op->id == NULL) ? 0 : strlen(op->id);
  val[3] = op->id; vsize[3] = vsize[2];
  val[4] = op->id; vsize[4] = vsize[2];
  opque_add(q, os_set_multiple_attrs(op->lc->os, op->creds, dfd, vkeys, (void **)val, vsize, 5));


  //** Wait for everything to complete
  err = opque_waitall(q);
  if (err != OP_STATE_SUCCESS) {
     log_printf(15, "ERROR with attr link or owner set src(%s) or dest(%s) file\n", op->src_path, op->dest_path);
     status = op_failure_status;
     goto open_fail;
  }

  status = op_success_status;

open_fail:
  if (dfd != NULL) opque_add(q, os_close_object(op->lc->os, dfd));
  opque_waitall(q);

  opque_free(q, OP_DESTROY);

finished:
  return(status);

}

//***********************************************************************
// lc_link_object - Generates a link object task
//***********************************************************************

op_generic_t *lioc_link_object(lio_config_t *lc, creds_t *creds, int symlink, char *src_path, char *dest_path, char *id)
{
  lioc_mk_mv_rm_t *op;

  type_malloc_clear(op, lioc_mk_mv_rm_t, 1);

  op->lc = lc;
  op->creds = creds;
  op->type = symlink;
  op->src_path = strdup(src_path);
  op->dest_path = strdup(dest_path);
  op->id = (id != NULL) ? strdup(id) : NULL;
  return(new_thread_pool_op(lc->tpc_unlimited, NULL, lioc_link_object_fn, (void *)op, lioc_free_mk_mv_rm, 1));
}

//***********************************************************************
// lioc_fsck_check_file - Checks a file for errors and optionally repairs them
//***********************************************************************

int lioc_fsck_check_object(lio_config_t *lc, creds_t *creds, char *path, int ftype, int owner_mode, char *owner, int exnode_mode, char **val, int *v_size)
{
  int state, err, srepair, ex_mode, index, vs, ex_index;
  char *dir, *file, ssize[128], *v;
  ex_id_t ino;
  ex_off_t nbytes;
  exnode_abstract_set_t my_ess;
  exnode_exchange_t *exp;
  exnode_t *ex, *cex;
  segment_t *seg;
  int do_clone;

  ex_index = 2;
  state = 0;

  srepair = exnode_mode & LIO_FSCK_SIZE_REPAIR;
  ex_mode = (srepair > 0) ? exnode_mode - LIO_FSCK_SIZE_REPAIR : exnode_mode;

log_printf(15, "fname=%s vs[0]=%d vs[1]=%d vs[2]=%d\n", path, v_size[0], v_size[1], v_size[2]);

  //** Check the owner
  index = 0; v = val[index]; vs= v_size[index];
  if (vs <= 0) { //** Missing owner
     switch (owner_mode) {
       case LIO_FSCK_MANUAL:
           state |= LIO_FSCK_MISSING_OWNER;
log_printf(15, "fname=%s missing owner\n", path);
           break;
       case LIO_FSCK_PARENT:
           os_path_split(path, &dir, &file);
log_printf(15, "fname=%d parent=%s file=%s\n", path, dir, file);
           free(file); file = NULL; vs = -lc->max_attr;
           lioc_get_attr(lc, creds, dir, NULL, "system.owner", (void **)&file, &vs);
log_printf(15, "fname=%d parent=%s owner=%s\n", path, dir, file);
           if (vs > 0) {
             lioc_set_attr(lc, creds, path, NULL, "system.owner", (void *)file, strlen(file));
             free(file);
           } else {
             state |= LIO_FSCK_MISSING_OWNER;
           }
           free(dir);
           break;
       case LIO_FSCK_DELETE:
          gop_sync_exec(lioc_remove_object(lc, creds, path, val[ex_index], ftype));
          return(state);
          break;
       case LIO_FSCK_USER:
          lioc_set_attr(lc, creds, path, NULL, "system.owner", (void *)owner, strlen(owner));
          break;
     }
  }

  //** Check the inode
  index = 1; v = val[index]; vs= v_size[index];
  if (vs <= 0) { //** Missing inode
     switch (owner_mode) {
       case LIO_FSCK_MANUAL:
           state |= LIO_FSCK_MISSING_INODE;
log_printf(15, "fname=%s missing owner\n", path);
           break;
       case LIO_FSCK_PARENT:
       case LIO_FSCK_USER:
           ino = 0;
           generate_ex_id(&ino);
           snprintf(ssize, sizeof(ssize),  XIDT, ino);
           lioc_set_attr(lc, creds, path, NULL, "system.inode", (void *)ssize, strlen(ssize));
           break;
       case LIO_FSCK_DELETE:
          gop_sync_exec(lioc_remove_object(lc, creds, path, val[ex_index], ftype));
          return(state);
          break;
     }
  }


  //** Check if we have an exnode
  do_clone = 0;
  index = 2; v = val[index]; vs= v_size[index];
  if (vs <= 0) {
     switch (ex_mode) {
       case LIO_FSCK_MANUAL:
           state |= LIO_FSCK_MISSING_EXNODE;
           return(state);
           break;
       case LIO_FSCK_PARENT:
           os_path_split(path, &dir, &file);
           free(file); file = NULL; vs = -lc->max_attr;
           lioc_get_attr(lc, creds, dir, NULL, "system.exnode", (void **)&file, &vs);
           if (vs > 0) {
             val[index] = file;
             do_clone = 1;  //** flag we need to clone and store it
           } else {
             state |= LIO_FSCK_MISSING_EXNODE;
             free(dir);
             return(state);
           }
           free(dir);
           break;
       case LIO_FSCK_DELETE:
          gop_sync_exec(lioc_remove_object(lc, creds, path, val[ex_index], ftype));
          return(state);
          break;
     }
  }

  //** Make sure it's valid by loading it
  //** If this has a caching segment we need to disable it from being adding
  //** to the global cache table cause there could be multiple copies of the
  //** same segment being serialized/deserialized.
  my_ess = *(lc->ess);
  my_ess.cache = NULL;

  //** Deserialize it
  exp = exnode_exchange_create(EX_TEXT);
  exp->text = val[ex_index];
  ex = exnode_create();
  exnode_deserialize(ex, exp, &my_ess);
  exp->text = NULL;

     //** Execute the clone operation if needed
  if (do_clone == 1) {
     err = gop_sync_exec(exnode_clone(lc->tpc_unlimited, ex, lc->da, &cex, NULL, CLONE_STRUCTURE, lc->timeout));
     if (err != OP_STATE_SUCCESS) {
        log_printf(15, "ERROR closing parent fname=%s\n", dir);
        state |= LIO_FSCK_MISSING_EXNODE;
        goto finished;
     }

     //** Serialize it for storage
     exnode_serialize(cex, exp);
     lioc_set_attr(lc, creds, path, NULL, "system.exnode", (void *)exp->text, strlen(exp->text));
     exnode_destroy(ex);
     ex = cex;   //** WE use the clone for size checking
  }

  if ((ftype & OS_OBJECT_DIR) > 0) goto finished;  //** Nothing else to do if a directory

  //** Get the default view to use
  seg = exnode_get_default(ex);
  if (seg == NULL) {
     state |= LIO_FSCK_MISSING_EXNODE;
     goto finished;
  }

  index = 3; v = val[index]; vs= v_size[index];
  if (vs <= 0) {  //** No size of correct if they want to
     if (srepair == LIO_FSCK_SIZE_REPAIR) {
        state |= LIO_FSCK_MISSING_EXNODE_SIZE;
        goto finished;
     }
     sprintf(ssize, I64T, segment_size(seg));
     lioc_set_attr(lc, creds, path, NULL, "system.exnode.size", (void *)ssize, strlen(ssize));
     goto finished;
  }

  //** Verify the size
  sscanf(v, XOT, &nbytes);
  if (nbytes != segment_size(seg)) {
     if (srepair == LIO_FSCK_SIZE_REPAIR) {
        state |= LIO_FSCK_MISSING_EXNODE_SIZE;
        goto finished;
     }
     sprintf(ssize, I64T, segment_size(seg));
     lioc_set_attr(lc, creds, path, NULL, "system.exnode.size", (void *)ssize, strlen(ssize));
  }

  //** Clean up
finished:
  exnode_destroy(ex);
  exnode_exchange_destroy(exp);

log_printf(15, "fname=%s state=%d\n", path, state);

  return(state);
}

//***********************************************************************
// lioc_fsck_object - Inspects and optionally repairs the file
//***********************************************************************

op_status_t lioc_fsck_object_fn(void *arg, int id)
{
  lioc_fsck_check_t *op = (lioc_fsck_check_t *)arg;
  int err, i;
  op_status_t status;
  char *val[_n_fsck_keys];
  int v_size[_n_fsck_keys];
log_printf(15, "fname=%s START\n", op->path); flush_log();

  if (op->ftype <= 0) { //** Bad Ftype so see if we can figure it out
    op->ftype = lioc_exists(op->lc, op->creds, op->path);
  }

  if (op->ftype == 0) { //** No file
    status = op_failure_status;
    status.error_code = LIO_FSCK_MISSING;
    return(status);
  }

  if (op->full == 0) {
log_printf(15, "fname=%s getting attrs\n", op->path); flush_log();
     for (i=0; i<_n_fsck_keys; i++) {
       val[i] = NULL;
       v_size[i] = -op->lc->max_attr;
     }
     lioc_get_multiple_attrs(op->lc, op->creds, op->path, NULL, _fsck_keys, (void **)&val, v_size, _n_fsck_keys);
     err = lioc_fsck_check_object(op->lc, op->creds, op->path, op->ftype, op->owner_mode, op->owner, op->exnode_mode, val, v_size);
     for (i=0; i<_n_fsck_keys; i++) if (val[i] != NULL) free(val[i]);
  } else {
     err = lioc_fsck_check_object(op->lc, op->creds, op->path, op->ftype, op->owner_mode, op->owner, op->exnode_mode, op->val, op->v_size);
  }

log_printf(15, "fname=%s status=%d\n", op->path, err);
  status = op_success_status;
  status.error_code = err;
  return(status);
}

//***********************************************************************
// lioc_fsck_object - Inspects and optionally repairs the file
//***********************************************************************

op_generic_t *lioc_fsck_object(lio_config_t *lc, creds_t *creds, char *fname, int ftype, int owner_mode, char *owner, int exnode_mode)
{
  lioc_fsck_check_t *op;

log_printf(15, "fname=%s START\n", fname); flush_log();

  type_malloc_clear(op, lioc_fsck_check_t, 1);

  op->lc = lc;
  op->creds = creds;
  op->ftype = ftype;
  op->path = fname;
  op->owner_mode = owner_mode;
  op->owner = owner;
  op->exnode_mode = exnode_mode;

  return(new_thread_pool_op(lc->tpc_unlimited, NULL, lioc_fsck_object_fn, (void *)op, free, 1));
}

//***********************************************************************
// lioc_fsck_object - Inspects and optionally repairs the file
//***********************************************************************

op_generic_t *lioc_fsck_object_full(lio_config_t *lc, creds_t *creds, char *fname, int ftype, int owner_mode, char *owner, int exnode_mode, char **val, int *v_size)
{
  lioc_fsck_check_t *op;

  type_malloc(op, lioc_fsck_check_t, 1);

  op->lc = lc;
  op->creds = creds;
  op->ftype = ftype;
  op->path = fname;
  op->owner_mode = owner_mode;
  op->owner = owner;
  op->exnode_mode = exnode_mode;
  op->val = val;
  op->v_size = v_size;
  op->full = 1;

  return(new_thread_pool_op(lc->tpc_unlimited, NULL, lioc_fsck_object_fn, (void *)op, free, 1));
}


//***********************************************************************
// lioc_next_fsck - Returns the next broken object
//***********************************************************************

int lioc_next_fsck(lio_config_t *lc, lio_fsck_iter_t *oit, char **bad_fname, int *bad_atype)
{
  lioc_fsck_iter_t *it = (lioc_fsck_iter_t *)oit;
  int i, prefix_len, slot;
  lioc_fsck_task_t *task;
  op_generic_t *gop;
  op_status_t status;

  if (it->firsttime == 1) {  //** First time through so fill up the tasks
     it->firsttime = 2;
     for (slot=0; slot< it->n; slot++) {
        task = &(it->task[slot]);
        task->ftype = os_next_object(it->lc->os, it->it, &(task->fname), &prefix_len);
        if (task->ftype <= 0) break;  //** No more tasks
log_printf(15, "fname=%s slot=%d\n", task->fname, slot);

        memcpy(task->val, it->val, _n_fsck_keys*sizeof(char *));
        memcpy(task->v_size, it->v_size, _n_fsck_keys*sizeof(int));

        gop = lioc_fsck_object_full(it->lc, it->creds, task->fname, task->ftype, it->owner_mode, it->owner, it->exnode_mode, task->val, task->v_size);
        gop_set_myid(gop, slot);
        opque_add(it->q, gop);
     }
  }

log_printf(15, "main loop start nque=%d\n", opque_tasks_left(it->q));

  //** Start processing the results
  while ((gop = opque_waitany(it->q)) != NULL) {
     slot = gop_get_myid(gop);
     task = &(it->task[slot]);
     status = gop_get_status(gop);
     gop_free(gop, OP_DESTROY);
     *bad_atype = task->ftype;  //** Preserve the info before launching a new one
     *bad_fname = task->fname;
log_printf(15, "fname=%s slot=%d state=%d\n", task->fname, slot, status.error_code);
     for (i=0; i<_n_fsck_keys; i++) { if (task->val[i] != NULL) free(task->val[i]); };

     if (it->firsttime == 2) {  //** Only go here if we hanve't finished iterating
        task->ftype = os_next_object(it->lc->os, it->it, &(task->fname), &prefix_len);
        if (task->ftype <= 0) {
           it->firsttime = 3;
        } else {
           memcpy(task->val, it->val, _n_fsck_keys*sizeof(char *));
           memcpy(task->v_size, it->v_size, _n_fsck_keys*sizeof(int));

           gop = lioc_fsck_object_full(it->lc, it->creds, task->fname, task->ftype, it->owner_mode, it->owner, it->exnode_mode, task->val, task->v_size);
           gop_set_myid(gop, slot);
           opque_add(it->q, gop);
        }
     }

log_printf(15, "fname=%s state=%d LIO_FSCK_GOOD=%d\n", *bad_fname, status.error_code, LIO_FSCK_GOOD);
     if (status.error_code != LIO_FSCK_GOOD) { //** Found one
log_printf(15, "ERROR fname=%s state=%d\n", *bad_fname, status.error_code);
        return(status.error_code);
     }

     free(*bad_fname);  //** IF we made it here we can throw away the old fname
  }

log_printf(15, "nothing left\n");
  *bad_atype = 0;
  *bad_fname = NULL;
  return(LIO_FSCK_FINISHED);

}

//***********************************************************************
// lioc_create_fsck_iter - Creates an FSCK iterator
//***********************************************************************

lio_fsck_iter_t *lioc_create_fsck_iter(lio_config_t *lc, creds_t *creds, char *path, int owner_mode, char *owner, int exnode_mode)
{
  lioc_fsck_iter_t *it;
  int i;

  type_malloc_clear(it, lioc_fsck_iter_t, 1);

  it->lc = lc;
  it->creds = creds;
  it->path = strdup(path);
  it->owner_mode = owner_mode;
  it->owner = owner;
  it->exnode_mode = exnode_mode;

  it->regex = os_path_glob2regex(it->path);

  for (i=0; i<_n_fsck_keys; i++) {
    it->v_size[i] = -lc->max_attr;
    it->val[i] = NULL;
  }

  it->it = os_create_object_iter_alist(it->lc->os, creds, it->regex, NULL, OS_OBJECT_ANY, 10000, _fsck_keys, (void **)it->val, it->v_size, _n_fsck_keys);
  if (it->it == NULL) {
     log_printf(0, "ERROR: Failed with object_iter creation %s\n", path);
     return(NULL);
  }

  it->n = lio_parallel_task_count;
  it->firsttime = 1;
  type_malloc_clear(it->task, lioc_fsck_task_t, it->n);
  it->q = new_opque();
  opque_start_execution(it->q);

  return((lio_fsck_iter_t *)it);

}

//***********************************************************************
// lioc_destroy_fsck_iter - Creates an FSCK iterator
//***********************************************************************

void lioc_destroy_fsck_iter(lio_config_t *lc, lio_fsck_iter_t *oit)
{
  lioc_fsck_iter_t *it = (lioc_fsck_iter_t *)oit;
  op_generic_t *gop;
  int slot;

  while ((gop = opque_waitany(it->q)) != NULL) {
     slot = gop_get_myid(gop);
     if (it->task[slot].fname != NULL) free(it->task[slot].fname);
  }
  opque_free(it->q, OP_DESTROY);

  os_destroy_object_iter(it->lc->os, it->it);

  os_regex_table_destroy(it->regex);
  free(it->path);
  free(it->task);
  free(it);

  return;
}


//***********************************************************************
// lc_move_object - Generates a move object task
//***********************************************************************

op_generic_t *lioc_move_object(lio_config_t *lc, creds_t *creds, char *src_path, char *dest_path)
{
  return(os_move_object(lc->os, creds, src_path, dest_path));
}


//***********************************************************************
// lio_core_destroy - Destroys the service
//***********************************************************************

void lio_core_destroy(lio_config_t *lc)
{
  free(lc->lio);
  lc->lio = NULL;
}

//***********************************************************************
//  lio_core_create - Creates a new lio_fn_t routine
//***********************************************************************

lio_fn_t *lio_core_create()
{
  lio_fn_t *lio;

  type_malloc_clear(lio, lio_fn_t, 1);

  lio->destroy_service = lio_core_destroy;
  lio->create_object = lioc_create_object;
  lio->remove_object = lioc_remove_object;
  lio->remove_regex_object = lioc_remove_regex_object;
  lio->link_object = lioc_link_object;
  lio->move_object = lioc_move_object;
  lio->create_fsck_iter = lioc_create_fsck_iter;
  lio->destroy_fsck_iter = lioc_destroy_fsck_iter;
  lio->next_fsck = lioc_next_fsck;
  lio->fsck_object = lioc_fsck_object;

  return(lio);
}


