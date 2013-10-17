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
  ex_off_t visited_count;
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
  int err, serr;
  os_fd_t *fd;

  err = gop_sync_exec(os_open_object(lc->os, creds, path, OS_MODE_READ_IMMEDIATE, id, &fd, lc->timeout));
  if (err != OP_STATE_SUCCESS) {
     log_printf(15, "ERROR opening object=%s\n", path);
     return(err);
  }

  serr = gop_sync_exec(os_set_multiple_attrs(lc->os, creds, fd, key, val, v_size, n));
  
  //** Close the parent
  err = gop_sync_exec(os_close_object(lc->os, fd));
  if (err != OP_STATE_SUCCESS) {
     log_printf(15, "ERROR closing object=%s\n", path);
  }

  if (serr != OP_STATE_SUCCESS) err = OP_STATE_FAILURE;

  return(err);
}

//***********************************************************************
// lioc_set_attr - Returns an attribute
//***********************************************************************

int lioc_set_attr(lio_config_t *lc, creds_t *creds, char *path, char *id, char *key, void *val, int v_size)
{
  int err, serr;
  os_fd_t *fd;

  err = gop_sync_exec(os_open_object(lc->os, creds, path, OS_MODE_READ_IMMEDIATE, id, &fd, lc->timeout));
  if (err != OP_STATE_SUCCESS) {
     log_printf(15, "ERROR opening object=%s\n", path);
     return(err);
  }

  serr = gop_sync_exec(os_set_attr(lc->os, creds, fd, key, val, v_size));

  //** Close the parent
  err = gop_sync_exec(os_close_object(lc->os, fd));
  if (err != OP_STATE_SUCCESS) {
     log_printf(15, "ERROR closing object=%s\n", path);
  }

  if (serr != OP_STATE_SUCCESS) err = OP_STATE_FAILURE;

  return(err);
}

//***********************************************************************
// lioc_get_error_counts - Gets the error counts
//***********************************************************************

void lioc_get_error_counts(lio_config_t *lc, segment_t *seg, int *hard_errors, int *soft_errors)
{
  op_generic_t *gop;
  op_status_t status;

  gop = segment_inspect(seg, lc->da, lio_ifd, INSPECT_HARD_ERRORS, 0, NULL, 1);
  gop_waitall(gop);
  status = gop_get_status(gop);
  gop_free(gop, OP_DESTROY);
  *hard_errors = status.error_code;

  gop = segment_inspect(seg, lc->da, lio_ifd, INSPECT_SOFT_ERRORS, 0, NULL, 1);
  gop_waitall(gop);
  status = gop_get_status(gop);
  gop_free(gop, OP_DESTROY);
  *soft_errors = status.error_code;

  return;
}

//***********************************************************************
// lioc_update_error_count - Updates the error count attributes if needed
//***********************************************************************

int lioc_update_error_counts(lio_config_t *lc, creds_t *creds, char *path, segment_t *seg)
{
  int hard_errors, soft_errors;
  char *keys[] = { "system.hard_errors", "system.soft_errors" };
  char *val[2];
  char buf[2][128];
  int v_size[2];

  lioc_get_error_counts(lc, seg, &hard_errors, &soft_errors);

  //** Got errors so update the attributes
  if ((hard_errors > 0) || (soft_errors > 0)) {
     snprintf(buf[0], 127, "%d", hard_errors);
     snprintf(buf[1], 127, "%d", soft_errors);
     val[0] = buf[0];  v_size[0] = strlen(val[0]);
     val[1] = buf[1];  v_size[1] = strlen(val[1]);

     lioc_set_multiple_attrs(lc, creds, path, NULL, keys, (void **)val, v_size, 2);
  }

  return(hard_errors);
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
     exp = exnode_exchange_text_parse(ex_data);
     ex = exnode_create();
     if (exnode_deserialize(ex, exp, op->lc->ess) != 0) {
        log_printf(15, "ERROR removing data for object fname=%s\n", op->src_path);
        status = op_failure_status;
     } else {  //** Execute the remove operation since we have a good exnode
        err = gop_sync_exec(exnode_remove(op->lc->tpc_unlimited, ex, op->lc->da, op->lc->timeout));
        if (err != OP_STATE_SUCCESS) {
           log_printf(15, "ERROR removing data for object fname=%s\n", op->src_path);
           status = op_failure_status;
        }
     }

     //** Clean up
     if (op->ex != NULL) exp->text.text = NULL;  //** The inital exnode is free() by the TP op
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

     //** Deserialize it
     exp = exnode_exchange_text_parse(val[ex_key]);
     ex = exnode_create();
     if (exnode_deserialize(ex, exp, op->lc->ess_nocache) != 0) {
        log_printf(15, "ERROR parsing parent exnode fname=%s\n", dir);
        status = op_failure_status;
        exnode_exchange_destroy(exp);
        exnode_destroy(ex);
        goto fail;
     }

     //** Execute the clone operation
     err = gop_sync_exec(exnode_clone(op->lc->tpc_unlimited, ex, op->lc->da, &cex, NULL, CLONE_STRUCTURE, op->lc->timeout));
     if (err != OP_STATE_SUCCESS) {
        log_printf(15, "ERROR cloning parent fname=%s\n", dir);
        status = op_failure_status;
        exnode_exchange_destroy(exp);
        exnode_destroy(ex);
        exnode_destroy(cex);
        goto fail;
     }

     //** Serialize it for storage
     exnode_exchange_free(exp);
     exnode_serialize(cex, exp);
     val[ex_key] = exp->text.text;
     exp->text.text = NULL;
     exnode_exchange_destroy(exp);
     exnode_destroy(ex);
     exnode_destroy(cex);
  }


  //** Open the object so I can add the required attributes
  err = gop_sync_exec(os_open_object(op->lc->os, op->creds, op->src_path, OS_MODE_WRITE_IMMEDIATE, op->id, &fd, op->lc->timeout));
  if (err != OP_STATE_SUCCESS) {
     log_printf(15, "ERROR opening object fname=%s\n", op->src_path);
     status = op_failure_status;
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
  //** Deserialize it
  exp = exnode_exchange_text_parse(val[ex_index]);
  ex = exnode_create();
  if (exnode_deserialize(ex, exp, lc->ess_nocache) != 0) {
     log_printf(15, "ERROR parsing parent exnode fname=%s\n", dir);
     state |= LIO_FSCK_MISSING_EXNODE;
     exp->text.text = NULL;
     goto finished;
  }
  exp->text.text = NULL;

     //** Execute the clone operation if needed
  if (do_clone == 1) {
     err = gop_sync_exec(exnode_clone(lc->tpc_unlimited, ex, lc->da, &cex, NULL, CLONE_STRUCTURE, lc->timeout));
     if (err != OP_STATE_SUCCESS) {
        log_printf(15, "ERROR cloning parent fname=%s\n", dir);
        state |= LIO_FSCK_MISSING_EXNODE;
        goto finished;
     }

     //** Serialize it for storage
     exnode_serialize(cex, exp);
     lioc_set_attr(lc, creds, path, NULL, "system.exnode", (void *)exp->text.text, strlen(exp->text.text));
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
     it->visited_count++;
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
// lioc_fsck_visited_count - Returns the number of files checked
//***********************************************************************

ex_off_t lioc_fsck_visited_count(lio_config_t *lc, lio_fsck_iter_t *oit)
{
  lioc_fsck_iter_t *it = (lioc_fsck_iter_t *)oit;

  return(it->visited_count);
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
  lio->fsck_visited_count = lioc_fsck_visited_count;
  lio->fsck_object = lioc_fsck_object;

  return(lio);
}

//-------------------------------------------------------------------------
//------- Universal Object Iterators
//-------------------------------------------------------------------------


//*************************************************************************
//  unified_create_object_iter - Create an ls object iterator
//*************************************************************************

unified_object_iter_t *unified_create_object_iter(lio_path_tuple_t tuple, os_regex_table_t *path_regex, os_regex_table_t *obj_regex, int obj_types, int rd)
{
  unified_object_iter_t *it;

  type_malloc_clear(it, unified_object_iter_t, 1);

  it->tuple = tuple;
  if (tuple.is_lio == 1) {
     it->oit = os_create_object_iter(tuple.lc->os, tuple.creds, path_regex, obj_regex, obj_types, NULL, rd, NULL, 0);
  } else {
     it->lit = create_local_object_iter(path_regex, obj_regex, obj_types, rd);
  }

  return(it);
}

//*************************************************************************
//  unified_destroy_object_iter - Destroys an ls object iterator
//*************************************************************************

void unified_destroy_object_iter(unified_object_iter_t *it)
{

  if (it->tuple.is_lio == 1) {
    os_destroy_object_iter(it->tuple.lc->os, it->oit);
  } else {
    destroy_local_object_iter(it->lit);
  }

  free(it);
}

//*************************************************************************
//  unified_next_object - Returns the next object to work on
//*************************************************************************

int unified_next_object(unified_object_iter_t *it, char **fname, int *prefix_len)
{
  int err = 0;

  if (it->tuple.is_lio == 1) {
    err = os_next_object(it->tuple.lc->os, it->oit, fname, prefix_len);
  } else {
    err = local_next_object(it->lit, fname, prefix_len);
  }

log_printf(15, "ftype=%d\n", err);
  return(err);
}


//-------------------------------------------------------------------------
//------- LIO copy path routines ------------
//-------------------------------------------------------------------------

//*************************************************************************
// cp_lio2lio - Performs a lio->lio copy
//*************************************************************************

op_status_t cp_lio2lio(lio_cp_file_t *cp)
{
  char *buffer;
  op_status_t status;
  op_generic_t *gop;
  opque_t *q;
  int sigsize = 10*1024;
  char sig1[sigsize], sig2[sigsize];
  char *keys[] = {"system.exnode", "system.exnode.size", "os.timestamp.system.modify_data"};
  char *sex_data, *dex_data;
  char *val[3];
  char mysize[100];
  exnode_t *sex, *dex;
  exnode_exchange_t *sexp, *dexp;
  segment_t *sseg, *dseg;
  os_fd_t *sfd, *dfd;
  ex_off_t ssize;
  int sv_size[2], dv_size[3];
  int dtype, err, used, hard_errors;

  info_printf(lio_ifd, 0, "copy %s@%s:%s %s@%s:%s\n", an_cred_get_id(cp->src_tuple.creds), cp->src_tuple.lc->section_name, cp->src_tuple.path, an_cred_get_id(cp->dest_tuple.creds), cp->dest_tuple.lc->section_name, cp->dest_tuple.path);

  status = op_failure_status;
  q = new_opque();
  hard_errors = 0;
  sexp = dexp = NULL;
  sex = dex = NULL;
  sfd = dfd = NULL;

  //** Check if the dest exists and if not creates it
  dtype = lioc_exists(cp->dest_tuple.lc, cp->dest_tuple.creds, cp->dest_tuple.path);

log_printf(5, "src=%s dest=%s dtype=%d\n", cp->src_tuple.path, cp->dest_tuple.path, dtype);

  if (dtype == 0) { //** Need to create it
     err = gop_sync_exec(lio_create_object(cp->dest_tuple.lc, cp->dest_tuple.creds, cp->dest_tuple.path, OS_OBJECT_FILE, NULL, NULL));
     if (err != OP_STATE_SUCCESS) {
        info_printf(lio_ifd, 1, "ERROR creating file(%s)!\n", cp->dest_tuple.path);
        goto finished;
     }
  } else if ((dtype & OS_OBJECT_DIR) > 0) { //** It's a dir so fail
     info_printf(lio_ifd, 0, "Destination(%s) is a dir!\n", cp->dest_tuple.path);
     goto finished;
  }

  //** Now get both the exnodes
  opque_add(q, os_open_object(cp->src_tuple.lc->os, cp->src_tuple.creds, cp->src_tuple.path, OS_MODE_READ_BLOCKING, NULL, &sfd, cp->src_tuple.lc->timeout));
  opque_add(q, os_open_object(cp->dest_tuple.lc->os, cp->dest_tuple.creds, cp->dest_tuple.path, OS_MODE_READ_BLOCKING, NULL, &dfd, cp->dest_tuple.lc->timeout));

  //** Wait for the opens to complete
  err = opque_waitall(q);
  if (err != OP_STATE_SUCCESS) {
     log_printf(10, "ERROR with os_open src=%s sfd=%p dest=%s dfd=%p\n", cp->src_tuple.path, sfd, cp->dest_tuple.path, dfd);
     goto finished;
  }

  //** Get both exnodes
  sex_data = dex_data = NULL;
  sv_size[0] = -cp->src_tuple.lc->max_attr;
  dv_size[0] = -cp->dest_tuple.lc->max_attr;
  opque_add(q, os_get_attr(cp->src_tuple.lc->os, cp->src_tuple.creds, sfd, "system.exnode", (void **)&sex_data, sv_size));
  opque_add(q, os_get_attr(cp->dest_tuple.lc->os, cp->dest_tuple.creds, dfd, "system.exnode", (void **)&dex_data, dv_size));

  //** Wait for the exnode retrieval to complete
  err = opque_waitall(q);
  if (err != OP_STATE_SUCCESS) {
     log_printf(10, "ERROR with os_open src=%s sex=%p dest=%s dex=%p\n", cp->src_tuple.path, sex_data, cp->dest_tuple.path, dex_data);
     if (sex_data != NULL) free(sex_data);
     if (dex_data != NULL) free(dex_data);
     goto finished;
  }

  //** Deserailize them
  sexp = exnode_exchange_text_parse(sex_data);
  sex = exnode_create();
  if (exnode_deserialize(sex, sexp, cp->src_tuple.lc->ess) != 0) {
     info_printf(lio_ifd, 0, "ERROR parsing source exnode(%s)!\n", cp->src_tuple.path);
     goto finished;
  }

  sseg = exnode_get_default(sex);
  if (sseg == NULL) {
     info_printf(lio_ifd, 0, "No default segment for source(%s)!\n", cp->src_tuple.path);
     if (dex_data != NULL) free(dex_data);
     goto finished;
  }

  dexp = exnode_exchange_text_parse(dex_data);
  dex = exnode_create();
  if (exnode_deserialize(dex, dexp, cp->dest_tuple.lc->ess) != 0) {
     info_printf(lio_ifd, 0, "ERROR parsing destination exnode(%s)!\n", cp->dest_tuple.path);
     goto finished;
  }

  dseg = exnode_get_default(dex);
  if (dseg == NULL) {
     info_printf(lio_ifd, 0, "No default segment for source(%s)!\n", cp->dest_tuple.path);
     goto finished;
  }

  //** What kind of copy do we do
  used = 0; segment_signature(sseg, sig1, &used, sigsize);
  used = 0; segment_signature(dseg, sig2, &used, sigsize);

  if (strcmp(sig1, sig2) == 0) {
     info_printf(lio_ifd, 1, "Cloning %s->%s\n", cp->src_tuple.path, cp->dest_tuple.path);
     gop = segment_clone(sseg, cp->dest_tuple.lc->da, &dseg, CLONE_STRUCT_AND_DATA, NULL, cp->dest_tuple.lc->timeout);
     log_printf(5, "src=%s  clone gid=%d\n", cp->src_tuple.path, gop_id(gop));
  } else {
     info_printf(lio_ifd, 1, "Slow copy:( %s->%s\n", cp->src_tuple.path, cp->dest_tuple.path);
     type_malloc(buffer, char, cp->bufsize+1);
     gop = segment_copy(cp->dest_tuple.lc->tpc_unlimited, cp->dest_tuple.lc->da, sseg, dseg, 0, 0, -1, cp->bufsize, buffer, 1, cp->dest_tuple.lc->timeout);
     free(buffer);
  }
  err = gop_waitall(gop);

  ssize = segment_size(sseg);
  if (err != OP_STATE_SUCCESS) {
     info_printf(lio_ifd, 0, "Failed uploading data!  path=%s\n", cp->dest_tuple.path);
     ssize =  0;
  }

  gop_free(gop, OP_DESTROY);

  //** Serialize the exnode
  exnode_exchange_free(dexp);
  exnode_serialize(dex, dexp);

  //** Update the dest exnode and size
  val[0] = dexp->text.text;  dv_size[0] = strlen(val[0]);
  sprintf(mysize, I64T, ssize);
  val[1] = mysize; dv_size[1] = strlen(val[1]);
  val[2] = NULL; dv_size[2] = 0;
  err = gop_sync_exec(os_set_multiple_attrs(cp->dest_tuple.lc->os, cp->dest_tuple.creds, dfd, keys, (void **)val, dv_size, 3));

  //**Update the error counts if needed
  hard_errors = lioc_update_error_counts(cp->src_tuple.lc, cp->src_tuple.creds, cp->src_tuple.path, sseg);
  hard_errors += lioc_update_error_counts(cp->dest_tuple.lc, cp->dest_tuple.creds, cp->dest_tuple.path, dseg);

finished:

  //** Close the files
  if (sfd != NULL) opque_add(q, os_close_object(cp->src_tuple.lc->os, sfd));
  if (dfd != NULL) opque_add(q, os_close_object(cp->dest_tuple.lc->os, dfd));
  opque_waitall(q);

  opque_free(q, OP_DESTROY);

  if (sex != NULL) exnode_destroy(sex);
  if (sexp != NULL) exnode_exchange_destroy(sexp);
  if (dex != NULL) exnode_destroy(dex);
  if (dexp != NULL) exnode_exchange_destroy(dexp);

  if ((hard_errors == 0) && (err == OP_STATE_SUCCESS)) status = op_success_status;

  if (status.op_status != OP_STATE_SUCCESS) { //** Destroy the file
     log_printf(5, "ERROR with copy.  Destroying file=%s\n", cp->dest_tuple.path);
     gop_sync_exec(lioc_remove_object(cp->dest_tuple.lc, cp->dest_tuple.creds, cp->dest_tuple.path, NULL, OS_OBJECT_FILE)); 
  }

  return(status);
}

//*************************************************************************
//  cp_local2lio - local->lio copy
//*************************************************************************

op_status_t cp_local2lio(lio_cp_file_t *cp)
{
  char *buffer;
  char *ex_data;
  exnode_t *ex;
  exnode_exchange_t *exp;
  segment_t *seg;
  char *key[] = {"system.exnode", "system.exnode.size", "os.timestamp.system.modify_data"};
  char *val[3];
  int v_size[3], dtype, err, hard_errors;
  ex_off_t ssize;
  op_status_t status;
  FILE *fd;

  info_printf(lio_ifd, 0, "copy %s %s@%s:%s\n", cp->src_tuple.path, an_cred_get_id(cp->dest_tuple.creds), cp->dest_tuple.lc->section_name, cp->dest_tuple.path);

  status = op_failure_status;

  //** Check if it exists and if not create it
  dtype = lioc_exists(cp->dest_tuple.lc, cp->dest_tuple.creds, cp->dest_tuple.path);

log_printf(5, "src=%s dest=%s dtype=%d bufsize=" XOT "\n", cp->src_tuple.path, cp->dest_tuple.path, dtype, cp->bufsize);

  if (dtype == 0) { //** Need to create it
     err = gop_sync_exec(lio_create_object(cp->dest_tuple.lc, cp->dest_tuple.creds, cp->dest_tuple.path, OS_OBJECT_FILE, NULL, NULL));
     if (err != OP_STATE_SUCCESS) {
        info_printf(lio_ifd, 1, "ERROR creating file(%s)!\n", cp->dest_tuple.path);
        goto finished;
     }
  } else if ((dtype & OS_OBJECT_DIR) > 0) { //** It's a dir so fail
     info_printf(lio_ifd, 0, "Destination(%s) is a dir!\n", cp->dest_tuple.path);
     goto finished;
  }

  //** Get the exnode
  v_size[0] = -cp->dest_tuple.lc->max_attr;
  err = lioc_get_attr(cp->dest_tuple.lc, cp->dest_tuple.creds, cp->dest_tuple.path, NULL, "system.exnode", (void **)&ex_data, v_size);
  if (err != OP_STATE_SUCCESS) {
     info_printf(lio_ifd, 0, "Failed retrieving exnode!  path=%s\n", cp->dest_tuple.path);
     goto finished;
  }

  fd = fopen(cp->src_tuple.path, "r");
  if (fd == NULL) {
     info_printf(lio_ifd, 0, "Failed opening source file!  path=%s\n", cp->dest_tuple.path);
     goto finished;
  }

  //** Load it
  exp = exnode_exchange_text_parse(ex_data);
  ex = exnode_create();
  if (exnode_deserialize(ex, exp, cp->dest_tuple.lc->ess) != 0) {
     info_printf(lio_ifd, 0, "ERROR parsing exnode!  Aborting!\n");
     exnode_destroy(ex);
     exnode_exchange_destroy(exp);
     fclose(fd);
     goto finished;
  }

  //** Get the default view to use
  seg = exnode_get_default(ex);
  if (seg == NULL) {
     info_printf(lio_ifd, 0, "No default segment!  Aborting!\n");
     exnode_destroy(ex);
     exnode_exchange_destroy(exp);
     fclose(fd);
     goto finished;
  }

  type_malloc(buffer, char, cp->bufsize+1);

log_printf(0, "BEFORE PUT\n");
  err = gop_sync_exec(segment_put(cp->dest_tuple.lc->tpc_unlimited, cp->dest_tuple.lc->da, fd, seg, 0, -1, cp->bufsize, buffer, 1, 3600));
log_printf(0, "AFTER PUT\n");

  fclose(fd);

  ssize = segment_size(seg);
  if (err != OP_STATE_SUCCESS) {
     info_printf(lio_ifd, 0, "Failed uploading data!  path=%s\n", cp->dest_tuple.path);
     ssize =  0;
  }

  //** Serialize the exnode
  exnode_exchange_free(exp);
  exnode_serialize(ex, exp);

  //** Update the OS exnode
  val[0] = exp->text.text;  v_size[0] = strlen(val[0]);
  sprintf(buffer, I64T, ssize);
  val[1] = buffer; v_size[1] = strlen(val[1]);
  val[2] = NULL; v_size[2] = 0;
  err = lioc_set_multiple_attrs(cp->dest_tuple.lc, cp->dest_tuple.creds, cp->dest_tuple.path, NULL, key, (void **)val, v_size, 3);

  //**Update the error counts if needed
  hard_errors = lioc_update_error_counts(cp->dest_tuple.lc, cp->dest_tuple.creds, cp->dest_tuple.path, seg);

  exnode_destroy(ex);
  exnode_exchange_destroy(exp);

  free(buffer);

  if ((hard_errors == 0) && (err == OP_STATE_SUCCESS)) status = op_success_status;

finished:

  return(status);
}

//*************************************************************************
//  cp_lio2local - lio>local copy
//*************************************************************************

op_status_t cp_lio2local(lio_cp_file_t *cp)
{
  int err, ftype;
  char *ex_data, *buffer;
  exnode_t *ex;
  exnode_exchange_t *exp;
  segment_t *seg;
  int v_size, hard_errors;
  op_status_t status;
  FILE *fd;

  info_printf(lio_ifd, 0, "copy %s@%s:%s %s\n", an_cred_get_id(cp->src_tuple.creds), cp->src_tuple.lc->section_name, cp->src_tuple.path, cp->dest_tuple.path);

  status = op_failure_status;

  //** Check if it exists
  ftype = lioc_exists(cp->src_tuple.lc, cp->src_tuple.creds, cp->src_tuple.path);

log_printf(5, "src=%s dest=%s dtype=%d\n", cp->src_tuple.path, cp->dest_tuple.path, ftype);

  if ((ftype & OS_OBJECT_FILE) == 0) { //** Doesn't exist or is a dir
     info_printf(lio_ifd, 1, "ERROR source file(%s) doesn't exist or is a dir ftype=%d!\n", cp->src_tuple.path, ftype);
     goto finished;
  }

  //** Get the exnode
  v_size = -cp->src_tuple.lc->max_attr;
  err = lioc_get_attr(cp->src_tuple.lc, cp->src_tuple.creds, cp->src_tuple.path, NULL, "system.exnode", (void **)&ex_data, &v_size);
  if (err != OP_STATE_SUCCESS) {
     info_printf(lio_ifd, 0, "Failed retrieving exnode!  path=%s\n", cp->src_tuple.path);
     goto finished;
  }

  //** Load it
  exp = exnode_exchange_text_parse(ex_data);
  ex = exnode_create();
  if (exnode_deserialize(ex, exp, cp->src_tuple.lc->ess) != 0) {
     info_printf(lio_ifd, 0, "ERROR parsing exnode!  Aborting!\n");
     exnode_destroy(ex);
     exnode_exchange_destroy(exp);
     goto finished;
  }

  //** Get the default view to use
  seg = exnode_get_default(ex);
  if (seg == NULL) {
     info_printf(lio_ifd, 0, "No default segment!  Aborting!\n");
     exnode_destroy(ex);
     exnode_exchange_destroy(exp);
     goto finished;
  }

  fd = fopen(cp->dest_tuple.path, "w");
  if (fd == NULL) {
     info_printf(lio_ifd, 0, "Failed opending dest file!  path=%s\n", cp->dest_tuple.path);
     exnode_destroy(ex);
     exnode_exchange_destroy(exp);
     goto finished;
  }

  type_malloc(buffer, char, cp->bufsize+1);
  err = gop_sync_exec(segment_get(cp->src_tuple.lc->tpc_unlimited, cp->src_tuple.lc->da, seg, fd, 0, -1, cp->bufsize, buffer, 3600));
  free(buffer);

  fclose(fd);

  //**Update the error counts if needed
  hard_errors = lioc_update_error_counts(cp->src_tuple.lc, cp->src_tuple.creds, cp->src_tuple.path, seg);

  exnode_destroy(ex);
  exnode_exchange_destroy(exp);

  if (hard_errors == 0) status = op_success_status;

finished:
  return(status);
}

//*************************************************************************
// lio_cp_file_fn - Actual cp function.  Copies a regex to a dest *dir*
//*************************************************************************

op_status_t lio_cp_file_fn(void *arg, int id)
{
  lio_cp_file_t *cp = (lio_cp_file_t *)arg;
  op_status_t status;
//printf("dummy %d", cp->src_tuple.is_lio);
//printf(" %d", cp->dest_tuple.is_lio);
//printf(" %s", cp->src_tuple.path);
//printf(" %s\n", cp->dest_tuple.path);

//info_printf(lio_ifd, 0, "copy src_lio=%d sfname=%s  dest_lio=%d dfname=%s\n", cp->src_tuple.is_lio, cp->src_tuple.path, cp->dest_tuple.is_lio, cp->dest_tuple.path);
//return(op_success_status);

  if ((cp->src_tuple.is_lio == 0) && (cp->dest_tuple.is_lio == 0)) {  //** Not allowed to both go to disk
     info_printf(lio_ifd, 0, "Both source(%s) and destination(%s) are local files!\n", cp->src_tuple.path, cp->dest_tuple.path);
     return(op_failure_status);
  }

  if (cp->src_tuple.is_lio == 0) {  //** Source is a local file and dest is lio
     status = cp_local2lio(cp);
  } else if (cp->dest_tuple.is_lio == 0) {  //** Source is lio and dest is local
     status = cp_lio2local(cp);
  } else {               //** both source and dest are lio
     status = cp_lio2lio(cp);
  }

  return(status);
}

//*************************************************************************
// lio_cp_create_dir - Ensures the new directory exists and updates the valid
//     dir table
//*************************************************************************

int lio_cp_create_dir(list_t *table, lio_path_tuple_t tuple)
{
  int i, n, err;
  struct stat s;
  int *dstate = NULL;
  char *dname = tuple.path;

  n = strlen(dname);
  for (i=1; i<n; i++) {
      if ((dname[i] == '/') || (i==n-1)) {
         dstate = list_search(table, dname);
         if (dstate == NULL) {  //** Need to make the dir
            if (i<n-1) dname[i] = 0;
            if (tuple.is_lio == 0) { //** Local dir
              err = mkdir(dname, S_IRWXU|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH);
              if (err != 0) { //** Check if it was already created by someone else
                 err = stat(dname, &s);
              }
              err = (err == 0) ? OP_STATE_SUCCESS : OP_STATE_FAILURE;
            } else {
              err = gop_sync_exec(lio_create_object(tuple.lc, tuple.creds, dname, OS_OBJECT_DIR, NULL, NULL));
              if (err != OP_STATE_SUCCESS) {  //** See if it was created by someone else
                 err = lioc_exists(tuple.lc, tuple.creds, dname);
                 err = ((err & OS_OBJECT_DIR) > 0) ? OP_STATE_SUCCESS : OP_STATE_FAILURE;
              }
            }

            //** Add the path to the table
            type_malloc(dstate, int, 1);
            *dstate = (err = OP_STATE_SUCCESS) ? 0 : 1;
            list_insert(table, dname, dstate);

            if (i<n-1) dname[i] = '/';
         }
      }
  }

  return((dstate == NULL) ? 1 : *dstate);
}

//*************************************************************************
// lio_cp_path_fn - Copies a regex to a dest *dir*
//*************************************************************************

op_status_t lio_cp_path_fn(void *arg, int id)
{
  lio_cp_path_t *cp = (lio_cp_path_t *)arg;
  unified_object_iter_t *it;
  lio_path_tuple_t create_tuple;
  int ftype, prefix_len, slot, count, nerr;
  int *dstate;
  char dname[OS_PATH_MAX];
  char *fname, *dir, *file;
  list_t *dir_table;
  lio_cp_file_t *cplist, *c;
  op_generic_t *gop;
  opque_t *q;
  op_status_t status;

log_printf(15, "START src=%s dest=%s max_spawn=%d bufsize=" XOT "\n", cp->src_tuple.path, cp->dest_tuple.path, cp->max_spawn, cp->bufsize);
flush_log();

  status = op_success_status;

  it = unified_create_object_iter(cp->src_tuple, cp->path_regex, cp->obj_regex, cp->obj_types, cp->recurse_depth);
  if (it == NULL) {
     info_printf(lio_ifd, 0, "ERROR: Failed with object_iter creation src_path=%s\n", cp->src_tuple.path);
     return(op_failure_status);
   }

  type_malloc_clear(cplist, lio_cp_file_t, cp->max_spawn);
  dir_table = list_create(0, &list_string_compare, list_string_dup, list_simple_free, list_simple_free);

  q = new_opque();
  nerr = 0;
  slot = 0;
  count = 0;
  while ((ftype = unified_next_object(it, &fname, &prefix_len)) > 0) {
     snprintf(dname, OS_PATH_MAX, "%s/%s", cp->dest_tuple.path, &(fname[prefix_len+1]));
//info_printf(lio_ifd, 0, "copy dtuple=%s sfname=%s  dfname=%s plen=%d\n", cp->dest_tuple.path, fname, dname, prefix_len);

     if ((ftype & OS_OBJECT_DIR) > 0) { //** Got a directory
        dstate = list_search(dir_table, fname);
        if (dstate == NULL) { //** New dir so have to check and possibly create it
           create_tuple = cp->dest_tuple;
           create_tuple.path = fname;
           lio_cp_create_dir(dir_table, create_tuple);
        }

        continue;  //** Nothing else to do so go to the next file.
     }

     os_path_split(dname, &dir, &file);
     dstate = list_search(dir_table, dir);
     if (dstate == NULL) { //** New dir so have to check and possibly create it
        create_tuple = cp->dest_tuple;
        create_tuple.path = dir;
        lio_cp_create_dir(dir_table, create_tuple);
     }
     if (dir) { free(dir); dir = NULL; }
     if (file) { free(file); file = NULL; }

     c = &(cplist[slot]);
     c->src_tuple = cp->src_tuple; c->src_tuple.path = fname;
     c->dest_tuple = cp->dest_tuple; c->dest_tuple.path = strdup(dname);
     c->bufsize = cp->bufsize;

     gop = new_thread_pool_op(lio_gc->tpc_unlimited, NULL, lio_cp_file_fn, (void *)c, NULL, 1);
     gop_set_myid(gop, slot);
log_printf(1, "gid=%d i=%d sname=%s dname=%s\n", gop_id(gop), slot, fname, dname);
     opque_add(q, gop);

     count++;

     if (count >= cp->max_spawn) {
        gop = opque_waitany(q);
        slot = gop_get_myid(gop);
        c = &(cplist[slot]);
        status = gop_get_status(gop);
        if (status.op_status != OP_STATE_SUCCESS) {nerr++; info_printf(lio_ifd, 0, "Failed with path %s\n", c->src_tuple.path); }
        free(c->src_tuple.path); free(c->dest_tuple.path);
        gop_free(gop, OP_DESTROY);
     } else {
        slot = count;
     }
  }

  unified_destroy_object_iter(it);

  while ((gop = opque_waitany(q)) != NULL) {
     status = gop_get_status(gop);
     slot = gop_get_myid(gop);
     c = &(cplist[slot]);
log_printf(15, "slot=%d fname=%s\n", slot, c->src_tuple.path);
     if (status.op_status != OP_STATE_SUCCESS) {nerr++; info_printf(lio_ifd, 0, "Failed with path %s\n", c->src_tuple.path); }
     free(c->src_tuple.path); free(c->dest_tuple.path);
     gop_free(gop, OP_DESTROY);
  }

  opque_free(q, OP_DESTROY);

  free(cplist);
  list_destroy(dir_table);

  status = op_success_status;
  if (nerr > 0) {status.op_status = OP_STATE_FAILURE; status.error_code = nerr; }
  return(status);
}

