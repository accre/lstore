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

#define _log_module_index 203

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <assert.h>
#include "exnode.h"
#include "log.h"
#include "iniparse.h"
#include "type_malloc.h"
#include "thread_pool.h"
#include "lio.h"
#include "string_token.h"
#include "random.h"

int max_spawn;
int keepln = 0;
ex_off_t bufsize;

typedef struct {
  lio_path_tuple_t src_tuple;
  lio_path_tuple_t dest_tuple;
} cp_file_t;

typedef struct {
  lio_path_tuple_t src_tuple;
  lio_path_tuple_t dest_tuple;
  os_regex_table_t *regex;
  int recurse_depth;
  int dest_type;
} cp_path_t;

typedef struct {
  lio_path_tuple_t tuple;
  os_object_iter_t *oit;
  local_object_iter_t *lit;
} copy_object_iter_t;

//*************************************************************************
// cp_lio - Performs a lio->lio copy
//*************************************************************************

op_status_t cp_lio(cp_file_t *cp)
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
  int dtype, err, used;

  info_printf(lio_ifd, 0, "copy %s@%s:%s %s@%s:%s\n", an_cred_get_id(cp->src_tuple.creds), cp->src_tuple.lc->section_name, cp->src_tuple.path, an_cred_get_id(cp->dest_tuple.creds), cp->dest_tuple.lc->section_name, cp->dest_tuple.path);

  status = op_failure_status;
  q = new_opque();

  //** Check if the dest exists and if not creates it
  dtype = lioc_exists(cp->dest_tuple.lc, cp->dest_tuple.creds, cp->dest_tuple.path);

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
     goto finished;
  }

  //** Deserailize them
  sexp = exnode_exchange_create(EX_TEXT);
  sexp->text = sex_data;
  sex = exnode_create();
  exnode_deserialize(sex, sexp, cp->src_tuple.lc->ess);

  sseg = exnode_get_default(sex);
  if (sseg == NULL) {
     info_printf(lio_ifd, 0, "No default segment for source(%s)!\n", cp->src_tuple.path);
     exnode_destroy(sex);
     exnode_exchange_destroy(sexp);
     goto finished;
  }

  dexp = exnode_exchange_create(EX_TEXT);
  dexp->text = dex_data;
  dex = exnode_create();
  exnode_deserialize(dex, dexp, cp->dest_tuple.lc->ess);

  dseg = exnode_get_default(dex);
  if (dseg == NULL) {
     info_printf(lio_ifd, 0, "No default segment for source(%s)!\n", cp->dest_tuple.path);
     exnode_destroy(sex);
     exnode_exchange_destroy(sexp);
     exnode_destroy(dex);
     exnode_exchange_destroy(dexp);
     goto finished;
  }

  //** What kind of copy do we do
  used = 0; segment_signature(sseg, sig1, &used, sigsize);
  used = 0; segment_signature(dseg, sig2, &used, sigsize);

  if (strcmp(sig1, sig2) == 0) {
     info_printf(lio_ifd, 1, "Cloning %s->%s\n", cp->src_tuple.path, cp->dest_tuple.path);
     gop = segment_clone(sseg, cp->dest_tuple.lc->da, &dseg, CLONE_STRUCT_AND_DATA, NULL, cp->dest_tuple.lc->timeout);
  } else {
     info_printf(lio_ifd, 1, "Slow copy:( %s->%s\n", cp->src_tuple.path, cp->dest_tuple.path);
     type_malloc(buffer, char, bufsize+1);
     gop = segment_copy(cp->dest_tuple.lc->tpc_unlimited, cp->dest_tuple.lc->da, sseg, dseg, 0, 0, -1, bufsize, buffer, 1, cp->dest_tuple.lc->timeout);
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
  free(dexp->text);  dexp->text = NULL;
  exnode_serialize(dex, dexp);

  //** Update the dest exnode and size
  val[0] = dexp->text;  dv_size[0] = strlen(val[0]);
  sprintf(mysize, I64T, ssize);
  val[1] = mysize; dv_size[1] = strlen(val[1]);
  val[2] = NULL; dv_size[2] = 0;
  gop_sync_exec(os_set_multiple_attrs(cp->dest_tuple.lc->os, cp->dest_tuple.creds, dfd, keys, (void **)val, dv_size, 3));

  //**Update the error counts if needed
  lioc_update_error_counts(cp->src_tuple.lc, cp->src_tuple.creds, cp->src_tuple.path, sseg);
  lioc_update_error_counts(cp->dest_tuple.lc, cp->dest_tuple.creds, cp->dest_tuple.path, dseg);

  //** Close the files
  opque_add(q, os_close_object(cp->src_tuple.lc->os, sfd));
  opque_add(q, os_close_object(cp->dest_tuple.lc->os, dfd));
  opque_waitall(q);

  opque_free(q, OP_DESTROY);

  exnode_destroy(sex);
  exnode_exchange_destroy(sexp);
  exnode_destroy(dex);
  exnode_exchange_destroy(dexp);

  status = op_success_status;

finished:
  return(status);
}

//*************************************************************************
//  cp_src_local - local->lio copy
//*************************************************************************

op_status_t cp_src_local(cp_file_t *cp)
{
  char *buffer;
  char *ex_data;
  exnode_t *ex;
  exnode_exchange_t *exp;
  segment_t *seg;
  char *key[] = {"system.exnode", "system.exnode.size", "os.timestamp.system.modify_data"};
  char *val[3];
  int v_size[3], dtype, err;
  ex_off_t ssize;
  op_status_t status;
  FILE *fd;

  info_printf(lio_ifd, 0, "copy %s %s@%s:%s\n", cp->src_tuple.path, an_cred_get_id(cp->dest_tuple.creds), cp->dest_tuple.lc->section_name, cp->dest_tuple.path);

  status = op_failure_status;

  //** Check if it exists and if not create it
  dtype = lioc_exists(cp->dest_tuple.lc, cp->dest_tuple.creds, cp->dest_tuple.path);

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
  exp = exnode_exchange_create(EX_TEXT);
  exp->text = ex_data;
  ex = exnode_create();
  exnode_deserialize(ex, exp, cp->dest_tuple.lc->ess);

  //** Get the default view to use
  seg = exnode_get_default(ex);
  if (seg == NULL) {
     info_printf(lio_ifd, 0, "No default segment!  Aborting!\n");
     exnode_destroy(ex);
     exnode_exchange_destroy(exp);
     fclose(fd);
     goto finished;
  }

  type_malloc(buffer, char, bufsize+1);

log_printf(0, "BEFORE PUT\n");
  err = gop_sync_exec(segment_put(cp->dest_tuple.lc->tpc_unlimited, cp->dest_tuple.lc->da, fd, seg, 0, -1, bufsize, buffer, 1, 3600));
log_printf(0, "AFTER PUT\n");

  fclose(fd);

  ssize = segment_size(seg);
  if (err != OP_STATE_SUCCESS) {
     info_printf(lio_ifd, 0, "Failed uploading data!  path=%s\n", cp->dest_tuple.path);
     ssize =  0;
  }

  //** Serialize the exnode
  free(exp->text);  exp->text = NULL;
  exnode_serialize(ex, exp);

  //** Update the OS exnode
  val[0] = exp->text;  v_size[0] = strlen(val[0]);
  sprintf(buffer, I64T, ssize);
  val[1] = buffer; v_size[1] = strlen(val[1]);
  val[2] = NULL; v_size[2] = 0;
  err = lioc_set_multiple_attrs(cp->dest_tuple.lc, cp->dest_tuple.creds, cp->dest_tuple.path, NULL, key, (void **)val, v_size, 3);

  //**Update the error counts if needed
  lioc_update_error_counts(cp->dest_tuple.lc, cp->dest_tuple.creds, cp->dest_tuple.path, seg);

  exnode_destroy(ex);
  exnode_exchange_destroy(exp);

  free(buffer);

  status = op_success_status;

finished:

  return(status);
}

//*************************************************************************
//  cp_dest_local - lio>local copy
//*************************************************************************

op_status_t cp_dest_local(cp_file_t *cp)
{
  int err, ftype;
  char *ex_data, *buffer;
  exnode_t *ex;
  exnode_exchange_t *exp;
  segment_t *seg;
  int v_size;
  op_status_t status;
  FILE *fd;

  info_printf(lio_ifd, 0, "copy %s@%s:%s %s\n", an_cred_get_id(cp->src_tuple.creds), cp->src_tuple.lc->section_name, cp->src_tuple.path, cp->dest_tuple.path);

  status = op_failure_status;

  //** Check if it exists
  ftype = lioc_exists(cp->src_tuple.lc, cp->src_tuple.creds, cp->src_tuple.path);

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
  exp = exnode_exchange_create(EX_TEXT);
  exp->text = ex_data;
  ex = exnode_create();
  exnode_deserialize(ex, exp, cp->src_tuple.lc->ess);

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

  type_malloc(buffer, char, bufsize+1);
  err = gop_sync_exec(segment_get(cp->src_tuple.lc->tpc_unlimited, cp->src_tuple.lc->da, seg, fd, 0, -1, bufsize, buffer, 3600));
  free(buffer);

  fclose(fd);

  //**Update the error counts if needed
  lioc_update_error_counts(cp->src_tuple.lc, cp->src_tuple.creds, cp->src_tuple.path, seg);

  exnode_destroy(ex);
  exnode_exchange_destroy(exp);

  status = op_success_status;

finished:
  return(status);
}

//*************************************************************************
// cp_file_fn - Actual cp function.  Copies a regex to a dest *dir*
//*************************************************************************

op_status_t cp_file_fn(void *arg, int id)
{
  cp_file_t *cp = (cp_file_t *)arg;
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
     status = cp_src_local(cp);
  } else if (cp->dest_tuple.is_lio == 0) {  //** Source is lio and dest is local
     status = cp_dest_local(cp);
  } else {               //** both source and dest are lio
     status = cp_lio(cp);
  }

  status = op_success_status;

  return(status);
}


//*************************************************************************
// create_dir - Ensures the new directory exists and updates the valid
//     dir table
//*************************************************************************

int create_dir(list_t *table, lio_path_tuple_t tuple)
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
//  copy_create_object_iter - Create a copy object iterator
//*************************************************************************

copy_object_iter_t *copy_create_object_iter(lio_path_tuple_t tuple, os_regex_table_t *regex, int rd)
{
  copy_object_iter_t *it;

  type_malloc_clear(it, copy_object_iter_t, 1);

  it->tuple = tuple;
  if (tuple.is_lio == 1) {
     it->oit = os_create_object_iter(tuple.lc->os, tuple.creds, regex, NULL, OS_OBJECT_FILE, NULL, rd, NULL, 0);
  } else {
     it->lit = create_local_object_iter(regex, NULL, OS_OBJECT_FILE, rd);
  }

  return(it);
}


//*************************************************************************
//  copy_destroy_object_iter - Destroys a copy object iterator
//*************************************************************************

void copy_destroy_object_iter(copy_object_iter_t *it)
{

  if (it->tuple.is_lio == 1) {
    os_destroy_object_iter(it->tuple.lc->os, it->oit);
  } else {
    destroy_local_object_iter(it->lit);
  }

  free(it);
}

//*************************************************************************
//  copy_next_object - Returns the next object to copy
//*************************************************************************

int copy_next_object(copy_object_iter_t *it, char **fname, int *prefix_len)
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

//*************************************************************************
// cp_path_fn - Copies a regex to a dest *dir*
//*************************************************************************

op_status_t cp_path_fn(void *arg, int id)
{
  cp_path_t *cp = (cp_path_t *)arg;
  copy_object_iter_t *it;
  lio_path_tuple_t create_tuple;
  int ftype, prefix_len, slot, count, nerr;
  int *dstate;
  char dname[OS_PATH_MAX];
  char *fname, *dir, *file;
  list_t *dir_table;
  cp_file_t *cplist, *c;
  op_generic_t *gop;
  opque_t *q;
  op_status_t status;

log_printf(15, "START src=%s dest=%s\n", cp->src_tuple.path, cp->dest_tuple.path);
flush_log();

  status = op_success_status;

  it = copy_create_object_iter(cp->src_tuple, cp->regex, cp->recurse_depth);
  if (it == NULL) {
     info_printf(lio_ifd, 0, "ERROR: Failed with object_iter creation src_path=%s\n", cp->src_tuple.path);
     return(op_failure_status);
   }

  type_malloc_clear(cplist, cp_file_t, max_spawn);
  dir_table = list_create(0, &list_string_compare, list_string_dup, list_simple_free, list_simple_free);

  q = new_opque();
  nerr = 0;
  slot = 0;
  count = 0;
  while ((ftype = copy_next_object(it, &fname, &prefix_len)) > 0) {
     snprintf(dname, OS_PATH_MAX, "%s/%s", cp->dest_tuple.path, &(fname[prefix_len+1]));
//info_printf(lio_ifd, 0, "copy dtuple=%s sfname=%s  dfname=%s plen=%d tweak=%d\n", cp->dest_tuple.path, fname, dname, prefix_len, tweak);
     os_path_split(dname, &dir, &file);
     dstate = list_search(dir_table, dir);
     if (dstate == NULL) { //** New dir so have to check and possibly create it
        create_tuple = cp->dest_tuple;
        create_tuple.path = dir;
        create_dir(dir_table, create_tuple);
     }
     free(dir); free(file);

     c = &(cplist[slot]);
     c->src_tuple = cp->src_tuple; c->src_tuple.path = fname;
     c->dest_tuple = cp->dest_tuple; c->dest_tuple.path = strdup(dname);

     gop = new_thread_pool_op(lio_gc->tpc_unlimited, NULL, cp_file_fn, (void *)c, NULL, 1);
     gop_set_myid(gop, slot);
log_printf(1, "gid=%d i=%d sname=%s dname=%s\n", gop_id(gop), slot, fname, dname);
     opque_add(q, gop);

     count++;

     if (count >= max_spawn) {
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

  copy_destroy_object_iter(it);

  while ((gop = opque_waitany(q)) != NULL) {
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

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
  int i, start_index, start_option, n_paths;
  int bufsize_mb = 20;
  cp_path_t *flist;
  cp_file_t cp_single;
  op_generic_t *gop;
  opque_t *q;
  lio_path_tuple_t dtuple;
  int err, dtype, recurse_depth;
  op_status_t status;

  recurse_depth = 10000;
//printf("argc=%d\n", argc);
  if (argc < 2) {
     printf("\n");
     printf("lio_cp LIO_COMMON_OPTIONS [-rd recurse_depth] [-ln] [-b bufsize_mb] src_path1 .. src_pathN dest_path\n");
     lio_print_options(stdout);
     printf("\n");
     printf("    -ln                - Follow links.  Otherwise they are ignored\n");
     printf("    -rd recurse_depth  - Max recursion depth on directories. Defaults to %d\n", recurse_depth);
     printf("    -b bufsize_mb      - Buffer size to use in MBytes for *each* transfer (Default=%dMB)\n", bufsize_mb);
     printf("    src_path*          - Source path glob to copy\n");
     printf("    dest_path          - Destination file or directory\n");
     printf("\n");
     printf("*** NOTE: It's imperative that the user@host:/path..., @:/path..., etc    ***\n");
     printf("***   be used since this signifies where the files come from.             ***\n");
     printf("***   If no '@:' is used the path is assumed to reside on the local disk. ***\n");
     return(1);
  }

  lio_init(&argc, &argv);

  //*** Parse the args
  keepln = 0;
  i=1;
  do {
     start_option = i;

     if (strcmp(argv[i], "-ln") == 0) {  //** Follow links
        i++;
        keepln = 1;
     } else if (strcmp(argv[i], "-rd") == 0) { //** Recurse depth
        i++;
        recurse_depth = atoi(argv[i]); i++;
     } else if (strcmp(argv[i], "-b") == 0) {  //** Get the buffer size
        i++;
        bufsize_mb = atoi(argv[i]); i++;
     }

  } while ((start_option < i) && (i<argc));
  start_index = i;


  //** Store the boffer size
  bufsize = 1024*1024*bufsize_mb;

  //** Make the dest tuple
  dtuple = lio_path_resolve(argv[argc-1]);

  if (i>=argc) {
     info_printf(lio_ifd, 0, "Missing directory!\n");
     return(2);
  }

  //** Get the dest filetype/exists
  if (dtuple.is_lio == 1) {
     dtype = lioc_exists(dtuple.lc, dtuple.creds, dtuple.path);
  } else {
     dtype = os_local_filetype(dtuple.path);
  }

  //** Create the simple path iterator
  n_paths = argc - start_index - 1;
log_printf(15, "n_paths=%d argc=%d si=%d dtype=%d\n", n_paths, argc, start_index, dtype);
//printf("n_paths=%d argc=%d si=%d\n", n_paths, argc, start_index);
//for (i=start_index; i<argc; i++) {
//  printf("argv[%d]=%s\n", i, argv[i]);
//}

  type_malloc_clear(flist, cp_path_t, n_paths);

  for (i=0; i<n_paths; i++) {
     flist[i].src_tuple = lio_path_resolve(argv[i+start_index]);
     if (flist[i].src_tuple.is_lio == 0) lio_path_local_make_absolute(&(flist[i].src_tuple));
     flist[i].dest_tuple = dtuple;
     flist[i].dest_type = dtype;
     flist[i].regex = os_path_glob2regex(flist[i].src_tuple.path);
     flist[i].recurse_depth = recurse_depth;
  }

  //** Do some sanity checking and handle the simple case directly
  //** If multiple paths then the dest must be a dir and it has to exist
  if ((n_paths > 1) && ((dtype & OS_OBJECT_DIR) == 0)) {
     if (dtype == 0) {
        info_printf(lio_ifd, 0, "ERROR: Multiple paths selected but the dest(%s) doesn't exist!\n", dtuple.path);
     } else {
        info_printf(lio_ifd, 0, "ERROR: Multiple paths selected but the dest(%s) isn't a directory!\n", dtuple.path);
     }
     goto finished;
  } else if (n_paths == 1) {
log_printf(15, "11111111\n"); flush_log();
     if (((dtype & OS_OBJECT_FILE) > 0) || (dtype == 0)) {  //** Single path and dest is an existing file or doesn't exist
        if (os_regex_is_fixed(flist[0].regex) == 0) {  //** Uh oh we have a wildcard with a single file dest
           info_printf(lio_ifd, 0, "ERROR: Single wildcard path(%s) selected but the dest(%s) is a file or doesn't exist!\n", flist[0].src_tuple.path, dtuple.path);
           goto finished;
        }
     }

log_printf(15, "2222222222222222 fixed=%d exp=%s\n", os_regex_is_fixed(flist[0].regex), flist[0].regex->regex_entry[0].expression); flush_log();

     //**if it's a fixed src with a dir dest we skip and use the cp_fn routines
     if ((os_regex_is_fixed(flist[0].regex) == 1) && ((dtype == 0) || ((dtype & OS_OBJECT_FILE) > 0))) {
        //** IF we made it here we have a simple cp
        cp_single.src_tuple = flist[0].src_tuple;
        cp_single.dest_tuple = flist[0].dest_tuple;
        status = cp_file_fn(&cp_single, 0);
        if (status.op_status != OP_STATE_SUCCESS) {
           info_printf(lio_ifd, 0, "ERROR: with copy src=%s  dest=%s\n", flist[0].src_tuple.path, dtuple.path);
           goto finished;
        }
log_printf(15, "333333333333333333\n"); flush_log();

        goto finished;
     }
  }



  //** IF we made it here we have mv's to a directory
  max_spawn = lio_parallel_task_count / n_paths;
  if (max_spawn < 0) max_spawn = 1;

  q = new_opque();
  opque_start_execution(q);
  for (i=0; i<n_paths; i++) {
     gop = new_thread_pool_op(lio_gc->tpc_unlimited, NULL, cp_path_fn, (void *)&(flist[i]), NULL, 1);
     gop_set_myid(gop, i);
log_printf(0, "gid=%d i=%d fname=%s\n", gop_id(gop), i, flist[i].src_tuple.path);
     opque_add(q, gop);

     if (opque_tasks_left(q) > lio_parallel_task_count) {
        gop = opque_waitany(q);
        i = gop_get_myid(gop);
        status = gop_get_status(gop);
        if (status.op_status != OP_STATE_SUCCESS) info_printf(lio_ifd, 0, "Failed with path %s\n", flist[i].src_tuple.path);
        gop_free(gop, OP_DESTROY);
     }
  }

  err = opque_waitall(q);
  if (err != OP_STATE_SUCCESS) {
     while ((gop = opque_get_next_failed(q)) != NULL) {
         i = gop_get_myid(gop);
         status = gop_get_status(gop);
         info_printf(lio_ifd, 0, "Failed with path %s\n", flist[i].src_tuple.path);
     }
  }

  opque_free(q, OP_DESTROY);


finished:
  lio_path_release(&dtuple);
  for(i=0; i<n_paths; i++) {
    lio_path_release(&(flist[i].src_tuple));
    os_regex_table_destroy(flist[i].regex);
  }

  free(flist);

  lio_shutdown();

  return(0);
}

