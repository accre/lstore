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

#define _log_module_index 196

#include <assert.h>
#include "exnode.h"
#include "log.h"
#include "iniparse.h"
#include "type_malloc.h"
#include "thread_pool.h"
#include "lio.h"

typedef struct {
  char *fname;
  char *link;
  char *vals[5];
  int v_size[5];
  int prefix_len;
  int ftype;
} ls_entry_t;

lio_path_tuple_t tuple;

//*************************************************************************
// ls_format_entry - Prints an LS entry
//*************************************************************************

void ls_format_entry(info_fd_t *ifd, ls_entry_t *lse)
{
  char *dtype;
  char *perms;
  char *owner;
  char dt_create[128], dt_modify[128];
  apr_time_t dt;
  int64_t n;
  long int fsize;
  int nlink;

//printf("lsfe: ftype=%d fname=%s\n", lse->ftype, lse->fname);

  if ((lse->ftype & OS_OBJECT_SYMLINK) > 0) {
     if ((lse->ftype & OS_OBJECT_BROKEN_LINK) > 0) {
        perms = "L---------";
     } else {
        perms = "l---------";
     }
  }else if ((lse->ftype & OS_OBJECT_DIR) > 0) {
     perms = "d---------";
  } else {
     perms = "----------";
  }

  dtype = ((lse->ftype & OS_OBJECT_DIR) > 0) ? "/" : "";

  owner = lse->vals[0];
  if (owner == NULL) owner = "root";

  n = 0; if (lse->vals[1] != NULL) sscanf(lse->vals[1], I64T, &n);
  fsize = n;

  memset(dt_create, '-', 24); dt_create[24] = 0;
  n = -1; if (lse->vals[3] != NULL) sscanf(lse->vals[3], I64T, &n);
  if (n>0) {
     n = apr_time_from_sec(n);
    apr_ctime(dt_create, n);
  }

  memcpy(dt_modify, dt_create, 25);
  n = -1; if (lse->vals[2] != NULL) sscanf(lse->vals[2], I64T, &n);
  if (n > 0) {
     dt = apr_time_from_sec(n);
     apr_ctime(dt_modify, dt);
  }

  nlink = 1; if (lse->vals[4] != NULL) sscanf(lse->vals[4], "%d", &nlink);

  if (lse->link == NULL) {
     info_printf(ifd, 0, "%s  %3d  %10s  %10ld  %s  %s  %s%s\n", perms, nlink, owner, fsize, dt_create, dt_modify, lse->fname, dtype);
  } else {
     info_printf(ifd, 0, "%s  %3d  %10s  %10ld  %s  %s  %s%s -> %s\n", perms, nlink, owner, fsize, dt_create, dt_modify, lse->fname, dtype, lse->link);
  }

  return;
}


//*************************************************************************
// readlink_fn - Reads the softlink
//*************************************************************************

op_status_t readlink_fn(void *arg, int id)
{
  ls_entry_t *lse = (ls_entry_t *)arg;
  int err, v_size;
  os_fd_t *fd;

  err = gop_sync_exec(os_open_object(tuple.lc->os, tuple.creds, lse->fname, OS_MODE_READ_IMMEDIATE, NULL, &fd, tuple.lc->timeout));
  if (err != OP_STATE_SUCCESS) {
     log_printf(15, "ERROR opening object=%s\n", lse->fname);
     return(op_failure_status);
  }

  //** IF the attribute doesn't exist *val == NULL an *v_size = 0
  v_size = -32*1024;
  gop_sync_exec(os_get_attr(tuple.lc->os, tuple.creds, fd, "os.link", (void **)&(lse->link), &v_size));

  //** Close the parent
  err = gop_sync_exec(os_close_object(tuple.lc->os, fd));
  if (err != OP_STATE_SUCCESS) {
     log_printf(15, "ERROR closing object=%s\n", lse->fname);
     return(op_failure_status);
  }

  return(op_success_status);
}


//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
  int i, j, ftype, rg_mode, start_option, start_index, prefix_len, nosort;
  char *fname;
  ls_entry_t *lse;
  list_t *table;
//  lio_path_tuple_t tuple;
  os_regex_table_t *rp_single, *ro_single;
  os_object_iter_t *it;
  list_iter_t lit;
  opque_t *q;
  op_generic_t *gop;
  char *keys[] = { "system.owner", "system.exnode.size", "system.exnode.modified", "os.create",  "os.link_count" };
  char *vals[5];
  int v_size[5];
  int n_keys = 5;
  int recurse_depth = 0;
  int obj_types = OS_OBJECT_ANY;

//printf("argc=%d\n", argc);
  if (argc < 2) {
     printf("\n");
     printf("lio_ls LIO_COMMON_OPTIONS [-rd recurse_depth] [-ns] LIO_PATH_OPTIONS\n");
     lio_print_options(stdout);
     lio_print_path_options(stdout);
     printf("\n");
     printf("    -rd recurse_depth  - Max recursion depth on directories. Defaults to %d\n", recurse_depth);
     printf("    -t  object_types   - Types of objects to list bitwise OR of 1=Files, 2=Directories, 4=symlink, 8=hardlink.  Default is %d.\n", obj_types);
     printf("    -ns                - Don't sort the output\n");
     return(1);
  }

  lio_init(&argc, &argv);

  //*** Parse the args
  rg_mode = 0;
  rp_single = ro_single = NULL;
  nosort = 0;

  rg_mode = lio_parse_path_options(&argc, argv, lio_gc->auto_translate, &tuple, &rp_single, &ro_single);

  i=1;
  do {
     start_option = i;

     if (strcmp(argv[i], "-rd") == 0) { //** Recurse depth
        i++;
        recurse_depth = atoi(argv[i]); i++;
     } else if (strcmp(argv[i], "-t") == 0) {  //** Object types
        i++;
        obj_types = atoi(argv[i]); i++;
     } else if (strcmp(argv[i], "-ns") == 0) {  //** Strip off the path prefix
        i++;
        nosort = 1;
     }

  } while ((start_option < i) && (i<argc));
  start_index = i;

  if (rg_mode == 0) {
     if (i>=argc) {
        info_printf(lio_ifd, 0, "Missing directory!\n");
        return(2);
     }
  } else {
    start_index--;  //** Ther 1st entry will be the rp created in lio_parse_path_options
  }


  info_printf(lio_ifd, 0, "  Perms     Ref   Owner        Size           Creation date              Modify date             Filename [-> link]\n");
  info_printf(lio_ifd, 0, "----------  ---  ----------  ----------  ------------------------  ------------------------  ------------------------------\n");

  q = new_opque();
  table = list_create(0, &list_string_compare, NULL, list_no_key_free, list_no_data_free);


  for (j=start_index; j<argc; j++) {
     log_printf(5, "path_index=%d argc=%d rg_mode=%d\n", j, argc, rg_mode);
     if (rg_mode == 0) {
        //** Create the simple path iterator
        tuple = lio_path_resolve(lio_gc->auto_translate, argv[j]);
        lio_path_wildcard_auto_append(&tuple);
        rp_single = os_path_glob2regex(tuple.path);
     } else {
        rg_mode = 0;  //** Use the initial rp
     }

     for (i=0; i<n_keys; i++) v_size[i] = -tuple.lc->max_attr;
     memset(vals, 0, sizeof(vals));
     it = os_create_object_iter_alist(tuple.lc->os, tuple.creds, rp_single, ro_single, OS_OBJECT_ANY, recurse_depth, keys, (void **)vals, v_size, n_keys);
     if (it == NULL) {
        info_printf(lio_ifd, 0, "ERROR: Failed with object_iter creation\n");
        goto finished;
     }

     while ((ftype = os_next_object(tuple.lc->os, it, &fname, &prefix_len)) > 0) {
        type_malloc_clear(lse, ls_entry_t, 1);
        lse->fname = fname;
        lse->ftype = ftype;
        lse->prefix_len = prefix_len;
        memcpy(lse->v_size, v_size, sizeof(v_size));
        memcpy(lse->vals, vals, sizeof(vals));

        for (i=0; i<n_keys; i++) v_size[i] = -tuple.lc->max_attr;
        memset(vals, 0, sizeof(vals));

        //** Check if we have a link.  If so we need to resolve the link path
        if ((ftype & OS_OBJECT_SYMLINK) > 0) {
           gop = new_thread_pool_op(tuple.lc->tpc_unlimited, NULL, readlink_fn, (void *)lse, NULL, 1);
           gop_set_private(gop, lse);
           opque_add(q, gop);
           if (nosort == 1) opque_waitall(q);
        }

        if (nosort == 1) {
           ls_format_entry(lio_ifd, lse);
        } else {
           list_insert(table, lse->fname, lse);
        }
     }

     os_destroy_object_iter(tuple.lc->os, it);

     lio_path_release(&tuple);
     if (rp_single != NULL) { os_regex_table_destroy(rp_single); rp_single = NULL; }
     if (ro_single != NULL) { os_regex_table_destroy(ro_single); ro_single = NULL; }
  }

  //** Wait for any readlinks to complete
  opque_waitall(q);

  //** Now sort and print things if needed
  if (nosort == 0) {
     lit = list_iter_search(table, NULL, 0);
     while ((list_next(&lit, (list_key_t **)&fname, (list_data_t **)&lse)) == 0) {
       ls_format_entry(lio_ifd, lse);
     }
//  } else {
//    while ((gop = opque_get_next_finished(q)) != NULL) {
//       lse = gop_get_private(gop);
//       gop_free(gop, OP_DESTROY);
//       ls_format_entry(lio_ifd, lse);
//    }
  }

  list_destroy(table);

finished:
  opque_free(q, OP_DESTROY);

  lio_shutdown();

  return(0);
}

