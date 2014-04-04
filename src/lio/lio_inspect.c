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

#define _log_module_index 208

#include <assert.h>
#include "exnode.h"
#include "log.h"
#include "iniparse.h"
#include "type_malloc.h"
#include "thread_pool.h"
#include "lio.h"
#include "ds_ibp_priv.h"
#include "ibp.h"
#include "string_token.h"
#include "rs_query_base.h"

#define n_inspect 10
char *inspect_opts[] = { "DUMMY", "inspect_quick_check",  "inspect_scan_check",  "inspect_full_check",
                                  "inspect_quick_repair", "inspect_scan_repair", "inspect_full_repair",
                                  "inspect_soft_errors",  "inspect_hard_errors", "inspect_migrate" };

typedef struct {
  char *fname;
  char *exnode;
  int ftype;
  int pslot;
} inspect_t;

static creds_t *creds;
static int global_whattodo;
static int bufsize;
rs_query_t *query;
lio_path_tuple_t *tuple_list;
char **argv_list = NULL;
int error_only_check = 0;
int start_index = -1;
int final_index = -1;
int current_index = -1;
int from_stdin = 0;

apr_thread_mutex_t *lock = NULL;
list_t *seg_index;

//*************************************************************************
//  inspect_task
//*************************************************************************

op_status_t inspect_task(void *arg, int id)
{
  inspect_t *w = (inspect_t *)arg;
  op_status_t status;
  op_generic_t *gop;
  exnode_t *ex;
  exnode_exchange_t *exp, *exp_out;
  segment_t *seg;
  char *keys[] = { "os.timestamp.system.inspect", "system.hard_errors", "system.soft_errors", "system.exnode" };
  char *val[4];
  char *dsegid, *ptr, *exnode2;
  int v_size[4];
  int whattodo, repair_mode, count;
  inip_file_t *ifd;

  whattodo = global_whattodo;

log_printf(15, "inspecting fname=%s global_whattodo=%d\n", w->fname, global_whattodo);

  if (w->exnode == NULL) {
     info_printf(lio_ifd, 0, "ERROR  Failed with file %s (ftype=%d). No exnode!\n", w->fname, w->ftype);
     free(w->fname);
     return(op_failure_status);
  }

  //** Kind of kludgy to load the ex twice but this is more of a prototype fn
  ifd = inip_read_text(w->exnode);
  dsegid = inip_get_string(ifd, "view", "default", NULL);
  inip_destroy(ifd);
  if (dsegid == NULL) {
     info_printf(lio_ifd, 0, "ERROR  Failed with file %s (ftype=%d). No default segment!\n", w->fname, w->ftype);
     free(w->exnode);
     free(w->fname);
     return(op_failure_status);
  }

  apr_thread_mutex_lock(lock);
  log_printf(15, "checking fname=%s segid=%s\n", w->fname, dsegid); flush_log();
  ptr = list_search(seg_index, dsegid);
  log_printf(15, "checking fname=%s segid=%s got=%s\n", w->fname, dsegid, ptr); flush_log();
  if (ptr != NULL) {
     apr_thread_mutex_unlock(lock);
     info_printf(lio_ifd, 0, "Skipping file %s (ftype=%d). Already loaded/processed.\n", w->fname, w->ftype);
     free(dsegid);
     free(w->exnode);
     free(w->fname);
     return(op_success_status);
  }
  list_insert(seg_index, dsegid, dsegid);
  apr_thread_mutex_unlock(lock);

  //** If we made it here the exnode is unique and loaded.
  //** Load it
  exp = exnode_exchange_text_parse(w->exnode);
  ex = exnode_create();
  if (exnode_deserialize(ex, exp, lio_gc->ess) != 0) {
     info_printf(lio_ifd, 0, "ERROR  Failed with file %s (ftype=%d). Problem parsing exnode!\n", w->fname, w->ftype);
     status = op_failure_status;
     goto finished;
  }

//  printf("Initial exnode=====================================\n");
//  printf("%s", exp->text);
//  printf("===================================================\n");


  //** Get the default view to use
  seg = exnode_get_default(ex);
  if (seg == NULL) {
     info_printf(lio_ifd, 0, "ERROR  Failed with file %s (ftype=%d). No default segment!\n", w->fname, w->ftype);
     status = op_failure_status;
     goto finished;
  }

  info_printf(lio_ifd, 1, XIDT ": Inspecting file %s\n", segment_id(seg), w->fname);

log_printf(15, "whattodo=%d\n", whattodo);
  //** Execute the inspection operation
  gop = segment_inspect(seg, lio_gc->da, lio_ifd, whattodo, bufsize, query, lio_gc->timeout);
  if (gop == NULL) { printf("File not found.\n"); return(op_failure_status); }

log_printf(15, "fname=%s inspect_gid=%d whattodo=%d\n", w->fname, gop_id(gop), whattodo);

flush_log();
  gop_waitall(gop);
flush_log();
  status = gop_get_status(gop);
log_printf(15, "fname=%s inspect_gid=%d status=%d\n", w->fname, gop_id(gop), status.op_status);

  gop_free(gop, OP_DESTROY);

  //** Print out the results
  whattodo = whattodo & INSPECT_COMMAND_BITS;

  repair_mode = 0;
  switch(whattodo) {
    case (INSPECT_QUICK_REPAIR):
    case (INSPECT_SCAN_REPAIR):
    case (INSPECT_FULL_REPAIR):
         repair_mode = 1;
    case (INSPECT_QUICK_CHECK):
    case (INSPECT_SCAN_CHECK):
    case (INSPECT_FULL_CHECK):
    case (INSPECT_MIGRATE):
        if (status.op_status == OP_STATE_SUCCESS) {
           info_printf(lio_ifd, 0, "Success with file %s!\n", w->fname);
        } else {
           info_printf(lio_ifd, 0, "ERROR:  Failed with file %s.  status=%d error_code=%d\n", w->fname, status.op_status, status.error_code);
        }
        break;
    case (INSPECT_SOFT_ERRORS):
    case (INSPECT_HARD_ERRORS):
        if (status.op_status == OP_STATE_SUCCESS) {
           info_printf(lio_ifd, 0, "Success with file %s!\n", w->fname);
        } else {
           info_printf(lio_ifd, 0, "ERROR  Failed with file %s.  status=%d error_code=%d\n", w->fname, status.op_status, status.error_code);
        }
        break;
  }

  if ((status.op_status == OP_STATE_SUCCESS) && (repair_mode == 1)) {
     //** Store the updated exnode back to disk
     exp_out = exnode_exchange_create(EX_TEXT);
     exnode_serialize(ex, exp_out);
     //printf("Updated remote: %s\n", fname);
     //printf("-----------------------------------------------------\n");
     //printf("%s", exp_out->text);
     //printf("-----------------------------------------------------\n");

     val[0] = NULL;  v_size[0] = 0;
     val[1] = NULL;  v_size[1] = -1;  //** Remove the system.*_errors
     val[2] = NULL;  v_size[2] = -1;
     val[3] = exp_out->text.text; v_size[3]= strlen(val[3]);

     count = (strcmp(exp->text.text, exp_out->text.text) == 0) ? 1 : 0;  //** Only update the exnode if it's changed
     if (count == 0) {  //** Do a further check to make sure the exnode hans't changed during the inspection
        count = -lio_gc->max_attr;
        exnode2 = NULL;
        lioc_get_attr(lio_gc, creds, w->fname, NULL, "system.exnode", (void **)&exnode2, &count);
        if (exnode2 != NULL) {
           count = (strcmp(exnode2, exp->text.text) == 0) ? 0 : 1;
           free(exnode2);
           if (count == 1) { info_printf(lio_ifd, 0, "WARN Exnode changed during inspection for file %s (ftype=%d). Aborting exnode update\n", w->fname, w->ftype); }
        } else {
           count = 1;
        }
     }

     lioc_set_multiple_attrs(lio_gc, creds, w->fname, NULL, keys, (void **)val, v_size, 4-count);
     exnode_exchange_destroy(exp_out);
  }

  //** Clean up
finished:
  exnode_exchange_destroy(exp);

  exnode_destroy(ex);

  free(w->fname);

  return(status);
}

//*************************************************************************
//  next_path - Returns the next path from either argv or stdin
//*************************************************************************

char *next_path()
{
 char *p, *p2;

 if (from_stdin == 0) {
    if (current_index == -1) current_index = start_index;
    if (current_index > final_index) return(NULL);

    p = argv_list[current_index];
    current_index++;
 } else {
    type_malloc(p2, char, 8192);
    p = fgets(p2, 8192, stdin);
    if (p) p2[strlen(p)-1] = 0;  //** Truncate the \n
 }

 return(p);
}

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
  int i, j,  start_option, rg_mode, ftype, prefix_len;
  int force_repair, option;
  int bufsize_mb = 20;
  char *fname, *qstr, *path;
  rs_query_t *rsq;
  apr_pool_t *mpool;
  opque_t *q;
  op_generic_t *gop;
  op_status_t status;
  char *vals[3];
  char *keys[3] = {"system.exnode", "system.soft_errors", "system.hard_errors" };
  int v_size[3], acount;
  int slot, pslot, q_count, gotone;
  os_object_iter_t *it;
  os_regex_table_t *rp_single, *ro_single;
  lio_path_tuple_t static_tuple, tuple;
  int submitted, good, bad, do_print;
  int recurse_depth = 10000;
  inspect_t *w;

//printf("argc=%d\n", argc);
  if (argc < 2) {
     printf("\n");
     printf("lio_inspect LIO_COMMON_OPTIONS [-rd recurse_depth] [-b bufsize_mb] [-f] [-s] [-r] [-q extra_query] [-bl key value] [-p] -o inspect_opt [LIO_PATH_OPTIONS | -]\n");
     lio_print_options(stdout);
     lio_print_path_options(stdout);
     printf("    -rd recurse_depth  - Max recursion depth on directories. Defaults to %d\n", recurse_depth);
     printf("    -b bufsize_mb      - Buffer size to use in MBytes for *each* inspect (Default=%dMB)\n", bufsize_mb);
     printf("    -s                 - Report soft errors, like a missing RID in the config file but the allocation is good.\n");
     printf("                         The default is to ignore these type of errors.\n");
     printf("    -r                 - Use reconstruction for all repairs. Even for data placement issues.\n");
     printf("                         Not always successful.The default is to use depot-to-depot copies if possible.\n");
     printf("                         This can lead to drive hotspots if migrating data from a failing drive\n");
     printf("                         which can be avoided by using this option.\n");
     printf("    -q  extra_query    - Extra RS query for data placement. AND-ed with default query\n");
     printf("    -bl key value      - Blacklist the given key/value combination. Multiple -bl options can be provided\n");
     printf("                         For a RID use: rid_key rid     Hostname: host hostname\n");
     printf("    -f                 - Forces data replacement even if it would result in data loss\n");
     printf("    -x                 - Stop scanning a file if an unrecoverable error is detected.\n");
     printf("    -p                 - Print the resulting query string\n");
     printf("    -e                 - Only check files that have soft or hard errors\n");
     printf("    -o inspect_opt     - Inspection option.  One of the following:\n");
     for (i=1; i<n_inspect; i++) { printf("                 %s\n", inspect_opts[i]); }
     printf("    -                  - If no file is given but a single dash is used the files are taken from stdin\n");
     return(1);
  }

  lio_init(&argc, &argv);
  argv_list = argv;

  //*** Parse the path args
  rg_mode = 0;
  rp_single = ro_single = NULL;
  rg_mode = lio_parse_path_options(&argc, argv, lio_gc->auto_translate, &static_tuple, &rp_single, &ro_single);

  i=1;
  force_repair = 0;
  option = INSPECT_QUICK_CHECK;
  global_whattodo = 0;
  query = rs_query_new(lio_gc->rs);
  final_index = argc - 1;
  do_print = 0;
  q_count = 0;
  acount = 1;
  do {
     start_option = i;

     if (strcmp(argv[i], "-rd") == 0) { //** Recurse depth
        i++;
        recurse_depth = atoi(argv[i]); i++;
     } else if (strcmp(argv[i], "-b") == 0) {  //** Get the buffer size
        i++;
        bufsize_mb = atoi(argv[i]); i++;
     } else if (strcmp(argv[i], "-") == 0) {  //** Take files from stdin
        i++;
        from_stdin = 1;
     } else if (strcmp(argv[i], "-f") == 0) { //** Force repair
        i++;
        force_repair = INSPECT_FORCE_REPAIR;
     } else if (strcmp(argv[i], "-e") == 0) { //** Only check files that have soft/hard errors
        i++;
        error_only_check = 1;
        acount = 3;
     } else if (strcmp(argv[i], "-r") == 0) { //** Force reconstruction
        i++;
        global_whattodo |= INSPECT_FORCE_RECONSTRUCTION;
     } else if (strcmp(argv[i], "-x") == 0) { //** Kick out if an unrecoverable error is detected
        i++;
        global_whattodo |= INSPECT_FAIL_ON_ERROR;
     } else if (strcmp(argv[i], "-s") == 0) { //** Report soft errors
        i++;
        global_whattodo |= INSPECT_SOFT_ERROR_FAIL;
     } else if (strcmp(argv[i], "-p") == 0) { //** Print resulting query string
        i++;
        do_print = 1;
     } else if (strcmp(argv[i], "-q") == 0) { //** Add additional query
        i++;
        rsq = rs_query_parse(lio_gc->rs, argv[i]);
        if (rsq == NULL) {
           printf("ERROR parsing Query: %s\nAborting!\n",argv[i]);
           exit(1);
        }
        q_count++;
        rs_query_append(lio_gc->rs, query, rsq);
        rs_query_destroy(lio_gc->rs, rsq);
        i++;
     } else if (strcmp(argv[i], "-bl") == 0) { //** Blacklist
        i++;
        q_count++;
        rs_query_add(lio_gc->rs, &query, RSQ_BASE_OP_KV, argv[i], RSQ_BASE_KV_EXACT, argv[i+1], RSQ_BASE_KV_EXACT);
        rs_query_add(lio_gc->rs, &query, RSQ_BASE_OP_NOT, "*", RSQ_BASE_KV_ANY, "*", RSQ_BASE_KV_ANY);
        i = i + 2;
     } else if (strcmp(argv[i], "-o") == 0) { //** Inspect option
        i++;
        option = -1;
        for(j=1; j<n_inspect; j++) {
           if (strcasecmp(inspect_opts[j], argv[i]) == 0) { option = j; break; }
        }
        if (option == -1) {
            printf("Invalid inspect option:  %s\n", argv[i]);
           abort();
        }
        i++;
     }

  } while ((start_option < i) && (i<argc));
  start_index = i;

  //** Finish forming the query.  We need to add all the AND operations
  if (q_count == 0) {
     rs_query_destroy(lio_gc->rs, query);
     query = NULL;
  } else {
     for (j=0; j<q_count-1; j++) {
        rs_query_add(lio_gc->rs, &query, RSQ_BASE_OP_AND, "*", RSQ_BASE_KV_ANY, "*", RSQ_BASE_KV_ANY);
     }
  }

  //** Print the resulting query
  if (do_print == 1) {
     qstr = rs_query_print(lio_gc->rs, query);
     printf("RS query=%s\n", qstr);
     free(qstr);
  }

  global_whattodo |= option;
  if ((option == INSPECT_QUICK_REPAIR) || (option == INSPECT_SCAN_REPAIR) || (option == INSPECT_FULL_REPAIR)) global_whattodo |= force_repair;

  bufsize = bufsize_mb * 1024 *1024;

  if ((rg_mode == 0) && (from_stdin == 0)) {
     if (argc <= start_index) {
        info_printf(lio_ifd, 0, "Missing directory!\n");
        return(2);
     }
  } else {
    start_index--;  //** Ther 1st entry will be the rp created in lio_parse_path_options
  }

  type_malloc_clear(w, inspect_t, lio_parallel_task_count);
  seg_index = list_create(0, &list_string_compare, NULL, list_simple_free, NULL);
  assert(apr_pool_create(&mpool, NULL) == APR_SUCCESS);
  apr_thread_mutex_create(&lock, APR_THREAD_MUTEX_DEFAULT, mpool);

  q = new_opque();
  opque_start_execution(q);

  slot = pslot = 0;
  submitted = good = bad = 0;

  while ((path = next_path()) != NULL) {
     log_printf(5, "path=%s argc=%d rg_mode=%d pslot=%d\n", path, argc, rg_mode, pslot);
     if (rg_mode == 0) {
        //** Create the simple path iterator
        tuple = lio_path_resolve(lio_gc->auto_translate, path);
        lio_path_wildcard_auto_append(&tuple);
        rp_single = os_path_glob2regex(tuple.path);
     } else {
        rg_mode = 0;  //** Use the initial rp
     }

     creds = tuple.lc->creds;

     for (i=0; i< acount; i++) v_size[i] = -tuple.lc->max_attr;
     it = os_create_object_iter_alist(tuple.lc->os, tuple.creds, rp_single, ro_single, OS_OBJECT_FILE, recurse_depth, keys, (void **)vals, v_size, acount);
     if (it == NULL) {
        info_printf(lio_ifd, 0, "ERROR: Failed with object_iter creation\n");
        goto finished;
      }

     while ((ftype = os_next_object(tuple.lc->os, it, &fname, &prefix_len)) > 0) {
        gotone = 0;
        if (error_only_check == 1) {  //** Want to only check files with soft/hard errors
           if (vals[1] != NULL) {
              printf("fname=%s soft=%s\n", fname, vals[1]);
              if (atol(vals[1]) > 0) gotone = 1;
              free(vals[1]);
           }
           if (vals[2] != NULL) {
              printf("fname=%s hard=%s\n", fname, vals[2]);
              if (atol(vals[2]) > 0) gotone = 1;
              free(vals[2]);
           }
        } else {  //** Normal mode is to check everything
           gotone = 1;
        }

        if (gotone == 1) {
          w[slot].fname = fname;
          w[slot].exnode = vals[0];
          w[slot].ftype = ftype;
          vals[0] = NULL;  fname = NULL;

          submitted++;
          gop = new_thread_pool_op(lio_gc->tpc_unlimited, NULL, inspect_task, (void *)&(w[slot]), NULL, 1);
          gop_set_myid(gop, slot);
log_printf(0, "gid=%d i=%d fname=%s\n", gop_id(gop), slot, fname);
//info_printf(lio_ifd, 0, "n=%d gid=%d slot=%d fname=%s\n", submitted, gop_id(gop), slot, fname);
          opque_add(q, gop);

          if (submitted >= lio_parallel_task_count) {
             gop = opque_waitany(q);
             status = gop_get_status(gop);
             if (status.op_status == OP_STATE_SUCCESS) {
                good++;
             } else {
                bad++;
             }
             slot = gop_get_myid(gop);
             gop_free(gop, OP_DESTROY);
          } else {
             slot++;
          }
        } else {  //** Do some cleanup since we aren't doing a check
          free(fname);
          for (i=0; i<acount; i++) if (vals[i] != NULL) free(vals[i]);
        }
     }

     os_destroy_object_iter(lio_gc->os, it);

     if (rp_single != NULL) { os_regex_table_destroy(rp_single); rp_single = NULL; }
     if (ro_single != NULL) { os_regex_table_destroy(ro_single); ro_single = NULL; }
     lio_path_release(&tuple);
  }

  //** Wait for everything to complete
  while ((gop = opque_waitany(q)) != NULL) {
     status = gop_get_status(gop);
     if (status.op_status == OP_STATE_SUCCESS) {
        good++;
     } else {
        bad++;
     }
     slot = gop_get_myid(gop);
     gop_free(gop, OP_DESTROY);
  }


  opque_free(q, OP_DESTROY);

  apr_thread_mutex_destroy(lock);
  apr_pool_destroy(mpool);
  list_destroy(seg_index);

  info_printf(lio_ifd, 0, "--------------------------------------------------------------------\n");
  info_printf(lio_ifd, 0, "Submitted: %d   Success: %d   Fail: %d\n", submitted, good, bad);
  if (submitted != (good+bad)) {
     info_printf(lio_ifd, 0, "ERROR FAILED self-consistency check! Submitted != Success+Fail\n");
  }
  if (bad > 0) {
     info_printf(lio_ifd, 0, "ERROR Some files failed inspection!\n");
  }

  free(w);

finished:
  lio_shutdown();

  return(0);
}


