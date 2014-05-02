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

#define _log_module_index 207

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


typedef struct {
  char *rid_key;
  ex_off_t good;
  ex_off_t bad;
  ex_off_t nbytes;
} warm_hash_entry_t;

typedef struct {
  char **cap;
  char *fname;
  char *exnode;
  creds_t *creds;
  ibp_context_t *ic;
  apr_hash_t *hash;
  apr_pool_t *mpool;
  int n;
} warm_t;

static int dt = 86400;

//*************************************************************************
//  gen_warm_task
//*************************************************************************

op_status_t gen_warm_task(void *arg, int id)
{
  warm_t *w = (warm_t *)arg;
  op_status_t status;
  op_generic_t *gop;
  inip_file_t *fd;
  int i, j, nfailed;
  warm_hash_entry_t *wrid;
  char *etext;
  opque_t *q;

log_printf(15, "warming fname=%s, dt=%d\n", w->fname, dt);
  fd = inip_read_text(w->exnode);
  inip_group_t *g;

  q = new_opque();
  opque_start_execution(q);

  type_malloc(w->cap, char *, inip_n_groups(fd));
  g = inip_first_group(fd);
  w->n = 0;
  while (g) {
    if (strncmp(inip_get_group(g), "block-", 6) == 0) { //** Got a data block
      //** Get the RID key
      etext = inip_get_string(fd, inip_get_group(g), "rid_key", NULL);
      if (etext != NULL) {
         wrid = apr_hash_get(w->hash, etext, APR_HASH_KEY_STRING);
         if (wrid == NULL) { //** 1st time so need to make an entry
            type_malloc_clear(wrid, warm_hash_entry_t, 1);
            wrid->rid_key = etext;
            apr_hash_set(w->hash, wrid->rid_key, APR_HASH_KEY_STRING, wrid);
         } else {
            free(etext);
         }
      }

      //** Get the data size and update thr counts
      wrid->nbytes += inip_get_integer(fd, inip_get_group(g), "max_size", 0);

      //** Get the manage cap
      etext = inip_get_string(fd, inip_get_group(g), "manage_cap", "");
log_printf(1, "fname=%s cap[%d]=%s\n", w->fname, w->n, etext);
      w->cap[w->n] = unescape_text('\\', etext); free(etext);

      //** Add the task
      gop = new_ibp_modify_alloc_op(w->ic, w->cap[w->n], -1, dt, -1, lio_gc->timeout);
      gop_set_myid(gop, w->n);
      gop_set_private(gop, wrid);
      opque_add(q, gop);
      w->n++;
    }
    g = inip_next_group(g);
  }

  inip_destroy(fd);

  nfailed = 0;
  while ((gop = opque_waitany(q)) != NULL) {
     status = gop_get_status(gop);
     wrid = gop_get_private(gop);

     if (status.op_status == OP_STATE_SUCCESS) {
        wrid->good++;
     } else {
        nfailed++;
        wrid->bad++;
        j = gop_get_myid(gop);
        info_printf(lio_ifd, 1, "ERROR: %s  cap=%s\n", w->fname, w->cap[j]);
     }

     gop_free(gop, OP_DESTROY);
  }

  if (nfailed == 0) {
     status = op_success_status;
     info_printf(lio_ifd, 0, "Succeeded with file %s with %d allocations\n", w->fname, w->n);
  } else {
     status = op_failure_status;
     info_printf(lio_ifd, 0, "Failed with file %s on %d out of %d allocations\n", w->fname, nfailed, w->n);
  }

  etext = NULL; i = 0;
  lioc_set_attr(lio_gc, w->creds, w->fname, NULL, "os.timestamp.system.warm", (void *)etext, i);

  opque_free(q, OP_DESTROY);
  
  free(w->exnode);
  free(w->fname);
  for (i=0; i<w->n; i++) free(w->cap[i]);
  free(w->cap);

  return(status);
}


//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
  int i, j, start_option, start_index, rg_mode, ftype, prefix_len;
  char *fname;
  opque_t *q;
  op_generic_t *gop;
  op_status_t status;
//  char *ex;
  char *keys[] = { "system.exnode", "system.write_errors" };
  char *vals[2];
  int slot, v_size[2];
  os_object_iter_t *it;
  os_regex_table_t *rp_single, *ro_single;
  list_t *master;
  apr_hash_index_t *hi;
  apr_ssize_t klen;
  char *rkey, *config, *value;
  char *line_end;
  warm_hash_entry_t *mrid, *wrid;
  inip_file_t *ifd;
  inip_group_t *ig;
  inip_element_t *ele;
  char ppbuf[128], ppbuf2[128];
  lio_path_tuple_t tuple;
  ex_off_t total, good, bad, nbytes, submitted, werr;
  list_iter_t lit;
  Stack_t *stack;
  int recurse_depth = 10000;
  int summary_mode;
  warm_t *w;

//printf("argc=%d\n", argc);
  if (argc < 2) {
     printf("\n");
     printf("lio_warm LIO_COMMON_OPTIONS [-rd recurse_depth] [-dt time] [-sb] [-sf] LIO_PATH_OPTIONS\n");
     lio_print_options(stdout);
     lio_print_path_options(stdout);
     printf("    -rd recurse_depth  - Max recursion depth on directories. Defaults to %d\n", recurse_depth);
     printf("    -dt time           - Duration time in sec.  Default is %d sec\n", dt);
     printf("    -sb                - Print the summary but only list the bad RIDs\n");
     printf("    -sf                - Print the the full summary\n");
     return(1);
  }

  lio_init(&argc, &argv);

  //*** Parse the path args
  rg_mode = 0;
  rp_single = ro_single = NULL;
  rg_mode = lio_parse_path_options(&argc, argv, lio_gc->auto_translate, &tuple, &rp_single, &ro_single);

  i=1;
  summary_mode = 0;
  do {
     start_option = i;

     if (strcmp(argv[i], "-dt") == 0) { //** Time
        i++;
        dt = atoi(argv[i]); i++;
     } else if (strcmp(argv[i], "-rd") == 0) { //** Recurse depth
        i++;
        recurse_depth = atoi(argv[i]); i++;
     } else if (strcmp(argv[i], "-sb") == 0) { //** Print only bad RIDs
        i++;
        summary_mode = 1;
     } else if (strcmp(argv[i], "-sf") == 0) { //** Print the full summary
        i++;
        summary_mode = 2;
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

  q = new_opque();
  opque_start_execution(q);

  type_malloc_clear(w, warm_t, lio_parallel_task_count);
  for (j=0; j<lio_parallel_task_count; j++) {
     apr_pool_create(&(w[j].mpool), NULL);
     w[j].hash = apr_hash_make(w[j].mpool);
  }

  submitted = good = bad = werr = 0;

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

     v_size[0] = v_size[1] = - tuple.lc->max_attr;
     it = os_create_object_iter_alist(tuple.lc->os, tuple.creds, rp_single, ro_single, OS_OBJECT_FILE, recurse_depth, keys, (void **)vals, v_size, 2);
     if (it == NULL) {
        info_printf(lio_ifd, 0, "ERROR: Failed with object_iter creation\n");
        goto finished;
      }


     slot = 0;
     while ((ftype = os_next_object(tuple.lc->os, it, &fname, &prefix_len)) > 0) {
        w[slot].fname = fname;
        w[slot].exnode = vals[0];
        w[slot].creds = tuple.lc->creds;
        w[slot].ic = ((ds_ibp_priv_t *)(tuple.lc->ds->priv))->ic;

        if (v_size[1] != -1) {
           werr++;
           info_printf(lio_ifd, 0, "WRITE_ERROR for file %s\n", fname);
           if (vals[1] != NULL) { free(vals[1]); vals[1] = NULL; }
        }

        vals[0] = NULL;  fname = NULL;
        submitted++;
        gop = new_thread_pool_op(lio_gc->tpc_unlimited, NULL, gen_warm_task, (void *)&(w[slot]), NULL, 1);
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
     }

     os_destroy_object_iter(lio_gc->os, it);

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

     lio_path_release(&tuple);
     if (rp_single != NULL) { os_regex_table_destroy(rp_single); rp_single = NULL; }
     if (ro_single != NULL) { os_regex_table_destroy(ro_single); ro_single = NULL; }
  }

  opque_free(q, OP_DESTROY);

  info_printf(lio_ifd, 0, "--------------------------------------------------------------------\n");
  info_printf(lio_ifd, 0, "Submitted: " XOT "   Success: " XOT "   Fail: " XOT "    Write Errors: " XOT "\n", submitted, good, bad, werr);
  if (submitted != (good+bad)) {
     info_printf(lio_ifd, 0, "ERROR FAILED self-consistency check! Submitted != Success+Fail\n");
  }
  if (bad > 0) {
     info_printf(lio_ifd, 0, "ERROR Some files failed to warm!\n");
  }


  if (submitted == 0) goto cleanup;

  //** Merge the data from all the tables
  master = list_create(0, &list_string_compare, list_string_dup, list_simple_free, list_no_data_free);
  for (i=0; i<lio_parallel_task_count; i++) {
    hi = apr_hash_first(NULL, w[i].hash);
    while (hi != NULL) {
      apr_hash_this(hi, (const void **)&rkey, &klen, (void **)&wrid);
      mrid = list_search(master, wrid->rid_key);
      if (mrid == NULL) {
         list_insert(master, wrid->rid_key, wrid);
      } else {
         mrid->good += wrid->good;
         mrid->bad += wrid->bad;
         mrid->nbytes += wrid->nbytes;

         apr_hash_set(w[i].hash, wrid->rid_key, APR_HASH_KEY_STRING, NULL);
         free(wrid->rid_key);
         free(wrid);
      }

      hi = apr_hash_next(hi);
    }
  }

  //** Get the RID config which is used in the summary
  config = rs_get_rid_config(lio_gc->rs);
  assert(ifd = inip_read_text(config));

  //** Convert it for easier lookup
  ig = inip_first_group(ifd);
  while (ig != NULL) {
     rkey = inip_get_group(ig);
     if (strcmp("rid", rkey) == 0) {  //** Found a resource
        //** Now cycle through the attributes
        ele = inip_first_element(ig);
        while (ele != NULL) {
           rkey = inip_get_element_key(ele);
           value = inip_get_element_value(ele);
           if (strcmp(rkey, "rid_key") == 0) { 
              free(ig->group);
              ig->group = strdup(value);
           }

          ele = inip_next_element(ele);
        }
     }

    ig = inip_next_group(ig);
  }
  
  //** Print the summary
  info_printf(lio_ifd, 0, "                                                              Allocations\n");
  info_printf(lio_ifd, 0, "                 RID Key                    Size        Total      Good         Bad\n");
  info_printf(lio_ifd, 0, "----------------------------------------  ---------  ----------  ----------  ----------\n");
  nbytes = good = bad = j = i = 0;
  stack = new_stack();
  lit = list_iter_search(master, NULL, 0);
  while (list_next(&lit, (list_key_t **)&rkey, (list_data_t **)&mrid) == 0) {
     j++;
     nbytes += mrid->nbytes;
     good += mrid->good;
     bad += mrid->bad;
     total = mrid->good + mrid->bad;
     if (mrid->bad > 0) i++;

     push(stack, mrid);

     if ((summary_mode == 0) || ((summary_mode == 1) && (mrid->bad == 0))) continue;
     line_end = (mrid->bad == 0) ? "\n" : "  RID_ERR\n";
     rkey = inip_get_string(ifd, mrid->rid_key, "ds_key", mrid->rid_key);
     info_printf(lio_ifd, 0, "%-40s  %s  %10" PXOT "  %10" PXOT "  %10" PXOT "%s", rkey, 
         pretty_print_double_with_scale(1024, (double)mrid->nbytes, ppbuf), total, mrid->good, mrid->bad, line_end);
     free(rkey);
  }
  if (summary_mode != 0) info_printf(lio_ifd, 0, "----------------------------------------  ---------  ----------  ----------  ----------\n");

  snprintf(ppbuf2, sizeof(ppbuf2), "SUM (%d RIDs, %d bad)", j, i);
  total = good + bad;
  info_printf(lio_ifd, 0, "%-40s  %s  %10" PXOT "  %10" PXOT "  %10" PXOT "\n", ppbuf2, 
      pretty_print_double_with_scale(1024, (double)nbytes, ppbuf), total, good, bad);

  list_destroy(master);

  inip_destroy(ifd);
  free(config);

  while ((mrid = pop(stack)) != NULL) {
     free(mrid->rid_key);
     free(mrid);
  }
  free_stack(stack, 0);
cleanup:
  for (j=0; j<lio_parallel_task_count; j++) {
     apr_pool_destroy(w[j].mpool);
  }

  free(w);

finished:
  lio_shutdown();

  return(0);
}


