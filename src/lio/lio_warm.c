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
  char **cap;
  char *fname;
  char *exnode;
  creds_t *creds;
  ibp_context_t *ic;
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
  int err, i, j, n;
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
      etext = inip_get_string(fd, inip_get_group(g), "manage_cap", "");
log_printf(1, "fname=%s cap[%d]=%s\n", w->fname, w->n, etext);
      w->cap[w->n] = unescape_text('\\', etext); free(etext);
//      opque_add(q, new_ibp_modify_alloc_op(ic, w->cap[w->n], -1, dt, -1, lio_gc->timeout));
      gop = new_ibp_modify_alloc_op(w->ic, w->cap[w->n], -1, dt, -1, lio_gc->timeout);
      gop_set_myid(gop, w->n);
      opque_add(q, gop);
      w->n++;
    }
    g = inip_next_group(g);
  }

  inip_destroy(fd);

  if (w->n > 0) {
    err = opque_waitall(q);
    n = opque_tasks_failed(q);
  } else {
    err = OP_STATE_SUCCESS;
  }
  if (err != OP_STATE_SUCCESS) {
     status = op_failure_status;
     info_printf(lio_ifd, 0, "Failed with file %s on %d out of %d allocations\n", w->fname, n, w->n);
     for (i=0; i<n; i++) {
        gop = opque_get_next_failed(q);
        j = gop_get_myid(gop);
        info_printf(lio_ifd, 1, "  cap[%d]=%s\n", j, w->cap[j]);
     }
  } else {
     etext = NULL; i = 0;
     lioc_set_attr(lio_gc, w->creds, w->fname, NULL, "os.timestamp.system.warm", (void *)etext, i);
     status = op_success_status;
     info_printf(lio_ifd, 0, "Succeeded with file %s with %d allocations\n", w->fname, w->n);
  }

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
  char *ex;
  char *key = "system.exnode";
  int ex_size, slot;
  os_object_iter_t *it;
  os_regex_table_t *rp_single, *ro_single;
  lio_path_tuple_t tuple;
  int submitted, good, bad;
  int recurse_depth = 10000;
  warm_t *w;

//printf("argc=%d\n", argc);
  if (argc < 2) {
     printf("\n");
     printf("lio_warm LIO_COMMON_OPTIONS [-rd recurse_depth] [-dt time] LIO_PATH_OPTIONS\n");
     lio_print_options(stdout);
     lio_print_path_options(stdout);
     printf("    -rd recurse_depth  - Max recursion depth on directories. Defaults to %d\n", recurse_depth);
     printf("    -dt time       - Duration time in sec.  Default is %d sec\n", dt);
     return(1);
  }

  lio_init(&argc, &argv);

  //*** Parse the path args
  rg_mode = 0;
  rp_single = ro_single = NULL;
  rg_mode = lio_parse_path_options(&argc, argv, lio_gc->auto_translate, &tuple, &rp_single, &ro_single);

  i=1;
  do {
     start_option = i;

     if (strcmp(argv[i], "-dt") == 0) { //** Time
        i++;
        dt = atoi(argv[i]); i++;
     } else if (strcmp(argv[i], "-rd") == 0) { //** Recurse depth
        i++;
        recurse_depth = atoi(argv[i]); i++;
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
  submitted = good = bad = 0;

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

     ex_size = - tuple.lc->max_attr;
     it = os_create_object_iter_alist(tuple.lc->os, tuple.creds, rp_single, ro_single, OS_OBJECT_FILE, recurse_depth, &key, (void **)&ex, &ex_size, 1);
     if (it == NULL) {
        info_printf(lio_ifd, 0, "ERROR: Failed with object_iter creation\n");
        goto finished;
      }


     slot = 0;
     while ((ftype = os_next_object(tuple.lc->os, it, &fname, &prefix_len)) > 0) {
        w[slot].fname = fname;
        w[slot].exnode = ex;
        w[slot].creds = tuple.lc->creds;
        w[slot].ic = ((ds_ibp_priv_t *)(tuple.lc->ds->priv))->ic;

        ex = NULL;  fname = NULL;
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
  info_printf(lio_ifd, 0, "Submitted: %d   Success: %d   Fail: %d\n", submitted, good, bad);
  if (submitted != (good+bad)) {
     info_printf(lio_ifd, 0, "ERROR FAILED self-consistency check! Submitted != Success+Fail\n");
  }
  if (bad > 0) {
     info_printf(lio_ifd, 0, "ERROR Some files failed to warm!\n");
  }

  free(w);

finished:
  lio_shutdown();

  return(0);
}


