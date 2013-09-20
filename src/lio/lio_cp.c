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

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
  int i, start_index, start_option, n_paths, n_errors;
  int max_spawn, keepln;
  int obj_types = OS_OBJECT_ANY;
  ex_off_t bufsize;
  int bufsize_mb = 20;
  lio_cp_path_t *flist;
  lio_cp_file_t cp_single;
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
  n_errors = 0;
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
  dtuple = lio_path_resolve(lio_gc->auto_translate, argv[argc-1]);

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

  type_malloc_clear(flist, lio_cp_path_t, n_paths);

  max_spawn = lio_parallel_task_count / n_paths;
  if (max_spawn <= 0) max_spawn = 1;

  for (i=0; i<n_paths; i++) {
     flist[i].src_tuple = lio_path_resolve(lio_gc->auto_translate, argv[i+start_index]);
     if (flist[i].src_tuple.is_lio == 0) lio_path_local_make_absolute(&(flist[i].src_tuple));
     flist[i].dest_tuple = dtuple;
     flist[i].dest_type = dtype;
     flist[i].path_regex = os_path_glob2regex(flist[i].src_tuple.path);
     flist[i].recurse_depth = recurse_depth;
     flist[i].obj_types = obj_types;
     flist[i].max_spawn = max_spawn;
     flist[i].bufsize = bufsize;
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
        if (os_regex_is_fixed(flist[0].path_regex) == 0) {  //** Uh oh we have a wildcard with a single file dest
           info_printf(lio_ifd, 0, "ERROR: Single wildcard path(%s) selected but the dest(%s) is a file or doesn't exist!\n", flist[0].src_tuple.path, dtuple.path);
           goto finished;
        }
     }

log_printf(15, "2222222222222222 fixed=%d exp=%s\n", os_regex_is_fixed(flist[0].path_regex), flist[0].path_regex->regex_entry[0].expression); flush_log();

     //**if it's a fixed src with a dir dest we skip and use the cp_fn routines
     if ((os_regex_is_fixed(flist[0].path_regex) == 1) && ((dtype == 0) || ((dtype & OS_OBJECT_FILE) > 0))) {
        //** IF we made it here we have a simple cp
        cp_single.src_tuple = flist[0].src_tuple;
        cp_single.dest_tuple = flist[0].dest_tuple;
        status = lio_cp_file_fn(&cp_single, 0);
        if (status.op_status != OP_STATE_SUCCESS) {
           info_printf(lio_ifd, 0, "ERROR: with copy src=%s  dest=%s\n", flist[0].src_tuple.path, dtuple.path);
           n_errors += status.error_code;
           goto finished;
        }
log_printf(15, "333333333333333333\n"); flush_log();

        goto finished;
     }
  }

  q = new_opque();
  opque_start_execution(q);
  for (i=0; i<n_paths; i++) {
     gop = new_thread_pool_op(lio_gc->tpc_unlimited, NULL, lio_cp_path_fn, (void *)&(flist[i]), NULL, 1);
     gop_set_myid(gop, i);
log_printf(0, "gid=%d i=%d fname=%s\n", gop_id(gop), i, flist[i].src_tuple.path);
     opque_add(q, gop);

     if (opque_tasks_left(q) > lio_parallel_task_count) {
        gop = opque_waitany(q);
//        j = gop_get_myid(gop);
        status = gop_get_status(gop);
        n_errors += status.error_code;
//        if (status.op_status != OP_STATE_SUCCESS) info_printf(lio_ifd, 0, "Failed with path %s\n", flist[j].src_tuple.path);
        gop_free(gop, OP_DESTROY);
     }
  }

  err = opque_waitall(q);
  if (err != OP_STATE_SUCCESS) {
     while ((gop = opque_get_next_failed(q)) != NULL) {
//         j = gop_get_myid(gop);
         status = gop_get_status(gop);
         n_errors += status.error_code;
//         info_printf(lio_ifd, 0, "Failed with path %s\n", flist[j].src_tuple.path);
     }
  }

  opque_free(q, OP_DESTROY);


finished:
  lio_path_release(&dtuple);
  for(i=0; i<n_paths; i++) {
    lio_path_release(&(flist[i].src_tuple));
    os_regex_table_destroy(flist[i].path_regex);
  }

  free(flist);

  if (n_errors > 0) info_printf(lio_ifd, 0, "Failed copying %d file(s)!\n", n_errors);

//set_log_level(20);
//printf("Before shutdown\n");
//apr_time_t dt = apr_time_now();
  lio_shutdown();
//dt = apr_time_now() - dt;
//double sec = dt;
//sec = sec / (1.0*APR_USEC_PER_SEC);
//printf("After shutdown dt=%lf\n", sec);

  return((n_errors == 0) ? 0 : 1);
}

