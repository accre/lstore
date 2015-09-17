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

#define _log_module_index 199

#include <assert.h>
#include "exnode.h"
#include "log.h"
#include "iniparse.h"
#include "type_malloc.h"
#include "thread_pool.h"
#include "lio.h"

char *exnode_data = NULL;

//*************************************************************************
// touch_fn - Actual touch function
//*************************************************************************

op_status_t mkdir_fn(void *arg, int id)
{
  lio_path_tuple_t *tuple = (lio_path_tuple_t *)arg;
  int ftype, err;
  op_status_t status;

  status = op_success_status;

  //** Make sure it doesn't exist
  ftype = lio_exists(tuple->lc, tuple->creds, tuple->path);

  if (ftype != 0) { //** The file exists
     log_printf(1, "ERROR The dir exists\n");
     status.op_status = OP_STATE_FAILURE;
     status.error_code = 2;
  }

  //** Now create the object
  err = gop_sync_exec(gop_lio_create_object(tuple->lc, tuple->creds, tuple->path, OS_OBJECT_DIR, exnode_data, NULL));
  if (err != OP_STATE_SUCCESS) {
     log_printf(1, "ERROR creating dir!\n");
     status.op_status = OP_STATE_FAILURE;
     status.error_code = 3;
  }

  return(status);
}


//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
  int i, j, n, start_index, err, start_option;
  char *ex_fname;
  opque_t *q;
  op_generic_t *gop;
  op_status_t status;
  lio_path_tuple_t *flist;
  char *error_table[] = { "", "ERROR checking dir existence", "ERROR dir already exists", "ERROR creating dir" };
  FILE *fd;
  int return_code = 0;

//printf("argc=%d\n", argc);
  if (argc < 2) {
     printf("\n");
     printf("lio_mkdir LIO_COMMON_OPTIONS [-ex exnode.ex3] dir1 dir2 ...\n");
     lio_print_options(stdout);
     printf("    -ex exnode.ex3      - Optional exnode to use.  Defaults to using the parent directory's exnode.\n");
     printf("    dir1 dir2 ...       - New directories to create\n");
     return(1);
  }

  lio_init(&argc, &argv);

  ex_fname = NULL;

  //*** Parse the args
  i=1;
  do {
     start_option = i;

     if (strcmp(argv[i], "-ex") == 0) { //** Use an alternative exnode
        i++;
        ex_fname = argv[i]; i++;
     }

  } while (start_option < i);
  start_index = i;

  //** This is the file to create
  if (argv[i] == NULL) {
    info_printf(lio_ifd, 0, "Missing source file!\n");
    return(2);
  }

  //** Load an alternative exnode if specified
  if (ex_fname != NULL) {
     fd = fopen(ex_fname, "r");
     if (fd == NULL) {
        info_printf(lio_ifd, 0, "ERROR reading exnode!\n");
        return(2);
     }
     fseek(fd, 0, SEEK_END);

     i = ftell(fd);
     type_malloc(exnode_data, char, i+1);
     exnode_data[i] = 0;

     fseek(fd, 0, SEEK_SET);
     if (fread(exnode_data, i, 1, fd) != 1) { //**
        info_printf(lio_ifd, 0, "ERROR reading exnode from disk!\n");
        return(2);
     }
     fclose(fd);
  }

  if (exnode_data != NULL) free(exnode_data);

  //** Spawn the tasks
  n = argc - start_index;
  type_malloc(flist, lio_path_tuple_t, n);

  q = new_opque();
  opque_start_execution(q);
  for (i=0; i<n; i++) {
     flist[i] = lio_path_resolve(lio_gc->auto_translate, argv[i+start_index]);
     gop = new_thread_pool_op(lio_gc->tpc_unlimited, NULL, mkdir_fn, (void *)&(flist[i]), NULL, 1);
     gop_set_myid(gop, i);
log_printf(0, "gid=%d i=%d fname=%s\n", gop_id(gop), i, flist[i].path);
     opque_add(q, gop);

     if (opque_tasks_left(q) > lio_parallel_task_count) {
        gop = opque_waitany(q);
        j = gop_get_myid(gop);
        status = gop_get_status(gop);
        if (status.op_status != OP_STATE_SUCCESS) {
           info_printf(lio_ifd, 0, "Failed with directory %s with error %s\n", argv[j+start_index], error_table[status.error_code]);
           return_code = EIO;
        }
        gop_free(gop, OP_DESTROY);
     }
  }

  err = opque_waitall(q);
  if (err != OP_STATE_SUCCESS) {
     return_code = EIO;
     while ((gop = opque_get_next_failed(q)) != NULL) {
         j = gop_get_myid(gop);
         status = gop_get_status(gop);
         info_printf(lio_ifd, 0, "Failed with directory %s with error %s\n", argv[j+start_index], error_table[status.error_code]);
     }
  }

  opque_free(q, OP_DESTROY);

  for(i=0; i<n; i++) {
    lio_path_release(&(flist[i]));
  }

  free(flist);

  lio_shutdown();

  return(return_code);
}


