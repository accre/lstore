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

#define _log_module_index 204

#include <assert.h>
#include "exnode.h"
#include "log.h"
#include "iniparse.h"
#include "type_malloc.h"
#include "thread_pool.h"
#include "lio.h"


//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
  int bufsize_mb = 20;
  ex_off_t bufsize;
  int err, dtype, i, start_index, start_option;
  char *ex_data, *buffer;
  exnode_t *ex;
  exnode_exchange_t *exp;
  segment_t *seg;
  char *key[] = {"system.exnode", "system.exnode.size"};
  char *val[2];
  int v_size[2];
  lio_path_tuple_t tuple;

//printf("argc=%d\n", argc);
  if (argc < 2) {
     printf("\n");
     printf("lio_put LIO_COMMON_OPTIONS [-b bufsize] dest_file\n");
     lio_print_options(stdout);
     printf("    -b bufsize         - Buffer size to use in MBytes (Default=%dMB)\n", bufsize_mb);
     printf("    dest_file          - Destination file\n");
     return(1);
  }

  lio_init(&argc, &argv);

  i=1;
  do {
     start_option = i;

     if (strcmp(argv[i], "-b") == 0) {  //** Get the buffer size
        i++;
        bufsize_mb = atoi(argv[i]); i++;
     }

  } while ((start_option < i) && (i<argc));
  start_index = i;

  //** This is the 1st dir to remove
  if (argv[start_index] == NULL) {
    info_printf(lio_ifd, 0, "Missing destination file!\n");
    return(2);
  }

  //** Make the buffer
  bufsize = 1024*1024*bufsize_mb;
  type_malloc(buffer, char, bufsize+1);

  //** Get the destination
  tuple = lio_path_resolve(argv[start_index]);

  //** Check if it exists and if not create it
  dtype = lioc_exists(tuple.lc, tuple.creds, tuple.path);

  if (dtype == 0) { //** Need to create it
     err = gop_sync_exec(lio_create_object(tuple.lc, tuple.creds, tuple.path, OS_OBJECT_FILE, NULL, NULL));
     if (err != OP_STATE_SUCCESS) {
        info_printf(lio_ifd, 1, "ERROR creating file(%s)!\n", tuple.path);
        goto finished;
     }
  } else if ((dtype & OS_OBJECT_DIR) > 0) { //** It's a dir so fail
     info_printf(lio_ifd, 0, "Destination(%s) is a dir!\n", tuple.path);
     goto finished;
  }

  //** Get the exnode
  v_size[0] = -tuple.lc->max_attr;
  lioc_get_attr(tuple.lc, tuple.creds, tuple.path, NULL, "system.exnode", (void **)&ex_data, v_size);
  if (v_size[0] <= 0) {
     info_printf(lio_ifd, 0, "Failed retrieving exnode!  path=%s\n", tuple.path);
     return(1);
  }

  //** Load it
  exp = exnode_exchange_create(EX_TEXT);
  exp->text = ex_data;
  ex = exnode_create();
  exnode_deserialize(ex, exp, tuple.lc->ess);

  //** Get the default view to use
  seg = exnode_get_default(ex);
  if (seg == NULL) {
     info_printf(lio_ifd, 0, "No default segment!  Aborting!\n");
     goto finished;
  }

//FILE *fd = fopen("dummy.out", "r");
//log_printf(0, "FILE fd=%p\n",fd);
  //** Do the put
log_printf(0, "BEFORE PUT\n");
  err = gop_sync_exec(segment_put(tuple.lc->tpc_unlimited, tuple.lc->da, stdin, seg, 0, -1, bufsize, buffer, 1, 3600));
log_printf(0, "AFTER PUT\n");
//fclose(fd);
  if (err != OP_STATE_SUCCESS) {
     info_printf(lio_ifd, 0, "Failed uploading data!  path=%s\n", tuple.path);
     goto finished;
  }

  //** Serialize the exnode
  free(exp->text);  exp->text = NULL;
  exnode_serialize(ex, exp);

  //** Update the OS exnode
  val[0] = exp->text;  v_size[0] = strlen(val[0]);
  sprintf(buffer, I64T, segment_size(seg));
  val[1] = buffer; v_size[1] = strlen(val[1]);
  err = lioc_set_multiple_attrs(tuple.lc, tuple.creds, tuple.path, NULL, key, (void **)val, v_size, 2);

finished:
  exnode_destroy(ex);
  exnode_exchange_destroy(exp);

  free(buffer);

  lio_path_release(&tuple);

  lio_shutdown();

  return(0);
}


