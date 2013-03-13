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

#define _log_module_index 209

#include <assert.h>
#include "exnode.h"
#include "log.h"
#include "iniparse.h"
#include "type_malloc.h"
#include "thread_pool.h"
#include "lio.h"
#include "object_service_abstract.h"
#include "iniparse.h"
#include "string_token.h"

int main(int argc, char **argv)
{
  int i, owner_mode, exnode_mode, size_mode, n, nfailed, start_option;
  lio_fsck_iter_t *it;
  char *owner;
  char *fname;
  op_generic_t *gop;
  op_status_t status;
  lio_path_tuple_t tuple;
  int ftype, err;

  if (argc < 2) {
     printf("\n");
     printf("lio_fsck LIO_COMMON_OPTIONS  [-o parent|manual|delete|user valid_user]  [-ex parent|manual|delete] [-s manual|repair] path\n");
     lio_print_options(stdout);
     printf("    -o                 - How to handle missing system.owner issues.  Default is manual.\n");
     printf("                            parent - Make the object owner the same as the parent directory.\n");
     printf("                            manual - Do nothing.  Leave the owner as missing.\n");
     printf("                            delete - Remove the object\n");
     printf("                            user valid_user - Make the provided user the object owner.\n");
     printf("    -ex                - How to handle missing exnode issues.  Default is manual.\n");
     printf("                            parent - Create an empty exnode using the parent exnode.\n");
     printf("                            manual - Do nothing.  Leave the exnode as missing or blank.\n");
     printf("                            delete - Remove the object\n");
     printf("    -s                 - How to handle missing exnode size.  Default is repair.\n");
     printf("                            manual - Do nothing.  Leave the size missing.\n");
     printf("                            repair - If the exnode existst load it and determine the size.\n");
     printf("    path               - Path prefix to use\n");
     printf("\n");
     return(1);
  }

  lio_init(&argc, &argv);

  if (argc < 2) {
     printf("Missing path!\n");
     return(1);
  }

  owner_mode = LIO_FSCK_MANUAL;
  owner = NULL;
  exnode_mode = 0;
  size_mode = 0;
  i=1;
  do {
     start_option = i;

     if (strcmp(argv[i], "-o") == 0) {    //** How to handle owner problems
        i++;
        if (strcmp(argv[i], "manual") == 0) {
           owner_mode = LIO_FSCK_MANUAL;
        } else if (strcmp(argv[i], "parent") == 0) {
           owner_mode = LIO_FSCK_PARENT;
        } else if (strcmp(argv[i], "delete") == 0) {
           owner_mode = LIO_FSCK_DELETE;
        } else if (strcmp(argv[i], "user") == 0) {
           owner_mode = LIO_FSCK_USER;
           i++;
           owner = argv[i];
        }
        i++;
     } else if (strcmp(argv[i], "-ex") == 0) {  //** How to handle exnode issues
        i++;
        if (strcmp(argv[i], "manual") == 0) {
           exnode_mode = LIO_FSCK_MANUAL;
        } else if (strcmp(argv[i], "parent") == 0) {
           exnode_mode = LIO_FSCK_PARENT;
        } else if (strcmp(argv[i], "delete") == 0) {
           exnode_mode = LIO_FSCK_DELETE;
        }
        i++;
     } else if (strcmp(argv[i], "-s") == 0) {   //** How to handle size problems
        i++;
        if (strcmp(argv[i], "manual") == 0) {
           size_mode = LIO_FSCK_MANUAL;
        } else if (strcmp(argv[i], "repair") == 0) {
           size_mode = LIO_FSCK_SIZE_REPAIR;
        }
        i++;
     }
  } while ((start_option < i) && (i<argc));

  if (i>=argc) {
     info_printf(lio_ifd, 0, "Missing directory!\n");
     return(2);
  }

  //** Create the simple path iterator
  tuple = lio_path_resolve(lio_gc->auto_translate, argv[i]);

  info_printf(lio_ifd, 0, "--------------------------------------------------------------------\n");
  if (owner_mode == LIO_FSCK_USER) {
     info_printf(lio_ifd, 0, "Using path=%s and owner_mode=%d (%s) exnode_mode=%d size_mode=%d (%d=manual, %d=parent, %d=delete, %d=user, %d=repair)\n",
            tuple.path, owner_mode, owner, exnode_mode, size_mode, LIO_FSCK_MANUAL, LIO_FSCK_PARENT, LIO_FSCK_DELETE, LIO_FSCK_USER, LIO_FSCK_SIZE_REPAIR);
  } else {
     info_printf(lio_ifd, 0, "Using path=%s and owner_mode=%d exnode_mode=%d size_mode=%d (%d=manual, %d=parent, %d=delete, %d=user, %d=repair)\n",
            tuple.path, owner_mode, exnode_mode, size_mode, LIO_FSCK_MANUAL, LIO_FSCK_PARENT, LIO_FSCK_DELETE, LIO_FSCK_USER, LIO_FSCK_SIZE_REPAIR);
  }
  info_printf(lio_ifd, 0, "Possible error states: %d=missing owner, %d=missing exnode, %d=missing size, %d=missing inode\n", LIO_FSCK_MISSING_OWNER, LIO_FSCK_MISSING_EXNODE, LIO_FSCK_MISSING_EXNODE_SIZE, LIO_FSCK_MISSING_INODE);
  info_printf(lio_ifd, 0, "--------------------------------------------------------------------\n");
  info_flush(lio_ifd);

  n = 0; nfailed = 0;
  exnode_mode = exnode_mode | size_mode;
  it = lio_create_fsck_iter(tuple.lc, tuple.creds, tuple.path, LIO_FSCK_MANUAL, NULL, LIO_FSCK_MANUAL);  //** WE use resolve to clean up so we can see the problem objects
  while ((err = lio_next_fsck(tuple.lc, it, &fname, &ftype)) != LIO_FSCK_FINISHED) {
     info_printf(lio_ifd, 0, "err:%d  type:%d  object:%s\n", err, ftype, fname);
     if ((owner_mode != LIO_FSCK_MANUAL) || (exnode_mode != LIO_FSCK_MANUAL)) {
        gop = lio_fsck_object(tuple.lc, tuple.creds, fname, ftype, owner_mode, owner, exnode_mode);
        gop_waitany(gop);
        status = gop_get_status(gop);
        gop_free(gop, OP_DESTROY);
        if (status.error_code != OS_FSCK_GOOD) nfailed++;
        info_printf(lio_ifd, 0, "    resolve:%d  object:%s\n", status.error_code, fname);
     }

     free(fname);
     fname = NULL;
     n++;
  }

  lio_destroy_fsck_iter(tuple.lc, it);

  info_printf(lio_ifd, 0, "--------------------------------------------------------------------\n");
  info_printf(lio_ifd, 0, "Problem objects: %d  Repair Failed count: %d\n", n, nfailed);
  info_printf(lio_ifd, 0, "--------------------------------------------------------------------\n");

  lio_path_release(&tuple);

  lio_shutdown();

  return(0);
}

