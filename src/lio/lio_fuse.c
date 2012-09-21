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

#define _log_module_index 211


#include <assert.h>
#include "lio_fuse.h"
#include "exnode.h"
#include "log.h"
#include "iniparse.h"
#include "type_malloc.h"
#include "thread_pool.h"
#include "lio.h"

extern struct fuse_lowlevel_ops llops;

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
  struct fuse_chan *ch;
  char *mountpoint;
  struct fuse_args fargs;

  int err = -1;


//printf("argc=%d\n", argc);

  if (argc < 2) {
     printf("\n");
     printf("lio_fuse LIO_COMMON_OPTIONS mount_point FUSE_OPTIONS\n");
     lio_print_options(stdout);
     return(1);
  }

  lio_init(&argc, &argv);

//int i;
//printf("argc=%d\n", argc);
//for (i=0; i<argc; i++) {
//  printf("argv[%d]=%s\n", i, argv[i]);
//}

  fargs.argc = argc;
  fargs.argv = argv;
  fargs.allocated = 0;

  lfs_gc = lio_fuse_init(lio_gc);


  if (fuse_parse_cmdline(&fargs, &mountpoint, NULL, NULL) != -1 &&
      (ch = fuse_mount(mountpoint, &fargs)) != NULL) {
      struct fuse_session *se;

      se = fuse_lowlevel_new(&fargs, &lfs_gc_llops, sizeof(struct fuse_lowlevel_ops), NULL);
      if (se != NULL) {
         if (fuse_set_signal_handlers(se) != -1) {
            fuse_session_add_chan(se, ch);
            err = fuse_session_loop(se);
            fuse_remove_signal_handlers(se);
            fuse_session_remove_chan(ch);
         }
         fuse_session_destroy(se);
      }
      fuse_unmount(mountpoint, ch);
  }
  fuse_opt_free_args(&fargs);

  lio_fuse_destroy(lfs_gc);

  lio_shutdown();

  return(err);
}
