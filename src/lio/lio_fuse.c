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

//*************************************************************************
//*************************************************************************

void print_usage(void)
{
     printf("\n"
            "lio_fuse mount_point [FUSE_OPTIONS] [--lio LIO_COMMON_OPTIONS]\n");
     lio_print_options(stdout);
     printf("    FUSE_OPTIONS:\n"
            "       -h   --help            print this help\n"
	    "       -ho                    print FUSE mount options help\n"
            "       -V   --version         print FUSE version\n"
            "       -d   -o debug          enable debug output (implies -f)\n"
            "       -s                     disable multi-threaded operation\n"
            "       -f                     foreground operation\n"
            "                                (REQUIRED unless  '-c /absolute/path/lio.cfg' is specified and all included files are absolute paths)"
            "       -o OPT[,OPT...]        mount options\n"
            "                                (for possible values of OPT see 'man mount.fuse' or see 'lio_fuse -ho')\n");
}

int main(int argc, char **argv)
{
  int err = -1;
  lio_fuse_init_args_t lio_args;
  int fuse_argc;
  char **fuse_argv;
  
  // DEBUG
  int i;
  printf("argc=%d\n", argc);
  for (i=0; i<argc; i++) {
    printf("argv[%d]=%s\n", i, argv[i]);
  }

  if (argc < 2 || strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0) {
     print_usage();
     return(1);
  }

  // ** split lio and fuse arguments **

  int idx;
  lio_args.mount_point = argv[1];

  // these defaults hold if --lio is not used on the commandline
  fuse_argc = argc;
  fuse_argv = argv;
  lio_args.lio_argc = 1;
  lio_args.lio_argv = argv;

  for (idx=1; idx<argc; idx++) {
    if(strcmp(argv[idx], "--lio") == 0) {
      fuse_argc = idx;
      lio_args.lio_argc = argc - idx;
      lio_args.lio_argv = &argv[idx];
      lio_args.lio_argv[0] = argv[0]; //replace "--lio" with the executable name because the parser may reasonably expect the zeroth argument to be the program name
    }
  }

  // DEBUG
  printf("\nfuse_argc=%d\n", fuse_argc);
  for (i=0; i<fuse_argc; i++) {
    printf("fuse_argv[%d]=%s\n", i, fuse_argv[i]);
  }
  printf("\nlio_argc=%d\n", lio_args.lio_argc);
  for (i=0; i<lio_args.lio_argc; i++) {
    printf("fuse_argv[%d]=%s\n", i, lio_args.lio_argv[i]);
  }
  printf("mountpoint=%s\n",lio_args.mount_point);

  umask(0);
  
  err = fuse_main(fuse_argc, fuse_argv, &lfs_fops, &lio_args /* <- stored to fuse's ctx->private_data*/);

  return(err);
}
