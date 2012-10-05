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

#define _log_module_index 192

#include <assert.h>
#include "exnode.h"
#include "log.h"
#include "iniparse.h"
#include "type_malloc.h"
#include "thread_pool.h"
#include "lio.h"

char *exnode_data = NULL;


//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
  int ftype, err, symlink, i, force, start_option;
  char *src_fname, *dest_fname;
  char *dir, *file;
  lio_path_tuple_t stuple, dtuple;
  char fullname[OS_PATH_MAX];

//printf("argc=%d\n", argc);

  if (argc < 3) {
     printf("\n");
     printf("lio_ln LIO_COMMON_OPTIONS [-s] [-f] source_file linked_dest_file\n");
     lio_print_options(stdout);
     printf("    -s               - Make symbolic links instead of hard links\n");
     printf("    -f               - Force the link creation even if source doesn't exist. Only used for symlinks\n");

     return(1);
  }

  lio_init(&argc, &argv);

  if (argc < 3) {
     printf("\n");
     printf("lio_ln LIO_COMMON_OPTIONS source_file linked_dest_file\n");
     lio_print_options(stdout);
     printf("    -s               - Make symbolic links instead of hard links\n");
     printf("    -f               - Force the link creation even if source doesn't exist. Only used for symlinks\n");
     return(1);
  }

  symlink = 0;
  force = 0;
  i = 1;
  do {
     start_option = i;

    if (strcmp(argv[i], "-s") == 0) {
       symlink = 1;
       i++;
    } else if (strcmp(argv[i], "-f") == 0) {
       force = 1;
       i++;
    }
  } while ((start_option < i) && (i<argc));

  src_fname = argv[i];  i++;  stuple = lio_path_resolve(src_fname);
  dest_fname = argv[i]; i++;  dtuple = lio_path_resolve(dest_fname);

  //** Make sure we're linking in the same system
  if (strcmp(stuple.lc->section_name, dtuple.lc->section_name) != 0) {
     printf("Source and destination objects must exist in the same system!\n");
     printf("Source: %s   Dest: %s\n", stuple.lc->section_name, dtuple.lc->section_name);
     return(1);
  }

  if (symlink == 0) force = 0;

  if (stuple.path[0] == '/') { //** Absolute path
     ftype = lioc_exists(stuple.lc, stuple.creds, stuple.path);
  } else {  //** Relative path
     os_path_split(dtuple.path, &dir, &file);
     snprintf(fullname, OS_PATH_MAX, "%s/%s", dir, stuple.path);
     ftype = lioc_exists(stuple.lc, stuple.creds, fullname);
     free(dir); free(file);
  }

  //** Check on the source file
  if (force == 0) {
     if (ftype == 0) { //** The file doesn't exists
        printf("ERROR source file doesn't exist: %s\n", stuple.path);
        err = 1;
        goto finished;
     }
  }

  if (((ftype & OS_OBJECT_DIR)>0) && (symlink == 0))  { //** Can only symlink a file
     printf("ERROR Can't hard link directories!  Source: %s\n", stuple.path);
     err = 1;
     goto finished;
  }

  //** Check on the dest file
  ftype = lioc_exists(dtuple.lc, dtuple.creds, dtuple.path);
  if (ftype != 0) { //** The file doesn't exists
     printf("ERROR destination file exists: %s\n", dtuple.path);
     err = 1;
     goto finished;
  }

  //** Now create the link
  err = gop_sync_exec(lio_link_object(dtuple.lc, dtuple.creds, symlink, stuple.path, dtuple.path, NULL));
  if (err != OP_STATE_SUCCESS) {
     info_printf(lio_ifd, 0, "ERROR linking file!\n");
     err = 1;
     goto finished;
  }

  err = 0;

finished:
  lio_path_release(&stuple);
  lio_path_release(&dtuple);

  lio_shutdown();

  return(err);
}
