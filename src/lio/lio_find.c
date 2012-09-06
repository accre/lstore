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

#define _log_module_index 195

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
  int i, ftype, rg_mode, start_index, start_option, prefix_len, nopre;
  char *fname;
  lio_path_tuple_t tuple;
  os_regex_table_t *rp_single, *ro_single;
  os_object_iter_t *it;

  int recurse_depth = 10000;
  int obj_types = OS_OBJECT_FILE;

//printf("argc=%d\n", argc);
  if (argc < 2) {
     printf("\n");
     printf("lio_find LIO_COMMON_OPTIONS [-rd recurse_depth] [-t object_types] [-nopre] path\n");
     printf("lio_find LIO_COMMON_OPTIONS [-rd recurse_depth] [-t object_types] [-nopre] LIO_PATH_OPTIONS\n");
     lio_print_options(stdout);
     lio_print_path_options(stdout);
     printf("\n");
     printf("    -rd recurse_depth  - Max recursion depth on directories. Defaults to %d\n", recurse_depth);
     printf("    -t  object_types   - Types of objects to list bitwise OR of 1=Files, 2=Directories, 4=link.  Default is %d.\n", obj_types);
     printf("    -nopre             - Don't print the scan common prefix\n");
     printf("    path               - Path glob to scan\n");
     return(1);
  }

  lio_init(&argc, &argv);


  //*** Parse the args
  rg_mode = 0;
  rp_single = ro_single = NULL;
  nopre = 0;

  rg_mode = lio_parse_path_options(&argc, argv, &tuple, &rp_single, &ro_single);

  i=1;
  do {
     start_option = i;

     if (strcmp(argv[i], "-rd") == 0) { //** Recurse depth
        i++;
        recurse_depth = atoi(argv[i]); i++;
     } else if (strcmp(argv[i], "-t") == 0) {  //** Object types
        i++;
        obj_types = atoi(argv[i]); i++;
     } else if (strcmp(argv[i], "-nopre") == 0) {  //** Strip off the path prefix
        i++;
        nopre = 1;
     }

  } while ((start_option < i) && (i<argc));
  start_index = i;


  if (rg_mode == 0) {
     if (i>=argc) {
        info_printf(lio_ifd, 0, "Missing directory!\n");
        return(2);
     }

     //** Create the simple path iterator
     tuple = lio_path_resolve(argv[i]);
     rp_single = os_path_glob2regex(tuple.path);
  }

  it = os_create_object_iter(tuple.lc->os, tuple.creds, rp_single, ro_single, obj_types, NULL, recurse_depth, NULL, 0);
  if (it == NULL) {
     log_printf(0, "ERROR: Failed with object_iter creation\n");
     goto finished;
   }

  while ((ftype = os_next_object(tuple.lc->os, it, &fname, &prefix_len)) > 0) {
//     printf("len=%d full=%s nopref=%s\n", prefix_len, fname, &(fname[prefix_len]));

     if (nopre == 1) {
        info_printf(lio_ifd, 0, "%s\n", &(fname[prefix_len]));
     } else {
        info_printf(lio_ifd, 0, "%s\n", fname);
     }
  }

  os_destroy_object_iter(tuple.lc->os, it);

finished:
  lio_path_release(&tuple);
  if (ro_single != NULL) os_regex_table_destroy(ro_single);
  if (rp_single != NULL) os_regex_table_destroy(rp_single);

  lio_shutdown();

  return(0);
}


