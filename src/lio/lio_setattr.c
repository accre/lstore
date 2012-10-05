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

#define _log_module_index 200

#include <assert.h>
#include "exnode.h"
#include "log.h"
#include "iniparse.h"
#include "type_malloc.h"
#include "thread_pool.h"
#include "lio.h"
#include "string_token.h"

#define MAX_SET 1000


//*************************************************************************
// load_file - Loads a file from disk
//*************************************************************************

void load_file(char *fname, char **val, int *v_size)
{
  FILE *fd;
  int i;

  *v_size = 0;  *val = NULL;

  fd = fopen(fname, "r");
  if (fd == NULL) {
     info_printf(lio_ifd, 0, "ERROR opeing file=%s!  Exiting\n", fname);
     exit(1);
  }
  fseek(fd, 0, SEEK_END);

  i = ftell(fd);
  type_malloc(*val, char, i+1);
  (*val)[i] = 0;
  *v_size = i;

  fseek(fd, 0, SEEK_SET);
  if (fread(*val, i, 1, fd) != 1) { //**
     info_printf(lio_ifd, 0, "ERROR reading file=%s! Exiting\n", fname);
     exit(1);
  }
  fclose(fd);

//info_printf(lio_ifd, 0, "fname=%s size=%d val=%s\n", fname, i, *val);

}

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
  int i, rg_mode, start_index, start_option, err, fin;
  lio_path_tuple_t tuple;
  os_regex_table_t *rp_single, *ro_single;
  os_object_iter_t *it;
  os_fd_t *fd;
  char *bstate;
  char *path;
  char *key[MAX_SET];
  char *val[MAX_SET];
  int v_size[MAX_SET];
  int n_keys;
  char *dkey[MAX_SET], *tmp;
  char *sobj[MAX_SET], *skey[MAX_SET];
  int n_skeys;
  int ftype, prefix_len;
  char *fname;

  memset(dkey, 0, sizeof(dkey));
  memset(sobj, 0, sizeof(sobj));
  memset(skey, 0, sizeof(skey));

  char *delims = "=";

  int recurse_depth = 10000;
  int obj_types = OS_OBJECT_FILE;

//printf("argc=%d\n", argc);
  if (argc < 2) {
     printf("\n");
     printf("lio_setattr LIO_COMMON_OPTIONS [-rd recurse_depth] [-t object_types] [-delim c] -p path -as key=value | -ar key | -af key=vfilename | -al key=obj_path/dkey\n");
     printf("lio_setattr LIO_COMMON_OPTIONS [-rd recurse_depth] [-t object_types] LIO_PATH_OPTIONS -as key=value | -ar key | -af key=vfilename | -al key=obj_path/dkey\n");
     lio_print_options(stdout);
     lio_print_path_options(stdout);
     printf("\n");
     printf("    -rd recurse_depth  - Max recursion depth on directories. Defaults to %d\n", recurse_depth);
     printf("    -t  object_types   - Types of objects to list bitwise OR of 1=Files, 2=Directories, 4=symlink, 8=hardlink.  Default is %d.\n", obj_types);
     printf("    -p  path           - Path glob to scan\n");
     printf("    -delim c           - Key/value delimeter characters.  Defauls is %s.\n", delims);
     printf("    -as key=value      - Breaks up the literal string into the key/value pair and stores it.\n");
     printf("    -ar key            - Remove the key.\n");
     printf("    -af key=vfilename  - Similar to -as but the valure is loaded from the given vfilename.\n");
     printf("    -al key=sobj_path/skey - Symlink the key to another objects (sobj_path) key(skey).\n");
     printf("\n");
     printf("       NOTE: Multiple -as/-ar/-af/-al clauses are allowed\n\n");
     return(1);
  }

  lio_init(&argc, &argv);


  //*** Parse the args
  rg_mode = 0;
  rp_single = ro_single = NULL;
  n_keys = 0; n_skeys = 0;
  path = NULL;

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
     } else if (strcmp(argv[i], "-delim") == 0) {  //** Get the delimiter
        i++;
        delims = argv[i]; i++;
     } else if (strcmp(argv[i], "-p") == 0) {  //** Path
        i++;
        path = argv[i]; i++;
     } else if (strcmp(argv[i], "-as") == 0) {  //** String attribute
        i++;
        key[n_keys] = string_token(argv[i], delims, &bstate, &fin);
        val[n_keys] = string_token(NULL, delims, &bstate, &fin);
        v_size[n_keys] = strlen(val[n_keys]);
        if (strcmp(val[n_keys], "") == 0) val[n_keys] = NULL;
        n_keys++;  i++;
     } else if (strcmp(argv[i], "-ar") == 0) {  //** Remove the attribute
        i++;
        key[n_keys] = string_token(argv[i], delims, &bstate, &fin);
        val[n_keys] = NULL;
        v_size[n_keys] = -1;
        n_keys++;  i++;
     } else if (strcmp(argv[i], "-af") == 0) {  //** File attribute
        i++;
        key[n_keys] = string_token(argv[i], delims, &bstate, &fin);
        load_file(string_token(NULL, delims, &bstate, &fin), &(val[n_keys]), &(v_size[n_keys]));
        n_keys++;  i++;
     } else if (strcmp(argv[i], "-al") == 0) {  //** Symlink attributes
        i++;
        dkey[n_skeys] = string_token(argv[i], delims, &bstate, &fin);
        tmp = string_token(NULL, delims, &bstate, &fin);
        os_path_split(tmp, &(sobj[n_skeys]), &(skey[n_skeys]));
        n_skeys++;  i++;
     }
  } while ((start_option < i) && (i<argc));
  start_index = i;


  if (rg_mode == 0) {
     if (path == NULL) {
        info_printf(lio_ifd, 0, "Missing directory!\n");
        return(2);
     }

     //** Create the simple path iterator
     tuple = lio_path_resolve(path);
     rp_single = os_path_glob2regex(tuple.path);
  }


  if (n_keys > 0) {
     err = gop_sync_exec(os_regex_object_set_multiple_attrs(tuple.lc->os, tuple.creds, NULL, rp_single,  ro_single, obj_types, recurse_depth, key, (void **)val, v_size, n_keys));
     if (err != OP_STATE_SUCCESS) {
        info_printf(lio_ifd, 0, "ERROR with operation! \n");
     }
  }

  if (n_skeys > 0) {  //** For symlink attrs we have to manually iterate
     it = os_create_object_iter(tuple.lc->os, tuple.creds, rp_single, ro_single, obj_types, NULL, recurse_depth, NULL, 0);
     if (it == NULL) {
        info_printf(lio_ifd, 0, "ERROR: Failed with object_iter creation\n");
        goto finished;
     }

     while ((ftype = os_next_object(tuple.lc->os, it, &fname, &prefix_len)) > 0) {
        err = gop_sync_exec(os_open_object(tuple.lc->os, tuple.creds, fname, OS_MODE_READ_IMMEDIATE, NULL, &fd, 30));
        if (err != OP_STATE_SUCCESS) {
           info_printf(lio_ifd, 0, "ERROR: opening file: %s.  Skipping.\n", fname);
        } else {
           //** Do the symlink
           err = gop_sync_exec(os_symlink_multiple_attrs(tuple.lc->os, tuple.creds, sobj, skey, fd, dkey, n_skeys));
           if (err != OP_STATE_SUCCESS) {
              info_printf(lio_ifd, 0, "ERROR: with linking file: %s\n", fname);
           }

           //** Close the file
           err = gop_sync_exec(os_close_object(tuple.lc->os, fd));
           if (err != OP_STATE_SUCCESS) {
              info_printf(lio_ifd, 0, "ERROR: closing file: %s\n", fname);
           }
        }

        free(fname);
     }

     os_destroy_object_iter(tuple.lc->os, it);
  }

finished:
  for (i=0; i<n_skeys; i++) {
    if (sobj[i] != NULL) free(sobj[i]);
    if (skey[i] != NULL) free(skey[i]);
  }

  lio_path_release(&tuple);
  if (ro_single != NULL) os_regex_table_destroy(ro_single);
  if (rp_single != NULL) os_regex_table_destroy(rp_single);

  lio_shutdown();

  return(0);
}


