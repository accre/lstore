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

#define _log_module_index 194

#include <assert.h>
#include "exnode.h"
#include "log.h"
#include "iniparse.h"
#include "type_malloc.h"
#include "thread_pool.h"
#include "lio.h"
#include "string_token.h"

typedef struct {
  char *fname;
  int64_t bytes;
  int64_t count;
  int ftype;
} du_entry_t;

lio_path_tuple_t tuple;

int base = 1;

//*************************************************************************
// ls_format_entry - Prints an LS entry
//*************************************************************************

void du_format_entry(info_fd_t *ifd, du_entry_t *de, int sumonly)
{
  char *dtype;
  char ppsize[128];
  double fsize;
  long int n;

//  fprintf(stdout, "----------  ------------------------------\n");

  fsize = de->bytes;
  if (base == 1) {
       sprintf(ppsize, I64T, de->bytes);
  } else {
     pretty_print_double_with_scale(base, fsize, ppsize);
  }

  dtype = ((de->ftype & OS_OBJECT_DIR) > 0) ? "/" : "";
  if (sumonly == 1) {
     n = de->count;
     info_printf(ifd, 0, "%10s  %10ld  %s%s\n", ppsize, n, de->fname, dtype);
  } else {
     info_printf(ifd, 0, "%10s  %s%s\n", ppsize, de->fname, dtype);
  }

  return;
}

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
  int i, ftype, rg_mode, start_index, start_option, nosort, prefix_len;
  char *fname;
  du_entry_t *de;
  list_t *table, *sum_table, *lt;
  os_regex_table_t *rp_single, *ro_single;
  os_object_iter_t *it;
  list_iter_t lit;
  char *key = "system.exnode.size";
  char *val, *file;
  int64_t bytes;
  int v_size, sumonly, ignoreln;
  int recurse_depth = 10000;

//printf("argc=%d\n", argc);
  if (argc < 2) {
     printf("\n");
     printf("lio_du LIO_COMMON_OPTIONS [-rd recurse_depth] [-ns] [-h|-hi] [-s] [-ln] path\n");
     printf("lio_du LIO_COMMON_OPTIONS [-rd recurse_depth] [-ns] [-h|-hi] [-s] [-ln] LIO_PATH_OPTIONS\n");
     lio_print_options(stdout);
     lio_print_path_options(stdout);
     printf("\n");
     printf("    -rd recurse_depth  - Max recursion depth on directories. Defaults to %d\n", recurse_depth);
     printf("    -ns                - Don't sort the output\n");
     printf("    -h                 - Print using base 1000\n");
     printf("    -hi                - Print using base 1024\n");
     printf("    -s                 - Print directory summaries only\n");
     printf("    -ln                - Follow links.  Otherwise they are ignored\n");
     printf("    path               - Path glob to scan\n");
     return(1);
  }

  lio_init(&argc, &argv);

  rg_mode = 0;
  rp_single = ro_single = NULL;

  rg_mode = lio_parse_path_options(&argc, argv, &tuple, &rp_single, &ro_single);

  //*** Parse the args
  nosort = 0;
  base = 1;
  ignoreln = 1;
  sumonly = 0;

  i=1;
  do {
     start_option = i;

     if (strcmp(argv[i], "-rd") == 0) { //** Recurse depth
        i++;
        recurse_depth = atoi(argv[i]); i++;
     } else if (strcmp(argv[i], "-ns") == 0) {  //** Strip off the path prefix
        i++;
        nosort = 1;
     } else if (strcmp(argv[i], "-s") == 0) {  //** Summary only
        i++;
        sumonly = 1;
     } else if (strcmp(argv[i], "-h") == 0) {  //** Use base 10
        i++;
        base = 1000;
     } else if (strcmp(argv[i], "-hi") == 0) {  //** Use base 2
        i++;
        base = 1024;
     } else if (strcmp(argv[i], "-ln") == 0) {  //** Follow links
        i++;
        ignoreln = 0;
     }

  } while ((start_option < i) && (i<argc));
  start_index = i;

  if (sumonly == 1) nosort = 0;  //** Doing a tally overides the no sort option

  if (rg_mode == 0) {
     if (i>=argc) {
        printf("Missing directory!\n");
        return(2);
     }

     //** Create the simple path iterator
     tuple = lio_path_resolve(argv[i]);
     rp_single = os_path_glob2regex(tuple.path);
  }

//log_printf(15, "argv[%d]=%s\n", i, argv[i]);


  //** Make the toplevel list
  if (sumonly == 1) {
log_printf(15, "MAIN SUMONLY=1\n");
     sum_table = list_create(0, &list_string_compare, NULL, list_no_key_free, list_no_data_free);

     v_size = -1024;
     val = NULL;
     it = os_create_object_iter_alist(tuple.lc->os, tuple.creds, rp_single, ro_single, OS_OBJECT_ANY, 0, &key, (void **)&val, &v_size, 1);
     if (it == NULL) {
        log_printf(0, "ERROR: Failed with object_iter creation\n");
        goto finished;
      }

     while ((ftype = os_next_object(tuple.lc->os, it, &fname, &prefix_len)) > 0) {
        if (((ftype & OS_OBJECT_LINK) > 0) && (ignoreln == 1)) continue;  //** Ignoring links

log_printf(15, "sumonly inserting fname=%s\n", fname);
        type_malloc_clear(de, du_entry_t, 1);
        de->fname = fname;
        de->ftype = ftype;

        if (val != NULL) sscanf(val, I64T, &(de->bytes));
        list_insert(sum_table, de->fname, de);

        v_size = -1024;
        free(val); val = NULL;
     }

     os_destroy_object_iter(tuple.lc->os, it);

log_printf(15, "sum_table=%d\n", list_key_count(sum_table));
  }

log_printf(15, "MAIN LOOP\n");

  v_size = -1024;
  val = NULL;
  it = os_create_object_iter_alist(tuple.lc->os, tuple.creds, rp_single, ro_single, OS_OBJECT_ANY, recurse_depth, &key, (void **)&val, &v_size, 1);
  if (it == NULL) {
     log_printf(0, "ERROR: Failed with object_iter creation\n");
     goto finished;
   }

  if (sumonly == 1) {
     info_printf(lio_ifd, 0, "  Size      File count            Filename\n");
     info_printf(lio_ifd, 0, "----------  ----------  ------------------------------\n");
  } else {
     info_printf(lio_ifd, 0, "  Size               Filename\n");
     info_printf(lio_ifd, 0, "----------  ------------------------------\n");
  }

  table = list_create(0, &list_string_compare, NULL, list_no_key_free, list_no_data_free);
  while ((ftype = os_next_object(tuple.lc->os, it, &fname, &prefix_len)) > 0) {
//printf("fname=%s\n", fname);
     if (((ftype & OS_OBJECT_LINK) > 0) && (ignoreln == 1)) continue;  //** Ignoring links
//printf("fname2=%s\n", fname);

      if ((sumonly == 1) && ((ftype & OS_OBJECT_FILE) > 0)) {
        bytes = 0;
        if (val != NULL) sscanf(val, I64T, &bytes);

        lit = list_iter_search(sum_table, NULL, 0);
        while ((list_next(&lit, (list_key_t **)&file, (list_data_t **)&de)) == 0) {
             if (strncmp(de->fname, fname, strlen(de->fname)) == 0) {
                log_printf(15, "accum de->fname=%s fname=%s\n", de->fname, fname);
                de->bytes += bytes;
                de->count++;
             }
        }
      } else {
        type_malloc_clear(de, du_entry_t, 1);
        de->fname = fname;
        de->ftype = ftype;

        if (val != NULL) sscanf(val, I64T, &(de->bytes));

//printf("fname=%s size=" I64T "\n", de->fname, de->bytes);
        if (nosort == 1) {
           du_format_entry(lio_ifd, de, sumonly);
           free(de->fname);
           free(de);
        } else {
           list_insert(table, de->fname, de);
        }
     }

     v_size = -1024;
     free(val); val = NULL;
  }

  os_destroy_object_iter(tuple.lc->os, it);

  if ((nosort == 1) && (sumonly == 0)) goto finished;  //** Check if we're done

  //** Now summarize and print things
  lt = (sumonly == 1) ? sum_table : table;

  lit = list_iter_search(lt, "", 0);
  while ((list_next(&lit, (list_key_t **)&fname, (list_data_t **)&de)) == 0) {
      du_format_entry(lio_ifd, de, sumonly);
  }

finished:
  lio_path_release(&tuple);
  if (ro_single != NULL) os_regex_table_destroy(ro_single);
  if (rp_single != NULL) os_regex_table_destroy(rp_single);

  lio_shutdown();

  return(0);
}

