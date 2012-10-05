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

#define _log_module_index 187

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

int nfailed = 0;
char *prefix = NULL;

int wait_time = 20;

#define PATH_LEN 4096



// **********************************************************************************
int qcompare(const void *s1, const void *s2)
{
   return strcmp(* (char * const *) s1, * (char * const *) s2);
}


// **********************************************************************************
// path_scan_and_check - Does a regex scan of the given path and does a comparison
//    of the results
// **********************************************************************************

int path_scan_and_check(char *path, char **match, int n_match, int recurse_depth, int object_types)
{
   os_object_iter_t *it;
   os_regex_table_t *regex;
   int err, size_err, i, n, n_max, j, plen;
   char **name;

   regex = os_path_glob2regex(path);
   it = os_create_object_iter(lio_gc->os, lio_gc->creds, regex, NULL, object_types, NULL, 1000, NULL, 0);
   if (it == NULL) {
      log_printf(0, "ERROR: Failed with object_iter creation %s\n", path);
      return(1);
   }

   n_max = n_match + 1;
   type_malloc_clear(name, char *, n_max);
   n = 0;
   while (os_next_object(lio_gc->os, it, &(name[n]), &plen) > 0) {
     n++;
     if (n>=n_max) {
        n_max = n_max + 5;
        type_realloc(name, char *, n_max);
     }
   }
   os_destroy_object_iter(lio_gc->os, it);
   os_regex_table_destroy(regex);


   // ** Sort both lists and do the comparison
   qsort(match, n_match, sizeof(char *), qcompare);
   qsort(name, n, sizeof(char *), qcompare);

   size_err = 0;
   if (n != n_match) { size_err = -1; log_printf(0, "ERROR: Sizes don't match n_match=%d found=%d\n", n_match, n); }
   j = (n>n_match) ? n_match : n;

   err = 0;
   for (i=0; i<j; i++) {
     if (strcmp(match[i], name[i]) != 0) {
        err = i;
        break;
     }
   }

   if ((err != 0) || (size_err != 0)) {
      log_printf(0,"-------------Initial slot mismatch=%d (-1 if sizes don't match)-------------------------------\n", err);
      for (i=0; i<n_match; i++) log_printf(0, "match[%d]=%s\n", i, match[i]);
      for (i=0; i<n; i++) log_printf(0, "found[%d]=%s\n", i, name[i]);
      log_printf(0,"----------------------------------------------------------------------------------------------\n");
   }

   for (i=0; i<n; i++) free(name[i]);
   free(name);

   if (size_err != 0) err = size_err;
   return(err);
}


// **********************************************************************************
//  os_create_remove_tests - Just does object tests: create, remove, recurse
// **********************************************************************************

void os_create_remove_tests()
{
   object_service_fn_t *os = lio_gc->os;
   creds_t  *creds = lio_gc->creds;
   int err, i;
   char foo_path[PATH_LEN];
   char bar_path[PATH_LEN];
   char hfoo_path[PATH_LEN];
   char *match_path[10];
   os_fd_t *foo_fd;
   os_regex_table_t *regex;

   for (i=0; i<10; i++) {
      type_malloc_clear(match_path[i], char, PATH_LEN);
   }

   // ** Create FILE foo
   snprintf(foo_path, PATH_LEN, "%s/foo", prefix);
   err = gop_sync_exec(os_create_object(os, creds, foo_path, OS_OBJECT_FILE, "me"));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: creating file: %s err=%d\n", foo_path, err);
      return;
   }

   // ** Verify foo exists
   err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: opening file: %s err=%d\n", foo_path, err);
      return;
   }
   err = gop_sync_exec(os_close_object(os, foo_fd));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
      return;
   }

   // ** Rename it
   snprintf(bar_path, PATH_LEN, "%s/bar", prefix);
   err = gop_sync_exec(os_move_object(os, creds, foo_path, bar_path));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: moving file src: %s  dest: %s err=%d\n", foo_path, bar_path, err);
      return;
   }


   // ** Verify foo is gone
   err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
   if (err == OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Oops!  file existst after rename: %s err=%d\n", foo_path, err);
      return;
   }

   // ** Verify it's there under the new name bar
   err = gop_sync_exec(os_open_object(os, creds, bar_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: opening file: %s err=%d\n", bar_path, err);
      return;
   }
   err = gop_sync_exec(os_close_object(os, foo_fd));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Closing file: %s err=%d\n", bar_path, err);
      return;
   }

   // ** Make a softlink foo->bar
   err = gop_sync_exec(os_symlink_object(os, creds, bar_path, foo_path, "me"));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: linking file src: %s  dest: %s err=%d\n", bar_path, foo_path, err);
      return;
   }

   // ** Verify it exists
   err = gop_sync_exec(os_exists(os, creds, foo_path));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Oops! Lost bar! %s\n", bar_path);
      return;
   }

   // ** hardlink hfoo->foo
   snprintf(hfoo_path, PATH_LEN, "%s/hfoo", prefix);
   err = gop_sync_exec(os_hardlink_object(os, creds, foo_path, hfoo_path, "me"));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: hard linking file src: %s  dest: %s err=%d\n", foo_path, hfoo_path, err);
      return;
   }

   // ** Verify they all exist
   err = gop_sync_exec(os_exists(os, creds, hfoo_path));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Oops! Lost %s\n", hfoo_path);
      return;
   }
   err = gop_sync_exec(os_exists(os, creds, foo_path));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Oops! Lost %s\n", foo_path);
      return;
   }
   err = gop_sync_exec(os_exists(os, creds, bar_path));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Oops! Lost %s\n", bar_path);
      return;
   }

//abort();

   // ** Remove bar
   err = gop_sync_exec(os_remove_object(os, creds, bar_path));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: removing file: %s err=%d\n", foo_path, err);
      return;
   }

   // ** Verify the state
   err = gop_sync_exec(os_exists(os, creds, hfoo_path));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Oops! Lost %s\n", hfoo_path);
      return;
   }
   err = gop_sync_exec(os_exists(os, creds, foo_path));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Oops! Lost %s\n", foo_path);
      return;
   }
   err = gop_sync_exec(os_exists(os, creds, bar_path));
   if (err != OP_STATE_FAILURE) {
      nfailed++;
      log_printf(0, "ERROR: Oops! Still have %s\n", bar_path);
      return;
   }

//abort();

   // ** Remove foo
   err = gop_sync_exec(os_remove_object(os, creds, foo_path));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: removing file: %s err=%d\n", foo_path, err);
      return;
   }

   // ** And verify the state
   err = gop_sync_exec(os_exists(os, creds, hfoo_path));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Oops! Lost %s\n", hfoo_path);
      return;
   }
   err = gop_sync_exec(os_exists(os, creds, foo_path));
   if (err != OP_STATE_FAILURE) {
      nfailed++;
      log_printf(0, "ERROR: Oops! Still have %s\n", foo_path);
      return;
   }

//abort();

   // ** Remove hfoo
   err = gop_sync_exec(os_remove_object(os, creds, hfoo_path));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: removing file: %s err=%d\n", hfoo_path, err);
      return;
   }

   // ** MAke sure it's gone
   err = gop_sync_exec(os_exists(os, creds, hfoo_path));
   if (err != OP_STATE_FAILURE) {
      nfailed++;
      log_printf(0, "ERROR: Oops! Still have %s\n", hfoo_path);
      return;
   }

//abort();


   // ** Create DIRECTORY foodir
   snprintf(foo_path, PATH_LEN, "%s/foodir", prefix);
   err = gop_sync_exec(os_create_object(os, creds, foo_path, OS_OBJECT_DIR, "me"));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: creating dir: %s err=%d\n", foo_path, err);
      return;
   }

   // ** Verify foodir exists
   err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: opening dir: %s err=%d\n", foo_path, err);
      return;
   }
   err = gop_sync_exec(os_close_object(os, foo_fd));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Closing dir: %s err=%d\n", foo_path, err);
      return;
   }

   // ** Create foodir/foo
   snprintf(foo_path, PATH_LEN, "%s/foodir/foo", prefix);
   err = gop_sync_exec(os_create_object(os, creds, foo_path, OS_OBJECT_FILE, "me"));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: creating file: %s err=%d\n", foo_path, err);
      return;
   }

   // ** Verify foodir/foo exists
   err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: opening file: %s err=%d\n", foo_path, err);
      return;
   }
   err = gop_sync_exec(os_close_object(os, foo_fd));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
      return;
   }

   // ** Create DIRECTORY foodir/bardir
   snprintf(foo_path, PATH_LEN, "%s/foodir/bardir", prefix);
   err = gop_sync_exec(os_create_object(os, creds, foo_path, OS_OBJECT_DIR, "me"));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: creating dir: %s err=%d\n", foo_path, err);
      return;
   }

   // ** Verify foodir/bardir exists
   err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: opening dir: %s err=%d\n", foo_path, err);
      return;
   }
   err = gop_sync_exec(os_close_object(os, foo_fd));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Closing dir: %s err=%d\n", foo_path, err);
      return;
   }

   // ** Create foodir/bardir/foobar
   snprintf(foo_path, PATH_LEN, "%s/foodir/bardir/foobar", prefix);
   err = gop_sync_exec(os_create_object(os, creds, foo_path, OS_OBJECT_FILE, "me"));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: creating file: %s err=%d\n", foo_path, err);
      return;
   }

   // ** Verify foodir/bardir/foobar exists
   err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: opening file: %s err=%d\n", foo_path, err);
      return;
   }
   err = gop_sync_exec(os_close_object(os, foo_fd));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
      return;
   }

   // ** Create foodir/bardir/bar
   snprintf(foo_path, PATH_LEN, "%s/foodir/bardir/bar", prefix);
   err = gop_sync_exec(os_create_object(os, creds, foo_path, OS_OBJECT_FILE, "me"));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: creating file: %s err=%d\n", foo_path, err);
      return;
   }

   // ** Verify foodir/bardir/bar exists
   err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: opening file: %s err=%d\n", foo_path, err);
      return;
   }
   err = gop_sync_exec(os_close_object(os, foo_fd));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
      return;
   }

   // ** Do a regex scan for foo*/bar*/foo*
   // ** This should return foodir/bardir/foobar
   snprintf(foo_path, PATH_LEN, "%s/foo*/bar*/foo*", prefix);
   snprintf(match_path[0], PATH_LEN, "%s/foodir/bardir/foobar", prefix);
   err = path_scan_and_check(foo_path, match_path, 1, 1000, OS_OBJECT_FILE);
   if (err != 0) {
      nfailed++;
      log_printf(0, "ERROR: Regex scan: foo*/bar*/foo* err=%d\n", err);
      return;
   }

   // ** Do a regex scan for foo* with recursion
   snprintf(foo_path, PATH_LEN, "%s/foo*", prefix);
   snprintf(match_path[0], PATH_LEN, "%s/foodir/foo", prefix);
   snprintf(match_path[1], PATH_LEN, "%s/foodir/bardir/foobar", prefix);
   snprintf(match_path[2], PATH_LEN, "%s/foodir/bardir/bar", prefix);
   err = path_scan_and_check(foo_path, match_path, 3, 1000, OS_OBJECT_FILE);
   if (err != 0) {
      nfailed++;
      log_printf(0, "ERROR: Regex scan: foo* err=%d\n", err);
      return;
   }

   // ** Do a regex scan for foodir/bardir/foo* with recursion
   snprintf(foo_path, PATH_LEN, "%s/foodir/bardir/foo*", prefix);
   snprintf(match_path[0], PATH_LEN, "%s/foodir/bardir/foobar", prefix);
   err = path_scan_and_check(foo_path, match_path, 1, 1000, OS_OBJECT_FILE);
   if (err != 0) {
      nfailed++;
      log_printf(0, "ERROR: Regex scan: foo* err=%d\n", err);
      return;
   }

   // ** Do a regex scan for foo*/bardir/foobar with recursion
   snprintf(foo_path, PATH_LEN, "%s/foo*/bardir/foo*", prefix);
   snprintf(match_path[0], PATH_LEN, "%s/foodir/bardir/foobar", prefix);
   err = path_scan_and_check(foo_path, match_path, 1, 1000, OS_OBJECT_FILE);
   if (err != 0) {
      nfailed++;
      log_printf(0, "ERROR: Regex scan: foo* err=%d\n", err);
      return;
   }

   // ** Do a regex scan for foo*/bardir/foo* with recursion
   snprintf(foo_path, PATH_LEN, "%s/foo*/bardir/foo*", prefix);
   snprintf(match_path[0], PATH_LEN, "%s/foodir/bardir/foobar", prefix);
   err = path_scan_and_check(foo_path, match_path, 1, 1000, OS_OBJECT_FILE);
   if (err != 0) {
      nfailed++;
      log_printf(0, "ERROR: Regex scan: foo* err=%d\n", err);
      return;
   }

   // ** Do a regex scan for foodir/bar*/foobar with recursion
   snprintf(foo_path, PATH_LEN, "%s/foo*/bardir/foo*", prefix);
   snprintf(match_path[0], PATH_LEN, "%s/foodir/bardir/foobar", prefix);
   err = path_scan_and_check(foo_path, match_path, 1, 1000, OS_OBJECT_FILE);
   if (err != 0) {
      nfailed++;
      log_printf(0, "ERROR: Regex scan: foo* err=%d\n", err);
      return;
   }


   // ** Create DIRECTORY foodir/bardir/subdir
   snprintf(foo_path, PATH_LEN, "%s/foodir/bardir/subdir", prefix);
   err = gop_sync_exec(os_create_object(os, creds, foo_path, OS_OBJECT_DIR, "me"));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: creating dir: %s err=%d\n", foo_path, err);
      return;
   }

   // ** Verify foodir/bardir/subdir exists
   err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: opening dir: %s err=%d\n", foo_path, err);
      return;
   }
   err = gop_sync_exec(os_close_object(os, foo_fd));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Closing dir: %s err=%d\n", foo_path, err);
      return;
   }


   // ** Create foodir/bardir/subdir/last
   snprintf(foo_path, PATH_LEN, "%s/foodir/bardir/subdir/last", prefix);
   err = gop_sync_exec(os_create_object(os, creds, foo_path, OS_OBJECT_FILE, "me"));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: creating file: %s err=%d\n", foo_path, err);
      return;
   }

   // ** Verify foodir/bardir/subdir/last exists
   err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: opening file: %s err=%d\n", foo_path, err);
      return;
   }
   err = gop_sync_exec(os_close_object(os, foo_fd));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
      return;
   }

//abort();

   // ** Create DIRECTORY LINK foodir/linkdir -> fooddir/bardir/subdir
   snprintf(foo_path, PATH_LEN, "%s/foodir/linkdir", prefix);
   snprintf(bar_path, PATH_LEN, "%s/foodir/bardir/subdir", prefix);
   err = gop_sync_exec(os_symlink_object(os, creds, bar_path, foo_path, "me"));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: creating link dir: %s err=%d\n", foo_path, err);
      return;
   }

   // ** Verify foodir/linkdir exists
   err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: opening dir: %s err=%d\n", foo_path, err);
      return;
   }
   err = gop_sync_exec(os_close_object(os, foo_fd));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Closing dir: %s err=%d\n", foo_path, err);
      return;
   }

   // ** Do a regex scan for foo* with recursion
   snprintf(foo_path, PATH_LEN, "%s/foo*", prefix);
   snprintf(match_path[0], PATH_LEN, "%s/foodir/foo", prefix);
   snprintf(match_path[1], PATH_LEN, "%s/foodir/bardir/foobar", prefix);
   snprintf(match_path[2], PATH_LEN, "%s/foodir/bardir/bar", prefix);
   snprintf(match_path[3], PATH_LEN, "%s/foodir/bardir/subdir/last", prefix);
   snprintf(match_path[4], PATH_LEN, "%s/foodir/linkdir/last", prefix);
   err = path_scan_and_check(foo_path, match_path, 5, 1000, OS_OBJECT_FILE);
   if (err != 0) {
      nfailed++;
      log_printf(0, "ERROR: Regex scan: foo* err=%d\n", err);
      return;
   }

   // ** Do a regex scan for foodir/linkdir with recursion
   snprintf(foo_path, PATH_LEN, "%s/foodir/linkdir", prefix);
   err = path_scan_and_check(foo_path, &(match_path[4]), 1, 1000, OS_OBJECT_FILE);
   if (err != 0) {
      nfailed++;
      log_printf(0, "ERROR: Regex scan: foo* err=%d\n", err);
      return;
   }

//abort();

   // ** Remove foodir/linkdir
   snprintf(foo_path, PATH_LEN, "%s/foodir/linkdir", prefix);
   err = gop_sync_exec(os_remove_object(os, creds, foo_path));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: filed removing link directory: %s err=%d\n", foo_path, err);
      return;
   }

   // ** Verify foodir/linkdir was removed
   err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
   if (err == OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Oops! opened a removed dir dir: %s err=%d\n", foo_path, err);
      return;
   }

   // ** Try removing foodir/bardir/subdir.  This should fail since it has files in it
   snprintf(foo_path, PATH_LEN, "%s/foodir/bardir/subdir", prefix);
   err = gop_sync_exec(os_remove_object(os, creds, foo_path));
   if (err != OP_STATE_FAILURE) {
      nfailed++;
      log_printf(0, "ERROR: Oops!  I could rmdir a non-empty directory: %s err=%d\n", foo_path, err);
      return;
   }

   // ** Do a rm -r foodir/bardir/subdir
   snprintf(foo_path, PATH_LEN, "%s/foodir/bardir/subdir", prefix);
   regex = os_path_glob2regex(foo_path);
   err = gop_sync_exec(os_remove_regex_object(os, creds, regex, NULL, OS_OBJECT_ANY, 1000));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: rm -r %s failed with err=%d\n", foo_path, err);
      return;
   }
   os_regex_table_destroy(regex);

   // ** Verify foodir/bardir/subdir is gone
   err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
   if (err == OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Oops!  I could access the removed dir: %s err=%d\n", foo_path, err);
      return;
   }

   // ** Do an rm -r foo*/bar*/foo*
   snprintf(foo_path, PATH_LEN, "%s/foo*/bar*/foo*", prefix);
   regex = os_path_glob2regex(foo_path);
   err = gop_sync_exec(os_remove_regex_object(os, creds, regex, NULL, OS_OBJECT_ANY, 1000));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: rm -r %s failed with err=%d\n", foo_path, err);
      return;
   }
   os_regex_table_destroy(regex);

   // ** Verify that foodir/foo, foodir/bardir/bar still exist
   snprintf(foo_path, PATH_LEN, "%s/foo*", prefix);
   snprintf(match_path[0], PATH_LEN, "%s/foodir/foo", prefix);
   snprintf(match_path[1], PATH_LEN, "%s/foodir/bardir/bar", prefix);
   err = path_scan_and_check(foo_path, match_path, 2, 1000, OS_OBJECT_FILE);
   if (err != 0) {
      nfailed++;
      log_printf(0, "ERROR: Regex scan: foo*/bar*/foo* err=%d\n", err);
      return;
   }


   // ** Rename it foodir->bardir
   snprintf(foo_path, PATH_LEN, "%s/foodir", prefix);
   snprintf(bar_path, PATH_LEN, "%s/bardir", prefix);
   err = gop_sync_exec(os_move_object(os, creds, foo_path, bar_path));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: moving file src: %s  dest: %s err=%d\n", foo_path, bar_path, err);
      return;
   }


   // ** Verify foodir is gone
   err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
   if (err == OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Oops!  file existst after rename: %s err=%d\n", foo_path, err);
      return;
   }

   // ** Verift it's there under the new name bar
   // ** Verify that foodir/foo, foodir/bardir/bar still exist
   snprintf(foo_path, PATH_LEN, "%s/bardir/*", prefix);
   snprintf(match_path[0], PATH_LEN, "%s/bardir/foo", prefix);
   snprintf(match_path[1], PATH_LEN, "%s/bardir/bardir/bar", prefix);
   err = path_scan_and_check(foo_path, match_path, 2, 1000, OS_OBJECT_FILE);
   if (err != 0) {
      nfailed++;
      log_printf(0, "ERROR: Regex scan: bardir err=%d\n", err);
      return;
   }
   err = gop_sync_exec(os_open_object(os, creds, bar_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: opening file: %s err=%d\n", bar_path, err);
      return;
   }
   err = gop_sync_exec(os_close_object(os, foo_fd));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Closing file: %s err=%d\n", bar_path, err);
      return;
   }


   // ** Remove everything else rm -r bardir
   snprintf(foo_path, PATH_LEN, "%s/bardir", prefix);
   regex = os_path_glob2regex(foo_path);
   err = gop_sync_exec(os_remove_regex_object(os, creds, regex, NULL, OS_OBJECT_ANY, 1000));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: rm -r %s failed with err=%d\n", foo_path, err);
      return;
   }
   err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
   if (err == OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Oops!  I could access the removed dir: %s err=%d\n", foo_path, err);
      return;
   }
   os_regex_table_destroy(regex);

   for (i=0; i<10; i++) {
       free(match_path[i]);
   }

   log_printf(0, "PASSED!\n");

   return;
}

// **********************************************************************************
//  attr_check - Checks that the regex key/val mataches
// **********************************************************************************

int attr_check(os_attr_iter_t *it, char **key, char **val, int *v_size, int n)
{
  object_service_fn_t *os = lio_gc->os;
  char *ikey, *ival;
  int hits[n];
  int i, i_size, err;

  for (i=0; i<n; i++) hits[i] = 0;

  err = 0;
  while (os_next_attr(os, it, &ikey, (void **)&ival, &i_size) == 0) {
    for (i=0; i<n; i++) {
       if (strcmp(key[i], ikey) == 0) {
          hits[i] = 1;
          if (strcmp(val[i], ival) != 0) {
             err += 1;
             hits[i] = -1;
             log_printf(0, "ERROR bad value key=%s val=%s should be mval=%s\n", ikey, ival, val[i]);
          }
          break;
       }
    }

    free(ikey); free(ival);
    ikey = ival = NULL;
  }

  for (i=0; i<n; i++) {
     if (hits[i] == 0) {
        err += 1;
        log_printf(0, "ERROR missed key=%s\n", key[i]);
     }
  }

  return(err);
}


// **********************************************************************************
//  os_attribute_tests - Tests attribute manipulation
// **********************************************************************************

void os_attribute_tests()
{
   object_service_fn_t *os = lio_gc->os;
   creds_t  *creds = lio_gc->creds;
   char foo_path[PATH_LEN];
   char bar_path[PATH_LEN];
   char root_path[PATH_LEN];
   char fpath[PATH_LEN];
   os_regex_table_t *regex, *attr_regex, *obj_regex;
   os_fd_t *foo_fd, *bar_fd, *root_fd;
   os_attr_iter_t *it;
   os_object_iter_t *oit;
   char *key, *val, *rval, *fname;
   char **mkey, **mval, **mrval;
   char *match[10];
   char *path[10];
   int i, err, v_size, n, nstart, j, plen;
   int m_size[10];
   int64_t dt, now, got;

   type_malloc(mkey, char *, 10);
   type_malloc(mval, char *, 10);
   type_malloc(mrval, char *, 10);
   for (i=0; i<10; i++) { type_malloc_clear(match[i], char, PATH_LEN); }

   // ** Create FILE foo
   snprintf(foo_path, PATH_LEN, "%s/foo", prefix);
   err = gop_sync_exec(os_create_object(os, creds, foo_path, OS_OBJECT_FILE, "me"));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: creating file: %s err=%d\n", foo_path, err);
      return;
   }
   now = time(0);

   // ** Verify foo exists
   err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: opening file: %s err=%d\n", foo_path, err);
      return;
   }


   //** get_attr(os.create) for foo
   v_size = -1000;
   key = "os.create";  rval=NULL;
   err = gop_sync_exec(os_get_attr(os, creds, foo_fd, key, (void **)&rval, &v_size));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: getting attr=%s err=%d\n", key, err);
      return;
   }
   sscanf((char *)rval, I64T, &got);
   dt = got - now;
   if (labs(dt) > 5) {
      nfailed++;
      log_printf(0, "ERROR: val mismatch attr=%s should be=" I64T " got=%s\n", key, val, rval);
      return;
   }
   free(rval);

   // ** Create FILE LINK bar->foo
   snprintf(bar_path, PATH_LEN, "%s/bar", prefix);
   err = gop_sync_exec(os_symlink_object(os, creds, foo_path, bar_path, "me"));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: creating file: %s err=%d\n", foo_path, err);
      return;
   }

   // ** Verify bar exists
   err = gop_sync_exec(os_open_object(os, creds, bar_path, OS_MODE_READ_IMMEDIATE, "me", &bar_fd, wait_time));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: opening file: %s err=%d\n", bar_path, err);
      return;
   }

   //** get_attr(type) for foo
   v_size = -1000;
   key = "os.type";  rval=NULL;
   val = "1";
   err = gop_sync_exec(os_get_attr(os, creds, foo_fd, key, (void **)&rval, &v_size));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: getting attr=%s err=%d\n", key, err);
      return;
   }
   if (strcmp(val, rval) != 0) {
      nfailed++;
      log_printf(0, "ERROR: val mismatch attr=%s should be=%s got=%s\n", key, val, rval);
      return;
   }
   free(rval);

   //** get_attr(type) bar
   v_size = -1000;
   val = "5";
   err = gop_sync_exec(os_get_attr(os, creds, bar_fd, key, (void **)&rval, &v_size));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: getting attr=%s err=%d\n", key, err);
      return;
   }
   if (strcmp(val, rval) != 0) {
      nfailed++;
      log_printf(0, "ERROR: val mismatch attr=%s should be=%d got=%s\n", key, val, rval);
      return;
   }
   free(rval);

   //** get_attr(link) for foo
   v_size = -1000;
   key = "os.link";  rval=NULL;
   err = gop_sync_exec(os_get_attr(os, creds, foo_fd, key, (void **)&rval, &v_size));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: getting attr=%s err=%d\n", key, err);
      return;
   }
   if (v_size != 0) {
      nfailed++;
      log_printf(0, "ERROR: val mismatch attr=%s should be=NULL got=%s\n", key, rval);
      return;
   }
   free(rval);

   //** get_attr(link) bar
   v_size = -1000;
   val = foo_path;
   err = gop_sync_exec(os_get_attr(os, creds, bar_fd, key, (void **)&rval, &v_size));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: getting attr=%s err=%d\n", key, err);
      return;
   }
   if (strcmp(val, rval) != 0) {
      nfailed++;
      log_printf(0, "ERROR: val mismatch attr=%s should be=%s got=%s\n", key, val, rval);
      return;
   }
   free(rval);

   //** Check out the timestamps
   //** make /bar{user.timestamp}
   v_size = -1000;
   key = "os.timestamp.user.timestamp";  val="my_ts";
   err = gop_sync_exec(os_set_attr(os, creds, bar_fd, key, val, strlen(val)));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: setting attr=%s err=%d\n", key, err);
      return;
   }

   //** Read it back through the VA
   v_size = -1000;
   rval=NULL;
   err = gop_sync_exec(os_get_attr(os, creds, bar_fd, key, (void **)&rval, &v_size));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: getting attr=%s err=%d\n", key, err);
      return;
   }
   if (v_size > 0) {
      if (strstr(rval, val) == NULL) {
         nfailed++;
         log_printf(0, "ERROR: Cant find my tag in key=%d timestamp=%s tag=%s err=%d\n", key, rval, val, err);
      }
      free(rval);
   } else {
      nfailed++;
      log_printf(0, "ERROR: val missing\n");
      return;
   }

   //** And directly
   v_size = -1000;
   rval=NULL;
   key = "user.timestamp";
   err = gop_sync_exec(os_get_attr(os, creds, bar_fd, key, (void **)&rval, &v_size));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: getting attr=%s err=%d\n", key, err);
      return;
   }
   if (v_size > 0) {
      if (strstr(rval, val) == NULL) {
         nfailed++;
         log_printf(0, "ERROR: Cant find my tag in key=%d timestamp=%s tag=%s err=%d\n", key, rval, val, err);
      }
      free(rval);
   } else {
      nfailed++;
      log_printf(0, "ERROR: val missing\n");
      return;
   }

   //** Just get the time
   v_size = -1000;
   rval=NULL;
   key = "os.timestamp";
   err = gop_sync_exec(os_get_attr(os, creds, bar_fd, key, (void **)&rval, &v_size));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: getting attr=%s err=%d\n", key, err);
      return;
   }
   if (v_size <= 0) {
      nfailed++;
      log_printf(0, "ERROR: val missing\n");
      return;
   }
   free(rval);

   //** Make an attribute for root/prefix "/"
   snprintf(root_path, PATH_LEN, "%s", prefix);
   err = gop_sync_exec(os_open_object(os, creds, root_path, OS_MODE_READ_IMMEDIATE, "me", &root_fd, wait_time));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: opening file: %s err=%d\n", root_path, err);
      return;
   }

   // ** set_attr(foo1=foo1) root
   v_size = -1000;
   key = "user.foo1";  val="foo1";
   err = gop_sync_exec(os_set_attr(os, creds, root_fd, key, val, strlen(val)));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: setting attr=%s err=%d\n", key, err);
      return;
   }

   // ** get_attr(user.foo1) for root
   v_size = -1000;
   key = "user.foo1";  rval=NULL;
   err = gop_sync_exec(os_get_attr(os, creds, root_fd, key, (void **)&rval, &v_size));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: getting attr=%s err=%d\n", key, err);
      return;
   }
   if (strcmp(val, rval) != 0) {
      nfailed++;
      log_printf(0, "ERROR: val mismatch attr=%s should be=foo1 got=$s\n", key, val);
      return;
   }
   free(rval);

   //** Now softlink /bar{user.link_root} -> /{foo1}
   //** Now do a link between attributes
   mkey[0] = "user.foo1"; mval[0] = "user.link_root";
   err = gop_sync_exec(os_symlink_attr(os, creds, root_path, mkey[0], bar_fd, mval[0]));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: with attr link: err=%d\n", err);
      return;
   }

   //** Verify it worked
   key = "user.link_root";
   v_size = -1000;
   err = gop_sync_exec(os_get_attr(os, creds, bar_fd, key, (void **)&rval, &v_size));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: getting attrs err=%d\n", err);
      return;
   }
   if (strcmp(val, rval) != 0) {
      nfailed++;
      log_printf(0, "ERROR: val mismatch attr=%s should be=%s got=%s\n", mkey[i], mval[i], mrval[i]);
      return;
   }

   free(rval);

   //** Now remove the foo1 attribute for root
   v_size = -1;
   key = "user.foo1";  val=NULL;
   err = gop_sync_exec(os_set_attr(os, creds, root_fd, key, val, v_size));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: setting attr=%s err=%d\n", key, err);
      return;
   }

   //** Close root
   err = gop_sync_exec(os_close_object(os, root_fd));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Closing file: %s err=%d\n", root_path, err);
      return;
   }


   //** Close bar
   err = gop_sync_exec(os_close_object(os, bar_fd));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
      return;
   }

   //** And remove it
   err = gop_sync_exec(os_remove_object(os, creds, bar_path));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: removing file: %s err=%d\n", bar_path, err);
      return;
   }


   // ** set_attr(foo1=foo1);
   v_size = -1000;
   key = "user.foo1";  val="foo1";
log_printf(15, "PTR1 before val=%s sizeof(void)=%d sizeof(void *)=%d sizeof(void**)=%d\n", val, sizeof(void), sizeof(void *), sizeof(void **));
log_printf(15, "PTR1 before val=%s sizeof(char)=%d sizeof(char *)=%d sizeof(char**)=%d\n", val, sizeof(char), sizeof(char *), sizeof(char **));
   err = gop_sync_exec(os_set_attr(os, creds, foo_fd, key, val, strlen(val)));
log_printf(15, "PTR1 after val=%s\n", val);
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: setting attr=%s err=%d\n", key, err);
      return;
   }

   // ** get_attr(foo1);
   v_size = -1000;
   key = "user.foo1";  rval=NULL;
log_printf(15, "PTR1 after2 val=%s\n", val);
log_printf(15, "PTR rval=%p *rval=%s\n", &rval, rval); flush_log();
   err = gop_sync_exec(os_get_attr(os, creds, foo_fd, key, (void **)&rval, &v_size));
log_printf(15, "PTR rval=%p *rval=%s v_size=%d\n", &rval, rval, v_size); flush_log();
log_printf(15, "PTR1 after3 val=%s\n", val); flush_log();
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: getting attr=%s err=%d\n", key, err);
      return;
   }
   if (strcmp(val, rval) != 0) {
      nfailed++;
      log_printf(0, "ERROR: val mismatch attr=%s should be=foo1 got=$s\n", key, val);
      return;
   }
   free(rval);

   // ** set_attr(foo2=foo2);
   key = "user.foo2";  val="foo2";
   err = gop_sync_exec(os_set_attr(os, creds, foo_fd, key, val, strlen(val)));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: setting attr=%s err=%d\n", key, err);
      return;
   }

   // ** get_attr(foo2);
   v_size = -1000;
   rval = NULL;
log_printf(15, "CHECK val=%s\n", val);
   err = gop_sync_exec(os_get_attr(os, creds, foo_fd, key, (void **)&rval, &v_size));
log_printf(15, "CHECK2 val=%s\n", val);
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: getting attr=%s err=%d\n", key, err);
      return;
   }
   if (strcmp(val, rval) != 0) {
      nfailed++;
      log_printf(0, "ERROR: val mismatch attr=%s should be=%s got=$s\n", key, val, rval);
      return;
   }
   free(rval);

   // ** set_mult_attr(foo1=FOO1,foo3=foo3, bar1=bar1);
   mkey[0] = "user.foo1";  mval[0]="FOO1";  m_size[0] = strlen(mval[0]);
   mkey[1] = "user.foo3";  mval[1]="foo3";  m_size[1] = strlen(mval[1]);
   mkey[2] = "user.bar1";  mval[2]="bar1";  m_size[2] = strlen(mval[2]);
   err = gop_sync_exec(os_set_multiple_attrs(os, creds, foo_fd, mkey, (void **)mval, m_size, 3));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: setting multple err=%d\n", err);
      return;
   }


   // ** get_mult_attr(foo1,foo3,bar1);
   m_size[0] = m_size[1] = m_size[2] = -1000;
   mrval[0] = mrval[1] = mrval[2] = NULL;
   err = gop_sync_exec(os_get_multiple_attrs(os, creds, foo_fd, mkey, (void **)mrval, m_size, 3));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: getting mult attrs err=%d\n", err);
      return;
   }
   for (i=0; i<3; i++) {
      if (strcmp(mval[i], mrval[i]) != 0) {
         nfailed++;
         log_printf(0, "ERROR: val mismatch attr=%s should be=%s got=$s\n", mkey[i], mval[i], mrval[i]);
         return;
      }

      free(mrval[i]);
   }

   // ** get_attr(*);
   mkey[0] = "user.foo1";  mval[0]="FOO1";  m_size[0] = strlen(mval[0]);
   mkey[1] = "user.foo2";  mval[1]="foo2";  m_size[1] = strlen(mval[1]);
   mkey[2] = "user.foo3";  mval[2]="foo3";  m_size[2] = strlen(mval[2]);
   mkey[3] = "user.bar1";  mval[3]="bar1";  m_size[3] = strlen(mval[3]);
   regex = os_path_glob2regex("user.*");
   it = os_create_attr_iter(os, creds, foo_fd, regex, -1000);
   err = attr_check(it, mkey, mval, m_size, 4);
   os_regex_table_destroy(regex);
   os_destroy_attr_iter(os, it);
   if (err != 0) {
      nfailed++;
      log_printf(0, "ERROR: Scanning attr regex!\n");
      return;
   }

   // ** get_attr(foo*);
   regex = os_path_glob2regex("user.foo*");
   it = os_create_attr_iter(os, creds, foo_fd, regex, -1000);
   err = attr_check(it, mkey, mval, m_size, 3);
   os_regex_table_destroy(regex);
   os_destroy_attr_iter(os, it);
   if (err != 0) {
      nfailed++;
      log_printf(0, "ERROR: Scanning attr regex!\n");
      return;
   }

   // ** set_mult_attr(foo1="",foo2="");
   mkey[0] = "user.foo1";  mval[0]=NULL;  m_size[0] = -1;
   mkey[1] = "user.foo2";  mval[1]=NULL;  m_size[1] = -1;
   err = gop_sync_exec(os_set_multiple_attrs(os, creds, foo_fd, mkey, (void **)mval, m_size, 2));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: setting multple err=%d\n", err);
      return;
   }


   // ** get_attr(foo*).. should just get foo3=foo3 back
   mkey[0] = "user.foo3";  mval[0]="foo3";  m_size[0] = strlen(mval[0]);
   regex = os_path_glob2regex("user.*");
   it = os_create_attr_iter(os, creds, foo_fd, regex, -1000);
   err = attr_check(it, mkey, mval, m_size, 1);
   os_regex_table_destroy(regex);
   os_destroy_attr_iter(os, it);
   if (err != 0) {
      nfailed++;
      log_printf(0, "ERROR: Scanning attr regex!\n");
      return;
   }

   // ** set_attr(foo2=foo2);  (We're getting ready to do a multi attr rename)
   key = "user.foo2";  val="foo2";
   err = gop_sync_exec(os_set_attr(os, creds, foo_fd, key, val, strlen(val)));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: setting attr=%s err=%d\n", key, err);
      return;
   }

   // ** get_attr(foo2);
   v_size = -1000;
   rval = NULL;
   err = gop_sync_exec(os_get_attr(os, creds, foo_fd, key, (void **)&rval, &v_size));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: getting attr=%s err=%d\n", key, err);
      return;
   }
   if (strcmp(val, rval) != 0) {
      nfailed++;
      log_printf(0, "ERROR: val mismatch attr=%s should be=%s got=$s\n", key, val, rval);
      return;
   }
   free(rval);

   // ** Now rename foo2->bar2 and foo3->bar3
   mkey[0] = "user.foo2";    mval[0] = "user.bar2";
   mkey[1] = "user.foo3";    mval[1] = "user.bar3";
   err = gop_sync_exec(os_move_multiple_attrs(os, creds, foo_fd, mkey, mval, 2));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: getting renaming mult err=%d\n", err);
      return;
   }


   // ** Verify it worked
   mkey[0] = "user.bar2";  mval[0]="foo2";  m_size[0] = strlen(mval[0]);
   mkey[1] = "user.bar3";  mval[1]="foo3";  m_size[1] = strlen(mval[1]);
   regex = os_path_glob2regex("user.*");
   it = os_create_attr_iter(os, creds, foo_fd, regex, -1000);
   err = attr_check(it, mkey, mval, m_size, 2);
   os_regex_table_destroy(regex);
   os_destroy_attr_iter(os, it);
   if (err != 0) {
      nfailed++;
      log_printf(0, "ERROR: Scanning attr regex!\n");
      return;
   }


   // ** Close foo
   err = gop_sync_exec(os_close_object(os, foo_fd));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
      return;
   }


   // ** Now we need to set up a recursive mult attribute write
   // ** Right now we have foo.  But we need to also do a subdir to verify recursion.
   // ** So we need to add foodir/foo as well.
   // ** Create DIRECTORY foodir
   snprintf(foo_path, PATH_LEN, "%s/foodir", prefix);
   err = gop_sync_exec(os_create_object(os, creds, foo_path, OS_OBJECT_DIR, "me"));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: creating dir: %s err=%d\n", foo_path, err);
      return;
   }

   // ** Verify foodir exists
   err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: opening dir: %s err=%d\n", foo_path, err);
      return;
   }
   err = gop_sync_exec(os_close_object(os, foo_fd));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Closing dir: %s err=%d\n", foo_path, err);
      return;
   }

   // ** Create foodir/foo
   snprintf(foo_path, PATH_LEN, "%s/foodir/foo", prefix);
   err = gop_sync_exec(os_create_object(os, creds, foo_path, OS_OBJECT_FILE, "me"));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: creating file: %s err=%d\n", foo_path, err);
      return;
   }

   // ** Verify foodir/foo exists
   err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: opening file: %s err=%d\n", foo_path, err);
      return;
   }
   err = gop_sync_exec(os_close_object(os, foo_fd));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
      return;
   }

   // ** Create foodir/bar
   snprintf(foo_path, PATH_LEN, "%s/foodir/bar", prefix);
   err = gop_sync_exec(os_create_object(os, creds, foo_path, OS_OBJECT_FILE, "me"));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: creating file: %s err=%d\n", foo_path, err);
      return;
   }

   // ** Verify foodir/bar exists
   err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: opening file: %s err=%d\n", foo_path, err);
      return;
   }
   err = gop_sync_exec(os_close_object(os, foo_fd));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
      return;
   }

   // ** Now do the recursive attr set
   log_printf(0, "Starting recursive attribute set\n");
   mkey[0] = "user.foo1"; mval[0] = "FOO1"; m_size[0] = 4;
   mkey[1] = "user.bar2"; mval[1] = "BAR2"; m_size[1] = 4;
   snprintf(foo_path, PATH_LEN, "%s/foo*", prefix);
   obj_regex = os_path_glob2regex("foo*");
   regex = os_path_glob2regex(foo_path);
   err = gop_sync_exec(os_regex_object_set_multiple_attrs(os, creds, "me", regex, obj_regex, OS_OBJECT_FILE, 1000, mkey, (void **)mval, m_size, 2));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR:with recurse attr set: %s err=%d\n", foo_path, err);
      return;
   }
   os_regex_table_destroy(regex);
   log_printf(0, "Finished recursive attribute set\n");


   //** Verify it took
   snprintf(foo_path, PATH_LEN, "%s/*", prefix);
   regex = os_path_glob2regex(foo_path);
   attr_regex = os_regex2table("^user\.((foo1)|(bar2))$");
   oit = os_create_object_iter(lio_gc->os, lio_gc->creds, regex, NULL, OS_OBJECT_FILE, attr_regex, 1000, &it, -1000);
   if (oit == NULL) {
      nfailed++;
      log_printf(0, "ERROR: Failed with object_iter creation %s\n", foo_path);
      return;
   }

   n = 0;
   while (os_next_object(lio_gc->os, oit, &fname, &plen) > 0) {
      //** Check which file it is foo or foodir/foo or foodir/bar(which should have been skipped)
      nstart = n;
      j = 0;
      snprintf(fpath, PATH_LEN, "%s/foo", prefix);
      if (strcmp(fpath, fname) == 0) n = n | 1;
      snprintf(fpath, PATH_LEN, "%s/foodir/foo", prefix);
      if (strcmp(fpath, fname) == 0) n = n | 2;
      snprintf(fpath, PATH_LEN, "%s/foodir/bar", prefix);
      if (strcmp(fpath, fname) == 0) { n = n | 4; j = 1; }

      if (n == nstart) {
         nfailed++;
         log_printf(0, "ERROR:  EXtra file!  fname=%s\n", fname);
         return;
      }

      v_size = -1000;
      m_size[0] = m_size[1] = 0;
      while (os_next_attr(os, it, &key, (void **)&val, &v_size) == 0) {
        if (j == 0) {
           if (strcmp(key, "user.foo1") == 0) {
              m_size[0] = 1;
              if (strcmp(val, mval[0]) != 0) {
                 nfailed++;
                 log_printf(0, "ERROR bad value fname=%s key=%s val=%s should be mval=%s\n", fname, key, val, mval[0]);
                 return;
              }
           } else if (strcmp(key, "user.bar2") == 0) {
              m_size[1] = 1;
              if (strcmp(val, mval[1]) != 0) {
                 nfailed++;
                 log_printf(0, "ERROR bad value fname=%s key=%s val=%s should be mval=%s\n", fname, key, val, mval[1]);
                 return;
              }
           }
        } else {
           if ((strcmp(key, "user.foo1") == 0) || (strcmp(key, "user.bar2") == 0)) {
              nfailed++;
              log_printf(0, "ERROR Oops!  fname=%s has key=%s when it shouldnt\n", fname, key);
              return;
           }
        }

        free(key); free(val);
        v_size = -1000;
      }

      if (((m_size[0] + m_size[1]) != 2) && (j == 0)) {
         nfailed++;
         log_printf(0, "ERROR fname=%s missed an attr hit[0]=%d hit[1]=%d\n", fname, m_size[0], m_size[1]);
         return;
      }

      free(fname);
   }
   os_destroy_object_iter(os, oit);
   os_regex_table_destroy(regex);
   os_regex_table_destroy(obj_regex);
   os_regex_table_destroy(attr_regex);

   if (n != 7) {
      nfailed++;
      log_printf(0, "ERROR missed a file n=%d\n", n);
      return;
   }

   //** Do the same thing but with a fixed attr list
   snprintf(foo_path, PATH_LEN, "%s/*", prefix);
   regex = os_path_glob2regex(foo_path);
   obj_regex = os_path_glob2regex("foo*");
//         os_create_object_iter_alist(os, c, path, obj_regex, otypes, depth, key, val, v_size, n_keys)
   mrval[0] = mrval[1] = NULL;
   m_size[0] = m_size[1] = -1000;
   log_printf(0, "Doing iter_alist check\n");
   oit = os_create_object_iter_alist(lio_gc->os, lio_gc->creds, regex, obj_regex, OS_OBJECT_FILE, 1000, mkey, (void **)mrval, m_size, 2);
   if (oit == NULL) {
      nfailed++;
      log_printf(0, "ERROR: Failed with object_iter creation %s\n", foo_path);
      return;
   }

   //** Do the actual check
   n = 0;
   while (os_next_object(lio_gc->os, oit, &fname, &plen) > 0) {
      //** Check which file it is foo or foodir/foo or foodir/bar(which should have been skipped)
      nstart = n;
      snprintf(fpath, PATH_LEN, "%s/foo", prefix);
      if (strcmp(fpath, fname) == 0) n = n | 1;
      snprintf(fpath, PATH_LEN, "%s/foodir/foo", prefix);
      if (strcmp(fpath, fname) == 0) n = n | 2;

      if (n == nstart) {
         nfailed++;
         log_printf(0, "ERROR:  EXtra file!  fname=%s\n", fname);
         return;
      }

      for (i=0; i<2; i++) {
         if (strcmp(mrval[i], mval[i]) != 0) {
            nfailed++;
            log_printf(0, "ERROR bad value fname=%s key=%s val=%s should be mval=%s\n", fname, key[i], mrval[i], mval[i]);
            return;
         }
         if (mrval[i] != NULL) free(mrval[i]);
      }

      free(fname);
   }
   os_destroy_object_iter(os, oit);
   os_regex_table_destroy(regex);
   os_regex_table_destroy(obj_regex);

   if (n != 3) {
      nfailed++;
      log_printf(0, "ERROR missed a file n=%d\n", n);
      return;
   }


   //** Do a mult attr copy betwen foo and foodir/bar
   snprintf(bar_path, PATH_LEN, "%s/foodir/bar", prefix); //QWERTY
   err = gop_sync_exec(os_open_object(os, creds, bar_path, OS_MODE_READ_IMMEDIATE, "me", &bar_fd, wait_time));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: opening file: %s err=%d\n", bar_path, err);
      return;
   }
   snprintf(foo_path, PATH_LEN, "%s/foo", prefix);
   err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: opening file: %s err=%d\n", foo_path, err);
      return;
   }

   mkey[0] = "user.bar2"; mval[0] = "user.bar2";
   mkey[1] = "user.bar3"; mval[1] = "user.dummy";
log_printf(15, "COPY_START\n"); flush_log();
   err = gop_sync_exec(os_copy_multiple_attrs(os, creds, foo_fd, mkey, bar_fd, mval, 2));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: with mutl attr copy: err=%d\n", err);
      return;
   }
log_printf(0, "COPY_END\n"); flush_log();


   //** Verify it worked
   mkey[0] = "user.bar2"; mkey[1] = "user.dummy";
   mval[0] = "BAR2";      mval[1] = "foo3";
   m_size[0] = m_size[1] = -1000;
   mrval[0] = mrval[1] = NULL;
   err = gop_sync_exec(os_get_multiple_attrs(os, creds, bar_fd, mkey, (void **)mrval, m_size, 2));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: getting mult attrs err=%d\n", err);
      return;
   }
   for (i=0; i<2; i++) {
      if (strcmp(mval[i], mrval[i]) != 0) {
         nfailed++;
         log_printf(0, "ERROR: val mismatch attr=%s should be=%s got=%s\n", mkey[i], mval[i], mrval[i]);
         return;
      }

      free(mrval[i]);
   }

   //** Now do a link between attributes
   mkey[0] = "user.bar2"; mval[0] = "user.link_bar2";
   mkey[1] = "user.bar3"; mval[1] = "user.link_bar3";
   path[0] = foo_path;
   path[1] = foo_path;
log_printf(15, "LINK_START\n"); flush_log();
   err = gop_sync_exec(os_symlink_multiple_attrs(os, creds, path, mkey, bar_fd, mval, 2));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: with multi attr link: err=%d\n", err);
      return;
   }

   //** Verify it worked
   mkey[0] = "user.link_bar2"; mkey[1] = "user.link_bar3";
   mval[0] = "BAR2";           mval[1] = "foo3";
   m_size[0] = m_size[1] = -1000;
   mrval[0] = mrval[1] = NULL;
   err = gop_sync_exec(os_get_multiple_attrs(os, creds, bar_fd, mkey, (void **)mrval, m_size, 2));
//abort();
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: getting mult attrs err=%d\n", err);
      return;
   }
   for (i=0; i<2; i++) {
      if (strcmp(mval[i], mrval[i]) != 0) {
         nfailed++;
         log_printf(0, "ERROR: val mismatch attr=%s should be=%s got=%s\n", mkey[i], mval[i], mrval[i]);
         return;
      }

      free(mrval[i]);
   }

  //** Now get the type of bar user.link_bar2 and user.bar2 os.create
   mkey[0] = "os.attr_type.user.link_bar2";  mval[0] = "5";
   mkey[1] = "os.attr_type.user.bar2";       mval[1] = "1";
   mkey[2] = "os.attr_type.os.create";       mval[2] = "32";
   m_size[0] = m_size[1] = m_size[2] = -1000;
   mrval[0] = mrval[1] = mrval[2] = NULL;
   err = gop_sync_exec(os_get_multiple_attrs(os, creds, bar_fd, mkey, (void **)mrval, m_size, 3));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: getting mult attrs err=%d\n", err);
      return;
   }
   for (i=0; i<3; i++) {
      if (strcmp(mval[i], mrval[i]) != 0) {
         nfailed++;
         log_printf(0, "ERROR: val mismatch attr=%s should be=%s got=%s\n", mkey[i], mval[i], mrval[i]);
         return;
      }

      free(mrval[i]);
   }

  //** Now get the type of bar user.link_bar2 and user.bar2 os.create
   mkey[0] = "os.attr_link.user.link_bar2";  mval[0] = match[0]; sprintf(match[0], "%s/foo/user.bar2", prefix);
   mkey[1] = "os.attr_link.user.bar2";       mval[1] = NULL;
   mkey[2] = "os.attr_link.os.create";       mval[2] = NULL;
   m_size[0] = m_size[1] = m_size[2] = -1000;
   mrval[0] = mrval[1] = mrval[2] = NULL;
   err = gop_sync_exec(os_get_multiple_attrs(os, creds, bar_fd, mkey, (void **)mrval, m_size, 3));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Oops!  Error getting mult attrs!  err=%d\n", err);
      return;
   }
   if (strcmp(mval[0], mrval[0]) != 0) {
      nfailed++;
      log_printf(0, "ERROR: val mismatch attr=%s should be=%s got=%s\n", mkey[0], mval[0], mrval[0]);
      return;
   }
   free(mrval[0]);


   for (i=1; i<2; i++) {
      if (mrval[i] != NULL) {
         nfailed++;
         log_printf(0, "ERROR: val mismatch attr=%s should not have a link got=%s\n", mkey[i], mrval[i]);
         return;
      }
   }
log_printf(15, "LINK_END\n"); flush_log();


   //** Close the 2 files
   err = gop_sync_exec(os_close_object(os, foo_fd));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Closing file foo_fd err=%d\n", err);
      return;
   }
   err = gop_sync_exec(os_close_object(os, bar_fd));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Closing file bar_fd err=%d\n", err);
      return;
   }


   //** rm foo
   snprintf(foo_path, PATH_LEN, "%s/foo", prefix);
   err = gop_sync_exec(os_remove_object(os, creds, foo_path));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: opening file: %s err=%d\n", foo_path, err);
      return;
   }

   // ** And verify it's gone
   err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
   if (err != OP_STATE_FAILURE) {
      nfailed++;
      log_printf(0, "ERROR: Oops! Can open the recently deleted file: %s err=%d\n", foo_path, err);
      return;
   }

   //** Remove everything else rm -r foodir
   snprintf(foo_path, PATH_LEN, "%s/foodir", prefix);
   regex = os_path_glob2regex(foo_path);
   err = gop_sync_exec(os_remove_regex_object(os, creds, regex, NULL, OS_OBJECT_ANY, 1000));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: rm -r %s failed with err=%d\n", foo_path, err);
      return;
   }
   err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
   if (err != OP_STATE_FAILURE) {
      nfailed++;
      log_printf(0, "ERROR: Oops!  I could access the removed dir: %s err=%d\n", foo_path, err);
      return;
   }
   os_regex_table_destroy(regex);

   log_printf(0, "PASSED!\n");

   for (i=0; i<10; i++) free(match[i]);
   free(mval);
   free(mkey);
   free(mrval);
   return;
}

// **********************************************************************************
// check_lock_state - Validates the lock state
// **********************************************************************************

int check_lock_state(os_fd_t *foo_fd, char **active, int n_active, char **pending, int n_pending)
{
   object_service_fn_t *os = lio_gc->os;
   creds_t  *creds = lio_gc->creds;
   int err, v_size, ai, pi, fin;
   char *key, *val, *lval, *tmp, *bstate;
   inip_file_t *ifd;
   inip_group_t *grp;
   inip_element_t *ele;

   v_size = -10000;
   key = "os.lock";  val=NULL;
   err = gop_sync_exec(os_get_attr(os, creds, foo_fd, key, (void **)&lval, &v_size));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: getting attr=%s err=%d\n", key, err);
      return(1);
   }

   err = 0;

   ifd = inip_read_text(lval);
   grp = inip_find_group(ifd, "os.lock");

   ele = inip_first_element(grp);
   ai = pi = 0;
   while (ele != NULL) {
      key = inip_get_element_key(ele);
      if (strcmp(key, "active_id") == 0) {
        if (ai < n_active) {
           val = inip_get_element_value(ele);
           tmp = string_token(val, ":", &bstate, &fin);
           if (strcmp(tmp, active[ai]) != 0) {
              err++;
              log_printf(0, "Active slot mismatch!  active_id[%d]=%s should be %s\n", ai, val, active[ai]);
           }
        } else {
          err++;
          log_printf(0, "To many Active slots!  active_id[%d]=%s should n_active=%d\n", ai, val, n_active);
        }
        ai++;
      } else if (strcmp(key, "pending_id") == 0) {
        if (pi < n_pending) {
           val = inip_get_element_value(ele);
           tmp = string_token(val, ":", &bstate, &fin);
           if (strcmp(tmp, pending[pi]) != 0) {
              err++;
              log_printf(0, "Pending slot mismatch!  pending_id[%d]=%s should be %s\n", pi, val, pending[pi]);
           }
        } else {
          err++;
          log_printf(0, "To many Pending slots!  pending_id[%d]=%s should n_pending=%d\n", pi, val, n_pending);
        }
        pi++;
      }

     //** Move to the next segmnet to load
     ele = inip_next_element(ele);
   }

   if ((ai != n_active) || ( pi != n_pending)) {
      err++;
      log_printf(0, "Incorrect slot count.  n_active=%d found=%d  **  n_pending=%d found=%d\n", n_active, ai, n_pending, pi);
   }

   if (err != 0) {
     log_printf(0, "-------------------Printing Lock attribute---------------------\n");
     slog_printf(0, "%s", lval);
     log_printf(0, "---------------------------------------------------------------\n");
   }

   inip_destroy(ifd);

   free(lval);

   return(err);
}

// **********************************************************************************

void lock_pause()
{
  usleep(0.25*1000000);
}

// **********************************************************************************
//  os_locking_tests - Tests object locking routines
// **********************************************************************************

void os_locking_tests()
{
   object_service_fn_t *os = lio_gc->os;
   creds_t  *creds = lio_gc->creds;
   char foo_path[PATH_LEN];
   os_fd_t *foo_fd;
   int err, i;
   os_fd_t *fd_read[5], *fd_write[3], *fd_abort[2];
   char *task[10];
   opque_t *q;
   op_generic_t *gop, *gop_read[5], *gop_write[3], *gop_abort[2];

   q = new_opque();
   opque_start_execution(q);

   //** Create the file foo
   snprintf(foo_path, PATH_LEN, "%s/foo", prefix);
   err = gop_sync_exec(os_create_object(os, creds, foo_path, OS_OBJECT_FILE, "me"));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: creating file: %s err=%d\n", foo_path, err);
      return;
   }

   // ** Verify foo exists
   err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: opening file: %s err=%d\n", foo_path, err);
      return;
   }

   //** Spawn 3 open(foo, READ)=r{0,1,2} tasks
   gop = os_open_object(os, creds, foo_path, OS_MODE_READ_BLOCKING, "r0", &fd_read[0], wait_time);
   gop_read[0] = gop;
   opque_add(q, gop);
   lock_pause();

   gop = os_open_object(os, creds, foo_path, OS_MODE_READ_BLOCKING, "r1", &fd_read[1], wait_time);
   gop_read[1] = gop;
   opque_add(q, gop);
   lock_pause();

   gop = os_open_object(os, creds, foo_path, OS_MODE_READ_BLOCKING, "r2", &fd_read[2], wait_time);
   gop_read[2] = gop;
   opque_add(q, gop);
   lock_pause();


   //** Spawn 2 open(foo, WRITE)=w{0,1} tasks
   gop = os_open_object(os, creds, foo_path, OS_MODE_WRITE_BLOCKING, "w0", &fd_write[0], wait_time);
   gop_write[0] = gop;
   opque_add(q, gop);
   lock_pause();

   gop = os_open_object(os, creds, foo_path, OS_MODE_WRITE_BLOCKING, "w1", &fd_write[1], wait_time);
   gop_write[1] = gop;
   opque_add(q, gop);
   lock_pause();

   //** Spawn 2 open(foo, WRITE)=a{0,1} tasks.  These will be aborted opens
   gop = os_open_object(os, creds, foo_path, OS_MODE_WRITE_BLOCKING, "a0", &fd_abort[0], 4);
   gop_abort[0] = gop;
   opque_add(q, gop);
   lock_pause();

   gop = os_open_object(os, creds, foo_path, OS_MODE_WRITE_BLOCKING, "a1", &fd_abort[1], wait_time);
   gop_abort[1] = gop;
   opque_add(q, gop);
   lock_pause();


   //** Spawn 2 open(foo, READ)=r{3,4} task
   gop = os_open_object(os, creds, foo_path, OS_MODE_READ_BLOCKING, "r3", &fd_read[3], wait_time);
   gop_read[3] = gop;
   opque_add(q, gop);
   lock_pause();

   gop = os_open_object(os, creds, foo_path, OS_MODE_READ_BLOCKING, "r4", &fd_read[4], wait_time);
   gop_read[4] = gop;
   opque_add(q, gop);
   lock_pause();

   //** Spawn 1 open(foo, WRITE)=w2 task
   gop = os_open_object(os, creds, foo_path, OS_MODE_WRITE_BLOCKING, "w2", &fd_write[2], wait_time);
   gop_write[2] = gop;
   opque_add(q, gop);
   lock_pause();

   log_printf(0, "STATE:  active=r0,r1,r2  pending=w0,w1,a0,a1,r3,r4,w2\n");  flush_log();

   //** Wait for the opens to complete
   gop_waitany(gop_read[0]);
   gop_waitany(gop_read[1]);
   gop_waitany(gop_read[2]);

   //** Verify the above state: active r0,r1,r2 pending w0,w1,r3,r4,w2
   task[0] = "r0";
   task[1] = "r1";
   task[2] = "r2";
   task[3] = "w0";
   task[4] = "w1";
   task[5] = "a0";
   task[6] = "a1";
   task[7] = "r3";
   task[8] = "r4";
   task[9] = "w2";
   err = check_lock_state(foo_fd, &(task[0]), 3, &(task[3]), 7);
   if (err != 0) {
      nfailed++;
      log_printf(0, "ERROR: checking state\n");
      return;
   }

   //** Close the 3 open tasks(r0,r1,r2).
   err = gop_sync_exec(os_close_object(os, fd_read[0]));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
      return;
   }
   err = gop_sync_exec(os_close_object(os, fd_read[1]));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
      return;
   }
   err = gop_sync_exec(os_close_object(os, fd_read[2]));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
      return;
   }

   log_printf(0, "STATE: active=w0  pending=w1,a0,a1,r3,r4,w2\n");  flush_log();

   //** Wait for the opens to complete
   gop_waitany(gop_write[0]);

   //** Verify state. This should leave you with w0 active and w1,a0,a1,r3,r4,w2
   err = check_lock_state(foo_fd, &(task[3]), 1, &(task[4]), 6);
   if (err != 0) {
      nfailed++;
      log_printf(0, "ERROR: checking state\n");
      return;
   }


   log_printf(0, "STATE: ABORT_TIMEOUT(a0)  active=w0  pending=w1,a1,r3,r4,w2\n");  flush_log();

   //** Let's do the aborts now
   err = gop_waitall(gop_abort[0]);  //** This should timeout on it's own
   if (err == OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Open succeded for a0 when it should have timed out file: %s err=%d\n", foo_path, err);
      return;
   }

   //** Verify the state. active=w0  pending=w1,a1,r3,r4,w2
   for (i=5; i<9; i++) task[i] = task[i+1];  //** Drop a0 fro mthe list
   err = check_lock_state(foo_fd, &(task[3]), 1, &(task[4]), 5);
   if (err != 0) {
      nfailed++;
      log_printf(0, "ERROR: checking state\n");
      return;
   }

   log_printf(0, "STATE: ABORT_CMD(a1)  active=w0  pending=w1,r3,r4,w2\n");  flush_log();

   //** Issue an abort for a1
   err = gop_sync_exec(os_abort_open_object(os, gop_abort[1]));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Aborting a1 file: %s err=%d\n", foo_path, err);
      return;
   }

   //** Verify the state. active=w0  pending=w1,r3,r4,w2
   for (i=5; i<8; i++) task[i] = task[i+1];  //** Drop a0 fro mthe list
   err = check_lock_state(foo_fd, &(task[3]), 1, &(task[4]), 4);
   if (err != 0) {
      nfailed++;
      log_printf(0, "ERROR: checking state\n");
      return;
   }

   //** Close w0.
   err = gop_sync_exec(os_close_object(os, fd_write[0]));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
      return;
   }

   log_printf(0, "STATE: active=w1  pending=r3,r4,w2\n");  flush_log();

   //** Wait for the opens to complete
   gop_waitany(gop_write[1]);

   //** Verify state: active=w1, pending=r3,r4,w2
   err = check_lock_state(foo_fd, &(task[4]), 1, &(task[5]), 3);
   if (err != 0) {
      nfailed++;
      log_printf(0, "ERROR: checking state\n");
      return;
   }

   //** Close w1;
   err = gop_sync_exec(os_close_object(os, fd_write[1]));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
      return;
   }

   log_printf(0, "STATE: active=r3,r4  pending=w2\n");  flush_log();

   //** Wait for the opens to complete
   gop_waitany(gop_read[3]);
   gop_waitany(gop_read[4]);

   //** Verify state: active=r3,r4 pending=w2
   err = check_lock_state(foo_fd, &(task[5]), 2, &(task[7]), 1);
   if (err != 0) {
      nfailed++;
      log_printf(0, "ERROR: checking state\n");
      return;
   }

   //** Close r3, r4
   err = gop_sync_exec(os_close_object(os, fd_read[3]));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
      return;
   }
   err = gop_sync_exec(os_close_object(os, fd_read[4]));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
      return;
   }


   log_printf(0, "STATE: active=w2  pending=\n");  flush_log();

   //** Wait for the opens to complete
   gop_waitany(gop_write[2]);

   //** Verify state active=w2  pending=
   err = check_lock_state(foo_fd, &(task[7]), 1, NULL, 0);
   if (err != 0) {
      nfailed++;
      log_printf(0, "ERROR: checking state\n");
      return;
   }

   //** Close w2;
   err = gop_sync_exec(os_close_object(os, fd_write[2]));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
      return;
   }

   log_printf(0, "Performing lock test cleanup\n");  flush_log();

   //** Close the inital foo_fd
   err = gop_sync_exec(os_close_object(os, foo_fd));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
      return;
   }

   //** rm foo
   err = gop_sync_exec(os_remove_object(os, creds, foo_path));
   if (err != OP_STATE_SUCCESS) {
      nfailed++;
      log_printf(0, "ERROR: opening file: %s err=%d\n", foo_path, err);
      return;
   }

   // ** And verify it's gone
   err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
   if (err != OP_STATE_FAILURE) {
      nfailed++;
      log_printf(0, "ERROR: Oops! Can open the recently deleted file: %s err=%d\n", foo_path, err);
      return;
   }

   opque_free(q, OP_DESTROY);

   log_printf(0, "PASSED!\n");
}


//*************************************************************************
//  Object service test program
//*************************************************************************

int main(int argc, char **argv)
{

  if (argc < 2) {
     printf("\n");
     printf("os_test LIO_COMMON_OPTIONS path\n");
     lio_print_options(stdout);
     printf("    path  - Path prefix to use\n");
     printf("\n");
     return(1);
  }

  lio_init(&argc, &argv);

//  if (argc < 2) {
//     printf("Missing prefix!\n");
//     return(1);
//  }

  prefix ="";
  if (argc > 1) prefix = argv[1];

  log_printf(0, "--------------------------------------------------------------------\n");
  log_printf(0, "Using prefix=%s\n", prefix);
  log_printf(0, "--------------------------------------------------------------------\n");
  flush_log();

  os_create_remove_tests();
  if (nfailed > 0) goto oops;

  os_attribute_tests();
  if (nfailed > 0) goto oops;

  os_locking_tests();
  if (nfailed > 0) goto oops;

oops:
  log_printf(0, "--------------------------------------------------------------------\n");
  log_printf(0, "Tasks failed: %d\n", nfailed);
  log_printf(0, "--------------------------------------------------------------------\n");

  lio_shutdown();

  return(0);
}


