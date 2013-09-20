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

#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include "iniparse.h"
#include "lio.h"
#include "type_malloc.h"


typedef struct {
  lio_path_tuple_t src_tuple;
} ls_file_t;

typedef struct {
  lio_path_tuple_t src_tuple;
  os_regex_table_t *regex;
  int recurse_depth;
  int dest_type;
} ls_path_t;

typedef struct {
  lio_path_tuple_t tuple;
  os_object_iter_t *oit;
  local_object_iter_t *lit;
} ls_object_iter_t;



//*************************************************************************
//  ls_create_object_iter - Create an ls object iterator
//*************************************************************************

ls_object_iter_t *ls_create_object_iter(lio_path_tuple_t tuple, os_regex_table_t *path_regex, os_regex_table_t *obj_regex, int obj_types, int rd)
{
  ls_object_iter_t *it;

  type_malloc_clear(it, ls_object_iter_t, 1);

  it->tuple = tuple;
  if (tuple.is_lio == 1) {
     it->oit = os_create_object_iter(tuple.lc->os, tuple.creds, path_regex, obj_regex, obj_types, NULL, rd, NULL, 0);
  } else {
     it->lit = create_local_object_iter(path_regex, obj_regex, obj_types, rd);
  }

  return(it);
}

//*************************************************************************
//  ls_destroy_object_iter - Destroys an ls object iterator
//*************************************************************************

void ls_destroy_object_iter(ls_object_iter_t *it)
{

  if (it->tuple.is_lio == 1) {
    os_destroy_object_iter(it->tuple.lc->os, it->oit);
  } else {
    destroy_local_object_iter(it->lit);
  }

  free(it);
}

//*************************************************************************
//  ls_next_object - Returns the next object to work on
//*************************************************************************

int ls_next_object(ls_object_iter_t *it, char **fname, int *prefix_len)
{
  int err = 0;

  if (it->tuple.is_lio == 1) {
    err = os_next_object(it->tuple.lc->os, it->oit, fname, prefix_len);
  } else {
    err = local_next_object(it->lit, fname, prefix_len);
  }

log_printf(15, "ftype=%d\n", err);
  return(err);
}


//**********************************************************************************

//**********************************************************************************

void run_ls(char *path, char *regex_path, char *regex_object, int obj_types, int recurse_depth) {
  printf("%s  %s  %s  %i\n", path, regex_path, regex_object, recurse_depth);

  ls_object_iter_t *it;  
  os_regex_table_t *rp, *ro;
  lio_path_tuple_t tuple;
  int prefix_len, ftype;
  char *fname = NULL;

  rp = ro = NULL;

  tuple = lio_path_resolve(lio_gc->auto_translate, path);
  if (path != NULL) {
     lio_path_wildcard_auto_append(&tuple);
     rp = os_path_glob2regex(tuple.path);     
  } else {
     rp = os_regex2table(regex_path);     
     ro = os_regex2table(regex_object);     
  }

  it = ls_create_object_iter(tuple, rp, ro, obj_types, recurse_depth);
  if (it == NULL) {
     info_printf(lio_ifd, 0, "ERROR: Failed with object_iter creation! path=%s regex_path=%s regex_object=%s\n", path, regex_path, regex_object);
     exit(3);
  }

   while ((ftype = ls_next_object(it, &fname, &prefix_len)) > 0) {
     printf("fname: %s    ftype=%d\n", fname, ftype);
     free(fname);
   }

  ls_destroy_object_iter(it);

  if (rp != NULL) os_regex_table_destroy(rp);
  if (ro != NULL) os_regex_table_destroy(ro);

  lio_path_release(&tuple);
}


void process_tag_file(char *tag_file) {

  char *path = NULL;
  char *regex_path = NULL;
  char *regex_object = NULL;
  int recurse_depth, obj_types;
  inip_file_t *ini_fd;
  inip_group_t *ini_g;
  inip_element_t *ele;
  char *key, *value;

  /*** Check for tag file existence and read permission ***/
  if (((access (tag_file, F_OK)) == -1) || ((access(tag_file, R_OK)) == -1)) {
    printf("%s does not exist or you do not have read permission!\n", tag_file);
    exit(1);
  } else { 
    /*** process tag file ***/
    assert(ini_fd = inip_read(tag_file));
    ini_g = inip_first_group(ini_fd);
    obj_types = OS_OBJECT_ANY;
    while (ini_g != NULL) {
      if (strcmp(inip_get_group(ini_g), "TAG") == 0) {
	ele = inip_first_element(ini_g);
	while (ele != NULL) {
	  key = inip_get_element_key(ele);
	  value = inip_get_element_value(ele);
	  if (strcmp(key, "path") == 0) {
	    path = value;
	  } else if (strcmp(key, "regex_path") == 0) {
	    regex_path = value;
	  } else if (strcmp(key, "regex_object") == 0) {
	    regex_object = value;
	  } else if (strcmp(key, "recurse_depth") == 0) {
	    recurse_depth = atoi(value);
	  } else if (strcmp(key, "object_types") == 0) {
	    obj_types = atoi(value);
	  }
	  ele = inip_next_element(ele);
	}
	run_ls(path, regex_path, regex_object, obj_types, recurse_depth);
      }
      ini_g = inip_next_group(ini_g);
    }
    /*** proper cleanup ***/
    inip_destroy(ini_fd);
  }
}


void print_usage() {
  printf("\nUsage: arc_tag_ls [-t tag file] \n");
  printf("\t-t\ttag file to use (default if not specified: ~./arc_tag_file.txt)\n");
  printf("\nExamples to come soon\n");
  exit(0);
}


int main(int argc, char **argv) {
 
 int i = 1, start_option = 0;   
 char *tag_file = NULL;
 
 lio_init(&argc, &argv);

  /*** Parse the args ***/
  if (argc > 3) {
    print_usage();
  } else if (argc > 1) {
    do {
      start_option = i;
      if (strcmp(argv[i], "-h") == 0) {
	print_usage();
      } else if (strcmp(argv[i], "-t") == 0) {
	i++;
	tag_file = argv[i];
      }

    } while ((start_option < i) && (i < argc));  
  }
  /*** If no tag file was specified, set to the default ***/
  if (tag_file == NULL) {
    char *homedir = getenv("HOME");
    tag_file = strcat(homedir, "/.arc_tag_file.txt");
  }
  process_tag_file(tag_file);

  lio_shutdown();

}
