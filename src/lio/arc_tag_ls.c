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
  local_object_iter_t *lit;
} ls_object_iter_t;


void run_ls(char *path, char *regex_path, char *regex_object, int recurse_depth) {
  printf("%s  %s  %s  %i\n", path, regex_path, regex_object, recurse_depth);

  ls_object_iter_t *it;  
  int *prefix_len;
  char *fname = NULL;
  char *test = strcat(path, regex_path);

  type_malloc_clear(it, ls_object_iter_t, 1);
  it->tuple = lio_path_resolve(0, test);
  it->lit = create_local_object_iter(os_regex2table(test), os_regex2table(regex_object), OS_OBJECT_FILE, recurse_depth);

  if (it == NULL) {
    printf("ERROR: Failed to parse arc_tag: %s  %s  %s  %i\n", path, regex_path, regex_object, recurse_depth);
    exit(3);
  }
   while ((local_next_object(it->lit, &fname, &prefix_len)) > 0) {
     //printf("fname: %s\n", fname);
   }

}


void process_tag_file(char *tag_file) {

  char *path = NULL;
  char *regex_path = NULL;
  char *regex_object = NULL;
  int recurse_depth;
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
    while (ini_g != NULL) {
      if (strcmp(inip_get_group(ini_g), "TAG") == 0) {
	ele = inip_first_element(ini_g);
	while (ele != NULL) {
	  key = inip_get_element_key(ele);
	  value = inip_get_element_value(ele);
	  if (strcmp(key, "path") == 0) {
	    path = value;
	  } else if (strcmp(key, "regex_path") == 0) {
	    regex_path = strdup(value);
	  } else if (strcmp(key, "regex_object") == 0) {
	    regex_object = strdup(value);
	  } else if (strcmp(key, "recurse_depth") == 0) {
	    recurse_depth = atoi(strdup(value));
	  }
	  ele = inip_next_element(ele);
	}
	run_ls(path, regex_path, regex_object, recurse_depth);
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
  /*** If no tag file was specified, set to the default***/
  if (tag_file == NULL) {
    char *homedir = getenv("HOME");
    tag_file = strcat(homedir, "/.arc_tag_file.txt");
  }
  process_tag_file(tag_file);
}
