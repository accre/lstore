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

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include "object_service_abstract.h"

void print_usage()
{
  printf("\nUsage: arc_tag [-t tag name] [-rd recurse depth] [-rp regex of path to scan] [-ro regex for file selection] [-gp glob of path to scan] [-go glob for file selection] [PATH]...\n");
  printf("\t-t\ttag file to use (default if not specified: ~./arc_tag_file.txt)\n");
  printf("\t-rd\trecurse depth (default: 10000)\n");
  printf("\t-rp\tregular expression for path\n");
  printf("\t-ro\tregular expression for file selection\n");
  printf("\t-gp\tglob for path\n");
  printf("\t-go\tglob for file selection\n");
  printf("\nExamples to come soon\n");
  exit(0);
}

int main(int argc, char **argv)
{
  int i = 1, d, recurse_depth = 10000, start_option = 0, start_index;  
  char *regex_path = NULL;
  char *regex_object = NULL;
  char *tag_file = NULL;

  if (argc < 2) {
    print_usage();
  } else {
    do {
      start_option = i;
      if (strcmp(argv[i], "-h") == 0) {
	print_usage();
      } else if (strcmp(argv[i], "-t") == 0) {
	i++;
	tag_file = argv[i];
	i++;
      } else if (strcmp(argv[i], "-rd") == 0) {
	i++;
	recurse_depth = atoi(argv[i]);
	i++;
      } else if (strcmp(argv[i], "-rp") == 0) {
	i++;  
	regex_path = argv[i];
	i++;
      } else if (strcmp(argv[i], "-ro") == 0) {
	i++;
	regex_object = argv[i];
	i++;
      } else if (strcmp(argv[i], "-gp") == 0) {
	i++; 
	regex_path = os_glob2regex(argv[i]);
	i++;
      } else if (strcmp(argv[i], "-go") == 0) {
	i++;
	regex_object = os_glob2regex(argv[i]);
	i++;
      }

    } while ((start_option < i) && (i < argc));
    
    start_index = i;
  
    printf("regex_path:   %s\n", regex_path);
    printf("regex_object:   %s\n", regex_object);
    if (tag_file == NULL) {
      char *homedir = getenv("HOME");
      tag_file = strcat(homedir, "/.arc_tag_file.txt");
    }
  
    if (((access (tag_file, F_OK)) == -1) || ((access(tag_file, W_OK)) == -1)) {
      printf("%s does not exist or you do not have write permission!\n", tag_file);
      return(1);
    } else {
      if (i>=argc) {
        printf("Missing directory(s)!\n");
        print_usage();
      } else {
	for (d=start_index; d<argc; d++) {
	  printf("This is path %i   %s\n", d, argv[d]);
	}
      }
    }
  }

}
