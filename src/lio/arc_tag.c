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
#include "lio_abstract.h"
#include "archive.h"

/*** TODO: check for duplicate tags (compairing both to INI contents and current command contents)  ***/
void add_tag(char *tag_file, char *tag_name, char *path, const char *regex_path, const char *regex_object, int obj_types, int recurse_depth)
{
    lio_path_tuple_t tuple;
    FILE *fd = fopen(tag_file, "a");

    if (fd == NULL) {
        printf("Failed to open %s!\n", tag_file);
        exit(1);
    }

    if (path != NULL) {
        tuple = lio_path_resolve(lio_gc->auto_translate, path);
        fprintf(fd, "\n[TAG]\npath=%s\nname=%s\nrecurse_depth=%d\nobject_types=%d\n\n", tuple.path, tag_name, recurse_depth, obj_types);
        lio_path_release(&tuple);
    } else {
        fprintf(fd, "\n[TAG]\ntag_name=%s\nrecurse_depth=%d\nobject_types=%d\nregex_path=%s\nregex_object=%s\n", tag_name, recurse_depth, obj_types, regex_path, regex_object);
    }
}

void print_usage()
{
    printf("\nUsage: arc_tag [-t tag name] [-rd recurse depth] [-rp regex of path to scan] [-ro regex for file selection] [-gp glob of path to scan] [-go glob for file selection] [PATH]...\n");
    printf("\t-t\ttag file to use (default if not specified: ~./arc_tag_file.txt)\n");
    printf("\t-n\ttag name (REQUIRED - this is an identifier and the directory name used in L-Store)\n");
    printf("\t-rd\trecurse depth (default: 10000)\n");
    printf("\t-o\tTypes of objects to select. Bitwise OR of 1=Files, 2=Directories, 4=symlink, 8=hardlink.  Default is %d.\n", OS_OBJECT_ANY);
    printf("\t-rp\tregular expression for path\n");
    printf("\t-ro\tregular expression for file selection\n");
    printf("\t-gp\tglob for path\n");
    printf("\t-go\tglob for file selection\n");
    printf("\nExamples to come soon\n");
    exit(0);
}

int main(int argc, char **argv)
{
    int i = 1, d, recurse_depth = 10000, start_option = 0, start_index, obj_types;
    char *regex_path = NULL;
    char *regex_object = NULL;
    char *tag_file = NULL;
    char *tag_name = NULL;

    lio_init(&argc, &argv);

    obj_types = OS_OBJECT_ANY;

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
            } else if (strcmp(argv[i], "-n") == 0) {
                i++;
                tag_name = argv[i];
                i++;
            } else if (strcmp(argv[i], "-rd") == 0) {
                i++;
                recurse_depth = atoi(argv[i]);
                i++;
            } else if (strcmp(argv[i], "-o") == 0) {
                i++;
                obj_types = atoi(argv[i]);
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
        if (tag_name == NULL) {
            printf("ERROR: tag name not specified!\n");
            print_usage();
        }
        start_index = i;
        if (tag_file == NULL) {
            char *homedir = getenv("HOME");
            tag_file = concat(homedir, "/.arc_tag_file.txt");
        }

        if (((access (tag_file, F_OK)) == -1) || ((access(tag_file, W_OK)) == -1)) {
            printf("ERROR: %s does not exist!  Please run arc_tag_create\n", tag_file);
            return(1);
        } else if ((access(tag_file, W_OK)) == -1) {
            printf("ERROR: Permission Denied!\n");
            return(1);
        } else {
            if (i>=argc) {
                printf("Missing target path(s)!\n\n");
                print_usage();
            } else {
                if ((regex_path != NULL) || (regex_object != NULL)) add_tag(tag_file, tag_name, NULL, regex_path, regex_object, obj_types, recurse_depth);
                for (d=start_index; d<argc; d++) {
                    add_tag(tag_file, tag_name, argv[d], NULL, NULL, obj_types, recurse_depth);
                }
            }
        }
    }

    lio_shutdown();

    return(0);
}
