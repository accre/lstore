/*
   Copyright 2016 Vanderbilt University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include "archive.h"


void create_tag_file(const char *filepath, int flush)
{
    /* Check for existence */
    if((access(filepath, F_OK)) != -1) {
        if (flush == 0) {
            printf("Tag file already exists!  %s\n", filepath);
            exit(1);
            /* Check for write permission */
        } else if ((access(filepath, W_OK)) != -1) {
            remove(filepath);
        } else {
            printf("You do not have write permission to overwrite %s!\n", filepath);
            exit(1);
        }
    }
    time_t cur_time = time(NULL);
    FILE *fd = fopen(filepath, "w");
    /* One final check */
    if (fd == NULL) {
        printf("Failed to create/open %s\n", filepath);
        exit(1);
    }
    fprintf(fd, "#\n# Tag file created %s#\n", ctime(&cur_time));

}


void print_usage()
{
    printf("\nUsage: arc_tag_create [-h] [-f] [tag name]\nIf no tag name is specified, the default ~/.arc_tag_file.txt will be used\n");
    printf("\t-h\tPrint help message\n");
    printf("\t-f\tFlush the specifed tag file\n");
    printf("\n");
    exit(0);
}

int main(int argc, char **argv)
{
    int i = 1, flush = 0, start_option = 0;
    char *path = NULL;

    if (argc > 3) {
        print_usage();
    } else if (argc > 1) {
        do {
            start_option = i;
            if (strcmp(argv[i], "-h") == 0) {
                print_usage();
            } else if (strcmp(argv[i], "-f") == 0) {
                flush = 1;
            } else {
                path = argv[i];
            }
        } while ((start_option < i) && (i < argc));
    }
    if (path == NULL) {
        char *homedir = getenv("HOME");
        path = concat(homedir, "/.arc_tag_file.txt");
    }
    create_tag_file(path, flush);
}
