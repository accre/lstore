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
#include "archive.h"


void destroy_tag_file(const char *filepath)
{
    /* Check for existence */
    if ((access(filepath, F_OK)) != -1) {
        remove(filepath);
    } else {
        printf("%s does not exist!  \n", filepath);
    }
}


void print_usage()
{
    printf("\nUsage: arc_tag_destrot [tag file name]\n\tIf no tag name is specified, the default ~/.arc_tag_file.txt will be used\n");
    exit(0);
}

int main(int argc, char **argv)
{
    int i = 1, start_option = 0;
    char *path = NULL;

    if (argc > 2) {
        print_usage();
    } else if (argc > 1) {
        do {
            start_option = i;
            if (strcmp(argv[i], "-h") == 0) {
                print_usage();
            } else {
                path = argv[i];
            }
        } while ((start_option < i) && (i < argc));
    }
    if (path == NULL) {
        char *homedir = getenv("HOME");
        path = concat(homedir, "/.arc_tag_file.txt");
    }
    destroy_tag_file(path);
}
