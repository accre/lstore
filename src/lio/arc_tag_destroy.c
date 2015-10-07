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
    printf("");
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
