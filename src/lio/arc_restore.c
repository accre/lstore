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

 
 File:   arc_restore.c
 Author: Bobby Brown

 Created on November 11, 2013, 1:11 PM
*/

#include <stdio.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include "iniparse.h"
#include "lio.h"
#include "archive.h"

void process_tag_file(char *tag_file, char *tag_name) {
    
}

void print_usage() {
    printf("\nUsage: arc_restore [-t tag file] [-n tag name]\n");
    printf("\t-t\ttag file to use (default if not specified: ~./arc_tag_file.txt)\n");
    printf("\t\n\ttag name to restore (if none, all tags will be restored)");
    printf("\nExamples to come soon\n");
    exit(0);
}

/*
 * 
 */
int main(int argc, char** argv) {
int i = 1, start_option = 0;
    char *tag_file = NULL;
    char *tag_name = NULL;

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
                i++;
            } else if (strcmp(argv[i], "-n") == 0) {
                i++;
                tag_name = argv[i];
                i++;
            }
        } while ((start_option < i) && (i < argc));
    }
    /*** If no tag file was specified, set to the default ***/
    if (tag_file == NULL) {
        char *homedir = getenv("HOME");
        tag_file = concat(homedir, "/.arc_tag_file.txt");
    }
    process_tag_file(tag_file, tag_name);

    lio_shutdown();

    return(EXIT_SUCCESS);
}

