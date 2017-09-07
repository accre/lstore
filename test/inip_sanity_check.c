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

#include <stdlib.h>
#include <string.h>
#include <tbx/iniparse.h>
#include <tbx/log.h>

//*************************************************************************
//  Simple sanity check that all INI files included can be resolved and
//     is not malformed.
//*************************************************************************

void print_help()
{
    printf("\n");
    printf("inip_sanity_check [-p] INI-file\n");
    printf("     -p   Print the resolved INI\n");
    printf("\n");
}

int main(int argc, char **argv)
{
    int err, nbytes, do_print;
    char *text, *fname;
    tbx_inip_file_t *ifd;

    err = 0;
    if (argc < 2) {
        print_help();
        return(1);
    }

    tbx_log_open("stderr", 0);

    fname = argv[1];
    do_print = 0;
    if (strcmp(argv[1], "-p") == 0) {
        do_print = 1;
        fname = argv[2];
    }

    ifd = tbx_inip_file_read(fname);
    if (ifd == NULL) {
        fprintf(stdout, "ERROR: parsing file!\n");
        return(1);
    }
    tbx_inip_destroy(ifd);

    if (do_print) {
        err = tbx_inip_file2string(fname, &text, &nbytes);
        if (err == 0) {
            printf("nbytes=%d\n------\n%s", nbytes, text);
            free(text);
        }
    }
    return(err);
}


