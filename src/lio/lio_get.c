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

#define _log_module_index 205

#include <assert.h>
#include "exnode.h"
#include "log.h"
#include "iniparse.h"
#include "string_token.h"
#include "type_malloc.h"
#include "thread_pool.h"
#include "lio.h"


//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    ex_off_t bufsize;
    int err, err_close, ftype, i, start_index, start_option;
    char *buffer;
    lio_fd_t *fd;
    lio_path_tuple_t tuple;
    char ppbuf[32];

    err = 0;

    _lio_ifd = stderr;  //** Default to all information going to stderr since the output is file data.

    bufsize = 20*1024*1024;

//printf("argc=%d\n", argc);
    if (argc < 2) {
        printf("\n");
        printf("lio_get LIO_COMMON_OPTIONS [-b bufsize] src_file1 .. src_file_N\n");
        lio_print_options(stdout);
        printf("    -b bufsize         - Buffer size to use. Units supported (Default=%s)\n", pretty_print_int_with_scale(bufsize, ppbuf));
        printf("    src_file           - Source file\n");
        return(1);
    }

    lio_init(&argc, &argv);
    i=1;
    if (i < argc) {
        do {
            start_option = i;

            if (strcmp(argv[i], "-b") == 0) {  //** Get the buffer size
                i++;
                bufsize = string_get_integer(argv[i]);
                i++;
            }

        } while ((start_option < i) && (i<argc));
    }
    start_index = i;

    //** This is the 1st dir to remove
    if (argv[start_index] == NULL) {
        info_printf(lio_ifd, 0, "Missing Source!\n");
        return(2);
    }

    //** Make the buffer
    type_malloc(buffer, char, bufsize+1);

    for (i=start_index; i<argc; i++) {
        //** Get the source
        tuple = lio_path_resolve(lio_gc->auto_translate, argv[i]);

        //** Check if it exists
        ftype = lio_exists(tuple.lc, tuple.creds, tuple.path);

        if ((ftype & OS_OBJECT_FILE) == 0) { //** Doesn't exist or is a dir
            info_printf(lio_ifd, 1, "ERROR source file(%s) doesn't exist or is a dir ftype=%d!\n", tuple.path, ftype);
            goto finished_early;
        }

        gop_sync_exec(gop_lio_open_object(tuple.lc, tuple.creds, tuple.path, LIO_READ_MODE, NULL, &fd, 60));
        if (fd == NULL) {
            info_printf(lio_ifd, 0, "Failed opening file!  path=%s\n", tuple.path);
            goto finished_early;
        }

        //** Do the get
        err = gop_sync_exec(gop_lio_cp_lio2local(fd, stdout, bufsize, buffer, NULL));
        if (err != OP_STATE_SUCCESS) {
            info_printf(lio_ifd, 0, "Failed reading data!  path=%s\n", tuple.path);
        }

        //** Close the file
        err_close = gop_sync_exec(gop_lio_close_object(fd));
        if (err_close != OP_STATE_SUCCESS) {
            err = err_close;
            info_printf(lio_ifd, 0, "Failed closing LIO file!  path=%s\n", tuple.path);
        }

finished_early:
        lio_path_release(&tuple);
    }

    free(buffer);

    lio_shutdown();

    return((err == OP_STATE_SUCCESS) ? 0 : EIO);
}


