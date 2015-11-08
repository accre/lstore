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

#define _log_module_index 204

#include <assert.h>
#include "assert_result.h"
#include "exnode.h"
#include "log.h"
#include "iniparse.h"
#include "type_malloc.h"
#include "thread_pool.h"
#include "lio.h"
#include "string_token.h"

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    ex_off_t bufsize;
    int err, err_close, dtype, i, start_index, start_option;
    lio_fd_t *fd;
    char *buffer;
    char ppbuf[32];
    lio_path_tuple_t tuple;

    bufsize = 20*1024*1024;

    if (argc < 2) {
        printf("\n");
        printf("lio_put LIO_COMMON_OPTIONS [-b bufsize] dest_file\n");
        lio_print_options(stdout);
        printf("    -b bufsize         - Buffer size to use. Units supported (Default=%s)\n", pretty_print_int_with_scale(bufsize, ppbuf));
        printf("    dest_file          - Destination file\n");
        return(1);
    }


    err = 0;

    lio_init(&argc, &argv);

    i=1;
    do {
        start_option = i;

        if (strcmp(argv[i], "-b") == 0) {  //** Get the buffer size
            i++;
            bufsize = string_get_integer(argv[i]);
            i++;
        }

    } while ((start_option < i) && (i<argc));
    start_index = i;

    //** This is the 1st dir to remove
    if (argv[start_index] == NULL) {
        info_printf(lio_ifd, 0, "Missing destination file!\n");
        return(2);
    }

    //** Make the buffer
    type_malloc(buffer, char, bufsize+1);

    //** Get the destination
    tuple = lio_path_resolve(lio_gc->auto_translate, argv[start_index]);

    //** Check if it exists and if not create it
    dtype = lio_exists(tuple.lc, tuple.creds, tuple.path);

    if (dtype == 0) { //** Need to create it
        err = gop_sync_exec(gop_lio_create_object(tuple.lc, tuple.creds, tuple.path, OS_OBJECT_FILE, NULL, NULL));
        if (err != OP_STATE_SUCCESS) {
            info_printf(lio_ifd, 1, "ERROR creating file(%s)!\n", tuple.path);
            goto finished;
        }
    } else if ((dtype & OS_OBJECT_DIR) > 0) { //** It's a dir so fail
        info_printf(lio_ifd, 0, "Destination(%s) is a dir!\n", tuple.path);
        goto finished;
    }

    gop_sync_exec(gop_lio_open_object(tuple.lc, tuple.creds, tuple.path, lio_fopen_flags("w"), NULL, &fd, 60));
    if (fd == NULL) {
        info_printf(lio_ifd, 0, "Failed opening file!  path=%s\n", tuple.path);
        goto finished;
    }

    //** Do the put
    err = gop_sync_exec(gop_lio_cp_local2lio(stdin, fd, bufsize, buffer, NULL));
    if (err != OP_STATE_SUCCESS) {
        info_printf(lio_ifd, 0, "Failed writing data!  path=%s\n", tuple.path);
    }

    //** Close the file
    err_close = gop_sync_exec(gop_lio_close_object(fd));
    if (err_close != OP_STATE_SUCCESS) {
        err = err_close;
        info_printf(lio_ifd, 0, "Failed closing LIO file!  path=%s\n", tuple.path);
    }


finished:
    free(buffer);

    lio_path_release(&tuple);

    lio_shutdown();

    return(err);
}


