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

#define _log_module_index 204

#include <assert.h>
#include <tbx/assert_result.h>
#include "exnode.h"
#include <tbx/log.h>
#include <tbx/iniparse.h>
#include <tbx/type_malloc.h>
#include "thread_pool.h"
#include "lio.h"
#include <tbx/string_token.h>

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
        printf("    -b bufsize         - Buffer size to use. Units supported (Default=%s)\n", tbx_stk_pretty_print_int_with_scale(bufsize, ppbuf));
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
            bufsize = tbx_stk_string_get_integer(argv[i]);
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
    tbx_type_malloc(buffer, char, bufsize+1);

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


