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

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <gop/gop.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/assert_result.h>
#include <tbx/log.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>

#include <lio/ex3.h>
#include <lio/lio.h>
#include <lio/os.h>


//*************************************************************************
// local_put - Put data to a local file

//*************************************************************************

int local_put(lio_path_tuple_t *tuple, ex_off_t bufsize, char *buffer, ex_off_t offset, ex_off_t len, int truncate)
{
    FILE *fd;
    gop_op_status_t status;

    fd = fopen(tuple->path, "r+");
    if (fd == NULL) {  //* File may not exist so create if necessary
        fd = fopen(tuple->path, "w+");
        if (fd == NULL) {
            fprintf(stderr, "ERROR opening file %s\n", tuple->path);
            return(errno);
        }
    }

    status = gop_sync_exec_status(lio_cp_local2local_gop(stdin, fd, bufsize, buffer, -1, offset, len, truncate, NULL));

    fclose(fd);
    return(status.error_code);
}

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    ex_off_t bufsize, offset, len;
    int err, err_close, dtype, i, start_index, start_option, truncate, enable_local;
    lio_fd_t *fd;
    char *buffer;
    char ppbuf[32];
    lio_path_tuple_t tuple;

    bufsize = 20*1024*1024;

    if (argc < 2) {
        printf("\n");
        printf("lio_put LIO_COMMON_OPTIONS [-b bufsize] [-o offset len] [--no-truncate] [--local] dest_file\n");
        lio_print_options(stdout);
        printf("    -b bufsize         - Buffer size to use. Units supported (Default=%s)\n", tbx_stk_pretty_print_int_with_scale(bufsize, ppbuf));
        printf("    -o offset len      - Place the data starting at the provided offset and length.\n");
        printf("                         If the length is -1 then all data is stored (default). Units are supported\n");
        printf("    --no-truncate      - Don't truncate the file. Defaults to truncating the file\n");
        printf("    --local            - Enable writing to a local file in addition to LStore file.\n");
        printf("    dest_file          - Destination file\n");
        return(1);
    }


    err = 0;

    lio_init(&argc, &argv);

    offset = 0;
    len = -1;
    truncate = 1;
    enable_local = 0;
    i=1;
    do {
        start_option = i;

        if (strcmp(argv[i], "-b") == 0) {  //** Get the buffer size
            i++;
            bufsize = tbx_stk_string_get_integer(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-o") == 0) {
            i++;
            offset = tbx_stk_string_get_integer(argv[i]);
            i++;
            len = tbx_stk_string_get_integer(argv[i]);
            i++;
        } else if (strcmp(argv[i], "--no-truncate") == 0) {
            i++;
            truncate = 0;
        } else if (strcmp(argv[i], "--local") == 0) {
            i++;
            enable_local = 1;
        }
    } while ((start_option - i < 0) && (i<argc));
    start_index = i;

    //** This is the 1st dir to remove
    if (argv[start_index] == NULL) {
        info_printf(lio_ifd, 0, "Missing destination file!\n");
        return(2);
    }

    //** Make the buffer. This makes sure it's page aligned for both R and W buffers
    tbx_fudge_align_size(bufsize, 2*getpagesize());
    tbx_malloc_align(buffer, getpagesize(), bufsize);

    //** Get the destination
    tuple = lio_path_resolve(lio_gc->auto_translate, argv[start_index]);
    if (tuple.is_lio == 0) {
        if (enable_local == 1) {
            err = local_put(&tuple, bufsize, buffer, offset, len, truncate);
        } else {
            fprintf(stderr, "Unable to parse path: %s\n", tuple.path);
            err = EINVAL;
        }

        goto finished;
    }

    //** Check if it exists and if not create it
    dtype = lio_exists(tuple.lc, tuple.creds, tuple.path);

    if (dtype == 0) { //** Need to create it
        err = gop_sync_exec(lio_create_gop(tuple.lc, tuple.creds, tuple.path, OS_OBJECT_FILE_FLAG, NULL, NULL));
        if (err != OP_STATE_SUCCESS) {
            info_printf(lio_ifd, 1, "ERROR creating file(%s)!\n", tuple.path);
            goto finished;
        }
    } else if ((dtype & OS_OBJECT_DIR_FLAG) > 0) { //** It's a dir so fail
        info_printf(lio_ifd, 0, "Destination(%s) is a dir!\n", tuple.path);
        goto finished;
    }

    gop_sync_exec(lio_open_gop(tuple.lc, tuple.creds, tuple.path, lio_fopen_flags("r+"), NULL, &fd, 60));
    if (fd == NULL) {
        info_printf(lio_ifd, 0, "Failed opening file!  path=%s\n", tuple.path);
        goto finished;
    }

    //** Do the put
    err = gop_sync_exec(lio_cp_local2lio_gop(stdin, fd, bufsize, buffer, offset, len, truncate, NULL));
    if (err != OP_STATE_SUCCESS) {
        info_printf(lio_ifd, 0, "Failed writing data!  path=%s\n", tuple.path);
    }

    //** Close the file
    err_close = gop_sync_exec(lio_close_gop(fd));
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


