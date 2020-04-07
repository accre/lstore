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

#define _log_module_index 205

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <errno.h>
#include <gop/gop.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/assert_result.h>
#include <tbx/direct_io.h>
#include <tbx/log.h>
#include <tbx/string_token.h>
#include <tbx/stdinarray_iter.h>
#include <tbx/type_malloc.h>

#include <lio/ex3.h>
#include <lio/lio.h>
#include <lio/os.h>

//*************************************************************************
// local_get - Get data from a local file
//*************************************************************************

int local_get(lio_path_tuple_t *tuple, ex_off_t bufsize, char *buffer, ex_off_t offset, ex_off_t len)
{
    FILE *fd;
    gop_op_status_t status;

    fd = fopen(tuple->path, "r");
    if (fd == NULL) {
        fprintf(stderr, "ERROR opening file %s\n", tuple->path);
        return(errno);
    }

    tbx_dio_init(fd);
    status = gop_sync_exec_status(lio_cp_local2local_gop(fd, stdout, bufsize, buffer, offset, -1, len, 0, NULL, 0));
    tbx_dio_finish(fd, 0);

    fclose(fd);
    return(status.error_code);
}

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    ex_off_t bufsize, offset, len;
    int err, err_close, ftype, i, start_index, start_option, return_code, enable_local;
    char *buffer;
    char *path;
    tbx_stdinarray_iter_t *it;
    lio_fd_t *fd;
    lio_path_tuple_t tuple;
    char ppbuf[32];

    err = 0;
    return_code = 0;
    _lio_ifd = stderr;  //** Default to all information going to stderr since the output is file data.

    bufsize = 20*1024*1024;
    offset = 0;
    len = -1;
    enable_local = 0;

    if (argc < 2) {
        printf("\n");
        printf("lio_get LIO_COMMON_OPTIONS [-b bufsize] [-o offset len] [--local] src_file1 .. src_file_N\n");
        lio_print_options(stdout);
        printf("    -b bufsize         - Buffer size to use. Units supported (Default=%s)\n", tbx_stk_pretty_print_int_with_scale(bufsize, ppbuf));
        printf("    -o offset len      - Only return the file starting at the provided offset and length.\n");
        printf("                         The default is to return the whole file.  Units are supported.\n");
        printf("                         If the length is -1 then the rest of the file is returned.\n");
        printf("    --local            - Enable reading of local files in addition to LStore files.\n");
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
                bufsize = tbx_stk_string_get_integer(argv[i]);
                i++;
            } else if (strcmp(argv[i], "-o") == 0) {
                i++;
                offset = tbx_stk_string_get_integer(argv[i]);
                i++;
                len = tbx_stk_string_get_integer(argv[i]);
                i++;
            } else if (strcmp(argv[i], "--local") == 0) {
                i++;
                enable_local = 1;
            }

        } while ((start_option - i < 0) && (i<argc));
    }
    start_index = i;

    //** This is the 1st dir to remove
    if (argv[start_index] == NULL) {
        fprintf(stderr, "Missing Source!\n");
        return(EINVAL);
    }

    //** Make the buffer. This makes sure it's page aligned for both R and W buffers
    tbx_fudge_align_size(bufsize, 2*getpagesize());
    tbx_malloc_align(buffer, getpagesize(), bufsize);
    it = tbx_stdinarray_iter_create(argc-start_index, (const char **)(argv+start_index));
    while ((path = tbx_stdinarray_iter_next(it)) != NULL) {
        //** Get the source
        tuple = lio_path_resolve(lio_gc->auto_translate, path);
        free(path);
        if (tuple.is_lio == 0) {
            if (enable_local == 1) {
                return_code = local_get(&tuple, bufsize, buffer, offset, len);
            } else {
                fprintf(stderr, "Unable to parse path: %s\n", tuple.path);
                return_code = EINVAL;
            }

            goto finished_early;
        }

        //** Check if it exists
        ftype = lio_exists(tuple.lc, tuple.creds, tuple.path);

        if ((ftype & OS_OBJECT_FILE_FLAG) == 0) { //** Doesn't exist or is a dir
            fprintf(stderr, "ERROR source file(%s) doesn't exist or is a dir ftype=%d!\n", tuple.path, ftype);
            return_code = EIO;
            goto finished_early;
        }

        gop_sync_exec(lio_open_gop(tuple.lc, tuple.creds, tuple.path, LIO_READ_MODE, NULL, &fd, 60));
        if (fd == NULL) {
            fprintf(stderr, "Failed opening file!  path=%s\n", tuple.path);
            return_code = EIO;
            goto finished_early;
        }

        //** Do the get
        err = gop_sync_exec(lio_cp_lio2local_gop(fd, stdout, bufsize, buffer, offset, len, NULL));
        if (err != OP_STATE_SUCCESS) {
            return_code = EIO;
            fprintf(stderr, "Failed reading data!  path=%s\n", tuple.path);
        }

        //** Close the file
        err_close = gop_sync_exec(lio_close_gop(fd));
        if (err_close != OP_STATE_SUCCESS) {
            err = err_close;
            return_code = EIO;
            fprintf(stderr, "Failed closing LIO file!  path=%s\n", tuple.path);
        }

finished_early:
        lio_path_release(&tuple);
    }

    free(buffer);
    tbx_stdinarray_iter_destroy(it);
    lio_shutdown();

    return(return_code);
}


