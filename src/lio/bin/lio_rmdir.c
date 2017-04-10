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

#define _log_module_index 198

#include <gop/gop.h>
#include <gop/mq.h>
#include <gop/opque.h>
#include <gop/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <tbx/assert_result.h>
#include <tbx/list.h>
#include <tbx/log.h>
#include <tbx/stdinarray_iter.h>
#include <tbx/type_malloc.h>

#include <lio/blacklist.h>
#include <lio/cache.h>
#include <lio/lio.h>
#include <lio/os.h>


//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int rg_mode, return_code;
    gop_op_status_t status;
    lio_os_regex_table_t *rpath;
    lio_path_tuple_t *tuple;
    lio_os_regex_table_t *rp_single, *ro_single;
    tbx_stdinarray_iter_t *it;
    char *path;
    lio_path_tuple_t rg_tuple;

    if (argc < 2) {
        printf("\n");
        printf("lio_rmdir LIO_COMMON_OPTIONS LIO_PATH_OPTIONS\n");
        lio_print_options(stdout);
        lio_print_path_options(stdout);
        return(1);
    }

    lio_init(&argc, &argv);

    //*** Parse the directory args
    rp_single = ro_single = NULL;

    rg_mode = lio_parse_path_options(&argc, argv, lio_gc->auto_translate, &rg_tuple, &rp_single, &ro_single);

    //** This is the 1st dir to remove
    if (argv[1] == NULL) {
        info_printf(lio_ifd, 0, "Missing directory!\n");
        return(EINVAL);
    }

    return_code = 0;

    //** Launch the REGEX op if needed
    if (rg_mode == 1) {
        status = gop_sync_exec_status(lio_remove_regex_gop(rg_tuple.lc, rg_tuple.creds, rp_single, ro_single, OS_OBJECT_DIR_FLAG, 0, lio_parallel_task_count));
        return_code = EIO;
        info_printf(lio_ifd, 0, "Failed with regex path\n");
        lio_path_release(&rg_tuple);
    }

    it = tbx_stdinarray_iter_create(argc-1, (const char **)(argv+1));
    while ((path = tbx_stdinarray_iter_next(it)) != NULL) {
        tbx_type_malloc_clear(tuple, lio_path_tuple_t, 1);
        *tuple = lio_path_resolve(lio_gc->auto_translate, path);
        if (tuple->is_lio < 0) {
            fprintf(stderr, "Unable to parse path: %s\n", path);
            free(path);
            free(tuple);
            return_code = EINVAL;
            continue;
        }
        free(path);
        rpath = lio_os_path_glob2regex(tuple->path);
        status = gop_sync_exec_status(lio_remove_regex_gop(tuple->lc, tuple->creds, rpath, NULL, OS_OBJECT_DIR_FLAG, 0, lio_parallel_task_count));
        if (status.op_status != OP_STATE_SUCCESS) {
            return_code = EIO;
            info_printf(lio_ifd, 0, "Failed with directory %s\n", tuple->path);
        }

        lio_path_release(tuple); free(tuple);
        lio_os_regex_table_destroy(rpath);
    }

    tbx_stdinarray_iter_destroy(it);
    lio_shutdown();

    return(return_code);
}


