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

#define _log_module_index 199

#include <errno.h>
#include <gop/gop.h>
#include <gop/mq.h>
#include <gop/opque.h>
#include <gop/tp.h>
#include <gop/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/assert_result.h>
#include <tbx/list.h>
#include <tbx/log.h>
#include <tbx/stdinarray_iter.h>
#include <tbx/type_malloc.h>

#include <lio/blacklist.h>
#include <lio/cache.h>
#include <lio/lio.h>
#include <lio/os.h>

char *exnode_data = NULL;

//*************************************************************************
// touch_fn - Actual touch function
//*************************************************************************

gop_op_status_t mkdir_fn(void *arg, int id)
{
    lio_path_tuple_t *tuple = (lio_path_tuple_t *)arg;
    int ftype, err;
    gop_op_status_t status;

    status = gop_success_status;

    //** Make sure it doesn't exist
    ftype = lio_exists(tuple->lc, tuple->creds, tuple->path);

    if (ftype != 0) { //** The file exists
        log_printf(1, "ERROR The dir exists\n");
        status.op_status = OP_STATE_FAILURE;
        status.error_code = 2;
    }

    //** Now create the object
    err = gop_sync_exec(lio_create_gop(tuple->lc, tuple->creds, tuple->path, OS_OBJECT_DIR_FLAG, exnode_data, NULL));
    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "ERROR creating dir!\n");
        status.op_status = OP_STATE_FAILURE;
        status.error_code = 3;
    }

    return(status);
}


//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int i, start_index, start_option;
    char *ex_fname, *path;
    gop_opque_t *q;
    gop_op_generic_t *gop;
    gop_op_status_t status;
    lio_path_tuple_t *tuple;
    tbx_stdinarray_iter_t *it;
    char *error_table[] = { "", "ERROR checking dir existence", "ERROR dir already exists", "ERROR creating dir" };
    FILE *fd;
    int return_code = 0;

    if (argc < 2) {
        printf("\n");
        printf("lio_mkdir LIO_COMMON_OPTIONS [-ex exnode.ex3] dir1 dir2 ...\n");
        lio_print_options(stdout);
        printf("    -ex exnode.ex3      - Optional exnode to use.  Defaults to using the parent directory's exnode.\n");
        printf("    dir1 dir2 ...       - New directories to create\n");
        return(1);
    }

    lio_init(&argc, &argv);

    ex_fname = NULL;

    //*** Parse the args
    i=1;
    do {
        start_option = i;

        if (strcmp(argv[i], "-ex") == 0) { //** Use an alternative exnode
            i++;
            ex_fname = argv[i];
            i++;
        }

    } while (start_option - i < 0);
    start_index = i;

    //** This is the file to create
    if (argv[i] == NULL) {
        fprintf(stderr, "Missing source directory!\n");
        return(EINVAL);
    }

    //** Load an alternative exnode if specified
    if (ex_fname != NULL) {
        fd = fopen(ex_fname, "r");
        if (fd == NULL) {
            fprintf(stderr, "ERROR reading exnode!\n");
            return(ENODEV);
        }
        fseek(fd, 0, SEEK_END);

        i = ftell(fd);
        tbx_type_malloc(exnode_data, char, i+1);
        exnode_data[i] = 0;

        fseek(fd, 0, SEEK_SET);
        if (fread(exnode_data, i, 1, fd) != 1) { //**
            fprintf(stderr, "ERROR reading exnode from disk!\n");
            return(ENODEV);
        }
        fclose(fd);
    }

    if (exnode_data != NULL) free(exnode_data);

    //** Spawn the tasks
    it = tbx_stdinarray_iter_create(argc-start_index, (const char **)(argv+start_index));
    q = gop_opque_new();
    opque_start_execution(q);
    while (1) {
        path = tbx_stdinarray_iter_next(it);
        if (path) {
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
            gop = gop_tp_op_new(lio_gc->tpc_unlimited, NULL, mkdir_fn, tuple, NULL, 1);
            gop_set_private(gop, tuple);
            log_printf(0, "gid=%d fname=%s\n", gop_id(gop), tuple->path);
            gop_opque_add(q, gop);
        }

        if ((gop_opque_tasks_left(q) > lio_parallel_task_count) || (path == NULL)) {
            gop = opque_waitany(q);
            if (!gop) break;
            tuple = gop_get_private(gop);
            status = gop_get_status(gop);
            if (status.op_status != OP_STATE_SUCCESS) {
                info_printf(lio_ifd, 0, "Failed with directory %s with error %s\n", tuple->path, error_table[status.error_code]);
                return_code = EIO;
            }
            gop_free(gop, OP_DESTROY);
            lio_path_release(tuple);
            free(tuple);
        }
    }

    gop_opque_free(q, OP_DESTROY);
    tbx_stdinarray_iter_destroy(it);

    lio_shutdown();

    return(return_code);
}


