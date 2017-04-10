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

#define _log_module_index 190

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

gop_op_status_t touch_fn(void *arg, int id)
{
    lio_path_tuple_t *tuple = (lio_path_tuple_t *)arg;
    int ftype, err;
    gop_op_status_t status;

    status = gop_success_status;


    ftype = lio_exists(tuple->lc, tuple->creds, tuple->path);
    if (ftype != 0) { //** The file exists so just update the modified attribute
        err = gop_sync_exec(lio_setattr_gop(tuple->lc, tuple->creds, tuple->path, NULL, "os.timestamp.system.modify_data", NULL, 0));
        if (err != OP_STATE_SUCCESS) {
            status.op_status = OP_STATE_FAILURE;
            status.error_code = 1;
        }
    } else {  //** New file so create the object
        err = gop_sync_exec(lio_create_gop(tuple->lc, tuple->creds, tuple->path, OS_OBJECT_FILE_FLAG, exnode_data, NULL));
        if (err != OP_STATE_SUCCESS) {
            log_printf(1, "ERROR creating file!\n");
            status.op_status = OP_STATE_FAILURE;
            status.error_code = 2;
        }
    }

    return(status);
}


//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int i, start_index, start_option, return_code;
    char *ex_fname, *path;
    tbx_stdinarray_iter_t *it;
    gop_opque_t *q;
    gop_op_generic_t *gop;
    gop_op_status_t status;
    lio_path_tuple_t *tuple;
    char *error_table[] = { "", "ERROR Failed to update modify timestamp", "ERROR creating file" };
    FILE *fd;

    if (argc < 2) {
        printf("\n");
        printf("lio_touch LIO_COMMON_OPTIONS [-ex exnode.ex3] file1 file2 ...\n");
        lio_print_options(stdout);
        printf("    -ex exnode.ex3  - Optional exnode to use.  Defaults to parent dir.\n");
        printf("    file*           - New files to create\n");
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
        printf("Missing source file!\n");
        return(2);
    }

    //** Load an alternative exnode if specified
    if (ex_fname != NULL) {
        fd = fopen(ex_fname, "r");
        if (fd == NULL) {
            printf("ERROR reading exnode!\n");
            return(EIO);
        }
        fseek(fd, 0, SEEK_END);

        i = ftell(fd);
        tbx_type_malloc(exnode_data, char, i+1);
        exnode_data[i] = 0;

        fseek(fd, 0, SEEK_SET);
        if (fread(exnode_data, i, 1, fd) != 1) { //**
            printf("ERROR reading exnode from disk!\n");
            return(EIO);
        }
        fclose(fd);
    }

    if (exnode_data != NULL) free(exnode_data);

    //** Spawn the tasks
    q = gop_opque_new();
    opque_start_execution(q);
    return_code = 0;
    it = tbx_stdinarray_iter_create(argc-start_index, (const char **)(argv+start_index));
    while (1) {
        path = tbx_stdinarray_iter_next(it);
        if (path) {
            tbx_type_malloc(tuple, lio_path_tuple_t, 1);
            *tuple = lio_path_resolve(lio_gc->auto_translate, path);
            if (tuple->is_lio < 0) {
                fprintf(stderr, "Unable to parse path: %s\n", path);
                free(path);
                free(tuple);
                return_code = EINVAL;
                continue;
            }
            free(path);

            gop = gop_tp_op_new(lio_gc->tpc_unlimited, NULL, touch_fn, tuple, NULL, 1);
            gop_set_private(gop, tuple);
            gop_opque_add(q, gop);
        }

        if ((gop_opque_tasks_left(q) > lio_parallel_task_count) || (path == NULL)) {
            gop = opque_waitany(q);
            if (!gop) break;

            status = gop_get_status(gop);
            tuple = gop_get_private(gop);
            if (status.op_status != OP_STATE_SUCCESS) {
                info_printf(lio_ifd, 0, "Failed with file %s with: %s\n", tuple->path, error_table[status.error_code]);
                return_code = EIO;
            }
            lio_path_release(tuple);
            free(tuple);
            gop_free(gop, OP_DESTROY);
        }
    }

    gop_opque_free(q, OP_DESTROY);

    tbx_stdinarray_iter_destroy(it);
    lio_shutdown();

    return(return_code);
}


