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

#include <assert.h>
#include <tbx/assert_result.h>
#include "exnode.h"
#include <tbx/log.h>
#include <tbx/iniparse.h>
#include <tbx/type_malloc.h>
#include "thread_pool.h"
#include "lio.h"

char *exnode_data = NULL;

//*************************************************************************
// touch_fn - Actual touch function
//*************************************************************************

op_status_t touch_fn(void *arg, int id)
{
    lio_path_tuple_t *tuple = (lio_path_tuple_t *)arg;
    int ftype, err;
    op_status_t status;

    status = op_success_status;


    ftype = lio_exists(tuple->lc, tuple->creds, tuple->path);
    if (ftype != 0) { //** The file exists so just update the modified attribute
        err = gop_sync_exec(gop_lio_set_attr(tuple->lc, tuple->creds, tuple->path, NULL, "os.timestamp.system.modify_data", NULL, 0));
        if (err != OP_STATE_SUCCESS) {
            status.op_status = OP_STATE_FAILURE;
            status.error_code = 1;
        }
    } else {  //** New file so create the object
        err = gop_sync_exec(gop_lio_create_object(tuple->lc, tuple->creds, tuple->path, OS_OBJECT_FILE, exnode_data, NULL));
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
    int i, j, n, start_index, err, start_option;
    char *ex_fname;
    opque_t *q;
    op_generic_t *gop;
    op_status_t status;
    lio_path_tuple_t *flist;
    char *error_table[] = { "", "ERROR Failed to update modify timestamp", "ERROR creating file" };
    FILE *fd;

//printf("argc=%d\n", argc);
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

    } while (start_option < i);
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
            return(2);
        }
        fseek(fd, 0, SEEK_END);

        i = ftell(fd);
        tbx_type_malloc(exnode_data, char, i+1);
        exnode_data[i] = 0;

        fseek(fd, 0, SEEK_SET);
        if (fread(exnode_data, i, 1, fd) != 1) { //**
            printf("ERROR reading exnode from disk!\n");
            return(2);
        }
        fclose(fd);
    }

    if (exnode_data != NULL) free(exnode_data);

    //** Spawn the tasks
    n = argc - start_index;
    tbx_type_malloc(flist, lio_path_tuple_t, n);

    q = new_opque();
    opque_start_execution(q);
    for (i=0; i<n; i++) {
        flist[i] = lio_path_resolve(lio_gc->auto_translate, argv[i+start_index]);
        gop = new_thread_pool_op(lio_gc->tpc_unlimited, NULL, touch_fn, (void *)&(flist[i]), NULL, 1);
        gop_set_myid(gop, i);
        log_printf(0, "gid=%d i=%d fname=%s\n", gop_id(gop), i, flist[i].path);
        opque_add(q, gop);

        if (opque_tasks_left(q) > lio_parallel_task_count) {
            gop = opque_waitany(q);
            j = gop_get_myid(gop);
            status = gop_get_status(gop);
            if (status.op_status != OP_STATE_SUCCESS) info_printf(lio_ifd, 0, "Failed with file %s with: %s\n", argv[j+start_index], error_table[status.error_code]);
            gop_free(gop, OP_DESTROY);
        }
    }

    err = opque_waitall(q);
    if (err != OP_STATE_SUCCESS) {
        while ((gop = opque_get_next_failed(q)) != NULL) {
            j = gop_get_myid(gop);
            status = gop_get_status(gop);
            info_printf(lio_ifd, 0, "Failed with file %s with: %s\n", argv[j+start_index], error_table[status.error_code]);
        }
    }

    opque_free(q, OP_DESTROY);

    for(i=0; i<n; i++) {
        lio_path_release(&(flist[i]));
    }

    free(flist);

    lio_shutdown();

    return((err == OP_STATE_SUCCESS) ? 0: EIO);
}


