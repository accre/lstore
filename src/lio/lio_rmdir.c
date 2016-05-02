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

#include <assert.h>
#include "assert_result.h"
#include "exnode.h"
#include "log.h"
#include "iniparse.h"
#include "type_malloc.h"
#include "thread_pool.h"
#include "lio.h"


//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int i, j, n, err, rg_mode;
    opque_t *q;
    op_generic_t *gop;
    op_status_t status;
    os_regex_table_t **rpath;
    lio_path_tuple_t *flist;
    os_regex_table_t *rp_single, *ro_single;
    lio_path_tuple_t tuple;

//printf("argc=%d\n", argc);
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

    rg_mode = lio_parse_path_options(&argc, argv, lio_gc->auto_translate, &tuple, &rp_single, &ro_single);

    //** This is the 1st dir to remove
    if (argv[1] == NULL) {
        info_printf(lio_ifd, 0, "Missing directory!\n");
        return(2);
    }

    //** Spawn the tasks
    n = argc - 1;
    type_malloc(flist, lio_path_tuple_t, n);
    type_malloc(rpath, os_regex_table_t *, n);

    q = new_opque();
    opque_start_execution(q);

    if (rg_mode == 1) {
        gop = gop_lio_remove_regex_object(tuple.lc, tuple.creds, rp_single, ro_single, OS_OBJECT_DIR, 0, lio_parallel_task_count);
        gop_set_myid(gop, -1);
        opque_add(q, gop);
    }


    for (i=0; i<n; i++) {
        flist[i] = lio_path_resolve(lio_gc->auto_translate, argv[i+1]);
        rpath[i] = os_path_glob2regex(flist[i].path);
        gop = gop_lio_remove_regex_object(flist[i].lc, flist[i].creds, rpath[i], NULL, OS_OBJECT_DIR, 0, lio_parallel_task_count);
        gop_set_myid(gop, i);
        log_printf(0, "gid=%d i=%d fname=%s\n", gop_id(gop), i, flist[i].path);
        opque_add(q, gop);

        if (opque_tasks_left(q) > lio_parallel_task_count) {
            gop = opque_waitany(q);
            j = gop_get_myid(gop);
            status = gop_get_status(gop);
            if (status.op_status != OP_STATE_SUCCESS) {
                if (j >= 0) {
                    info_printf(lio_ifd, 0, "Failed with directory %s\n", argv[j+1]);
                } else {
                    info_printf(lio_ifd, 0, "Failed with regex path\n");
                }
            }
            gop_free(gop, OP_DESTROY);
        }
    }

    err = opque_waitall(q);
    if (err != OP_STATE_SUCCESS) {
        while ((gop = opque_get_next_failed(q)) != NULL) {
            j = gop_get_myid(gop);
            if (j >= 0) {
                info_printf(lio_ifd, 0, "Failed with directory %s\n", argv[j+1]);
            } else {
                info_printf(lio_ifd, 0, "Failed with regex path\n");
            }
        }
    }

    opque_free(q, OP_DESTROY);

    if (rg_mode == 1) lio_path_release(&tuple);

    for(i=0; i<n; i++) {
        lio_path_release(&(flist[i]));
        os_regex_table_destroy(rpath[i]);
    }

    free(flist);
    free(rpath);

    lio_shutdown();

    return(0);
}


