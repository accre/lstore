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

#define _log_module_index 197

#include <gop/gop.h>
#include <gop/opque.h>
#include <gop/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/log.h>
#include <tbx/type_malloc.h>

#include <lio/lio.h>
#include <lio/os.h>


//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int i, j, n, err, rg_mode, start_index, start_option;
    opque_t *q;
    op_generic_t *gop;
    op_status_t status;
    os_regex_table_t **rpath;
    lio_path_tuple_t *flist, tuple;
    os_regex_table_t *rp_single, *ro_single;
    int recurse_depth = 0;
    int obj_types = OS_OBJECT_ANY;

    if (argc < 2) {
        printf("\n");
        printf("lio_rm LIO_COMMON_OPTIONS [-rd recurse_depth] [-t object_types] LIO_PATH_OPTIONS\n");
        lio_print_options(stdout);
        lio_print_path_options(stdout);
        printf("    -rd recurse_depth  - Max recursion depth on directories. Defaults to %d.\n", recurse_depth);
        printf("    -t  object_types   - Types of objects to list bitwise OR of 1=Files, 2=Directories, 4=symlink, 8=hardlink.  Default is %d.\n", obj_types);
        return(1);
    }

    lio_init(&argc, &argv);


    //*** Parse the args
    rp_single = ro_single = NULL;

    rg_mode = lio_parse_path_options(&argc, argv, lio_gc->auto_translate, &tuple, &rp_single, &ro_single);

    i=1;
    do {
        start_option = i;

        if (strcmp(argv[i], "-rd") == 0) { //** Recurse depth
            i++;
            recurse_depth = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-t") == 0) {  //** Object types
            i++;
            obj_types = atoi(argv[i]);
            i++;
        }

    } while ((start_option - i < 0) && (i<argc));
    start_index = i;


    if (rg_mode == 1) {  //** Got an explicit R/G path set
        err = gop_sync_exec(lio_remove_regex_op(tuple.lc, tuple.creds, rp_single, ro_single, obj_types, recurse_depth, lio_parallel_task_count));
        if (err != OP_STATE_SUCCESS) info_printf(lio_ifd, 0, "Error occured with remove\n");

        if (rp_single != NULL) lio_os_regex_table_destroy(rp_single);
        if (ro_single != NULL) lio_os_regex_table_destroy(ro_single);
        lio_path_release(&tuple);
    }

    //** Check if we have more things to remove
    if (i>=argc) {
        if (rp_single == NULL) {
            info_printf(lio_ifd, 0, "Missing directory!\n");
            return(2);
        }

        goto finished;
    }


    //** Spawn the tasks
    n = argc - start_index;
    tbx_type_malloc(flist, lio_path_tuple_t, n);
    tbx_type_malloc(rpath, os_regex_table_t *, n);

    q = gop_opque_new();
    opque_start_execution(q);
    for (i=0; i<n; i++) {
        flist[i] = lio_path_resolve(lio_gc->auto_translate, argv[i+start_index]);
        rpath[i] = lio_os_path_glob2regex(flist[i].path);
        gop = lio_remove_regex_op(flist[i].lc, flist[i].creds, rpath[i], NULL, obj_types, recurse_depth, lio_parallel_task_count);
        gop_set_myid(gop, i);
        log_printf(0, "gid=%d i=%d fname=%s\n", gop_id(gop), i, flist[i].path);
        gop_opque_add(q, gop);

        if (opque_tasks_left(q) > lio_parallel_task_count) {
            gop = opque_waitany(q);
            j = gop_get_myid(gop);
            status = gop_get_status(gop);
            if (status.op_status != OP_STATE_SUCCESS) info_printf(lio_ifd, 0, "ERROR with %s\n", argv[j+start_index]);
            gop_free(gop, OP_DESTROY);
        }
    }

    err = opque_waitall(q);
    if (err != OP_STATE_SUCCESS) {
        while ((gop = opque_waitany(q)) != NULL) {
            j = gop_get_myid(gop);
            status = gop_get_status(gop);
            if (status.op_status != OP_STATE_SUCCESS) info_printf(lio_ifd, 0, "ERROR with %s\n", argv[j+start_index]);
            gop_free(gop, OP_DESTROY);
        }
    }

    gop_opque_free(q, OP_DESTROY);

    for(i=0; i<n; i++) {
        lio_path_release(&(flist[i]));
        lio_os_regex_table_destroy(rpath[i]);
    }

    free(flist);
    free(rpath);

finished:
    lio_shutdown();

    return(0);
}


