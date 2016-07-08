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

#define _log_module_index 203

#include <gop/gop.h>
#include <gop/opque.h>
#include <gop/tp.h>
#include <gop/types.h>
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
//*************************************************************************

int main(int argc, char **argv)
{
    int i, start_index, start_option, n_paths, n_errors;
    int max_spawn;
    int obj_types = OS_OBJECT_ANY_FLAG;
    ex_off_t bufsize;
    char ppbuf[64];
    lio_cp_path_t *flist;
    lio_cp_file_t cpf;
    gop_op_generic_t *gop;
    gop_opque_t *q;
    lio_path_tuple_t dtuple;
    int err, dtype, recurse_depth;
    lio_copy_hint_t slow;
    gop_op_status_t status;

    recurse_depth = 10000;
    bufsize = 20*1024*1024;

    if (argc < 2) {
        printf("\n");
        printf("lio_cp LIO_COMMON_OPTIONS [-rd recurse_depth] [-b bufsize_mb] [-f] src_path1 .. src_pathN dest_path\n");
        lio_print_options(stdout);
        printf("\n");
        printf("    -rd recurse_depth  - Max recursion depth on directories. Defaults to %d\n", recurse_depth);
        printf("    -b bufsize         - Buffer size to use for *each* transfer. Units supported (Default=%s)\n", tbx_stk_pretty_print_int_with_scale(bufsize, ppbuf));
        printf("    -f                 - Force a slow or traditional copy by reading from the source and copying to the destination\n");
        printf("    src_path*          - Source path glob to copy\n");
        printf("    dest_path          - Destination file or directory\n");
        printf("\n");
        printf("*** NOTE: It's imperative that the user@host:/path..., @:/path..., etc    ***\n");
        printf("***   be used since this signifies where the files come from.             ***\n");
        printf("***   If no '@:' is used the path is assumed to reside on the local disk. ***\n");
        return(1);
    }

    lio_init(&argc, &argv);

    if (argc <= 1) {
        info_printf(lio_ifd, 0, "Missing Source and destination!\n");
        return(1);
    }


    //*** Parse the args
    n_errors = 0;
    slow = LIO_COPY_DIRECT;
    i=1;
    do {
        start_option = i;
        if (strcmp(argv[i], "-f") == 0) {  //** Force a slow copy
            i++;
            slow = LIO_COPY_INDIRECT;
        } else if (strcmp(argv[i], "-rd") == 0) { //** Recurse depth
            i++;
            recurse_depth = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-b") == 0) {  //** Get the buffer size
            i++;
            bufsize = tbx_stk_string_get_integer(argv[i]);
            i++;
        }

    } while ((start_option < i) && (i<argc));
    start_index = i;


    //** Make the dest tuple
    dtuple = lio_path_resolve(lio_gc->auto_translate, argv[argc-1]);

    if (i>=argc) {
        info_printf(lio_ifd, 0, "Missing directory!\n");
        return(2);
    }

    //** Get the dest filetype/exists
    if (dtuple.is_lio == 1) {
        dtype = lio_exists(dtuple.lc, dtuple.creds, dtuple.path);
    } else {
        dtype = lio_os_local_filetype(dtuple.path);
    }

    //** Create the simple path iterator
    n_paths = argc - start_index - 1;
    log_printf(15, "n_paths=%d argc=%d si=%d dtype=%d\n", n_paths, argc, start_index, dtype);

    if (n_paths <= 0) {
        info_printf(lio_ifd, 0, "Missing destination!\n");
        return(1);
    }

    tbx_type_malloc_clear(flist, lio_cp_path_t, n_paths);

    max_spawn = lio_parallel_task_count / n_paths;
    if (max_spawn <= 0) max_spawn = 1;

    for (i=0; i<n_paths; i++) {
        flist[i].src_tuple = lio_path_resolve(lio_gc->auto_translate, argv[i+start_index]);
        if (flist[i].src_tuple.is_lio == 0) lio_path_local_make_absolute(&(flist[i].src_tuple));
        flist[i].dest_tuple = dtuple;
        flist[i].dest_type = dtype;
        flist[i].path_regex = lio_os_path_glob2regex(flist[i].src_tuple.path);
        flist[i].recurse_depth = recurse_depth;
        flist[i].obj_types = obj_types;
        flist[i].max_spawn = max_spawn;
        flist[i].bufsize = bufsize;
        flist[i].slow = slow;
    }

    //** Do some sanity checking and handle the simple case directly
    //** If multiple paths then the dest must be a dir and it has to exist
    if ((n_paths > 1) && ((dtype & OS_OBJECT_DIR_FLAG) == 0)) {
        if (dtype == 0) {
            info_printf(lio_ifd, 0, "ERROR: Multiple paths selected but the dest(%s) doesn't exist!\n", dtuple.path);
        } else {
            info_printf(lio_ifd, 0, "ERROR: Multiple paths selected but the dest(%s) isn't a directory!\n", dtuple.path);
        }
        goto finished;
    } else if (n_paths == 1) {
        log_printf(15, "11111111\n");
        tbx_log_flush();
        if (((dtype & OS_OBJECT_FILE_FLAG) > 0) || (dtype == 0)) {  //** Single path and dest is an existing file or doesn't exist
            if (lio_os_regex_is_fixed(flist[0].path_regex) == 0) {  //** Uh oh we have a wildcard with a single file dest
                info_printf(lio_ifd, 0, "ERROR: Single wildcard path(%s) selected but the dest(%s) is a file or doesn't exist!\n", flist[0].src_tuple.path, dtuple.path);
                goto finished;
            }
        }

        log_printf(15, "2222222222222222 fixed=%d exp=%s dtype=%d\n", lio_os_regex_is_fixed(flist[0].path_regex), flist[0].path_regex->regex_entry[0].expression, dtype);
        tbx_log_flush();

        //**if it's a fixed src with a dir dest we skip and use the cp_fn routines
        if ((lio_os_regex_is_fixed(flist[0].path_regex) == 1) && ((dtype == 0) || ((dtype & OS_OBJECT_FILE_FLAG) > 0))) {
            //** IF we made it here we have a simple cp
            cpf.src_tuple = flist[0].src_tuple; //c->src_tuple.path = fname;
            cpf.dest_tuple = flist[0].dest_tuple; //c->dest_tuple.path = strdup(dname);
            cpf.bufsize = flist[0].bufsize;
            cpf.slow = flist[0].slow;
            cpf.rw_hints = NULL;
            status = lio_file_copy_op(&cpf, 0);

            if (status.op_status != OP_STATE_SUCCESS) {
                info_printf(lio_ifd, 0, "ERROR: with copy src=%s  dest=%s\n", flist[0].src_tuple.path, dtuple.path);
                if (status.op_status != OP_STATE_SUCCESS) n_errors++;
                goto finished;
            }
            log_printf(15, "333333333333333333\n");
            tbx_log_flush();

            goto finished;
        }
    }

    q = gop_opque_new();
    opque_start_execution(q);
    for (i=0; i<n_paths; i++) {
        gop = gop_tp_op_new(lio_gc->tpc_unlimited, NULL, lio_path_copy_op, (void *)&(flist[i]), NULL, 1);
        gop_set_myid(gop, i);
        log_printf(0, "gid=%d i=%d fname=%s\n", gop_id(gop), i, flist[i].src_tuple.path);

        gop_opque_add(q, gop);
        log_printf(0, "bufsize=" XOT "\n", flist[i].bufsize);

        if (gop_opque_tasks_left(q) > lio_parallel_task_count) {
            gop = opque_waitany(q);
            status = gop_get_status(gop);
            if (status.op_status != OP_STATE_SUCCESS) n_errors++;

            gop_free(gop, OP_DESTROY);
        }
    }

    err = opque_waitall(q);
    if (err != OP_STATE_SUCCESS) {
        while ((gop = opque_get_next_failed(q)) != NULL) {
            status = gop_get_status(gop);
            if (status.op_status != OP_STATE_SUCCESS) n_errors++;
        }
    }

    gop_opque_free(q, OP_DESTROY);


finished:
    lio_path_release(&dtuple);
    for(i=0; i<n_paths; i++) {
        lio_path_release(&(flist[i].src_tuple));
        lio_os_regex_table_destroy(flist[i].path_regex);
    }

    free(flist);

    if (n_errors > 0) info_printf(lio_ifd, 0, "Failed copying %d file(s)!\n", n_errors);

    lio_shutdown();

    return((n_errors == 0) ? 0 : 1);
}

