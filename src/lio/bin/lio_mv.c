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

#define _log_module_index 202

#include <gop/gop.h>
#include <gop/opque.h>
#include <gop/tp.h>
#include <gop/types.h>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/log.h>
#include <tbx/random.h>
#include <tbx/type_malloc.h>

#include <lio/lio.h>
#include <lio/os.h>

int max_spawn;

typedef struct {
    lio_path_tuple_t src_tuple;
    lio_path_tuple_t dest_tuple;
    lio_os_regex_table_t *regex;
    int dest_type;
} mv_t;

//*************************************************************************
// mv_fn - Actual mv function.  Moves a regex to a dest *dir*
//*************************************************************************

gop_op_status_t mv_fn(void *arg, int id)
{
    mv_t *mv = (mv_t *)arg;
    os_object_iter_t *it;
    int ftype, prefix_len, slot, count, nerr;
    char dname[OS_PATH_MAX];
    char **src_fname;
    gop_op_generic_t *gop;
    gop_opque_t *q;
    gop_op_status_t status;

    log_printf(15, "START src=%s dest=%s\n", mv->src_tuple.path, mv->dest_tuple.path);
    tbx_log_flush();

    status = gop_success_status;

    it = lio_create_object_iter(mv->src_tuple.lc, mv->src_tuple.creds, mv->regex, NULL, OS_OBJECT_ANY_FLAG, NULL, 0, NULL, 0);
    if (it == NULL) {
        info_printf(lio_ifd, 0, "ERROR: Failed with object_iter creation src_path=%s\n", mv->src_tuple.path);
        return(gop_failure_status);
    }

    tbx_type_malloc_clear(src_fname, char *, max_spawn);

    q = gop_opque_new();
    nerr = 0;
    slot = 0;
    count = 0;
//  tweak = (strcmp(mv->dest_tuple.path, "/") == 0) ? 1 : 0;  //** Tweak things for the root path
    while ((ftype = lio_next_object(mv->src_tuple.lc, it, &src_fname[slot], &prefix_len)) > 0) {
        snprintf(dname, OS_PATH_MAX, "%s/%s", mv->dest_tuple.path, &(src_fname[slot][prefix_len+1]));
        gop = lio_move_op(mv->src_tuple.lc, mv->src_tuple.creds, src_fname[slot], dname);
        gop_set_myid(gop, slot);
        log_printf(0, "gid=%d i=%d sname=%s dname=%s\n", gop_id(gop), slot, src_fname[slot], dname);
        gop_opque_add(q, gop);

        count++;

        if (count >= max_spawn) {
            gop = opque_waitany(q);
            slot = gop_get_myid(gop);
            status = gop_get_status(gop);
            if (status.op_status != OP_STATE_SUCCESS) {
                nerr++;
                info_printf(lio_ifd, 0, "Failed with path %s\n", src_fname[slot]);
            }
            free(src_fname[slot]);
            gop_free(gop, OP_DESTROY);
        } else {
            slot = count;
        }
    }

    lio_destroy_object_iter(mv->src_tuple.lc, it);

    while ((gop = opque_waitany(q)) != NULL) {
        slot = gop_get_myid(gop);
        log_printf(15, "slot=%d fname=%s\n", slot, src_fname[slot]);
        if (status.op_status != OP_STATE_SUCCESS) {
            nerr++;
            info_printf(lio_ifd, 0, "Failed with path %s\n", src_fname[slot]);
        }
        free(src_fname[slot]);
        gop_free(gop, OP_DESTROY);
    }

    gop_opque_free(q, OP_DESTROY);

    free(src_fname);

    status = gop_success_status;
    if (nerr > 0) {
        status.op_status = OP_STATE_FAILURE;
        status.error_code = nerr;
    }
    return(status);
}

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int i, j, start_index, start_option, n_paths;
    unsigned int ui;
    char *fname;
    mv_t *flist;
    gop_op_generic_t *gop;
    gop_opque_t *q;
    lio_path_tuple_t dtuple;
    int err, dtype;
    gop_op_status_t status;

    if (argc < 2) {
        printf("\n");
        printf("lio_mv LIO_COMMON_OPTIONS src_path1 .. src_pathN dest_path\n");
        lio_print_options(stdout);
        printf("\n");
        printf("    src_path1 .. src_pathN - Source path glob to move\n");
        printf("    dest_path              - Destination directory or file name\n");
        return(1);
    }

    lio_init(&argc, &argv);

    //*** Parse the args
    i=1;
    do {
        start_option = i;

    } while ((start_option < i) && (i<argc));
    start_index = i;

    //** Make the dest tuple
    dtuple = lio_path_resolve(lio_gc->auto_translate, argv[argc-1]);

    if (i>=argc) {
        info_printf(lio_ifd, 0, "Missing directory!\n");
        return(2);
    }

    //** Get the dest filetype/exists
    dtype = lio_exists(dtuple.lc, dtuple.creds, dtuple.path);


    //** Create the simple path iterator
    n_paths = argc - start_index - 1;
    log_printf(15, "n_paths=%d argc=%d si=%d dtype=%d\n", n_paths, argc, start_index, dtype);

    tbx_type_malloc_clear(flist, mv_t, n_paths);

    for (i=0; i<n_paths; i++) {
        flist[i].src_tuple = lio_path_resolve(lio_gc->auto_translate, argv[i+start_index]);
        flist[i].regex = lio_os_path_glob2regex(flist[i].src_tuple.path);
        flist[i].dest_tuple = dtuple;
        flist[i].dest_type = dtype;
        if (flist[i].src_tuple.lc != dtuple.lc) {
            info_printf(lio_ifd, 0, "Source(%s) and dest(%s) configs must match!\n", flist[i].src_tuple.lc->section_name, dtuple.lc->section_name);
        }
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
            if (lio_os_regex_is_fixed(flist[0].regex) == 0) {  //** Uh oh we have a wildcard with a single file dest
                info_printf(lio_ifd, 0, "ERROR: Single wildcard path(%s) selected but the dest(%s) is a file or doesn't exist!\n", flist[0].src_tuple.path, dtuple.path);
                goto finished;
            }
        }

        log_printf(15, "2222222222222222 fixed=%d exp=%s\n", lio_os_regex_is_fixed(flist[0].regex), flist[0].regex->regex_entry[0].expression);
        tbx_log_flush();

        //**if it's a fixed src with a dir dest we skip and use the mv_fn routines
        if ((lio_os_regex_is_fixed(flist[0].regex) == 1) && ((dtype == 0) || ((dtype & OS_OBJECT_FILE_FLAG) > 0))) {
            //** IF we made it here we have a simple mv but we need to preserve the dest file if things go wrong
            fname = NULL;
            if ((dtype & OS_OBJECT_FILE_FLAG) > 0) { //** Existing file so rename it for backup
                tbx_type_malloc(fname, char, strlen(dtuple.path) + 40);
                tbx_random_get_bytes(&ui, sizeof(ui));  //** MAke the random name
                sprintf(fname, "%s.mv.%ud", dtuple.path, ui);
                err = gop_sync_exec(lio_move_op(dtuple.lc, dtuple.creds, flist[0].src_tuple.path, fname));
                if (err != OP_STATE_SUCCESS) {
                    info_printf(lio_ifd, 0, "ERROR renaming dest(%s) to %s!\n", dtuple.path, fname);
                    free(fname);
                    goto finished;
                }
            }

            log_printf(15, "333333333333333333\n");
            tbx_log_flush();

            //** Now do the simple mv
            err = gop_sync_exec(lio_move_op(dtuple.lc, dtuple.creds, flist[0].src_tuple.path, dtuple.path));
            if (err != OP_STATE_SUCCESS) {
                info_printf(lio_ifd, 0, "ERROR renaming dest(%s) to %s!\n", dtuple.path, fname);

                //** Mv the original back due to the error
                if (fname != NULL) err = gop_sync_exec(lio_move_op(dtuple.lc, dtuple.creds, fname, dtuple.path));
                goto finished;
            }

            log_printf(15, "4444444444444444444\n");
            tbx_log_flush();

            //** Clean up by removing the original dest if needed
            if (fname != NULL) {
                err = gop_sync_exec(lio_remove_op(dtuple.lc, dtuple.creds, fname, NULL, dtype));
                if (err != OP_STATE_SUCCESS) {
                    info_printf(lio_ifd, 0, "ERROR removing temp dest(%s)!\n", fname);
                    free(fname);
                    goto finished;
                }
                free(fname);
            }

            log_printf(15, "55555555555555555\n");
            tbx_log_flush();

            goto finished;
        }
    }



    //** IF we made it here we have mv's to a directory
    max_spawn = lio_parallel_task_count / n_paths;
    if (max_spawn <= 0) max_spawn = 1;

    q = gop_opque_new();
    opque_start_execution(q);
    for (i=0; i<n_paths; i++) {
        gop = gop_tp_op_new(lio_gc->tpc_unlimited, NULL, mv_fn, (void *)&(flist[i]), NULL, 1);
        gop_set_myid(gop, i);
        log_printf(0, "gid=%d i=%d fname=%s\n", gop_id(gop), i, flist[i].src_tuple.path);
        gop_opque_add(q, gop);

        if (gop_opque_tasks_left(q) > lio_parallel_task_count) {
            gop = opque_waitany(q);
            j = gop_get_myid(gop);
            status = gop_get_status(gop);
            if (status.op_status != OP_STATE_SUCCESS) info_printf(lio_ifd, 0, "Failed with path %s\n", flist[j].src_tuple.path);
            gop_free(gop, OP_DESTROY);
        }
    }

    err = opque_waitall(q);
    if (err != OP_STATE_SUCCESS) {
        while ((gop = opque_get_next_failed(q)) != NULL) {
            j = gop_get_myid(gop);
            info_printf(lio_ifd, 0, "Failed with path %s\n", flist[j].src_tuple.path);
        }
    }

    gop_opque_free(q, OP_DESTROY);


finished:
    lio_path_release(&dtuple);
    for(i=0; i<n_paths; i++) {
        lio_path_release(&(flist[i].src_tuple));
        lio_os_regex_table_destroy(flist[i].regex);
    }

    free(flist);
    lio_shutdown();

    return(0);
}

