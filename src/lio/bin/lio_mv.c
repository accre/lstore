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
#include <tbx/assert_result.h>
#include <tbx/log.h>
#include <tbx/random.h>
#include <tbx/stdinarray_iter.h>
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
        fprintf(stderr, "ERROR: Failed with object_iter creation src_path=%s\n", mv->src_tuple.path);
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
        gop = lio_move_object_gop(mv->src_tuple.lc, mv->src_tuple.creds, src_fname[slot], dname);
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
    int i, start_index, start_option, n_paths, return_code;
    char *path;
    mv_t *mv;
    gop_op_generic_t *gop;
    gop_opque_t *q;
    lio_path_tuple_t dtuple;
    tbx_stdinarray_iter_t *it;
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

    //** Make the path iterator
    n_paths = argc-start_index;
    max_spawn = lio_parallel_task_count / n_paths;
    if (max_spawn <= 0) max_spawn = 1;
    it = tbx_stdinarray_iter_create(argc-start_option, (const char **)(argv+start_index));

    //** Make the dest tuple
    path = tbx_stdinarray_iter_last(it);
    if (!path) {
        fprintf(stderr, "Unable to parse destination path\n");
        return(EINVAL);
    }
    dtuple = lio_path_resolve(lio_gc->auto_translate, path);
    if (dtuple.is_lio < 0) {
        fprintf(stderr, "Unable to parse path: %s\n", path);
        return(EINVAL);
    }
    free(path);

    if (i>=argc) {
        info_printf(lio_ifd, 0, "Missing directory!\n");
        return(2);
    }

    //** Get the dest filetype/exists
    dtype = lio_exists(dtuple.lc, dtuple.creds, dtuple.path);

    return_code = 0;
    q = gop_opque_new();
    opque_start_execution(q);

    //** Do some sanity checking and handle the simple case directly
    //** If multiple paths then the dest must be a dir and it has to exist
    path = tbx_stdinarray_iter_peek(it, 2);
    if (path && ((dtype & OS_OBJECT_DIR_FLAG) == 0)) {
        if (dtype == 0) {
            fprintf(stderr, "ERROR: Multiple paths selected but the dest(%s) doesn't exist!\n", dtuple.path);
        } else {
            fprintf(stderr, "ERROR: Multiple paths selected but the dest(%s) isn't a directory!\n", dtuple.path);
        }
        return_code = EINVAL;
        goto finished;
    } else if (!path) {
        path = tbx_stdinarray_iter_next(it);
        tbx_type_malloc_clear(mv, mv_t, 1);
        mv->src_tuple = lio_path_resolve(lio_gc->auto_translate, path);
        if (mv->src_tuple.is_lio < 0) {
            fprintf(stderr, "Unable to parse path: %s\n", path);
            return_code = EINVAL;
            free(path);
            free(mv);
            goto finished;
        }
        free(path);
        mv->regex = lio_os_path_glob2regex(mv->src_tuple.path);
        mv->dest_tuple = dtuple;
        mv->dest_type = dtype;
        if (((dtype & OS_OBJECT_FILE_FLAG) > 0) || (dtype == 0)) {  //** Single path and dest is an existing file or doesn't exist
            if (lio_os_regex_is_fixed(mv->regex) == 0) {  //** Uh oh we have a wildcard with a single file dest
                fprintf(stderr, "ERROR: Single wildcard path(%s) selected but the dest(%s) is a file or doesn't exist!\n", mv->src_tuple.path, dtuple.path);
                return_code = EINVAL;
                goto finished;
            }
        }

        //**if it's a fixed src with a dir dest we skip and use the mv_fn routines
        if ((lio_os_regex_is_fixed(mv->regex) == 1) && ((dtype == 0) || ((dtype & OS_OBJECT_FILE_FLAG) > 0))) {
            err = gop_sync_exec(lio_move_object_gop(dtuple.lc, dtuple.creds, mv->src_tuple.path, dtuple.path));
            if (err != OP_STATE_SUCCESS) {
                fprintf(stderr, "ERROR renaming %s to %s!\n", mv->src_tuple.path, dtuple.path);
                return_code = EINVAL;
                lio_path_release(&mv->src_tuple);
                free(mv);
                goto finished;
            }
        } else {
            gop = gop_tp_op_new(lio_gc->tpc_unlimited, NULL, mv_fn, mv, NULL, 1);
            gop_opque_add(q, gop);
        }
    }

    while (1) {
        path = tbx_stdinarray_iter_next(it);
        if (path) {
            tbx_type_malloc_clear(mv, mv_t, 1);
            mv->src_tuple = lio_path_resolve(lio_gc->auto_translate, path);
            if (mv->src_tuple.is_lio < 0) {
                fprintf(stderr, "Unable to parse path: %s\n", path);
                return_code = EINVAL;
                free(path);
                free(mv);
                continue;
            }
            free(path);
            mv->regex = lio_os_path_glob2regex(mv->src_tuple.path);
            mv->dest_tuple = dtuple;
            mv->dest_type = dtype;

            gop = gop_tp_op_new(lio_gc->tpc_unlimited, NULL, mv_fn, mv, NULL, 1);
            gop_set_private(gop, mv);
            gop_opque_add(q, gop);
        }

        if ((gop_opque_tasks_left(q) > lio_parallel_task_count) || (!path)) {
            gop = opque_waitany(q);
            if (!gop) break;
            mv = gop_get_private(gop);
            status = gop_get_status(gop);
            if (status.op_status != OP_STATE_SUCCESS) {
                fprintf(stderr, "Failed with path %s\n", mv->src_tuple.path);
                return_code = EINVAL;
            }
            lio_path_release(&mv->src_tuple);
            lio_os_regex_table_destroy(mv->regex);
            free(mv);
            gop_free(gop, OP_DESTROY);
        }
    }


finished:
    lio_path_release(&dtuple);
    tbx_stdinarray_iter_destroy(it);
    gop_opque_free(q, OP_DESTROY);

    lio_shutdown();

    return(return_code);
}

