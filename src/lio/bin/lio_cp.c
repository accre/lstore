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
#include <tbx/stdinarray_iter.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>

#include <lio/ex3.h>
#include <lio/lio.h>
#include <lio/os.h>

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int i, start_index, start_option, n_paths, n_errors, return_code;
    int max_spawn, stype, sflag, dflag;
    int obj_types = OS_OBJECT_ANY_FLAG;
    ex_off_t bufsize;
    char ppbuf[64];
    char *path;
    tbx_stdinarray_iter_t *it;
    lio_cp_path_t *cp;
    lio_cp_file_t cpf;
    gop_op_generic_t *gop;
    gop_opque_t *q = NULL;
    lio_path_tuple_t dtuple;
    int dtype, recurse_depth;
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
        return(EINVAL);
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

    //** Make the iterator
    n_paths = argc-start_index;
    if (n_paths <= 0) n_paths = 1;
    max_spawn = lio_parallel_task_count / n_paths;
    if (max_spawn <= 0) max_spawn = 1;

    it = tbx_stdinarray_iter_create(argc-start_index, (const char **)(argv+start_index));
    path = tbx_stdinarray_iter_last(it);
    if (!path) {
        fprintf(stderr, "Unable to determine destination path!\n");
        return(EINVAL);
    }

    //** Make the dest tuple
    dtuple = lio_path_resolve(lio_gc->auto_translate, path);
    if (dtuple.is_lio < 0) {
        fprintf(stderr, "Unable to parse destination path: %s\n", argv[argc-1]);
        return(EINVAL);
    }
    free(path);

    if (i>=argc) {
        info_printf(lio_ifd, 0, "Missing directory!\n");
        return(EINVAL);
    }

    //** Get the dest filetype/exists
    if (dtuple.is_lio == 1) {
        dtype = lio_exists(dtuple.lc, dtuple.creds, dtuple.path);
    } else {
        dtype = lio_os_local_filetype(dtuple.path);
    }

    return_code = 0;
    q = gop_opque_new();
    opque_start_execution(q);
    status = gop_failure_status;

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
        tbx_type_malloc_clear(cp, lio_cp_path_t, 1);
        cp->src_tuple = lio_path_resolve(lio_gc->auto_translate, path);
        if (cp->src_tuple.is_lio == 0) {
            lio_path_local_make_absolute(&cp->src_tuple);
        } else if (cp->src_tuple.is_lio < 0) {   //** Can't parse path so skip
            fprintf(stderr, "Unable to parse source path: %s\n", path);
            free(path);
            free(cp);
            goto finished;
        }
        free(path);

        //**We'll be using one of these structuures depending on the type of copy
        cp->dest_tuple = dtuple;
        cp->dest_type = dtype;
        cp->path_regex = lio_os_path_glob2regex(cp->src_tuple.path);
        cp->recurse_depth = recurse_depth;
        cp->obj_types = obj_types;
        cp->max_spawn = max_spawn;
        cp->bufsize = bufsize;
        cp->slow = slow;

        memset(&cpf, 0, sizeof(cpf));
        cpf.src_tuple = cp->src_tuple;
        cpf.dest_tuple = cp->dest_tuple;
        cpf.bufsize = cp->bufsize;
        cpf.slow = cp->slow;
        cpf.rw_hints = NULL;

        if (lio_os_regex_is_fixed(cp->path_regex) == 1) {  //** Fixed source
            if (cp->src_tuple.is_lio == 1) {
                stype = lio_exists(cp->src_tuple.lc, cp->src_tuple.creds, cp->src_tuple.path);
            } else {
                stype = lio_os_local_filetype(cp->src_tuple.path);
            }

            if (dtype == 0) {                              //** Destination doesn't exist
                if (stype & OS_OBJECT_FILE_FLAG) {
                    status = lio_file_copy_op(&cpf, 0);
                } else if (stype & OS_OBJECT_DIR_FLAG) {
                    i = strlen(cp->src_tuple.path);
                    tbx_type_malloc(path, char, i+2+1);
                    if (cp->src_tuple.path[i] == '/') {
                        snprintf(path, i+2+1, "%s*", cp->src_tuple.path);
                    } else {
                        snprintf(path, i+2+1, "%s/*", cp->src_tuple.path);
                    }
                    free(cp->src_tuple.path);
                    cp->src_tuple.path = path;
                    lio_os_regex_table_destroy(cp->path_regex);
                    cp->path_regex = lio_os_path_glob2regex(cp->src_tuple.path);
                    cp->force_dest_create = 1;
                    status = lio_path_copy_op(cp, 0);
                }
            } else {                                        //** Destination already exists
                sflag = stype & (OS_OBJECT_FILE_FLAG|OS_OBJECT_DIR_FLAG);
                dflag = dtype & (OS_OBJECT_FILE_FLAG|OS_OBJECT_DIR_FLAG);
                if (dflag == sflag) {
                    if (dtype & OS_OBJECT_FILE_FLAG) {
                        status = lio_file_copy_op(&cpf, 0);
                    } else if (dtype & OS_OBJECT_DIR_FLAG) {
                        status = lio_path_copy_op(cp, 0);
                    }
                } else if (dtype & OS_OBJECT_DIR_FLAG) {
                    status = lio_path_copy_op(cp, 0);
                } else {
                    fprintf(stderr, "Source and destination files have incompatible types.\n");
                    return_code = EINVAL;
                    goto finished;
                }
            }
        } else {                //** Got a regex
            if (dtype & OS_OBJECT_DIR_FLAG) { //** Must exist and be a directory
                status = lio_path_copy_op(cp, 0);
            } else {  //** Wrong destination type
                fprintf(stderr, "ERROR: Single wildcard path(%s) selected but the dest(%s) is a file or doesn't exist!\n", cp->src_tuple.path, dtuple.path);
                return_code = EINVAL;
                goto finished;
            }
        }

        if (status.op_status != OP_STATE_SUCCESS) {
            fprintf(stderr, "ERROR with copy src=%s dest=%s\n", cp->src_tuple.path, dtuple.path);
        }
        lio_path_release(&cp->src_tuple);
        lio_os_regex_table_destroy(cp->path_regex);
        free(cp);
        if (status.op_status != OP_STATE_SUCCESS) {
            return_code = EIO;
            goto finished;
        }
    }

    while (1) {
        path = tbx_stdinarray_iter_next(it);
        if (path) {
            tbx_type_malloc_clear(cp, lio_cp_path_t, 1);
            cp->src_tuple = lio_path_resolve(lio_gc->auto_translate, path);
            if (cp->src_tuple.is_lio == 0) {
                lio_path_local_make_absolute(&cp->src_tuple);
            } else if (cp->src_tuple.is_lio < 0) {   //** CAn't parse path so skip
                fprintf(stderr, "Unable to parse source path: %s\n", path);
                free(path);
                free(cp);
                goto finished;
            }
            free(path);
            cp->dest_tuple = dtuple;
            cp->dest_type = dtype;
            cp->path_regex = lio_os_path_glob2regex(cp->src_tuple.path);
            cp->recurse_depth = recurse_depth;
            cp->obj_types = obj_types;
            cp->max_spawn = max_spawn;
            cp->bufsize = bufsize;
            cp->slow = slow;

            gop = gop_tp_op_new(lio_gc->tpc_unlimited, NULL, lio_path_copy_op, cp, NULL, 1);
            gop_set_private(gop, cp);
            gop_opque_add(q, gop);
        }

        if ((gop_opque_tasks_left(q) > lio_parallel_task_count) || (!path)) {
            gop = opque_waitany(q);
            if (!gop) break;
            cp = gop_get_private(gop);
            status = gop_get_status(gop);
            if (status.op_status != OP_STATE_SUCCESS) {
                fprintf(stderr, "Failed with path %s\n", cp->src_tuple.path);
                return_code = EIO;
                n_errors++;
            }
            lio_path_release(&cp->src_tuple);
            lio_os_regex_table_destroy(cp->path_regex);
            free(cp);
            gop_free(gop, OP_DESTROY);
        }
    }


finished:
    if (q) gop_opque_free(q, OP_DESTROY);
    lio_path_release(&dtuple);
    tbx_stdinarray_iter_destroy(it);

    if (n_errors > 0) fprintf(stderr, "Failed copying %d file(s)!\n", n_errors);

    lio_shutdown();

    return(return_code);
}

