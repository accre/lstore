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

#define _log_module_index 209

#include <gop/gop.h>
#include <gop/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/log.h>
#include <tbx/stdinarray_iter.h>
#include <lio/ex3.h>
#include <lio/lio.h>
#include <lio/os.h>

int repair_mode = 0;

int ask_user() {
    char ans[20];

    for (;;) {
        info_printf(lio_ifd, 0, "Repair(y/n/ALL)?");
        if (fgets(ans, sizeof(ans), stdin) != NULL) {
            if (strcasecmp("y\n", ans) == 0) {
                return(1);
            } else if (strcasecmp("n\n", ans) == 0) {
                return(0);
            } else if (strcasecmp("ALL\n", ans) == 0) {
                repair_mode = 1;  //** Change the global repair mode as well
                return(1);
            }
        }
    }

    return(0);
}

int main(int argc, char **argv)
{
    int i, start_option, start_index, return_code, doit;
    lio_fsck_repair_t owner_mode, exnode_mode, size_mode;
    lio_fsck_iter_t *it;
    tbx_stdinarray_iter_t *it_args;
    char *owner;
    char *fname;
    char *path;
    gop_op_generic_t *gop;
    gop_op_status_t status;
    lio_path_tuple_t tuple;
    int ftype, err;
    ex_off_t n, nfailed, checked;

    if (argc < 2) {
        printf("\n");
        printf("lio_fsck LIO_COMMON_OPTIONS [-y|-n] [-o parent|manual|delete|user valid_user]  [-ex parent|manual|delete] [-s manual|repair] path_1 .. path_N\n");
        lio_print_options(stdout);
        lio_print_path_options(stdout);
        printf("    -y                 - Automatically correct issues using the options provided. Default is to ask user if no option provided.\n");
        printf("    -n                 - Don't correct any issues found just print them. Default is to ask user if no option provided.\n");
        printf("    -o                 - How to handle missing system.owner issues.  Default is manual.\n");
        printf("                            parent - Make the object owner the same as the parent directory.\n");
        printf("                            manual - Do nothing.  Leave the owner as missing.\n");
        printf("                            delete - Remove the object\n");
        printf("                            user valid_user - Make the provided user the object owner.\n");
        printf("    -ex                - How to handle missing exnode issues.  Default is manual.\n");
        printf("                            parent - Create an empty exnode using the parent exnode.\n");
        printf("                            manual - Do nothing.  Leave the exnode as missing or blank.\n");
        printf("                            delete - Remove the object\n");
        printf("    -s                 - How to handle missing exnode size.  Default is repair.\n");
        printf("                            manual - Do nothing.  Leave the size missing.\n");
        printf("                            repair - If the exnode existst load it and determine the size.\n");
        printf("    path               - Path prefix to use\n");
        printf("\n");
        return(1);
    }

    lio_init(&argc, &argv);

    if (argc < 2) {
        printf("Missing path!\n");
        return(1);
    }

    return_code = 0;
    owner_mode = LIO_FSCK_MANUAL;
    owner = NULL;
    exnode_mode = 0;
    size_mode = 0;
    i=1;
    do {
        start_option = i;

        if (strcmp(argv[i], "-o") == 0) {    //** How to handle owner problems
            i++;
            if (strcmp(argv[i], "manual") == 0) {
                owner_mode = LIO_FSCK_MANUAL;
            } else if (strcmp(argv[i], "parent") == 0) {
                owner_mode = LIO_FSCK_PARENT;
            } else if (strcmp(argv[i], "delete") == 0) {
                owner_mode = LIO_FSCK_DELETE;
            } else if (strcmp(argv[i], "user") == 0) {
                owner_mode = LIO_FSCK_USER;
                i++;
                owner = argv[i];
            }
            i++;
        } else if (strcmp(argv[i], "-ex") == 0) {  //** How to handle exnode issues
            i++;
            if (strcmp(argv[i], "manual") == 0) {
                exnode_mode = LIO_FSCK_MANUAL;
            } else if (strcmp(argv[i], "parent") == 0) {
                exnode_mode = LIO_FSCK_PARENT;
            } else if (strcmp(argv[i], "delete") == 0) {
                exnode_mode = LIO_FSCK_DELETE;
            }
            i++;
        } else if (strcmp(argv[i], "-s") == 0) {   //** How to handle size problems
            i++;
            if (strcmp(argv[i], "manual") == 0) {
                size_mode = LIO_FSCK_MANUAL;
            } else if (strcmp(argv[i], "repair") == 0) {
                size_mode = LIO_FSCK_SIZE_REPAIR;
            }
            i++;
        } else if (strcmp(argv[i], "-y") == 0) {   //** Auto fix issues
            i++;
            repair_mode = 1;
        } else if (strcmp(argv[i], "-n") == 0) {   //** Don't fix anything
            i++;
            repair_mode = 2;
        }
    } while ((start_option < i) && (i<argc));
    start_index = i;

    if (i>=argc) {
        fprintf(stderr, "Missing directory!\n");
        return(EINVAL);
    }

    info_printf(lio_ifd, 0, "--------------------------------------------------------------------\n");
    if (owner_mode == LIO_FSCK_USER) {
        info_printf(lio_ifd, 0, "owner_mode=%d (%s) exnode_mode=%d size_mode=%d (%d=manual, %d=parent, %d=delete, %d=user, %d=repair)\n",
                    owner_mode, owner, exnode_mode, size_mode, LIO_FSCK_MANUAL, LIO_FSCK_PARENT, LIO_FSCK_DELETE, LIO_FSCK_USER, LIO_FSCK_SIZE_REPAIR);
    } else {
        info_printf(lio_ifd, 0, "owner_mode=%d exnode_mode=%d size_mode=%d (%d=manual, %d=parent, %d=delete, %d=user, %d=repair)\n",
                    owner_mode, exnode_mode, size_mode, LIO_FSCK_MANUAL, LIO_FSCK_PARENT, LIO_FSCK_DELETE, LIO_FSCK_USER, LIO_FSCK_SIZE_REPAIR);
    }
    info_printf(lio_ifd, 0, "Possible error states: %d=missing owner, %d=missing exnode, %d=missing size, %d=missing inode\n", LIO_FSCK_MISSING_OWNER, LIO_FSCK_MISSING_EXNODE, LIO_FSCK_MISSING_EXNODE_SIZE, LIO_FSCK_MISSING_INODE);
    info_printf(lio_ifd, 0, "--------------------------------------------------------------------\n");
    tbx_info_flush(lio_ifd);

    n = 0;
    nfailed = 0;
    checked = 0;
    exnode_mode = exnode_mode | size_mode;
    it_args = tbx_stdinarray_iter_create(argc-start_index, (const char **)(argv+start_index));
    while ((path = tbx_stdinarray_iter_next(it_args)) != NULL) {
        //** Create the simple path iterator
        tuple = lio_path_resolve(lio_gc->auto_translate, path);
        if (tuple.is_lio < 0) {  //** Mangled path
            fprintf(stderr, "Unable to resolve path: %s\n", path);
            free(path);
            return_code = EINVAL;
            continue;
        }
        free(path);

        it = lio_create_fsck_iter(tuple.lc, tuple.creds, tuple.path, LIO_FSCK_MANUAL, NULL, LIO_FSCK_MANUAL);  //** WE use resolve to clean up so we can see the problem objects
        while ((err = lio_next_fsck(tuple.lc, it, &fname, &ftype)) != LIO_FSCK_FINISHED) {
            info_printf(lio_ifd, 0, "err:%d  type:%d  object:%s\n", err, ftype, fname);
            if (repair_mode < 2) {
                doit = (repair_mode == 1) ? 1 : ask_user();
                if (doit == 1) {
                    if ((owner_mode != LIO_FSCK_MANUAL) || (exnode_mode != LIO_FSCK_MANUAL)) {
                        gop = lio_fsck_gop(tuple.lc, tuple.creds, fname, ftype, owner_mode, owner, exnode_mode);
                        gop_waitany(gop);
                        status = gop_get_status(gop);
                        gop_free(gop, OP_DESTROY);
                        if (status.error_code != OS_FSCK_GOOD) nfailed++;
                        info_printf(lio_ifd, 0, "    resolve:%d  object:%s\n", status.error_code, fname);
                    }
                }
            }
            free(fname);
            fname = NULL;
            n++;
        }

        checked += lio_fsck_visited_count(tuple.lc, it);
        lio_destroy_fsck_iter(tuple.lc, it);
        lio_path_release(&tuple);
    }

    info_printf(lio_ifd, 0, "--------------------------------------------------------------------\n");
    info_printf(lio_ifd, 0, "Problem objects: " XOT "  Repair Failed count: " XOT " Processed: " XOT "\n", n, nfailed, checked);
    info_printf(lio_ifd, 0, "--------------------------------------------------------------------\n");

    tbx_stdinarray_iter_destroy(it_args);
    lio_shutdown();

    return(return_code);
}

