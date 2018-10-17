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

#define _log_module_index 188

#include <gop/gop.h>
#include <gop/mq.h>
#include <gop/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/log.h>

#include <lio/blacklist.h>
#include <lio/cache.h>
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
    char *path;
    int i, mode, n, nfailed, doit, start_option;
    os_fsck_iter_t *it;
    char *fname;
    gop_op_generic_t *gop;
    gop_op_status_t status;
    int ftype, err;

    if (argc < 2) {
        printf("\n");
        printf("os_fsck LIO_COMMON_OPTIONS [-y|-n] [-fix manual|delete|repair] path\n");
        lio_print_options(stdout);
        printf("    -y    - Automatically correct issues using the options provided. Default is to ask user if no option provided.\n");
        printf("    -n    - Don't correct any issues found just print them. Default is to ask user if no option provided\n");
        printf("    -fix  - How to handle issues. Default is manual. Can also be delete or repair.\n");
        printf("    path  - Path prefix to use\n");
        printf("\n");
        return(1);
    }

    lio_init(&argc, &argv);

    if (argc < 2) {
        printf("Missing path!\n");
        return(1);
    }

    mode = OS_FSCK_MANUAL;

    i = 1;
    do {
        start_option = i;
        if (strcmp(argv[i], "-fix") == 0) {
            i++;
            if (strcmp(argv[i], "delete") == 0) {
                mode = OS_FSCK_REMOVE;
            } else if (strcmp(argv[i], "repair") == 0) {
                mode = OS_FSCK_REPAIR;
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

    path = argv[i];

    info_printf(lio_ifd, 0, "--------------------------------------------------------------------\n");
    info_printf(lio_ifd, 0, "Using path=%s and mode=%d (%d=manual, %d=delete, %d=repair)\n", path, mode, OS_FSCK_MANUAL, OS_FSCK_REMOVE, OS_FSCK_REPAIR);
    info_printf(lio_ifd, 0, "Possible error states: %d=missing attr, %d=missing object\n", OS_FSCK_MISSING_ATTR, OS_FSCK_MISSING_OBJECT);
    info_printf(lio_ifd, 0, "--------------------------------------------------------------------\n");
    tbx_info_flush(lio_ifd);

    n = 0;
    nfailed = 0;
    it = os_create_fsck_iter(lio_gc->os, lio_gc->creds, path, OS_FSCK_MANUAL);  //** WE use reolve to clean up so we can see the problem objects
    while ((err = os_next_fsck(lio_gc->os, it, &fname, &ftype)) != OS_FSCK_GOOD) {
        info_printf(lio_ifd, 0, "err:%d  type:%d  object:%s\n", err, ftype, fname);
        if (err == OS_FSCK_ERROR) {  //** Internal error so abort!
            info_printf(lio_ifd, 0, "Internal FSCK error! Aborting!\n");
            break;
        }

        if (repair_mode < 2) {
            if (mode != OS_FSCK_MANUAL) {
                doit = (repair_mode == 1) ? 1 : ask_user();
                if (doit == 1) {
                    gop = os_fsck_object(lio_gc->os, lio_gc->creds, fname, ftype, mode);
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

    os_destroy_fsck_iter(lio_gc->os, it);

    info_printf(lio_ifd, 0, "--------------------------------------------------------------------\n");
    info_printf(lio_ifd, 0, "Problem objects: %d  Repair Failed count: %d\n", n, nfailed);
    info_printf(lio_ifd, 0, "--------------------------------------------------------------------\n");

    lio_shutdown();

    return(0);
}

