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

int main(int argc, char **argv)
{
    char *path;
    int i, mode, n, nfailed;
    os_fsck_iter_t *it;
    char *fname;
    gop_op_generic_t *gop;
    gop_op_status_t status;
    int ftype, err;

    if (argc < 2) {
        printf("\n");
        printf("os_fsck LIO_COMMON_OPTIONS [-fix manual|delete|repair] path\n");
        lio_print_options(stdout);
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
    if (strcmp(argv[i], "-fix") == 0) {
        i++;
        if (strcmp(argv[i], "delete") == 0) {
            mode = OS_FSCK_REMOVE;
        } else if (strcmp(argv[i], "repair") == 0) {
            mode = OS_FSCK_REPAIR;
        }
        i++;
    }

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

        if (mode != OS_FSCK_MANUAL) {
            gop = os_fsck_object(lio_gc->os, lio_gc->creds, fname, ftype, mode);
            gop_waitany(gop);
            status = gop_get_status(gop);
            gop_free(gop, OP_DESTROY);
            if (status.error_code != OS_FSCK_GOOD) nfailed++;
            info_printf(lio_ifd, 0, "    resolve:%d  object:%s\n", status.error_code, fname);
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

