/*
Advanced Computing Center for Research and Education Proprietary License
Version 1.0 (April 2006)

Copyright (c) 2006, Advanced Computing Center for Research and Education,
 Vanderbilt University, All rights reserved.

This Work is the sole and exclusive property of the Advanced Computing Center
for Research and Education department at Vanderbilt University.  No right to
disclose or otherwise disseminate any of the information contained herein is
granted by virtue of your possession of this software except in accordance with
the terms and conditions of a separate License Agreement entered into with
Vanderbilt University.

THE AUTHOR OR COPYRIGHT HOLDERS PROVIDES THE "WORK" ON AN "AS IS" BASIS,
WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, TITLE, FITNESS FOR A PARTICULAR
PURPOSE, AND NON-INFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Vanderbilt University
Advanced Computing Center for Research and Education
230 Appleton Place
Nashville, TN 37203
http://www.accre.vanderbilt.edu
*/

#define _log_module_index 188

#include <assert.h>
#include "exnode.h"
#include "log.h"
#include "iniparse.h"
#include "type_malloc.h"
#include "thread_pool.h"
#include "lio.h"
#include "object_service_abstract.h"
#include "iniparse.h"
#include "string_token.h"

int main(int argc, char **argv)
{
    char *path;
    int i, mode, n, nfailed;
    os_fsck_iter_t *it;
    char *fname;
    op_generic_t *gop;
    op_status_t status;
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
    info_flush(lio_ifd);

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

