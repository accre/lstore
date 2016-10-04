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

/**
 * @file command.c
 * Implements filesystem operations (mostly metadata things)
 */

#include <lio/lio.h>

#include "lstore_dsi.h"

int plugin_checksum(lstore_handle_t *h, char *path, char **response) {
    int retval = -1;

    /*
     * Adler32 checksum in human-readable form is 8 bytes long. Additionally add
     * an extra byte for the null terminator
     */
    int buf_length = 9 * sizeof(char);
    char *buf = malloc(buf_length);
    if (!buf) {
        return -1;
    }

    retval = lio_getattr(lio_gc,
                            lio_gc->creds,
                            path,
                            NULL,
                            "user.gridftp.adler32",
                            (void **) buf,
                            &buf_length);
    if (!retval) {
        (*response) = buf;
    }

    return retval;
}

int plugin_mkdir(lstore_handle_t *h, char *path) {
    int retval = gop_sync_exec(lio_create_op(lio_gc, lio_gc->creds, path,
                                                OS_OBJECT_DIR, NULL, NULL));
    return retval;
}

int plugin_rmdir(lstore_handle_t *h, char *path) {
    // FIXME Unsure if this is supposed to be recursive or not
    return 0;
}

int plugin_rm(lstore_handle_t *h, char *path) {
    int retval = gop_sync_exec(lio_remove_op(lio_gc, lio_gc->creds, path,
                                                NULL, 0));
    return retval;
}

