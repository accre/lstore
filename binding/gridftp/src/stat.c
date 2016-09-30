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
 * @file stat.c
 * Functions to handle stat from gridftp
 */

#include <lio/lio.h>
#include <stdio.h>
#include <tbx/stack.h>

#include "lstore_dsi.h"

#define gridftp_printf(...) globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "[lstore] " __VA_ARGS__);

int plugin_stat(lstore_handle_t *h, tbx_stack_t *stack, const char *path,
                    int file_only) {
    gridftp_printf("Plugin Stat\n");
    int retval = 1;

    // Extract the LStore-specific path
    const char *lstore_path = path_to_lstore(h->prefix, path);
    gridftp_printf("%s %s %s\n", lstore_path, h->prefix, path);
    if (!lstore_path) {
        gridftp_printf("Path not in lstore\n");
        return 1;
    }
    char *path_copy = strdup(lstore_path);
    if (!path_copy) {
        return 1;
    }

    struct stat *file_info = malloc(sizeof(struct stat));
    if (!file_info) {
        goto error_allocstat;
    }

    char *readlink = NULL;
    gridftp_printf("Beginning stat\n");
    retval = lio_stat(lio_gc, lio_gc->creds, path_copy, file_info, h->prefix, &readlink);
    gridftp_printf("Got stat: %d\n", retval);
    if (retval) {
        gridftp_printf("Bad stat: %d\n", retval);
    }
    
    int old_count = tbx_stack_count(stack);
    tbx_stack_push(stack, file_info);
    tbx_stack_push(stack, path_copy);
    tbx_stack_push(stack, readlink);
    // Someone didn't make it to the stack
    while ((tbx_stack_count(stack) != old_count + 3) &&
                (tbx_stack_count(stack) > old_count)) {
        retval = 1;
        void *data = tbx_stack_pop(stack);
        if (data) {
            free(data);
        }
    }

    /*
     * FIXME: At this point, if we've stat'ed a directory and file_only is
     *        false, we should iterate over the contents of the directory and
     *        add them to the stack
     */

    return retval;

error_allocstat:
    free(path_copy);
    return retval;
}
