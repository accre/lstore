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
 * @file thunk.c
 * Entry point(s) from the GridFTP side to LStore
 */

#include <lio/lio.h>
#include <stdio.h>

#include "lstore_dsi.h"

int activate() {
    log_printf(0,"Loaded\n");

    int argc = 3;
    char **argv = malloc(sizeof(char *)*argc);
    argv[0] = "lio_gridftp";
    argv[1] = "-c";
    argv[2] = "/etc/lio/lio-gridftp.cfg";

    // char **argvp = argv;
    lio_init(&argc, &argv);
    free(argv);
    if (!lio_gc) {
        log_printf(-1,"Failed to load LStore\n");
        return 1;
    }

    return 0;
}

int deactivate() {
    log_printf(0,"Unloaded\n");
    lio_shutdown();
    return 0;
}

lstore_handle_t *user_connect(globus_gfs_operation_t op, int *retval) {
    log_printf(0,"Connect\n");
    lstore_handle_t *h;
    h = user_handle_new(retval);
    if (!h) {
        // retval is set in user_handle_new
        return NULL;
    }
    memcpy(&h->op, &op, sizeof(op));

    return h;
}

int user_close(lstore_handle_t *h) {
    log_printf(0,"Close\n");
    user_handle_del(h);
    return 0;
}

/**
 * Given a stack with our stat info, fill a globus stat structure
 * @param dest Structure to fill
 * @param stack Stack to fill from
 */
static void globus_stat_fill(globus_gfs_stat_t *dest, tbx_stack_t *stack) {
    char *fname;
    char *readlink = NULL;
    struct stat *stat;
    readlink = tbx_stack_pop(stack);
    fname = tbx_stack_pop(stack);
    stat = tbx_stack_pop(stack);
    transfer_stat(dest, stat, fname, readlink);
    free(stat);
    free(fname);
    free(readlink);
}


int user_stat(lstore_handle_t *h, globus_gfs_stat_info_t *info,
                globus_gfs_stat_t ** ret, int *ret_count) {
    int retval = GLOBUS_FAILURE;
    (*ret) = NULL;
    (*ret_count) = 0;

    tbx_stack_t *stack = tbx_stack_new();
    if (!stack) {
        globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "[lstore] Couldnt init stack\n");
        goto error_initstack;
    }

    int retcode = plugin_stat(h, stack, info->pathname, info->file_only);
    if (retcode) {
        globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "[lstore] Couldnt plugin_stat\n");
        goto error_stat;
    }

    // Once plugin_stat fills the struct with however many struct stat's we
    // need, we have to then convert it to an array of globus' special stat
    // structs
    int stat_count = tbx_stack_count(stack) / 3;
    globus_gfs_stat_t *stats =
        globus_malloc(sizeof(globus_gfs_stat_t) * stat_count);
    if (!stats) {
        goto error_allocarray;
    }
    
    for (int idx=0; idx < stat_count; ++idx) {
        globus_stat_fill(&stats[idx], stack);
    }

    (*ret) = stats;
    (*ret_count) = stat_count;
    retval = GLOBUS_SUCCESS;

error_allocarray:
error_stat:
    tbx_stack_del(stack);

error_initstack:

    return retval;
}


int user_command(lstore_handle_t *h, globus_gfs_command_info_t * info,
                    char **response) {
    int retval = -1;
    char *path_copy = copy_path_to_lstore(h->prefix, info->pathname);
    if (!path_copy) {
        return -1;
    }
    switch (info->command) {
        case GLOBUS_GFS_CMD_CKSM:
            if (!strcmp(info->cksm_alg, "adler32") ||
                !strcmp(info->cksm_alg, "ADLER32")) {
                retval = plugin_checksum(h, path_copy, response);
            } else {
                retval = -1;
            }
            break;
        case GLOBUS_GFS_CMD_DELE:
            retval = plugin_rm(h, path_copy);
            break;
        case GLOBUS_GFS_CMD_MKD:
            retval = plugin_mkdir(h, path_copy);
            break;
        case GLOBUS_GFS_CMD_RMD:
            retval = plugin_rmdir(h, path_copy);
            break;
        default:
            retval = -2;
    }
    free(path_copy);
    return retval;
}

int user_recv_callback(lstore_handle_t *h,
                        char *buffer,
                        globus_size_t nbytes,
                        globus_off_t offset) {
    int retval = lio_write(h->fd, (char *)buffer, nbytes, offset, NULL);
    free(buffer);
    return (retval == nbytes) ? 0 : 1;
}

int user_recv_init(lstore_handle_t *h,
                    globus_gfs_transfer_info_t * transfer_info) {
    h->xfer_direction = XFER_RECV;
    int retval = plugin_xfer_init(h, transfer_info, XFER_RECV);
    if (!retval) {
        return retval;
    }

    return 0;
}

// AKA globus_l_gfs_posix_write_to_storage
int user_xfer_pump(lstore_handle_t *h, char **buf_idx, int *buf_len) {
    int count = 0;

    while ((h->outstanding_count < h->optimal_count) && (count < *(buf_len))) {

        // This implementation is obviously junk. Make it better.
        buf_idx[count] = globus_malloc(h->block_size);
        if (buf_idx[count] == NULL) {
            goto error_allocblock;
        }
        ++count;
        ++(h->outstanding_count);
    }

    (*buf_len) = count;
    return 0;

error_allocblock:
    while (count > 0) {
        if (buf_idx[count]) {
            globus_free(buf_idx[count]);
        }
        --count;
    }
    (*buf_len) = 0;
    return -1;
}

lstore_handle_t *user_handle_new(int *retval_ext) {
    log_printf(0,"New handle\n");
    (*retval_ext) = 0;
    lstore_handle_t *h = (lstore_handle_t *)
            globus_malloc(sizeof(lstore_handle_t));
    if (!h) {
        (*retval_ext) = -1;
        return NULL;
    }
    memset(h, '\0', sizeof(lstore_handle_t));

    if (globus_mutex_init(&h->mutex, GLOBUS_NULL)) {
        (*retval_ext) = -3;
        return NULL;
    }
    h->optimal_count = 2;
    h->block_size = 262144;
    h->prefix = strdup("/lio/lfs");
    h->done = GLOBUS_FALSE;
    if (!h->prefix) {
        (*retval_ext) = -4;
        return NULL;
    }

    return h;
}


void user_handle_del(lstore_handle_t *h) {
    log_printf(0,"Del handle\n");
    if (!h) {
        return;
    }
    if (h->prefix) {
        free(h->prefix);
    }
    if (h->expected_checksum) {
        free(h->expected_checksum);
    }
    if (h->fd) {
        gop_sync_exec(lio_close_op(h->fd));
    }
    globus_free(h);
}
