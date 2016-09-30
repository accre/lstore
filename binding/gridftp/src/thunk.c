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
#include <zlib.h>

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
    // See if we're configured to write to statsd
    char * local_host = globus_malloc(256);
    if (local_host) {
        memset(local_host, 0, 256);
        if (gethostname(local_host, 255)) {
            strcpy(local_host, "UNKNOWN");
        }
    }

    char statsd_namespace_prefix [] = "lfs.gridftp.";
    char * statsd_namespace = globus_malloc(strlen(statsd_namespace_prefix)+
                                            strlen(local_host)+1);
    strcpy(statsd_namespace, statsd_namespace_prefix);
    char * source = local_host;
    char * dest;
    for (dest = statsd_namespace + strlen(statsd_namespace_prefix);
            *source != '\0';
            ++source, ++dest) {
        if (*source == '.') {
            *dest = '_';
        } else {
            *dest = *source;
        }
    }
    *dest = '\0';
    lfs_statsd_link = statsd_init_with_namespace("10.0.32.126", 8125, statsd_namespace);

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

int user_recv_init(lstore_handle_t *h,
                    globus_gfs_transfer_info_t * transfer_info) {
    /*
     * Configure buffers for checksumming
     * Guess 10GB file length if we don't get an actual guess
     */
    long length_guess = (h->xfer_length > 0) ? h->xfer_length : (10*1024L*1024L*1024L);
    size_t num_adler = (length_guess / h->block_size) + 10;
    h->cksum_adler = globus_calloc(num_adler, sizeof(globus_size_t));
    h->cksum_offset = globus_calloc(num_adler, sizeof(globus_size_t));
    h->cksum_nbytes = globus_calloc(num_adler, sizeof(globus_size_t));
    h->cksum_blocks = num_adler;
    if (!h->cksum_adler || !h->cksum_offset || !h->cksum_nbytes) {
        return -1;
    }

    h->xfer_direction = XFER_RECV;
    int retval = plugin_xfer_init(h, transfer_info, XFER_RECV);
    if (!retval) {
        return retval;
    }
    

    return 0;
}

int user_send_init(lstore_handle_t *h,
                    globus_gfs_transfer_info_t * transfer_info) {
    h->xfer_direction = XFER_SEND;
    int retval = plugin_xfer_init(h, transfer_info, XFER_SEND);
    if (!retval) {
        return retval;
    }

    return 0;
}

static void human_readable_adler32(char *adler32_human, uLong adler32) {
    unsigned int i;
    unsigned char * adler32_char = (unsigned char*)&adler32;
    char * adler32_ptr = (char *)adler32_human;
    for (i = 0; i < 4; i++) {
        sprintf(adler32_ptr, "%02x", adler32_char[sizeof(ulong)-4-1-i]);
        adler32_ptr++;
        adler32_ptr++;
    }
    adler32_ptr = '\0';
}

void user_xfer_close(lstore_handle_t *h) {
    if (h->fd) {
        time_t close_timer;
        STATSD_TIMER_RESET(close_timer);
        if (gop_sync_exec(lio_close_op(h->fd)) != OP_STATE_SUCCESS) {
            STATSD_TIMER_POST("lfs_close_time", close_timer);
            h->error = XFER_ERROR_DEFAULT;
        } else if (h->xfer_direction == XFER_RECV) {
            STATSD_TIMER_POST("lfs_close_time", close_timer);
            int bottom = 0;
            size_t i = 0;
            int keep_going = 1;
            globus_off_t offset = 0;
            uint32_t adler = adler32(0L, Z_NULL, 0);
            while (keep_going) {
                keep_going = 0;
                for (i = bottom;i < h->cksum_end_blocks ; ++i) {
                    if ((h->cksum_offset[i] == offset) && (h->cksum_nbytes[i] > 0)) {
                        adler = adler32_combine(adler,
                                                h->cksum_adler[i],
                                                h->cksum_nbytes[i]);
                        offset += h->cksum_nbytes[i];
                        keep_going = 1;
                    }
                }
            }
            // Update checksum
            char adler32_human[2*sizeof(uLong)+1];
            human_readable_adler32(adler32_human, adler);
            lio_setattr(lio_gc, lio_gc->creds, h->path, NULL,
                            "user.gridftp.adler32",
                            adler32_human, strlen(adler32_human));

            // Final flag to say everything is okay
            lio_setattr(lio_gc, lio_gc->creds, h->path, NULL,
                            "user.gridftp.success", "okay", 4);

        } else {
            STATSD_TIMER_POST("lfs_close_time", close_timer);
        }
    } else {
        globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "[lstore] Missing FD in CB??\n");
        h->error = XFER_ERROR_DEFAULT;
    }  
}

void user_xfer_callback(lstore_handle_t *h,
                                globus_gfs_operation_t op,
                                globus_result_t result,
                                globus_byte_t *buffer,
                                globus_size_t nbytes,
                                globus_off_t offset,
                                globus_bool_t eof) {
    if (result != GLOBUS_SUCCESS) {
        user_handle_done(h, XFER_ERROR_DEFAULT);
        h->done = GLOBUS_TRUE;
    } else if (eof) {
        user_handle_done(h, XFER_ERROR_NONE);
    }
    if ((h->xfer_direction == XFER_RECV) && (nbytes > 0) && (h->fd)) {
        // Store the adler32 for this block
        uint32_t adler32_accum = adler32(0L, Z_NULL, 0);
        adler32_accum = adler32(adler32_accum, (const Bytef *)buffer, nbytes);
        size_t adler32_idx = offset / h->block_size;
        while (h->cksum_nbytes[adler32_idx] != 0) {
            ++adler32_idx;
        }
        h->cksum_nbytes[adler32_idx] = nbytes;
        h->cksum_offset[adler32_idx] = offset;
        h->cksum_adler[adler32_idx] = adler32_accum;
        if (adler32_idx + 1 > h->cksum_end_blocks) {
            h->cksum_end_blocks = adler32_idx + 1;
        }
        if (offset + nbytes > h->cksum_total_len) {
            h->cksum_total_len = offset + nbytes;
        }
        globus_size_t written = lio_write(h->fd,
                                            (char *)buffer,
                                            nbytes,
                                            offset,
                                            NULL);
        if (written != nbytes) {
            user_handle_done(h, XFER_ERROR_DEFAULT);
        }
    }
    if (!h->fd) {
        globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "[lstore] Missing FD??\n");
        user_handle_done(h, XFER_ERROR_DEFAULT);
    }

    globus_free(buffer);
}

int user_xfer_pump(lstore_handle_t *h,
                    char **buf_idx,
                    lstore_reg_info_t *reg_idx,
                    int *buf_len) {
    //globus_result_t rc = GLOBUS_SUCCESS;
    int count = 0;
    // We increment outstanding count on the outside, but we want to use it as
    // a counter here
    int outstanding_sum = h->outstanding_count;
    while ((outstanding_sum < h->optimal_count) && (count < *(buf_len)) && (!h->done)) {
        // This implementation is obviously junk. Make it better.
        buf_idx[count] = globus_malloc(h->block_size);
        if (buf_idx[count] == NULL) {
            goto error_allocblock;
        }
        // If we're pumping read operations, fill the buffer to hand to gridftp
        if (h->xfer_direction == XFER_SEND) {

            globus_size_t read_length;
            if (h->xfer_length < 0 || h->xfer_length > h->block_size) {
                read_length = h->block_size;
            } else {
                read_length = h->xfer_length;
            }

            /*
             * call down to plugin_read
             * lio_read(h->fd, (char *)buffer, nbytes, offset, NULL);
             */ 
            globus_off_t offset  = h->offset;
            globus_size_t nbytes = lio_read(h->fd,
                                            buf_idx[count],
                                            read_length,
                                            offset,
                                            NULL);

            // Then tell gridftp what we just read
            reg_idx[count].buffer = (globus_byte_t *)buf_idx[count];
            reg_idx[count].nbytes = nbytes;
            reg_idx[count].offset = offset;
            if ((nbytes == 0)) {
                // got EOF
                user_handle_done(h, XFER_ERROR_NONE);
            }
            if (!h->done) {
                // Advance offset
                h->offset += nbytes;
                h->xfer_length -= nbytes;
            }
        }
        // TODO for RECV/write, need to register the callback
        ++count;
        ++outstanding_sum;
    }

    (*buf_len) = count;
    return 0;

error_allocblock:
    while (count > 0) {
        if (buf_idx[count]) {
            globus_free(buf_idx[count]);
        }
        --(h->outstanding_count);
        --count;
    }
    (*buf_len) = 0;
    globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "[lstore] Failed to user_pump.\n");
    return -1;
}

void user_handle_done2(lstore_handle_t *h, xfer_error_t error, char *file, int line) {
    globus_gfs_log_message(GLOBUS_GFS_LOG_INFO,
                            "[lstore] Handle done. Error: %d Outstanding: %d Loc: %s:%d\n",
                            error,
                            h->outstanding_count,
                            file,
                            line);
    h->done = GLOBUS_TRUE;
    if (h->error == XFER_ERROR_NONE) {
        h->error = error;
    }
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
    h->error = XFER_ERROR_NONE;
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
    //if (h->fd) {
    //    gop_sync_exec(lio_close_op(h->fd));
    //}
    if (h->cksum_adler) {
        globus_free(h->cksum_adler);
    } 
    if (h->cksum_offset) {
        globus_free(h->cksum_offset);
    }
    if (h->cksum_nbytes) {
        globus_free(h->cksum_nbytes);
    }
    globus_free(h);
}
