/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * @file lstore_dsi.c
 * Basic GridFTP boilerplate generated from dsi_bones
 *
 * As much as possible, this handles all the conversion to/from GridFTP's API
 * and the rest of the plugin. Hopefully this separation of interests will
 * let the plugin keep clean in spite of the API quirks.
 */

#include <globus_gridftp_server.h>
#include <lio/lio.h>
#include <time.h>
#include <zlib.h>

#include "lstore_dsi.h"
#include "version.h"

// Forward declaration
static void gfs_send_callback(globus_gfs_operation_t op,
                                globus_result_t result,
                                globus_byte_t *buffer,
                                globus_size_t nbytes,
                                void *user_arg);
/* Marked unread since only gridftp calls it */
void gfs_recv_callback(globus_gfs_operation_t op,
                                globus_result_t result,
                                globus_byte_t * buffer,
                                globus_size_t nbytes,
                                globus_off_t offset,
                                globus_bool_t eof,
                                void * user_arg);
static void gfs_xfer_pump(lstore_handle_t *h);
static void gfs_xfer_callback(globus_gfs_operation_t op,
                                globus_result_t result,
                                globus_byte_t * buffer,
                                globus_size_t nbytes,
                                globus_off_t offset,
                                globus_bool_t eof,
                                void * user_arg);
static void globus_l_gfs_file_destroy_stat(
                                globus_gfs_stat_t *stat_array,
                                int stat_count);
static
globus_version_t local_version =
{
    LSTORE_DSI_VERSION_MAJOR, /* major version number */
    LSTORE_DSI_VERSION_MINOR, /* minor version number */
    LSTORE_DSI_TIMESTAMP,
    0 /* branch ID */
};

/*
 * start
 * -----
 * This function is called when a new session is initialized, ie a user
 * connectes to the server.  This hook gives the dsi an oppertunity to
 * set internal state that will be threaded through to all other
 * function calls associated with this session.  And an oppertunity to
 * reject the user.
 *
 * finished_info.info.session.session_arg should be set to an DSI
 * defined data structure.  This pointer will be passed as the void *
 * user_arg parameter to all other interface functions.
 *
 * NOTE: at nice wrapper function should exist that hides the details
 *       of the finished_info structure, but it currently does not.
 *       The DSI developer should jsut follow this template for now
 */
static
void
globus_l_gfs_lstore_start(
    globus_gfs_operation_t              op,
    globus_gfs_session_info_t *         session_info)
{
    GlobusGFSName(globus_l_gfs_lstore_start);
    globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "[lstore] start\n");

    globus_result_t result = GLOBUS_SUCCESS;
    lstore_handle_t *lstore_handle;

    int retval = 0;
    lstore_handle = user_connect(op, &retval);
    if (retval) {
        GlobusGFSErrorGenericStr(result, ("[lstore] Failed to start session."));
    }

    globus_gfs_finished_info_t finished_info;
    memset(&finished_info, '\0', sizeof(globus_gfs_finished_info_t));
    finished_info.type = GLOBUS_GFS_OP_SESSION_START;
    finished_info.result = result;
    finished_info.info.session.session_arg = lstore_handle;
    finished_info.info.session.username = session_info->username;
    finished_info.info.session.home_dir = "/lio/lfs/";

    globus_gridftp_server_operation_finished(
        op, result, &finished_info);
}

/*
 * destroy
 * -------
 * This is called when a session ends, ie client quits or disconnects.
 * The dsi should clean up all memory they associated wit the session
 * here.
 */
static
void
globus_l_gfs_lstore_destroy(
    void *                              user_arg)
{
    lstore_handle_t *       lstore_handle;

    GlobusGFSName(globus_l_gfs_lstore_destroy);
    globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "[lstore] destroy\n");
    lstore_handle = (lstore_handle_t *) user_arg;

    // Set any needed options in handle here
    int retval = user_close(lstore_handle);
    if (retval) {
        globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "[lstore] Failed to destroy session.");
    }
}

/*
 * stat
 * ----
 * This interface function is called whenever the server needs
 * information about a given file or resource.  It is called then an
 * LIST is sent by the client, when the server needs to verify that
 * a file exists and has the proper permissions, etc.
 */
static
void
globus_l_gfs_lstore_stat(
    globus_gfs_operation_t              op,
    globus_gfs_stat_info_t *            stat_info,
    void *                              user_arg)
{
    GlobusGFSName(globus_l_gfs_lstore_stat);
    globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "[lstore] stat\n");

    lstore_handle_t * lstore_handle;
    lstore_handle = (lstore_handle_t *) user_arg;

    globus_result_t result = GLOBUS_SUCCESS;
    globus_gfs_stat_t *stat_array = NULL;
    int stat_count = 0;

    int retval = user_stat(lstore_handle, stat_info, &stat_array, &stat_count);
    if (retval == GLOBUS_FAILURE) {
        // Catchall for generic globus oopsies
        GlobusGFSErrorGenericStr(result, ("[lstore] Failed to perform stat."));
    } else if (retval != GLOBUS_SUCCESS) {
        // If we get something that's not GLOBUS_FAILURE or SUCCESS, treat it
        // like a real globus error string
        result = retval;
    }

    globus_gridftp_server_finished_stat(
        op, result, stat_array, stat_count);
    globus_l_gfs_file_destroy_stat(stat_array, stat_count);
}

/*
 * command
 * -------
 * This interface function is called when the client sends a 'command'.
 * commands are such things as mkdir, remdir, delete.  The complete
 * enumeration is below.
 *
 * To determine which command is being requested look at:
 *     cmd_info->command
 *
 *     GLOBUS_GFS_CMD_MKD = 1,
 *     GLOBUS_GFS_CMD_RMD,
 *     GLOBUS_GFS_CMD_DELE,
 *     GLOBUS_GFS_CMD_RNTO,
 *     GLOBUS_GFS_CMD_RNFR,
 *     GLOBUS_GFS_CMD_CKSM,
 *     GLOBUS_GFS_CMD_SITE_CHMOD,
 *     GLOBUS_GFS_CMD_SITE_DSI
 */
static
void
globus_l_gfs_lstore_command(
    globus_gfs_operation_t              op,
    globus_gfs_command_info_t *         cmd_info,
    void *                              user_arg)
{
    GlobusGFSName(globus_l_gfs_lstore_command);
    globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "[lstore] command\n");

    lstore_handle_t * lstore_handle;
    lstore_handle = (lstore_handle_t *) user_arg;
    globus_result_t result = GLOBUS_SUCCESS;

    char *response = GLOBUS_NULL;
    int retval = user_command(lstore_handle, cmd_info, &response);
    if (retval == GLOBUS_FAILURE) {
        // Catchall for generic globus oopsies
        GlobusGFSErrorGenericStr(result, ("[lstore] Failed to execute generic command"));
    } else if (retval != GLOBUS_SUCCESS) {
        // If we get something that's not GLOBUS_FAILURE or SUCCESS, treat it
        // like a real globus error string
        result = retval;
    }

    globus_gridftp_server_finished_command(op, result, response);
}


/*
 * recv
 * ----
 * This interface function is called when the client requests that a
 * file be transfered to the server.
 *
 * To receive a file the following functions will be used in roughly
 * the presented order.  They are doced in more detail with the
 * gridftp server documentation.
 *
 *     globus_gridftp_server_begin_transfer();
 *     globus_gridftp_server_register_read();
 *     globus_gridftp_server_finished_transfer();
 *
 * Function heavily stolen from xrootd-dsi plugin
 */
static
void
globus_l_gfs_lstore_recv(
    globus_gfs_operation_t              op,
    globus_gfs_transfer_info_t *        transfer_info,
    void *                              user_arg)
{
    GlobusGFSName(globus_l_gfs_lstore_recv);
    globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "[lstore] recv\n");

    lstore_handle_t * lstore_handle;
    lstore_handle = (lstore_handle_t *) user_arg;
    lstore_handle->op = op;
    globus_result_t result = GLOBUS_SUCCESS;

    globus_gridftp_server_get_block_size(lstore_handle->op,
                                            &lstore_handle->block_size);
    globus_gridftp_server_get_read_range(lstore_handle->op,
                                            &lstore_handle->offset,
                                            &lstore_handle->xfer_length);
    globus_gridftp_server_get_optimal_concurrency(lstore_handle->op,
                                            &lstore_handle->optimal_count);
    /*
     * Once GridFTP is notified by begin_transfer, you can at any point kill
     * the xfer by issuing a globus_gridftp_server_finished_transfer(). Since
     * we're going to perform the transfers asynchronously, we don't call that
     * function unless there's an error condition we can detect very early.
     * Otherwise, we'll just let control fall off the end of this function.
     */
    globus_gridftp_server_begin_transfer(lstore_handle->op, 0, lstore_handle);
    int retval = user_recv_init(lstore_handle, transfer_info);
    if (!lstore_handle->fd) {
        globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "[lstore] No FD in init?\n");
    }

    if (retval != 0) {
        // Catchall for generic globus oopsies
        globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "[lstore] recv fail\n");
        GlobusGFSErrorGenericStr(result, ("[lstore] Failed to recv file."));
        globus_gridftp_server_finished_transfer(op, result);
    } else {
       /*
        * Now that we've begun the transfer, we trigger the initial
        * asynchronous I/O requests. After this point, the control flow is
        * enirely through callbacks being submitted and handled
        */
        gfs_xfer_pump(lstore_handle);
    }

}

/*
 * send
 * ----
 * This interface function is called when the client requests to receive
 * a file from the server.
 *
 * To send a file to the client the following functions will be used in roughly
 * the presented order.  They are doced in more detail with the
 * gridftp server documentation.
 *
 *     globus_gridftp_server_begin_transfer();
 *     globus_gridftp_server_register_write();
 *     globus_gridftp_server_finished_transfer();
 *
 */
static
void
globus_l_gfs_lstore_send(
    globus_gfs_operation_t              op,
    globus_gfs_transfer_info_t *        transfer_info,
    void *                              user_arg)
{
    GlobusGFSName(globus_l_gfs_lstore_send);
    globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "[lstore] send\n");

    lstore_handle_t * lstore_handle;
    lstore_handle = (lstore_handle_t *) user_arg;
    lstore_handle->op = op;
    globus_result_t result = GLOBUS_SUCCESS;

    globus_gridftp_server_get_block_size(lstore_handle->op,
                                            &lstore_handle->block_size);
    globus_gridftp_server_get_write_range(lstore_handle->op,
                                            &lstore_handle->offset,
                                            &lstore_handle->xfer_length);
    globus_gridftp_server_get_optimal_concurrency(lstore_handle->op,
                                                    &lstore_handle->optimal_count);
    /*
     * Once GridFTP is notified by begin_transfer, you can at any point kill
     * the xfer by issuing a globus_gridftp_server_finished_transfer(). Since
     * we're going to perform the transfers asynchronously, we don't call that
     * function unless there's an error condition we can detect very early.
     * Otherwise, we'll just let control fall off the end of this function.
     */
    globus_gridftp_server_begin_transfer(lstore_handle->op, 0, lstore_handle);
    int retval = user_send_init(lstore_handle, transfer_info);

    if (retval != 0) {
        // Catchall for generic globus oopsies
        GlobusGFSErrorGenericStr(result, ("[lstore] Failed to send file."));
        globus_gridftp_server_finished_transfer(op, result);
    } else {
        /*
         * Now that we've begun the transfer, we trigger the initial
         * asynchronous I/O requests. After this point, the control flow is
         * enirely through callbacks being submitted and handled
         */
         gfs_xfer_pump(lstore_handle);
    }

}
/**
 * Enumerates this plugin's function pointers to gridftp
 */
globus_gfs_storage_iface_t globus_l_gfs_lstore_dsi_iface =
{
    .descriptor = GLOBUS_GFS_DSI_DESCRIPTOR_BLOCKING | \
                    GLOBUS_GFS_DSI_DESCRIPTOR_SENDER,
    .init_func = globus_l_gfs_lstore_start,
    .destroy_func = globus_l_gfs_lstore_destroy,
    .send_func = globus_l_gfs_lstore_send,
    .recv_func = globus_l_gfs_lstore_recv,
    .command_func = globus_l_gfs_lstore_command,
    .stat_func = globus_l_gfs_lstore_stat,
};

/**
 * Describes this plugin
 *
 * Forward declare since the struct and functions reference each other.
 */
static int globus_l_gfs_lstore_activate(void);
static int globus_l_gfs_lstore_deactivate(void);

__attribute__((visibility ("default"))) GlobusExtensionDefineModule(globus_gridftp_server_lstore) =
{
    .module_name = "globus_gridftp_server_lstore",
    .activation_func = globus_l_gfs_lstore_activate,
    .deactivation_func = globus_l_gfs_lstore_deactivate,
    .version = &local_version
};

/*
 * activate
 * --------
 * This interface function is called when the plugin is loaded (i.e. when
 * GridFTP starts)
 */
static
int
globus_l_gfs_lstore_activate(void)
{
    GlobusGFSName(globus_l_gfs_lstore_activate);
    globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "[lstore] activate\n");
    globus_result_t result = GLOBUS_SUCCESS;

    int retval = activate();
    if (retval) {
        globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "[lstore] Failed to activate.\n");
        GlobusGFSErrorGenericStr(result, ("[lstore] Failed to activate."));
        if (!result) {
            result = GLOBUS_FAILURE;
        }
        return result;
    }

    globus_extension_registry_add(
        GLOBUS_GFS_DSI_REGISTRY,
        "lstore",
        GlobusExtensionMyModule(globus_gridftp_server_lstore),
        &globus_l_gfs_lstore_dsi_iface);

    return result;
}

/*
 * deactivate
 * ----------
 * This interface function is called when the plugin is unloaded (i.e. when
 * GridFTP shuts down)
 */
static
int
globus_l_gfs_lstore_deactivate(void)
{
    GlobusGFSName(globus_l_gfs_lstore_deactivate);
    globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "[lstore] deactivate\n");
    globus_result_t result = GLOBUS_SUCCESS;

    globus_extension_registry_remove(
        GLOBUS_GFS_DSI_REGISTRY, "lstore");

    int retval = deactivate();
    if (retval) {
        GlobusGFSErrorGenericStr(result, ("[lstore] Failed to deactivate."));
    }

    return result;
}

/*
 * Stat-handling functions stolen from "file" DSI
 */
static void
globus_l_gfs_file_destroy_stat(
    globus_gfs_stat_t *                 stat_array,
    int                                 stat_count)
{
    int                                 i;
    GlobusGFSName(globus_l_gfs_file_destroy_stat);

    for(i = 0; i < stat_count; i++)
    {
        if(stat_array[i].name != NULL)
        {
            globus_free(stat_array[i].name);
        }
        if(stat_array[i].symlink_target != NULL)
        {
            globus_free(stat_array[i].symlink_target);
        }
    }
    globus_free(stat_array);
}

static int my_min(int a, int b) {
    return (a < b) ? a : b;
}
static int my_max(int a, int b) {
    return (a > b) ? a : b;
}

/*
 * These functions exist instead of just the user_ functions because they are
 * callbacks triggerd by Globus.
 */
#define MAX_CONCURRENCY_PER_LOOP ((int) 32)
static void gfs_xfer_pump(lstore_handle_t *h) {
    GlobusGFSName(gfs_xfer_pump);
    globus_mutex_lock(&h->mutex);
    
    globus_result_t rc = GLOBUS_SUCCESS;
    int concurrency_needed =  h->optimal_count - h->outstanding_count;
    concurrency_needed = my_min(MAX_CONCURRENCY_PER_LOOP, concurrency_needed);
    concurrency_needed = my_max(0, concurrency_needed);
    // for pump (concur && (!done, read)
    for (int i = 0; i < concurrency_needed; ++i) {
        if ((h->xfer_direction == XFER_SEND) && h->done) {
            break;
        }
        // alloc (USER CODE)
        globus_byte_t *buf = globus_malloc(h->block_size);
        
        // if recv
        if (h->xfer_direction == XFER_RECV) {
            // register read
            rc = globus_gridftp_server_register_read(h->op,
                                       buf,
                                       h->block_size,
                                       gfs_recv_callback,
                                       h);
            if (rc == GLOBUS_SUCCESS) {
                // inc outstanding
                ++(h->outstanding_count);
            } else {
                // failed to register
                user_handle_done(h, XFER_ERROR_DEFAULT);
            }
        } else {
            // if send
            // do read (USER CODE)
            globus_size_t read_length;
            if (h->xfer_length < 0 || h->xfer_length > h->block_size) {
                read_length = h->block_size;
            } else {
                read_length = h->xfer_length;
            }
            globus_off_t offset = h->offset;
            time_t read_timer;
            STATSD_TIMER_RESET(read_timer);
            int nbytes = lio_read(h->fd,
                                            (char *)buf,
                                            read_length,
                                            offset,
                                            NULL);
            STATSD_TIMER_POST("lfs_read_time", read_timer);
            STATSD_COUNT("lfs_bytes_read", nbytes);
            //   if bytes = 0
            if (nbytes == 0) {
                // done eof
                user_handle_done(h, XFER_ERROR_NONE);
            } else if (nbytes < 0) {
                // bad read
                user_handle_done(h, XFER_ERROR_DEFAULT);
            } else {
                // more coming
                                // int offset
                h->offset += nbytes;
                // register read
                rc = globus_gridftp_server_register_write(h->op,
                                                            buf,
                                                            nbytes,
                                                            h->offset,
                                                            -1,
                                                            gfs_send_callback,
                                                            h);
                if (rc == GLOBUS_SUCCESS) {
                    // inc outstanding
                    ++(h->outstanding_count);
                } else {
                    // failed to add the write
                    user_handle_done(h, XFER_ERROR_DEFAULT);
                }
            }
        }
    }
    globus_mutex_unlock(&h->mutex);
}

static void gfs_send_callback(globus_gfs_operation_t op,
                                globus_result_t result,
                                globus_byte_t *buffer,
                                globus_size_t nbytes,
                                void *user_arg) {
    gfs_xfer_callback(op, result, buffer, nbytes, 0, 0, user_arg);
}

void gfs_recv_callback(globus_gfs_operation_t op,
                                globus_result_t result,
                                globus_byte_t * buffer,
                                globus_size_t nbytes,
                                globus_off_t offset,
                                globus_bool_t eof,
                                void * user_arg) {
    gfs_xfer_callback(op, result, buffer, nbytes, offset, eof, user_arg);
}

static void gfs_xfer_callback(globus_gfs_operation_t op,
                                globus_result_t result,
                                globus_byte_t * buffer,
                                globus_size_t nbytes,
                                globus_off_t offset,
                                globus_bool_t eof,
                                void * user_arg) {
    GlobusGFSName(gfs_recv_callback);
    lstore_handle_t *h = (lstore_handle_t *) user_arg;
    if (((offset == 0) && (h->xfer_direction == XFER_RECV)) || (result != 0) || (eof != 0)) {
        globus_gfs_log_message(GLOBUS_GFS_LOG_INFO,
                                "[lstore] gfs_CB xf: %d res: %d nb: %d off: %d eof: %d\n",
                                h->xfer_direction, result, nbytes, offset, eof);
    }
    globus_mutex_lock(&h->mutex);
    if (result != 0) {
        user_handle_done(h, XFER_ERROR_DEFAULT);
    }
    if (eof) {
        user_handle_done(h, XFER_ERROR_NONE);
    }
    if ((nbytes > 0) && (h->xfer_direction == XFER_RECV)) {
        // write (USER CODE)
        // if written != nbytes
            // done -> error
        // else
            // update_bytes_written

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
        time_t write_timer;
        STATSD_TIMER_RESET(write_timer);
        globus_size_t written = lio_write(h->fd,
                                            (char *)buffer,
                                            nbytes,
                                            offset,
                                            NULL);
        STATSD_TIMER_POST("lfs_write_time", write_timer);
        STATSD_COUNT("lfs_bytes_written", written);
        if (written != nbytes) {
            user_handle_done(h, XFER_ERROR_DEFAULT);
        } else {
            globus_gridftp_server_update_bytes_written(h->op, offset, nbytes);
        }

    }
    // free buffer (USER CORE)
    globus_free(buffer);
    // dec outstanding
    --(h->outstanding_count);
    /*
     * The transfer is done when h->done is set and h->outstanding_count reaches
     * zero
     *
     * Unlock within the if statements since the conditions are protected by
     * mutex
     */
    if (h->done && (h->outstanding_count == 0)) {
        user_xfer_close(h);
        if (h->error == XFER_ERROR_NONE) {
            globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "[lstore] xfer success: %s\n", h->path);
            globus_gridftp_server_finished_transfer(h->op, GLOBUS_SUCCESS);
        } else {
            globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "[lstore] xfer failure: %s\n", h->path);
            globus_gridftp_server_finished_transfer(h->op, GLOBUS_FAILURE);
        }
        globus_mutex_unlock(&h->mutex);
    } else if (!h->done) {
        globus_mutex_unlock(&h->mutex);
        gfs_xfer_pump(h);
    } else {
        globus_mutex_unlock(&h->mutex);
    }

    return;
}
