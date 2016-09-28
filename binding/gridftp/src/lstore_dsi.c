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
#include "lstore_dsi.h"
#include "version.h"

// Forward declaration
static void gfs_xfer_pump(lstore_handle_t *h);
static void gfs_recv_callback(globus_gfs_operation_t op,
                                globus_result_t result,
                                globus_byte_t * buffer,
                                globus_size_t nbytes,
                                globus_off_t offset,
                                globus_bool_t eof,
                                void * user_arg);

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
    if (!retval) {
        GlobusGFSErrorGenericStr(result, ("[lstore] Failed to start session."));
    }

    globus_gfs_finished_info_t finished_info;
    memset(&finished_info, '\0', sizeof(globus_gfs_finished_info_t));
    finished_info.type = GLOBUS_GFS_OP_SESSION_START;
    finished_info.result = result;
    finished_info.info.session.session_arg = lstore_handle;
    finished_info.info.session.username = session_info->username;
    finished_info.info.session.home_dir = "/";

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
    globus_result_t result = GLOBUS_SUCCESS;
    int retval = user_close(lstore_handle);
    if (!retval) {
        GlobusGFSErrorGenericStr(result, ("[lstore] Failed to start session."));
    }

    globus_free(lstore_handle);
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
        GlobusGFSErrorGenericStr(result, ("[lstore] Failed to start session."));
    } else if (retval != GLOBUS_SUCCESS) {
        // If we get something that's not GLOBUS_FAILURE or SUCCESS, treat it
        // like a real globus error string
        result = retval;
    }

    globus_gridftp_server_finished_stat(
        op, result, stat_array, stat_count);
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
        GlobusGFSErrorGenericStr(result, ("[lstore] Failed to start session."));
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
    globus_gridftp_server_get_write_range(lstore_handle->op,
                                            &lstore_handle->offset,
                                            &lstore_handle->write_length);
    /*
     * Once GridFTP is notified by begin_transfer, you can at any point kill
     * the xfer by issuing a globus_gridftp_server_finished_transfer(). Since
     * we're going to perform the transfers asynchronously, we don't call that
     * function unless there's an error condition we can detect very early.
     * Otherwise, we'll just let control fall off the end of this function.
     */
    globus_gridftp_server_begin_transfer(lstore_handle->op, 0, lstore_handle);
    int retval = user_recv_init(lstore_handle, transfer_info);

    if (retval == GLOBUS_FAILURE) {
        // Catchall for generic globus oopsies
        GlobusGFSErrorGenericStr(result, ("[lstore] Failed to recv file."));
        globus_gridftp_server_finished_transfer(op, result);
        return;
    } else if (retval != GLOBUS_SUCCESS) {
        // If we get something that's not GLOBUS_FAILURE or SUCCESS, treat it
        // like a real globus error string
        result = retval;
        globus_gridftp_server_finished_transfer(op, result);
        return;
    }

    /*
     * Now that we've begun the transfer, we trigger the initial asynchronous
     * I/O requests. After this point, the control flow is enirely through
     * callbacks being submitted and handled
     */
    gfs_xfer_pump(lstore_handle);

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
    lstore_handle_t *       lstore_handle;
    GlobusGFSName(globus_l_gfs_lstore_send);

    globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "[lstore] send\n");
    lstore_handle = (lstore_handle_t *) user_arg;

    globus_gridftp_server_finished_transfer(op, GLOBUS_SUCCESS);
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

GlobusExtensionDefineModule(globus_gridftp_server_lstore) =
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
    if (!retval) {
        GlobusGFSErrorGenericStr(result, ("[lstore] Failed to deactivate."));
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
    if (!retval) {
        GlobusGFSErrorGenericStr(result, ("[lstore] Failed to deactivate."));
    }

    return result;
}

/*
 * These functions exist instead of just the user_ functions because they are
 * callbacks triggerd by Globus.
 */
#define MAX_CONCURRENCY_PER_LOOP ((int) 32)
static void gfs_xfer_pump(lstore_handle_t *h) {
    GlobusGFSName(gfs_xfer_pump);
    globus_gridftp_server_get_optimal_concurrency(h->op, &h->optimal_count);
    globus_byte_t *buf_list[MAX_CONCURRENCY_PER_LOOP];
    int buf_len = MAX_CONCURRENCY_PER_LOOP;
    int retval = user_xfer_pump(h, (char **)&buf_list, &buf_len);
    globus_result_t rc = GLOBUS_SUCCESS;
    if (retval) {
        // Convert return codes in user_ to gridftp's async calls
        switch (retval) {
            case -1:
                rc = GlobusGFSErrorGeneric("Failed to allocate buffer");
                break;
            default:
                rc = GLOBUS_FAILURE;
                break;
        }
    } else {
        for (int i = 0; i < buf_len; ++i) {
            /*
             * The register functions are one of:
             *     globus_gridftp_server_register_read
             *     globus_gridftp_server_register_write
             */
            rc = h->register_fn(h->op,
                                    buf_list[i],
                                    h->block_size,
                                    gfs_recv_callback,
                                    h);
            if (rc != GLOBUS_SUCCESS) {
                rc = GlobusGFSErrorGeneric("register_read fail");
                break;
            }
        }
    }

    if (rc != GLOBUS_SUCCESS) {
        globus_gridftp_server_finished_transfer(h->op, rc);
    }
}

/*
 * Wraps the user_ version of the same. This function handles all the gridftp-
 * specific setup/teardown
 */
static void gfs_recv_callback(globus_gfs_operation_t op,
                                globus_result_t result,
                                globus_byte_t * buffer,
                                globus_size_t nbytes,
                                globus_off_t offset,
                                globus_bool_t eof,
                                void * user_arg) {
    GlobusGFSName(gfs_recv_callback);
    lstore_handle_t *h = (lstore_handle_t *) user_arg;

    globus_gridftp_server_update_bytes_written(op, offset, nbytes);
    int retval = user_recv_callback(h, (char *)buffer, nbytes, offset);

    globus_mutex_lock(h->mutex);
    if (retval == GLOBUS_FAILURE) {
        // Catchall for generic globus oopsies
        GlobusGFSErrorGenericStr(result, ("[lstore] Failure in recv_callback."));
        globus_gridftp_server_finished_transfer(h->op, result);
    } else if (retval != GLOBUS_SUCCESS) {
        // If we get something that's not GLOBUS_FAILURE or SUCCESS, treat it
        // like a real globus error string
        result = retval;
        globus_gridftp_server_finished_transfer(h->op, result);
    }
    globus_mutex_unlock(h->mutex);
}
