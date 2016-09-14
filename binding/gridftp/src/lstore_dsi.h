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

#ifndef LSTORE_DSI_H_INCLUDED
#define LSTORE_DSI_H_INCLUDED

/**
 * @file lstore_dsi.h
 * Typedefs and function declarations needed for the LStore GridFTP DSI
 */

#include <globus_gridftp_server.h>
#include <lio/lio.h>

// Typedefs
typedef struct lstore_handle_t lstore_handle_t;

// Functions
/**
 * Performs initialization of the LStore plugin
 *
 * @returns 0 on success, errno otherwise
 */
int activate();

/**
 * Performs shutdown of the LStore plugin
 *
 * @returns 0 on success, errno otherwise
 */
int deactivate();

/**
 * Destroys a globus stat array
 * @param stat_array Array to be destroyed
 * @param stat_count Number of elements in array
 */
void destroy_stat(globus_gfs_stat_t * stat_array, int stat_count);

/**
 * Converts a GridFTP to a path within LStore
 * @param path Input path
 * @param prefix LStore mount prefix
 * @returns LStore path if it exists, NULL otherwise
 */
const char *path_to_lstore(const char *prefix, const char *path);

/**
 * Handles the LStore half of stat'ing a file/directory
 * @param stack Stack to fill with stat structs
 * @param path Filename to stat
 * @param file_only If true, only return files, not entire directories
 * @returns zero on success, error otherwise
 */
int plugin_stat(lstore_handle_t *h, tbx_stack_t *stack, const char *path, int file_only);

/**
 * Tranfers a POSIX stat struct into a globus stat struct
 * @param stat_object Globus target object
 * @param fileInfo POSIX source object
 * @param filename Path to described stat info
 * @param symlink_target Symlinked target
 */
void transfer_stat(globus_gfs_stat_t * stat_object,
                                        struct stat * fileInfo,
                                        const char * filename,
                                        const char * symlink_target);
/**
 * Begins a user session
 * @param h Session handle
 * @returns 0 on success, errno otherwise
 */
int user_connect(lstore_handle_t *h, globus_gfs_operation_t op);

/**
 * Stat a file/directory
 * @param h Session handle
 * @param info Information on what Globus wants
 * @param ret Globus-specific struct to fill
 * @param ret_count Number of elements in ret
 * @returns 0 on success, errno otherwise
 */
int user_stat(lstore_handle_t *h,
                globus_gfs_stat_info_t *info,
                globus_gfs_stat_t ** ret,
                int *ret_count);

/**
 * Closes a user session
 * @param h Session handle
 * @returns 0 on success, errno otherwise
 */
int user_close(lstore_handle_t *h);

// Structures
struct lstore_handle_t {
    globus_gfs_operation_t op;
    lio_fd_t *fd;
    char *prefix;
};

// Globals

//* Needed for testing to locate the function pointers
extern globus_module_descriptor_t globus_gridftp_server_lstore_module;
extern globus_gfs_storage_iface_t globus_l_gfs_lstore_dsi_iface;

#endif
