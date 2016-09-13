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
typedef struct globus_l_gfs_lstore_handle_t globus_l_gfs_lstore_handle_t;

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


// Structures
struct globus_l_gfs_lstore_handle_t {
    lio_config_t * fs;
};

// Globals

//* Needed for testing to locate the function pointers
extern globus_module_descriptor_t globus_gridftp_server_lstore_module;
extern globus_gfs_storage_iface_t globus_l_gfs_lstore_dsi_iface;

#endif
