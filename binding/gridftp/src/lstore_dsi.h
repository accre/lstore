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
 * @file lstore_dsi.c
 * Typedefs and function declarations needed for the LStore GridFTP DSI
 */


#include <lio/lio.h>

struct globus_l_gfs_lstore_handle_t {
    lio_config_t * fs;
};

typedef struct globus_l_gfs_lstore_handle_t globus_l_gfs_lstore_handle_t;

#endif
