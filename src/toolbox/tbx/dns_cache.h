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

/*! \file
 * Wrappers for cached DNS resolution
 */

#pragma once
#ifndef ACCRE_DNS_CACHE_H_INCLUDED
#define ACCRE_DNS_CACHE_H_INCLUDED

#include "tbx/toolbox_visibility.h"

#ifdef __cplusplus
extern "C" {
#endif

// Functions
/*! \brief Resolve a hostname via DNS, caching results
 *  \param name Hostname to resolve
 *  \param byte_addr Resulting IP address in binary form
 *  \param ip_addr Resulting IP address in text form
 *  \return Zero on success
 *
 *  The caller is responsible for freeing ip_addr and byte_addr
 */
TBX_API int tbx_dnsc_lookup(const char * name, char * byte_addr, char * ip_addr);
/*! @brief Performs global initialization. Called automatically on startup
 */
TBX_API int tbx_dnsc_shutdown();
/*! @brief Performs global deinitialization. Called automatically on shutdown
 */
TBX_API int tbx_dnsc_startup();

// Preprocessor macros
#define DNS_ADDR_MAX 4
#define DNS_IPV4  0
#define DNS_IPV6  1

#ifdef __cplusplus
}
#endif

#endif
