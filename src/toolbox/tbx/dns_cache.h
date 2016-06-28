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

#pragma once
#ifndef ACCRE_DNS_CACHE_H_INCLUDED
#define ACCRE_DNS_CACHE_H_INCLUDED

#include <tbx/visibility.h>

#ifdef __cplusplus
extern "C" {
#endif

// Functions
TBX_API int tbx_dnsc_lookup(const char * name, char * byte_addr, char * ip_addr);
TBX_API int tbx_dnsc_shutdown();
TBX_API int tbx_dnsc_startup();
TBX_API int tbx_dnsc_startup_sized(int size);

// Preprocessor macros
#define DNS_ADDR_MAX 16
#define DNS_IPV4  0
#define DNS_IPV6  1

#ifdef __cplusplus
}
#endif

#endif
