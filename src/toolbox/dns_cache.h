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

//***************************************
//***************************************

#ifndef __DNS_CACHE_H__
#define __DNS_CACHE_H__

#ifdef __cplusplus
extern "C" {
#endif
#include "tbx/toolbox_visibility.h"

//#define DNS_IPV4_LEN 4
//#define DNS_IPV6_LEN 16
#define DNS_ADDR_MAX 4
#define DNS_IPV4  0
#define DNS_IPV6  1

TBX_API int lookup_host(const char * name, char * byte_addr, char * ip_addr);
TBX_API void dns_cache_init(int);
TBX_API void finalize_dns_cache();

#ifdef __cplusplus
}
#endif

#endif


