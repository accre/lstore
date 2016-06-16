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

//*************************************************************************
//*************************************************************************

#include <tbx/iniparse.h>

#include "cache_priv.h"
#include "data_service_abstract.h"

#ifndef __CACHE_ROUND_ROBIN_H_
#define __CACHE_ROUND_ROBIN_H_


#ifdef __cplusplus
extern "C" {
#endif

#define CACHE_TYPE_ROUND_ROBIN "round_robin"

cache_t *round_robin_cache_create(void *arg, data_attr_t *da, int timeout);
cache_t *round_robin_cache_load(void *arg, tbx_inip_file_t *ifd, char *section, data_attr_t *da, int timeout);


#ifdef __cplusplus
}
#endif

#endif


