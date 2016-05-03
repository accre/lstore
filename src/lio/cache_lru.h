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

#ifndef __CACHE_LRU_H_
#define __CACHE_LRU_H_

#include "cache_priv.h"
#include "iniparse.h"


#ifdef __cplusplus
extern "C" {
#endif


#define CACHE_TYPE_LRU "lru"

cache_t *lru_cache_create(void *arg, data_attr_t *da, int timeout);
cache_t *lru_cache_load(void *arg, tbx_inip_file_t *ifd, char *section, data_attr_t *da, int timeout);


#ifdef __cplusplus
}
#endif

#endif


