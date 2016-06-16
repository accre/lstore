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

#ifndef __CACHE_H_
#define __CACHE_H_

#include "cache_amp.h"
#include "cache_priv.h"
#include "cache_round_robin.h"

#define CACHE_PRINT
#define CACHE_PRINT_LOCK
#define CACHE_LOAD_AVAILABLE "cache_load_available"
#define CACHE_CREATE_AVAILABLE "cache_create_available"

void print_cache_table(int dolock);
typedef cache_t *(cache_load_t)(void *arg, tbx_inip_file_t *ifd, char *section, data_attr_t *da, int timeout);
typedef cache_t *(cache_create_t)(void *arg, data_attr_t *da, int timeout);

#ifdef __cplusplus
extern "C" {
#endif

#ifdef __cplusplus
}
#endif

#endif


