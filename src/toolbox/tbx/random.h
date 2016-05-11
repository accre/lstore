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
#ifndef ACCRE_RANDOM_H_INCLUDED
#define ACCRE_RANDOM_H_INCLUDED

#include <inttypes.h>
#include "tbx/toolbox_visibility.h"

#ifdef __cplusplus
extern "C" {
#endif

// Functions
TBX_API int tbx_random_shutdown();

TBX_API int tbx_random_bytes_get(void *buf, int nbytes);

TBX_API int tbx_random_startup();

TBX_API int64_t tbx_random_int64(int64_t lo, int64_t hi);

#ifdef __cplusplus
}
#endif

#endif
