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
#ifndef ACCRE_PIGEON_HOLE_H_INCLUDED
#define ACCRE_PIGEON_HOLE_H_INCLUDED

#include <tbx/visibility.h>

#ifdef __cplusplus
extern "C" {
#endif

// Types
typedef struct tbx_ph_iter_t tbx_ph_iter_t;

typedef struct tbx_ph_t tbx_ph_t;

TBX_API tbx_ph_t *tbx_ph_new(const char *name, int size);
TBX_API void tbx_ph_destroy(tbx_ph_t *ph);
TBX_API int tbx_ph_reserve(tbx_ph_t *ph);
TBX_API tbx_ph_iter_t tbx_ph_iter_init(tbx_ph_t *ph);
TBX_API int tbx_ph_next(tbx_ph_iter_t *pi);
TBX_API int tbx_ph_used(tbx_ph_t *ph);
TBX_API int tbx_ph_free(tbx_ph_t *ph);
TBX_API void tbx_ph_release(tbx_ph_t *ph, int slot);

#ifdef __cplusplus
}
#endif

#endif
