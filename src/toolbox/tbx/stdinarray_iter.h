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
#ifndef ACCRE_STDINARRAY_ITER_H_INCLUDED
#define ACCRE_STDINARRAY_ITER_H_INCLUDED

#include <inttypes.h>
#include <tbx/visibility.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tbx_stdinarray_iter_s tbx_stdinarray_iter_t;

// Functions
TBX_API tbx_stdinarray_iter_t *tbx_stdinarray_iter_create(int argc, const char **argv);
TBX_API void tbx_stdinarray_iter_destroy(tbx_stdinarray_iter_t *it);
TBX_API char *tbx_stdinarray_iter_next(tbx_stdinarray_iter_t *it);
TBX_API char *tbx_stdinarray_iter_last(tbx_stdinarray_iter_t *it);
TBX_API char *tbx_stdinarray_iter_peek(tbx_stdinarray_iter_t *it, int ahead);
#ifdef __cplusplus
}
#endif

#endif
