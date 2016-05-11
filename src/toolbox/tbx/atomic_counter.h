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
#ifndef ACCRE_ATOMIC_COUNTER_H_INCLUDED
#define ACCRE_ATOMIC_COUNTER_H_INCLUDED

#include <apr_atomic.h>
#include "tbx/toolbox_visibility.h"

#ifdef __cplusplus
extern "C" {
#endif

// Types
typedef apr_uint32_t tbx_atomic_unit32_t;

// Functions
TBX_API int tbx_atomic_counter(tbx_atomic_unit32_t *counter);

TBX_API void tbx_atomic_shutdown();

TBX_API int tbx_atomic_global_counter();

TBX_API void tbx_atomic_startup();

TBX_API int *tbx_a_thread_id_ptr();

// Preprocessor macros
#define tbx_atomic_inc(v) apr_atomic_inc32(&(v))
#define tbx_atomic_dec(v) apr_atomic_dec32(&(v))
#define tbx_atomic_set(v, n) apr_atomic_set32(&(v), n)
#define tbx_atomic_get(v) apr_atomic_read32(&(v))
#define tbx_atomic_exchange(a, v) apr_atomic_xchg32(&a, v)
#define tbx_atomic_thread_id (*tbx_a_thread_id_ptr())

#ifdef __cplusplus
}
#endif

#endif
