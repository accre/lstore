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

#include <apr.h>
#include <apr_atomic.h>
#include <tbx/visibility.h>

#include <inttypes.h>
#include <tbx/fmttypes.h>

#ifdef __cplusplus
extern "C" {
#endif

#define AIT I64T  //** printf format type

// Types
typedef int64_t tbx_atomic_int_t;

// Functions
TBX_API int tbx_atomic_counter(tbx_atomic_int_t *counter);

TBX_API void tbx_atomic_shutdown();

TBX_API int tbx_atomic_global_counter();

TBX_API void tbx_atomic_startup();

TBX_API int *tbx_a_thread_id_ptr();

// Preprocessor macros

#define tbx_atomic_inc(v) __atomic_fetch_add(&(v), 1, __ATOMIC_SEQ_CST)
#define tbx_atomic_dec(v) __atomic_sub_fetch(&(v), 1, __ATOMIC_SEQ_CST)
#define tbx_atomic_set(v, n) __atomic_store_n(&(v), n, __ATOMIC_SEQ_CST)
#define tbx_atomic_get(v) __atomic_load_n(&(v),  __ATOMIC_SEQ_CST)
#define tbx_atomic_exchange(v, n) __atomic_exchange_n(&(v), n, __ATOMIC_SEQ_CST)
#define tbx_atomic_thread_id (*tbx_a_thread_id_ptr())

#ifdef __cplusplus
}
#endif

#endif
