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

#ifndef __ATOMIC_COUNTER_H_
#define __ATOMIC_COUNTER_H_

#include "tbx/toolbox_visibility.h"
#include "apr_atomic.h"

typedef apr_uint32_t tbx_atomic_unit32_t;

#define atomic_inc(v) apr_atomic_inc32(&(v))
#define atomic_dec(v) apr_atomic_dec32(&(v))
#define atomic_set(v, n) apr_atomic_set32(&(v), n)
#define atomic_get(v) apr_atomic_read32(&(v))
#define atomic_exchange(a, v) apr_atomic_xchg32(&a, v)

TBX_API int atomic_global_counter();
TBX_API int atomic_counter(tbx_atomic_unit32_t *counter);

extern TBX_API int *_a_thread_id_ptr();
#define atomic_thread_id (*_a_thread_id_ptr())

TBX_API void atomic_init();
TBX_API void atomic_destroy();

#endif


