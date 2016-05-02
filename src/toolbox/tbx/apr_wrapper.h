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
#ifndef ACCRE_APR_WRAPPER_H_INCLUDED
#define ACCRE_APR_WRAPPER_H_INCLUDED

#include "tbx/assert_result.h"
#include "tbx/toolbox_visibility.h"

#ifdef __cplusplus
extern "C" {
#endif

// Precompiler macros
#define tbx_thread_create_warn(err, thread, attr, thread_fn, arg, mpool) \
  if ((err = apr_thread_create(thread, attr, thread_fn, arg, mpool)) != APR_SUCCESS) { \
     log_printf(0, "WARN: Possible deadlock can occur!  Failed launching new thread!  Increase maxproc in limit/ulimit.\n"); \
     fprintf(stderr, "WARN: Possible deadlock can occur!  Failed launching new thread!  Increase maxproc in limit/ulimit.\n"); \
  }

#define tbx_thread_create_assert(thread, attr, thread_fn, arg, mpool) \
  assert_result(apr_thread_create(thread, attr, thread_fn, arg, mpool), APR_SUCCESS);

#ifdef __cplusplus
}
#endif

#endif
