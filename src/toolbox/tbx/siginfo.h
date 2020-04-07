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
#ifndef ACCRE_SIGINFO_H_INCLUDED
#define ACCRE_SIGINFO_H_INCLUDED

#include <apr_time.h>
#include <apr_thread_mutex.h>
#include <tbx/visibility.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void (*tbx_siginfo_fn_t)(void *arg, FILE *fd);

// Functions
TBX_API void tbx_siginfo_handler_add(int signal, tbx_siginfo_fn_t fn, void *arg);
TBX_API void tbx_siginfo_handler_remove(int signal, tbx_siginfo_fn_t fn, void *arg);
TBX_API void tbx_siginfo_install(char *fname, int signal);
TBX_API void tbx_siginfo_shutdown();

#ifdef __cplusplus
}
#endif

#endif
