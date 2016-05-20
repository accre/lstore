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

//**********************************************************
//**********************************************************


#ifndef __DEBUG_H_
#define __DEBUG_H_

#ifdef _ENABLE_DEBUG

#include <stdio.h>
#include <log.h>

//extern FILE *_debug_fd;   //**Default all I/O to stdout
//extern int _debug_level;

#define debug_code(a) a
#define debug_printf(n, ...) log_printf(n, __VA_ARGS__)
#define set_debug_level(n) tbx_set_log_level(n)
#define flush_debug() tbx_log_flush()
#define debug_level() tbx_log_level()

#else

#define debug_code(a)
#define debug_printf(n, ...)
#define set_debug_level(n)
#define flush_debug()
#define debug_level()

#endif
#endif
