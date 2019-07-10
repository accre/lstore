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

//***********************************************************************
// Routines for managing the segment loading framework
//***********************************************************************

#pragma once
#ifndef ACCRE_RANGE_STACK_H_INCLUDED
#define ACCRE_RANGE_STACK_H_INCLUDED

#include <stddef.h>
#include <tbx/visibility.h>
#include <tbx/tbx_decl.h>

#include <tbx/stack.h>

#ifdef __cplusplus
extern "C" {
#endif

//** Functions
TBX_API tbx_stack_t *tbx_range_stack_string2range(char *string, char *range_delimiter);
TBX_API char *tbx_range_stack_range2string(tbx_stack_t *range_stack, char *range_delimiter);
TBX_API void tbx_range_stack_merge(tbx_stack_t **range_stack_ptr, int64_t *new_rng);
TBX_API void tbx_range_stack_merge2(tbx_stack_t **range_stack_ptr, int64_t lo, int64_t hi);

TBX_API int tbx_range_stack_test();

#ifdef __cplusplus
}
#endif

#endif
