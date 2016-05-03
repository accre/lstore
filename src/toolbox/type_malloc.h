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

//*************************************************************
// type_malloc simple malloc wrapper using assert, type, andsize
//*************************************************************

#ifndef __TYPE_MALLOC_H_
#define __TYPE_MALLOC_H_

#include <assert.h>
#include "assert_result.h"
#include "stdlib.h"
#include "string.h"

#ifdef __cplusplus
extern "C" {
#endif

#define type_malloc_clear(var, type, count) type_malloc(var, type, count); type_memclear(var, type, count)

#define type_malloc(var, type, count) var = (type *)malloc(sizeof(type)*(count)); assert(var != NULL)
#define type_realloc(var, type, count) var = (type *)realloc(var, sizeof(type)*(count)); assert(var != NULL)
#define type_memclear(var, type, count) memset(var, 0, sizeof(type)*(count))

#ifdef __cplusplus
}
#endif


#endif

