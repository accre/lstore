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
#ifndef ACCRE_ASSERT_RESULT_H_INCLUDED
#define ACCRE_ASSERT_RESULT_H_INCLUDED

#include <assert.h>

#ifdef __cplusplus
extern "C" {
#endif

// Preprocessor macros
#ifndef NDEBUG
#define assert_result(eval_this, expected_result) assert((eval_this) == expected_result)
#define assert_result_not(eval_this, result) assert((eval_this) != result)
#define assert_result_not_null(eval_this) assert((eval_this) != NULL)
#define ASSERT_ALWAYS(expr) assert(expr)
#else
#define assert_result(eval_this, expected_result) (eval_this)
#define assert_result_not(eval_this, result) (eval_this)
#define assert_result_not_null(eval_this) (eval_this)
#define ASSERT_ALWAYS(expr) do { expr } while (0)
#endif


#ifdef __cplusplus
}
#endif

#endif
