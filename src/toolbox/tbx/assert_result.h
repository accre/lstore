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

/*! \file
 * Wrap assert to support side-effects. This is a temporary utility while we
 * remove those cases
 */

#ifdef __cplusplus
extern "C" {
#endif

// Preprocessor macros
#ifndef NDEBUG
/*! \brief Evaluates an expression. If assertions are enabled, assert the result
 *         is equal to the result
 *  @param eval_this Expression to evaluate
 *  @param result Desired result of expression
 */
#define assert_result(eval_this, result) assert((eval_this) == result)
/*! \brief Evaluates an expression. If assertions are enabled, assert the result
 *         is not equal to the result
 *  @param eval_this Expression to evaluate
 *  @param result Undesired result of expression
 */
#define assert_result_not(eval_this, result) assert((eval_this) != result)
/*! \brief Evaluates an expression. If assertions are enabled, assert the result
 *         is not NULL
 *  @param eval_this Expression to evaluate
 */
#define assert_result_not_null(eval_this) assert((eval_this) != NULL)
/*! \brief Always evaluate an expression. Perform an assertion if asserts are
 *         enabled
 *  @param expr Expression to evaluate and possibly assert
 */
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
