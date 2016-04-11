#ifndef ASSERT_RESULT_H
#define ASSERT_RESULT_H

#include <assert.h>
#include "assert_result.h"

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

#endif /* ASSERT_RESULT_H */
