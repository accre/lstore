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

#include <stdlib.h>
#include <tbx/log.h>

// Preprocessor macros
#define FATAL_UNLESS(expr)                                      \
    do {                                                        \
        if (!(expr)) {                                          \
            log_printf(-1, "Fatal Error in %s at %s:%d\n",      \
                            __FUNCTION__, __FILE__, __LINE__);  \
            abort();                                            \
            exit(EXIT_FAILURE);                                 \
        }                                                       \
    } while(0)

#define WARN_UNLESS(expr)                                       \
    do {                                                        \
        if ((expr) != 0) {                                      \
            log_printf(0, "Warning in %s at %s:%d\n",           \
                            __FUNCTION__, __FILE__, __LINE__);  \
        }                                                       \
    } while(0)

#define assert_result(eval_this, expected_result) \
            FATAL_UNLESS((eval_this) == (expected_result))
#define assert_result_not(eval_this, result) \
            FATAL_UNLESS((eval_this) != (result))
#define assert_result_not_null(eval_this) \
            FATAL_UNLESS((eval_this) != NULL)

#ifdef __cplusplus
}
#endif

#endif
