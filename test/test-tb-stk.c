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
#include "task.h"
#include <tbx/string_token.h>
#include <string.h>

TEST_IMPL(tb_stk_escape_text) {
    char *ret;
    ASSERT(strlen(ret = tbx_stk_escape_text("a", '$', "1")) == 1);
    free(ret);
    ASSERT(strlen(ret = tbx_stk_escape_text("a", '$', "a")) == 2);
    free(ret);
    ASSERT(strlen(ret = tbx_stk_escape_text("a", '$', "$")) == 2);
    free(ret);
    ASSERT(strlen(ret = tbx_stk_escape_text("a", '$', "$a")) == 4);
    free(ret);
    ASSERT(strlen(ret = tbx_stk_escape_text("a", '$', "a$")) == 4);
    free(ret);

    return 0;
}
