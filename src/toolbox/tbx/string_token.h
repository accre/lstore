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
#ifndef ACCRE_STRING_TOKEN_H_INCLUDED
#define ACCRE_STRING_TOKEN_H_INCLUDED

#include <inttypes.h>
#include <tbx/visibility.h>

#ifdef __cplusplus
extern "C" {
#endif

// Functions
TBX_API char *tbx_stk_argv2format(char *arg);
TBX_API char *tbx_stk_escape_strchr(char escape_char, char *data, char match);
TBX_API char *tbx_stk_escape_string_token(char *str,
                                            const char *delims,
                                            char escape_char,
                                            int compress_delims,
                                            char **last,
                                            int *finished);
TBX_API char *tbx_stk_escape_text(char *special_chars,
                                    char escape_char,
                                    char *data);
TBX_API char *tbx_stk_pretty_print_double_with_scale(int base,
                                                        double value,
                                                        char *buffer);
TBX_API char *tbx_stk_pretty_print_int_with_scale(int64_t value, char *buffer);
TBX_API double tbx_stk_string_get_double(char *value);
TBX_API int64_t tbx_stk_string_get_integer(char *value);
TBX_API char *tbx_stk_string_token(char *str,
                                    const char *sep,
                                    char **last,
                                    int *finished);
TBX_API char *tbx_stk_string_trim(char *str);
TBX_API char *tbx_stk_unescape_text(char escape_char, char *data);

#ifdef __cplusplus
}
#endif

#endif
