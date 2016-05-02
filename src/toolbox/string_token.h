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

#ifndef __STRING_TOKEN_H_
#define __STRING_TOKEN_H_
#include "tbx/toolbox_visibility.h"

#ifdef __cplusplus
extern "C" {
#endif
#include <inttypes.h>
extern TBX_API char *string_token(char *str, const char *sep, char **last, int *finished);
TBX_API char *argv2format(char *arg);
TBX_API char *escape_string_token(char *str, const char *delims, char escape_char, int compress_delims, char **last, int *finished);
int escape_count(char *special_chars, char escape_char, char *data);
TBX_API char *escape_text(char *special_chars, char escape_char, char *data);
TBX_API char *unescape_text(char escape_char, char *data);
char *escape_strchr(char escape_char, char *data, char match);
char *string_trim(char *str);
int64_t split_token_into_number_and_scale(char *token);
TBX_API int64_t string_get_integer(char *value);
TBX_API double string_get_double(char *value);
TBX_API char *pretty_print_int_with_scale(int64_t value, char *buffer);
TBX_API char *pretty_print_double_with_scale(int base, double value, char *buffer);

#ifdef __cplusplus
}
#endif

#endif





