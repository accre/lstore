/*
Advanced Computing Center for Research and Education Proprietary License
Version 1.0 (April 2006)

Copyright (c) 2006, Advanced Computing Center for Research and Education,
 Vanderbilt University, All rights reserved.

This Work is the sole and exclusive property of the Advanced Computing Center
for Research and Education department at Vanderbilt University.  No right to
disclose or otherwise disseminate any of the information contained herein is
granted by virtue of your possession of this software except in accordance with
the terms and conditions of a separate License Agreement entered into with
Vanderbilt University.

THE AUTHOR OR COPYRIGHT HOLDERS PROVIDES THE "WORK" ON AN "AS IS" BASIS,
WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, TITLE, FITNESS FOR A PARTICULAR
PURPOSE, AND NON-INFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Vanderbilt University
Advanced Computing Center for Research and Education
230 Appleton Place
Nashville, TN 37203
http://www.accre.vanderbilt.edu
*/

#ifndef __STRING_TOKEN_H_
#define __STRING_TOKEN_H_

#ifdef __cplusplus
extern "C" {
#endif

char *string_token(char *str, const char *sep, char **last, int *finished);
char *argv2format(char *arg);
char *escape_string_token(char *str, const char *delims, char escape_char, int compress_delims, char **last, int *finished);
int escape_count(char special_chars, char escape_char, char *data);
char *escape_text(char *special_chars, char escape_char, char *data);
char *unescape_text(char escape_char, char *data);
char *escape_strchr(char escape_char, char *data, char match);
char *string_trim(char *str);
int64_t split_token_into_number_and_scale(char *token);
char *pretty_print_int_with_scale(int64_t value, char *buffer);
char *pretty_print_double_with_scale(int base, double value, char *buffer);

#ifdef __cplusplus
}
#endif

#endif





