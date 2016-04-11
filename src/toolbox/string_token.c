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

#define _log_module_index 112

#include <string.h>
#include <strings.h>
#include <stdio.h>
#include <math.h>
#include "fmttypes.h"
#include "type_malloc.h"

char NULL_TERMINATOR = '\0';

//*****************************************************************
// string_token - Same as strtok_r except that instead of returing
//   a NULL it returns a '\0' which won't cause sscanf to barf.
//   IF no more strings exist finished == 1;
//*****************************************************************

char *string_token(char *str, const char *sep, char **last, int *finished)
{
    char *token = strtok_r(str, sep, last);

    if (token == NULL) {
        token = &NULL_TERMINATOR;
        *finished = 1;
    } else {
        *finished = 0;
    }

    return(token);
}

//*****************************************************************
// argv2format - Converts an argument with \n, \t, etc to a proper
//     format for passing to printf
//*****************************************************************

char *argv2format(char *arg)
{
    int i, j, k, n, sub;
    char special[] = { 'a', 'b', 'f', 'n', 'r', 't', 'v', '\\' };
    char replace[] = { '\a', '\b', '\f', '\n', '\r', '\t', '\v', '\\' };
    char *str = strdup(arg);

    n = strlen(arg);
    if (n < 2) return(str);

    j = 0;
    for (i=0; i<n-1; i++) {
        sub = 0;
        if (arg[i] == '\\') {
            for (k=0; k<8; k++) {
                if (arg[i+1] == special[k]) break;
            }
            if (k<8) {
                sub = 1;
                str[j] = replace[k];
                j++;
                i++;
            } else {
                str[j] = arg[i];
                j++;
            }
        } else {
            str[j] = arg[i];
            j++;
        }
    }

    if (sub == 0) {
        str[j] = arg[n-1];
        j++;
    }
    str[j] = 0;
    return(str);
}


//*****************************************************************
// escape_string_token - Same as string_token except it supports
//   parsing escape sequences.
//*****************************************************************

char *escape_string_token(char *str, const char *delims, char escape_char, int compress_delims, char **last, int *finished)
{
    int n, ndata;
    char *ptr, *token;

    ptr = (str == NULL) ? *last : str;
    if (ptr == NULL) {
        *finished = 1;
        *last = NULL;
        return(&NULL_TERMINATOR);
    }

    ndata = strlen(ptr);

    //** Skip any beginning delims
    n = 0;
    if (compress_delims != 0) {
        while ((n < ndata) && (index(delims, ptr[n]) != NULL)) {
            n++;
        }
    }

    //** This is where the token starts
    token = &(ptr[n]);

    //** Cycle trough until we find the next delim
    while ((n < ndata) && (index(delims, ptr[n]) == NULL)) {
        if (ptr[n] == escape_char) n++;
        n++;
    }

    //** Now null terminate the token
    ptr[n] = '\0';

    //** Update the state
    if (n >= (ndata-1)) {
        *last = NULL;
        *finished = 1;
    } else {
        *last = &(ptr[n+1]);
        *finished = 0;
    }

    return(token);
}

//***********************************************************************
// escape_strchr - Same as strchr but supports escaping of text
//***********************************************************************

char *escape_strchr(char escape_char, char *data, char match)
{
    int n, ndata;

    //** Cycle trough until we find a match
    ndata = strlen(data);
    n = 0;
    while ((n < ndata) && (data[n] != match)) {
        if (data[n] == escape_char) n++;
        n++;
    }

    return(&(data[n]));
}


//***********************************************************************
// escape_count - Counts the number of escape characters
//***********************************************************************

int escape_count(char *special_chars, char escape_char, char *data)
{
    int i, n, count;

    n = strlen(data);
    count = 0;
    for (i=0; i<n; i++) {
        if ((data[i] == escape_char) || (index(special_chars, data[i]) != NULL)) count++;
    }

    return(count);
}

//***********************************************************************
//  escape_text - Simple routine to escape text in a string
//***********************************************************************

char *escape_text(char *special_chars, char escape_char, char *data)
{
    char *str;
    int n, i, j, nchar;

    n = escape_count(special_chars, escape_char, data);

    nchar = strlen(data);
    type_malloc_clear(str, char, nchar + n + 1);

    j = 0;
    for (i=0; i<nchar; i++) {
        if ((data[i] == escape_char) || (index(special_chars, data[i]) != NULL)) {
            str[j] = escape_char;
            j++;
        }

        str[j] = data[i];
        j++;
    }

    str[j] = '\0';

    return(str);
}

//***********************************************************************
//  unescape_text - Removes the escape text in a string
//***********************************************************************

char *unescape_text(char escape_char, char *data)
{
    char *str;
    int ndata, i, j;

    ndata = strlen(data);
    type_malloc_clear(str, char, ndata + 1);

    j = 0;
    i = 0;
    while (j<ndata) {
        if (escape_char == data[j]) {
            j++;
            if (j<ndata) {
                str[i] = data[j];
                j++;
            }
        } else {
            str[i] = data[j];
            j++;
        }

        i++;
    }

    str[i] = '\0';
    return(str);
}


//***********************************************************************
// split_token_into_number_and_scale - Splits the token into
//    the number portion of the string and returns the scale factor.
//    NOTE:  The provided token is MODIFIED!
//***********************************************************************

int64_t split_token_into_number_and_scale(char *token)
{
    int len = strlen(token);
    int64_t base = 1000;

    if (len == 0) return(1);  //** Nothing to do

    //** See which base we are using
    if ((token[len-1] == 'I') || (token[len-1] == 'i')) {
        base = 1024;
        token[len-1] = 0;
        len--;
        if (len == 0) return(base);
    }

    //** Now pick of the scale
    switch (token[len-1]) {
    case 'b' :
        base = 1;
        token[len-1] = 0;
        break;
    case 'K':
    case 'k':
        token[len-1] = 0;
        break;
    case 'M':
    case 'm':
        base = base * base;
        token[len-1] = 0;
        break;
    case 'G':
    case 'g':
        base = base * base * base;
        token[len-1] = 0;
        break;
    case 'T':
    case 't':
        base = base * base * base * base;
        token[len-1] = 0;
        break;
    default :
        base = 1;
    }

    return(base);
}

//***********************************************************************
// string_get_integer - Parses the string and returns the integer.
//      The string can include a scale unit
//***********************************************************************

int64_t string_get_integer(char *value)
{
    char *string;
    int64_t scale, n;

    string = strdup(value);
    scale = split_token_into_number_and_scale(string);
//printf("token=%s scale=" I64T "\n", string, scale);
    sscanf(string, I64T, &n);
    n = scale * n;
    free(string);

    return(n);
}

//***********************************************************************
// string_get_double - Parses the string and returns the double.
//      The string can include a scale unit
//***********************************************************************

double string_get_double(char *value)
{
    char *string;
    double scale, n;

    string = strdup(value);
    scale = split_token_into_number_and_scale(string);
//printf("token=%s scale=" I64T "\n", string, scale);
    sscanf(string, "%lf", &n);
    n = scale * n;
    free(string);

    return(n);
}

//***********************************************************************
//  pretty_print_int_with_scale - Stores the integer as a string using
//     the largest divisible scale factor. Buffer is used to
//     hold the converted number and must have enough characters to store
//     an unsigned 64-bit integer.
//
//     NOTE: If buffer==NULL then a new string is created and returned
//           which must be freed.
//***********************************************************************

char *pretty_print_int_with_scale(int64_t value, char *buffer)
{
    int64_t base, n;
    int i;
    char *unit="\0KMGTPE";

    if ((value % 1000) == 0) {
        base = 1000;
    } else if ((value % 1024) == 0) {
        base = 1024;
    } else {
        base = 1;
    }

    if (buffer == NULL) type_malloc(buffer, char, 30);

    if (base == 1) {
        sprintf(buffer, I64T, value);
        return(buffer);
    }

    n = value;
    for (i=0; i<7; i++) {
        if ((llabs(n) % base) != 0) break;
        n = n / base;
    }


    if (base == 1024) {
        if ( i == 0) {
            sprintf(buffer, I64T "%c ", n, unit[i]);
        } else {
            sprintf(buffer, I64T "%ci", n, unit[i]);
        }
    } else {
        sprintf(buffer, I64T "%c ", n, unit[i]);
    }

//printf("prettyprint: value=" I64T " (%s)\n", value, buffer);

    return(buffer);
}


//***********************************************************************
//  pretty_print_double_with_scale - Stores the double as a string using
//     the largest divisible scale factor. Buffer is used to
//     hold the converted number and must have enough characters to store
//     the double.
//
//     base is either 1, 1000, or 1024.
//
//     NOTE: If buffer==NULL then a new string is created and returned
//           which must be freed.
//***********************************************************************

char *pretty_print_double_with_scale(int base, double value, char *buffer)
{
    double n;
    int i;
    char *unit=" KMGTPE";

    if (buffer == NULL) type_malloc(buffer, char, 30);

    if (base == 1) {
        sprintf(buffer, "%lf", value);
        return(buffer);
    }

    n = value;
    for (i=0; i<7; i++) {
        if (fabs(n) < base) break;
        n = n / base;
    }


    if (base == 1024) {
        if (i == 0) {
            sprintf(buffer, "%7.3lf%c ", n, unit[i]);
        } else {
            sprintf(buffer, "%7.3lf%ci", n, unit[i]);
        }
    } else {
        sprintf(buffer, "%7.3lf%c ", n, unit[i]);
    }

    return(buffer);
}


//***********************************************************************
// string_trim - TRims the whitespace around a string
//***********************************************************************

char *string_trim(char *str)
{
    int i, n;
    char *start;

    i = 0;
    while (str[i] == ' ') {
        i++;
    }
    start = str + i;

    n =  strlen(start);
    i = n-1;
    while (start[i] == ' ') {
        i--;
    }
    start[i+1] = '\0';

    return(start);
}

