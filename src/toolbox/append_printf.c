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

#define _log_module_index 102

#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>

#include "tbx/append_printf.h"

//***************************************************************************
// append_printf_valist - Appends data to the end of a string and also updates the
//     length.
//***************************************************************************

int append_printf_valist(char *buffer, int *used, int nbytes, const char *fmt, va_list args)
{
    int n, nleft;

    nleft = nbytes - *used;
    if (nleft <= 0) return(-1);

    n = vsnprintf(&(buffer[*used]), nleft, fmt, args);

    *used = *used + n;

    if (*used >= nbytes) n = -n;

    return(n);
}

//***************************************************************************
// tbx_append_printf - Appends data to the end of a string and also updates the
//     length.
//***************************************************************************

int tbx_append_printf(char *buffer, int *used, int nbytes, const char *fmt, ...)
{
    int n;
    va_list args;

    va_start(args, fmt);
    n = append_printf_valist(buffer, used, nbytes, fmt, args);
    va_end(args);

    return(n);
}

//***************************************************************************
// tbx_alloc_append_printf - Appends data to the end of a string and automatically grows
//     the the buffer and updates the length
//***************************************************************************

int tbx_alloc_append_printf(char **buffer, int *used, int *nbytes, const char *fmt, ...)
{
    va_list args;
    int n;
    int len = *used;

    if (*nbytes == 0) {
        *nbytes = 256;
        *buffer = malloc(*nbytes);
    }

    va_start(args, fmt);
again:
    n = append_printf_valist(*buffer, used, *nbytes, fmt, args);
    if (n < 0) {
        *used = len;
        *nbytes = 2 * (*nbytes) + n;
        *buffer = realloc(*buffer, *nbytes);
        goto again;
    }
    va_end(args);

    return(n);
}
