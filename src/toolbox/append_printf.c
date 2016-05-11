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

#include <stdarg.h>
#include "tbx/log.h"
#include "tbx/append_printf.h"

//***************************************************************************
// append_printf - Appends data to the end of a string and also updates the
//     length.
//***************************************************************************

int tbx_append_printf(char *buffer, int *used, int nbytes, const char *fmt, ...)
{
    va_list args;
    int n, nleft;

    nleft = nbytes - *used;
    if (nleft <= 0) return(-1);

    va_start(args, fmt);
    n = vsnprintf(&(buffer[*used]), nleft, fmt, args);
    va_end(args);

    *used = *used + n;

    if (*used >= nbytes) n = -1;

    return(n);
}
