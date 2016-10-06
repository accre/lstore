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

#define _log_module_index 189

#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/assert_result.h>
#include <tbx/fmttypes.h>
#include <tbx/log.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>
#include <tbx/stdinarray_iter.h>
#include <unistd.h>

//-------------------------------------------------------------------------
//------------------- Stdin/list Iterators --------------------------------
//   These routines are designed to simplify processing arrays of globs
//   or other paths gotten from the command line and/or from stdin
//   To specify stdin use the "-" as a list element.  At that pint
//   lines come from stdin until EOF is reached at which point it switches
//   to getting elements from the list.
//
//   The caller is repsonsible for destroying the return elements
//-------------------------------------------------------------------------
//-------------------------------------------------------------------------

#define SARRAY_STDIN "-"  //** Stdin list name

struct tbx_stdinarray_iter_s {  //** Stdin/list iterator
    int argc;           //** Size of argv
    int current;        //** Current argv index
    int from_stdin;     //** Processing args from stdin.
    const char **argv;  //** Array of paths
};

//*************************************************************************
//  tbx_stdinarray_iter_create - Create stdin/list iterator
//*************************************************************************

tbx_stdinarray_iter_t *tbx_stdinarray_iter_create(int argc, const char **argv)
{
    tbx_stdinarray_iter_t *it;

    tbx_type_malloc(it, tbx_stdinarray_iter_t, 1);
    it->argc = argc;
    it->argv = argv;
    it->current = 0;
    it->from_stdin = 0;

    return(it);
}

//*************************************************************************
// tbx_stdinarray_iter_create - Destroys the stdin/list iterator
//*************************************************************************

void tbx_stdinarray_iter_destroy(tbx_stdinarray_iter_t *it)
{
   free(it);
}

//*************************************************************************
//  tbx_stdinarray_iter_next - Returns the next path from either argv or stdin
//    IF no arguments are left then NULL is returned.
//
//    NOTE:  The caller is responsible for destroying the return argument
//*************************************************************************

char *tbx_stdinarray_iter_next(tbx_stdinarray_iter_t *it)
{
    char stmp[8192];
    char *p;

next_from_list:
    if (it->from_stdin == 0) {
        if (it->current >= it->argc) return(NULL);  //** Nothing left to do

        if (strcmp(it->argv[it->current], SARRAY_STDIN) != 0) { //** Normal line
            p = strdup(it->argv[it->current]);
            it->current++;
            return(p);
        }

        it->from_stdin = 1;  //** Got a request for stdin
        it->current++;
    }

    //** If we made it here then we are processing text from stdin
    p = fgets(stmp, sizeof(stmp), stdin);
    if (p) {
        p[strlen(p)-1] = 0;  //** Truncate the \n
        return(strdup(p));  //** Return
    } else {
      it->from_stdin = 0;
      goto next_from_list;
    }

    return(NULL);  //** Nothing left to do
}

