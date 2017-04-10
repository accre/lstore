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
    int last_used;      //** Already processed the last argument
    int last_used_hit;  //** Flags that the last arg has been used
    int curr_last;      //** Current is the last arg
    int next_last;      //** Next is the last arg
    const char **argv;  //** Array of paths
    const char *last;   //** Last argument if available
    char *curr;         //** Current element to be returned
    char *next;         //** Element to return after curr
};

char *sa_next(tbx_stdinarray_iter_t *it);

//*************************************************************************
//  tbx_stdinarray_iter_create - Create stdin/list iterator
//*************************************************************************

tbx_stdinarray_iter_t *tbx_stdinarray_iter_create(int argc, const char **argv)
{
    tbx_stdinarray_iter_t *it;

    tbx_type_malloc_clear(it, tbx_stdinarray_iter_t, 1);
    it->argc = argc;
    it->argv = argv;
    it->current = 0;
    it->from_stdin = 0;

    if (it->argc == 0) return(it);  //** Nothing left to do

    //** check on the last arg
    if (strcmp(it->argv[it->argc-1], SARRAY_STDIN) != 0) { //** Normal line
        it->last = it->argv[argc-1];
    }

    //** Get the peek items
    it->curr = sa_next(it);
    if (it->last_used_hit) {
        it->curr_last = 1;
    } else {
        it->next = sa_next(it);
        if (it->last_used_hit) it->next_last = 1;
    }

    return(it);
}

//*************************************************************************
// tbx_stdinarray_iter_last - Returns the last argument if available.
//     IF the last argument is "-" NULL is returned since it is
//     impossible to tell from stdin without buffering.
//
//     Calling this routine also removes the last argument from being
//     returned via the next() routine
//*************************************************************************

char *tbx_stdinarray_iter_last(tbx_stdinarray_iter_t *it)
{
    if (it->last_used == 0) {
        it->argc--;
        it->last_used = 1;
    }

    if (it->curr_last == 1) {
        free(it->curr);
        it->curr = NULL;
    }
    if (it->next_last == 1) {
        free(it->next);
        it->next = NULL;
    }
    return((it->last) ? strdup(it->last) : NULL);
}

//*************************************************************************
//   tbx_stdinarray_iter_peek - Peeks ahead in the stream to see what
//       will be returned in upcoming next() calls.
//       You can only peek ahead 1 or 2 slots.  The data returned is 
//       an internally managed string and should not be destroyed.
//*************************************************************************

char *tbx_stdinarray_iter_peek(tbx_stdinarray_iter_t *it, int ahead)
{
    if (ahead == 1) {
        return(it->curr);
    } else if (ahead == 2) {
        return(it->next);
    }

    return(NULL);
}

//*************************************************************************
// tbx_stdinarray_iter_create - Destroys the stdin/list iterator
//*************************************************************************

void tbx_stdinarray_iter_destroy(tbx_stdinarray_iter_t *it)
{
   if (it->curr) free(it->curr);
   if (it->next) free(it->next);
   free(it);
}

//*************************************************************************
//  tbx_stdinarray_iter_next - Returns the next path from either argv or stdin
//    IF no arguments are left then NULL is returned.
//
//    NOTE:  The caller is responsible for destroying the return argument
//*************************************************************************

char *sa_next(tbx_stdinarray_iter_t *it)
{
    char stmp[8192];
    char *p;

next_from_list:
    if (it->from_stdin == 0) {
        if (it->current >= it->argc) return(NULL);  //** Nothing left to do

        if (strcmp(it->argv[it->current], SARRAY_STDIN) != 0) { //** Normal line
            p = strdup(it->argv[it->current]);
            it->current++;
            if (it->current >= it->argc) it->last_used_hit = 1;
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

//*************************************************************************
//  tbx_stdinarray_iter_next - Returns the next path from either argv or stdin
//    IF no arguments are left then NULL is returned.
//
//    NOTE:  The caller is responsible for destroying the return argument
//*************************************************************************

char *tbx_stdinarray_iter_next(tbx_stdinarray_iter_t *it)
{
    char *path;

    path = it->curr;
    it->curr = it->next;  it->curr_last = it->next_last;
    it->next = sa_next(it);
    it->next_last = ((it->curr_last == 0) && (it->last_used_hit == 1)) ? 1 : 0;

    return(path);
}
