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

#include <gop/gop.h>
#include <gop/mq.h>
#include <gop/opque.h>
#include <gop/tp.h>
#include <gop/types.h>
#include <lio/segment.h>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/assert_result.h>
#include <tbx/fmttypes.h>
#include <tbx/log.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>
#include <unistd.h>

#include "authn.h"
#include "blacklist.h"
#include "cache.h"
#include "ex3.h"
#include "ex3/types.h"
#include "lio.h"
#include "os.h"
#include "os/file.h"


//***********************************************************************
// Misc Core LIO functionality
//***********************************************************************

//***********************************************************************
//  lio_parse_path - Parses a path ofthe form: user@service:/my/path
//        The user and service are optional
//
//  Returns 1 if @: are encountered and 0 otherwise
//***********************************************************************

int lio_parse_path(char *startpath, char **user, char **service, char **path)
{
    int i, j, found, n, ptype;

    *user = *service = *path = NULL;
    n = strlen(startpath);
    ptype = 0;
    found = -1;
    for (i=0; i<n; i++) {
        if (startpath[i] == '@') {
            found = i;
            ptype = 1;
            break;
        }
    }

    if (found == -1) {
        *path = strdup(startpath);
        return(ptype);
    }

    if (found > 0) { //** Got a valid user
        *user = strndup(startpath, found);
    }

    j = found+1;
    found = -1;
    for (i=j; i<n; i++) {
        if (startpath[i] == ':') {
            found = i;
            break;
        }
    }

    if (found == -1) {  //**No path.  Just a service
        if (j < n) {
            *service = strdup(&(startpath[j]));
        }
        return(ptype);
    }

    i = found - j;
    *service = (i == 0) ? NULL : strndup(&(startpath[j]), i);

    //** Everything else is the path
    j = found + 1;
    if (found < n) {
        *path = strdup(&(startpath[j]));
    }

    return(ptype);
}

//***********************************************************************
// lio_set_timestamp - Sets the timestamp val/size for a attr put
//***********************************************************************

void lio_set_timestamp(char *id, char **val, int *v_size)
{
    *val = id;
    *v_size = (id == NULL) ? 0 : strlen(id);
    return;
}

//***********************************************************************
// lio_get_timestamp - Splits the timestamp ts/id field
//***********************************************************************

void lio_get_timestamp(char *val, int *timestamp, char **id)
{
    char *bstate;
    int fin;

    *timestamp = 0;
    sscanf(tbx_stk_string_token(val, "|", &bstate, &fin), "%d", timestamp);
    if (id != NULL) *id = tbx_stk_string_token(NULL, "|", &bstate, &fin);
    return;
}

//-------------------------------------------------------------------------
//------- Universal Object Iterators
//-------------------------------------------------------------------------

//*************************************************************************
//  lio_unified_object_iter_create - Create an ls object iterator
//*************************************************************************

lio_unified_object_iter_t *lio_unified_object_iter_create(lio_path_tuple_t tuple, lio_os_regex_table_t *path_regex, lio_os_regex_table_t *obj_regex, int obj_types, int rd)
{
    lio_unified_object_iter_t *it;

    tbx_type_malloc_clear(it, lio_unified_object_iter_t, 1);

    it->tuple = tuple;
    if (tuple.is_lio == 1) {
        it->oit = os_create_object_iter(tuple.lc->os, tuple.creds, path_regex, obj_regex, obj_types, NULL, rd, NULL, 0);
    } else {
        it->lit = create_local_object_iter(path_regex, obj_regex, obj_types, rd);
    }

    return(it);
}

//*************************************************************************
//  lio_unified_object_iter_destroy - Destroys an ls object iterator
//*************************************************************************

void lio_unified_object_iter_destroy(lio_unified_object_iter_t *it)
{

    if (it->tuple.is_lio == 1) {
        os_destroy_object_iter(it->tuple.lc->os, it->oit);
    } else {
        destroy_local_object_iter(it->lit);
    }

    free(it);
}

//*************************************************************************
//  lio_unified_next_object - Returns the next object to work on
//*************************************************************************

int lio_unified_next_object(lio_unified_object_iter_t *it, char **fname, int *prefix_len)
{
    int err = 0;

    if (it->tuple.is_lio == 1) {
        err = os_next_object(it->tuple.lc->os, it->oit, fname, prefix_len);
    } else {
        err = local_next_object(it->lit, fname, prefix_len);
    }

    log_printf(15, "ftype=%d\n", err);
    return(err);
}


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

#define SLIST_STDIN "-"  //** Stdin list name

typedef struct {  //** Stdin/list iterator
    int argc;           //** Size of argv
    int current;        //** Current argv index
    int from_stdin;     //** Processing args from stdin.
    const char **argv;  //** Array of paths
} slist_iter_t;

//*************************************************************************
//  lio_stdinlist_iter_create - Create stdin/list iterator
//*************************************************************************

void *lio_stdinlist_iter_create(int argc, const char **argv)
{
    slist_iter_t *it;

    tbx_type_malloc(it, slist_iter_t, 1);
    it->argc = argc;
    it->argv = argv;
    it->current = 0;
    it->from_stdin = 0;

    return(it);
}

//*************************************************************************
// lio_stdinlist_iter_create - Destroys the stdin/list iterator
//*************************************************************************

void lio_stdinlist_iter_destroy(void *ptr)
{
   free(ptr);
}

//*************************************************************************
//  lio_stdinlist_iter_next - Returns the next path from either argv or stdin
//    IF no arguments are left then NULL is returned.
//
//    NOTE:  The caller is responsible for destroying the return argument
//*************************************************************************

char *lio_stdinlist_iter_next(void *ptr)
{
    slist_iter_t *it = ptr;
    char stmp[8192];
    char *p;

next_from_list:
    if (it->from_stdin == 0) {
        if (it->current >= it->argc) return(NULL);  //** Nothing left to do

        if (strcmp(it->argv[it->current], SLIST_STDIN) != 0) { //** Normal line
            p = strdup(it->argv[it->current]);
            it->current++;
            return(p);
        }

        it->from_stdin = 1;  //** Got a request for stdin
        it->current++;
    }

    //** IF we made it here then we are processing text from stdin
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

