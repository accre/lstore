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

//***********************************************************************
//  Routines for managing an ordered non-overlapping list
//***********************************************************************

#include <tbx/append_printf.h>
#include <tbx/fmttypes.h>
#include <tbx/range_stack.h>
#include <tbx/string_token.h>
#include <tbx/stack.h>
#include <tbx/type_malloc.h>

//*******************************************************************************
// tbx_range_stack_string2range - Converts a string containing a range to a list
//*******************************************************************************

tbx_stack_t *tbx_range_stack_string2range(char *string, char *range_delimiter)
{
    tbx_stack_t *range_stack = NULL;
    char *token, *bstate;
    int64_t *rng;
    int fin, good;

    if (string == NULL) return(NULL);
    token = strdup(string);
    bstate = NULL;

    do {
        tbx_type_malloc(rng, int64_t, 2);
        good = 1;
        if (sscanf(tbx_stk_escape_string_token((bstate==NULL) ? token : NULL, ":", '\\', 0, &bstate, &fin), I64T, &rng[0]) != 1) { good = 0; break; }
        if (sscanf(tbx_stk_escape_string_token(NULL, range_delimiter, '\\', 0, &bstate, &fin), I64T, &rng[1]) != 1) { good = 0; break; }
        tbx_range_stack_merge(&range_stack, rng);
    } while (fin == 0);

    free(token);
    if (!good) free(rng);

    return(range_stack);
}
//*******************************************************************************
// tbx_range_stack_range2string - Prints the byte range list
//*******************************************************************************

char *tbx_range_stack_range2string(tbx_stack_t *range_stack, char *range_delimiter)
{
    int64_t *rng;
    tbx_stack_ele_t *cptr;
    char *string;
    int used, bufsize;

    if (range_stack == NULL) return(NULL);

    bufsize = tbx_stack_count(range_stack)*(2*20+2);
    tbx_type_malloc(string, char, bufsize);
    string[0] = 0;
    used = 0;
    cptr = tbx_stack_get_current_ptr(range_stack);
    tbx_stack_move_to_top(range_stack);
    while ((rng = tbx_stack_get_current_data(range_stack)) != NULL) {
        tbx_append_printf(string, &used, bufsize, I64T":" I64T "%s", rng[0], rng[1], range_delimiter);
        tbx_stack_move_down(range_stack);
    }

    tbx_stack_move_to_ptr(range_stack, cptr);

    return(string);
}

//*******************************************************************************
//  _range_collapse - Collapses the byte ranges.  Starts processing
//    from the current range and iterates if needed.
//*******************************************************************************

void _range_collapse(tbx_stack_t *range_stack)
{
    int64_t *rng, *trng, hi1;
    int more;

    trng = tbx_stack_get_current_data(range_stack);  //** This is the range just expanded
    hi1 = trng[1]+1;

    tbx_stack_move_down(range_stack);
    more = 1;
    while (((rng = tbx_stack_get_current_data(range_stack)) != NULL) && (more == 1)) {
        if (hi1 >= rng[0]) { //** Got an overlap so collapse
            if (rng[1] > trng[1]) {
                trng[1] = rng[1];
                more = 0;  //** Kick out this is the last range
            }
            tbx_stack_delete_current(range_stack, 0, 1);
        } else {
            more = 0;
        }
    }

//    log_printf(5, "n_ranges=%d\n", tbx_stack_count(range_stack));
}

//*******************************************************************************
// tbx_range_stack_merge - Adds and merges the range w/ existing ranges
//*******************************************************************************

void tbx_range_stack_merge(tbx_stack_t **range_stack_ptr, int64_t *new_rng)
{
    int64_t *rng, *prng, trng[2];
    int64_t lo, hi;
    tbx_stack_t *range_stack;

    if (!(*range_stack_ptr)) *range_stack_ptr = tbx_stack_new();
    range_stack = *range_stack_ptr;

    //** If an empty stack can handle it quickly
    if (tbx_stack_count(range_stack) == 0) {
        tbx_stack_push(range_stack, new_rng);
        return;
    }


    //** Find the insertion point
    lo = new_rng[0]; hi = new_rng[1];
    tbx_stack_move_to_top(range_stack);
    prng = NULL;
    while ((rng = tbx_stack_get_current_data(range_stack)) != NULL) {
        if (lo <= rng[0]) break;  //** Got it
        prng = rng;
        tbx_stack_move_down(range_stack);
    }

    if (prng == NULL) {  //** Fudge to get proper logic
        trng[0] = 12345;
        trng[1] = lo - 10;
        prng = trng;
    }

    if (lo <= prng[1]+1) { //** Expand prev range
        if (prng[1] < hi) {
            prng[1] = hi;  //** Extend the range
            if (rng != NULL) {  //** Move back before collapsing.  Otherwise we're at the end and we've already extended the range
                tbx_stack_move_up(range_stack);
                _range_collapse(range_stack);
            }
        }
        free(new_rng);
    } else if (rng != NULL) {  //** Check if overlap on curr range
        if (rng[0] <= hi+1) {  //** Got an overlap
            free(new_rng);
            rng[0] = lo;
            if (rng[1] < hi) {  //** Expanding on the hi side so need to check for collapse
                rng[1] = hi;
                _range_collapse(range_stack);
            }
        } else {  //** No overlap.  This is a new range to insert
            tbx_stack_insert_above(range_stack, new_rng);
        }
    } else {  //** Adding to the end
        tbx_stack_move_to_bottom(range_stack);
        tbx_stack_insert_below(range_stack, new_rng);
    }

    return;
}

//*******************************************************************************
// tbx_range_stack_merge2 - Adds and merges the range w/ existing ranges
//*******************************************************************************

void tbx_range_stack_merge2(tbx_stack_t **range_stack_ptr, int64_t lo, int64_t hi)
{
    int64_t *rng;

    tbx_type_malloc(rng, int64_t, 2);
    rng[0] = lo; rng[1] = hi;
    tbx_range_stack_merge(range_stack_ptr, rng);
    return;
}

//*******************************************************************************
//  _do_test
//*******************************************************************************

int _do_test(int i, char *in, char *out)
{
    int err;
    char *final;
    tbx_stack_t *r;

    err = 0;
    r = tbx_range_stack_string2range(in, ";");
    final = tbx_range_stack_range2string(r, ";");
    if (strcmp(final, out) != 0) {
        err = 1;
        fprintf(stderr, "ERROR: i=%d input  =%s\n", i, in);
        fprintf(stderr, "ERROR: i=%d output =%s\n", i, final);
        fprintf(stderr, "ERROR: i=%d correct=%s\n", i, out);
    }

    free(final);
    tbx_stack_free(r, 1);

    return(err);
}

//*******************************************************************************
// tbx_range_stack_test - Test the range stack routines
//*******************************************************************************

int tbx_range_stack_test()
{
    int err;

    err = 0;
    err += _do_test(0, "100:200;", "100:200;");
    err += _do_test(1, "100:200;150:200;", "100:200;");
    err += _do_test(2, "100:200;150:210;", "100:210;");
    err += _do_test(3, "100:200;50:101;", "50:200;");
    err += _do_test(4, "100:200;50:99;", "50:200;");
    err += _do_test(5, "100:200;50:98;", "50:98;100:200;");
    err += _do_test(6, "100:200;50:98;95:101;", "50:200;");
    err += _do_test(7, "100:200;50:98;99:99;", "50:200;");
    err += _do_test(8, "101:200;50:98;99:99;", "50:99;101:200;");
    err += _do_test(9, "101:200;50:75;20:100;", "20:200;");
    err += _do_test(10, "100:200;50:75;77:98", "50:75;77:98;100:200;");
    err += _do_test(11, "100:200;50:75;77:98;76:99", "50:200;");

    return(err);
}