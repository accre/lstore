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
// Comparison routines for lists
//***********************************************************************

#define _log_module_index 147

#include "ex3_abstract.h"
#include <tbx/skiplist.h>
#include <tbx/log.h>

int skiplist_compare_fn_ex_off(void *arg, tbx_sl_key_t *k1, tbx_sl_key_t *k2);
tbx_sl_compare_t skiplist_compare_ex_off= {skiplist_compare_fn_ex_off, NULL};

int skiplist_compare_fn_ex_id(void *arg, tbx_sl_key_t *k1, tbx_sl_key_t *k2);
tbx_sl_compare_t skiplist_compare_ex_id= {skiplist_compare_fn_ex_id, NULL};

int skiplist_compare_fn_ex_off(void *arg, tbx_sl_key_t *k1, tbx_sl_key_t *k2)
{
    ex_off_t *a = (ex_off_t *)k1;
    ex_off_t *b = (ex_off_t *)k2;
    int cmp = 1;

    if (*a < *b) {
        cmp = -1;
    } else if ( *a == *b) {
        cmp = 0;
    }

    log_printf(15, "skiplist_compare_fn_ex_off: cmp(" XOT ", " XOT ")=%d\n", *a, *b, cmp);
    return(cmp);
}

//*************************************************************************************

int skiplist_compare_fn_ex_id(void *arg, tbx_sl_key_t *k1, tbx_sl_key_t *k2)
{
    ex_id_t *a = (ex_id_t *)k1;
    ex_id_t *b = (ex_id_t *)k2;
    int cmp = 1;

    if (*a < *b) {
        cmp = -1;
    } else if ( *a == *b) {
        cmp = 0;
    }
    return(cmp);
}

