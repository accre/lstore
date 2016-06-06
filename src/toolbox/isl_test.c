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

#define _log_module_index 174

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include "tbx/assert_result.h"
#include "tbx/log.h"
#include "tbx/skiplist.h"
#include "skiplist.h"
#include "tbx/interval_skiplist.h"
#include "interval_skiplist.h"
#include "tbx/atomic_counter.h"

typedef struct {
    int lo, hi, index, match;
} interval_t;

int main(int argc, char **argv)
{
    int n_max, l_max, i, j, k, n, err, n_int_checks;
    int lo, hi;
    interval_t *data_list, *d;
    double p;
    tbx_isl_t *isl;
    tbx_isl_iter_t it;

    if (argc < 4) {
        printf("isl_test [-d log_level] n l_max p n_int_check\n");
        exit(1);
    }

    i = 1;
    if (strcmp(argv[i], "-d") == 0) {
        i++;
        j = atoi(argv[i]);
        i++;
        tbx_set_log_level(j);
    }
    n_max = atol(argv[i]);
    i++;
    l_max = atol(argv[i]);
    i++;
    p = atof(argv[i]);
    i++;
    n_int_checks = atol(argv[i]);
    i++;

    assert_result(apr_initialize(), APR_SUCCESS);
    tbx_atomic_startup();

    isl = tbx_isl_new_full(l_max, p, &tbx_sl_compare_int, NULL, NULL, NULL);

    data_list = (interval_t *)malloc(sizeof(interval_t)*n_max);

    drand48();
    drand48();  //** First couple of calls default to 0

    //** Insert phase
    for (i=0; i<n_max; i++) {
        data_list[i].lo = 100000*drand48();
        data_list[i].hi = data_list[i].lo + 1000*drand48();
        data_list[i].index = i;
//    if (i>=(n_max-2)) key_list[i] = key_list[0];  //** Force dups

        printf("==============inserting interval[%d]=%d .. %d\n", data_list[i].index, data_list[i].lo, data_list[i].hi);
        err = tbx_isl_insert(isl, (tbx_sl_key_t *)&(data_list[i].lo), (tbx_sl_key_t *)&(data_list[i].hi), (tbx_sl_data_t *)&(data_list[i]));
        if (err != 0) {
            printf("----------+Error inserting interval[%d]=%d .. %d\n", data_list[i].index, data_list[i].lo, data_list[i].hi);
        }
    }


    //** Iterate through the list to verify order
    printf("===============Iterating through Interval Skiplist\n");
    it = tbx_isl_iter_search(isl, (tbx_sl_key_t *)NULL, (tbx_sl_key_t *)NULL);
    d = (interval_t *)tbx_isl_next(&it);
    j = d->lo;
    i = 0;
    while (d != NULL) {
        printf("i=%d lo=%d hi=%d\n", i, d->lo, d->hi);
        if (j>d->lo) {
            printf("Error! in order! i=%d prev=%d curr=%d\n", i, j, d->lo);
        }
        j = d->lo;

        d = (interval_t *)tbx_isl_next(&it);
        i++;
    }

    if (i != n_max) {
        printf("Error scanning list! found=%d should be %d\n", i, n_max);
    }


    //**Check phase
    for (i=0; i<n_int_checks; i++) {
        lo = 100000*drand48();
        hi = lo + 1000*drand48();

        printf("==========Checking for overlaps of interval[%d]=%d .. %d\n", i, lo, hi);
        k = 0;
        for (j=0; j<n_max; j++) {  //** Scan for matches manually
            d = &(data_list[j]);
            if ((d->lo <= lo) && (d->hi >= lo)) {
                k++;
                d->match = 1;
                printf("    %d: %d .. %d\n", j, d->lo, d->hi);
            } else if ((d->lo > lo) && (d->lo <= hi)) {
                k++;
                d->match = 1;
                printf("    %d: %d .. %d\n", j, d->lo, d->hi);
            }
            fflush(stdout);
            tbx_log_flush();
        }

        printf("----manual matches=%d\n", k);
        fflush(stdout);
        tbx_log_flush();

        it = tbx_isl_iter_search(isl, (tbx_sl_key_t *)&lo, (tbx_sl_key_t *)&hi);
//    printf("    after iter creation\n"); fflush(stdout); tbx_log_flush();
        n = 0;
        d = tbx_isl_next(&it);
//    printf("    after initial next\n"); fflush(stdout); tbx_log_flush();
        while (d != NULL) {
            n++;
            printf("    %d: %d .. %d\n", d->index, d->lo, d->hi);
            if (d->match == 1) {
                d->match = 2;
            } else if (d->match == 2) {
                printf("----------Error!  Found duplicate match!\n");
            } else {
                printf("----------Error!  Found incorrect match!\n");
            }
            fflush(stdout);
            tbx_log_flush();

            d = tbx_isl_next(&it);
        }


        printf("----iter matches=%d\n", n);
        fflush(stdout);
        tbx_log_flush();

        if (n != k) {
            printf("----------Error mismatch match count k=%d n=%d\n", k, n);
        }

    }

    //** Now delete everything manually
//  for (i=0; i<n_max; i++) {
    for (i=n_max-1; i>=0; i--) {
        printf("==========Removing interval[%d]=%d .. %d\n", data_list[i].index, data_list[i].lo, data_list[i].hi);
        err = tbx_isl_remove(isl, (tbx_sl_key_t *)&(data_list[i].lo), (tbx_sl_key_t *)&(data_list[i].hi), (tbx_sl_data_t *)&(data_list[i]));
        if (err != 0) {
            printf("----------Error removing interval[%d]=%d .. %d\n", data_list[i].index, data_list[i].lo, data_list[i].hi);
        }
    }

    tbx_isl_del(isl);
    fflush(stdout);
    tbx_log_flush();

    free(data_list);

    tbx_atomic_shutdown();
    apr_terminate();
    return(0);
}

