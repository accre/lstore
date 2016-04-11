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

#define _log_module_index 174

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include "assert_result.h"
#include "log.h"
#include "skiplist.h"
#include "interval_skiplist.h"
#include "atomic_counter.h"

typedef struct {
    int lo, hi, index, match;
} interval_t;

int main(int argc, char **argv)
{
    int n_max, l_max, i, j, k, n, err, n_int_checks;
    int lo, hi;
    interval_t *data_list, *d;
    double p;
    interval_skiplist_t *isl;
    interval_skiplist_iter_t it;

    if (argc < 4) {
        printf("isl_test [-d log_level] n l_max p n_int_check\n");
        exit(1);
    }

    i = 1;
    if (strcmp(argv[i], "-d") == 0) {
        i++;
        j = atoi(argv[i]);
        i++;
        set_log_level(j);
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
    atomic_init();

    isl = create_interval_skiplist_full(l_max, p, &skiplist_compare_int, NULL, NULL, NULL);

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
        err = insert_interval_skiplist(isl, (skiplist_key_t *)&(data_list[i].lo), (skiplist_key_t *)&(data_list[i].hi), (skiplist_data_t *)&(data_list[i]));
        if (err != 0) {
            printf("----------+Error inserting interval[%d]=%d .. %d\n", data_list[i].index, data_list[i].lo, data_list[i].hi);
        }
    }


    //** Iterate through the list to verify order
    printf("===============Iterating through Interval Skiplist\n");
    it = iter_search_interval_skiplist(isl, (skiplist_key_t *)NULL, (skiplist_key_t *)NULL);
    d = (interval_t *)next_interval_skiplist(&it);
    j = d->lo;
    i = 0;
    while (d != NULL) {
        printf("i=%d lo=%d hi=%d\n", i, d->lo, d->hi);
        if (j>d->lo) {
            printf("Error! in order! i=%d prev=%d curr=%d\n", i, j, d->lo);
        }
        j = d->lo;

        d = (interval_t *)next_interval_skiplist(&it);
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
            flush_log();
        }

        printf("----manual matches=%d\n", k);
        fflush(stdout);
        flush_log();

        it = iter_search_interval_skiplist(isl, (skiplist_key_t *)&lo, (skiplist_key_t *)&hi);
//    printf("    after iter creation\n"); fflush(stdout); flush_log();
        n = 0;
        d = next_interval_skiplist(&it);
//    printf("    after initial next\n"); fflush(stdout); flush_log();
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
            flush_log();

            d = next_interval_skiplist(&it);
        }


        printf("----iter matches=%d\n", n);
        fflush(stdout);
        flush_log();

        if (n != k) {
            printf("----------Error mismatch match count k=%d n=%d\n", k, n);
        }

    }

    //** Now delete everything manually
//  for (i=0; i<n_max; i++) {
    for (i=n_max-1; i>=0; i--) {
        printf("==========Removing interval[%d]=%d .. %d\n", data_list[i].index, data_list[i].lo, data_list[i].hi);
        err = remove_interval_skiplist(isl, (skiplist_key_t *)&(data_list[i].lo), (skiplist_key_t *)&(data_list[i].hi), (skiplist_data_t *)&(data_list[i]));
        if (err != 0) {
            printf("----------Error removing interval[%d]=%d .. %d\n", data_list[i].index, data_list[i].lo, data_list[i].hi);
        }
    }

    destroy_interval_skiplist(isl);
    fflush(stdout);
    flush_log();

    free(data_list);

    atomic_destroy();
    apr_terminate();
    return(0);
}

