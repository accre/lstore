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

#define _log_module_index 175

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include "log.h"
#include "skiplist.h"
#include "atomic_counter.h"

int main(int argc, char **argv)
{
  int n_max, rnd_max, l_max, dummy, i, j, k, best, best_n, n, err, *key_list, *data_list;
  int min_key, max_key;
  double p;
  skiplist_t *sl;
  skiplist_iter_t it;
  int *key, *data;
  int check_slot = 0;

  if (argc < 4) {
     printf("sk_test [-d log_level] n_max random_max l_max p\n");
     exit(1);
  }

  assert(apr_initialize() == APR_SUCCESS);
  atomic_init();

  open_log("stdout");

  i = 1;
  if (strcmp(argv[i], "-d") == 0) {
    i++;
    j = atoi(argv[i]); i++;
    set_log_level(j);
  }
  n_max = atol(argv[i]); i++;
  rnd_max = atol(argv[i]); i++;
  l_max = atol(argv[i]); i++;
  p = atof(argv[i]); i++;

  check_slot = n_max / 2;

  sl = create_skiplist_full(l_max, p, 1, &skiplist_compare_int, NULL, NULL, NULL);

  //** Make sure everything works fine with an empty list
  i = 12345;
  it = iter_search_skiplist(sl, (skiplist_key_t *)&i, 0);
  next_skiplist(&it, (skiplist_key_t **)&key, (skiplist_data_t **)&data);
  if (data != NULL) {
     printf("ERROR got something from an EMPTY list\n");
  }

  key_list = (int *)malloc(sizeof(int)*n_max);
  data_list = (int *)malloc(sizeof(int)*n_max);

  //** Insert phase
  min_key = max_key = -1;

  for (i=0; i<n_max; i++) {
    key_list[i] = rand();
    if (i>=(n_max-2)) key_list[i] = key_list[0];  //** Force dups

    if ((i==0) || (min_key > key_list[i])) min_key = key_list[i];
    if ((i==0) || (max_key < key_list[i])) max_key = key_list[i];

    data_list[i] = i;
    printf("inserting key[%d]=%d\n", i, key_list[i]);
    err = insert_skiplist(sl, (skiplist_key_t *)&(key_list[i]), (skiplist_data_t *)&(data_list[i]));
    if (err != 0) {
       printf("ERROR inserting key_list[%d]=%d\n", i, key_list[i]);
    }
  }


  printf("********** min_key=%d    max_key=%d **********\n", min_key, max_key);

  //**Check phase
  for (i=0; i<n_max; i++) {
    printf("Looking for key[%d]=%d\n", i, key_list[i]);
    it = iter_search_skiplist(sl, (skiplist_key_t *)&(key_list[i]), 0);
    j = 0;
    do {
       err = next_skiplist(&it, (skiplist_key_t *)&key, (skiplist_data_t *)&data);
       if (err != 0) {
          printf("Err locating key_list[%d]=%d\n", i, key_list[i]);
       }
       if ((*key == key_list[i]) && (data == &(data_list[i]))) j = 1;
    } while ((err == 0) && (j == 0) && (*key == key_list[i]));

    if (j == 0) {
       printf("ERROR locating key_list[%d]=%d\n", i, key_list[i]);
    } else {
       printf("Found key[%d]=%d key=%d data=%d\n", i, key_list[i], *key, *data);
    }
  }

  //** Check that I can get the start/end keys

  printf("Checking access to the 1st key\n");
  key = skiplist_first_key(sl);
  if (*key != min_key) {
     printf("ERROR getting 1st key! min_key=%d got=%d\n", min_key, *key);
  }

  printf("Checking query for min_key-1\n");
  j = min_key - 1;
  it = iter_search_skiplist(sl, (skiplist_key_t *)&j, 0);
  err = next_skiplist(&it, (skiplist_key_t *)&key, (skiplist_data_t *)&data);
  if (*key != min_key) {
     printf("ERROR getting 1st key using min_key-1! min_key-1=%d got=%d\n", j, *key);
  }

  printf("Checking query for min_key\n");
  j = min_key;
  it = iter_search_skiplist(sl, (skiplist_key_t *)&j, 0);
  err = next_skiplist(&it, (skiplist_key_t *)&key, (skiplist_data_t *)&data);
  if (*key != min_key) {
     printf("ERROR getting 1st key using min_key! min_key=%d got=%d\n", j, *key);
  }

  printf("Checking query for min_key+1\n");
  j = min_key + 1;
  it = iter_search_skiplist(sl, (skiplist_key_t *)&j, 0);
  err = next_skiplist(&it, (skiplist_key_t *)&key, (skiplist_data_t *)&data);
  if (*key <= min_key) {
     printf("ERROR querying min_key+1! min_key+1=%d got=%d\n", j, *key);
  }


  printf("Checking access to the last key\n");
  key = skiplist_last_key(sl);
  if (*key != max_key) {
     printf("ERROR getting last key! max_key=%d got=%d\n", max_key, *key);
  }

  printf("Checking query for max_key-1\n");
  j = max_key - 1;
  it = iter_search_skiplist(sl, (skiplist_key_t *)&j, 0);
  err = next_skiplist(&it, (skiplist_key_t *)&key, (skiplist_data_t *)&data);
  if (*key != max_key) {
     printf("ERROR with last key query using max_key-1! max_key-1=%d got=%d\n", j, *key);
  }

  printf("Checking query for max_key\n");
  j = max_key;
  it = iter_search_skiplist(sl, (skiplist_key_t *)&j, 0);
  err = next_skiplist(&it, (skiplist_key_t *)&key, (skiplist_data_t *)&data);
  if (*key != max_key) {
     printf("ERROR getting last key using max_key! max_key=%d got=%d\n", j, *key);
  }

  printf("Checking query for max_key+1\n");
  j = max_key + 1;
  it = iter_search_skiplist(sl, (skiplist_key_t *)&j, 0);
  err = next_skiplist(&it, (skiplist_key_t *)&key, (skiplist_data_t *)&data);
  if (key != NULL) {
     printf("ERROR getting key using max_key+1! max_key+1=%d got=%d should be NULL\n", j, *key);
  }

  //** Iterate through the list to verify order
  printf("Iterating through the list to verify order\n");
  j = -1;
  it = iter_search_skiplist(sl, (skiplist_key_t *)NULL, 0);
  err = next_skiplist(&it, (skiplist_key_t *)&key, (skiplist_data_t *)&data);
  i = 0;
  while (err == 0) {
     printf("i=%d key=%d data=%d\n", i, *key, *data);
     if (j>*key) {
        printf("ERROR! in order! i=%d prev=%d curr=%d\n", i, j, *key);
     }
     err = next_skiplist(&it, (skiplist_key_t *)&key, (skiplist_data_t *)&data);
     i++;
  }
  printf("After iteration through the list.  i=%d n_max=%d\n", i, n_max);
  if (i<n_max) {
    printf("ERROR Incorrect number of items!\n");
  }

  printf("Checking that we return the key or the next higher key\n");
  j = key_list[check_slot]-1;
  it = iter_search_skiplist(sl, (skiplist_key_t *)&j, 0);
  err = next_skiplist(&it, (skiplist_key_t *)&key, (skiplist_data_t *)&data);
  printf("Checking for j=%d key_list[%d]=%d got key=%d\n", j, check_slot, key_list[check_slot], *key);
  if (*key != key_list[check_slot]) {
     printf("ERROR! key<j (%d<%d)!!!!!\n", *key, j);
  }

  printf("Checking that round down works\n");
  j = key_list[check_slot]+1;
  it = iter_search_skiplist(sl, (skiplist_key_t *)&j, -1);
  err = next_skiplist(&it, (skiplist_key_t *)&key, (skiplist_data_t *)&data);
  printf("Checking for j=%d key_list[%d]=%d got key=%d\n", j, check_slot, key_list[check_slot], *key);
  if (*key != key_list[check_slot]) {
     printf("ERROR! key>j (%d<%d)!!!!!\n", *key, j);
  }

  printf("min_key:  Checking that round down works\n");
  j = min_key+1;
  it = iter_search_skiplist(sl, (skiplist_key_t *)&j, -1);
  err = next_skiplist(&it, (skiplist_key_t *)&key, (skiplist_data_t *)&data);
  printf("Checking for j=%d min_key=%d got key=%d\n", j, min_key, *key);
  if (*key != min_key) {
     printf("ERROR! key>j (%d<%d)!!!!!\n", *key, j);
  }


  j = key_list[check_slot];
  it = iter_search_skiplist(sl, (skiplist_key_t *)&j, 0);
  err = next_skiplist(&it, (skiplist_key_t *)&key, (skiplist_data_t *)&data);
  printf("Checking for j=%d key_list[%d]=%d got key=%d\n", j, check_slot, key_list[check_slot], *key);
  if (*key != j) {
     printf("ERROR! key!=j (%d!=%d)!!!!!\n", *key, j);
  }

  printf("Performing random checks\n");
  dummy = -1;
  for (i=0; i<rnd_max; i++) {
     n = rand();
     it = iter_search_skiplist(sl, (skiplist_key_t *)&n, 0);
     err = next_skiplist(&it, (skiplist_key_t *)&key, (skiplist_data_t *)&data);
     best = -1;
     best_n = -1;
     for (j=0; j<n_max; j++) {
        k = key_list[j] - n;
        if (k > 0) {
           if ((best == -1) || (best > k)) {
              best = k;
              best_n = key_list[j];
           }
        }
     }

     if (key == NULL) {
        key = &dummy;
        //printf("ERROR (OK) NULL returned\n");
     }
     if (*key != best_n) {
        j = *key - n;
        printf("ERROR checking n=%d skiplist best=%d dt=%d  ---  scan best=%d dt=%d err=%d\n", n, *key, j, best_n, best, err);
     }
  }

  printf("Performing random round down checks\n");
  dummy = -1;
  for (i=0; i<rnd_max; i++) {
     n = rand();
     it = iter_search_skiplist(sl, (skiplist_key_t *)&n, -1);
     err = next_skiplist(&it, (skiplist_key_t *)&key, (skiplist_data_t *)&data);
     best = -1;
     best_n = -1;
     for (j=0; j<n_max; j++) {
        k = n - key_list[j];
        if (k > 0) {
           if ((best == -1) || (best > k)) {
              best = k;
              best_n = key_list[j];
           }
        }
     }

     if (key == NULL) {
        key = &dummy;
        //printf("ERROR (OK) NULL returned\n");
     }
     if (*key != best_n) {
        j = n - *key;
        printf("ERROR checking n=%d skiplist best=%d dt=%d  ---  scan best=%d dt=%d err=%d\n", n, *key, j, best_n, best, err);
     }
  }


  //** Now delete everything manually
//  for (i=0; i<n_max; i++) {
  for (i=n_max-1; i>=0; i--) {
     printf("removing key[%d]=%d\n", i, key_list[i]); fflush(stdout); fflush(stderr);
     err = remove_skiplist(sl, (skiplist_key_t *)&(key_list[i]), (skiplist_data_t *)&(data_list[i]));
     if (err != 0) {
       printf("ERROR removing key_list[%d]=%d\n", i, key_list[i]); fflush(stdout); fflush(stderr);
     }
  }


  //** Now insert a couple of elements and "empty and repeat"
  printf("Checking empty_skiplist\n");
  for (i=0; i<n_max; i++) {
    err = insert_skiplist(sl, (skiplist_key_t *)&(key_list[i]), (skiplist_data_t *)&(data_list[i]));
    if (err != 0) {
       printf("empty 1 ERROR inserting key_list[%d]=%d\n", i, key_list[i]);
    }
  }
  j = key_list[check_slot];
  it = iter_search_skiplist(sl, (skiplist_key_t *)&j, 0);
  err = next_skiplist(&it, (skiplist_key_t *)&key, (skiplist_data_t *)&data);
  printf("empty 1 Checking for j=%d key_list[%d]=%d got key=%d\n", j, check_slot, key_list[check_slot], *key);
  if (*key != j) {
     printf("ERROR! key!=j (%d!=%d)!!!!!\n", *key, j);
  }

  empty_skiplist(sl);

  for (i=0; i<n_max; i++) {
    err = insert_skiplist(sl, (skiplist_key_t *)&(key_list[i]), (skiplist_data_t *)&(data_list[i]));
    if (err != 0) {
       printf("empty 2 ERROR inserting key_list[%d]=%d\n", i, key_list[i]);
    }
  }
  j = key_list[check_slot];
  it = iter_search_skiplist(sl, (skiplist_key_t *)&j, 0);
  err = next_skiplist(&it, (skiplist_key_t *)&key, (skiplist_data_t *)&data);
  printf("empty 2 Checking for j=%d key_list[%d]=%d got key=%d\n", j, check_slot, key_list[check_slot], *key);
  if (*key != j) {
     printf("ERROR! key!=j (%d!=%d)!!!!!\n", *key, j);
  }

  destroy_skiplist(sl);
  fflush(stdout); flush_log();

  free(key_list); free(data_list);

  atomic_destroy();
  apr_terminate();
  return(0);
}

