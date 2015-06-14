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

//*************************************************************************
//*************************************************************************

#ifndef __INTERVAL_SKIPLIST_H_
#define __INTERVAL_SKIPLIST_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "skiplist.h"

struct isl_data_s;
typedef struct isl_data_s isl_data_t;

struct isl_data_s {
    isl_data_t *data;
    isl_data_t *next;
};

typedef struct {
    isl_data_t **edge;
    isl_data_t *point;
    isl_data_t  *start;
    int         n_end;
} isl_node_t;

typedef struct {  //** Generic Interval Skip Lists container
    skiplist_t *sl;
    void (*data_free)(skiplist_data_t *a);
    int n_intervals;
} interval_skiplist_t;

typedef struct {
    skiplist_key_t *lo;
    skiplist_key_t *hi;
    interval_skiplist_t *isl;
    skiplist_node_t *sn;
    isl_node_t *isln;
    isl_data_t *ele;
    skiplist_node_t *ptr[SKIPLIST_MAX_LEVEL];
    int mode;
    int ptr_level;
    int finished;
} interval_skiplist_iter_t;

#define interval_skiplist_lock(a) apr_thread_mutex_lock((a)->sl->lock)
#define interval_skiplist_unlock(a) apr_thread_mutex_unlock((a)->sl->lock)
#define interval_skiplist_count(a) (a)->n_intervals

skiplist_key_t *interval_skiplist_first_key(interval_skiplist_t *isl);
skiplist_key_t *interval_skiplist_last_key(interval_skiplist_t *isl);


interval_skiplist_t *create_interval_skiplist_full(int maxlevels, double p,
        skiplist_compare_t *compare,
        skiplist_key_t *(*dup)(skiplist_key_t *a),
        void (*key_free)(skiplist_key_t *a),
        void (*data_free)(skiplist_data_t *a));
interval_skiplist_t *create_interval_skiplist(skiplist_compare_t *compare,
        skiplist_key_t *(*dup)(skiplist_key_t *a),
        void (*key_free)(skiplist_key_t *a),
        void (*data_free)(skiplist_data_t *a));
void destroy_interval_skiplist(interval_skiplist_t *isl);
int insert_interval_skiplist(interval_skiplist_t *sl, skiplist_key_t *lo, skiplist_key_t *hi, skiplist_data_t *data);
int remove_interval_skiplist(interval_skiplist_t *sl, skiplist_key_t *lo, skiplist_key_t *hi, skiplist_data_t *data);
int count_interval_skiplist(interval_skiplist_t *sl, skiplist_key_t *lo, skiplist_key_t *hi);

interval_skiplist_iter_t iter_search_interval_skiplist(interval_skiplist_t *sl, skiplist_key_t *lo, skiplist_key_t *hi);
skiplist_data_t *next_interval_skiplist(interval_skiplist_iter_t *it);


#ifdef __cplusplus
}
#endif

#endif


