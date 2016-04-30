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

#include "tbx/toolbox_visibility.h"
#include "skiplist.h"

typedef struct tbx_isl_data_t tbx_isl_data_t;
struct tbx_isl_data_t {
    tbx_isl_data_t *data;
    tbx_isl_data_t *next;
};

typedef struct tbx_isl_node_t tbx_isl_node_t;
struct tbx_isl_node_t {
    tbx_isl_data_t **edge;
    tbx_isl_data_t *point;
    tbx_isl_data_t  *start;
    int         n_end;
};

typedef struct tbx_isl_t tbx_isl_t;
struct tbx_isl_t {  //** Generic Interval Skip Lists container
    tbx_sl_t *sl;
    void (*data_free)(tbx_sl_data_t *a);
    int n_intervals;
};

typedef struct tbx_isl_iter_t tbx_isl_iter_t;
struct tbx_isl_iter_t {
    tbx_sl_key_t *lo;
    tbx_sl_key_t *hi;
    tbx_isl_t *isl;
    tbx_sl_node_t *sn;
    tbx_isl_node_t *isln;
    tbx_isl_data_t *ele;
    tbx_sl_node_t *ptr[SKIPLIST_MAX_LEVEL];
    int mode;
    int ptr_level;
    int finished;
};

#define interval_skiplist_lock(a) apr_thread_mutex_lock((a)->sl->lock)
#define interval_skiplist_unlock(a) apr_thread_mutex_unlock((a)->sl->lock)
#define interval_skiplist_count(a) (a)->n_intervals

TBX_API tbx_sl_key_t *interval_skiplist_first_key(tbx_isl_t *isl);
TBX_API tbx_sl_key_t *interval_skiplist_last_key(tbx_isl_t *isl);


TBX_API tbx_isl_t *create_interval_skiplist_full(int maxlevels, double p,
        tbx_sl_compare_t *compare,
        tbx_sl_key_t *(*dup)(tbx_sl_key_t *a),
        void (*key_free)(tbx_sl_key_t *a),
        void (*data_free)(tbx_sl_data_t *a));
TBX_API tbx_isl_t *create_interval_skiplist(tbx_sl_compare_t *compare,
        tbx_sl_key_t *(*dup)(tbx_sl_key_t *a),
        void (*key_free)(tbx_sl_key_t *a),
        void (*data_free)(tbx_sl_data_t *a));
TBX_API void destroy_interval_skiplist(tbx_isl_t *isl);
TBX_API int insert_interval_skiplist(tbx_isl_t *sl, tbx_sl_key_t *lo, tbx_sl_key_t *hi, tbx_sl_data_t *data);
TBX_API int remove_interval_skiplist(tbx_isl_t *sl, tbx_sl_key_t *lo, tbx_sl_key_t *hi, tbx_sl_data_t *data);
TBX_API int count_interval_skiplist(tbx_isl_t *sl, tbx_sl_key_t *lo, tbx_sl_key_t *hi);

TBX_API tbx_isl_iter_t iter_search_interval_skiplist(tbx_isl_t *sl, tbx_sl_key_t *lo, tbx_sl_key_t *hi);
TBX_API tbx_sl_data_t *next_interval_skiplist(tbx_isl_iter_t *it);


#ifdef __cplusplus
}
#endif

#endif


