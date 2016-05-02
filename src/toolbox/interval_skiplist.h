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

//*************************************************************************
//*************************************************************************

#ifndef __INTERVAL_SKIPLIST_H_
#define __INTERVAL_SKIPLIST_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "tbx/toolbox_visibility.h"
#include "tbx/skiplist.h"

struct tbx_isl_data_t {
    tbx_isl_data_t *data;
    tbx_isl_data_t *next;
};

struct tbx_isl_node_t {
    tbx_isl_data_t **edge;
    tbx_isl_data_t *point;
    tbx_isl_data_t  *start;
    int         n_end;
};

struct tbx_isl_t {  //** Generic Interval Skip Lists container
    tbx_sl_t *sl;
    void (*data_free)(tbx_sl_data_t *a);
    int n_intervals;
};

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





#ifdef __cplusplus
}
#endif

#endif


