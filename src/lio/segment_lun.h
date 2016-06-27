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
// Linear segment support
//***********************************************************************

#ifndef _SEGMENT_LUN_H_
#define _SEGMENT_LUN_H_

#include <gop/opque.h>
#include <lio/blacklist.h>
#include <tbx/fmttypes.h>

#include "ex3.h"
#include "ex3_types.h"

#ifdef __cplusplus
extern "C" {
#endif

#define SEGMENT_TYPE_LUN "lun"

segment_t *segment_lun_load(void *arg, ex_id_t id, exnode_exchange_t *ex);
segment_t *segment_lun_create(void *arg);
int seglun_row_decompose_test();

struct seglun_priv_t {
    ex_off_t used_size;
    ex_off_t total_size;
    ex_off_t max_block_size;
    ex_off_t excess_block_size;
    ex_off_t max_row_size;
    ex_off_t chunk_size;
    ex_off_t stripe_size;
    apr_time_t grow_time;
    rs_query_t *rsq;
    thread_pool_context_t *tpc;
    int grow_count;
    int n_devices;
    int n_shift;
    int hard_errors;
    int grow_break;
    int map_version;
    int inprogress_count;
    rs_mapping_notify_t notify;
    tbx_isl_t *isl;
    resource_service_fn_t *rs;
    data_service_fn_t *ds;
    tbx_stack_t *db_cleanup;
    blacklist_t *bl;
};


#ifdef __cplusplus
}
#endif

#endif
