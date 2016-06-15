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
#include "gop/gop.h"
#ifndef ACCRE_GOP_H_INCLUDED
#define ACCRE_GOP_H_INCLUDED

#include "gop/gop_visibility.h"
#include <apr_thread_mutex.h>
#include <apr_thread_cond.h>
#include <apr_hash.h>
#include <tbx/atomic_counter.h>
#include <tbx/network.h>
#include <tbx/stack.h>
#include "callback.h"
#include <tbx/pigeon_coop.h>
#include <gop/types.h>

#ifdef __cplusplus
extern "C" {
#endif

// Types
struct gop_control_t {
    apr_thread_mutex_t *lock;  //** shared lock
    apr_thread_cond_t *cond;   //** shared condition variable
    tbx_pch_t  pch;   //** Pigeon coop hole for the lock and cond
};


extern tbx_atomic_unit32_t _opque_counter;


void gop_simple_cb(void *v, int mode);

void gop_set_success_state(op_generic_t *g, op_status_t state);
int gop_will_block(op_generic_t *g);
int gop_timed_waitall(op_generic_t *g, int dt);


void gop_mark_completed(op_generic_t *gop, op_status_t status);
void gop_callback_append(op_generic_t *gop, callback_t *cb);
apr_time_t gop_start_time(op_generic_t *gop);
apr_time_t gop_end_time(op_generic_t *gop);

#ifdef __cplusplus
}
#endif


#endif

