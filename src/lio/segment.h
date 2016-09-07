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

#ifndef _SEGMENT_H_INCLUDED
#define _SEGMENT_H_INCLUDED

#include <lio/segment.h>

gop_op_generic_t *segment_put_gop(gop_thread_pool_context_t *tpc, data_attr_t *da, lio_segment_rw_hints_t *rw_hints, FILE *fd, lio_segment_t *dest_seg, ex_off_t dest_offset, ex_off_t len, ex_off_t bufsize, char *buffer, int do_truncate, int timeout);
gop_op_generic_t *segment_get_gop(gop_thread_pool_context_t *tpc, data_attr_t *da, lio_segment_rw_hints_t *rw_hints, lio_segment_t *src_seg, FILE *fd, ex_off_t src_offset, ex_off_t len, ex_off_t bufsize, char *buffer, int timeout);
lio_segment_t *load_segment(lio_service_manager_t *ess, ex_id_t id, lio_exnode_exchange_t *ex);


// Preprocessor macros
#define lio_segment_type(s) (s)->header.type
#define segment_block_size(s) ((lio_segment_vtable_t *)(s)->obj.vtable)->block_size(s)
#define segment_clone(s, da, clone_ex, mode, attr, to) \
    ((lio_segment_vtable_t *)(s)->obj.vtable)->clone(s, da, clone_ex, mode, attr, to)
#define segment_deserialize(s, id, exp) ((lio_segment_vtable_t *)(s)->obj.vtable)->deserialize(s, id, exp)
#define segment_get_gop_header(seg) &((seg)->header)
#define segment_lock(s) apr_thread_mutex_lock((s)->lock)
#define segment_remove(s, da, to) ((lio_segment_vtable_t *)(s)->obj.vtable)->remove(s, da, to)
#define segment_serialize(s, exp) ((lio_segment_vtable_t *)(s)->obj.vtable)->serialize(s, exp)
#define segment_set_header(seg, new_head) (seg)->header = *(new_head)
#define segment_unlock(s) apr_thread_mutex_unlock((s)->lock)

#endif
