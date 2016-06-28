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
// Exnode3 abstract class
//***********************************************************************

#ifndef _EX3_ABSTRACT_H_
#define _EX3_ABSTRACT_H_

#include <gop/opque.h>
#include <gop/tp.h>
#include <lio/ex3.h>
#include <tbx/atomic_counter.h>
#include <tbx/iniparse.h>
#include <tbx/list.h>
#include <tbx/log.h>
#include <tbx/transfer_buffer.h>

#include "data_block.h"
#include "ds.h"
#include "ex3/header.h"
#include "ex3/types.h"
#include "os.h"
#include "rs.h"
#include "service_manager.h"

#ifdef __cplusplus
extern "C" {
#endif


#define LO_SIZE_USED 0
#define LO_SIZE_MAX  1

#define INSPECT_RESULT_MIGRATE_ERROR  4096   //** Migrate errors found
#define INSPECT_RESULT_COUNT_MASK      511    //** Bit mask for LUN counts
#define SEG_SM_LOAD   "segment_load"



struct segment_rw_hints_t {     //** Structure for contaiing hints to the various segment drivers
    int lun_max_blacklist;  //** Max number of devs to blacklist per stripe for performance
    int number_blacklisted;
};

//#define inspect_printf(fd, ...) if ((fd) != NULL) fprintf(fd, __VA_ARGS__)

#define segment_type(s) (s)->header.type
#define segment_destroy(s) (s)->fn.destroy(s)
#define segment_remove(s, da, to) (s)->fn.remove(s, da, to)
#define segment_clone(s, da, clone_ex, mode, attr, to) (s)->fn.clone(s, da, clone_ex, mode, attr, to)
#define segment_block_size(s) (s)->fn.block_size(s)
#define segment_serialize(s, exp) (s)->fn.serialize(s, exp)
#define segment_deserialize(s, id, exp) (s)->fn.deserialize(s, id, exp)
#define segment_lock(s) apr_thread_mutex_lock((s)->lock)
#define segment_unlock(s) apr_thread_mutex_unlock((s)->lock)

struct exnode_t {
    ex_header_t header;
    segment_t *default_seg;
    tbx_list_t *block;
    tbx_list_t *view;
};



typedef data_service_fn_t *(ds_create_t)(service_manager_t *ess, tbx_inip_file_t *ifd, char *section);
typedef segment_t *(segment_load_t)(void *arg, ex_id_t id, exnode_exchange_t *ex);


//** Exnode related functions
op_generic_t *exnode_remove(thread_pool_context_t *tpc, exnode_t *ex, data_attr_t *da, int timeout);
void exnode_exchange_append_text(exnode_exchange_t *exp, char *buffer);
void exnode_exchange_append(exnode_exchange_t *exp, exnode_exchange_t *exp_append);
ex_header_t *exnode_get_header(exnode_t *ex);
//Exnode3__Exnode *exnode_native2pb(exnode_t *exnode);
void exnode_exchange_init(exnode_exchange_t *exp, int type);
void exnode_exchange_free(exnode_exchange_t *exp);
ex_id_t exnode_exchange_get_default_view_id(exnode_exchange_t *exp);
void exnode_set_default(exnode_t *ex, segment_t *seg);

//exnode_t *exnode_pb2native(Exnode3__Exnode *pb);
int exnode_printf(exnode_t *ex, void *buffer, int *nbytes);
exnode_t *exnode_load(char *fname);
int exnode_save(char *fname, exnode_t *ex);

//** View related functions
int view_remove(exnode_t *ex, segment_t *view);
segment_t *view_search_by_name(exnode_t *ex, char *name);
segment_t *view_search_by_id(exnode_t *ex, ex_id_t id);

//** Segment related functions
#define segment_get_header(seg) &((seg)->header)
#define segment_set_header(seg, new_head) (seg)->header = *(new_head)
op_generic_t *segment_put(thread_pool_context_t *tpc, data_attr_t *da, segment_rw_hints_t *rw_hints, FILE *fd, segment_t *dest_seg, ex_off_t dest_offset, ex_off_t len, ex_off_t bufsize, char *buffer, int do_truncate, int timeout);
op_generic_t *segment_get(thread_pool_context_t *tpc, data_attr_t *da, segment_rw_hints_t *rw_hints, segment_t *src_seg, FILE *fd, ex_off_t src_offset, ex_off_t len, ex_off_t bufsize, char *buffer, int timeout);
segment_t *load_segment(service_manager_t *ess, ex_id_t id, exnode_exchange_t *ex);

void generate_ex_id(ex_id_t *id);

#ifdef __cplusplus
}
#endif

#endif
