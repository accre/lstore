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

//***********************************************************************
// Exnode3 abstract class
//***********************************************************************

#include "list.h"
#include "ex3_header.h"
#include "ex3_types.h"
#include "data_service_abstract.h"
#include "service_manager.h"
#include "data_block.h"
#include "resource_service_abstract.h"
#include "object_service_abstract.h"
#include "atomic_counter.h"
#include "opque.h"
#include "exnode3.pb-c.h"
#include "thread_pool.h"
#include "transfer_buffer.h"
#include "log.h"
#include "iniparse.h"

#ifndef _EX3_ABSTRACT_H_
#define _EX3_ABSTRACT_H_

#ifdef __cplusplus
extern "C" {
#endif

#define EX_TEXT             0
#define EX_PROTOCOL_BUFFERS 1

#define LO_SIZE_USED 0
#define LO_SIZE_MAX  1

#define INSPECT_FORCE_REPAIR          128   //** Make the repair even if it leads to data loss
#define INSPECT_SOFT_ERROR_FAIL       256   //** Treat soft errors as hard
#define INSPECT_FORCE_RECONSTRUCTION  512   //** Don't use depot-depot copies for data movement.  Instead use reconstruction
#define INSPECT_COMMAND_BITS 15

#define INSPECT_QUICK_CHECK  1
#define INSPECT_SCAN_CHECK   2
#define INSPECT_FULL_CHECK   3
#define INSPECT_QUICK_REPAIR 4
#define INSPECT_SCAN_REPAIR  5
#define INSPECT_FULL_REPAIR  6
#define INSPECT_SOFT_ERRORS  7
#define INSPECT_HARD_ERRORS  8
#define INSPECT_MIGRATE      9

#define CLONE_STRUCTURE       0
#define CLONE_STRUCT_AND_DATA 1

#define SEG_SM_LOAD   "segment_load"
#define SEG_SM_CREATE "segment_create"

typedef void segment_priv_t;

struct segment_s;
typedef struct segment_s segment_t;

typedef struct {
  op_generic_t *(*read)(segment_t *seg, data_attr_t *da, int n_iov, ex_iovec_t *iov, tbuffer_t *buffer, ex_off_t boff, int timeout);
  op_generic_t *(*write)(segment_t *seg, data_attr_t *da, int n_iov, ex_iovec_t *iov, tbuffer_t *buffer, ex_off_t boff, int timeout);
  op_generic_t *(*inspect)(segment_t *seg, data_attr_t *da, info_fd_t *fd, int mode, ex_off_t buffer_size, int timeout);
  op_generic_t *(*truncate)(segment_t *seg, data_attr_t *da, ex_off_t new_size, int timeout);
  op_generic_t *(*remove)(segment_t *seg, data_attr_t *da, int timeout);
  op_generic_t *(*flush)(segment_t *seg, data_attr_t *da, ex_off_t lo, ex_off_t hi, int timeout);
  op_generic_t *(*clone)(segment_t *seg, data_attr_t *da, segment_t **clone, int mode, void *attr, int timeout);
  int (*signature)(segment_t *seg, char *buffer, int *used, int bufsize);
  ex_off_t (*block_size)(segment_t *seg);
  ex_off_t (*size)(segment_t *seg);
  int (*serialize)(segment_t *seg, exnode_exchange_t *exp);
  int (*deserialize)(segment_t *seg, ex_id_t id, exnode_exchange_t *exp);
  void (*destroy)(segment_t *seg);
} segment_fn_t;

//#define inspect_printf(fd, ...) if ((fd) != NULL) fprintf(fd, __VA_ARGS__)

#define segment_id(s) (s)->header.id
#define segment_type(s) (s)->header.type
#define segment_destroy(s) (s)->fn.destroy(s)
#define segment_read(s, da, n_iov, iov, tbuf, boff, to) (s)->fn.read(s, da, n_iov, iov, tbuf, boff, to)
#define segment_write(s, da, n_iov, iov, tbuf, boff, to) (s)->fn.write(s, da, n_iov, iov, tbuf, boff, to)
#define segment_inspect(s, da, fd, mode, bsize, to) (s)->fn.inspect(s, da, fd, mode, bsize, to)
#define segment_truncate(s, da, new_size, to) (s)->fn.truncate(s, da, new_size, to)
#define segment_remove(s, da, to) (s)->fn.remove(s, da, to)
#define segment_flush(s, da, lo, hi, to) (s)->fn.flush(s, da, lo, hi, to)
#define segment_clone(s, da, clone_ex, mode, attr, to) (s)->fn.clone(s, da, clone_ex, mode, attr, to)
#define segment_size(s) (s)->fn.size(s)
#define segment_signature(s, buffer, used, bufsize) (s)->fn.signature(s, buffer, used, bufsize)
#define segment_block_size(s) (s)->fn.block_size(s)
#define segment_serialize(s, exp) (s)->fn.serialize(s, exp)
#define segment_deserialize(s, id, exp) (s)->fn.deserialize(s, id, exp)
#define segment_lock(s) apr_thread_mutex_lock((s)->lock)
#define segment_unlock(s) apr_thread_mutex_unlock((s)->lock)

typedef struct {
  ex_header_t header;
  segment_t *default_seg;
  list_t *block;
  list_t *view;
} exnode_t;

struct segment_s {
  ex_header_t header;
  atomic_int_t ref_count;
  segment_priv_t *priv;
  service_manager_t *ess;
  segment_fn_t fn;
  apr_thread_mutex_t *lock;
  apr_thread_cond_t *cond;
  apr_pool_t *mpool;
};


typedef data_service_fn_t *(ds_create_t)(service_manager_t *ess, inip_file_t *ifd, char *section);
typedef segment_t *(segment_load_t)(void *arg, ex_id_t id, exnode_exchange_t *ex);
typedef segment_t *(segment_create_t)(void *arg);


//** Exnode related functions
exnode_t *exnode_create();
op_generic_t *exnode_remove(thread_pool_context_t *tpc, exnode_t *ex, data_attr_t *da, int timeout);
op_generic_t *exnode_clone(thread_pool_context_t *tpc, exnode_t *ex, data_attr_t *da, exnode_t **clone_ex, void *arg, int mode, int timeout);
void exnode_destroy(exnode_t *ex);
void exnode_exchange_append_text(exnode_exchange_t *exp, char *buffer);
int exnode_serialize(exnode_t *ex, exnode_exchange_t *exp);
int exnode_deserialize(exnode_t *ex, exnode_exchange_t *exp, service_manager_t *ess);
ex_header_t *exnode_get_header(exnode_t *ex);
Exnode3__Exnode *exnode_native2pb(exnode_t *exnode);
void exnode_exchange_init(exnode_exchange_t *exp, int type);
exnode_exchange_t *exnode_exchange_create(int type);
void exnode_exchange_destroy(exnode_exchange_t *exp);
void exnode_exchange_free(exnode_exchange_t *exp);
exnode_exchange_t *exnode_exchange_load_file(char *fname);
segment_t *exnode_get_default(exnode_t *ex);
void exnode_set_default(exnode_t *ex, segment_t *seg);

exnode_t *exnode_pb2native(Exnode3__Exnode *pb);
int exnode_printf(exnode_t *ex, void *buffer, int *nbytes);
exnode_t *exnode_load(char *fname);
int exnode_save(char *fname, exnode_t *ex);

//** View related functions
int view_insert(exnode_t *ex, segment_t *view);
int view_remove(exnode_t *ex, segment_t *view);
segment_t *view_search_by_name(exnode_t *ex, char *name);
segment_t *view_search_by_id(exnode_t *ex, ex_id_t id);

//** Segment related functions
#define segment_get_header(seg) &((seg)->header)
#define segment_set_header(seg, new_head) (seg)->header = *(new_head)
op_generic_t *segment_copy(thread_pool_context_t *tpc, data_attr_t *da, segment_t *src_seg, segment_t *dest_seg, ex_off_t src_offset, ex_off_t dest_offset, ex_off_t len, ex_off_t bufsize, char *buffer, int do_truncate, int timoeut);
op_generic_t *segment_put(thread_pool_context_t *tpc, data_attr_t *da, FILE *fd, segment_t *dest_seg, ex_off_t dest_offset, ex_off_t len, ex_off_t bufsize, char *buffer, int do_truncate, int timeout);
op_generic_t *segment_get(thread_pool_context_t *tpc, data_attr_t *da, segment_t *src_seg, FILE *fd, ex_off_t src_offset, ex_off_t len, ex_off_t bufsize, char *buffer, int timeout);
segment_t *load_segment(service_manager_t *ess, ex_id_t id, exnode_exchange_t *ex);

void generate_ex_id(ex_id_t *id);

#ifdef __cplusplus
}
#endif

#endif

