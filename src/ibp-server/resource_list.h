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

//****************************************************************
//****************************************************************

#ifndef __RESOURCE_LIST_H_
#define __RESOURCE_LIST_H_

#include <apr_hash.h>
#include "resource.h"
#include <tbx/log.h>
#include "debug.h"
#include "stack.h"

#define RL_PICK_RANDOM      0
#define RL_PICK_MOST_FREE   1
#define RL_PICK_ROUND_ROBIN 2

typedef struct {
  Resource_t *r;
  char *crid;
  int  used;
} rl_ele_t;

struct Resource_list_s {
   apr_pool_t *mpool;
   apr_hash_t *table;
   apr_thread_mutex_t *lock;
   rl_ele_t   *res;
   tbx_stack_t *pending;
   int        n;
   int        max_res;
   int        pick_policy;
   int        pick_index;
   Resource_t *(*pick_routine)(struct Resource_list_s *rl, rid_t *rid);
};

typedef struct Resource_list_s Resource_list_t;
typedef int resource_list_iterator_t;

IBPS_API int resource_list_n_used(Resource_list_t *rl);
IBPS_API resource_list_iterator_t resource_list_iterator(Resource_list_t *rl);
IBPS_API Resource_t *resource_list_iterator_next(Resource_list_t *rl, resource_list_iterator_t *it);
IBPS_API void resource_list_iterator_destroy(Resource_list_t *rl, resource_list_iterator_t *it);
IBPS_API int resource_list_pending_insert(Resource_list_t *rl, char *rid);
IBPS_API int resource_list_pending_activate(Resource_list_t *rl, char *rid, Resource_t *r);
IBPS_API int resource_list_insert(Resource_list_t *rl, Resource_t *r);
IBPS_API int resource_list_delete(Resource_list_t *rl, Resource_t *r);
IBPS_API Resource_list_t *create_resource_list(int n);
IBPS_API void free_resource_list(Resource_list_t *rl);
IBPS_API Resource_t *resource_lookup(Resource_list_t *rl, char *rid);
IBPS_API Resource_t *resource_pick_most_free(Resource_list_t *rl, rid_t *rid);
IBPS_API Resource_t *resource_pick_random(Resource_list_t *rl, rid_t *rid);
IBPS_API Resource_t *resource_pick_round_robin(Resource_list_t *rl, rid_t *rid);
IBPS_API Resource_t *resource_pick(Resource_list_t *rl, rid_t *rid);
IBPS_API void resource_set_pick_policy(Resource_list_t *rl, int policy);

#endif

