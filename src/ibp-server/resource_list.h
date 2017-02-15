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

