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
//******************************************************************
//******************************************************************

#ifndef __PIGEON_COOP_H_
#define __PIGEON_COOP_H_

#include "tbx/toolbox_visibility.h"
#include <apr_thread_mutex.h>
#include <apr_pools.h>
#include "pigeon_hole.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tbx_pc_t tbx_pc_t;
struct tbx_pc_t {
    apr_thread_mutex_t *lock;
    apr_pool_t *pool;
    int nshelves;
    int shelf_size;
    int item_size;
    int check_shelf;
    int nused;
    const char *name;
    void *new_arg;
    tbx_ph_t **ph_shelf;
    char **data_shelf;
    void *(*new)(void *arg, int size);
    void (*free)(void *arg, int size, void *dshelf);
};

typedef struct tbx_pch_t tbx_pch_t;
struct tbx_pch_t {
    int shelf;
    int hole;
    void *data;
};

typedef struct tbx_pc_iter_t tbx_pc_iter_t;
struct tbx_pc_iter_t {
    tbx_pc_t *pc;
    int shelf;
    tbx_ph_iter_t pi;
};

tbx_pc_iter_t pigeon_coop_iterator_init(tbx_pc_t *ph);
tbx_pch_t pigeon_coop_iterator_next(tbx_pc_iter_t *pci);
TBX_API void *pigeon_coop_hole_data(tbx_pch_t *pch);
TBX_API int release_pigeon_coop_hole(tbx_pc_t *ph, tbx_pch_t *pch);
TBX_API tbx_pch_t reserve_pigeon_coop_hole(tbx_pc_t *pc);
TBX_API void destroy_pigeon_coop(tbx_pc_t *ph);
TBX_API tbx_pc_t *new_pigeon_coop(const char *name, int size, int item_size, void *new_arg, void *(*new)(void *arg, int size),
                               void (*free)(void *arg, int size, void *dshelf));

#ifdef __cplusplus
}
#endif

#endif


