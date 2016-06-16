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

#ifndef __PIGEON_HOLE_H_
#define __PIGEON_HOLE_H_

#include <apr_pools.h>
#include <apr_thread_mutex.h>

#include "tbx/pigeon_hole.h"

#ifdef __cplusplus
extern "C" {
#endif

struct tbx_ph_t {
    apr_thread_mutex_t *lock;
    apr_pool_t *pool;
    int nholes;
    int nused;
    int next_slot;
    char *hole;
    const char *name;
};

struct tbx_ph_iter_t {
    tbx_ph_t *ph;
    int start_slot;
    int count;
    int found;
};

tbx_ph_iter_t pigeon_hole_iterator_init(tbx_ph_t *ph);
int pigeon_hole_iterator_next(tbx_ph_iter_t *pi);
int pigeon_holes_used(tbx_ph_t *ph);
int pigeon_holes_free(tbx_ph_t *ph);
void release_pigeon_hole(tbx_ph_t *ph, int slot);
int reserve_pigeon_hole(tbx_ph_t *ph);
void destroy_pigeon_hole(tbx_ph_t *ph);
tbx_ph_t *new_pigeon_hole(const char *name, int size);

#ifdef __cplusplus
}
#endif

#endif

