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

#include <apr_pools.h>
#include <apr_thread_mutex.h>

#include "pigeon_hole.h"
#include "tbx/pigeon_coop.h"

#ifdef __cplusplus
extern "C" {
#endif

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

struct tbx_pc_iter_t {
    struct tbx_pc_t *pc;
    int shelf;
    tbx_ph_iter_t pi;
};

#ifdef __cplusplus
}
#endif

#endif


