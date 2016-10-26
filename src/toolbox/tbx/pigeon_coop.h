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

#pragma once
#ifndef ACCRE_PIGEON_COOP_H_INCLUDED
#define ACCRE_PIGEON_COOP_H_INCLUDED

#include <tbx/visibility.h>

#ifdef __cplusplus
extern "C" {
#endif

// Types
typedef struct tbx_pc_iter_t tbx_pc_iter_t;

typedef struct tbx_pc_t tbx_pc_t;

typedef struct tbx_pch_t tbx_pch_t;
typedef void *(*tbx_pc_new_fn_t)(void *arg, int size);
typedef void (*tbx_pc_free_fn_t)(void *arg, int size, void *dshelf);

// Functions
TBX_API void tbx_pc_destroy(tbx_pc_t *pc);
TBX_API tbx_pc_t *tbx_pc_new(const char *name, int size, int item_size,
                                void *new_arg,
                                tbx_pc_new_fn_t new_fn,
                                tbx_pc_free_fn_t free);
TBX_API void *tbx_pch_data(tbx_pch_t *pch);
TBX_API int tbx_pch_release(tbx_pc_t *pc, tbx_pch_t *pch);
TBX_API tbx_pch_t tbx_pch_reserve(tbx_pc_t *pc);
TBX_API tbx_pc_iter_t tbx_pc_iter_init(struct tbx_pc_t *pc);
TBX_API tbx_pch_t tbx_pc_next(struct tbx_pc_iter_t *pci);

// TEMPORARY
struct tbx_pch_t {
    int shelf;
    int hole;
    void *data;
};

#ifdef __cplusplus
}
#endif

#endif
