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
#ifndef ACCRE_STACK_H_INCLUDED
#define ACCRE_STACK_H_INCLUDED

#include "tbx/toolbox_visibility.h"

#ifdef __cplusplus
extern "C" {
#endif

// Types
typedef struct tbx_stack_ele_t tbx_stack_ele_t;

typedef struct tbx_stack_t tbx_stack_t;

// Functions
TBX_API int tbx_stack_delete_current(tbx_stack_t *stack, int mv_up, int data_also);

TBX_API void tbx_stack_dup(tbx_stack_t *new, tbx_stack_t *old);

TBX_API void tbx_stack_empty(tbx_stack_t *stack, int data_also);

TBX_API void tbx_stack_free(tbx_stack_t *stack, int data_also);

TBX_API void *tbx_stack_get_current_data(tbx_stack_t *stack);

TBX_API tbx_stack_ele_t *tbx_stack_get_current_ptr(tbx_stack_t *stack);

TBX_API void *tbx_stack_ele_get_data(tbx_stack_ele_t *ele);

TBX_API void tbx_stack_init(tbx_stack_t *stack);

TBX_API int tbx_stack_insert_above(tbx_stack_t *stack, void *data);

TBX_API int tbx_stack_insert_below(tbx_stack_t *stack, void *data);

TBX_API int tbx_stack_link_insert_above(tbx_stack_t *stack, tbx_stack_ele_t *ele);

TBX_API int tbx_stack_move_down(tbx_stack_t *stack);

TBX_API int tbx_stack_move_to_bottom(tbx_stack_t *stack);

TBX_API int tbx_stack_move_to_ptr(tbx_stack_t *stack, tbx_stack_ele_t *ptr);

TBX_API int tbx_stack_move_to_top(tbx_stack_t *stack);

TBX_API int tbx_stack_move_up(tbx_stack_t *stack);

TBX_API tbx_stack_t *tbx_stack_new();

TBX_API void *tbx_stack_pop(tbx_stack_t *stack);

TBX_API void tbx_stack_push(tbx_stack_t *stack, void *data);

TBX_API void tbx_stack_link_push(tbx_stack_t *stack, tbx_stack_ele_t *ele);

TBX_API int tbx_stack_size(tbx_stack_t *stack);

TBX_API tbx_stack_ele_t *tbx_stack_unlink_current(tbx_stack_t *stack, int mv_up);

TBX_API void *tbx_stack_ele_get_data(tbx_stack_ele_t *ele);

TBX_API tbx_stack_ele_t *tbx_stack_ele_get_down(tbx_stack_ele_t * stack);

TBX_API tbx_stack_ele_t *tbx_stack_top_get(tbx_stack_t * stack);

// TEMPORARY
#if !defined toolbox_EXPORTS && defined LSTORE_HACK_EXPORT
    struct tbx_stack_ele_t {
        void *data;
        struct tbx_stack_ele_t *down, *up;
    };

    struct tbx_stack_t {
        tbx_stack_ele_t *top, *bottom, *curr;
        int n;
    };
#endif


#ifdef __cplusplus
}
#endif

#endif
