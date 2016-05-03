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

#ifndef __STACK_H__
#define __STACK_H__

#ifdef __cplusplus
extern "C" {
#endif
#include "tbx/toolbox_visibility.h"

typedef struct tbx_stack_ele_t tbx_stack_ele_t;
struct tbx_stack_ele_t {
    void *data;
    struct tbx_stack_ele_t *down, *up;
};

typedef struct tbx_stack_t tbx_stack_t;
struct tbx_stack_t {
    tbx_stack_ele_t *top, *bottom, *curr;
    int n;
};

TBX_API int stack_size(tbx_stack_t *stack);
TBX_API void init_stack(tbx_stack_t *stack);
TBX_API tbx_stack_t *new_stack();
TBX_API void dup_stack(tbx_stack_t *new, tbx_stack_t *old);
TBX_API void empty_stack(tbx_stack_t *stack, int data_also);
TBX_API void free_stack(tbx_stack_t *stack, int data_also);
TBX_API void *get_stack_ele_data(tbx_stack_ele_t *ele);
void set_stack_ele_data(tbx_stack_ele_t *ele, void *data);
TBX_API void push_link(tbx_stack_t *stack, tbx_stack_ele_t *ele);
TBX_API void push(tbx_stack_t *stack, void *data);
tbx_stack_ele_t *pop_link(tbx_stack_t *stack);
TBX_API void *pop(tbx_stack_t *stack);
TBX_API void *get_ele_data(tbx_stack_t *stack);
TBX_API int move_to_top(tbx_stack_t *stack);
TBX_API int move_to_bottom(tbx_stack_t *stack);
int move_to_link(tbx_stack_t *stack, tbx_stack_ele_t *ele);
TBX_API int move_down(tbx_stack_t *stack);
TBX_API int move_up(tbx_stack_t *stack);
int insert_link_below(tbx_stack_t *stack, tbx_stack_ele_t *ele);
TBX_API int insert_below(tbx_stack_t *stack, void *data);
TBX_API int insert_link_above(tbx_stack_t *stack, tbx_stack_ele_t *ele);
TBX_API int insert_above(tbx_stack_t *stack, void *data);
TBX_API tbx_stack_ele_t *stack_unlink_current(tbx_stack_t *stack, int mv_up);
TBX_API int delete_current(tbx_stack_t *stack, int mv_up, int data_also);
TBX_API tbx_stack_ele_t *get_ptr(tbx_stack_t *stack);
TBX_API int move_to_ptr(tbx_stack_t *stack, tbx_stack_ele_t *ptr);

#ifdef __cplusplus
}
#endif

#endif

