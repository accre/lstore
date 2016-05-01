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

