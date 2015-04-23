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

typedef struct stack_ele {
  void *data;
  struct stack_ele *down, *up;
} Stack_ele_t;

typedef struct {
   Stack_ele_t *top, *bottom, *curr;
   int n;
} Stack_t;

int stack_size(Stack_t *);
void init_stack(Stack_t *stack);
Stack_t *new_stack();
void dup_stack(Stack_t *new, Stack_t *old);
void empty_stack(Stack_t *, int);
void free_stack(Stack_t *, int);
void *get_stack_ele_data(Stack_ele_t *ele);
void set_stack_ele_data(Stack_ele_t *ele, void *data);
void push_link(Stack_t *stack, Stack_ele_t *ele);
void push(Stack_t *, void *);
Stack_ele_t *pop_link(Stack_t *stack);
void *pop(Stack_t *);
void *get_ele_data(Stack_t *);
int move_to_top(Stack_t *);
int move_to_bottom(Stack_t *);
int move_to_link(Stack_t *, Stack_ele_t *ele);
int move_down(Stack_t *);
int move_up(Stack_t *);
int insert_link_below(Stack_t *stack, Stack_ele_t *ele);
int insert_below(Stack_t *, void *);
int insert_link_above(Stack_t *stack, Stack_ele_t *ele);
int insert_above(Stack_t *, void *);
Stack_ele_t *stack_unlink_current(Stack_t *stack, int mv_up);
int delete_current(Stack_t *, int, int);
Stack_ele_t *get_ptr(Stack_t *);
int move_to_ptr(Stack_t *, Stack_ele_t *);

#ifdef __cplusplus
}
#endif

#endif

