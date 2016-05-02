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

#define _log_module_index 111

#include <stdio.h>
#include <stdlib.h>
#include "stack.h"

#define MOVE_NOTHING 0
#define MOVE_TOP     1
#define MOVE_BOTTOM  2
#define MOVE_BOTH    3

//**************************************
// check_ends - Checks to see if an
//     insert effects top/bottom
//**************************************

int check_ends(tbx_stack_t *stack)
{
    int status;

    status = 0;
    if (stack->curr == stack->top) status = status + MOVE_TOP;
    if (stack->curr == stack->bottom) status = status + MOVE_BOTTOM;

    return(status);
}

//**************************************
// stack_size - Returns the number of elements in the stack
//**************************************

int stack_size(tbx_stack_t *stack)
{
    return(stack->n);
}

//**************************************
// get_stack_ele_data - Returns the data associated with a tbx_stack_ele_t structure
//**************************************

void *get_stack_ele_data(tbx_stack_ele_t *ele)
{
    if (ele == NULL) return(NULL);
    return(ele->data);
}

//**************************************
// get_stack_ele_data - Modifies the data associated with a tbx_stack_ele_t structure
//**************************************

void set_stack_ele_data(tbx_stack_ele_t *ele, void *data)
{
    if (ele != NULL) ele->data = data;
}

//**************************************
//new_stack - Creates a new stack
//**************************************

void init_stack(tbx_stack_t *stack)
{
    stack->top = NULL;
    stack->bottom = NULL;
    stack->curr = NULL;
    stack->n = 0;
}

//**************************************
//new_stack - Creates a new stack
//**************************************

tbx_stack_t *new_stack()
{
    tbx_stack_t *stack;

    stack = (tbx_stack_t *)malloc(sizeof(tbx_stack_t));

    init_stack(stack);

    return(stack);
}

//***************************************************
//empty_stack - REmoves all stack entries.
//   If data_also == 1 then the data is also freed.
//***************************************************

void empty_stack(tbx_stack_t *stack, int data_also)
{
    void *ptr;

    if (data_also == 1) {
        while ((ptr = pop(stack)) != NULL) {
            free(ptr);
        }
    } else {
        while ((ptr = pop(stack)) != NULL) { };
    }
}

//***************************************************
//free_stack - frees a stack.  If data_also == 1 then
//     the data is also freed.
//***************************************************

void free_stack(tbx_stack_t *stack, int data_also)
{
    empty_stack(stack, data_also);
    free(stack);
}

//***************************************************
//  dup_stack - Duplicates a stack
//***************************************************

void dup_stack(tbx_stack_t *new, tbx_stack_t *old)
{
    void *ptr;

    move_to_bottom(old);
    while ((ptr = get_ele_data(old)) != NULL) {
        push(new, ptr);
        move_up(old);
    }
}

//***************************************************
// push_link - push an unlinked element on top of the stack
//***************************************************

void push_link(tbx_stack_t *stack, tbx_stack_ele_t *ele)
{
    ele->down = stack->top;
    ele->up = NULL;

    if (stack->top == NULL) {
        stack->bottom = ele;
    } else {
        stack->top->up = ele;
    }

    stack->top = ele;
    stack->curr = ele;
    stack->n++;
}


//***************************************************
// push - push an element on top of the stack
//***************************************************

void push(tbx_stack_t *stack, void *data)
{
    tbx_stack_ele_t *ele;

    ele = (tbx_stack_ele_t *)malloc(sizeof(tbx_stack_ele_t));
    ele->data = data;

    push_link(stack, ele);
}

//***************************************************
// pop - push an element on top of the stack
//***************************************************

tbx_stack_ele_t *pop_link(tbx_stack_t *stack)
{
    move_to_top(stack);

    return(stack_unlink_current(stack, 0));
}

//***************************************************
// pop - push an element on top of the stack
//***************************************************

void *pop(tbx_stack_t *stack)
{
    tbx_stack_ele_t *ele;
    void *data;

    ele = pop_link(stack);
    if (ele == NULL) return(NULL);

    data = ele->data;
    free(ele);

    return(data);
//--------------

    if (stack->top) {
        stack->n--;
        data = stack->top->data;
        ele = stack->top;
        stack->top = stack->top->down;
        if (stack->top) {
            stack->top->up = NULL;
        } else {
            stack->bottom = NULL;   //** Empty stack
        }
        free(ele);
    } else {
        data = NULL;
    }

    return(data);
}

//***************************************************
//  get_ptr - Returns a ptr to the current stack element
//***************************************************

tbx_stack_ele_t *get_ptr(tbx_stack_t *stack)
{

    if (stack->curr) {
        return(stack->curr);
    } else {
        return(NULL);
    }
}

//***************************************************
//  get_ele_data - Returns the current elements data
//***************************************************

void *get_ele_data(tbx_stack_t *stack)
{

    if (stack->curr) {
        return(stack->curr->data);
    } else {
        return(NULL);
    }
}

//***************************************************
// move_to_ptr - Moves to the "ptr" element
//***************************************************

int move_to_ptr(tbx_stack_t *stack, tbx_stack_ele_t *ptr)
{

    stack->curr = ptr;
    return(1);
}

//***************************************************
// move_to_top - Makes the "top" element the
//    current element.
//***************************************************

int move_to_top(tbx_stack_t *stack)
{

    stack->curr = stack->top;
    return(1);
}


//***************************************************
// move_to_bottom - Makes the "bottom" element the
//    current element.
//***************************************************

int move_to_bottom(tbx_stack_t *stack)
{

    stack->curr = stack->bottom;
    return(1);
}


//***************************************************
// move_down - Move the pointer "down" to the next element.
//***************************************************

int move_down(tbx_stack_t *stack)
{

    if (stack->curr) {
        stack->curr = stack->curr->down;
        return(1);
    } else {
        return(0);
    }
}

//***************************************************
// move_up - Moves the pointer "up" to the next element.
//***************************************************

int move_up(tbx_stack_t *stack)
{

    if (stack->curr) {
        stack->curr = stack->curr->up;
        return(1);
    } else {
        return(0);
    }
}

//***************************************************
// insert_link_below - Inserts an existing "unlinked"
//      element "below" the current element and makes
//      the new element the current element
//***************************************************

int insert_link_below(tbx_stack_t *stack, tbx_stack_ele_t *ele)
{
    int move_ends;

    move_ends = check_ends(stack);
    if (stack->curr) {
        stack->n++;

        ele->down = stack->curr->down;
        ele->up = stack->curr;
        if (stack->curr->down) stack->curr->down->up = ele;
        stack->curr->down = ele;

        if ((move_ends == MOVE_TOP) || (move_ends == MOVE_BOTH)) stack->top = stack->curr;
        if ((move_ends == MOVE_BOTTOM) || (move_ends == MOVE_BOTH)) stack->bottom = ele;

        stack->curr = ele;
        return(1);
    } else if (stack->top) {
        printf("insert_link_below: Can't determine position!!!!!!!! move_ends = %d\n",move_ends);
        return(0);         // Can't determine position since curr=NULL
    } else {
        push_link(stack, ele);
        return(1);
    }
}


//***************************************************
// insert_below - Inserts the element "below" the
//    current element and makes the new element the
//    current element
//***************************************************

int insert_below(tbx_stack_t *stack, void *data)
{
    tbx_stack_ele_t *ele;

    ele =(tbx_stack_ele_t *) malloc(sizeof(tbx_stack_ele_t));
    ele->data = data;

    return(insert_link_below(stack, ele));
}


//***************************************************
// insert_link_above - Inserts the unlinked element
//    "above" the current element.
//***************************************************

int insert_link_above(tbx_stack_t *stack, tbx_stack_ele_t *ele)
{
    int move_ends;

    move_ends = check_ends(stack);

    if (stack->curr) {
        stack->n++;

        ele->down = stack->curr;
        ele->up = stack->curr->up;
        if (stack->curr->up) stack->curr->up->down = ele;
        stack->curr->up = ele;

        if ((move_ends == MOVE_TOP) || (move_ends == MOVE_BOTH)) stack->top = ele;
        if ((move_ends == MOVE_BOTTOM) || (move_ends == MOVE_BOTH)) stack->bottom = stack->curr;

        stack->curr = ele;
        return(1);
    } else if (stack->top) {
        return(0);         // Can't determine position since curr=NULL
    } else {
        push_link(stack, ele);
        return(1);
    }
}

//***************************************************
// insert_above - Inserts the element "above" the
//    current element.
//***************************************************

int insert_above(tbx_stack_t *stack, void *data)
{
    tbx_stack_ele_t *ele;

    ele =(tbx_stack_ele_t *) malloc(sizeof(tbx_stack_ele_t));
    ele->data = data;

    return(insert_link_above(stack, ele));

}


//***************************************************
// stack_unlink_current - Unblinks the current node and
//     returns the unlinked stack element or NULL.
//***************************************************

tbx_stack_ele_t *stack_unlink_current(tbx_stack_t *stack, int mv_up)
{
    tbx_stack_ele_t *ele, *up, *down;
    int move_ends;

    move_ends = check_ends(stack);

    ele = stack->curr;
    if (ele) {
        stack->n--;

        up = ele->up;
        down = ele->down;
        if (up) up->down = down;
        if (down) down->up = up;
        if (mv_up) {
            stack->curr = up;
        } else {
            stack->curr = down;
        }

        if ((move_ends == MOVE_TOP) || (move_ends == MOVE_BOTH)) stack->top = down;
        if ((move_ends == MOVE_BOTTOM) || (move_ends == MOVE_BOTH)) stack->bottom = up;

        return(ele);
    } else {
        return(NULL);
    }
}


//***************************************************
// delete_current - Deletes the current node and
//     frees the data if requested(data_also=1).
//     The pointer is repositioned above (mv_up=1) or
//     below (mv_up=0) the deleted element.
//***************************************************

int delete_current(tbx_stack_t *stack, int mv_up, int data_also)
{
    tbx_stack_ele_t *ele = stack_unlink_current(stack, mv_up);

    if (ele != NULL) {
        if (data_also) free(ele->data);
        free(ele);
        return(1);
    } else {
        return(0);
    }
}

