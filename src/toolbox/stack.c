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

int check_ends(Stack_t *stack) {
   int status;

   status = 0;
   if (stack->curr == stack->top) status = status + MOVE_TOP;
   if (stack->curr == stack->bottom) status = status + MOVE_BOTTOM;

   return(status);
}

//**************************************
// stack_size - Returns the number of elements in the stack
//**************************************

int stack_size(Stack_t *stack)
{
  return(stack->n);
}

//**************************************
// get_stack_ele_data - Returns the data associated with a Stack_ele_t structure
//**************************************

void *get_stack_ele_data(Stack_ele_t *ele)
{
  if (ele == NULL) return(NULL);
  return(ele->data);
}

//**************************************
// get_stack_ele_data - Modifies the data associated with a Stack_ele_t structure
//**************************************

void set_stack_ele_data(Stack_ele_t *ele, void *data)
{
  if (ele != NULL) ele->data = data;
}

//**************************************
//new_stack - Creates a new stack
//**************************************

void init_stack(Stack_t *stack) {
  stack->top = NULL;
  stack->bottom = NULL;
  stack->curr = NULL;
  stack->n = 0;
}

//**************************************
//new_stack - Creates a new stack
//**************************************

Stack_t *new_stack() {
  Stack_t *stack;

  stack = (Stack_t *)malloc(sizeof(Stack_t));

  init_stack(stack);

  return(stack);
}

//***************************************************
//empty_stack - REmoves all stack entries.
//   If data_also == 1 then the data is also freed.
//***************************************************

void empty_stack(Stack_t *stack, int data_also) {
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

void free_stack(Stack_t *stack, int data_also) {
  empty_stack(stack, data_also);
  free(stack);
}

//***************************************************
// push_link - push an unlinked element on top of the stack
//***************************************************

void push_link(Stack_t *stack, Stack_ele_t *ele) {
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

void push(Stack_t *stack, void *data) {
   Stack_ele_t *ele;

   ele = (Stack_ele_t *)malloc(sizeof(Stack_ele_t));
   ele->data = data;

   push_link(stack, ele);
}

//***************************************************
// pop - push an element on top of the stack
//***************************************************

Stack_ele_t *pop_link(Stack_t *stack) {
   move_to_top(stack);

   return(stack_unlink_current(stack, 0));
}

//***************************************************
// pop - push an element on top of the stack
//***************************************************

void *pop(Stack_t *stack) {
   Stack_ele_t *ele;
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

Stack_ele_t *get_ptr(Stack_t *stack) {

  if (stack->curr) {
     return(stack->curr);
  } else {
     return(NULL);
  }
}

//***************************************************
//  get_ele_data - Returns the current elements data
//***************************************************

void *get_ele_data(Stack_t *stack) {

  if (stack->curr) {
     return(stack->curr->data);
  } else {
     return(NULL);
  }
}

//***************************************************
// move_to_ptr - Moves to the "ptr" element
//***************************************************

int move_to_ptr(Stack_t *stack, Stack_ele_t *ptr) {

  stack->curr = ptr;
  return(1);
}

//***************************************************
// move_to_top - Makes the "top" element the
//    current element.
//***************************************************

int move_to_top(Stack_t *stack) {

  stack->curr = stack->top;
  return(1);
}


//***************************************************
// move_to_bottom - Makes the "bottom" element the
//    current element.
//***************************************************

int move_to_bottom(Stack_t *stack) {

  stack->curr = stack->bottom;
  return(1);
}


//***************************************************
// move_down - Move the pointer "down" to the next element.
//***************************************************

int move_down(Stack_t *stack) {

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

int move_up(Stack_t *stack) {

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

int insert_link_below(Stack_t *stack, Stack_ele_t *ele) {
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

int insert_below(Stack_t *stack, void *data) {
  Stack_ele_t *ele;

  ele =(Stack_ele_t *) malloc(sizeof(Stack_ele_t));
  ele->data = data;

  return(insert_link_below(stack, ele));
}


//***************************************************
// insert_link_above - Inserts the unlinked element
//    "above" the current element.
//***************************************************

int insert_link_above(Stack_t *stack, Stack_ele_t *ele) {
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

int insert_above(Stack_t *stack, void *data) {
  Stack_ele_t *ele;

  ele =(Stack_ele_t *) malloc(sizeof(Stack_ele_t));
  ele->data = data;

  return(insert_link_above(stack, ele));

}


//***************************************************
// stack_unlink_current - Unblinks the current node and
//     returns the unlinked stack element or NULL.
//***************************************************

Stack_ele_t *stack_unlink_current(Stack_t *stack, int mv_up) {
   Stack_ele_t *ele, *up, *down;
   int move_ends;

   move_ends = check_ends(stack);

   ele = stack->curr;
   if (ele) {
     stack->n--;

     up = ele->up;  down = ele->down;
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

int delete_current(Stack_t *stack, int mv_up, int data_also)
{
  Stack_ele_t *ele = stack_unlink_current(stack, mv_up);

  if (ele != NULL) {
     if (data_also) free(ele->data);
     free(ele);
     return(1);
  } else {
     return(0);
  }
}

