#include "task.h"
#include <tbx/stack.h>
#include <stdio.h>

// Obviously, this should/could be fleshed out more
TEST_IMPL(tb_stack) {
    tbx_stack_t * stack = tbx_stack_new();
    ASSERT(stack != 0);
    ASSERT(tbx_stack_get_current_ptr(stack) == NULL);
    ASSERT(tbx_stack_get_current_data(stack) == NULL);
    ASSERT(tbx_stack_count(stack) == 0);

    // Add a few elements
    void *ele1 = (void *) 1;
    tbx_stack_push(stack, ele1);
    ASSERT(tbx_stack_count(stack) == 1);
    ASSERT(tbx_stack_get_current_data(stack) == ele1);
    
    void *ele2 = (void *) 2;
    tbx_stack_push(stack, ele2);
    ASSERT(tbx_stack_count(stack) == 2);
    ASSERT(tbx_stack_get_current_data(stack) == ele2);

    void *ele3 = (void *) 3;
    tbx_stack_push(stack, ele3);
    ASSERT(tbx_stack_count(stack) == 3);
    ASSERT(tbx_stack_get_current_data(stack) == ele3);

    // Move up and down
    tbx_stack_move_to_bottom(stack);
    ASSERT(tbx_stack_get_current_data(stack) == ele1);
    tbx_stack_move_to_top(stack);
    ASSERT(tbx_stack_get_current_data(stack) == ele3);
    tbx_stack_move_down(stack);
    ASSERT(tbx_stack_get_current_data(stack) == ele2);
    tbx_stack_move_down(stack);
    ASSERT(tbx_stack_get_current_data(stack) == ele1);
    tbx_stack_move_up(stack);
    ASSERT(tbx_stack_get_current_data(stack) == ele2);
    tbx_stack_move_up(stack);
    ASSERT(tbx_stack_get_current_data(stack) == ele3);

    tbx_stack_empty(stack, 0);
    ASSERT(stack != 0);

    tbx_stack_empty(stack, 1);
    ASSERT(stack != 0);
    
    tbx_stack_t * stack2 = tbx_stack_new();
    ASSERT(stack2 != 0);

    tbx_stack_free(stack, 1);
    tbx_stack_free(stack2, 1);
    
    return 0;
}
