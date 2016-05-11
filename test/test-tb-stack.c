#include "task.h"
#include <tbx/stack.h>
#include <stack.h>
#include <stdio.h>

// Obviously, this should/could be fleshed out more
TEST_IMPL(tb_stack) {
    tbx_stack_t * stack = tbx_stack_new();
    ASSERT(stack != 0);
    ASSERT(tbx_get_ptr(stack) == NULL);
    ASSERT(tbx_get_ele_data(stack) == NULL);
    ASSERT(tbx_stack_size(stack) == 0);
    
    tbx_stack_empty(stack, 0);
    ASSERT(stack != 0);

    tbx_stack_empty(stack, 1);
    ASSERT(stack != 0);
    
    tbx_stack_t * stack2 = tbx_stack_new();
    ASSERT(stack2 != 0);

    tbx_free_stack(stack, 1);
    tbx_free_stack(stack2, 1);
    
    return 0;
}
