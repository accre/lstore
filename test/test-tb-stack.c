#include "task.h"
#include <tbx/stack.h>
#include <stack.h>
#include <stdio.h>

// Obviously, this should/could be fleshed out more
TEST_IMPL(tb_stack) {
    tbx_stack_t * stack = tbx_stack_new();
    NOTE("STACK");
    ASSERT(stack != 0);
    NOTE("STACK");
    ASSERT(tbx_get_ptr(stack) == NULL);
    NOTE("STACK");
    ASSERT(tbx_get_ele_data(stack) == NULL);
    NOTE("STACK");
    ASSERT(tbx_stack_size(stack) == 0);
    NOTE("STACK");
    
    tbx_stack_empty(stack, 0);
    NOTE("STACK");
    ASSERT(stack != 0);
    NOTE("STACK");

    tbx_stack_empty(stack, 1);
    NOTE("STACK");
    ASSERT(stack != 0);
    NOTE("STACK");
    
    tbx_stack_t * stack2 = tbx_stack_new();
    NOTE("STACK");
    ASSERT(stack2 != 0);
    NOTE("STACK");

    tbx_free_stack(stack, 1);
    NOTE("STACK");
    tbx_free_stack(stack2, 1);
    NOTE("STACK");
    
    return 0;
}
