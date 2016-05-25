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
    ASSERT(tbx_stack_get_current_ptr(stack) == NULL);
    NOTE("STACK");
    ASSERT(tbx_stack_get_current_data(stack) == NULL);
    NOTE("STACK");
    ASSERT(tbx_stack_count(stack) == 0);
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

    tbx_stack_free(stack, 1);
    NOTE("STACK");
    tbx_stack_free(stack2, 1);
    NOTE("STACK");
    
    return 0;
}
