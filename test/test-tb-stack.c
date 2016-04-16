#include "task.h"
#include "stack.h"
#include <stdio.h>

// Obviously, this should/could be fleshed out more
TEST_IMPL(tb_stack) {
    Stack_t * stack = new_stack();
    ASSERT(stack != 0);
    ASSERT(get_ptr(stack) == NULL);
    ASSERT(get_ele_data(stack) == NULL);
    ASSERT(stack_size(stack) == 0);
    
    empty_stack(stack, 0);
    ASSERT(stack != 0);

    empty_stack(stack, 1);
    ASSERT(stack != 0);
    
    Stack_t * stack2 = new_stack();
    ASSERT(stack2 != 0);
    
    return 0;
}
