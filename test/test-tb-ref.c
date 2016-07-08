#include "task.h"
#include <tbx/ref.h>
#include <stdio.h>

struct test_obj_t {
    tbx_ref_t ref;
};
static int counter = 0;
void inc_counter(tbx_ref_t *ref) {
    counter++;
}

TEST_IMPL(tb_ref) {
    struct test_obj_t test_obj;
    tbx_ref_init(&test_obj.ref);
    tbx_ref_get(&test_obj.ref);
    tbx_ref_get(&test_obj.ref);
    tbx_ref_put(&test_obj.ref, inc_counter);
    ASSERT(counter == 0);
    tbx_ref_put(&test_obj.ref, inc_counter);
    ASSERT(counter == 0);
    tbx_ref_put(&test_obj.ref, inc_counter);
    ASSERT(counter == 1);
    return 0;
}
