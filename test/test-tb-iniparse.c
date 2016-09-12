#include "task.h"
#include <string.h>
#include <tbx/iniparse.h>

TEST_IMPL(tb_iniparse) {
    const char *buf = "[foo]\nbar = baz\n";
    tbx_inip_file_t *inip = tbx_inip_string_read(buf); 
    ASSERT(inip != NULL);
    ASSERT(tbx_inip_group_count(inip) == 1);
    tbx_inip_group_t *group = tbx_inip_group_first(inip);
    ASSERT(strcmp(tbx_inip_group_get(group), "foo") == 0);
    tbx_inip_element_t *ele = tbx_inip_ele_first(group);
    ASSERT(strcmp(tbx_inip_ele_get_key(ele), "bar") == 0);
    ASSERT(strcmp(tbx_inip_ele_get_value(ele), "baz") == 0);
    ASSERT(tbx_inip_ele_next(ele) == NULL);
    tbx_inip_destroy(inip);
    return 0;
}
