#include "rr_sinker.h"

rrtask_data_t *sinker_task_fn_test(rrtask_data_t *input)
{
    //** Add your own codes to process the data
    //rr_dump(rrtask_get_data(input), rrtask_get_len(input));
    printf("Received %Zd Bytes.\n", rrtask_get_len(input));
    return NULL;
}

int main()
{
    apr_initialize();
    rrsinker_t *sinker = rrsinker_new();
    rrsinker_load_config(sinker, "zsock.cfg");
    rrsinker_start(sinker, sinker_task_fn_test);
    rrsinker_destroy(&sinker);
    apr_terminate();
    return 0;
}
