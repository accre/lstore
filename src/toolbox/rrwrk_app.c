#include "rr_wrk.h"

rrtask_data_t *task_fn_test(rrtask_data_t *input)
{
    //** Add your own codes to process the data
    // rr_dump(rrtask_get_data(input), rrtask_get_len(input));
    printf("Working on %Zd Bytes.\n", rrtask_get_len(input));
    rrtask_data_t *output = rrtask_data_new();
    rrtask_set_data(output, rrtask_get_data(input), rrtask_get_len(input));
    sleep(60);
    printf("Processed %Zd Bytes.\n", rrtask_get_len(output));
    return output;
}

int main()
{
    apr_initialize();
    rrwrk_t *wrk = rrwrk_new();
    rrwrk_load_config(wrk, "zsock.cfg");

    while(true) {
	int rc = rrwrk_start(wrk, task_fn_test);
	if (rc == -1)
	    break;
    }
   
    rrwrk_destroy(&wrk); 
    apr_terminate();
    return 0;
}
