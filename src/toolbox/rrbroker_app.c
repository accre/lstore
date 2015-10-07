#include "rr_broker.h"

int main()
{
    apr_initialize();
    rrbroker_t *broker = rrbroker_new();
    rrbroker_load_config(broker, "zsock.cfg");
    rrbroker_start(broker);
    rrbroker_destroy(&broker);
    apr_terminate();
    return 0;
}
