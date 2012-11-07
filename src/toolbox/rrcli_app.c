#include "rr_cli.h"

int main()
{
    rrcli_t *cli = rrcli_new();
    rrcli_load_config(cli, "zsock.cfg");

    char test[] = "Hello";

    void *buf;
    int len = 5;
    buf = malloc(len);
    memcpy(buf, test, len);
    rrcli_send(cli, test, len);

    void *result;
    result = malloc(len);
    int rc = rrcli_recv(cli, result, len);
    assert(rc != -1);

    rr_dump(result, len);
   
    free(buf);
    free(result); 

    rrcli_destroy(&cli);
    return 0;
}
