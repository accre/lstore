#include "rr_svr.h"

int main()
{
    srandom ((unsigned) time (NULL));

    rrsvr_t *svr = rrsvr_new();
    rrsvr_load_config(svr, "zsock.cfg");

    int cycles = 0;
    void *buf;
    int len = 15;
    buf = malloc(len);
    int stored_len;

    while (cycles < 1000 && !zctx_interrupted) {
        int rc = rrsvr_recv(svr, buf, len);
        assert (rc != -1);
        cycles++;
        printf ("I: normal request\n");
        stored_len = rc < len ? rc : len;
        rr_dump(buf, stored_len);
    }


    while (cycles > 0) {
        rrsvr_send(svr, buf, stored_len);
        printf("I: normal response:\n");
        rr_dump(buf, stored_len);
        cycles--;
    }

    free(buf);
    rrsvr_destroy(&svr);
    return 0;
}
