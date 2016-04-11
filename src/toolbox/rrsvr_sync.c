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

    while (!zctx_interrupted) {
        int rc = rrsvr_recv(svr, buf, len);
        cycles++;

        // Simulate various problems, after a few cycles
        if (cycles > 3 && randof (3) == 0) {
            printf ("I: simulating a crash\n");
            break;
        } else if (cycles > 3 && randof (3) == 0) {
            printf ("I: simulating CPU overload\n");
            sleep (2);
        }

        printf ("I: normal request\n");
        int stored_len = rc < len ? rc : len;
        rr_dump(buf, stored_len);
        sleep (1); // Do some heavy work
        rrsvr_send(svr, buf, stored_len);
    }

    free(buf);
    rrsvr_destroy(&svr);
    return 0;
}
