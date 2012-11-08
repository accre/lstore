#include "rr_cli.h"

int main()
{
    rrcli_t *cli = rrcli_new();
    rrcli_load_config(cli, "zsock.cfg");

    int count;
    char test[] = "Hello";
    int len = 5;
    int rc;
    for(count = 0; count < 1000; count++) {
        rc = rrcli_send(cli, test, len);
	assert (rc != -1);
    }

    while(rc != -1 && !zctx_interrupted && count-- > 0) {
        void *result;
	int recv_len = 20;
        result = malloc(recv_len);
        rc = rrcli_recv(cli, result, recv_len);
    
        if (rc != -1) {
	    printf("I: normal response\n");
    	    int stored_len = rc < recv_len ?rc : recv_len;
	    rr_dump(result, stored_len);
	    printf("Expected length: %d.\nReceived message length: %d.\nStored bytes len: %d.\n", recv_len, rc, stored_len);
        } else {
	    printf("I: something wrong with server...\n");
        }
        free(result); 
    }

    rrcli_destroy(&cli);
    return 0;
}
