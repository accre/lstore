#include "rr_cli.h"

// Sends file 
int f_send(rrcli_t *cli, char *fn)
{
    FILE *fp = fopen(fn, "rb");
    assert(fp != NULL);

    // Calculate file size
    fseek(fp, 0, SEEK_END);
    long file_size = ftell(fp);
    rewind(fp);

    // Allocate space to contain the whole file
    char *buffer = (char *)malloc(file_size * sizeof(char));
    assert (buffer != NULL);

    // Read file into buffer
    long rc = fread(buffer, 1, file_size, fp);
    assert(rc == file_size);

    // Send file
    rc = rrcli_send(cli, buffer, file_size);
    assert(rc != -1);
    free(buffer);
    fclose(fp);

    return file_size;
}

int main(int argc, char **argv)
{
    rrcli_t *cli = rrcli_new();
    rrcli_load_config(cli, "zsock.cfg");
    if (argc != 3) {
	printf("Command: ./rrcli_async num fname\n"); 
	exit(1);
    }

    int count;
    int rc;
    for(count = 0; count < atoi(argv[1]); count++) {
        rc = f_send(cli, argv[2]);
	assert (rc != -1);
	printf("Sent: %s,len %d\n", argv[2], rc);
    }
//sleep(15);

/*
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
*/

    rrcli_destroy(&cli);
    return 0;
}
