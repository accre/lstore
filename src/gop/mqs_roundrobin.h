#define TEST_SIZE	1024
#define TEST_DATA	1


char *test_data = NULL;
int test_size = 1024*1024;
int packet_min = 1024;
int packet_max = 1024*1024;
int send_min = 1024;
int send_max = 10*1024*1024;
int nparallel = 100;
int ntotal = 1000;
int timeout = 10;
int stream_max_size = 4096;
int launch_flusher = 0;
int delay_response = 0;
int in_process = 0;

int ongoing_server_interval = 5;
int ongoing_client_interval = 1;
