#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/uio.h>
#include <fcntl.h>
#include <errno.h>
#include <assert.h>
#include <tbx/network.h>
#include <tbx/net_sock.h>
#include <tbx/log.h>
#include <tbx/dns_cache.h>
#include <tbx/string_token.h>
#include <ibp-server/cmd_send.h>


//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
  int bufsize = 1024*1024;
  char buffer[bufsize], *bstate;
  int err;
  int n;
  apr_time_t end_time;
  tbx_ns_t *ns;
  char cmd[512];
  char *host;
  int port = 6714;
  int timeout = 15;

  if (argc < 2) {
     printf("get_config -a | host [port timeout]\n");
     printf("   -a   -Use the local host and default port\n");
     return(0);
  }

 if (strcmp(argv[1], "-a") == 0) {
    host = (char *)malloc(1024);
    gethostname(host, 1023);
  } else {
    host = argv[1];
  }

  if (argc > 2) port = atoi(argv[2]);
  if (argc == 4) timeout = atoi(argv[3]);

  sprintf(cmd, "1 94 %d\n", timeout);  // IBP_ST_VERSION command

  assert(apr_initialize() == APR_SUCCESS);

  tbx_dnsc_startup_sized(10);

  ns = cmd_send(host, port, cmd, &bstate, timeout);
  if (ns == NULL) return(-1);

  //** Get the number of bytes
  n = atoi(tbx_stk_string_token(NULL, " ", &bstate, &err));

  //** and read it in
  end_time = apr_time_now() + apr_time_make(timeout, 0);
  err = server_ns_read_block(ns, end_time, buffer, n);

  if (err != 0) {
     printf("Error %d returned while reading data %d bytes\n", err, n);
     tbx_ns_close(ns);
     return(n);
  }

  printf("%s", buffer);
  printf("\n");

  //** Close the connection
  tbx_ns_close(ns);

  apr_terminate();

  return(0);
}
