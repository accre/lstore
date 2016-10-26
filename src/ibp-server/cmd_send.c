#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
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
#include <tbx/fmttypes.h>
#include <ibp-server/ibp_server.h>
#include <ibp-server/cmd_send.h>

//** This is a hack to not have to have the ibp source
#define IBP_OK 1
#define IBP_E_OUT_OF_SOCKETS -66

//*************************************************************************
//*************************************************************************

tbx_ns_t *cmd_send(char *host, int port, char *cmd, char **res_buffer, int timeout)
{
  int bufsize = 10*1024;
  char buffer[bufsize];
  char *bstate;
  int err, failed;
  int n, retry;
  double swait;
  tbx_ns_timeout_t dt;
  tbx_ns_t *ns;
  apr_time_t await, sec, usec;

  *res_buffer = NULL;

//tbx_set_log_level(20);
//int loop =0;
  do {
//loop++;
//printf("cmd_send: loop=%d\n", loop);

     failed = 0;
     //** Make the host connection
     retry = 3;
     do {
        ns = tbx_ns_new();
        tbx_ns_sock_config(ns, -1);
        tbx_ns_timeout_set(&dt, 60, 0);

        err = tbx_ns_connect(ns, host, port, dt);
        if (err != 0) {
           tbx_ns_destroy(ns);
           printf("get_version: Can't connect to host!  host=%s port=%d  err=%d\n", host, port, err);
           if (retry <= 0) {
              printf("cmd_send: Aborting!\n");
              return(NULL);
           } else {
              printf("cmd_send: Sleeping for 1 second and retrying(%d).\n", retry);
              sleep(1);
              retry--;
           }
        }
     } while ((err != 0)  && (retry > -1));

     //** Send the command
     dt = apr_time_now() + apr_time_make(timeout,0);
     n = server_ns_write_block(ns, dt, cmd, strlen(cmd));

     //** Get the result line
     tbx_ns_timeout_set(&dt, timeout, 0);
     n = server_ns_readline(ns, buffer, bufsize, dt);
     if (n == NS_OK) {
        n = atoi(tbx_stk_string_token(buffer, " ", &bstate, &err));
        if (n != IBP_OK) {
           if (n == IBP_E_OUT_OF_SOCKETS) {
              swait = atof(tbx_stk_string_token(NULL, " ", &bstate, &err));
              printf("Depot is busy. Sleeping for %lf sec and retrying.\n", swait);
              tbx_ns_destroy(ns);
              failed = 1;
              sec = swait; usec = (swait-sec) * 1000000;
              await = apr_time_make(sec, usec);
//              printf("sec=" TT " usec=" TT "  await=" TT "\n", sec, usec,await);
              apr_sleep(await);
           } else {
              printf("Error %d returned!\n", n);
              tbx_ns_destroy(ns);
              return(NULL);
           }
        }
     } else {
//printf("cmd_send: server_ns_readline=%d\n", n);
       failed = 1;
     }
  } while (failed == 1);

  *res_buffer = strdup(bstate);

  return(ns);
}
