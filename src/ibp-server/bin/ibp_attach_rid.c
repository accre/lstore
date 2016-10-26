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
#include <ibp-server/cmd_send.h>

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
  int bufsize = 1024*1024;
  char buffer[bufsize], *bstate;
  int i, port, force_rebuild, timeout, slen;
  char *host, *rid, *msg;
  tbx_ns_t *ns;

  if (argc < 4) {
     printf("ibp_attach_rid [-r] host port RID [msg]\n");
     printf("\n");
     return(0);
  }

  i = 1;
  force_rebuild = 0;
  if (strcmp(argv[i], "-r") == 0) { force_rebuild = 2; i++; }

  host = argv[i]; i++;
  port = atoi(argv[i]); i++;
  rid = argv[i]; i++;

  timeout = 60;
  msg = "";
  if (argc > i) msg = argv[i];

  slen = strlen(msg);
  sprintf(buffer, "1 91 %s %d %d %d %s\n", rid, force_rebuild, timeout, slen, msg);  // IBP_INTERNAL_RID_MOUNT command

  assert(apr_initialize() == APR_SUCCESS);

  tbx_dnsc_startup_sized(10);

  ns = cmd_send(host, port, buffer, &bstate, timeout);
  if (ns == NULL) return(-1);
  if (bstate != NULL) free(bstate);

  //** Close the connection
  tbx_ns_close(ns);

  apr_terminate();

  return(0);
}
