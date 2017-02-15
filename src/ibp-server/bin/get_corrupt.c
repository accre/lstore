/*
   Copyright 2016 Vanderbilt University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
   
       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/ 

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
#include "cmd_send.h"

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
  int bufsize = 1024*1024;
  char buffer[bufsize], *bstate;
  int err;
  int n, i;
  tbx_ns_t *ns;
  tbx_ns_timeout_t dt;

  if (argc < 4) {
     printf("get_corrupt host port rid [timeout]\n");
     return(0);
  }

  char cmd[512];
  char *host = argv[1];
  int port = atoi(argv[2]);
  char *rid = argv[3];
  int timeout = 15;

  if (argc == 5) timeout = atoi(argv[4]);

  sprintf(cmd, "1 93 %s %d\n", rid, timeout);

  assert(apr_initialize() == APR_SUCCESS);

  tbx_dnsc_startup_sized(10);

  ns = cmd_send(host, port, cmd, &bstate, timeout);

  //** Get the number of corrupt allocations
  n = atoi(tbx_stk_string_token(NULL, " ", &bstate, &err));

  printf("Corrupt Allocation count: %d\n", n);

  //** and read them in
  tbx_ns_timeout_set(&dt, timeout, 0);
  for (i=0; i<n; i++) {
     err = server_ns_readline(ns, buffer, sizeof(buffer), dt);
     printf("%s\n", buffer);
  }

  //** Close the connection
  tbx_ns_close(ns);

  apr_terminate();

  return(0);
}
