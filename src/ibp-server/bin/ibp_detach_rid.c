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
  int i, port, delay, timeout, slen;
  char *host, *rid, *msg;
  tbx_ns_t *ns;

  if (argc < 5) {
     printf("ibp_detach_rid host port RID delay_before_umount [msg]\n");
     printf("\n");
     return(0);
  }

  i = 1;
  host = argv[i]; i++;
  port = atoi(argv[i]); i++;
  rid = argv[i]; i++;
  delay = atoi(argv[i]); i++;

  timeout = 3600;
  if (timeout < delay) timeout = delay + 30;  //** Make sure the timeout is longer than the delay

  msg = "";
  if (argc > i) msg = argv[i];

  slen = strlen(msg);
  sprintf(buffer, "1 92 %s %d %d %d %s\n", rid, delay, timeout, slen, msg);  // IBP_INTERNAL_RID_UMOUNT command

//printf("argc=%d i=%d command=%s\n", argc, i, buffer);
//return(0);
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
