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
#include <time.h>
#include <assert.h>
#include <tbx/fmttypes.h>
#include "allocation.h"
#include "ibp_ClientLib.h"
#include "ibp_server.h"
#include <tbx/log.h>
#include <tbx/fmttypes.h>
#include "subnet.h"
#include "ibp_time.h"
#include "print_alloc.h"
#include "osd_abstract.h"
//#include "osd_fs.h"
#include "osd_fs.h"
#include "resource.h"

#define HEADER_SIZE 4096
#define MAX_SIZE 100

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
  int i;
  int volatile start_option;  //** This makes the compiler not generate an error due to optimizations
  char buffer[10240];
  char *afile, *state;
  osd_fd_t *afd;
  osd_t *dev;
  struct stat sbuf;
  resource_usage_file_t usage;

//printf("sizeof(Allocation_t)=" LU " ALLOC_HEADER=%d\n", sizeof(Allocation_t), ALLOC_HEADER);

  if (argc < 2) {
     printf("get_rid_status [-d debug_level] RID|RID_usage_file\n");
     printf("   -d debug_level  Sets the debug level.  Default is 0.\n");
     printf("\n");
     return(0);
  }

  //** Initialize APR for use
  assert(apr_initialize() == APR_SUCCESS);
  tbx_set_log_level(0);

  i = 1;

  do {
     start_option = i;
     if (strcmp("-d", argv[i]) == 0) {
        i++;
        tbx_set_log_level(atoi(argv[i])); i++;
     }
  } while (start_option < i);

  //** Determine if this is just a RID or an actual full file path
  afile = argv[i]; i++;

  //** Assume it's a complete file path
  if (stat(afile, &sbuf) != 0) {  //** Not an absolute path. Flip to a RID check
     snprintf(buffer, sizeof(buffer), "/depot/rid-%s/data/1/1", afile);
     if (stat(buffer, &sbuf) != 0) {
        printf("ERROR:  Unable to open usage file: %s or %s\n", afile, buffer);
        return(1);
     }
     afile = buffer;
  }

  //** Read the Allocation ***
  dev = osd_mount_fs("loopback", 10, 1000);  //** Mount the file via loopback
  fs_associate_id(dev, 0, afile);  //** Associate the loopback id with the actual file

  afd = osd_open(dev, 0, OSD_READ_MODE);
  assert(afd != NULL);

  //** Read the Usage
  if (osd_read(dev, afd, 0, sizeof(usage), &usage) != sizeof(usage)) {
     printf("ERROR reading usage file! (%s)\n", afile);
     return(1);
  }

  printf("Status file: %s\n", afile);
  state = (usage.state == 0) ? "GOOD" : "BAD";
  printf("State: %s\n", state);
  printf("Used space[SOFT]: " I64T "\n", usage.used_space[ALLOC_SOFT]);
  printf("Used space[HARD]: " I64T "\n", usage.used_space[ALLOC_HARD]);
  printf("Total allocations: " I64T "\n", usage.n_allocs);
  printf("Total alias allocations: " I64T "\n", usage.n_alias);
  printf("\n");

  osd_close(dev, afd);
  osd_umount(dev);

  apr_terminate();

  return(usage.state);
}
