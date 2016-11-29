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
#include "allocation.h"
#include "ibp_ClientLib.h"
#include "ibp_server.h"
#include <tbx/log.h>
#include <tbx/fmttypes.h>
#include "subnet.h"
#include "ibp_time.h"
#include "print_alloc.h"
#include "osd_abstract.h"

#define HEADER_SIZE 4096
#define MAX_SIZE 100

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
  Allocation_t a;
  Allocation_history_t h;
  int changed, i, k, n;
  int volatile start_option;  //** This makes the compiler not generate an error due to optimizations
  char *afile;
  osd_fd_t *afd;
  osd_t *dev;

//printf("sizeof(Allocation_t)=" LU " ALLOC_HEADER=%d\n", sizeof(Allocation_t), ALLOC_HEADER);

  if (argc < 2) {
     printf("repair_history [-d debug_level] rid_file\n");
     printf("      -d debug_level  Sets the debug level.  Default is 0.\n");
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
        k = atoi(argv[i]); i++;
        tbx_set_log_level(k);
     }
  } while (start_option < i);

  afile = argv[i]; i++;


  //** Read the Allocation ***
  dev = osd_mount_fs("loopback", 10, 1000);  //** Mount the file via loopback
  fs_associate_id(dev, 0, afile);  //** Associate the loopback id with the actual file

  afd = osd_open(dev, 0, OSD_READ_MODE);
  assert(afd != NULL);

  //** Read the header and history
  osd_read(dev, afd, 0, sizeof(a), &a);
  osd_read(dev, afd, sizeof(a), sizeof(h), &h);

  osd_close(dev, afd);

  //** Print the current slots
  printf("Current slot values\n");
  n = h.manage_slot; printf("   manage_slot: %hd\n", n);
  n = h.read_slot;   printf("   read_slot: %hd\n", n);
  n = h.write_slot;  printf("   write_slot: %hd\n", n);

  changed = 0;
  if ((h.manage_slot < 0) || (h.manage_slot >= ALLOC_HISTORY)) { changed=1; h.manage_slot = 0; }
  if ((h.read_slot < 0) || (h.read_slot >= ALLOC_HISTORY)) { changed=1; h.read_slot = 0; }
  if ((h.write_slot < 0) || (h.write_slot >= ALLOC_HISTORY)) { changed=1; h.write_slot = 0; }

  if (changed == 1) {
     printf("Updating history\n");
     afd = osd_open(dev, 0, OSD_WRITE_MODE);
     osd_write(dev, afd, sizeof(a), sizeof(h), &h);
     osd_close(dev, afd);
  } else {
     printf("No changes required\n");
  }

  osd_umount(dev);

  apr_terminate();

  printf("\n");

  return(0);
}
