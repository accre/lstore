/*
Advanced Computing Center for Research and Education Proprietary License
Version 1.0 (April 2006)

Copyright (c) 2006, Advanced Computing Center for Research and Education,
 Vanderbilt University, All rights reserved.

This Work is the sole and exclusive property of the Advanced Computing Center
for Research and Education department at Vanderbilt University.  No right to
disclose or otherwise disseminate any of the information contained herein is
granted by virtue of your possession of this software except in accordance with
the terms and conditions of a separate License Agreement entered into with
Vanderbilt University.

THE AUTHOR OR COPYRIGHT HOLDERS PROVIDES THE "WORK" ON AN "AS IS" BASIS,
WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, TITLE, FITNESS FOR A PARTICULAR
PURPOSE, AND NON-INFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Vanderbilt University
Advanced Computing Center for Research and Education
230 Appleton Place
Nashville, TN 37203
http://www.accre.vanderbilt.edu
*/ 

#define _log_module_index 108

#include <stdio.h>
#include <hwloc.h>
#include "hwinfo.h"

//***************************************************************************
// procinfo - Returns the socket count, physical and virtual cores
//***************************************************************************

int proc_info(int *socket, int *core_phys, int *core_virt)
{
  int depth, err;
  hwloc_topology_t topology;

  //** Init the tolopogy object
  hwloc_topology_init(&topology);
  hwloc_topology_load(topology);

  err = 0;

  depth = hwloc_get_type_depth(topology, HWLOC_OBJ_SOCKET);
  if (depth == HWLOC_TYPE_DEPTH_UNKNOWN) {
     *socket = -1;
     err = 1;
  } else {
     *socket = hwloc_get_nbobjs_by_depth(topology, depth);
  }

  depth = hwloc_get_type_depth(topology, HWLOC_OBJ_CORE);
  if (depth == HWLOC_TYPE_DEPTH_UNKNOWN) {
     *core_phys = -1;
     err = 1;
  } else {
     *core_phys = hwloc_get_nbobjs_by_depth(topology, depth);
  }

  depth = hwloc_get_type_depth(topology, HWLOC_OBJ_PU);
  if (depth == HWLOC_TYPE_DEPTH_UNKNOWN) {
     *core_virt = -1;
     err = 1;
  } else {
     *core_virt = hwloc_get_nbobjs_by_depth(topology, depth);
  }

  return(err);
}

//***************************************************************************
// convert2bytes - Returns the scale factor to convert the unit to bytes
//***************************************************************************

long int convert2bytes(char *unit)
{
  if (strcasecmp("kb", unit) == 0) {
     return(1024);
  } else if (strcasecmp("mb", unit) == 0) {
     return(1024*1024);
  } else if (strcasecmp("gb", unit) == 0) {
     return(1024*1024*1024);
  } else if (strcasecmp("b", unit) == 0) {
     return(1);
  }

  return(0);
}

//***************************************************************************
// mem_info - Returns the total, used and free, mem
//***************************************************************************

void mem_info(long int *total, long int *used, long int *mfree)
{
  FILE *fd;
  char line[1024];
  char field[1024], unit[1024];
  long int val;

  *total = 0;
  *used = 0;
  *mfree = 0;

  fd = fopen("/proc/meminfo", "r");
  while (fgets(line, sizeof(line), fd) != NULL) {
     sscanf(line, "%s %ld %s\n", field, &val, unit);
//printf("mem_info: line=%s", line);
//printf("mem_info: f=%s v=%ld u=%s\n", field, val, unit); 

     if (strcasecmp("memtotal:", field) == 0) {
        *total = val * convert2bytes(unit);
     } else if (strcasecmp("memfree:", field) == 0) {
        *mfree = *mfree + val * convert2bytes(unit);
     } else if (strcasecmp("buffers:", field) == 0) {
        *mfree = *mfree + val * convert2bytes(unit);
     } else if (strcasecmp("cached:", field) == 0) {
        *mfree = *mfree + val * convert2bytes(unit);
     }
  }
  
  fclose(fd);

  *used = *total - *mfree;

  return;
}
