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

#define _log_module_index 137

//*****************************************************
// ibp_copyperf - Benchmarks IBP depot-to-depot copies, 
//*****************************************************

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <signal.h>
#include <apr_time.h>
#include "fmttypes.h"
#include "network.h"
#include "log.h"
#include "ibp.h"
#include "iovec_sync.h"
#include "io_wrapper.h"

int a_duration=900;    //** Default allocation duration

IBP_DptInfo depotinfo;
struct ibp_depot *src_depot_list;
struct ibp_depot *dest_depot_list;
int src_n_depots;
int dest_n_depots;
int ibp_timeout;
int sync_transfer;
int nthreads;
int ns_mode;
int print_progress;
ns_chksum_t *ncs;
int disk_cs_type = CHKSUM_DEFAULT;
ibp_off_t disk_blocksize = 0;

ibp_context_t *ic = NULL;


//*************************************************************************
//  create_alias_allocs - Creates a group of alias allocations in parallel
//   The alias allocations are based on the input allocations and round-robined 
//   among them
//*************************************************************************

ibp_capset_t *create_alias_allocs(int nallocs, ibp_capset_t *base_caps, int n_base)
{
  int i, err;
  opque_t *q;
  op_generic_t *op;
  ibp_capset_t *bcap;

  ibp_capset_t *caps = (ibp_capset_t *)malloc(sizeof(ibp_capset_t)*nallocs);

  q = new_opque();

  for (i=0; i<nallocs; i++) {
     bcap = &(base_caps[i % n_base]);
     op = new_ibp_alias_alloc_op(ic, &(caps[i]), get_ibp_cap(bcap, IBP_MANAGECAP), 0, 0, 0, ibp_timeout);
     opque_add(q, op);
  }

  io_start(q);
  err = io_waitall(q);  
  if (err != 0) {
     printf("create_alias_allocs: At least 1 error occured! * ibp_errno=%d * nfailed=%d\n", err, opque_tasks_failed(q)); 
  }    
  opque_free(q, OP_DESTROY);

  return(caps);
}

//*************************************************************************
// alias_remove_allocs - Remove a list of *ALIAS* allocations
//*************************************************************************

void alias_remove_allocs(ibp_capset_t *caps_list, ibp_capset_t *mcaps_list, int nallocs, int mallocs)
{
  int i, j, err;
  opque_t *q;
  op_generic_t *op;

  q = new_opque();

  for (i=0; i<nallocs; i++) {
     j = i % mallocs;
     op = new_ibp_alias_remove_op(ic, get_ibp_cap(&(caps_list[i]), IBP_MANAGECAP), 
              get_ibp_cap(&(mcaps_list[j]), IBP_MANAGECAP), ibp_timeout);
     opque_add(q, op);
  }
 
  io_start(q);
  err = io_waitall(q);  
  if (err != 0) {
     printf("alias_remove_allocs: At least 1 error occured! * ibp_errno=%d * nfailed=%d\n", err, opque_tasks_failed(q)); 
  }    
  opque_free(q, OP_DESTROY);

  //** Lastly free all the caps and the array
  for (i=0; i<nallocs; i++) {
     destroy_ibp_cap(get_ibp_cap(&(caps_list[i]), IBP_READCAP));   
     destroy_ibp_cap(get_ibp_cap(&(caps_list[i]), IBP_WRITECAP));   
     destroy_ibp_cap(get_ibp_cap(&(caps_list[i]), IBP_MANAGECAP));   
  }

  free(caps_list);

  return;
}

//*************************************************************************
//  create_allocs - Creates a group of allocations in parallel
//*************************************************************************

ibp_capset_t *create_allocs(int nallocs, int asize, int nthreads, ibp_depot_t *depot_list, int n_depots)
{
  int i, err;
  ibp_attributes_t attr;
  ibp_depot_t *depot;
  opque_t *q;
  op_generic_t *op;

  ibp_capset_t *caps = (ibp_capset_t *)malloc(sizeof(ibp_capset_t)*nallocs);

  set_ibp_attributes(&attr, time(NULL) + a_duration, IBP_HARD, IBP_BYTEARRAY);
  q = new_opque();

  for (i=0; i<nallocs; i++) {
     depot = &(depot_list[i % n_depots]);
     op = new_ibp_alloc_op(ic, &(caps[i]), asize, depot, &attr, disk_cs_type, disk_blocksize, ibp_timeout);
     opque_add(q, op);
  }
 
  io_start(q);
  err = io_waitall(q);  
  if (err != 0) {
     printf("create_allocs: At least 1 error occured! * ibp_errno=%d * nfailed=%d\n", err, opque_tasks_failed(q)); 
  }    
  opque_free(q, OP_DESTROY);

  return(caps);
}

//*************************************************************************
// remove_allocs - Remove a list of allocations
//*************************************************************************

void remove_allocs(ibp_capset_t *caps_list, int nallocs, int nthreads)
{
  int i, err;
  opque_t *q;
  op_generic_t *op;

  q = new_opque();

  for (i=0; i<nallocs; i++) {
     op = new_ibp_remove_op(ic, get_ibp_cap(&(caps_list[i]), IBP_MANAGECAP), ibp_timeout);
     opque_add(q, op);
  }
 
  io_start(q);
  err = io_waitall(q);  
  if (err != 0) {
     printf("remove_allocs: At least 1 error occured! * ibp_errno=%d * nfailed=%d\n", err, opque_tasks_failed(q)); 
  }    
  opque_free(q, OP_DESTROY);

  //** Lastly free all the caps and the array
  for (i=0; i<nallocs; i++) {
     destroy_ibp_cap(get_ibp_cap(&(caps_list[i]), IBP_READCAP));   
     destroy_ibp_cap(get_ibp_cap(&(caps_list[i]), IBP_WRITECAP));   
     destroy_ibp_cap(get_ibp_cap(&(caps_list[i]), IBP_MANAGECAP));   
  }

  free(caps_list);

  return;
}

//*************************************************************************
// write_allocs - Upload data to allocations
//*************************************************************************

void write_allocs(ibp_capset_t *caps, int n, int asize, int nthreads)
{
  int i, err;
  opque_t *q;
  op_generic_t *op;
  tbuffer_t buf[n];

  char *buffer = (char *)malloc(asize);
  memset(buffer, 'W', asize);

  q = new_opque();

  for (i=0; i<n; i++) {
     tbuffer_single(&(buf[i]), asize, buffer);
     op = new_ibp_write_op(ic, get_ibp_cap(&(caps[i]), IBP_WRITECAP), 0, &(buf[i]), 0, asize, ibp_timeout);
     opque_add(q, op);
  }

  io_start(q);
  err = io_waitall(q);  
  if (err != 0) {
     printf("write_allocs: At least 1 error occured! * ibp_errno=%d * nfailed=%d\n", err, opque_tasks_failed(q)); 
  }    
  opque_free(q, OP_DESTROY);

  free(buffer);
}

//*************************************************************************
// copy_allocs - Perform the depot-depot copy
//*************************************************************************

void copy_allocs(char *path, ibp_capset_t *src_caps, ibp_capset_t *dest_caps, int n_src, int n_dest, int asize, int nthreads)
{
  int i, j, err;
  opque_t *q;
  op_generic_t *op;

  char *buffer = (char *)malloc(asize);
  memset(buffer, 'W', asize);

  q = new_opque();

  for (i=0; i<n_dest; i++) {
     j = i % n_src;
     op = new_ibp_copyappend_op(ic, ns_mode, path, get_ibp_cap(&(src_caps[j]), IBP_READCAP), get_ibp_cap(&(dest_caps[i]), IBP_WRITECAP),
                  0, asize, ibp_timeout, ibp_timeout, ibp_timeout);
     opque_add(q, op);
  }

  io_start(q);
  err = io_waitall(q);  
  if (err != 0) {
     printf("copy_allocs: At least 1 error occured! * ibp_errno=%d * nfailed=%d\n", err, opque_tasks_failed(q)); 
  }    
  opque_free(q, OP_DESTROY);

  free(buffer);
}

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
  double r1, r2, dt;
  int i, start_option;
  char *path;
  ibp_capset_t *src_caps_list;
  ibp_capset_t *base_src_caps_list = NULL;
  ibp_capset_t *dest_caps_list;
  ibp_capset_t *base_dest_caps_list = NULL;
  int alias_source, alias_dest;
  rid_t rid;
  int port, cs_type;
  char buffer[1024];
  apr_time_t stime, dtime;
  chksum_t cs;
  ns_chksum_t ns_cs;
  int blocksize = -1;
  char *net_cs_name, *disk_cs_name;

  if (argc < 12) {
     printf("\n");
     printf("ibp_copyperf [-d|-dd] [-network_chksum type block_size] [-disk_chksum type block_size]\n");
     printf("          [-config ibp.cfg] [-phoebus phoebus_path] [-duration duration]\n");
     printf("          [-sync] [-alias-source] [-alias-dest] [-progress]\n");
     printf("          src_n_depots src_depot1 src_port1 src_resource_id1 ... src_depotN src_portN src_ridN\n");
     printf("          dest_n_depots dest_depot1 dest_port1 dest_resource_id1 ... dest_depotN dest_portN dest_ridN\n");
     printf("          nthreads ibp_timeout count size\n");
     printf("\n");
     printf("-d                  - Enable *minimal* debug output\n");
     printf("-dd                 - Enable *FULL* debug output\n");
     printf("-network_chksum type blocksize - Enable network checksumming for transfers.\n");
     printf("                      type should be SHA256, SHA512, SHA1, or MD5.\n");
     printf("                      blocksize determines how many bytes to send between checksums in kbytes.\n"); 
     printf("-disk_chksum type blocksize - Enable disk checksumming.\n");
     printf("                      type should be NONE, SHA256, SHA512, SHA1, or MD5.\n");
     printf("                      blocksize determines how many bytes to send between checksums in kbytes.\n"); 
     printf("-config ibp.cfg     - Use the IBP configuration defined in file ibp.cfg.\n");
     printf("                      nthreads overrides value in cfg file unless -1.\n");
     printf("-phoebus            - Make the transfer using the phoebus protocol.  Default is to use sockets\n");
     printf("   phoebus_list     - Specify the phoebus transfer path. Specify 'auto' for depot default\n");
     printf("                      Comma separated List of phoebus hosts/ports, eg gateway1/1234,gateway2/4321\n");
     printf("-duration duration  - Allocation duration in sec.  Needs to be big enough to last the entire\n");
     printf("                      run.  The default duration is %d sec.\n", a_duration);
     printf("-sync               - Use synchronous protocol.  Default uses async.\n");
     printf("-alias-source       - Use alias allocations for source allocations\n"); 
     printf("-alias-dest         - Use alias allocations for the destination allocations\n"); 
     printf("-progress           - Print completion progress.\n");
     printf("src_n_depots        - Number of *source* depot tuplets\n");
     printf("src_depot           - Source depot hostname\n");
     printf("src_port            - Source depot IBP port\n"); 
     printf("src_resource_id     - Resource ID to use on source depot\n");
     printf("dest_n_depots       - Number of *destination* depot tuplets\n");
     printf("dest_depot          - Destination depot hostname\n");
     printf("dest_port           - Destination depot IBP port\n"); 
     printf("dest_resource_id    - Resource ID to use on destination depot\n");
     printf("nthreads            - Max Number of simultaneous threads to use per depot.  Use -1 for defaults or value in ibp.cfg\n");
     printf("ibp_timeout         - Timeout(sec) for each IBP copmmand\n");
     printf("count               - Total Number of allocation on destination depots\n");
     printf("size                - Size of each allocation in KB on destination depot\n");
     printf("\n");

     return(-1);
  }

  ic = ibp_create_context();  //** Initialize IBP

  net_cs_name = NULL;
  disk_cs_name = NULL;
  ns_mode = NS_TYPE_SOCK;
  path = NULL;
  sync_transfer = 0;
  alias_source = 0;
  alias_dest = 0;
  print_progress = 0;

  i = 1;
  do {
     start_option = i;

     if (strcmp(argv[i], "-d") == 0) { //** Enable debugging
        set_log_level(5);
        i++;
     } else if (strcmp(argv[i], "-dd") == 0) { //** Enable debugging
        set_log_level(20);
        i++;
     } else if (strcmp(argv[i], "-network_chksum") == 0) { //** Add checksum capability
       i++;
        net_cs_name = argv[i];
        cs_type = chksum_name_type(net_cs_name);
        if (cs_type == -1) {
           printf("Invalid chksum type.  Got %s should be SHA1, SHA256, SHA512, or MD5\n", argv[i]);
           abort();
        }
        chksum_set(&cs, cs_type);
        i++;

        blocksize = atoi(argv[i])*1024; i++;
        ns_chksum_set(&ns_cs, &cs, blocksize);
        ncs = &ns_cs;
        ibp_set_chksum(ic, ncs);
     } else if (strcmp(argv[i], "-disk_chksum") == 0) { //** Add checksum capability
        i++;
        disk_cs_name = argv[i];
        disk_cs_type = chksum_name_type(argv[i]);
        if (disk_cs_type < CHKSUM_DEFAULT) {
           printf("Invalid chksum type.  Got %s should be NONE, SHA1, SHA256, SHA512, or MD5\n", argv[i]);
           abort();
        }

        disk_blocksize = atoi(argv[i])*1024; i++;
     } else if (strcmp(argv[i], "-config") == 0) { //** Read the config file
        i++;
        ibp_load_config(ic, argv[i], NULL);
        i++;
     } else if (strcmp(argv[i], "-phoebus") == 0) { //** Check if we want phoebus transfers
        ns_mode = NS_TYPE_PHOEBUS;
        i++;

        path = argv[i];
        i++;
     } else if (strcmp(argv[i], "-duration") == 0) { //** Check if we want sync tests
        i++;
        a_duration = atoi(argv[i]);
        i++;
     } else if (strcmp(argv[i], "-sync") == 0) { //** Check if we want sync tests
        sync_transfer = 1;
        i++;
     } else if (strcmp(argv[i], "-alias-source") == 0) { //** Check if we want alias allocs
        alias_source = 1;
        i++;
     } else if (strcmp(argv[i], "-alias-dest") == 0) { //** Check if we want alias allocs
        alias_dest = 1;
        i++;
     } else if (strcmp(argv[i], "-progress") == 0) { //** Check if we want to print the progress
        print_progress = 1;
        i++;
     }
  } while (start_option < i);

  //** Read in source depot list **
  src_n_depots = atoi(argv[i]);
  i++;
  src_depot_list = (ibp_depot_t *)malloc(sizeof(ibp_depot_t)*src_n_depots);
  int j;
  for (j=0; j<src_n_depots; j++) {
      port = atoi(argv[i+1]);
      rid = ibp_str2rid(argv[i+2]);
      set_ibp_depot(&(src_depot_list[j]), argv[i], port, rid);
      i = i + 3;
  }

  //** Read in destination depot list **
  dest_n_depots = atoi(argv[i]);
  i++;
  dest_depot_list = (ibp_depot_t *)malloc(sizeof(ibp_depot_t)*dest_n_depots);
  for (j=0; j<dest_n_depots; j++) {
      port = atoi(argv[i+1]);
      rid = ibp_str2rid(argv[i+2]);
      set_ibp_depot(&(dest_depot_list[j]), argv[i], port, rid);
      i = i + 3;
  }

  //*** Get thread count ***
  nthreads = atoi(argv[i]);
  if (nthreads <= 0) { 
     nthreads = ibp_get_max_depot_threads(ic);
  } else {
     ibp_set_max_depot_threads(ic, nthreads);
  }
  i++;

  ibp_timeout = atoi(argv[i]); i++;

   //****** Get the different Stream counts *****
  int count = atoi(argv[i]); i++;
  int size = atoi(argv[i])*1024; i++;

  //*** Print the ibp client version ***
  printf("\n");
  printf("================== IBP Client Version =================\n");
  printf("%s\n", ibp_client_version());

  //*** Print summary of options ***
  printf("\n");
  printf("======= Base options =======\n");
  printf("Source n_depots: %d\n", src_n_depots);
  for (i=0; i<src_n_depots; i++) {
     printf("depot %d: %s:%d rid:%s\n", i, src_depot_list[i].host, src_depot_list[i].port, ibp_rid2str(src_depot_list[i].rid, buffer));
  }
  printf("\n");
  printf("Destination n_depots: %d\n", dest_n_depots);
  for (i=0; i<dest_n_depots; i++) {
     printf("depot %d: %s:%d rid:%s\n", i, dest_depot_list[i].host, dest_depot_list[i].port, ibp_rid2str(dest_depot_list[i].rid, buffer));
  }
  printf("\n");
  printf("IBP timeout: %d\n", ibp_timeout);
  printf("Max Threads/depot: %d\n", nthreads);
  if (sync_transfer == 1) {
     printf("Transfer_mode: SYNC\n");
  } else {
     printf("Transfer_mode: ASYNC\n");
  }
  if (ns_mode == NS_TYPE_SOCK) {
     printf("Depot->Depot transfer type: NS_TYPE_SOCK\n");
  } else {
     printf("Depot->Depot transfer type: NS_TYPE_PHOEBUS\n");
     printf("Phoebus path: %s\n", path);
  }

  if (net_cs_name == NULL) {
     printf("Network Checksum Type: NONE\n");
  } else {
     printf("Network Checksum Type: %s   Block size: %dkb\n", net_cs_name, (blocksize/1024));     
  }
  if (net_cs_name == NULL) {
     printf("Disk Checksum Type: NONE\n");
  } else {
     printf("Disk Checksum Type: %s   Block size: " I64T "kb\n", disk_cs_name, (disk_blocksize/1024));     
  }
	
  printf("Alias source: %d\n", alias_source);
  printf("Alias destination: %d\n", alias_dest);
  printf("\n");
  printf("======= Bulk transfer options =======\n");
  printf("Count: %d\n", count);
  printf("Size: %dkb\n", size/1024);
  printf("\n");

  r1 =  1.0 * size/1024.0/1024.0;
  r1 = count * r1;
  printf("Approximate data for sequential tests: %lfMB\n", r1);
  printf("\n");

  io_set_mode(sync_transfer, print_progress, nthreads);

  //**************** Perform the tests ***************************
  i = count/nthreads;
  printf("Starting Bulk test (total files: %d, approx per thread: %d", count, i);
  r1 = 1.0*count*size/1024.0/1024.0;
  r2 = r1 / nthreads;
  printf(" -- total size: %lfMB, approx per thread: %lfMB\n", r1, r2);

  printf("Creating allocations...."); fflush(stdout);
  stime = apr_time_now();
  src_caps_list = create_allocs(src_n_depots, size, nthreads, src_depot_list, src_n_depots);
  if (alias_source == 1) {
     base_src_caps_list = src_caps_list;
     src_caps_list = create_alias_allocs(src_n_depots, base_src_caps_list, src_n_depots);
  }

  dest_caps_list = create_allocs(count, size, nthreads, dest_depot_list, dest_n_depots);
  if (alias_dest == 1) {
     base_dest_caps_list = dest_caps_list;
     dest_caps_list = create_alias_allocs(count, base_dest_caps_list, count);
  }
  dtime = apr_time_now() - stime;
  dt = dtime / (1.0 * APR_USEC_PER_SEC);
  r1 = 1.0*((1+alias_dest)*count + (1+alias_source)*src_n_depots)/dt;
  printf(" %lf creates/sec (%.2lf sec total) \n", r1, dt);


  printf("Uploading data to source depots...."); fflush(stdout);
  stime = apr_time_now();
  write_allocs(src_caps_list, src_n_depots, size, nthreads);
  dtime = apr_time_now() - stime;
  dt = dtime / (1.0 * APR_USEC_PER_SEC);
  r1 = 1.0*src_n_depots*size/(dt*1024*1024);
  printf(" %lf MB/sec (%.2lf sec total) \n", r1, dt);  fflush(stdout);

  printf("Depot-depot copy:"); fflush(stdout);
  stime = apr_time_now();
  copy_allocs(path, src_caps_list, dest_caps_list, src_n_depots, count, size, nthreads);
  dtime = apr_time_now() - stime;
  dt = dtime / (1.0 * APR_USEC_PER_SEC);
  r1 = 1.0*count*size/(dt*1024*1024);
  printf(" %lf MB/sec (%.2lf sec total) \n", r1, dt);  fflush(stdout);
  
  printf("Removing allocations...."); fflush(stdout);
  stime = apr_time_now();
  if (alias_source == 1) {
     alias_remove_allocs(src_caps_list, base_src_caps_list, src_n_depots, src_n_depots);
     src_caps_list = base_src_caps_list;
  }
  remove_allocs(src_caps_list, src_n_depots, nthreads);

  if (alias_dest == 1) {
     alias_remove_allocs(dest_caps_list, base_dest_caps_list, count, count);
     dest_caps_list = base_dest_caps_list;
  }
  remove_allocs(dest_caps_list, count, nthreads);
  dtime = apr_time_now() - stime;
  dt = dtime / (1.0 * APR_USEC_PER_SEC);
  r1 = 1.0*((1+alias_dest)*count + (1+alias_source)*src_n_depots)/dt;
  printf(" %lf removes/sec (%.2lf sec total) \n", r1, dt);
  printf("\n");

  printf("Final network connection counter: %d\n", network_counter(NULL));
     
  ibp_destroy_context(ic);  //** Shutdown IBP

  return(0);
}


