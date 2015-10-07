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

//*****************************************************
// ibp_perf - Benchmarks IBP depot creates, removes,
//      reads, and writes.  The read and write tests
//      use sync an async iovec style operations.
//*****************************************************

#define _log_module_index 139

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <signal.h>
#include <apr_time.h>
#include "network.h"
#include "fmttypes.h"
#include "network.h"
#include "log.h"
#include "ibp.h"
#include "iovec_sync.h"
#include "io_wrapper.h"
#include "type_malloc.h"

int a_duration=900;   //** Default duration

IBP_DptInfo depotinfo;
struct ibp_depot *depot_list;
int n_depots;
int ibp_timeout;
int sync_transfer;
int nthreads;
int use_alias;
int report_interval = 0;
int do_validate = 0;
int identical_buffers = 1;
int print_progress;
ibp_connect_context_t *cc = NULL;
ns_chksum_t *ncs;
int disk_cs_type = CHKSUM_DEFAULT;
ibp_off_t disk_blocksize = 0;

ibp_context_t *ic = NULL;

//*************************************************************************
//  init_buffer - Initializes a buffer.  This routine was added to
//     get around throwing network chksum errors by making all buffers
//     identical.  The char "c" may or may not be used.
//*************************************************************************

void init_buffer(char *buffer, char c, int size)
{
  if (identical_buffers == 1) {
     memset(buffer, 'A', size);
     return;
  }

  memset(buffer, c, size);
}

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

ibp_capset_t *create_allocs(int nallocs, int asize)
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
     abort();
  }
  opque_free(q, OP_DESTROY);

  return(caps);
}

//*************************************************************************
// remove_allocs - Remove a list of allocations
//*************************************************************************

void remove_allocs(ibp_capset_t *caps_list, int nallocs)
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
// save_allocs - Stores the allocations to the provided fd
//*************************************************************************

void save_allocs(FILE *fd, ibp_capset_t *caps_list, int nallocs)
{
  int i;

  //** Print the ds_read compatible portion of the file
  fprintf(fd, "%d\n", nallocs);
  for (i=0; i<nallocs; i++) {
     fprintf(fd,"%s\n", get_ibp_cap(&(caps_list[i]), IBP_READCAP));
  }

  //** Now print the full caps
  fprintf(fd, "=========FULL CAPS FOLLOW===========\n");
  for (i=0; i<nallocs; i++) {
     fprintf(fd,"%s\n", get_ibp_cap(&(caps_list[i]), IBP_READCAP));
     fprintf(fd,"%s\n", get_ibp_cap(&(caps_list[i]), IBP_WRITECAP));
     fprintf(fd,"%s\n", get_ibp_cap(&(caps_list[i]), IBP_MANAGECAP));
  }

  return;
}

//*************************************************************************
// validate_allocs - Validates a list of allocations
//*************************************************************************

void validate_allocs(ibp_capset_t *caps_list, int nallocs)
{
  int i, err;
  int nalloc_bad, nblocks_bad;
  opque_t *q;
  op_generic_t *op;
  int *bad_blocks = (int *) malloc(sizeof(int)*nallocs);
  int correct_errors = 0;

  q = new_opque();

  for (i=0; i<nallocs; i++) {
     bad_blocks[i] = 0;
     op = new_ibp_validate_chksum_op(ic, get_ibp_cap(&(caps_list[i]), IBP_MANAGECAP), correct_errors, &(bad_blocks[i]),
       ibp_timeout);
     opque_add(q, op);
  }

  io_start(q);
  err = io_waitall(q);
  if (err != 0) {
     printf("validate_allocs: At least 1 error occured! * ibp_errno=%d * nfailed=%d\n", err, opque_tasks_failed(q));
     nalloc_bad = 0; nblocks_bad = 0;
     for (i=0; i<nallocs; i++) {
       if (bad_blocks[i] != 0) {
          printf("  %d   cap=%s  blocks_bad=%d\n", i, get_ibp_cap(&(caps_list[i]), IBP_MANAGECAP), bad_blocks[i]);
          nalloc_bad++;  nblocks_bad = nblocks_bad + bad_blocks[i];
       }
     }

     printf("  Total Bad allocations: %d   Total Bad blocks: %d\n", nalloc_bad, nblocks_bad);
  }
  opque_free(q, OP_DESTROY);

  free(bad_blocks);

  return;
}

//*************************************************************************
// write_allocs - Upload data to allocations
//*************************************************************************

void write_allocs(ibp_capset_t *caps, int n, int asize, int block_size)
{
  int i, j, nblocks, rem, len, err, slot;
  opque_t *q;
  op_generic_t *op;
  tbuffer_t *buf;

  char *buffer = (char *)malloc(block_size);
  init_buffer(buffer, 'W', block_size);

  q = new_opque();

  nblocks = asize / block_size;
  rem = asize % block_size;
  if (rem > 0) nblocks++;

  type_malloc_clear(buf, tbuffer_t, n*nblocks);

//for (j=0; j<nblocks; j++) {
  for (j=nblocks-1; j>= 0; j--) {
     for (i=0; i<n; i++) {
         if ((j==(nblocks-1)) && (rem > 0)) { len = rem; } else { len = block_size; }
         slot = j*n + i;
         tbuffer_single(&(buf[slot]), len, buffer);
         op = new_ibp_write_op(ic, get_ibp_cap(&(caps[i]), IBP_WRITECAP), j*block_size, &(buf[slot]), 0, len, ibp_timeout);
         opque_add(q, op);
     }
  }

  io_start(q);
  err = io_waitall(q);
  if (err != 0) {
     printf("write_allocs: At least 1 error occured! * ibp_errno=%d * nfailed=%d\n", err, opque_tasks_failed(q));
  }
  opque_free(q, OP_DESTROY);

  free(buf);
  free(buffer);
}

//*************************************************************************
// read_allocs - Downlaod data from allocations
//*************************************************************************

void read_allocs(ibp_capset_t *caps, int n, int asize, int block_size)
{
  int i, j, nblocks, rem, len, err, slot;
  opque_t *q;
  op_generic_t *op;
  tbuffer_t *buf;

  char *buffer = (char *)malloc(block_size);

  q = new_opque();

  nblocks = asize / block_size;
  rem = asize % block_size;
  if (rem > 0) nblocks++;

  type_malloc_clear(buf, tbuffer_t, n*nblocks);

//  for (j=0; j<nblocks; j++) {
  for (j=nblocks-1; j>= 0; j--) {
     for (i=0; i<n; i++) {
         if ((j==(nblocks-1)) && (rem > 0)) { len = rem; } else { len = block_size; }
         slot = j*n + i;
         tbuffer_single(&(buf[slot]), len, buffer);
         op = new_ibp_read_op(ic, get_ibp_cap(&(caps[i]), IBP_READCAP), j*block_size, &(buf[slot]), 0, len, ibp_timeout);
         opque_add(q, op);
     }
  }

  io_start(q);
  err = io_waitall(q);
  if (err != 0) {
     printf("read_allocs: At least 1 error occured! * ibp_errno=%d * nfailed=%d\n", err, opque_tasks_failed(q)); 
  }
  opque_free(q, OP_DESTROY);

  free(buf);
  free(buffer);
}

//*************************************************************************
// random_allocs - Perform random R/W on allocations
//*************************************************************************

void random_allocs(ibp_capset_t *caps, int n, int asize, int block_size, double rfrac)
{
  int i, err, bslot;
  int j, nblocks, rem, len;
  opque_t *q;
  op_generic_t *op;
  double rnd;
  tbuffer_t *buf;

  char *rbuffer = (char *)malloc(block_size);
  char *wbuffer = (char *)malloc(block_size);
  init_buffer(rbuffer, 'r', block_size);
  init_buffer(wbuffer, 'w', block_size);

  q = new_opque();

  nblocks = asize / block_size;
  rem = asize % block_size;
  if (rem > 0) nblocks++;

  type_malloc_clear(buf, tbuffer_t, n*nblocks);

  for (j=0; j<nblocks; j++) {
     for (i=0; i<n; i++) {
         rnd = rand()/(RAND_MAX + 1.0);

         if ((j==(nblocks-1)) && (rem > 0)) { len = rem; } else { len = block_size; }

         bslot = j*n + i;

         if (rnd < rfrac) {
            tbuffer_single(&(buf[bslot]), len, rbuffer);
            op = new_ibp_read_op(ic, get_ibp_cap(&(caps[i]), IBP_READCAP), j*block_size, &(buf[bslot]), 0, len, ibp_timeout);
         } else {
            tbuffer_single(&(buf[bslot]), len, wbuffer);
            op = new_ibp_write_op(ic, get_ibp_cap(&(caps[i]), IBP_WRITECAP), j*block_size, &(buf[bslot]), 0, len, ibp_timeout);
         }
         opque_add(q, op);
     }
  }

  io_start(q);
  err = io_waitall(q);
  if (err != 0) {
     printf("random_allocs: At least 1 error occured! * ibp_errno=%d * nfailed=%d\n", err, opque_tasks_failed(q));
  }
  opque_free(q, OP_DESTROY);

  free(buf);
  free(rbuffer);
  free(wbuffer);
}

//*************************************************************************
// small_write_allocs - Performs small write I/O on the bulk allocations
//*************************************************************************

double small_write_allocs(ibp_capset_t *caps, int n, int asize, int small_count, int min_size, int max_size)
{
  int i, io_size, offset, slot, err;
  opque_t *q;
  op_generic_t *op;
  double rnd, lmin, lmax;
  double nbytes;
  tbuffer_t *buf;

  q = new_opque();

  if (asize < max_size) {
     max_size = asize;
     log_printf(0, "small_write_allocs:  Adjusting max_size=%d\n", max_size);
  }

  lmin = log(min_size);  lmax = log(max_size);

  char *buffer = (char *)malloc(max_size);
  init_buffer(buffer, 'a', max_size);

  type_malloc_clear(buf, tbuffer_t, small_count);

  nbytes = 0;
  for (i=0; i<small_count; i++) {
     rnd = rand()/(RAND_MAX+1.0);
     slot = n * rnd;

     rnd = rand()/(RAND_MAX+1.0);
     rnd = lmin + (lmax - lmin) * rnd;
     io_size = exp(rnd);
     if (io_size == 0) io_size = 1;
     nbytes = nbytes + io_size;

     rnd = rand()/(RAND_MAX+1.0);
     offset = (asize - io_size) * rnd;

     tbuffer_single(&(buf[i]), io_size, buffer);
     op = new_ibp_write_op(ic, get_ibp_cap(&(caps[slot]), IBP_WRITECAP), offset, &(buf[i]), 0, io_size, ibp_timeout);
     opque_add(q, op);
  }

  io_start(q);
  err = io_waitall(q);
  if (err != 0) {
     printf("small_write_allocs: At least 1 error occured! * ibp_errno=%d * nfailed=%d\n", err, opque_tasks_failed(q));
  }
  opque_free(q, OP_DESTROY);

  free(buf);
  free(buffer);

  return(nbytes);
}

//*************************************************************************
// small_read_allocs - Performs small read I/O on the bulk allocations
//*************************************************************************

double small_read_allocs(ibp_capset_t *caps, int n, int asize, int small_count, int min_size, int max_size)
{
  int i, io_size, offset, slot, err;
  opque_t *q;
  op_generic_t *op;
  double rnd, lmin, lmax;
  double nbytes;
  tbuffer_t *buf;

  q = new_opque();

  lmin = log(min_size);  lmax = log(max_size);

  if (asize < max_size) {
     max_size = asize;
     log_printf(0, "small_read_allocs:  Adjusting max_size=%d\n", max_size);
  }

  char *buffer = (char *)malloc(max_size);
  init_buffer(buffer, 'r', max_size);

  type_malloc_clear(buf, tbuffer_t, small_count);

  nbytes = 0;
  for (i=0; i<small_count; i++) {
     rnd = rand()/(RAND_MAX+1.0);
     slot = n * rnd;

     rnd = rand()/(RAND_MAX+1.0);
     rnd = lmin + (lmax - lmin) * rnd;
     io_size = exp(rnd);
     if (io_size == 0) io_size = 1;
     nbytes = nbytes + io_size;

     rnd = rand()/(RAND_MAX+1.0);
     offset = (asize - io_size) * rnd;

     tbuffer_single(&(buf[i]), io_size, buffer);

     op = new_ibp_read_op(ic, get_ibp_cap(&(caps[slot]), IBP_READCAP), offset, &(buf[i]), 0, io_size, ibp_timeout);
     opque_add(q, op);
  }

  io_start(q);
  err = io_waitall(q);
  if (err != 0) {
     printf("small_read_allocs: At least 1 error occured! * ibp_errno=%d * nfailed=%d\n", err, opque_tasks_failed(q));
  }
  opque_free(q, OP_DESTROY);

  free(buf);
  free(buffer);

  return(nbytes);
}

//*************************************************************************
// small_random_allocs - Performs small random I/O on the bulk allocations
//*************************************************************************

double small_random_allocs(ibp_capset_t *caps, int n, int asize, double readfrac, int small_count, int min_size, int max_size)
{
  int i, io_size, offset, slot, err;
  opque_t *q;
  op_generic_t *op;
  double rnd, lmin, lmax;
  double nbytes;
  tbuffer_t *buf;

  q = new_opque();

  lmin = log(min_size);  lmax = log(max_size);

  if (asize < max_size) {
     max_size = asize;
     log_printf(0, "small_random_allocs:  Adjusting max_size=%d\n", max_size);
  }

  char *rbuffer = (char *)malloc(max_size);
  char *wbuffer = (char *)malloc(max_size);
  init_buffer(rbuffer, '1', max_size);
  init_buffer(wbuffer, '2', max_size);

  type_malloc_clear(buf, tbuffer_t, small_count);

  nbytes = 0;
  for (i=0; i<small_count; i++) {
     rnd = rand()/(RAND_MAX+1.0);
     slot = n * rnd;

     rnd = rand()/(RAND_MAX+1.0);
     rnd = lmin + (lmax - lmin) * rnd;
     io_size = exp(rnd);
     if (io_size == 0) io_size = 1;
     nbytes = nbytes + io_size;

     rnd = rand()/(RAND_MAX+1.0);
     offset = (asize - io_size) * rnd;

//     log_printf(15, "small_random_allocs: slot=%d offset=%d size=%d\n", slot, offset, io_size);

     rnd = rand()/(RAND_MAX+1.0);
     if (rnd < readfrac) {
        tbuffer_single(&(buf[i]), io_size, rbuffer);
        op = new_ibp_read_op(ic, get_ibp_cap(&(caps[slot]), IBP_READCAP), offset, &(buf[i]), 0, io_size, ibp_timeout);
     } else {
        tbuffer_single(&(buf[i]), io_size, wbuffer);
        op = new_ibp_write_op(ic, get_ibp_cap(&(caps[slot]), IBP_WRITECAP), offset, &(buf[i]), 0, io_size, ibp_timeout);
     }

     opque_add(q, op);
  }

  io_start(q);
  err = io_waitall(q);
  if (err != 0) {
     printf("small_random_allocs: At least 1 error occured! * ibp_errno=%d * nfailed=%d\n", err, opque_tasks_failed(q));
  }
  opque_free(q, OP_DESTROY);

  free(buf);
  free(rbuffer);
  free(wbuffer);

  return(nbytes);
}

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
  double r1, r2, r3;
  int i, start_option, tcpsize, cs_type;
  ibp_capset_t *caps_list, *base_caps;
  rid_t rid;
  int port, fd_special;
  char buffer[1024];
  apr_time_t stime, dtime;
  double dt;
  char *ppath, *net_cs_name, *disk_cs_name, *out_fname;
  FILE *fd_out;
  phoebus_t pcc;
  char pstr[2048];
  chksum_t cs;
  ns_chksum_t ns_cs;
  int blocksize;

  base_caps = NULL;

  if (argc < 12) {
     printf("\n");
     printf("ibp_perf [-d|-dd] [-network_chksum type blocksize] [-disk_chksum type blocksize]\n");
     printf("         [-validate] [-config ibp.cfg] [-phoebus gateway_list] [-tcpsize tcpbufsize]\n");
     printf("         [-duration duration] [-sync] [-alias] [-progress] [-random]\n");
     printf("         n_depots depot1 port1 resource_id1 ... depotN portN ridN\n");
     printf("         nthreads ibp_timeout\n");
     printf("         alias_createremove_count createremove_count\n");
     printf("         readwrite_count readwrite_alloc_size rw_block_size read_mix_fraction\n");
     printf("         smallio_count small_min_size small_max_size small_read_fraction\n");
     printf("\n");
     printf("-d                  - Enable *minimal* debug output\n");
     printf("-dd                 - Enable *FULL* debug output\n");
     printf("-network_chksum type blocksize - Enable network checksumming for transfers.\n");
     printf("                      type should be SHA256, SHA512, SHA1, or MD5.\n");
     printf("                      blocksize determines how many bytes to send between checksums in kbytes.\n");
     printf("-disk_chksum type blocksize - Enable Disk checksumming.\n");
     printf("                      type should be NONE, SHA256, SHA512, SHA1, or MD5.\n");
     printf("                      blocksize determines how many bytes to send between checksums in kbytes.\n");
     printf("-validate           - Validate disk chksum data.  Option is ignored unless disk chksumming is enabled.\n");
     printf("-config ibp.cfg     - Use the IBP configuration defined in file ibp.cfg.\n");
     printf("                      nthreads overrides value in cfg file unless -1.\n");
     printf("-phoebus            - Use Phoebus protocol for data transfers.\n");
     printf("   gateway_list     - Comma separated List of phoebus hosts/ports, eg gateway1/1234,gateway2/4321\n");
     printf("-tcpsize tcpbufsize - Use this value, in KB, for the TCP send/recv buffer sizes\n");
     printf("-duration duration  - Allocation duration in sec.  Needs to be big enough to last the entire\n");
     printf("                      run.  The default duration is %d sec.\n", a_duration);
     printf("-save fname         - Don't delete the R/W allocations on completion.  Instead save them to the given filename.\n");
     printf("                      Can use 'stdout' and 'stderr' as the filename to redirect output\n");
     printf("-sync               - Use synchronous protocol.  Default uses async.\n");
     printf("-alias              - Use alias allocations for all I/O operations\n");
     printf("-progress           - Print completion progress.\n");
     printf("-random             - Initializes the transfer buffers with quasi-random data.\n");
     printf("                      Disabled if network chksums are enabled.\n");
     printf("n_depots            - Number of depot tuplets\n");
     printf("depot               - Depot hostname\n");
     printf("port                - IBP port on depot\n");
     printf("resource_id         - Resource ID to use on depot\n");
     printf("nthreads            - Max Number of simultaneous threads to use.  Use -1 for defaults or value in ibp.cfg\n");
     printf("ibp_timeout         - Timeout(sec) for each IBP copmmand\n");
     printf("alias_createremove_count* - Number of 0 byte files to create and remove using alias allocations\n");
     printf("createremove_count* - Number of 0 byte files to create and remove to test metadata performance\n");
     printf("readwrite_count*    - Number of files to write sequentially then read sequentially\n");
     printf("readwrite_alloc_size  - Size of each allocation in KB for sequential and random tests\n");
     printf("rw_block_size       - Size of each R/W operation in KB for sequential and random tests\n");
     printf("read_mix_fraction   - Fraction of Random I/O operations that are READS\n");
     printf("smallio_count*      - Number of random small I/O operations\n");
     printf("small_min_size      - Minimum size of each small I/O operation(kb)\n");
     printf("small_max_size      - Max size of each small I/O operation(kb)\n");
     printf("small_read_fraction - Fraction of small random I/O operations that are READS\n");
     printf("\n");
     printf("*If the variable is set to 0 then the test is skipped\n");
     printf("\n");

     return(-1);
  }

  set_log_level(-1);

  ic = ibp_create_context();  //** Initialize IBP

  i = 1;
  net_cs_name = NULL;
  disk_cs_name = NULL;
  sync_transfer = 0;
  use_alias = 0;
  print_progress = 0;
  fd_special = 0;
  fd_out = NULL;
  out_fname = NULL;
  do {
     start_option = i;

     if (strcmp(argv[i], "-d") == 0) { //** Enable debugging
        set_log_level(5);
        i++;
     } else if (strcmp(argv[i], "-dd") == 0) { //** Enable debugging
        set_log_level(20);
        i++;
     } else if (strcmp(argv[i], "-random") == 0) { //** Random buffers
        i++;
        identical_buffers = 0;
     } else if (strcmp(argv[i], "-network_chksum") == 0) { //** Add checksum capability
        i++;
        net_cs_name = argv[i];
        cs_type = chksum_name_type(argv[i]);
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
        i++;

        disk_blocksize = atoi(argv[i])*1024; i++;
     } else if (strcmp(argv[i], "-validate") == 0) { //** Enable validation
        i++;
        do_validate=1;
     } else if (strcmp(argv[i], "-config") == 0) { //** Read the config file
        i++;
        ibp_load_config_file(ic, argv[i], NULL);
        i++;
     } else if (strcmp(argv[i], "-save") == 0) { //** Save the allocations and don't delete them
        i++;
        out_fname = argv[i]; i++;
        if (strcmp(out_fname, "stderr") == 0) {
           fd_out = stderr;
           fd_special = 1;
        } else if (strcmp(out_fname, "stdout") == 0) {
           fd_out = stdout;
           fd_special = 1;
        } else {
           assert((fd_out = fopen(out_fname, "w")) != NULL);
        }
     } else if (strcmp(argv[i], "-phoebus") == 0) { //** Check if we want Phoebus transfers
        cc = (ibp_connect_context_t *)malloc(sizeof(ibp_connect_context_t));
        cc->type = NS_TYPE_PHOEBUS;
        i++;

        ppath = argv[i];
        phoebus_path_set(&pcc, ppath);
//   printf("ppath=%s\n", ppath);
        cc->data = &pcc;

        ibp_set_read_cc(ic, cc);
        ibp_set_write_cc(ic, cc);

        i++;
     } else if (strcmp(argv[i], "-tcpsize") == 0) { //** Check if we want sync tests
        i++;
        tcpsize = atoi(argv[i]) * 1024;
        ibp_set_tcpsize(ic, tcpsize);
        i++;
     } else if (strcmp(argv[i], "-duration") == 0) { //** Check if we want sync tests
        i++;
        a_duration = atoi(argv[i]);
        i++;
     } else if (strcmp(argv[i], "-sync") == 0) { //** Check if we want sync tests
        sync_transfer = 1;
        i++;
     } else if (strcmp(argv[i], "-alias") == 0) { //** Check if we want to use alias allocation
        use_alias = 1;
        i++;
     } else if (strcmp(argv[i], "-progress") == 0) { //** Check if we want to print the progress
        print_progress = 1;
        i++;
     }
  } while (start_option < i);

  if (net_cs_name != NULL) identical_buffers = 1;

  n_depots = atoi(argv[i]);
  i++;

  depot_list = (ibp_depot_t *)malloc(sizeof(ibp_depot_t)*n_depots);
  int j;
  for (j=0; j<n_depots; j++) {
      port = atoi(argv[i+1]);
      rid = ibp_str2rid(argv[i+2]);
      set_ibp_depot(&(depot_list[j]), argv[i], port, rid);
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
  int aliascreateremove_count = atol(argv[i]); i++;
  int createremove_count = atol(argv[i]); i++;
  int readwrite_count = atol(argv[i]); i++;
  int readwrite_size = atol(argv[i])*1024; i++;
  int rw_block_size = atol(argv[i])*1024; i++;
  double read_mix_fraction = atof(argv[i]); i++;

   //****** Get the different small I/O counts *****
  int smallio_count = atol(argv[i]); i++;
  int small_min_size = atol(argv[i])*1024; i++;
  int small_max_size = atol(argv[i])*1024; i++;
  double small_read_fraction = atof(argv[i]); i++;

  //*** Print the ibp client version ***
  printf("\n");
  printf("================== IBP Client Version =================\n");
  printf("%s\n", ibp_client_version());

  //*** Print summary of options ***
  printf("\n");
  printf("======= Base options =======\n");
  printf("n_depots: %d\n", n_depots);
  for (i=0; i<n_depots; i++) {
     printf("depot %d: %s:%d rid:%s\n", i, depot_list[i].host, depot_list[i].port, ibp_rid2str(depot_list[i].rid, buffer));
  }
  printf("\n");
  printf("IBP timeout: %d\n", ibp_timeout);
  printf("IBP duration: %d\n", a_duration);
  printf("Max Threads: %d\n", nthreads);
 if (sync_transfer == 1) {
     printf("Transfer_mode: SYNC\n");
  } else {
     printf("Transfer_mode: ASYNC\n");
  }
  printf("Use alias: %d\n", use_alias);

  if (cc != NULL) {
     switch (cc->type) {
       case NS_TYPE_SOCK:
          printf("Connection Type: SOCKET\n"); break;
       case NS_TYPE_PHOEBUS:
          phoebus_path_to_string(pstr, sizeof(pstr), &pcc);
          printf("Connection Type: PHOEBUS (%s)\n", pstr); break;
       case NS_TYPE_1_SSL:
          printf("Connection Type: Single SSL\n"); break;
       case NS_TYPE_2_SSL:
          printf("Connection Type: Dual SSL\n"); break;
     }
  } else {
    printf("Connection Type: SOCKET\n");
  }

  if (identical_buffers == 1) {
     printf("Identical buffers being used.\n");
  } else {
     printf("Quasi-random buffers being used.\n");
  }

  if (net_cs_name == NULL) {
     printf("Network Checksum Type: NONE\n");
  } else {
     printf("Network Checksum Type: %s   Block size: %dkb\n", net_cs_name, (blocksize/1024));
  }
  if (disk_cs_name == NULL) {
     printf("Disk Checksum Type: NONE\n");
  } else {
     printf("Disk Checksum Type: %s   Block size: " I64T "kb\n", disk_cs_name, (disk_blocksize/1024));
     if (do_validate == 1) {
        printf("Disk Validation: Enabled\n");
     } else {
        printf("Disk Validation: Disabled\n");
     }
  }

  if (fd_out != NULL) {
     printf("Saving allocations to %s\n", out_fname);
  }

  printf("TCP buffer size: %dkb (0 defaults to OS)\n", ibp_get_tcpsize(ic)/1024);
  printf("\n");

  printf("======= Bulk transfer options =======\n");
  printf("aliascreateremove_count: %d\n", aliascreateremove_count);
  printf("createremove_count: %d\n", createremove_count);
  printf("readwrite_count: %d\n", readwrite_count);
  printf("readwrite_alloc_size: %dkb\n", readwrite_size/1024);
  printf("rw_block_size: %dkb\n", rw_block_size/1024);
  printf("read_mix_fraction: %lf\n", read_mix_fraction);
  printf("\n");
  printf("======= Small Random I/O transfer options =======\n");
  printf("smallio_count: %d\n", smallio_count);
  printf("small_min_size: %dkb\n", small_min_size/1024);
  printf("small_max_size: %dkb\n", small_max_size/1024);
  printf("small_read_fraction: %lf\n", small_read_fraction);
  printf("\n");

  r1 =  1.0 * readwrite_size/1024.0/1024.0;
  r1 = readwrite_count * r1;
  printf("Approximate I/O for sequential tests: %lfMB\n", r1);
  printf("\n");

  io_set_mode(sync_transfer, print_progress, nthreads);

  //**************** Create/Remove tests ***************************
  if (aliascreateremove_count > 0) {
     i = aliascreateremove_count/nthreads;
     printf("Starting Alias create test (total files: %d, approx per thread: %d)\n",aliascreateremove_count, i);
     base_caps = create_allocs(1, 1);
     stime = apr_time_now();
     caps_list = create_alias_allocs(aliascreateremove_count, base_caps, 1);
     dtime = apr_time_now() - stime;
     dt = dtime / (1.0 * APR_USEC_PER_SEC);
     r1 = 1.0*aliascreateremove_count/dt;
     printf("Alias create : %lf creates/sec (%.2lf sec total) \n", r1, dt);

     stime = apr_time_now();
     alias_remove_allocs(caps_list, base_caps, aliascreateremove_count, 1);
     dtime = apr_time_now() - stime;
     dt = dtime / (1.0 * APR_USEC_PER_SEC);
     r1 = 1.0*aliascreateremove_count/dt;
     printf("Alias remove : %lf removes/sec (%.2lf sec total) \n", r1, dt);
     printf("\n");

printf("-----------------------------\n"); fflush(stdout);

     remove_allocs(base_caps, 1);

     printf("\n");
  }

  if (createremove_count > 0) {
     i = createremove_count/nthreads;
     printf("Starting Create test (total files: %d, approx per thread: %d)\n",createremove_count, i);

     stime = apr_time_now();
     caps_list = create_allocs(createremove_count, 1);
     dtime = apr_time_now() - stime;
     dt = dtime / (1.0 * APR_USEC_PER_SEC);
     r1 = 1.0*createremove_count/dt;
     printf("Create : %lf creates/sec (%.2lf sec total) \n", r1, dt);

     stime = apr_time_now();
     remove_allocs(caps_list, createremove_count);
     dtime = apr_time_now() - stime;
     dt = dtime / (1.0 * APR_USEC_PER_SEC);
     r1 = 1.0*createremove_count/dt;
     printf("Remove : %lf removes/sec (%.2lf sec total) \n", r1, dt);
     printf("\n");
  }

  //**************** Read/Write tests ***************************
  if (readwrite_count > 0) {
     i = readwrite_count/nthreads;
     printf("Starting Bulk tests (total files: %d, approx per thread: %d", readwrite_count, i);
     r1 = 1.0*readwrite_count*readwrite_size/1024.0/1024.0;
     r2 = r1 / nthreads;
     printf(" -- total size: %lfMB, approx per thread: %lfMB\n", r1, r2);

     printf("Creating allocations...."); fflush(stdout);
     stime = apr_time_now();
     caps_list = create_allocs(readwrite_count, readwrite_size);
     dtime = apr_time_now() - stime;
     dt = dtime / (1.0 * APR_USEC_PER_SEC);
     r1 = 1.0*readwrite_count/dt;
     printf(" %lf creates/sec (%.2lf sec total) \n", r1, dt);

     if (use_alias) {
        base_caps = caps_list;
        printf("Creating alias allocations...."); fflush(stdout);
        stime = apr_time_now();
        caps_list = create_alias_allocs(readwrite_count, base_caps, readwrite_count);
        dtime = apr_time_now() - stime;
        dt = dtime / (1.0 * APR_USEC_PER_SEC);
        r1 = 1.0*readwrite_count/dt;
        printf(" %lf creates/sec (%.2lf sec total) \n", r1, dt);
     }

     stime = apr_time_now();
     write_allocs(caps_list, readwrite_count, readwrite_size, rw_block_size);
     dtime = apr_time_now() - stime;
     dt = dtime / (1.0 * APR_USEC_PER_SEC);
     r1 = 1.0*readwrite_count*readwrite_size/(dt*1024*1024);
     printf("Write: %lf MB/sec (%.2lf sec total) \n", r1, dt);

     stime = apr_time_now();
     read_allocs(caps_list, readwrite_count, readwrite_size, rw_block_size);
     dtime = apr_time_now() - stime;
     dt = dtime / (1.0 * APR_USEC_PER_SEC);
     r1 = 1.0*readwrite_count*readwrite_size/(dt*1024*1024);
     printf("Read: %lf MB/sec (%.2lf sec total) \n", r1, dt);

     stime = apr_time_now();
     random_allocs(caps_list, readwrite_count, readwrite_size, rw_block_size, read_mix_fraction);
     dtime = apr_time_now() - stime;
     dt = dtime / (1.0 * APR_USEC_PER_SEC);
     r1 = 1.0*readwrite_count*readwrite_size/(dt*1024*1024);
     printf("Random: %lf MB/sec (%.2lf sec total) \n", r1, dt);

     //**************** Small I/O tests ***************************
     if (smallio_count > 0) {
        if (small_min_size == 0) small_min_size = 1;
        if (small_max_size == 0) small_max_size = 1;

        printf("\n");
        printf("Starting Small Random I/O tests...\n");

        stime = apr_time_now();
        r1 = small_write_allocs(caps_list, readwrite_count, readwrite_size, smallio_count, small_min_size, small_max_size);
        dtime = apr_time_now() - stime;
        dt = dtime / (1.0 * APR_USEC_PER_SEC);
        r1 = r1/(1024.0*1024.0);
        r2 = r1/dt;
        r3 = smallio_count; r3 = r3 / dt;
        printf("Small Random Write: %lf MB/sec (%.2lf sec total using %lfMB or %.2lf ops/sec) \n", r2, dt, r1, r3);

        stime = apr_time_now();
        r1 = small_read_allocs(caps_list, readwrite_count, readwrite_size, smallio_count, small_min_size, small_max_size);
        dtime = apr_time_now() - stime;
        dt = dtime / (1.0 * APR_USEC_PER_SEC);
        r1 = r1/(1024.0*1024.0);
        r2 = r1/dt;
        r3 = smallio_count; r3 = r3 / dt;
        printf("Small Random Read: %lf MB/sec (%.2lf sec total using %lfMB or %.2lf ops/sec) \n", r2, dt, r1, r3);

        stime = apr_time_now();
        r1 = small_random_allocs(caps_list, readwrite_count, readwrite_size, small_read_fraction, smallio_count, small_min_size, small_max_size);
        dtime = apr_time_now() - stime;
        dt = dtime / (1.0 * APR_USEC_PER_SEC);
        r1 = r1/(1024.0*1024.0);
        r2 = r1/dt;
        r3 = smallio_count; r3 = r3 / dt;
        printf("Small Random R/W: %lf MB/sec (%.2lf sec total using %lfMB or %.2lf ops/sec) \n", r2, dt, r1, r3);
     }

     if (use_alias) {
        printf("Removing alias allocations...."); fflush(stdout);
        stime = apr_time_now();
        alias_remove_allocs(caps_list, base_caps, readwrite_count, readwrite_count);
        dtime = apr_time_now() - stime;
        dt = dtime / (1.0 * APR_USEC_PER_SEC);
        r1 = 1.0*readwrite_count/dt;
        printf(" %lf removes/sec (%.2lf sec total) \n", r1, dt);

        caps_list = base_caps;
     }


     //** If disk chksumming is enabled then validate it as well
     if ((chksum_valid_type(disk_cs_type) == 1) && (do_validate == 1)) {
        printf("Validating allocations...."); fflush(stdout);
        stime = apr_time_now();
        validate_allocs(caps_list, readwrite_count);
        dtime = apr_time_now() - stime;
        dt = dtime / (1.0 * APR_USEC_PER_SEC);
        r1 = 1.0*readwrite_count/dt;
        printf(" %lf validates/sec (%.2lf sec total) \n", r1, dt);
        printf("\n");
     }

     if (fd_out == NULL) {
        printf("Removing allocations...."); fflush(stdout);
        stime = apr_time_now();
        remove_allocs(caps_list, readwrite_count);
        dtime = apr_time_now() - stime;
        dt = dtime / (1.0 * APR_USEC_PER_SEC);
        r1 = 1.0*readwrite_count/dt;
        printf(" %lf removes/sec (%.2lf sec total) \n", r1, dt);
        printf("\n");
     } else {
        printf("Saving allocations to %s instead of deleting them\n", out_fname);
        save_allocs(fd_out, caps_list, readwrite_count);
        if (fd_special == 0) fclose(fd_out);
     }
  }

  printf("Final network connection counter: %d\n", network_counter(NULL));

  ibp_destroy_context(ic);  //** Shutdown IBP

  return(0);
}


