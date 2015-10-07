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

#define _log_module_index 169

#include <assert.h>
#include "exnode.h"
#include "log.h"
#include "iniparse.h"
#include "type_malloc.h"
#include "thread_pool.h"
#include "lio.h"

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
  int bufsize_mb = 10;
  ex_off_t bufsize;
  char *rbuf, *wbuf, *tmpbuf;
  tbuffer_t tbuf;
  apr_time_t start_time, disk_start, cumulative_time, disk_cumulative;
  double dt, bandwidth, mbytes;
  ex_off_t i, err, size, rlen, wlen;
  int firsttime, start_option, print_timing;
  char *fname = NULL;
  exnode_t *ex;
  exnode_exchange_t *exp;
  segment_t *seg;
  op_generic_t *gop;
  ex_iovec_t iov;
  FILE *fd;

//printf("argc=%d\n", argc);
  if (argc < 2) {
     printf("\n");
     printf("ex_get LIO_COMMON_OPTIONS [-i] [-b bufsize] remote_file.ex3 local_file\n");
     lio_print_options(stdout);
     printf("    -b bufsize      - Buffer size to use in MBytes (Default=%dMB)\n", bufsize_mb);
     printf("    -i              - Print timing and bandwith information\n");
     printf("    remote_file.ex3 - Remote ex3 object to retreive and store in the local file\n");
     printf("    local_file - Local file to store data or \"-\" to use stdout\n");
     printf("\n");
     return(1);
  }

  lio_init(&argc, &argv);

  print_timing = 0;

  //*** Parse the args
  i=1;
  do {
     start_option = i;

     if (strcmp(argv[i], "-b") == 0) { //** Enable debugging
        i++;
        bufsize_mb = atoi(argv[i]); i++;
     } else if (strcmp(argv[i], "-i") == 0) { //** Enable timing
        i++;
        print_timing = 1;
     }

  } while (start_option < i);

  bufsize = 1024*1024*bufsize_mb / 2;
  type_malloc(rbuf, char, bufsize+1);
  type_malloc(wbuf, char, bufsize+1);
  log_printf(1, "bufsize= 2 * " XOT " bytes (%d MB total)\n", bufsize, bufsize_mb);

  //** This is the remote file to download
  fname = argv[i]; i++;
  if (fname == NULL) {
    printf("Missing remote file!\n");
    return(2);
  }

  //** Load it
  exp = exnode_exchange_load_file(fname);

//  printf("Initial exnode=====================================\n");
//  printf("%s", exp->text);
//  printf("===================================================\n");


  //** Open the local destination file
  if (strcmp(argv[i], "-") == 0) {
     fd = stdout;
  } else {
     fd = fopen(argv[i], "w");
     if (fd == NULL) {
        printf("Error opening destination file: %s\n", argv[i]);
        abort();
     }
  }
  i++;


  //** and parse the remote exnode
  ex = exnode_create();
  exnode_deserialize(ex, exp, lio_gc->ess);

  //** Get the default view to use
  seg = exnode_get_default(ex);
  if (seg == NULL) {
     printf("No default segment!  Aborting!\n");
     abort();
  }

  //** Get the object size
  size = segment_size(seg);

//FILE *fd2 = fopen("test.out", "w");

  start_time = apr_time_now();
  disk_cumulative = 0;
  i = 0;
  firsttime = 1;
  rlen = wlen = 0;
  do {
     //** Swap the buffers
     tmpbuf = rbuf;  rbuf = wbuf; wbuf = tmpbuf;
     wlen = rlen;

     rlen = ((i+bufsize)>=size) ? size%bufsize : bufsize;
     tbuffer_single(&tbuf, rlen, rbuf);
     ex_iovec_single(&iov, i, rlen);
log_printf(1, "ex_get: i=%d rlen=%d wlen=%d\n", i, rlen, wlen); flush_log();
     gop = segment_read(seg, lio_gc->da, NULL, 1, &iov, &tbuf, 0, 5);
log_printf(1, "ex_get: i=%d gid=%d\n", i, gop_id(gop)); flush_log();

     //** Dump the data to disk
     if (firsttime == 0) {
        disk_start = apr_time_now();
        fwrite(wbuf, wlen, 1, fd);
        disk_cumulative += apr_time_now() - disk_start;
     }

     err = gop_waitall(gop);
     if (err != OP_STATE_SUCCESS) {
        printf("Error reading offset=" XOT " len=" XOT "!\n",i, rlen);
        rlen = 0;  //** Kicks us out

     }

//fwrite(rbuf, rlen, 1, fd2);

     gop_free(gop, OP_DESTROY);

     firsttime = 0;
     i = i + rlen;
  } while (rlen == bufsize);

//fclose(fd2);

  //** Dump the last block to disk
  disk_start = apr_time_now();
  fwrite(rbuf, rlen, 1, fd);
  disk_cumulative += apr_time_now() - disk_start;

  fclose(fd);

  cumulative_time = apr_time_now() - start_time;

  //** Print the informational summary if needed
  if (print_timing == 1) {
    dt = cumulative_time / (1.0*APR_USEC_PER_SEC);
    mbytes = segment_size(seg) / 1024.0 / 1024.0;
    bandwidth = mbytes / dt;
    printf("Transfer time: %lf sec  Bandwidth: %lf MB/s  Size: %lf MB\n", dt, bandwidth, mbytes);

    dt = disk_cumulative / (1.0*APR_USEC_PER_SEC);
    bandwidth = mbytes / dt;
    printf("Disk Write time: %lf sec  Bandwidth: %lf MB/s\n", dt, bandwidth);
    printf("\n");
  }

  exnode_exchange_destroy(exp);

  exnode_destroy(ex);

  lio_shutdown();

  free(rbuf);
  free(wbuf);

  return(0);
}


