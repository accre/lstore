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
#include <ibp-server/ibp_server.h>
#include <tbx/log.h>
#include <tbx/fmttypes.h>
#include "subnet.h"
#include "ibp_time.h"
#include <ibp-server/print_alloc.h>
#include "osd_abstract.h"

#define HEADER_SIZE 4096
#define MAX_SIZE 100

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
  int bufsize = 1024*1024;
  char buffer[bufsize];
  Allocation_t a;
  Allocation_history_t h;
  osd_off_t fpos, npos, bs, hbs, n, nblocks, start_block, end_block;
  int  err, cs_type;
  int used, print_blocks, state;
  int offset, len, ndata, i, j, k, b, s, start_option;
  long long int bytes_used;
  tbx_chksum_t cs;
  osd_off_t blen[MAX_SIZE];
  int  bin_len;
  char good_block[MAX_SIZE];
  char block_chksum[MAX_SIZE*CHKSUM_MAX_SIZE];
  char calc_chksum[MAX_SIZE*CHKSUM_MAX_SIZE];
  char hex_digest[CHKSUM_MAX_SIZE+1];
  char hex_digest2[CHKSUM_MAX_SIZE+1];
  char *fname = NULL;
  char *afile;
  FILE *fd;
  osd_fd_t *afd;
  osd_t *dev;

//printf("sizeof(Allocation_t)=" LU " ALLOC_HEADER=%d\n", sizeof(Allocation_t), ALLOC_HEADER);

  if (argc < 2) {
     printf("read_alloc [-d debug_level] [--print_blocks] [--file fname data_offset len] rid_file\n");
     printf("where --file stores a portion of the allocation to fname based on the given offset and length\n");
     printf("          fname of where to store data.  If stdout or stderr redirects to that device\n");
     printf("          data_offset is the offset relative to the start of data, after the header.\n");
     printf("          len of 0 means return all data available starting from offset\n");
     printf("      --print_blocks  Prints the chksum block information if available\n");
     printf("      -d debug_level  Sets the debug level.  Default is 0.\n");
     printf("\n");
     return(0);
  }

  //** Initialize APR for use
  assert(apr_initialize() == APR_SUCCESS);
  tbx_set_log_level(0);

  i = 1;

  offset = -2;
  len = 0;
  ndata = 0;
  print_blocks = 0;
//printf("argc=%d i=%d\n", argc,i);
  do {
     start_option = i;
     if (strcmp("--file", argv[i]) == 0) {
        i++;
        fname = argv[i]; i++;
        offset = atoi(argv[i]); i++;
        len = atoi(argv[i]); i++;
//printf("fname=%s offset=%d len=%d\n", fname, offset, len);
     } else if (strcmp("--print_blocks", argv[i]) == 0) {
        i++;
        print_blocks = 1;
     } else if (strcmp("-d", argv[i]) == 0) {
        i++;
        k = atoi(argv[i]); i++;
        tbx_set_log_level(k);
     }
  } while (start_option < i);

  afile = argv[i]; i++;
//log_printf(0, "read_alloc: afile=%s i=%d\n", afile, i);


  //** Read the Allocation ***
  dev = osd_mount_fs("loopback", 10, 1000);  //** Mount the file via loopback
  fs_associate_id(dev, 0, afile);  //** Associate the loopback id with the actual file

  afd = osd_open(dev, 0, OSD_READ_MODE);
  assert(afd != NULL);

  //** Read the header and history
  err = osd_read(dev, afd, 0, sizeof(a), &a);
  err = osd_read(dev, afd, sizeof(a), sizeof(h), &h);

  state = osd_get_state(dev, afd);
  osd_chksum_info(dev, 0, &cs_type, &hbs, &bs);  //** Get the chksum info if available

  //** Print the allocation information
  used = 0;
  print_allocation(buffer, &used, sizeof(buffer)-1, &a, &h, state, cs_type, hbs, bs);
  printf("%s", buffer);

  //** Print the block information if needed
  if ((print_blocks == 1) && (tbx_chksum_type_valid(cs_type) == 1)) {
     printf("\n\n");
     printf(" Block      Bytes    State          Chksum (calculated if error) \n");
     printf("-------  ----------  -----   --------------------------------------------\n");
     tbx_chksum_set(&cs, cs_type);
     bin_len = tbx_chksum_size(&cs, CHKSUM_DIGEST_BIN);
     n = osd_fd_size(dev, afd) - hbs;
     nblocks = (n/bs) + 1;
     if ((n%bs) > 0) nblocks++;

//nblocks = 1;
     n = nblocks/MAX_SIZE;
     if ((nblocks%MAX_SIZE) != 0) n++;
     start_block = 0;
     for (i=0; i<n; i++) {
        end_block = start_block + MAX_SIZE-1;
        if (end_block >= nblocks) end_block = nblocks-1;

        osd_get_chksum(dev, 0, block_chksum, calc_chksum, sizeof(block_chksum), blen, good_block, start_block, end_block);
        k = end_block-start_block + 1;
        for (j=0; j<k; j++) {
            b = start_block + j;
            bytes_used = blen[j];
            s = good_block[j];
            tbx_chksum_bin2hex(bin_len, (unsigned char *)&(block_chksum[j*bin_len]), hex_digest);
            if (s == 0) {
               printf("%7d  %10lld   %2d     %s\n", b, bytes_used, s, hex_digest);
            } else {
               tbx_chksum_bin2hex(bin_len, (unsigned char *)&(calc_chksum[j*bin_len]), hex_digest2);
               printf("%7d  %10lld   %2d     %s (%s)\n", b, bytes_used, s, hex_digest, hex_digest2);
            }
        }

        start_block = start_block + MAX_SIZE;
     }

  }

  //** Lastly print any data that is requested.
  if (offset > -1) {
     i = 1;
     if (strcmp(fname, "stdout") == 0) {
        fd = stdout;
        i = 0;
     } else if (strcmp(fname, "stderr") == 0) {
        fd = stderr;
        i = 0;
     } else if ((fd = fopen(fname, "w")) == NULL) {
        printf("read_alloc: Can't open file %s!\n", fname);
     }

     printf("\n");
     printf("From relative data offset %d storing %d bytes in %s\n", offset, len, fname);

     fpos = offset + HEADER_SIZE;
     ndata = len;
     while (ndata > 0) {
        npos = (bufsize > ndata) ? ndata : bufsize;
//memset(buffer, 0, bufsize);
//printf("read_alloc: fpos=" I64T "\n", fpos);
        err = osd_read(dev, afd, fpos, npos, buffer);
        if (err != npos) {
           printf("read_alloc:  Error reading at fpos=" I64T " len=" I64T "  error=%d\n", fpos, npos, err);
        }

        fwrite(buffer, npos, 1, fd);
        fpos = fpos + npos;
        ndata = ndata - npos;
     }

     fclose(fd);
  }


  osd_close(dev, afd);
  osd_umount(dev);

  apr_terminate();

  printf("\n");

  return(0);
}
