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

#include <assert.h>
#include <stdio.h>
#include "zlib.h"
#include "type_malloc.h"
#include "string_token.h"

//*************************************************************************
// adler32_fd- Calculates the adler32 for the given FD
//*************************************************************************

uLong adler32_fd(FILE *fd, unsigned char *buffer, int bufsize)
{
  int nbytes;
  uLong adler = adler32(0L, Z_NULL, 0);

  while ((nbytes = fread(buffer, 1, bufsize, fd)) > 0) {
     adler = adler32(adler, buffer, nbytes);
  }

  return(adler);
}


//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
  int i, start_index, start_option;
  char *fname;
  unsigned char *buffer;
  unsigned int aval;
  int bufsize;
  FILE *fd;

  bufsize = 20*1024*1024;

//printf("argc=%d\n", argc);
  if (argc < 2) {
     printf("\n");
     printf("adler32 [-b bsize] file1 file2 ...\n");
     printf("    -b bsize           - Read buffer size (units accepted).  Default is 10Mi\n");
     printf("    file1 ....         - File list. Use '-' for reading from stdin.\n");
     return(1);
  }

  //*** Parse the args
  i=1;
  do {
     start_option = i;

     if (strcmp(argv[i], "-b") == 0) { //** Load the buffer
        i++;
        bufsize = string_get_integer(argv[i]); i++;
     }

  } while (start_option < i);
  start_index = i;

  //** Make the buffer
  type_malloc(buffer, unsigned char, bufsize);

  for (i=start_index; i<argc; i++) {
     fname = argv[i];
     fd = (strcmp(fname, "-") == 0) ? stdin : fopen(fname, "r");
     if (fd == NULL) {
        printf("--------  %s\n", fname);
        continue;
     }

     aval = adler32_fd(fd, buffer, bufsize);
     printf("%08x  %s\n", aval, fname);
  }

  free(buffer);
  return(0);
}
