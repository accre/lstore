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

#include <assert.h>
#include <tbx/assert_result.h>
#include <stdio.h>
#include <string.h>
#include "zlib.h"
#include <tbx/type_malloc.h>
#include <tbx/string_token.h>

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
            bufsize = tbx_stk_string_get_integer(argv[i]);
            i++;
        }

    } while (start_option < i);
    start_index = i;

    //** Make the buffer
    tbx_type_malloc(buffer, unsigned char, bufsize);

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
