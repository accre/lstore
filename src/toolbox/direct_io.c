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

#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

#include <tbx/fmttypes.h>
#include <tbx/direct_io.h>
#include <tbx/log.h>

//***********************************************************************
// tbx_dio_init - Preps the FD for direct I/O.  It returns the initial
//    for restoring if you want
//***********************************************************************

int tbx_dio_init(FILE *fd)
{
    int flags, flags_new;

    flags = fcntl(fileno(fd), F_GETFL);
    flags_new = flags |  O_DIRECT;
    fcntl(fileno(fd), F_SETFL, flags_new);  //** Enable direct I/O

    return(flags);
}

//***********************************************************************
// tbx_dio_finish - Unsets the O_DIRECT flag and adjusts the FD position
//    If flags == 0 they aren't  changed.  Just O_DIRECT is disabled
//***********************************************************************

void tbx_dio_finish(FILE *fd, int flags)
{
    if (flags == 0) {
        flags = fcntl(fileno(fd), F_GETFL);
        flags |=  O_DIRECT;
    }

    fcntl(fileno(fd), F_SETFL, flags);  //** Restore the original flags
}

//***********************************************************************
// tbx_dio_read - Uses O_DIRECT if possible to read data
//    if offset == -1 then command act like fread
//***********************************************************************

ssize_t tbx_dio_read(FILE *fd, char *buf, ssize_t nbytes, ssize_t offset)
{
    ssize_t n;
    int nfd, flags, old_flags;

    nfd = fileno(fd);
    //** Move the file pointer
    if (offset != -1) {
        lseek(nfd, offset, SEEK_SET);
    }

    //** Try and do the I/O with direct I/O
    n = read(nfd, buf, nbytes);
    log_printf(5, "nfd=%d nbytes=" SST " got=" SST " lseek=" SST " errno=%d\n", nfd, nbytes, n, lseek(nfd, 0L, SEEK_CUR), errno);
    if (n == -1) {  //** Got an error so try without Direct I/O
        old_flags = fcntl(nfd, F_GETFL);
        flags = old_flags ^ O_DIRECT;
        fcntl(nfd, F_SETFL, flags);

        n = read(nfd, buf, nbytes);
        log_printf(5, "RETRY nfd=%d nbytes=" SST " got=" SST " lseek=" SST " errno=%d\n", nfd, nbytes, n, lseek(nfd, 0L, SEEK_CUR), errno);

        fcntl(nfd, F_SETFL, old_flags);
    }

    return(n);
}

//***********************************************************************
// tbx_dio_write - Uses O_DIRECT if possible to write data
//    if offset == -1 then command act like fwrite
//***********************************************************************

ssize_t tbx_dio_write(FILE *fd, char *buf, ssize_t nbytes, ssize_t offset)
{
    ssize_t n;
    int nfd, flags, old_flags;

    nfd = fileno(fd);
    //** Move the file pointer
    if (offset != -1) {
        lseek(nfd, offset, SEEK_SET);
    }

    //** Try and do the I/O with direct I/O
    n = write(nfd, buf, nbytes);
    log_printf(5, "nfd=%d nbytes=" SST " got=" SST " lseek=" SST " errno=%d\n", nfd, nbytes, n, lseek(nfd, 0L, SEEK_CUR), errno);
    if (n == -1) {  //** Got an error so try without Direct I/O
        old_flags = fcntl(nfd, F_GETFL);
        flags = old_flags ^ O_DIRECT;
        fcntl(nfd, F_SETFL, flags);

        n = write(nfd, buf, nbytes);
        log_printf(5, "RETRY nfd=%d nbytes=" SST " got=" SST " lseek=" SST " errno=%d\n", nfd, nbytes, n, lseek(nfd, 0L, SEEK_CUR), errno);

        fcntl(nfd, F_SETFL, old_flags);
    }

    return(n);
}
