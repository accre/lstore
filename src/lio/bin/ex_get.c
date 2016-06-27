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

#define _log_module_index 169

#include <apr_time.h>
#include <gop/gop.h>
#include <gop/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/log.h>
#include <tbx/transfer_buffer.h>
#include <tbx/type_malloc.h>

#include <lio/ex3.h>
#include <lio/lio.h>

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int bufsize_mb = 10;
    ex_off_t bufsize;
    char *rbuf, *wbuf, *tmpbuf;
    tbx_tbuf_t tbuf;
    apr_time_t start_time, disk_start, cumulative_time, disk_cumulative;
    double dt, bandwidth, mbytes;
    ex_off_t i, err, size, rlen, wlen;
    int firsttime, start_option, print_timing;
    char *fname = NULL;
    exnode_t *ex;
    exnode_exchange_t *exp;
    segment_t *seg;
    op_generic_t *gop;
    ex_tbx_iovec_t iov;
    FILE *fd;

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
            bufsize_mb = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-i") == 0) { //** Enable timing
            i++;
            print_timing = 1;
        }

    } while (start_option < i);

    bufsize = 1024*1024*bufsize_mb / 2;
    tbx_type_malloc(rbuf, char, bufsize+1);
    tbx_type_malloc(wbuf, char, bufsize+1);
    log_printf(1, "bufsize= 2 * " XOT " bytes (%d MB total)\n", bufsize, bufsize_mb);

    //** This is the remote file to download
    fname = argv[i];
    i++;
    if (fname == NULL) {
        printf("Missing remote file!\n");
        return(2);
    }

    //** Load it
    exp = lio_exnode_exchange_load_file(fname);

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
    ex = lio_exnode_create();
    lio_exnode_deserialize(ex, exp, lio_gc->ess);

    //** Get the default view to use
    seg = lio_exnode_default_get(ex);
    if (seg == NULL) {
        printf("No default segment!  Aborting!\n");
        abort();
    }

    //** Get the object size
    size = segment_size(seg);

    start_time = apr_time_now();
    disk_cumulative = 0;
    i = 0;
    firsttime = 1;
    rlen = wlen = 0;
    do {
        //** Swap the buffers
        tmpbuf = rbuf;
        rbuf = wbuf;
        wbuf = tmpbuf;
        wlen = rlen;

        rlen = ((i+bufsize)>=size) ? size%bufsize : bufsize;
        tbx_tbuf_single(&tbuf, rlen, rbuf);
        ex_iovec_single(&iov, i, rlen);
        log_printf(1, "ex_get: i=" XOT " rlen=" XOT " wlen=" XOT "\n", i, rlen, wlen);
        tbx_log_flush();
        gop = segment_read(seg, lio_gc->da, NULL, 1, &iov, &tbuf, 0, 5);
        log_printf(1, "ex_get: i=" XOT " gid=%d\n", i, gop_id(gop));
        tbx_log_flush();

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

        gop_free(gop, OP_DESTROY);

        firsttime = 0;
        i = i + rlen;
    } while (rlen == bufsize);

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

    lio_exnode_exchange_destroy(exp);

    lio_exnode_destroy(ex);

    lio_shutdown();

    free(rbuf);
    free(wbuf);

    return(0);
}
