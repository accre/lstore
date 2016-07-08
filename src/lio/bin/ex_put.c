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

#define _log_module_index 170

#include <apr_time.h>
#include <gop/gop.h>
#include <gop/types.h>
#include <lio/segment.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/assert_result.h>
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
    ex_off_t i, rlen, wlen, tlen;
    int err, start_option;
    int print_timing;
    char *fname = NULL;
    lio_exnode_t *ex;
    lio_exnode_exchange_t *exp, *exp_out;
    ex_tbx_iovec_t iov;
    lio_segment_t *seg;
    gop_op_generic_t *gop;
    FILE *fd, *fd_out;

    if (argc < 2) {
        printf("\n");
        printf("ex_put LIO_COMMON_OPTIONS [-i] [-b bufsize] local_file remote_file.ex3\n");
        lio_print_options(stdout);
        printf("    -c system.cfg   - IBP and Cache configuration options\n");
        printf("    -b bufsize      - Buffer size to use in MBytes (Default=%dMB)\n", bufsize_mb);
        printf("    -i              - Print timing and bandwith information\n");
        printf("    local_file - Local file to upload or \"-\" to use stdin\n");
        printf("    remote_file.ex3 - Remote ex3 object to store the local file\n");
        printf("\n");
        return(1);
    }

    lio_init(&argc, &argv);

    print_timing = 0;

    //*** Parse the args
    i=1;
    do {
        start_option = i;

        if (strcmp(argv[i], "-b") == 0) { //** Get the buffer size
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

    //** Open the source file
    if (strcmp(argv[i], "-") == 0) {
        fd = stdin;
    } else {
        fd = fopen(argv[i], "r");
        if (fd == NULL) {
            printf("Error opening source file: %s\n", argv[i]);
            abort();
        }
    }
    i++;

    fname = argv[i];
    i++; // ** This is the remote file
    if (fname == NULL) {
        printf("Missing input filename!\n");
        return(2);
    }

    //** Load it
    exp = lio_exnode_exchange_load_file(fname);

    //** and parse it
    ex = lio_exnode_create();
    lio_exnode_deserialize(ex, exp, lio_gc->ess);

    //** Get the default view to use
    seg = lio_exnode_default_get(ex);
    if (seg == NULL) {
        printf("No default segment!  Aborting!\n");
        abort();
    }

    //** Truncate the file
    err = gop_sync_exec(lio_segment_truncate(seg, lio_gc->da, 0, 10));
    if (err != OP_STATE_SUCCESS) {
        printf("Error truncating the remote file!\n");
    }

    start_time = apr_time_now();
    i = 0;
    wlen = 0;
    disk_start = apr_time_now();
    rlen = fread(rbuf, 1, bufsize, fd);
    disk_cumulative = apr_time_now() - disk_start;
    rbuf[rlen] = '\0';
    do {
        //** Swap the buffers
        tmpbuf = rbuf;
        rbuf = wbuf;
        wbuf = tmpbuf;
        tlen = rlen;
        rlen = wlen;
        wlen = tlen;

        log_printf(1, "ex_put: pos=" XOT " rlen=" XOT " wlen=" XOT "\n", i, rlen, wlen);
        //** Start the write
        tbx_tbuf_single(&tbuf, wlen, wbuf);
        ex_iovec_single(&iov, i, wlen);
        gop = segment_write(seg, lio_gc->da, NULL, 1, &iov, &tbuf, 0, lio_gc->timeout);
        gop_start_execution(gop);

        log_printf(1, "ex_put: i=" XOT " gid=%d\n", i, gop_id(gop));
        tbx_log_flush();

        //** Read in the next block
        disk_start = apr_time_now();
        rlen = fread(rbuf, 1, bufsize, fd);
        disk_cumulative += apr_time_now() - disk_start;
        rbuf[rlen] = '\0';

        err = gop_waitall(gop);
        log_printf(1, "ex_put: i=" XOT " gid=%d err=%d\n", i, gop_id(gop), err);
        tbx_log_flush();
        if (err != OP_STATE_SUCCESS) {
            printf("ex_put: Error writing offset=" XOT " wlen=" XOT "!\n",i, wlen);
            tbx_log_flush();
            fflush(stdout);
            abort();
        }
        gop_free(gop, OP_DESTROY);

        i = i + wlen;
    } while (rlen > 0);

    //** Flush everything to backing store
    log_printf(1, "Flushing to disk size=" XOT "\n", segment_size(seg));
    tbx_log_flush();
    gop_sync_exec(segment_flush(seg, lio_gc->da, 0, segment_size(seg)+1, lio_gc->timeout));
    log_printf(1, "Flush completed\n");
    cumulative_time = apr_time_now() - start_time;

    //** Go ahead and trim the file back to it's actual size
    gop_sync_exec(lio_segment_truncate(seg, lio_gc->da, segment_size(seg), lio_gc->timeout));

    //** Print the informational summary if needed
    if (print_timing == 1) {
        dt = cumulative_time / (1.0*APR_USEC_PER_SEC);
        mbytes = segment_size(seg) / 1024.0 / 1024.0;
        bandwidth = mbytes / dt;
        printf("Transfer time: %lf sec  Bandwidth: %lf MB/s  Size: %lf MB\n", dt, bandwidth, mbytes);

        dt = disk_cumulative / (1.0*APR_USEC_PER_SEC);
        bandwidth = mbytes / dt;
        printf("Disk Read time: %lf sec  Bandwidth: %lf MB/s\n", dt, bandwidth);
        printf("\n");
    }

    //** Store the updated exnode back to disk
    exp_out = lio_exnode_exchange_create(EX_TEXT);
    lio_exnode_serialize(ex, exp_out);

    fd_out = fopen(fname, "w");
    fprintf(fd_out, "%s", exp_out->text.text);
    fclose(fd_out);
    lio_exnode_exchange_destroy(exp_out);

    lio_exnode_exchange_destroy(exp);

    //** Shut everything down;
    lio_exnode_destroy(ex);

    lio_shutdown();

    free(rbuf);
    free(wbuf);

    return(0);
}


