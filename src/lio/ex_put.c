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

#define _log_module_index 170

#include <assert.h>
#include "assert_result.h"
#include "exnode.h"
#include "log.h"
#include "type_malloc.h"
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
    ex_off_t i, rlen, wlen, tlen;
    int err, start_option;
    int print_timing;
    char *fname = NULL;
    exnode_t *ex;
    exnode_exchange_t *exp, *exp_out;
    ex_iovec_t iov;
    segment_t *seg;
    op_generic_t *gop;
    FILE *fd, *fd_out;

//printf("argc=%d\n", argc);
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
    type_malloc(rbuf, char, bufsize+1);
    type_malloc(wbuf, char, bufsize+1);
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
    exp = exnode_exchange_load_file(fname);

//  printf("Initial exnode=====================================\n");
//  printf("%s", exp->text);
//  printf("===================================================\n");

    //** and parse it
    ex = exnode_create();
    exnode_deserialize(ex, exp, lio_gc->ess);

    //** Get the default view to use
    seg = exnode_get_default(ex);
    if (seg == NULL) {
        printf("No default segment!  Aborting!\n");
        abort();
    }

    //** Truncate the file
    err = gop_sync_exec(segment_truncate(seg, lio_gc->da, 0, 10));
    if (err != OP_STATE_SUCCESS) {
        printf("Error truncating the remote file!\n");
//     abort();
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

//     log_printf(15, "ex_put: pos=%d rlen=%d text=!%s!\n", i, len, buffer);
        log_printf(1, "ex_put: pos=%d rlen=%d wlen=%d\n", i, rlen, wlen);
        //** Start the write
        tbuffer_single(&tbuf, wlen, wbuf);
        ex_iovec_single(&iov, i, wlen);
        gop = segment_write(seg, lio_gc->da, NULL, 1, &iov, &tbuf, 0, lio_gc->timeout);
        gop_start_execution(gop);

        log_printf(1, "ex_put: i=%d gid=%d\n", i, gop_id(gop));
        flush_log();

        //** Read in the next block
        disk_start = apr_time_now();
        rlen = fread(rbuf, 1, bufsize, fd);
        disk_cumulative += apr_time_now() - disk_start;
        rbuf[rlen] = '\0';

        err = gop_waitall(gop);
        log_printf(1, "ex_put: i=" XOT " gid=%d err=%d\n", i, gop_id(gop), err);
        flush_log();
        if (err != OP_STATE_SUCCESS) {
            printf("ex_put: Error writing offset=" XOT " wlen=" XOT "!\n",i, wlen);
            flush_log();
            fflush(stdout);
            abort();
        }
        gop_free(gop, OP_DESTROY);

        i = i + wlen;
    } while (rlen > 0);

    //** Flush everything to backing store
    log_printf(1, "Flushing to disk size=" XOT "\n", segment_size(seg));
    flush_log();
    gop_sync_exec(segment_flush(seg, lio_gc->da, 0, segment_size(seg)+1, lio_gc->timeout));
    log_printf(1, "Flush completed\n");
    cumulative_time = apr_time_now() - start_time;

    //** Go ahead and trim the file back to it's actual size
    gop_sync_exec(segment_truncate(seg, lio_gc->da, segment_size(seg), lio_gc->timeout));

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
    exp_out = exnode_exchange_create(EX_TEXT);
    exnode_serialize(ex, exp_out);
//  printf("Updated remote: %s\n", fname);
//  printf("-----------------------------------------------------\n");
//  printf("%s", exp_out->text);
//  printf("-----------------------------------------------------\n");

    fd_out = fopen(fname, "w");
    fprintf(fd_out, "%s", exp_out->text.text);
    fclose(fd_out);
    exnode_exchange_destroy(exp_out);

    exnode_exchange_destroy(exp);

    //** Shut everything down;
    exnode_destroy(ex);

    lio_shutdown();

    free(rbuf);
    free(wbuf);

    return(0);
}


