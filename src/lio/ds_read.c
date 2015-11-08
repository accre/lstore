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

#define _log_module_index 205

#include <assert.h>
#include "assert_result.h"
#include "exnode.h"
#include "log.h"
#include "iniparse.h"
#include "string_token.h"
#include "type_malloc.h"
#include "thread_pool.h"
#include "lio.h"


//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    apr_time_t start_time;
    double  dt;
    ex_off_t offset, len, dn, n;
    int err, i, j, start_index, start_option, n_iov, n_rcap, timeout;
    char **buffer, **rcap, *fname, *p;
    char ppbuf[32];
    FILE *fd;
    iovec_t **iov;
    opque_t *q;
    op_generic_t *gop, **gop_list;
    op_status_t status;
    tbuffer_t *tbuf;

    err = 0;
    len = 0;
    timeout = 60;

    _lio_ifd = stderr;  //** Default to all information going to stderr since the output is file data.

//printf("argc=%d\n", argc);
    if (argc < 2) {
        printf("\n");
        printf("ds_read LIO_COMMON_OPTIONS [-dt timeout] n_iov_per_rcap len_per_rcap rcap_file\n");
        lio_print_options(stdout);
        return(1);
    }

    lio_init(&argc, &argv);
    i=1;
    if (i < argc) {
        do {
            start_option = i;

            if (strcmp(argv[i], "-dt") == 0) {  //** Command timeout
                i++;
                timeout = string_get_integer(argv[i]);
                i++;
            }

        } while ((start_option < i) && (i<argc));
    }
    start_index = i;

    //** This is the 1st arg
    if ((argc-start_index) != 3) {
        info_printf(lio_ifd, 0, "Missing args! argc=%d start_index=%d\n", argc, start_index);
        return(2);
    }

    //** Make the buffer
    n_iov = string_get_integer(argv[start_index]);
    start_index++;
    len = string_get_integer(argv[start_index]);
    start_index++;
    fname = argv[start_index];
    start_index++;

    fd = fopen(fname, "r");
    assert(fd != NULL);
    p = fgets(ppbuf, sizeof(ppbuf), fd);
    if ((p = index(ppbuf, '\n')) != NULL) *p = 0;  //** Remove the \n if needed
    n_rcap = string_get_integer(ppbuf);

    info_printf(lio_ifd, 0, "n_rcap=%d len=" XOT " n_iov_per_cap=%d fname=%s timeout=%d\n", n_rcap, len, n_iov, fname, timeout);

    //** Make the space
    type_malloc(tbuf, tbuffer_t, n_rcap);
    type_malloc(rcap, char *, n_rcap);
    type_malloc(buffer, char *, n_rcap);
    type_malloc(iov, iovec_t *, n_rcap);
    type_malloc(gop_list, op_generic_t *, n_rcap);

    offset = 0;
    q = new_opque();
//  opque_start_execution(q);
    for (i=0; i<n_rcap; i++) {
        type_malloc(buffer[i], char, len+1);
        type_malloc(rcap[i], char, 256);
        p = fgets(rcap[i], 256, fd);
        if ((p = index(rcap[i], '\n')) != NULL) *p = '\0';  //** Remove the \n if needed

        type_malloc(iov[i], iovec_t, n_iov);
        dn = len / n_iov;
        n = 0;
        for (j=0; j<n_iov-1; j++) {
            iov[i][j].iov_base = buffer[i] + n;
            iov[i][j].iov_len = dn;
            n += dn;
        }
        iov[i][n_iov-1].iov_base = buffer[i] + n;
        iov[i][n_iov-1].iov_len = len - n;

        tbuffer_vec(&(tbuf[i]), len, n_iov, iov[i]);
        gop_list[i] = ds_read(lio_gc->ds, lio_gc->da, rcap[i], offset, tbuf, 0, len, timeout);
        info_printf(lio_ifd, 0, "i=%d gid=%d rcap=%s\n", i, gop_id(gop_list[i]), rcap[i]);

        gop_set_myid(gop_list[i], i);
        opque_add(q, gop_list[i]);
//     apr_sleep(0.1*APR_USEC_PER_SEC);
    }


//return(0);

    err = 0;
    start_time = apr_time_now();
    while ((gop = opque_waitany(q)) != NULL) {
        dt = apr_time_now() - start_time;
        dt = dt / APR_USEC_PER_SEC;

        status = gop_get_status(gop);
        i = gop_get_myid(gop);

        if (status.op_status != OP_STATE_SUCCESS) {
            info_printf(lio_ifd, 0, "i=%d ERROR: op_status=%d error_code=%d dt=%lf\n", i, status.op_status, status.error_code, dt);
            err++;
        } else {
            info_printf(lio_ifd, 0, "i=%d SUCCESS: op_status=%d error_code=%d dt=%lf\n", i, status.op_status, status.error_code, dt);
        }
    }

//  free(iov);
//  free(buffer);

    lio_shutdown();

    return(err);
}


