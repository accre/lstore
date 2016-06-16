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

#define _log_module_index 205

#include <apr_time.h>
#include <assert.h>
#include <gop/gop.h>
#include <gop/opque.h>
#include <gop/types.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <tbx/log.h>
#include <tbx/string_token.h>
#include <tbx/transfer_buffer.h>
#include <tbx/type_malloc.h>

#include "data_service_abstract.h"
#include "ex3_types.h"
#include "lio_abstract.h"


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
    tbx_iovec_t **iov;
    opque_t *q;
    op_generic_t *gop, **gop_list;
    op_status_t status;
    tbx_tbuf_t *tbuf;

    timeout = 60;

    _lio_ifd = stderr;  //** Default to all information going to stderr since the output is file data.

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
                timeout = tbx_stk_string_get_integer(argv[i]);
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
    n_iov = tbx_stk_string_get_integer(argv[start_index]);
    start_index++;
    len = tbx_stk_string_get_integer(argv[start_index]);
    start_index++;
    fname = argv[start_index];
    start_index++;

    fd = fopen(fname, "r");
    assert(fd != NULL);
    fgets(ppbuf, sizeof(ppbuf), fd);
    if ((p = index(ppbuf, '\n')) != NULL) *p = 0;  //** Remove the \n if needed
    n_rcap = tbx_stk_string_get_integer(ppbuf);

    info_printf(lio_ifd, 0, "n_rcap=%d len=" XOT " n_iov_per_cap=%d fname=%s timeout=%d\n", n_rcap, len, n_iov, fname, timeout);

    //** Make the space
    tbx_type_malloc(tbuf, tbx_tbuf_t, n_rcap);
    tbx_type_malloc(rcap, char *, n_rcap);
    tbx_type_malloc(buffer, char *, n_rcap);
    tbx_type_malloc(iov, tbx_iovec_t *, n_rcap);
    tbx_type_malloc(gop_list, op_generic_t *, n_rcap);

    offset = 0;
    q = gop_opque_new();
    for (i=0; i<n_rcap; i++) {
        tbx_type_malloc(buffer[i], char, len+1);
        tbx_type_malloc(rcap[i], char, 256);
        fgets(rcap[i], 256, fd);
        if ((p = index(rcap[i], '\n')) != NULL) *p = '\0';  //** Remove the \n if needed

        tbx_type_malloc(iov[i], tbx_iovec_t, n_iov);
        dn = len / n_iov;
        n = 0;
        for (j=0; j<n_iov-1; j++) {
            iov[i][j].iov_base = buffer[i] + n;
            iov[i][j].iov_len = dn;
            n += dn;
        }
        iov[i][n_iov-1].iov_base = buffer[i] + n;
        iov[i][n_iov-1].iov_len = len - n;

        tbx_tbuf_vec(&(tbuf[i]), len, n_iov, iov[i]);
        gop_list[i] = ds_read(lio_gc->ds, lio_gc->da, rcap[i], offset, tbuf, 0, len, timeout);
        info_printf(lio_ifd, 0, "i=%d gid=%d rcap=%s\n", i, gop_id(gop_list[i]), rcap[i]);

        gop_set_myid(gop_list[i], i);
        gop_opque_add(q, gop_list[i]);
    }

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

    lio_shutdown();

    return(err);
}


