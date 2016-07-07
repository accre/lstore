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

//***********************************************************************
// Routines for managing the segment loading framework
//***********************************************************************

#define _log_module_index 160

#include <apr_time.h>
#include <errno.h>
#include <gop/gop.h>
#include <gop/opque.h>
#include <gop/tp.h>
#include <gop/types.h>
#include <lio/segment.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <tbx/iniparse.h>
#include <tbx/log.h>
#include <tbx/transfer_buffer.h>
#include <tbx/type_malloc.h>

#include "ds.h"
#include "ex3.h"
#include "ex3/types.h"
#include "service_manager.h"

typedef struct {
    lio_segment_t *src;
    lio_segment_t *dest;
    data_attr_t *da;
    char *buffer;
    FILE *fd;
    lio_segment_rw_hints_t *rw_hints;
    ex_off_t src_offset;
    ex_off_t dest_offset;
    ex_off_t len;
    ex_off_t bufsize;
    int timeout;
    int truncate;
} lio_segment_copy_t;

//***********************************************************************
// load_segment - Loads the given segment from the file/struct
//***********************************************************************

lio_segment_t *load_segment(lio_service_manager_t *ess, ex_id_t id, lio_exnode_exchange_t *ex)
{
    char *type = NULL;
    char name[1024];
    segment_load_t *sload;

    if (ex->type == EX_TEXT) {
        snprintf(name, sizeof(name), "segment-" XIDT, id);
        tbx_inip_file_t *fd = ex->text.fd;
        type = tbx_inip_get_string(fd, name, "type", "");
    } else if (ex->type == EX_PROTOCOL_BUFFERS) {
        log_printf(0, "load_segment:  segment exnode parsing goes here\n");
    } else {
        log_printf(0, "load_segment:  Invalid exnode type type=%d for id=" XIDT "\n", ex->type, id);
        return(NULL);
    }

    sload = lio_lookup_service(ess, SEG_SM_LOAD, type);
    if (sload == NULL) {
        log_printf(0, "load_segment:  No matching driver for type=%s  id=" XIDT "\n", type, id);
        return(NULL);
    }

    free(type);
    return((*sload)(ess, id, ex));
}

//***********************************************************************
// lio_segment_copy_func - Does the actual segment copy operation
//***********************************************************************

gop_op_status_t lio_segment_copy_func(void *arg, int id)
{
    lio_segment_copy_t *sc = (lio_segment_copy_t *)arg;
    tbx_tbuf_t *wbuf, *rbuf, *tmpbuf;
    tbx_tbuf_t tbuf1, tbuf2;
    int err;
    ex_off_t bufsize;
    ex_off_t rpos, wpos, rlen, wlen, tlen, nbytes, dend;
    ex_tbx_iovec_t rex, wex;
    gop_opque_t *q;
    gop_op_generic_t *rgop, *wgop;
    gop_op_status_t status;

    //** Set up the buffers
    bufsize = sc->bufsize / 2;  //** The buffer is split for R/W
    tbx_tbuf_single(&tbuf1, bufsize, sc->buffer);
    tbx_tbuf_single(&tbuf2, bufsize, &(sc->buffer[bufsize]));
    rbuf = &tbuf1;
    wbuf = &tbuf2;

    //** Check the length
    nbytes = segment_size(sc->src) - sc->src_offset;
    if (nbytes < 0) {
        rlen = bufsize;
    } else {
        rlen = (nbytes > bufsize) ? bufsize : nbytes;
    }
    if ((sc->len != -1) && (sc->len < nbytes)) nbytes = sc->len;

    //** Go ahead and reserve the space in the destintaion
    dend = sc->dest_offset + nbytes;
    log_printf(1, "reserving space=" XOT "\n", dend);
    gop_sync_exec(lio_segment_truncate(sc->dest, sc->da, -dend, sc->timeout));

    //** Read the initial block
    rpos = sc->src_offset;
    wpos = sc->dest_offset;
//  rlen = (nbytes > bufsize) ? bufsize : nbytes;
    wlen = 0;
    ex_iovec_single(&rex, rpos, rlen);
    rpos += rlen;
    nbytes -= rlen;
    rgop = segment_read(sc->src, sc->da, sc->rw_hints, 1, &rex, rbuf, 0, sc->timeout);
    err = gop_waitall(rgop);
    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "Intial read failed! src=%" PRIu64 " rpos=" XOT " len=" XOT "\n", segment_id(sc->src), rpos, rlen);
        gop_free(rgop, OP_DESTROY);
        return(gop_failure_status);
    }
    gop_free(rgop, OP_DESTROY);

    q = gop_opque_new();
    do {
        //** Swap the buffers
        tmpbuf = rbuf;
        rbuf = wbuf;
        wbuf = tmpbuf;
        tlen = rlen;
        rlen = wlen;
        wlen = tlen;

        log_printf(1, "sseg=" XIDT " dseg=" XIDT " wpos=%" PRId64 " rlen=%" PRId64 " wlen=%" PRId64 "\n", segment_id(sc->src), segment_id(sc->dest), wpos, rlen, wlen);

        //** Start the write
        ex_iovec_single(&wex, wpos, wlen);
        wpos += wlen;
        wgop = segment_write(sc->dest, sc->da, sc->rw_hints, 1, &wex, wbuf, 0, sc->timeout);
        gop_opque_add(q, wgop);

        //** Read in the next block
        if (nbytes < 0) {
            rlen = bufsize;
        } else {
            rlen = (nbytes > bufsize) ? bufsize : nbytes;
        }
        if (rlen > 0) {
            ex_iovec_single(&rex, rpos, rlen);
            rpos += rlen;
            nbytes -= rlen;
            rgop = segment_read(sc->src, sc->da, sc->rw_hints, 1, &rex, rbuf, 0, sc->timeout);
            gop_opque_add(q, rgop);
        }

        err = opque_waitall(q);
        if (err != OP_STATE_SUCCESS) {
            log_printf(1, "ERROR read/write failed! src=" XIDT " rpos=" XOT " len=" XOT "\n", segment_id(sc->src), rpos, rlen);
            gop_opque_free(q, OP_DESTROY);
            return(gop_failure_status);
        }
    } while (rlen > 0);

    gop_opque_free(q, OP_DESTROY);

    if (sc->truncate == 1) {  //** Truncate if wanted
        gop_sync_exec(lio_segment_truncate(sc->dest, sc->da, wpos, sc->timeout));
    }

    status = gop_success_status;
    status.error_code = rpos;

    return(status);
}

//***********************************************************************
// lio_segment_copy - Copies data between segments.  This copy is performed
//      by reading from the source and writing to the destination.
//      This is not a depot-depot copy.  The data goes through the client.
//
//      If len == -1 then all available data from src is copied
//***********************************************************************

gop_op_generic_t *lio_segment_copy(gop_thread_pool_context_t *tpc, data_attr_t *da, lio_segment_rw_hints_t *rw_hints, lio_segment_t *src_seg, lio_segment_t *dest_seg, ex_off_t src_offset, ex_off_t dest_offset, ex_off_t len, ex_off_t bufsize, char *buffer, int do_truncate, int timeout)
{
    lio_segment_copy_t *sc;

    tbx_type_malloc(sc, lio_segment_copy_t, 1);

    sc->da = da;
    sc->timeout = timeout;
    sc->src = src_seg;
    sc->dest = dest_seg;
    sc->rw_hints = rw_hints;
    sc->src_offset = src_offset;
    sc->dest_offset = dest_offset;
    sc->len = len;
    sc->bufsize = bufsize;
    sc->buffer = buffer;
    sc->truncate = do_truncate;

    return(gop_tp_op_new(tpc, NULL, lio_segment_copy_func, (void *)sc, free, 1));
}


//***********************************************************************
// segment_get_func - Does the actual segment get operation
//***********************************************************************

gop_op_status_t segment_get_func(void *arg, int id)
{
    lio_segment_copy_t *sc = (lio_segment_copy_t *)arg;
    tbx_tbuf_t *wbuf, *rbuf, *tmpbuf;
    tbx_tbuf_t tbuf1, tbuf2;
    char *rb, *wb, *tb;
    ex_off_t bufsize;
    int err;
    ex_off_t rpos, wpos, rlen, wlen, tlen, nbytes, got, total;
    ex_tbx_iovec_t rex;
    apr_time_t loop_start, file_start;
    double dt_loop, dt_file;
    gop_op_generic_t *gop;
    gop_op_status_t status;

    //** Set up the buffers
    bufsize = sc->bufsize / 2;  //** The buffer is split for R/W
    rb = sc->buffer;
    wb = &(sc->buffer[bufsize]);
    tbx_tbuf_single(&tbuf1, bufsize, rb);
    tbx_tbuf_single(&tbuf2, bufsize, wb);
    rbuf = &tbuf1;
    wbuf = &tbuf2;

    status = gop_success_status;

    //** Read the initial block
    rpos = sc->src_offset;
    wpos = 0;
    nbytes = segment_size(sc->src) - sc->src_offset;
    if (nbytes < 0) {
        rlen = bufsize;
    } else {
        rlen = (nbytes > bufsize) ? bufsize : nbytes;
    }

    log_printf(5, "FILE fd=%p\n", sc->fd);

    ex_iovec_single(&rex, rpos, rlen);
    wlen = 0;
    rpos += rlen;
    nbytes -= rlen;
    loop_start = apr_time_now();
    gop = segment_read(sc->src, sc->da, sc->rw_hints, 1, &rex, rbuf, 0, sc->timeout);
    err = gop_waitall(gop);
    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "Intial read failed! src=" XIDT " rpos=" XOT " len=" XOT "\n", segment_id(sc->src), rpos, rlen);
        gop_free(gop, OP_DESTROY);
        return(gop_failure_status);
    }
    gop_free(gop, OP_DESTROY);

    total = 0;

    do {
        //** Swap the buffers
        tb = rb;
        rb = wb;
        wb = tb;
        tmpbuf = rbuf;
        rbuf = wbuf;
        wbuf = tmpbuf;
        tlen = rlen;
        rlen = wlen;
        wlen = tlen;

        log_printf(1, "sseg=" XIDT " rpos=" XOT " wpos=" XOT " rlen=" XOT " wlen=" XOT " nbytes=" XOT "\n", segment_id(sc->src), rpos, wpos, rlen, wlen, nbytes);

        //** Read in the next block
        if (nbytes < 0) {
            rlen = bufsize;
        } else {
            rlen = (nbytes > bufsize) ? bufsize : nbytes;
        }
        if (rlen > 0) {
            ex_iovec_single(&rex, rpos, rlen);
            loop_start = apr_time_now();
            gop = segment_read(sc->src, sc->da, sc->rw_hints, 1, &rex, rbuf, 0, sc->timeout);
            gop_start_execution(gop);  //** Start doing the transfer
            rpos += rlen;
            nbytes -= rlen;
        }

        //** Start the write
        file_start = apr_time_now();
        got = fwrite(wb, 1, wlen, sc->fd);
        dt_file = apr_time_now() - file_start;
        dt_file /= (double)APR_USEC_PER_SEC;
        total += got;
        log_printf(5, "sid=" XIDT " fwrite(wb,1," XOT ", sc->fd)=" XOT " total=" XOT "\n", segment_id(sc->src), wlen, got, total);
        if (wlen != got) {
            log_printf(1, "ERROR from fread=%d  dest sid=" XIDT "\n", errno, segment_id(sc->dest));
            status = gop_failure_status;
            gop_waitall(gop);
            gop_free(gop, OP_DESTROY);
            goto fail;
        }

        wpos += wlen;

        //** Wait for the read to complete
        if (rlen > 0) {
            err = gop_waitall(gop);
            gop_free(gop, OP_DESTROY);
            if (err != OP_STATE_SUCCESS) {
                log_printf(1, "ERROR write(dseg=" XIDT ") failed! wpos=" XOT " len=" XOT "\n", segment_id(sc->dest), wpos, wlen);
                status = gop_failure_status;
                goto fail;
            }
        }

        dt_loop = apr_time_now() - loop_start;
        dt_loop /= (double)APR_USEC_PER_SEC;
        log_printf(1, "dt_loop=%lf  dt_file=%lf\n", dt_loop, dt_file);

    } while (rlen > 0);

fail:

    return(status);
}



//***********************************************************************
// segment_get - Reads data from the given segment and copies it to the given FD
//      If len == -1 then all available data from src is copied
//***********************************************************************

gop_op_generic_t *segment_get(gop_thread_pool_context_t *tpc, data_attr_t *da, lio_segment_rw_hints_t *rw_hints, lio_segment_t *src_seg, FILE *fd, ex_off_t src_offset, ex_off_t len, ex_off_t bufsize, char *buffer, int timeout)
{
    lio_segment_copy_t *sc;

    tbx_type_malloc(sc, lio_segment_copy_t, 1);

    sc->da = da;
    sc->rw_hints = rw_hints;
    sc->timeout = timeout;
    sc->fd = fd;
    sc->src = src_seg;
    sc->src_offset = src_offset;
    sc->len = len;
    sc->bufsize = bufsize;
    sc->buffer = buffer;

    return(gop_tp_op_new(tpc, NULL, segment_get_func, (void *)sc, free, 1));
}

//***********************************************************************
// segment_put_func - Does the actual segment put operation
//***********************************************************************

gop_op_status_t segment_put_func(void *arg, int id)
{
    lio_segment_copy_t *sc = (lio_segment_copy_t *)arg;
    tbx_tbuf_t *wbuf, *rbuf, *tmpbuf;
    tbx_tbuf_t tbuf1, tbuf2;
    char *rb, *wb, *tb;
    ex_off_t bufsize;
    int err;
    ex_off_t rpos, wpos, rlen, wlen, tlen, nbytes, got, dend;
    ex_tbx_iovec_t wex;
    gop_op_generic_t *gop;
    gop_op_status_t status;
    apr_time_t loop_start, file_start;
    double dt_loop, dt_file;

    //** Set up the buffers
    bufsize = sc->bufsize / 2;  //** The buffer is split for R/W
    rb = sc->buffer;
    wb = &(sc->buffer[bufsize]);
    tbx_tbuf_single(&tbuf1, bufsize, rb);
    tbx_tbuf_single(&tbuf2, bufsize, wb);
    rbuf = &tbuf1;
    wbuf = &tbuf2;

    nbytes = sc->len;
    status = gop_success_status;

    //** Go ahead and reserve the space in the destintaion
    dend = sc->dest_offset + nbytes;
    gop_sync_exec(lio_segment_truncate(sc->dest, sc->da, -dend, sc->timeout));

    //** Read the initial block
    rpos = 0;
    wpos = sc->dest_offset;
    if (nbytes < 0) {
        rlen = bufsize;
    } else {
        rlen = (nbytes > bufsize) ? bufsize : nbytes;
    }

    wlen = 0;
    rpos += rlen;
    nbytes -= rlen;
    log_printf(0, "FILE fd=%p bufsize=" XOT " rlen=" XOT " nbytes=" XOT "\n", sc->fd, bufsize, rlen, nbytes);

    loop_start = apr_time_now();
    got = fread(rb, 1, rlen, sc->fd);
    dt_file = apr_time_now() - loop_start;
    dt_file /= (double)APR_USEC_PER_SEC;

    if (got == 0) {
        if (feof(sc->fd) == 0)  {
            log_printf(1, "ERROR from fread=%d  dest sid=" XIDT " rlen=" XOT " got=" XOT "\n", errno, segment_id(sc->dest), rlen, got);
            status = gop_failure_status;
        }
        goto finished;
    }
    rlen = got;

    do {
        //** Swap the buffers
        tb = rb;
        rb = wb;
        wb = tb;
        tmpbuf = rbuf;
        rbuf = wbuf;
        wbuf = tmpbuf;
        tlen = rlen;
        rlen = wlen;
        wlen = tlen;

        log_printf(1, "dseg=" XIDT " wpos=" XOT " rlen=" XOT " wlen=" XOT "\n", segment_id(sc->dest), wpos, rlen, wlen);

        //** Start the write
        ex_iovec_single(&wex, wpos, wlen);
        wpos += wlen;
        loop_start = apr_time_now();
        gop = segment_write(sc->dest, sc->da, sc->rw_hints, 1, &wex, wbuf, 0, sc->timeout);
        gop_start_execution(gop);  //** Start doing the transfer

        //** Read in the next block
        if (nbytes < 0) {
            rlen = bufsize;
        } else {
            rlen = (nbytes > bufsize) ? bufsize : nbytes;
        }
        if (rlen > 0) {
            file_start = apr_time_now();
            got = fread(rb, 1, rlen, sc->fd);
            dt_file = apr_time_now() - file_start;
            dt_file /= (double)APR_USEC_PER_SEC;
            if (got == 0) {
                if (feof(sc->fd) == 0)  {
                    log_printf(1, "ERROR from fread=%d  dest sid=" XIDT " got=" XOT " rlen=" XOT "\n", errno, segment_id(sc->dest), got, rlen);
                    status = gop_failure_status;
                    gop_waitall(gop);
                    gop_free(gop, OP_DESTROY);
                    goto finished;
                }
            }
            rlen = got;
            rpos += rlen;
            nbytes -= rlen;
        }

        //** Wait for it to complete
        err = gop_waitall(gop);
        dt_loop = apr_time_now() - loop_start;
        dt_loop /= (double)APR_USEC_PER_SEC;

        log_printf(1, "dt_loop=%lf  dt_file=%lf\n", dt_loop, dt_file);

        if (err != OP_STATE_SUCCESS) {
            log_printf(1, "ERROR write(dseg=" XIDT ") failed! wpos=" XOT " len=" XOT "\n", segment_id(sc->dest), wpos, wlen);
            status = gop_failure_status;
            gop_free(gop, OP_DESTROY);
            goto finished;
        }
        gop_free(gop, OP_DESTROY);
    } while (rlen > 0);

    if (sc->truncate == 1) {  //** Truncate if wanted
        gop_sync_exec(lio_segment_truncate(sc->dest, sc->da, wpos, sc->timeout));
    }

finished:
//  status.error_code = rpos;

    return(status);
}



//***********************************************************************
// segment_put - Stores data from the given FD into the segment.
//      If len == -1 then all available data from src is copied
//***********************************************************************

gop_op_generic_t *segment_put(gop_thread_pool_context_t *tpc, data_attr_t *da, lio_segment_rw_hints_t *rw_hints, FILE *fd, lio_segment_t *dest_seg, ex_off_t dest_offset, ex_off_t len, ex_off_t bufsize, char *buffer, int do_truncate, int timeout)
{
    lio_segment_copy_t *sc;

    tbx_type_malloc(sc, lio_segment_copy_t, 1);

    sc->da = da;
    sc->rw_hints = rw_hints;
    sc->timeout = timeout;
    sc->fd = fd;
    sc->dest = dest_seg;
    sc->dest_offset = dest_offset;
    sc->len = len;
    sc->bufsize = bufsize;
    sc->buffer = buffer;
    sc->truncate = do_truncate;

    return(gop_tp_op_new(tpc, NULL, segment_put_func, (void *)sc, free, 1));
}

