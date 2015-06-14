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

//***********************************************************************
//  Implementes the pack/unpack routines that support compression and
//  straight pass thru.
//***********************************************************************

#define _log_module_index 224

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "packer.h"
#include "type_malloc.h"
#include "log.h"
//#include "random.h"

//***********************************************************************
//-------------------- Compressed routines ------------------------------
//***********************************************************************

//***********************************************************************
// pack_read_zlib - Retreives data from the buffer and returns the number
//    of bytes retreived.
//***********************************************************************

int pack_read_zlib(pack_t *pack, unsigned char *data, int len) {
    pack_zlib_t *p = &(pack->zlib);
    int nbytes;

    p->z.avail_out = len;
    p->z.next_out = data;

    log_printf(15, "START z.avail_out=%d z.avail_in=%d len=%d\n", p->z.avail_out, p->z.avail_in, len);

    if (len == 0) return(0);

    nbytes = inflate(&(p->z), Z_NO_FLUSH);
    log_printf(15, "inflate=%d\n", nbytes);

    nbytes = ((nbytes == Z_OK) || (nbytes == Z_STREAM_END) || (nbytes == Z_BUF_ERROR)) ? len - p->z.avail_out : PACK_ERROR;

    log_printf(15, "END z.avail_out=%d z.avail_in=%d len=%d nbytes=%d\n", p->z.avail_out, p->z.avail_in, len, nbytes);
    return(nbytes);
}

//***********************************************************************
// pack_read_new_data_zlib - Replaces the current data array with that
//    provided.  The old data array should have been completely consumed!
//***********************************************************************

int pack_read_new_data_zlib(pack_t *pack, unsigned char *buffer, int bufsize) {
    pack_zlib_t *p = &(pack->zlib);
    int err = 0;

    if (p->z.avail_in > 0) err = PACK_ERROR;

    p->z.avail_in = bufsize;
    p->z.next_in = buffer;

    p->buffer = buffer;
    p->bufsize = bufsize;

    return(err);
}

//***********************************************************************
// pack_write_zlib - Stores data in the buffer and the number
//    of bytes retreived.
//***********************************************************************

int pack_write_zlib(pack_t *pack, unsigned char *data, int len) {
    pack_zlib_t *p = &(pack->zlib);
    int nbytes;

    if (len == 0) return(0);

    p->z.avail_in = len;
    p->z.next_in = data;
    nbytes = deflate(&(p->z), Z_NO_FLUSH);
//log_printf(5, "deflate_error=%d len=%d avail_in=%d pack_used=%d\n", nbytes, len, p->z.avail_in, pack_used(pack));
//double r = random_double(0, 1);
//if (r > 0.25) { log_printf(0, "FORCING PACK_ERROR\n"); return(PACK_ERROR); }

    nbytes = ((nbytes == Z_OK) || (nbytes == Z_BUF_ERROR)) ? len - p->z.avail_in : PACK_ERROR;

    return(nbytes);
}

//***********************************************************************
// pack_write_resized_zlib - Replaces the re-allocated data array with the
//    expanded one.  The old data should have been copied to the new array
//***********************************************************************

void pack_write_resized_zlib(pack_t *pack, unsigned char *buffer, int bufsize) {
    pack_zlib_t *p = &(pack->zlib);
    int offset;

    assert(bufsize >= (p->bufsize - p->z.avail_out));

    offset = p->bufsize - p->z.avail_out;
    p->z.next_out = &(buffer[offset]);
    p->z.avail_out = bufsize - offset;

    p->buffer = buffer;
    p->bufsize = bufsize;
}

//***********************************************************************
// pack_consumed_zlib - Flags the write data as consumed.  Resetting the buffer
//***********************************************************************

void pack_consumed_zlib(pack_t *pack) {
    pack_zlib_t *p = &(pack->zlib);

    p->z.avail_out = p->bufsize;
    p->z.next_out = p->buffer;
}

//***********************************************************************
// pack_end_zlib - Cleans up the ZLIB pack structure
//***********************************************************************

void pack_end_zlib(pack_t *pack) {
    pack_zlib_t *p = &(pack->zlib);

    if (pack->mode == PACK_READ) {
        inflateEnd(&(p->z));
    } else {
        deflateEnd(&(p->z));
    }

    return;
}

//***********************************************************************
// pack_used_zlib - Returns the number of buffer bytes used
//***********************************************************************

int pack_used_zlib(pack_t *pack) {
    pack_zlib_t *p = &(pack->zlib);

    return(p->bufsize - p->z.avail_out);
}

//***********************************************************************
// pack_write_flush_zlib - Flushes the ZLIB buffer.  Depending on space
//    available this routine may need to be called multiple times.
//***********************************************************************

int pack_write_flush_zlib(pack_t *pack) {
    pack_zlib_t *p = &(pack->zlib);
    int err;

    log_printf(5, "start avail_out=%d\n", p->z.avail_out);
    p->z.avail_in = 0;
    p->z.next_in = Z_NULL;
    err = deflate(&(p->z), Z_FINISH);
    log_printf(5, "end avail_out=%d err=%d\n", p->z.avail_out, err);

    if ((err == Z_OK) || (err == Z_BUF_ERROR)) {
        err = PACK_NONE;
    } else if (err == Z_STREAM_END) {
        err = PACK_FINISHED;
    } else {
        err = PACK_ERROR;
    }

    log_printf(5, "translated err=%d\n", err);
    return(err);
}

//***********************************************************************
// pack_init_zlib - Initializes a ZLIB pack object
//***********************************************************************

void pack_init_zlib(pack_t *pack, int type, int mode, unsigned char *buffer, int bufsize) {
    pack_zlib_t *p = &(pack->zlib);

    memset(pack, 0, sizeof(pack_t));

    pack->type = type;
    pack->mode = mode;
    p->buffer = buffer;
    p->bufsize = bufsize;
    p->bpos = 0;

    pack->end = pack_end_zlib;

    if (mode == PACK_READ) {
        p->z.zalloc = Z_NULL;
        p->z.zfree = Z_NULL;
        p->z.opaque = Z_NULL;
        assert(inflateInit(&(p->z)) == Z_OK);
        p->z.avail_in = bufsize;
        p->z.next_in = buffer;
        p->z.avail_out = 0;
        p->z.next_out = Z_NULL;;
        pack->read_new_data = pack_read_new_data_zlib;
        pack->read = pack_read_zlib;
        pack->used = pack_used_zlib;
    } else {
        p->z.zalloc = Z_NULL;
        p->z.zfree = Z_NULL;
        p->z.opaque = Z_NULL;
        assert(deflateInit(&(p->z), Z_DEFAULT_COMPRESSION) == Z_OK);
        p->z.avail_in = 0;
        p->z.next_in = Z_NULL;
        p->z.avail_out = bufsize;
        p->z.next_out = buffer;
        pack->write_resized = pack_write_resized_zlib;
        pack->write = pack_write_zlib;
        pack->used = pack_used_zlib;
        pack->consumed = pack_consumed_zlib;
        pack->write_flush = pack_write_flush_zlib;
    }
}


//***********************************************************************
//-------------------------- Raw routines -------------------------------
//***********************************************************************

//***********************************************************************
// pack_read_raw - Returns data from the buffer and returns the number
//    of bytes stored.
//***********************************************************************

int pack_read_raw(pack_t *pack, unsigned char *data, int len) {
    pack_raw_t *p = &(pack->raw);
    int nbytes;

    nbytes = (len > p->nleft) ? p->nleft : len;

    if (nbytes == 0) return(0);

    memcpy(data, &(p->buffer[p->bpos]), nbytes);

    p->nleft -= nbytes;
    p->bpos  += nbytes;

    return(nbytes);
}


//***********************************************************************
// pack_read_new_data_raw - Replaces the current data array with that
//    provided.  The old data array should have been completely consumed!
//***********************************************************************

int pack_read_new_data_raw(pack_t *pack, unsigned char *buffer, int bufsize) {
    pack_raw_t *p = &(pack->raw);
    int err = 0;

    if (p->nleft > 0) err = PACK_ERROR;

    p->buffer = buffer;
    p->bufsize = bufsize;

    p->bpos = 0;
    p->nleft = p->bufsize;

    return(err);
}

//***********************************************************************
// pack_write_raw - Stores data in the buffer and returns the number
//    of bytes stored.  IF the buffer is full or no more data can be
//    stored PACK_FULL is returned.
//***********************************************************************

int pack_write_raw(pack_t *pack, unsigned char *data, int len) {
    pack_raw_t *p = &(pack->raw);
    int nbytes;

    nbytes = (len > p->nleft) ? p->nleft : len;

    if (nbytes == 0) return(PACK_FULL);

    memcpy(&(p->buffer[p->bpos]), data, nbytes);

    p->nleft -= nbytes;
    p->bpos  += nbytes;

    return(nbytes);
}


//***********************************************************************
// pack_write_resized_raw - Replaces the re-allocated data array with the
//    expanded one.  The old data should have been copied to the new array
//***********************************************************************

void pack_write_resized_raw(pack_t *pack, unsigned char *buffer, int bufsize) {
    pack_raw_t *p = &(pack->raw);

    assert(bufsize >= p->bpos);

    p->buffer = buffer;
    p->bufsize = bufsize;
    p->nleft = bufsize - p->bpos;
}

//***********************************************************************
// pack_consumed_raw - Flags the write data as consumed.  Resetting the buffer
//***********************************************************************

void pack_consumed_raw(pack_t *pack) {
    pack_raw_t *p = &(pack->raw);

    p->bpos = 0;
    p->nleft = p->bufsize;
}

//***********************************************************************
// pack_end_raw - Cleans up the RAW pack structure
//***********************************************************************

void pack_end_raw(pack_t *pack) {
    return;
}

//***********************************************************************
// pack_used_raw - Returns the number of buffer bytes used
//***********************************************************************

int pack_used_raw(pack_t *pack) {
    return(pack->raw.bpos);
}

//***********************************************************************
// pack_write_flush_raw - Flushes the write buffer
//***********************************************************************

int pack_write_flush_raw(pack_t *pack) {
    return(PACK_FINISHED);
}

//***********************************************************************
// pack_init_raw - Initializes a RAW pack object
//***********************************************************************

void pack_init_raw(pack_t *pack, int type, int mode, unsigned char *buffer, int bufsize) {
    pack_raw_t *p = &(pack->raw);

    memset(pack, 0, sizeof(pack_t));

    pack->type = type;
    pack->mode = mode;
    p->buffer = buffer;
    p->bufsize = bufsize;
    p->bpos = 0;
    p->nleft = bufsize;

    pack->end = pack_end_raw;

    if (mode == PACK_READ) {
        pack->read_new_data = pack_read_new_data_raw;
        pack->read = pack_read_raw;
        pack->used = pack_used_raw;
    } else {
        pack->write_resized = pack_write_resized_raw;
        pack->write = pack_write_raw;
        pack->used = pack_used_raw;
        pack->consumed = pack_consumed_raw;
        pack->write_flush = pack_write_flush_raw;
    }
}

//***********************************************************************
//----------------------- Combined routines -----------------------------
//***********************************************************************

//***********************************************************************
// pack_destroy - Destroy's a pack structure
//***********************************************************************

void pack_destroy(pack_t *pack) {
    log_printf(15, "type=%d mode=%d\n", pack->type, pack->mode);
    pack_end(pack);
    free(pack);
}

//***********************************************************************
// pack_init - Initializes a pack structure
//***********************************************************************

void pack_init(pack_t *pack, int type, int mode, unsigned char *buffer, int bufsize) {
    if (type == PACK_COMPRESS) {
        pack_init_zlib(pack, type, mode, buffer, bufsize);
    } else {
        pack_init_raw(pack, type, mode, buffer, bufsize);
    }
}


//***********************************************************************
// pack_create - Creates a new pack structure and initializes it
//***********************************************************************

pack_t *pack_create(int type, int mode, unsigned char *buffer, int bufsize) {
    pack_t *pack;

    log_printf(15, "type=%d mode=%d\n", type, mode);

    type_malloc(pack, pack_t, 1);
    pack_init(pack, type, mode, buffer, bufsize);
    return(pack);
}

