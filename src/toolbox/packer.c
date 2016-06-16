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
//  Implementes the pack/unpack routines that support compression and
//  straight pass thru.
//***********************************************************************

#define _log_module_index 224

#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <zlib.h>

#include "packer.h"
#include "tbx/assert_result.h"
#include "tbx/log.h"
#include "tbx/type_malloc.h"

// Accessors
int tbx_pack_read_new_data(tbx_pack_t *pack, unsigned char *buffer, unsigned int bufsize) {
    return pack->read_new_data(pack, buffer, bufsize);
}
int tbx_pack_read(tbx_pack_t *pack, unsigned char *data, int nbytes) {
    return pack->read(pack, data, nbytes);
}
int tbx_pack_used(tbx_pack_t *pack) {
    return pack->used(pack);
}
void tbx_pack_consumed(tbx_pack_t *pack) {
    pack->consumed(pack);
}
void tbx_pack_write_resized(tbx_pack_t *pack, unsigned char *buffer, unsigned int bufsize) {
    pack->write_resized(pack, buffer, bufsize);
}
int tbx_pack_write(tbx_pack_t *pack, unsigned char *data, int nbytes) {
    return pack->write(pack, data, nbytes);
}
int tbx_pack_write_flush(tbx_pack_t *pack) {
    return pack->write_flush(pack);
}
//***********************************************************************
//-------------------- Compressed routines ------------------------------
//***********************************************************************

//***********************************************************************
// pack_read_zlib - Retreives data from the buffer and returns the number
//    of bytes retreived.
//***********************************************************************

int pack_read_zlib(tbx_pack_t *pack, unsigned char *data, int len)
{
    tbx_pack_zlib_t *p = &(pack->data.zlib);
    int nbytes;

    p->z.avail_out = len;
    p->z.next_out = data;

    log_printf(15, "START z.avail_out=%d z.avail_in=%d len=%d\n", p->z.avail_out, p->z.avail_in, len);

    if (len == 0) return(0);

    nbytes = inflate(&(p->z), Z_NO_FLUSH);
    log_printf(15, "inflate=%d\n", nbytes);

    if ((nbytes == Z_OK) ||
            (nbytes == Z_STREAM_END) ||
            (nbytes == Z_BUF_ERROR)) {
        nbytes = len - p->z.avail_out;
    } else {
        nbytes = PACK_ERROR;
    }
    log_printf(15, "END z.avail_out=%d z.avail_in=%d len=%d nbytes=%d\n", p->z.avail_out, p->z.avail_in, len, nbytes);
    return(nbytes);
}

//***********************************************************************
// pack_read_new_data_zlib - Replaces the current data array with that
//    provided.  The old data array should have been completely consumed!
//***********************************************************************

int pack_read_new_data_zlib(tbx_pack_t *pack, unsigned char *buffer, unsigned int bufsize)
{
    tbx_pack_zlib_t *p = &(pack->data.zlib);
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

int pack_write_zlib(tbx_pack_t *pack, unsigned char *data, int len)
{
    tbx_pack_zlib_t *p = &(pack->data.zlib);
    int nbytes;

    if (len == 0) return(0);

    p->z.avail_in = len;
    p->z.next_in = data;
    nbytes = deflate(&(p->z), Z_NO_FLUSH);
    if ((nbytes == Z_OK) || (nbytes == Z_BUF_ERROR)) {
        nbytes = len - p->z.avail_in;
    } else {
        nbytes = PACK_ERROR;
    }

    return(nbytes);
}

//***********************************************************************
// pack_write_resized_zlib - Replaces the re-allocated data array with the
//    expanded one.  The old data should have been copied to the new array
//***********************************************************************

void pack_write_resized_zlib(tbx_pack_t *pack, unsigned char *buffer, unsigned int bufsize)
{
    tbx_pack_zlib_t *p = &(pack->data.zlib);
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

void pack_consumed_zlib(tbx_pack_t *pack)
{
    tbx_pack_zlib_t *p = &(pack->data.zlib);

    p->z.avail_out = p->bufsize;
    p->z.next_out = p->buffer;
}

//***********************************************************************
// pack_end_zlib - Cleans up the ZLIB pack structure
//***********************************************************************

void pack_end_zlib(tbx_pack_t *pack)
{
    tbx_pack_zlib_t *p = &(pack->data.zlib);

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

int pack_used_zlib(tbx_pack_t *pack)
{
    tbx_pack_zlib_t *p = &(pack->data.zlib);

    return(p->bufsize - p->z.avail_out);
}

//***********************************************************************
// pack_write_flush_zlib - Flushes the ZLIB buffer.  Depending on space
//    available this routine may need to be called multiple times.
//***********************************************************************

int pack_write_flush_zlib(tbx_pack_t *pack)
{
    tbx_pack_zlib_t *p = &(pack->data.zlib);
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

void pack_init_zlib(tbx_pack_t *pack, int type, int mode, unsigned char *buffer, unsigned int bufsize)
{
    tbx_pack_zlib_t *p = &(pack->data.zlib);

    memset(pack, 0, sizeof(tbx_pack_t));

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
        assert_result(inflateInit(&(p->z)), Z_OK);
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
        assert_result(deflateInit(&(p->z), Z_DEFAULT_COMPRESSION), Z_OK);
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

int pack_read_raw(tbx_pack_t *pack, unsigned char *data, int len)
{
    tbx_pack_raw_t *p = &(pack->data.raw);
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

int pack_read_new_data_raw(tbx_pack_t *pack, unsigned char *buffer, unsigned int bufsize)
{
    tbx_pack_raw_t *p = &(pack->data.raw);
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

int pack_write_raw(tbx_pack_t *pack, unsigned char *data, int len)
{
    tbx_pack_raw_t *p = &(pack->data.raw);
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

void pack_write_resized_raw(tbx_pack_t *pack, unsigned char *buffer, unsigned int bufsize)
{
    tbx_pack_raw_t *p = &(pack->data.raw);

    assert(bufsize >= p->bpos);

    p->buffer = buffer;
    p->bufsize = bufsize;
    p->nleft = bufsize - p->bpos;
}

//***********************************************************************
// pack_consumed_raw - Flags the write data as consumed.  Resetting the buffer
//***********************************************************************

void pack_consumed_raw(tbx_pack_t *pack)
{
    tbx_pack_raw_t *p = &(pack->data.raw);

    p->bpos = 0;
    p->nleft = p->bufsize;
}

//***********************************************************************
// pack_end_raw - Cleans up the RAW pack structure
//***********************************************************************

void pack_end_raw(tbx_pack_t *pack)
{
    return;
}

//***********************************************************************
// pack_used_raw - Returns the number of buffer bytes used
//***********************************************************************

int pack_used_raw(tbx_pack_t *pack)
{
    return(pack->data.raw.bpos);
}

//***********************************************************************
// pack_write_flush_raw - Flushes the write buffer
//***********************************************************************

int pack_write_flush_raw(tbx_pack_t *pack)
{
    return(PACK_FINISHED);
}

//***********************************************************************
// pack_init_raw - Initializes a RAW pack object
//***********************************************************************

void pack_init_raw(tbx_pack_t *pack, int type, int mode, unsigned char *buffer, unsigned int bufsize)
{
    tbx_pack_raw_t *p = &(pack->data.raw);

    memset(pack, 0, sizeof(tbx_pack_t));

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

void tbx_pack_destroy(tbx_pack_t *pack)
{
    log_printf(15, "type=%d mode=%d\n", pack->type, pack->mode);
    tbx_pack_end(pack);
    free(pack);
}

//***********************************************************************
// pack_init - Initializes a pack structure
//***********************************************************************

void pack_init(tbx_pack_t *pack, int type, int mode, unsigned char *buffer, unsigned int bufsize)
{
    if (type == PACK_COMPRESS) {
        pack_init_zlib(pack, type, mode, buffer, bufsize);
    } else {
        pack_init_raw(pack, type, mode, buffer, bufsize);
    }
}


//***********************************************************************
// pack_create - Creates a new pack structure and initializes it
//***********************************************************************

tbx_pack_t *tbx_pack_create(int type, int mode, unsigned char *buffer, unsigned int bufsize)
{
    tbx_pack_t *pack;

    log_printf(15, "type=%d mode=%d\n", type, mode);

    tbx_type_malloc(pack, tbx_pack_t, 1);
    pack_init(pack, type, mode, buffer, bufsize);
    return(pack);
}

