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
// Pack/Unpack header file
//***********************************************************************

#ifndef _PACKER_H_
#define _PACKER_H_

#include <zlib.h>

#include "tbx/packer.h"

#ifdef __cplusplus
id (*write_resized)(tbx_pack_t *pack, unsigned char *buffer, unsigned int bufsize);

extern "C" {
#endif

struct tbx_pack_raw_t {
    unsigned char *buffer;
    unsigned int bufsize;
    unsigned int bpos;
    int nleft;
};

struct tbx_pack_zlib_t {
    unsigned char *buffer;
    unsigned int bufsize;
    unsigned int bpos;
    z_stream z;
};

struct tbx_pack_t {
    int type;
    int mode;
    union {
        tbx_pack_raw_t raw;
        tbx_pack_zlib_t zlib;
    } data;
    void (*end)(tbx_pack_t *pack);
    void (*write_resized)(tbx_pack_t *pack, unsigned char *buffer, unsigned int bufsize);
    int  (*write)(tbx_pack_t *pack, unsigned char *data, int nbytes);
    int (*read_new_data)(tbx_pack_t *pack, unsigned char *buffer, unsigned int bufsize);
    int  (*read)(tbx_pack_t *pack, unsigned char *data, int nbytes);
    int  (*used)(tbx_pack_t *pack);
    void (*consumed)(tbx_pack_t *pack);
    int (*write_flush)(tbx_pack_t *pack);
};
void pack_init(tbx_pack_t *pack, int type, int mode, unsigned char *buffer, unsigned int bufsize);

//void tbx_pack_end(tbx_pack_t *pack);
//void tbx_pack_write_resized(tbx_pack_t *pack, char *buffer, int bufsize);
//int tbx_pack_write(tbx_pack_t *pack, unsigned char *data, int nbytes);

//void tbx_pack_read_new_data(tbx_pack_t *pack, char *buffer, int bufsize);
//int tbx_pack_read(tbx_pack_t *pack, unsigned char *data, int nbytes);
//int tbx_pack_used(tbx_pack_t *pack);
//int tbx_pack_consumed(tbx_pack_t *pack);

#ifdef __cplusplus
}
#endif

#endif

