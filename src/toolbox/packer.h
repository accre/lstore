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

#include "tbx/toolbox_visibility.h"
#include "zlib.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tbx_pack_raw_t tbx_pack_raw_t;
struct tbx_pack_raw_t {
    unsigned char *buffer;
    unsigned int bufsize;
    unsigned int bpos;
    int nleft;
};

typedef struct tbx_pack_zlib_t tbx_pack_zlib_t;
struct tbx_pack_zlib_t {
    unsigned char *buffer;
    unsigned int bufsize;
    unsigned int bpos;
    z_stream z;
};

typedef struct tbx_pack_t tbx_pack_t;
struct tbx_pack_t {
    int type;
    int mode;
    union {
        tbx_pack_raw_t raw;
        tbx_pack_zlib_t zlib;
    };
    void (*end)(tbx_pack_t *pack);
    void (*write_resized)(tbx_pack_t *pack, unsigned char *buffer, unsigned int bufsize);
    int  (*write)(tbx_pack_t *pack, unsigned char *data, int nbytes);
    int (*read_new_data)(tbx_pack_t *pack, unsigned char *buffer, unsigned int bufsize);
    int  (*read)(tbx_pack_t *pack, unsigned char *data, int nbytes);
    int  (*used)(tbx_pack_t *pack);
    void (*consumed)(tbx_pack_t *pack);
    int (*write_flush)(tbx_pack_t *pack);
};

#define PACK_FINISHED -3
#define PACK_ERROR   -2
#define PACK_FULL    -1
#define PACK_NONE     0
#define PACK_COMPRESS 1
#define PACK_READ     0
#define PACK_WRITE    1

#define pack_end(p) (p)->end(p)

#define pack_write_resized(p, b, len) (p)->write_resized(p, b, len)
#define pack_write(p, b, len) (p)->write(p, b, len)

#define pack_read_new_data(p, b, len) (p)->read_new_data(p, b, len)
#define pack_read(p, b, len) (p)->read(p, b, len)

#define pack_used(p) (p)->used(p)
#define pack_consumed(p) (p)->consumed(p)
#define pack_write_flush(p) (p)->write_flush(p)

void pack_init(tbx_pack_t *pack, int type, int mode, unsigned char *buffer, unsigned int bufsize);
TBX_API tbx_pack_t *pack_create(int type, int mode, unsigned char *buffer, unsigned int bufsize);
TBX_API void pack_destroy(tbx_pack_t *pack);

//void pack_end(tbx_pack_t *pack);
//void pack_write_resized(tbx_pack_t *pack, char *buffer, int bufsize);
//int pack_write(tbx_pack_t *pack, unsigned char *data, int nbytes);

//void pack_read_new_data(tbx_pack_t *pack, char *buffer, int bufsize);
//int pack_read(tbx_pack_t *pack, unsigned char *data, int nbytes);
//int pack_used(tbx_pack_t *pack);
//int pack_consumed(tbx_pack_t *pack);

#ifdef __cplusplus
}
#endif

#endif

