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
// Pack/Unpack header file
//***********************************************************************

#ifndef _PACKER_H_
#define _PACKER_H_

#include "zlib.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    unsigned char *buffer;
    int bufsize;
    int bpos;
    int nleft;
} pack_raw_t;

typedef struct {
    unsigned char *buffer;
    int bufsize;
    int bpos;
    z_stream z;
} pack_zlib_t;

typedef struct pack_s pack_t;

struct pack_s {
    int type;
    int mode;
    union {
        pack_raw_t raw;
        pack_zlib_t zlib;
    };
    void (*end)(pack_t *pack);
    void (*write_resized)(pack_t *pack, unsigned char *buffer, int bufsize);
    int  (*write)(pack_t *pack, unsigned char *data, int nbytes);
    int (*read_new_data)(pack_t *pack, unsigned char *buffer, int bufsize);
    int  (*read)(pack_t *pack, unsigned char *data, int nbytes);
    int  (*used)(pack_t *pack);
    void (*consumed)(pack_t *pack);
    int (*write_flush)(pack_t *pack);
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

void pack_init(pack_t *pack, int type, int mode, unsigned char *buffer, int bufsize);
pack_t *pack_create(int type, int mode, unsigned char *buffer, int bufsize);
void pack_destroy(pack_t *pack);

//void pack_end(pack_t *pack);
//void pack_write_resized(pack_t *pack, char *buffer, int bufsize);
//int pack_write(pack_t *pack, unsigned char *data, int nbytes);

//void pack_read_new_data(pack_t *pack, char *buffer, int bufsize);
//int pack_read(pack_t *pack, unsigned char *data, int nbytes);
//int pack_used(pack_t *pack);
//int pack_consumed(pack_t *pack);

#ifdef __cplusplus
}
#endif

#endif

