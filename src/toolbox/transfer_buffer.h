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

#ifndef _TRANSFER_BUFFER_H_
#define _TRANSFER_BUFFER_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "tbx/toolbox_visibility.h"
#include <sys/uio.h>


#define TBUFFER_OK 1
#define TBUFFER_OUTOFSPACE 2

typedef struct iovec tbx_iovec_t;

typedef struct tbx_tbuf_info_t tbx_tbuf_info_t;
struct tbx_tbuf_info_t {
    size_t total_bytes;
    int n;
    tbx_iovec_t  *iov;
    tbx_iovec_t  io_single;
};

typedef struct tbx_tbuf_state_t tbx_tbuf_state_t;
struct tbx_tbuf_state_t {
    int    curr_slot;
    int    slot_total_pos;
    tbx_iovec_t single;
};

typedef struct tbx_tbuf_var_t tbx_tbuf_var_t;
struct tbx_tbuf_var_t {
    size_t nbytes;
    tbx_iovec_t *buffer;
    int    n_iov;
    tbx_tbuf_state_t priv;
};

typedef struct tbx_tbuf_t tbx_tbuf_t;
struct tbx_tbuf_t {
    void *arg;
    int (*next_block)(tbx_tbuf_t *tb, size_t off, tbx_tbuf_var_t *tbv);
    tbx_tbuf_info_t buf;
};

#define tbuffer_var_init(tbv) memset((tbv), 0, sizeof(tbx_tbuf_var_t))


tbx_tbuf_var_t *tbuffer_var_create();
void tbuffer_var_destroy(tbx_tbuf_var_t *tbv);
tbx_tbuf_t *tbuffer_create();
void tbuffer_destroy(tbx_tbuf_t *tb);

TBX_API void tbuffer_single(tbx_tbuf_t *tb, size_t nbytes, char *buffer);
TBX_API void tbuffer_vec(tbx_tbuf_t *tb, size_t total_bytes, size_t n_vec, tbx_iovec_t *iov);
TBX_API void tbuffer_fn(tbx_tbuf_t *tb, size_t total_bytes, void *arg, int (*next_block)(tbx_tbuf_t *tb, size_t pos, tbx_tbuf_var_t *tbv));

TBX_API size_t tbuffer_size(tbx_tbuf_t *tb);
TBX_API int tbuffer_copy(tbx_tbuf_t *tb_s, size_t off_s, tbx_tbuf_t *tb_d, size_t off_d, size_t nbytes, int blank_missing);
TBX_API int tbuffer_memset(tbx_tbuf_t *buffer, size_t boff, int c, size_t nbytes);

#define tbuffer_next(tb, pos, tbv) (tb)->next_block(tb, pos, tbv)

//** Testing routines
TBX_API int tbx_tbuf_test();


#ifdef __cplusplus
}
#endif

#endif
