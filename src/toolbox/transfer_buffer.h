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
