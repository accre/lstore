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

#pragma once
#ifndef ACCRE_TRANSFER_BUFFER_H_INCLUDED
#define ACCRE_TRANSFER_BUFFER_H_INCLUDED

#include <sys/uio.h>
#include <tbx/visibility.h>

#ifdef __cplusplus
extern "C" {
#endif

// Types
typedef struct iovec tbx_iovec_t;

typedef struct tbx_tbuf_info_t tbx_tbuf_info_t;

typedef struct tbx_tbuf_state_t tbx_tbuf_state_t;

typedef struct tbx_tbuf_t tbx_tbuf_t;

typedef struct tbx_tbuf_var_t tbx_tbuf_var_t;
typedef enum tbx_tbuf_ret_t tbx_tbuf_ret_t;
enum tbx_tbuf_ret_t {
    TBUFFER_OK = 1,
    TBUFFER_OUTOFSPACE = 2,
};
// Functions
TBX_API tbx_iovec_t * tbx_tbuf_var_buffer_get(tbx_tbuf_var_t *tbv);
TBX_API int tbx_tbuf_copy(tbx_tbuf_t *tb_s, size_t off_s, tbx_tbuf_t *tb_d,
                            size_t off_d, size_t nbytes, int blank_missing);
TBX_API void tbx_tbuf_fn(tbx_tbuf_t *tb, size_t total_bytes, void *arg,
                            int (*next_block)(tbx_tbuf_t *tb,
                                                size_t pos,
                                                tbx_tbuf_var_t *tbv));
TBX_API int tbx_tbuf_memset(tbx_tbuf_t *tb, size_t off, int c, size_t nbytes);
TBX_API int tbx_tbuf_next(tbx_tbuf_t *tb, size_t off, tbx_tbuf_var_t *tbv);
TBX_API int tbx_tbuf_next_block(tbx_tbuf_t *tb,
                                size_t off,
                                tbx_tbuf_var_t *tbv);
TBX_API void tbx_tbuf_single(tbx_tbuf_t *tb, size_t nbytes, char *buffer);
TBX_API size_t tbx_tbuf_size(tbx_tbuf_t *tb);
TBX_API int tbx_tbuf_test();
TBX_API int tbx_tbuf_var_n_iov_get(tbx_tbuf_var_t *tbv);
TBX_API void tbx_tbuf_var_nbytes_set(tbx_tbuf_var_t *tbv, size_t nbytes);
TBX_API size_t tbx_tbuf_var_size();
TBX_API void tbx_tbuf_vec(tbx_tbuf_t *tb,
                            size_t total_bytes,
                            size_t n_vec,
                            tbx_iovec_t *iov);
TBX_API tbx_tbuf_var_t *tbx_tbuf_var_create();
TBX_API void tbx_tbuf_var_destroy(tbx_tbuf_var_t *tbv);
TBX_API tbx_tbuf_t *tbx_tbuf_create();
TBX_API void tbx_tbuf_destroy(tbx_tbuf_t *tb);


#define tbx_tbuf_var_init(tbv) memset((tbv), 0, tbx_tbuf_var_size())
struct tbx_tbuf_info_t {
    size_t total_bytes;
    int n;
    tbx_iovec_t  *iov;
    tbx_iovec_t  io_single;
};

struct tbx_tbuf_state_t {
    int    curr_slot;
    int    slot_total_pos;
    tbx_iovec_t single;
};

struct tbx_tbuf_var_t {
    size_t nbytes;
    tbx_iovec_t *buffer;
    int    n_iov;
    tbx_tbuf_state_t priv;
};

struct tbx_tbuf_t {
    void *arg;
    int (*next_block)(tbx_tbuf_t *tb, size_t off, tbx_tbuf_var_t *tbv);
    tbx_tbuf_info_t buf;
};

#ifdef __cplusplus
}
#endif

#endif
