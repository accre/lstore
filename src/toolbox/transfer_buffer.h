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

#include <stddef.h>

#include "tbx/transfer_buffer.h"

#ifdef __cplusplus
extern "C" {
#endif

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
