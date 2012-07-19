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

#include <sys/uio.h>


#define TBUFFER_OK 1
#define TBUFFER_OUTOFSPACE 2

typedef struct iovec iovec_t;

typedef struct {
  size_t total_bytes;
  int n;
  iovec_t  *iov;
  iovec_t  io_single;
} tbinfo_t;

typedef struct {
  int    curr_slot;
  int    slot_total_pos;
  iovec_t single;
} tbuf_state_t;

typedef struct {
  size_t nbytes;
  iovec_t *buffer;
  int    n_iov;
  tbuf_state_t priv;
} tbuffer_var_t;


struct tbuffer_s;
typedef struct tbuffer_s tbuffer_t;
struct tbuffer_s {
  void *arg;
//  int (*next_block)(tbuffer_t *tb, size_t off, size_t *nbytes, int *n, iovec_t **buffer);
  int (*next_block)(tbuffer_t *tb, size_t off, tbuffer_var_t *tbv);
  tbinfo_t buf;
};

#define tbuffer_var_init(tbv) memset((tbv), 0, sizeof(tbuffer_var_t))


tbuffer_var_t *tbuffer_var_create();
void tbuffer_var_destroy(tbuffer_var_t *tbv);
tbuffer_t *tbuffer_create();
void tbuffer_destroy(tbuffer_t *tb);

void tbuffer_single(tbuffer_t *tb, size_t nbytes, char *buffer);
void tbuffer_vec(tbuffer_t *tb, size_t total_bytes, size_t n_vec, iovec_t *iov);
void tbuffer_fn(tbuffer_t *tb, size_t total_bytes, void *arg, int (*next_block)(tbuffer_t *tb, size_t pos, tbuffer_var_t *tbv));

size_t tbuffer_size(tbuffer_t *tb);
int tbuffer_copy(tbuffer_t *tb_s, size_t off_s, tbuffer_t *tb_d, size_t off_d, size_t nbytes);
int tbuffer_memset(tbuffer_t *buffer, size_t boff, int c, size_t nbytes);

#define tbuffer_next(tb, pos, tbv) (tb)->next_block(tb, pos, tbv)

//** Testing routines
int tbuffer_test();


#ifdef __cplusplus
}
#endif

#endif
