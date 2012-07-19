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

#define _log_module_index 165

#include <stdio.h>
#include <string.h>
#include "transfer_buffer.h"
#include "log.h"
#include "type_malloc.h"
#include "random.h"

//*************************************************************
// tbuffer_var_create - Creates a transfer buffer VARIABLE containing
//     tbuffer state information
//*************************************************************

tbuffer_var_t *tbuffer_var_create()
{
  tbuffer_var_t *tbv;

  type_malloc(tbv, tbuffer_var_t, 1);
  tbuffer_var_init(tbv);

  return(tbv);
}

//*************************************************************
// tbuffer_var_destroy - Destroys a previously created transfer buffer
//    VARIABLE
//*************************************************************

void tbuffer_var_destroy(tbuffer_var_t *tbv)
{
  free(tbv);
}

//*************************************************************
// tbuffer_create - Creates a transfer buffer
//*************************************************************

tbuffer_t *tbuffer_create()
{
  tbuffer_t *tb;

  type_malloc_clear(tb, tbuffer_t, 1);

  return(tb);
}

//*************************************************************
// tbuffer_destroy - Destroys a previously created transfer buffer
//*************************************************************

void tbuffer_destroy(tbuffer_t *tb)
{
  free(tb);
}

//*************************************************************
//  tbuf_next_block - Default routine for buffer transfers R/W data
//*************************************************************

int tb_next_block(tbuffer_t *tb, size_t pos, tbuffer_var_t *tbv)
{
  tbinfo_t   *ti = &(tb->buf);
  iovec_t    *v = ti->iov;
  int i, slot;
  size_t sum, ds, len, endpos;
//int n1, n2;
//int dn;

  if (pos >= ti->total_bytes) {
     i = pos; slot = ti->total_bytes;
     log_printf(15, "ERROR pos=%d >= ti->total_bytes=%d\n", i, slot);
     return(TBUFFER_OUTOFSPACE);
  }
  endpos = pos + tbv->nbytes - 1;

  //** Update our position in the total table;
  sum = tbv->priv.slot_total_pos;
  if (pos < sum) { sum = 0; tbv->priv.curr_slot = 0; }
//log_printf(15, "tb_next_block: start slot=%d total_slots=%d iov[0]=%p\n", tbv->priv.curr_slot, ti->n, v);
//flush_log();

//dn = v[0].iov_len;
//log_printf(15, "tb_next_block: v[0].len=%d\n", dn); flush_log();

  slot = tbv->priv.curr_slot;
  for (i=tbv->priv.curr_slot; i<ti->n; i++) {
//dn = v[i].iov_len;
//log_printf(15, "tb_next_block: i=%d len=%d\n", i, dn); flush_log();
     ds = sum + v[i].iov_len;
     if (ds > pos) {
        slot = i;
        break;
     }
     sum = ds;
  }

//log_printf(0, "tb_next_block: final slot=%d total_slots=%d\n", slot, ti->n);

  //** Found my slot
  tbv->priv.slot_total_pos = sum;
  tbv->priv.curr_slot = slot;
  i = pos - sum;

  if (i == 0) { //** Starting on a slot boundary so can return the remaining iovec list based on nbytes    
    ds = pos + tbv->nbytes;

//n1=ds; n2=ti->total_bytes;
//log_printf(0, "tb_next_block: on boundary  ds=%d total=%d\n", n1, n2);
    if (ds == ti->total_bytes) {  //** Want the rest of the buffer
       tbv->buffer = &(v[slot]);
       tbv->n_iov = ti->n - tbv->priv.curr_slot;
    } else {  //** Only want a fraction of the buffer
       len = 0;
       for (i=tbv->priv.curr_slot; i<ti->n; i++) {
          ds = sum + v[i].iov_len;
          if (ds > endpos) {
             slot = i;
             break;
          }
          sum = ds;
          len = len + v[i].iov_len;
       }
//log_printf(0, "tb_next_block: fraction curr_slot=%d slot=%d total_slots=%d\n", tbv->priv.curr_slot, slot, ti->n);

       if (slot == tbv->priv.curr_slot) {  //** Want a subset of the current element
          tbv->n_iov = 1;
          tbv->priv.single = v[tbv->priv.curr_slot];
          tbv->priv.single.iov_len = tbv->nbytes;
          tbv->buffer = &(tbv->priv.single);
       } else {         //** Multiple elements wanted so drop the parital element
          tbv->n_iov = slot - tbv->priv.curr_slot;
          tbv->buffer = &(v[tbv->priv.curr_slot]);
          tbv->nbytes = len;
       }
    }
  } else {  //** Position is mid element so can only return the parital element
    tbv->n_iov = 1;
    if (v[tbv->priv.curr_slot].iov_base != NULL) {
       tbv->priv.single.iov_base = v[tbv->priv.curr_slot].iov_base + i;
    } else {
       tbv->priv.single.iov_base = NULL;
    }
    tbv->priv.single.iov_len = v[tbv->priv.curr_slot].iov_len - i;
    if (tbv->priv.single.iov_len > tbv->nbytes) tbv->priv.single.iov_len = tbv->nbytes;
    tbv->buffer = &(tbv->priv.single);
    tbv->nbytes = tbv->priv.single.iov_len;
//n1 = v[tbv->priv.curr_slot].iov_len;
//n2 = tbv->priv.single.iov_len;
//log_printf(0, "tb_next_block: mid-element curr_slot=%d i=%d max_len=%d return_len=%d\n", tbv->priv.curr_slot, i, n1, n2);
  }

  return(TBUFFER_OK);
}

//***************************************************************************
//  tbuffer_single - Simple single buffer transfer
//***************************************************************************

void tbuffer_single(tbuffer_t *tb, size_t nbytes, char *buffer)
{
  tb->buf.n = 1;
  tb->buf.total_bytes = nbytes;
  tb->buf.iov = &(tb->buf.io_single);
  tb->arg = NULL;

  tb->next_block = tb_next_block;
  
  tb->buf.iov->iov_base = buffer;
  tb->buf.iov->iov_len = nbytes;
}

//***************************************************************************
//  tbuffer_vec - Mulitple buffer transfer
//***************************************************************************

void tbuffer_vec(tbuffer_t *tb, size_t total_bytes, size_t n_vec, iovec_t *iov)
{
  tb->buf.n = n_vec;
  tb->buf.total_bytes = total_bytes;
  tb->buf.iov = iov;
  tb->arg = NULL;

  tb->next_block = tb_next_block;
}


//***************************************************************************
//  tbuffer_fn - User specified transfer function
//***************************************************************************

void tbuffer_fn(tbuffer_t *tb, size_t total_bytes, void *arg, int (*next_block)(tbuffer_t *tb, size_t pos, tbuffer_var_t *tbv))

{
  tb->arg = arg;
  tb->next_block = next_block;
  tb->buf.total_bytes = total_bytes;

  tb->next_block = next_block;
}


//***************************************************************************
//  tbuffer_size - Returns the buffer size
//***************************************************************************

size_t tbuffer_size(tbuffer_t *tb)
{
  return(tb->buf.total_bytes);
}

//***************************************************************************
//  tbuffer_copy - Copies data between buffers
//     This is done very simply by copying only 1 elemet of the iovec_t at a time
//     and relying on the tbuffer_next() routine to keep track of the positions
//***************************************************************************

int tbuffer_copy(tbuffer_t *tb_s, size_t off_s, tbuffer_t *tb_d, size_t off_d, size_t nbytes)
{
  int si, di, n, err;
  size_t nleft, spos, dpos, len, sp_ele, dp_ele, slen_ele, dlen_ele, nskipped;
  tbuffer_var_t stv;
  tbuffer_var_t dtv;

  tbuffer_var_init(&stv);
  tbuffer_var_init(&dtv);

  nskipped = 0;
  nleft = nbytes;
  err = TBUFFER_OK;
  spos = off_s; dpos = off_d;
  while ((nleft > 0) && (err == TBUFFER_OK)) {
     stv.nbytes = nleft;
     err = tbuffer_next(tb_s, spos, &stv);
     if (err == TBUFFER_OK) {
        dtv.nbytes = nleft;
        err = tbuffer_next(tb_d, dpos, &dtv);
        if (err == TBUFFER_OK) {
           n = (stv.nbytes > dtv.nbytes) ? dtv.nbytes : stv.nbytes;
           si = di = 0;
           sp_ele = dp_ele = 0;
           slen_ele = stv.buffer[si].iov_len;
           dlen_ele = dtv.buffer[di].iov_len;
           while (n > 0) {
              len = (slen_ele > dlen_ele) ? dlen_ele : slen_ele;
              if ((dtv.buffer[di].iov_base != NULL) && (stv.buffer[si].iov_base != NULL)) {
//log_printf(15, "dtv.buffer[%d].iov_base=%p stv.buffer[%d].iov_base=%p\n", di, dtv.buffer[di].iov_base, si, stv.buffer[si].iov_base); flush_log();
                 memcpy(dtv.buffer[di].iov_base+dp_ele, stv.buffer[si].iov_base+sp_ele, len);
              } else {
                 log_printf(15, "ERROR dtv.buffer[%d].iov_base=%p stv.buffer[%d].iov_base=%p\n", di, dtv.buffer[di].iov_base, si, stv.buffer[si].iov_base);
                 nskipped = len;
              }

              n -= len;
              spos += len;
              dpos += len;
              nleft -= len;

              if (n>0) {
                 dlen_ele -= len;
                 dp_ele += len;
                 if (dlen_ele <= 0) {
                    di++;
                    dlen_ele = dtv.buffer[di].iov_len;
                    dp_ele = 0;
                 }

                 slen_ele -= len;
                 sp_ele += len;
                 if (slen_ele <= 0) {
                    si++;
                    slen_ele = stv.buffer[si].iov_len;
                    sp_ele = 0;
                 }
              }
           }
        }
     }
  }

  return(nskipped);
}

//***************************************************************************
// tbuffer_memset - Similar to memset but for tbuffers
//***************************************************************************

int tbuffer_memset(tbuffer_t *tb, size_t off, int c, size_t nbytes)
{
  iovec_t *v;
  int i;
  size_t nleft, dn, nclear, pos;
  tbuffer_var_t tbv;

  tbuffer_var_init(&tbv);

  //** Make sure the buffer has enough space
  nleft = tbuffer_size(tb) - off;
  if (nleft < nbytes) return(-1);

  //** Now actually do the setting
  nleft = nbytes;
  pos = off;
  while (nleft > 0) {
     tbv.nbytes = nleft;
     tbuffer_next(tb, pos, &tbv);
     v = tbv.buffer;
     nclear = 0;
     for (i=0; i<tbv.n_iov; i++) {
        dn = v[i].iov_len;
        if (v[i].iov_base != NULL) memset(v[i].iov_base, c, dn);
        nleft = nleft - dn;
        nclear = nclear + dn;
        pos = pos + dn;
        if (nleft <= 0) break;
     }
  }

  return(0);
}

//***************************************************************************
// tbuffer_test_read - Verifies the data to be accessed is correct
//***************************************************************************

int tbuffer_test_read(tbuffer_t *tbuf, char *buffer, int bufsize, int off, int len)
{
  char output[len+1], c;
  int i, err, pos, nleft, opos;
  tbuffer_var_t tv;
  iovec_t *iov;

  memset(output, '-', sizeof(output));
  output[len] = 0;

//log_printf(0, "tbuffer_test_read: START n_iov=%d off=%d len=%d \n", tbuf->buf.n, off, len);

  tbuffer_var_init(&tv);

  nleft = len;
  pos = off;
  opos = 0;
  while (nleft > 0) {
    tv.nbytes = nleft;
    err = tbuffer_next(tbuf, pos, &tv);
    iov = tv.buffer;
    if (err != TBUFFER_OK) {
       log_printf(0, "tbuffer_test_read:  Error from tbuffer_next! off=%d len=%d bufsize=%d nleft=%d pos=%d opos=%d\n", off, len, bufsize, nleft, pos, opos);
       return(1);
    }

//log_printf(0, "tbuffer_test_read: n_iov=%d off=%d len=%d bufsize=%d nleft=%d pos=%d opos=%d\n", tv.n_iov, off, len, bufsize, nleft, pos, opos);
    for (i=0; i<tv.n_iov; i++) {
//err = iov[i].iov_len;
//log_printf(0, "tbuffer_test_read: iov=%d iov.len=%d nleft=%d pos=%d opos=%d\n", i, err, nleft, pos, opos);
      memcpy(&(output[opos]), iov[i].iov_base, iov[i].iov_len);
      nleft = nleft - iov[i].iov_len; 
      opos = opos + iov[i].iov_len;    
      pos = pos + iov[i].iov_len;
    }
//log_printf(0, "tbuffer_test_read: after loop n_iov=%d off=%d len=%d bufsize=%d nleft=%d pos=%d opos=%d\n", tv.n_iov, off, len, bufsize, nleft, pos, opos);
  }

//flush_log();

  err = memcmp(&(buffer[off]), output, len);
  if (err != 0) {
     log_printf(0, "tbuffer_test_read: ERROR with compare! off=%d len=%d\n", off, len);
     opos = 0;
     if (len > 40) {
        opos = len - 40;
        off = off + len - 40;
        len = 40;
       
     }
     log_printf(0, "tbuffer_test_read: ERROR with compare! Printing substring off=%d len=%d\n", off, len);
     c = buffer[off+len];
     buffer[off+len] = 0;
     output[len] = 0;
     log_printf(0, "tbuffer_test_read: buffer=!%s!\n", &(buffer[off]));
     log_printf(0, "tbuffer_test_read: output=!%s!\n", &(output[opos]));
     buffer[off+len] = c;
     err = 1;
  }

//log_printf(0, "tbuffer_test_read: END n_iov=%d off=%d len=%d \n", tbuf->buf.n, off, len);

  return(err);
}


//***************************************************************************
// tbuffer_next_test_iovec - Performs a test with the given iovec size
//***************************************************************************

int tbuffer_next_test_iovec(int n_iovec, int n_random, char *buffer, int bufsize)
{
  iovec_t iov[n_iovec];
  tbuffer_t tbuf;
  int i, err, off, len, maxlen;
  double frac;

  //** Make the iov array
  off = 0;
  len = 0;
  for (i=0; i<n_iovec; i++) {
    frac = (i+1.0)/(n_iovec*1.0);
    maxlen = frac*bufsize - off;
//log_printf(0, "tbuffer_next_text_iovec: n_iovec=%d i=%d frac=%lf maxlen=%d\n", n_iovec, i, frac,maxlen);
    len = random_int(0, maxlen);
    iov[i].iov_base = &(buffer[off]);
    iov[i].iov_len = len;
    off += len;
  }
  iov[n_iovec-1].iov_len = len + (bufsize - off);  //** Make sure the last element covers the whole buffer

//log_printf(0, "tbuffer_next_text_iovec: n_iovec=%d nbytes=%d\n", n_iovec, bufsize);
//off = 0;
//for (i=0; i<n_iovec; i++) {
//  len = iov[i].iov_len;
//  log_printf(0, "tbuffer_next_test_iovec:       i=%d off=%d len=%d\n", i, off, len);
//  off += len;
//}

  tbuffer_vec(&tbuf, bufsize, n_iovec, iov);

  //** Read and verify the buffer in a single read
  err = tbuffer_test_read(&tbuf, buffer, bufsize, 0, bufsize);
  if (err != 0) {
     log_printf(0, "tbuffer_next_test_iovec: Error with single full read: n_iovec=%d\n", n_iovec);
     return(1);
  }

  //** Now do the random offset/len tests
  for (i=0; i<n_random; i++) {
     off = random_int(0, bufsize);
     len = random_int(0, bufsize - off);
     err = tbuffer_test_read(&tbuf, buffer, bufsize, off, len);
     if (err != 0) {
        log_printf(0, "tbuffer_next_test_iovec: Error with random read: n_iovec=%d off=%d len=%d\n", n_iovec, off, len);
        return(2);
     }
  }

  return(0);
}

//***************************************************************************
// tbuffer_next_test - Test the tbuffer's next routine
//***************************************************************************

int tbuffer_next_test()
{
  int n_random = 100;
  int iovsize = 10;
  int bufsize = 1024;
  char buffer[bufsize+1];
  int i, err;

  //** Make the initial buffer
  for (i=0; i<bufsize; i++) buffer[i] = 'a' + (i % 27);
  buffer[bufsize] = 0;

  for (i=1; i<iovsize; i++) {
   log_printf(1, "tbuffer_next_test: Performing test with i=%d n_iovec=%d\n", i, iovsize);    
    err = tbuffer_next_test_iovec(i, n_random, buffer, bufsize);
    if (err != 0) {
       log_printf(0, "tbuffer_next_test: Error with iovec size=%d\n", i);
       return(i);
    }
  }
  
  log_printf(0, "PASSED!\n");
  return(0);
}


//***************************************************************************
// tbuffer_copy_test_iovec - Performs a copy test with the given iovec sizes
//***************************************************************************

int tbuffer_copy_test_iovec(int n1_iovec, int n2_iovec, int n_random, char *buffer, int bufsize)
{
  char output[bufsize+1];
  iovec_t iov1[n1_iovec];
  iovec_t iov2[n2_iovec];
  tbuffer_t tbuf1, tbuf2;
  int i, err, off1, off2, len, maxlen;
  double frac;

  //** Make the iov arrays
  off1 = 0;
  len = 0;
  for (i=0; i<n1_iovec; i++) {
    frac = (i+1.0)/(n1_iovec*1.0);
    maxlen = frac*bufsize - off1;
    len = random_int(0, maxlen);
    iov1[i].iov_base = &(buffer[off1]);
    iov1[i].iov_len = len;
    off1 += len;
  }
  iov1[n1_iovec-1].iov_len = len + (bufsize - off1);  //** Make sure the last element covers the whole buffer

//log_printf(1, "tbuffer_copy_test_iovec: n1_iovec=%d nbytes=%d\n", n1_iovec, bufsize);
//off1 = 0;
//for (i=0; i<n1_iovec; i++) {
//  len = iov1[i].iov_len;
//  log_printf(0, "tbuffer_copy_test_iovec:       i=%d off=%d len=%d\n", i, off1, len);
//  off1 += len;
//}

  tbuffer_vec(&tbuf1, bufsize, n1_iovec, iov1);

  //** Make the iov arrays
  off2 = 0;
  for (i=0; i<n2_iovec; i++) {
    frac = (i+1.0)/(n2_iovec*1.0);
    maxlen = frac*bufsize - off2;
    len = random_int(0, maxlen);
    iov2[i].iov_base = &(output[off2]);
    iov2[i].iov_len = len;
    off2 += len;
  }
  iov2[n2_iovec-1].iov_len = len + (bufsize - off2);  //** Make sure the last element covers the whole buffer

//log_printf(1, "tbuffer_copy_test_iovec: n2_iovec=%d nbytes=%d\n", n2_iovec, bufsize);
//off1 = 0;
//for (i=0; i<n2_iovec; i++) {
//  len = iov2[i].iov_len;
//  log_printf(0, "tbuffer_copy_test_iovec:       i=%d off=%d len=%d\n", i, off1, len);
//  off1 += len;
//}

  tbuffer_vec(&tbuf2, bufsize, n2_iovec, iov2);

  //** Read and verify the buffer in a single read
  memset(output, '-', bufsize); output[bufsize] = 0; 
  tbuffer_copy(&tbuf1, 0, &tbuf2, 0, bufsize);
  err = tbuffer_test_read(&tbuf2, buffer, bufsize, 0, bufsize);
  if (err != 0) {
     log_printf(0, "tbuffer_copy_test_iovec: Error with single full read: n_iovec1=%d n_iovec2=%d\n", n1_iovec, n2_iovec);
     return(1);
  }

  //** Now do the random offset/len tests
  for (i=0; i<n_random; i++) {
     off1 = random_int(0, bufsize);
     off2 = random_int(0, bufsize);
     len = (off1 > off2) ?  bufsize - off1 : bufsize - off2;
     len = random_int(0, len);
     memset(output, '-', bufsize); output[bufsize] = 0; 
     tbuffer_copy(&tbuf1, 0, &tbuf2, 0, bufsize);
     err = tbuffer_test_read(&tbuf2, buffer, bufsize, off2, len);
     if (err != 0) {
        log_printf(0, "tbuffer_next_test_iovec: Error with random read: n_iovec1=%d n_iovec2=%d off=%d len=%d\n", n1_iovec, n2_iovec, off2, len);
        return(2);
     }
  }

  return(0);
}


//***************************************************************************
// tbuffer_copy_test - Test the tbuffer_copy routine
//***************************************************************************

int tbuffer_copy_test()
{
  int n_random = 100;
  int iovsize = 10;
  int bufsize = 1024;
  char buffer[bufsize+1];
  int i, j, err;

  //** Make the initial buffer
  for (i=0; i<bufsize; i++) buffer[i] = 'a' + (i % 27);
  buffer[bufsize] = 0;

  for (i=1; i<iovsize; i++) {
   log_printf(1, "tbuffer_copy_test: Performing test with i=%d n_iovec=%d\n", i, iovsize);    
    for (j=1; j<iovsize; j++) {
      err = tbuffer_copy_test_iovec(i, j, n_random, buffer, bufsize);
      if (err != 0) {
         log_printf(0, "tbuffer_copy_test: Error i=%d j=%d\n", i, j);
         return(i);
      }
    }
  }
  
  log_printf(0, "PASSED!\n");
  return(0);
}

//***************************************************************************
// tbuffer_test - Test the tbuffer copy and next routines
//***************************************************************************

int tbuffer_test()
{
  int err_next, err_copy;

  err_next = tbuffer_next_test();
  err_copy = tbuffer_copy_test();

  return(err_next+err_copy);
}

