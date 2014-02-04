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

#define _log_module_index 171

#include <assert.h>
#include <math.h>
#include <apr_time.h>

#include "exnode.h"
#include "log.h"
#include "iniparse.h"
#include "string_token.h"
#include "type_malloc.h"
#include "random.h"
#include "opque.h"
#include "lio.h"

typedef struct {
  char *filename;
  int n_parallel;
  int preallocate;
  ex_off_t buffer_size;
  ex_off_t file_size;
  int read_lag;
  int update_interval;
  int do_final_check;
  int do_flush_check;
  int timeout;
  double read_fraction;
  int mode;
  double min_size;
  double max_size;
  double ln_min;
  double ln_max;
  int read_sigma;
  int write_sigma;
  int seed;
} rw_config_t;

typedef struct {
  ex_off_t offset;
  ex_off_t len;
} tile_t;

typedef struct {
  int base_index;
  int curr;
  int *span;
} test_index_t;

typedef struct {
  int global_index;
  int local_index;
  int type;
  tbuffer_t tbuf;
  ex_iovec_t iov;
  char *buffer;
} task_slot_t;

//*** Globals used in the test
rw_config_t rwc;
task_slot_t *task_list;
char *tile_data;
tile_t *base_tile;
int *tile_index;
char *wc_span = NULL;
int  tile_size;
int total_scan_size;
int last_tile_index;
ex_off_t tile_bytes;
ex_off_t max_task_bytes;
ex_off_t total_scan_bytes;
int tile_compare_size;
test_index_t read_index;
test_index_t write_index;
//test_index_t wcompleted_index;
exnode_t *ex = NULL;
segment_t *seg = NULL;
data_attr_t *da = NULL;

int my_log_level = 10;

//*************************************************************************
//** My random routines for reproducible runs
//*************************************************************************

void my_random_seed(unsigned int seed) { srandom(seed); }

//*************************************************************************

int my_get_random(void *vbuf, int nbytes)
{
  char *buf = (char *)vbuf;
  int i;
  unsigned short int v;
  unsigned short int *p;
  int ncalls = nbytes / sizeof(v);
  int nrem = nbytes % sizeof(v);

  for (i=0; i<ncalls; i++) {
     p = (unsigned short int *)&(buf[i*sizeof(v)]);
     *p = random();

//     memcpy(&(buf[i*sizeof(v)]), &v, sizeof(v));
//     (long int)*&(buf[i*sizeof(v)]) = random();
  }

//i=sizeof(v);
//log_printf(10, "my_get_random: nbytes=%d ncall=%d nrem=%d sizeof(long int)=%d\n", nbytes, ncalls, nrem, i);

  if (nrem > 0) {
     v = random();
     memcpy(&(buf[ncalls*sizeof(v)]), &v, nrem);
  }

  return(0);
}

//*************************************************************************

double my_random_double(double lo, double hi)
{
  double dn, n;
  uint64_t rn;

  rn = 0;
  my_get_random(&rn, sizeof(rn));
//log_printf(10, "my_random_double: rn=" XIDT "\n", rn);
  dn = (1.0 * rn) / (UINT64_MAX + 1.0);
//  dn = rn;
//  n = (UINT64_MAX + 1.0);
//log_printf(10, "my_random_double: dn=%lf/%lf\n", dn, n);
//  dn = dn / n;

  n = lo + (hi - lo) * dn;
//log_printf(10, "my_random_double: rn=" XIDT " dn=%lf n=%lf\n", rn, dn, n);

  return(n);
}

//*******************************************************************

int64_t my_random_int(int64_t lo, int64_t hi)
{
  int64_t n, dn;

  dn = hi - lo + 1;
  n = lo + dn * my_random_double(0, 1);

  return(n);
}

//*************************************************************************
// tile_compare - Comparison routine
//*************************************************************************

int tile_compare(const void *p1, const void *p2)
{
  int i1 = *(int *)p1;
  int i2 = *(int *)p2;
  char *v1, *v2;

  v1 = &(tile_data[tile_compare_size*i1]);
  v2 = &(tile_data[tile_compare_size*i2]);
  return(memcmp(v1, v2, tile_compare_size));
}

//*************************************************************************
// gernerate_task_list - Creates the task list
//*************************************************************************

void generate_task_list()
{
  int inc_size = 100;
  int i, j, max_size, loops;
  ex_off_t offset, len, last_offset;
  double d;

  max_size = inc_size;
  base_tile = (tile_t *)malloc(sizeof(tile_t)*max_size);

  last_offset = rwc.file_size % rwc.buffer_size;
  last_tile_index = -1;
  max_task_bytes = -1;
  i = 0;
  offset = 0;
  do {
     if (i >= max_size) { max_size += inc_size; base_tile = (tile_t *)realloc(base_tile, sizeof(tile_t)*max_size); }
     base_tile[i].offset = offset;
     if (rwc.min_size == rwc.max_size) {
        len = rwc.min_size;
     } else {
        d = my_random_double(rwc.ln_min, rwc.ln_max);
        len = exp(d);
     }
     base_tile[i].len = len;

     offset += len;

     if (offset < rwc.buffer_size) log_printf(10, "generate_task_list: i=%d off=" XOT " len=" XOT "\n", i, base_tile[i].offset, base_tile[i].len);
     if (offset < last_offset) last_tile_index = i;
     if (max_task_bytes < len) max_task_bytes = len;
     i++;
//log_printf(10, "generate_task_list: ii=%d\n", i);
  } while (offset < rwc.buffer_size);

  //** If it's to big truncate the size
  if (offset > 1.01*rwc.buffer_size) {
     offset -= len;
     len = 1.01*rwc.buffer_size - offset;
     base_tile[i-1].len = len;
     offset += len;
  }

  //** PRint the last slot if needed
  log_printf(10, "generate_task_list: i=%d off=" XOT " len=" XOT "\n", i-1, base_tile[i-1].offset, base_tile[i-1].len);

//log_printf(10, "generate_task_list: tile_size=%d\n", i);

  tile_size = i;
  tile_bytes = offset;
  loops = rwc.file_size / rwc.buffer_size;
  total_scan_size = loops*tile_size;
  total_scan_bytes = loops*tile_bytes;
  if (last_tile_index > 0) {
     total_scan_size += last_tile_index + 1;
     total_scan_bytes += base_tile[last_tile_index].offset + base_tile[last_tile_index].len;
  }

  //** Adjust the size of the base_tile array to the correct size;
  base_tile = (tile_t *)realloc(base_tile, sizeof(tile_t)*tile_size);

  //** Make the actual buffer and fill it with random data
  type_malloc_clear(tile_data, char, tile_bytes);
  my_get_random(tile_data, tile_bytes);

  //** Make the array defining the order or operations
  type_malloc(tile_index, int, tile_size);

  //** This is used for linear mode
  for (i=0; i<tile_size; i++) tile_index[i] = i;

  //** If needed sort it
  tile_compare_size = (tile_bytes > 4*tile_size) ? 4 : tile_bytes / tile_size;
  if (rwc.mode == 1) {
log_printf(15, "generate_task_list: tile_compare_size=%d\n", tile_compare_size);
     qsort(tile_index, tile_size, sizeof(int), tile_compare);
 
     if (log_level() > 10) {
        log_printf(10, "generate_task_list: -------------------Random order requested-----------------\n");
        for (i=0; i<tile_size; i++) {
           j = tile_index[i];
           log_printf(10, "generate_task_list: i=%d index=%d off=" XOT " len=" XOT "\n", i, j, base_tile[j].offset, base_tile[j].len);
        }
        log_printf(10, "generate_task_list: ----------------------------------------------------------\n");
     }
  }

  //** Want to do the reading after thew write phase completes
  if (rwc.read_lag < 0) rwc.read_lag = total_scan_size;

  log_printf(0, "--------- Task Breakdown ----------\n");
  d = tile_bytes / 1024.0 / 1024.0;
  log_printf(0, "Tile size: %lfMB (" XOT " bytes)\n", d, tile_bytes);
  log_printf(0, "Tile ops: %d (Tile loops:%d  Last tile index: %d)\n", tile_size, loops, last_tile_index);

  d = total_scan_bytes / 1024.0 / 1024.0;
  log_printf(0, "Total size: %lfMB\n", d);
  log_printf(0, "Total ops: %d (write)", total_scan_size);
  if (rwc.read_fraction) {
     slog_printf(0, " + %d (read) = %d\n", total_scan_size, 2*total_scan_size);
  } else {
     slog_printf(0, "\n");
  }
  log_printf(0, "--------------------------------------------\n");
}

//*************************************************************************
// make_test_indices - Makes the test indices for keeping track of the R/W
//   task position
//*************************************************************************

void make_test_indices()
{
  int i;

  if (rwc.read_sigma > total_scan_size) rwc.read_sigma = total_scan_size;
  if (rwc.write_sigma > total_scan_size) rwc.write_sigma = total_scan_size;

  type_malloc(wc_span, char, total_scan_size);  memset(wc_span, '0', total_scan_size);

  type_malloc_clear(read_index.span, int, rwc.read_sigma);
  type_malloc_clear(write_index.span, int, rwc.write_sigma);

  read_index.base_index = 0;  read_index.curr = (rwc.read_fraction == 0) ? tile_size : 0;
  write_index.base_index = 0; write_index.curr = 0;

  type_malloc_clear(task_list, task_slot_t, rwc.n_parallel);
  for (i=0; i<rwc.n_parallel; i++) {
     type_malloc_clear(task_list[i].buffer, char, max_task_bytes);
  }
}

//*************************************************************************
// compare_buffers_print - FInds the 1st index where the buffers differ
//*************************************************************************

void compare_buffers_print(char *b1, char *b2, int len, ex_off_t offset)
{
  int i, k, mode, last, ok;
  ex_off_t start, end;
 
  mode = (b1[0] == b2[0]) ? 0 : 1;
  start = offset;
  last = len - 1;

  log_printf(0, "Printing comparision breakdown -- Single byte matches are suppressed (len=%d)\n", len);  
  for (i=0; i<len; i++) {
    if (mode == 0) {  //** Matching range
      if ((b1[i] != b2[i]) || (last == i)) {
         end = offset + i-1;
         k = end - start + 1;
         log_printf(0, "  MATCH : %d -> %d (%d bytes)\n", start, end, k);

         start = offset + i;
         mode = 1;
      }
    } else {
      if ((b1[i] == b2[i]) || (last == i)) {
         ok = 0;  //** Suppress single byte matches
         if (last != i) {
            if (b1[i+1] == b2[i+1]) ok = 1;
         }
         if ((ok == 1) || (last == i)) {
            end = offset + i-1;
            k = end - start + 1;
            log_printf(0, "  DIFFER: %d -> %d (%d bytes)\n", start, end, k);

            start = offset + i;
            mode = 0;
         }
      }
    }
  }

  return;
}


//*************************************************************************
// perform_final_verify - Does a final read verify of the data
//*************************************************************************

void perform_final_verify()
{
  char *buffer;
  int i, n, err, fail;
  ex_off_t off, len;
  ex_iovec_t iov;
  tbuffer_t tbuf;
  apr_time_t dt, dt2;
  double rate, dsec;
int ll = 15;

  type_malloc(buffer, char, tile_bytes);

  tbuffer_single(&tbuf, tile_bytes, buffer);

//set_log_level(20);

  n = total_scan_bytes / tile_bytes;
  off = 0;
  fail = 0;
  log_printf(0, "-------------- Starting final verify -----------------------\n"); flush_log();
  dt = apr_time_now();

  for (i=0; i<n; i++) {
    log_printf(ll, "checking offset=" XOT "\n", off, ll);
    memset(buffer, 'A', tile_bytes);
    ex_iovec_single(&iov, off, tile_bytes);
flush_log();
  dt2 = apr_time_now();

    err = gop_sync_exec(segment_read(seg, da, 1, &iov, &tbuf, 0, rwc.timeout));
  dt2 = apr_time_now() - dt2;
  dsec = dt2;
  dsec = dsec / APR_USEC_PER_SEC;
  rate = (1.0*tile_bytes) / ( 1024.0 * 1024.0 * dsec);
  log_printf(0, "gop er=%d: Time: %lf secs  (%lf MB/s)\n", err, dsec, rate);

log_printf(ll, "gop err=%d\n", err); flush_log();

    if (err != OP_STATE_SUCCESS) {
       log_printf(0, "ERROR with read! block offset=" XOT "\n", off);
    }
    err = memcmp(buffer, tile_data, tile_bytes);
log_printf(ll, "memcmp=%d\n", err); flush_log();

//err = 0;
    if (err != 0) {
       fail = 1;
       log_printf(0, "ERROR with compare! block offset=" XOT "\n", off);
       compare_buffers_print(buffer, tile_data, tile_bytes, off);
    }

    off += tile_bytes;
  }

  if (last_tile_index > 0) {
    log_printf(ll, "checking offset=" XOT "\n", off);
     len = base_tile[last_tile_index].offset + base_tile[last_tile_index].len;
     memset(buffer, 'A', len);
     ex_iovec_single(&iov, off, len);
     err = gop_sync_exec(segment_read(seg, da, 1, &iov, &tbuf, 0, rwc.timeout));
     if (err != OP_STATE_SUCCESS) {
         log_printf(0, "ERROR with read! block offset=" XOT "\n", off);
     }
     err = memcmp(buffer, tile_data, len);
//err = 0;
     if (err != 0) {
        fail = 1;
        log_printf(0, "perform_final_verify:  ERROR with compare! partial block offset=" XOT "\n", off);
        compare_buffers_print(buffer, tile_data, len, off);
     }
  }

  dt = apr_time_now() - dt;
  log_printf(0, "-------------- Completed final verify -----------------------\n");

  dsec = dt;
  dsec = dsec / APR_USEC_PER_SEC;
  rate = (1.0*total_scan_bytes) / ( 1024.0 * 1024.0 * dsec);
  log_printf(0, "perform_final_verify: Time: %lf secs  (%lf MB/s)\n", dsec, rate);

  if (fail == 0) {
     log_printf(0, "perform_final_verify:  PASSED\n");
  } else {
     log_printf(0, "perform_final_verify:  FAILED\n");
  }

  flush_log();

  free(buffer);
}

//*************************************************************************
// find_write_task - Finds a write task to perform
//*************************************************************************

op_generic_t *find_write_task(task_slot_t *tslot)
{
  int flip, i, j, n, slot;
  ex_off_t offset;
  op_generic_t *gop;

  if (write_index.curr >= total_scan_size) return(NULL);  //** Nothing left to do

  write_index.curr++;

  flip = my_random_int(0, rwc.write_sigma-1);
  n = -1;
  for (i=0; i< rwc.write_sigma; i++) {
     j = (flip + i) % rwc.write_sigma;  //** Used to map the slot to the span range
     slot = (write_index.base_index + j) % rwc.write_sigma;
     if (write_index.span[slot] == 0) { n = write_index.base_index + j; break; }
  }

log_printf(my_log_level, "find_write_task: span global=%d flip=%d i=%d base=%d spanindex=%d\n", n, flip, i, write_index.base_index, (n%rwc.write_sigma)); flush_log();

  write_index.span[n%rwc.write_sigma] = 1;
  tslot->global_index = n;
log_printf(my_log_level, "find_write_task: span slot=%d curr=%d base=%d global=%d\n", n, write_index.curr, write_index.base_index, tslot->global_index); flush_log();

  tslot->local_index = (tslot->global_index) % tile_size;
  tslot->type = 0;

  slot = tile_index[tslot->local_index];
  offset = ((tslot->global_index) / tile_size) * tile_bytes + base_tile[slot].offset;
  tbuffer_single(&(tslot->tbuf), base_tile[slot].len, &(tile_data[base_tile[slot].offset]));
  ex_iovec_single(&(tslot->iov), offset, base_tile[slot].len);

  gop = segment_write(seg, da, 1, &(tslot->iov), &(tslot->tbuf), 0, rwc.timeout);
  gop_set_private(gop, (void *)tslot);

  n = total_scan_size - rwc.write_sigma;
log_printf(my_log_level, "find_write_task: span_update max=%d base=%d\n", n, write_index.base_index); flush_log();
  for (i=0; i<rwc.write_sigma; i++) {
     slot = write_index.base_index % rwc.write_sigma;
log_printf(my_log_level, "find_write_task: span_update slot=%d i=%d base=%d span[slot]=%d\n", slot, i, write_index.base_index, write_index.span[slot]); flush_log();

     if (write_index.span[slot] == 0) break;
     if ( write_index.base_index >= n) break;  //** Don't move the window beyond the end
     write_index.span[slot] = 0;
     write_index.base_index++;
  }

  return(gop);
}

//*************************************************************************
// find_read_task - Finds a read task to perform
//*************************************************************************

op_generic_t *find_read_task(task_slot_t *tslot, int write_done)
{
  int flip, i, j, n, slot;
  ex_off_t offset;
  op_generic_t *gop;

  if (read_index.curr >= total_scan_size) return(NULL);  //** Nothing left to do

  flip = my_random_int(0, rwc.read_sigma-1);

  n = -1;
  for (i=0; i< rwc.read_sigma; i++) {
     j = (flip + i) % rwc.read_sigma;  //** Used to map the slot to the span range
     slot = (read_index.base_index + j) % rwc.read_sigma;
log_printf(my_log_level, "find_read_task: span i=%d j=%d flip=%d base=%d spanslot=%d span[slot]=%d\n", i, j, flip,  read_index.base_index, slot, read_index.span[slot]); flush_log();
     if (read_index.span[slot] == 0) {
        slot =  read_index.base_index + j;
log_printf(my_log_level, "find_read_task: slot=%d wc=%c\n", slot, wc_span[slot]); flush_log();

        if (wc_span[slot] == '1') {
           n = read_index.base_index + j;
           break;
       }
    }
  }

log_printf(my_log_level, "find_read_task: span global=%d flip=%d base=%d spanindex=%d\n", n, flip,  read_index.base_index, (n%rwc.read_sigma)); flush_log();

if ((n<0) && (write_done >= total_scan_size)) {
  log_printf(0, "find_read_task:  ERROR!! No viable taks found!! Printing wc_span table (READ base=%d curr=%d  ---- WRITE base=%d curr=%d\n", 
      read_index.base_index, read_index.curr, write_index.base_index, write_index.curr);
  for (i=0; i<rwc.read_sigma; i++) {
     slot = read_index.base_index + i;
     j = slot % rwc.read_sigma;
     log_printf(0, "find_read_task:  i=%d slot=%d wc_span[slot]=%c read_slot=%d\n", i, slot, wc_span[slot], read_index.span[j]);
  }
}


  if (n < 0) return(NULL);  //** Nothing to do

  read_index.curr++;

  read_index.span[n%rwc.read_sigma] = 1;
  tslot->global_index = n;
  tslot->local_index = (tslot->global_index) % tile_size;
  tslot->type = 1;

log_printf(my_log_level, "find_read_task: span slot=%d curr=%d base=%d global=%d\n", n, read_index.curr, read_index.base_index, tslot->global_index); flush_log();

  slot = tile_index[tslot->local_index];
  offset = ((tslot->global_index) / tile_size) * tile_bytes + base_tile[slot].offset;
  memset(tslot->buffer, 'A', base_tile[slot].len);
  tbuffer_single(&(tslot->tbuf), base_tile[slot].len, tslot->buffer);
  ex_iovec_single(&(tslot->iov), offset, base_tile[slot].len);

  gop = segment_read(seg, da, 1, &(tslot->iov), &(tslot->tbuf), 0, rwc.timeout);
log_printf(my_log_level, "find_read_task: global=%d off=" XOT " len=" XOT " gop=%p\n", tslot->global_index, offset, base_tile[slot].len, gop); flush_log();

  gop_set_private(gop, (void *)tslot);

  //** Move up the read base index
  n = total_scan_size - rwc.read_sigma;
//log_printf(my_log_level, "find_read_task: span_update max=%d base=%d\n", n, read_index.base_index); flush_log();
  for (i=0; i<rwc.read_sigma; i++) {
     slot = read_index.base_index % rwc.read_sigma;
//log_printf(my_log_level, "find_read_task: span_update slot=%d i=%d base=%d span[slot]=%d\n", slot, i, read_index.base_index, read_index.span[slot]); flush_log();

     if (read_index.span[slot] == 0) break;
     if (read_index.base_index >= n) break;  //** Don't move the window beyond the end
     read_index.span[slot] = 0;
     read_index.base_index++;
  }

  return(gop);
}

//*************************************************************************
// complete_write_task - Completes the task
//*************************************************************************

void complete_write_task(task_slot_t *tslot)
{
log_printf(my_log_level, "global=%d\n", tslot->global_index);
  wc_span[tslot->global_index] = '1';
}

//*************************************************************************
// complete_read_task - Completes the task
//*************************************************************************

void complete_read_task(task_slot_t *tslot)
{
  int err, len, off, n;
  ex_off_t goff;

  n = tile_index[tslot->local_index];
  len = base_tile[n].len;
  off = base_tile[n].offset;
  err = memcmp(tslot->buffer, &(tile_data[off]), len);

log_printf(my_log_level, "Marking global=%d as complete off=%d len=%d\n", tslot->global_index, off, len);
  if (err != 0) {
     goff = (n / tile_size) * tile_bytes;
     log_printf(0, "complete_read_task:  ERROR with compare! global=%d local=%d off=%d len=%d global_off=" XOT "\n", tslot->global_index, tslot->local_index, off, len, goff);
     compare_buffers_print(tslot->buffer, &(tile_data[off]), len, goff);
  }
}

//*************************************************************************
// rw_test - Does the actual test
//*************************************************************************

void rw_test()
{
  int i, success, fail, j;
  int write_done, read_done;
  double flip, dtask, dops;
  op_generic_t *gop;
  opque_t *q;
  task_slot_t *slot;
  double op_rate, mb_rate, dsec, dt;
  apr_time_t dtw, dtr, dtt, ds_start, ds_begin, ds, dstep;
  Stack_t *free_slots;
  cache_stats_t cs;
  int tbufsize = 10240;
  char text_buffer[tbufsize];

  //** Setup everything
  log_printf(0, "Generating tasks and random data\n");  flush_log();
  generate_task_list();
  make_test_indices();
  log_printf(0, "Completed task and data generation\n\n"); flush_log();

  //** Truncate the file to back to the correct size if needed
  if (rwc.preallocate == 1) {
     log_printf(0, "Preallocating all space\n\n"); flush_log();
     j = gop_sync_exec(segment_truncate(seg, da, total_scan_bytes, 10));
     if (j != OP_STATE_SUCCESS) {
        printf("Error truncating the file!\n");
        flush_log(); fflush(stdout);
        abort();
     }
  }

  q = new_opque();

  free_slots = new_stack();  //** Slot 0 is hard coded below
  for (i=1; i<rwc.n_parallel; i++) push(free_slots, &(task_list[i]));

//exit(1);

  //** Do the test
  flip = 0;
  i = 0;
  success = 0; fail = 0;
  slot = &(task_list[0]);

  dops = (rwc.read_fraction > 0) ? 2*total_scan_size : total_scan_size;
  j = dops;
  log_printf(0,"-------------- Starting R/W Test (%d ops) -----------------------\n", j);

  dtw = apr_time_now();
  dtt = apr_time_now();
  ds_start = apr_time_now();
  ds_begin = ds_start;

  dtr = 0;
  write_done = 0;  read_done = 0;
  do {
     if ((i%rwc.update_interval) == 0) {
        dtask = (double )i/dops * 100.0;
        ds = apr_time_now() - ds_begin;
        dsec = (1.0*ds) / APR_USEC_PER_SEC;
        dstep = apr_time_now() - ds_start;
        dt = (1.0*dstep) / APR_USEC_PER_SEC;
        ds_start = apr_time_now();
        log_printf(0, "i=%d (%lf%%) (write: %d  read: %d) (t=%lf dt=%lf)\n", i, dtask, write_index.curr, read_index.curr, dsec, dt);
     }

     gop = NULL;
     flip = my_random_double(0, 1);
     if (((flip < rwc.read_fraction) && (i>rwc.read_lag)) || (write_index.curr >= total_scan_size)) { //** Read op
        gop = find_read_task(slot, write_done);
        if (dtr == 0) dtr = apr_time_now();
     }

     if (gop == NULL) { //** Write op
        gop = find_write_task(slot);
     }

flush_log();

     if (gop != NULL) {
        log_printf(1, "rw_test: SUBMITTING i=%d gid=%d mode=%d global=%d off=" XOT " len=" XOT "\n", i, gop_id(gop), slot->type, slot->global_index, slot->iov.offset, slot->iov.len);
        i++;
        opque_add(q, gop);
     } else {
        log_printf(my_log_level, "Nothing to do. Waiting for write to complete.  Read: curr=%d done=%d  Write: curr=%d done=%d nleft=%d\n", read_index.curr, read_done, write_index.curr, write_done, opque_tasks_left(q));
        push(free_slots, slot);
     }

     if ((stack_size(free_slots) == 0) || (gop == NULL)) {
        gop = opque_waitany(q);

        slot = gop_get_private(gop);

        if (gop_completed_successfully(gop) != OP_STATE_SUCCESS) {
           fail++;
           log_printf(0, "rw_test: FINISHED ERROR gid=%d status=%d mode=%d global=%d off=" XOT " len=" XOT "\n", gop_id(gop), gop_get_status(gop), slot->type, slot->global_index, slot->iov.offset, slot->iov.len);
        } else {
           success++;
           log_printf(1, "rw_test: FINISHED SUCCESS gid=%d status=%d mode=%d global=%d off=" XOT " len=" XOT "\n", gop_id(gop), gop_get_status(gop), slot->type, slot->global_index, slot->iov.offset, slot->iov.len);
        }

        if (slot->type == 0) {
           write_done++;
           if (write_done >= total_scan_size) dtw = apr_time_now() - dtw;
           complete_write_task(slot);
        } else {
           read_done++;
           if (read_done >= total_scan_size) dtr = apr_time_now() - dtr;
           complete_read_task(slot);
        }

        gop_free(gop, OP_DESTROY);
     } else {
log_printf(my_log_level, "popping slot i=%d\n", i);
        slot = (task_slot_t *)pop(free_slots);
     }

log_printf(my_log_level, "rw_test: read_index.curr=%d (%d done) write_index.curr=%d (%d done) total_scan_size=%d\n", read_index.curr, read_done, write_index.curr, write_done, total_scan_size);
  } while ((read_index.curr < total_scan_size) || (write_index.curr < total_scan_size));

  //** Wait for the remaining tasks to complete
  while ((gop = opque_waitany(q)) != NULL) {
    slot = gop_get_private(gop);

    if (gop_completed_successfully(gop) != OP_STATE_SUCCESS) {
       fail++;
       log_printf(0, "rw_test: FINISHED ERROR gid=%d status=%d mode=%d global=%d off=" XOT " len=" XOT "\n", gop_id(gop), gop_get_status(gop), slot->type, slot->global_index, slot->iov.offset, slot->iov.len);
    } else {
       success++;
       log_printf(1, "rw_test: FINISHED SUCCESS gid=%d status=%d mode=%d global=%d off=" XOT " len=" XOT "\n", gop_id(gop), gop_get_status(gop), slot->type, slot->global_index, slot->iov.offset, slot->iov.len);
    }

    if (slot->type == 0) {
       write_done++;
       if (write_done >= total_scan_size) dtw = apr_time_now() - dtw;
       complete_write_task(slot);
    } else {
       read_done++;
       if (read_done >= total_scan_size) dtr = apr_time_now() - dtr;
       complete_read_task(slot);
    }

log_printf(my_log_level, "rw_test: stragglers -- read_index.curr=%d (%d done) write_index.curr=%d (%d done) total_scan_size=%d\n", read_index.curr, read_done, write_index.curr, write_done, total_scan_size);

    gop_free(gop, OP_DESTROY);
  }

  dtt = apr_time_now() - dtt;

  log_printf(0,"-------------- Completed R/W Test -----------------------\n");

  dsec = (1.0*dtw) / APR_USEC_PER_SEC;
  op_rate = (1.0*total_scan_size) / dsec;
  mb_rate = (1.0*total_scan_bytes) / (dsec * 1024.0 * 1024.0);
  log_printf(0, "Write performance: %lf s total --  %lf ops/s -- %lf MB/s\n", dsec, op_rate, mb_rate);

  if (rwc.read_fraction > 0) {
     dsec = (1.0*dtr) / APR_USEC_PER_SEC;
     op_rate = (1.0*total_scan_size) / dsec;
     mb_rate = (1.0*total_scan_bytes) / (dsec * 1024.0 * 1024.0);
     log_printf(0, "Read performance:  %lf s total --  %lf ops/s -- %lf MB/s\n", dsec, op_rate, mb_rate);

     dsec = (1.0*dtt) / APR_USEC_PER_SEC;
     op_rate = (2.0*total_scan_size) / dsec;
     mb_rate = (2.0*total_scan_bytes) / (dsec * 1024.0 * 1024.0);
     log_printf(0, "Run performance:   %lf s total --  %lf ops/s -- %lf MB/s\n", dsec, op_rate, mb_rate);
  }

  log_printf(0,"----------------------------------------------------------\n");

  printf("--------------------- Cache Stats ------------------------\n");
  cache_stats(lio_gc->cache, &cs);
  i = 0;
  cache_stats_print(&cs, text_buffer, &i, tbufsize);
  printf("%s", text_buffer);
  printf("----------------------------------------------------------\n");


  opque_free(q, OP_DESTROY);

  //** and the final verify if needed
  if (rwc.do_final_check > 0) {
     perform_final_verify();

     printf("--------------------- Cache Stats ------------------------\n");
     cache_stats(lio_gc->cache, &cs);
     i = 0;
     cache_stats_print(&cs, text_buffer, &i, tbufsize);
     printf("%s", text_buffer);
     printf("----------------------------------------------------------\n");
  }

  if (rwc.do_flush_check > 0) {
     log_printf(0, "============ Flushing data and doing a last verification =============\n");  flush_log();
     dtt = apr_time_now();
     gop_sync_exec(segment_flush(seg, da, 0, total_scan_bytes, 30));
     dtt = apr_time_now() - dtt;
     dsec = (1.0*dtt) / APR_USEC_PER_SEC;
     log_printf(0, "============ Flush completed (%lf s) Dropping pages as well =============\n", dsec);  flush_log();
     cache_drop_pages(seg, 0, total_scan_bytes+1);  //** Drop all the pages so they have to be reloaded on next test
     perform_final_verify();

     printf("--------------------- Cache Stats ------------------------\n");
     cache_stats(lio_gc->cache, &cs);
     i = 0;
     cache_stats_print(&cs, text_buffer, &i, tbufsize);
     printf("%s", text_buffer);
     printf("----------------------------------------------------------\n");
  }

  free_stack(free_slots, 0);
}

//*************************************************************************
// rw_load_options - Loads the test options form the config file
//*************************************************************************

void rw_load_options(char *cfgname, char *group)
{
  inip_file_t *fd;
  char *str;

  fd = inip_read(cfgname);
  if (fd == NULL) {
     printf("rw_load_config:  ERROR opening config file: %s\n", cfgname);
     flush_log(); fflush(stdout);
     abort();
  }

  //** Parse the global params
  rwc.n_parallel = inip_get_integer(fd, group, "parallel", 1);
  rwc.preallocate = inip_get_integer(fd, group, "preallocate", 0);
  rwc.buffer_size = inip_get_integer(fd, group, "buffer_size", 10*1024*1024);
  rwc.file_size = inip_get_integer(fd, group, "file_size", 10*1024*1024);
  rwc.do_final_check = inip_get_integer(fd, group, "do_final_check", 1);
  rwc.do_flush_check = inip_get_integer(fd, group, "do_flush_check", 1);
  rwc.timeout = inip_get_integer(fd, group, "timeout", 10);
  rwc.update_interval = inip_get_integer(fd, group, "update_interval", 1000);
  rwc.filename = inip_get_string(fd, group, "file", "");
  rwc.read_lag = inip_get_integer(fd, group, "read_lag", 10);
  rwc.read_fraction = inip_get_double(fd, group, "read_fraction", 0.0);
  rwc.seed = inip_get_integer(fd, group, "seed", 1);
  my_random_seed(rwc.seed);

  str = inip_get_string(fd, group, "mode", "linear");
  if (strcasecmp(str, "linear") == 0) {
     rwc.mode = 0;
  } else if (strcasecmp(str, "random") == 0) {
     rwc.mode = 1;
  } else {
     printf("rw_load_options: ERROR loading mode. mode=%s\n", str);
     printf("rw_load_options: Should be either 'linear' or 'random'.\n");
     flush_log(); fflush(stdout);
     abort();
  }
  free(str);

  rwc.min_size = 1024.0*inip_get_double(fd, group, "min_size", 1);
  if (rwc.min_size == 0) rwc.min_size = 1;
  rwc.max_size = 1024.0*inip_get_double(fd, group, "max_size", 10);
  if (rwc.max_size == 0) rwc.max_size = 1;
  rwc.ln_min = log(rwc.min_size);
  rwc.ln_max = log(rwc.max_size);

  rwc.read_sigma = inip_get_integer(fd, group, "read_sigma", 50);
  rwc.write_sigma = inip_get_integer(fd, group, "write_sigma", 50);

  inip_destroy(fd);
}

//*************************************************************************
// rw_print_options - Prints the options to fd
//*************************************************************************

void rw_print_options(FILE *fd)
{
  char ppbuf[100];
  char *group;
  double d;

  group = "rw_params";
  fprintf(fd, "[%s]\n", group);
  fprintf(fd, "preallocate=%d\n", rwc.preallocate);
  fprintf(fd, "parallel=%d\n", rwc.n_parallel);
  fprintf(fd, "buffer_size=%s\n", pretty_print_int_with_scale(rwc.buffer_size, ppbuf));
  fprintf(fd, "file_size=%s\n", pretty_print_int_with_scale(rwc.file_size, ppbuf));
  fprintf(fd, "do_final_check=%d\n", rwc.do_final_check);
  fprintf(fd, "do_flush_check=%d\n", rwc.do_flush_check);
  fprintf(fd, "timeout=%d\n", rwc.timeout);
  fprintf(fd, "update_interval=%d\n", rwc.update_interval);
  fprintf(fd, "filename=%s\n", rwc.filename);
  fprintf(fd, "seed=%d\n", rwc.seed);
  fprintf(fd, "read_lag=%d\n", rwc.read_lag);
  fprintf(fd, "read_fraction=%lf\n", rwc.read_fraction);

  if (rwc.mode == 0) {
    fprintf(fd, "mode=linear\n");
  } else {
     fprintf(fd, "mode=random\n");
  }

  d = rwc.min_size / 1024.0; fprintf(fd, "min_size=%lf\n", d);
  d = rwc.max_size / 1024.0; fprintf(fd, "max_size=%lf\n", d);
  fprintf(fd, "read_sigma=%d\n", rwc.read_sigma);
  fprintf(fd, "write_sigma=%d\n", rwc.write_sigma);

  fprintf(fd, "\n");
}

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
  int i, err, start_option, print_exnode, soft_errors, hard_errors;
  exnode_exchange_t *exp;
  exnode_exchange_t *exp_out;
  char *section = "rw_params";
  op_status_t status;
  op_generic_t *gop;

//printf("argc=%d\n", argc);
  if (argc < 2) {
     printf("\n");
     printf("ex_rw_test LIO_COMMON_OPTIONS [-ex] [-s section]\n");
     lio_print_options(stdout);
     printf("     -ex        Print the final exnode to the screen before truncation\n");
     printf("     -s section SEction in the config file to usse.  Defaults to %s.\n", section);
     printf("\n");
     return(1);
  }

  lio_init(&argc, &argv);

  da = lio_gc->da;
  rwc.timeout = lio_gc->timeout;

  //*** Parse the args
  print_exnode = 0;
  i=1;
  if (argc > 1) {
     do {
        start_option = i;

        if (strcmp(argv[i], "-ex") == 0) { //** Show the final exnode
           i++;
           print_exnode = 1;
        } else if (strcmp(argv[i], "-s") == 0) { //** Change the default section to use
           i++;
           section = argv[i];
           i++;
        }
     } while ((start_option < i) && (i<argc));
  }

  if (lio_gc->cfg_name == NULL) {
     printf("ex_rw_test:  Missing config file!\n");
     flush_log(); fflush(stdout);
     abort();
  }

  //** Lastly load the R/W test params
  rw_load_options(lio_gc->cfg_name, section);


  //** Print the options to the screen
  printf("Configuration options: %s\n", section);
  printf("------------------------------------------------------------------\n");
  rw_print_options(stdout);
  printf("------------------------------------------------------------------\n\n");

  //** Open the file
  exp = exnode_exchange_load_file(rwc.filename);
  //** and parse it
  ex = exnode_create();
  exnode_deserialize(ex, exp, lio_gc->ess);

  //** Get the default view to use
  seg = exnode_get_default(ex);
  if (seg == NULL) {
     printf("No default segment!  Aborting!\n");
     flush_log(); fflush(stdout);
     abort();
  }

  //** Truncate the file
  if (segment_size(seg) > 0) {
     err = gop_sync_exec(segment_truncate(seg, da, 0, 10));
     if (err != OP_STATE_SUCCESS) {
        printf("Error truncating the remote file!\n");
        flush_log(); fflush(stdout);
        abort();
     }
  }

  //** Now do the test
  rw_test();


  if (print_exnode == 1) {
    exp_out = exnode_exchange_create(EX_TEXT);
    exnode_serialize(ex, exp_out);

    printf("--------------------- Final Exnode ----------------------\n");
    printf("%s", exp_out->text.text);
    printf("---------------------------------------------------------\n");
  }

  //** Get the error counts
  gop = segment_inspect(seg, da, lio_ifd, INSPECT_SOFT_ERRORS, 0, NULL, 10);
  gop_waitall(gop);
  status = gop_get_status(gop);
  soft_errors = status.error_code;
  gop_free(gop, OP_DESTROY);

  gop = segment_inspect(seg, da, lio_ifd, INSPECT_HARD_ERRORS, 0, NULL, 10);
  gop_waitall(gop);
  status = gop_get_status(gop);
  hard_errors = status.error_code;
  gop_free(gop, OP_DESTROY);

  printf("---------------------Error counts------------------------\n");
  printf("Hard errors: %d\n", hard_errors);
  printf("Soft errors: %d\n", soft_errors);
  printf("---------------------------------------------------------\n");

  //** Truncate the file to back to 0
  err = gop_sync_exec(segment_truncate(seg, da, 0, 10));
  if (err != OP_STATE_SUCCESS) {
     printf("Error truncating the file!\n");
     flush_log(); fflush(stdout);
     abort();
  }

  //** Shut everything down
  exnode_destroy(ex);
  exnode_exchange_destroy(exp);

printf("tpc_unlimited=%d\n", lio_gc->tpc_unlimited->n_ops);

  lio_shutdown();

  return(0);
}


