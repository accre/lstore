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
//  Routines for testing the various segment_log routines
//***********************************************************************

// Verification tasks
//
// -Generate a base with an empty log and read back
// -Clone the base structure and the use segment_copy to copy the data and verify.
// -Write to the log and read back
// -merge_with base and verify
// -Write to log
// -Replace the clones base with current log(A)
// -Write to the clone and verify B+A+base
// -clone2 = clone (structure and data). Verify the contents
// -Clone A's structure again and call it C.
// -Replace C's base with B.
//    Write to C and verify C+B+A+base changes
// -Clone C's structure and data and verify


#include <assert.h>
#include "exnode.h"
#include "log.h"
#include "iniparse.h"
#include "type_malloc.h"
#include "thread_pool.h"
#include "segment_log_priv.h"

//*************************************************************************
// compare_buffers_print - FInds the 1st index where the buffers differ
//*************************************************************************

int compare_buffers_print(char *b1, char *b2, int len, ex_off_t offset)
{
  int i, k, mode, last, ok, err, last_byte;
  ex_off_t start, end;

  err = 0;
  mode = (b1[0] == b2[0]) ? 0 : 1;
  start = offset;
  last = len - 1;

  log_printf(0, "Printing comparision breakdown -- Single byte matches are suppressed (len=%d)\n", len);
b1[len] = 0; b2[len]=0;
log_printf(15, "b1=%s\n", b1);
log_printf(15, "b2=%s\n", b2);

  for (i=0; i<len; i++) {
    if (mode == 0) {  //** Matching range
      if ((b1[i] != b2[i]) || (last == i)) {
         last_byte = ((last == i) && (b1[i] == b2[i])) ? 1 : 0;
         end = offset + i-1 + last_byte;
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
            last_byte = ((last == i) && (b1[i] != b2[i])) ? 1 : 0;
            end = offset + i-1 + last_byte;
            k = end - start + 1;
            log_printf(0, "  DIFFER: %d -> %d (%d bytes)\n", start, end, k);

            start = offset + i;
            mode = 0;
            err = 1;
         }
      }
    }
  }

//i=(b1[last] == b2[last]) ? 0 : 1;
//log_printf(0, "last compare=%d lst=%d\n", i, last);

  flush_log();
  return(err);
}


//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
  int chunk_size = 10;
  int n_chunks = 10;
  int bufsize= n_chunks * chunk_size;
  char base_data[bufsize+1];
  char buffer[bufsize+1];
  char log1_data[bufsize+1];
  char log2_data[bufsize+1];
  char log3_data[bufsize+1];
  tbuffer_t tbuf;
  ex_iovec_t ex_iov, ex_iov_table[n_chunks];
  int i, err, start_option;
  int timeout, ll;
  ibp_context_t *ic;
  inip_file_t *ifd;
  data_service_fn_t *ds = NULL;
  resource_service_fn_t *rs = NULL;
  thread_pool_context_t *tpc_unlimited = NULL;
  thread_pool_context_t *tpc_cpu = NULL;
  cache_t *cache = NULL;
  data_attr_t *da;
  char *fname = NULL;
  char *cfg_name = NULL;
  char *ctype;
  exnode_t *ex;
  exnode_exchange_t *exp;
  segment_t *seg, *clone, *clone2, *clone3;
  seglog_priv_t *s;
  opque_t *q;

  if (argc < 2) {
     printf("\n");
     printf("log_test [-d log_level] [-c system.cfg] log.ex3\n");
     printf("    log.ex3 - Log file to use.  IF the file is not empty all it's contents are truncated\n");
     printf("    -c system.cfg   - IBP and Cache configuration options\n");
     printf("\n");
     return(1);
  }

//set_log_level(20);
  tpc_unlimited = thread_pool_create_context("UNLIMITED", 0, 2000);
  tpc_cpu = thread_pool_create_context("CPU", 0, 0);
  rs = NULL;
  ic = ibp_create_context();  //** Initialize IBP
  ds = ds_ibp_create(ic);
  da = ds_attr_create(ds);
  cache_system_init();
  timeout = 120;
  ll = -1;

  //*** Parse the args
  i=1;
  do {
     start_option = i;

     if (strcmp(argv[i], "-d") == 0) { //** Enable debugging
        i++;
        ll = atoi(argv[i]); i++;
     } else if (strcmp(argv[i], "-c") == 0) { //** Load the config file
        i++;
        cfg_name = argv[i]; i++;
     }

  } while (start_option < i);

  if (cfg_name != NULL) {
     ibp_load_config(ic, cfg_name);

     ifd = inip_read(cfg_name);
     ctype = inip_get_string(ifd, "cache", "type", CACHE_LRU_TYPE);
     inip_destroy(ifd);
     cache = load_cache(ctype, da, timeout, cfg_name);
     free(ctype);

     mlog_load(cfg_name);
     if (rs == NULL) rs = rs_simple_create(cfg_name, ds);
  } else {
     cache = create_cache(CACHE_LRU_TYPE, da, timeout);
  }

  if (ll > -1) set_log_level(ll);

  exnode_system_init(ds, rs, NULL, tpc_unlimited, tpc_cpu, cache);

  //** This is the remote file to download
  fname = argv[i]; i++;
  if (fname == NULL) {
    printf("Missing log file!\n");
    return(2);
  }

  //** Load it
  exp = exnode_exchange_load_file(fname);

  //** and parse the remote exnode
  ex = exnode_create();
  exnode_deserialize(ex, exp);

  //** Get the default view to use
  seg = exnode_get_default(ex);
  if (seg == NULL) {
     printf("No default segment!  Aborting!\n");
     abort();
  }
  s = (seglog_priv_t *)seg->priv;

  //** Verify the type
  if (strcmp(segment_type(seg), SEGMENT_TYPE_LOG) != 0) {
     printf("Invalid exnode type.  Segment should be a single level log but got a type of %s\n", segment_type(seg));
     abort();
  }

  //** Now get the base type.  It should NOT be a log
  if (strcmp(segment_type(s->base_seg), SEGMENT_TYPE_LOG) == 0) {
     printf("Log segments base should NOT be another log segment!\n");
     abort();
  }


  //** Truncate the log and base
  q = new_opque();
  opque_add(q, segment_truncate(s->table_seg, da, 0, timeout));
  opque_add(q, segment_truncate(s->data_seg, da, 0, timeout));
  opque_add(q, segment_truncate(s->base_seg, da, 0, timeout));
  err = opque_waitall(q);
  if (err != OP_STATE_SUCCESS) {
     printf("Error with truncate of initial log segment!\n");
     abort();
  }
  s->file_size = 0;
  s->data_size = 0;
  s->log_size = 0;

  //*************************************************************************
  //--------------------- Testing starts here -------------------------------
  //*************************************************************************

  //*************************************************************************
  //------- Generate a base with an empty log and read back -----------------
  //*************************************************************************
  //** Make the base buffer and write it
  memset(base_data, 'B', bufsize);  base_data[bufsize] = '\0';
  tbuffer_single(&tbuf, bufsize, base_data);
  ex_iovec_single(&ex_iov, 0, bufsize);
  assert(gop_sync_exec(segment_write(s->base_seg, da, 1, &ex_iov, &tbuf, 0, timeout)) == OP_STATE_SUCCESS);

  s->file_size = bufsize;  //** Since we're peeking we have to adjust the file size
  tbuffer_single(&tbuf, bufsize, buffer);  //** Read it directly back fro mthe base to make sure that works
  assert(gop_sync_exec(segment_read(s->base_seg, da, 1, &ex_iov, &tbuf, 0, timeout)) == OP_STATE_SUCCESS);
  assert(strcmp(buffer, base_data) == 0);

  //** Do the same for the log
  assert(gop_sync_exec(segment_read(seg, da, 1, &ex_iov, &tbuf, 0, timeout)) == OP_STATE_SUCCESS);
  assert(compare_buffers_print(buffer, base_data, bufsize, 0) == 0);

  //*************************************************************************
  //-- Clone the base structure and the use segment_copy to copy the data and verify --
  //*************************************************************************
  assert(gop_sync_exec(segment_clone(seg, da, &clone, CLONE_STRUCTURE, NULL, timeout)) == OP_STATE_SUCCESS);
  assert(gop_sync_exec(segment_copy(da, seg, clone, 0, 0, bufsize, chunk_size, buffer, timeout)) == OP_STATE_SUCCESS);
  memset(buffer, 0, bufsize);
  assert(gop_sync_exec(segment_read(clone, da, 1, &ex_iov, &tbuf, 0, timeout)) == OP_STATE_SUCCESS);
  assert(compare_buffers_print(buffer, base_data, bufsize, 0) == 0);

  //*************************************************************************
  //-------------------- Write to the log and read back ---------------------
  //*************************************************************************
  //** We are writing 1's to the even chunks
  memcpy(log1_data, base_data, bufsize);
  memset(buffer, '1', chunk_size);
  for (i=0; i<n_chunks; i+=2) {
     memcpy(&(log1_data[i*chunk_size]), buffer, chunk_size);
     ex_iovec_single(&(ex_iov_table[i]), i*chunk_size, chunk_size);
     opque_add(q, segment_write(seg, da, 1, &(ex_iov_table[i]), &tbuf, 0, timeout));
  }
  assert(opque_waitall(q) == OP_STATE_SUCCESS);

  //** Read it back
  memset(buffer, 0, bufsize);
  assert(gop_sync_exec(segment_read(seg, da, 1, &ex_iov, &tbuf, 0, timeout)) == OP_STATE_SUCCESS);
  assert(compare_buffers_print(buffer, log1_data, bufsize, 0) == 0);

  //*************************************************************************
  //------------------- Merge_with base and verify --------------------------
  //*************************************************************************
  assert(gop_sync_exec(slog_merge_with_base(seg, da, chunk_size, buffer, 1, timeout)) == OP_STATE_SUCCESS);

  //** Read it back
  memset(buffer, 0, bufsize);
  assert(gop_sync_exec(segment_read(seg, da, 1, &ex_iov, &tbuf, 0, timeout)) == OP_STATE_SUCCESS);
  assert(compare_buffers_print(buffer, log1_data, bufsize, 0) == 0);

  //*************************************************************************
  //--------------- Write to the new empty log and verify -------------------
  //*************************************************************************
  //** We are writing 2's to *most* of the odd chunks
  memcpy(log1_data, buffer, bufsize);
  memset(buffer, '2', chunk_size);
  for (i=1; i<n_chunks; i+=2) {
     memcpy(&(log1_data[i*chunk_size]), buffer, chunk_size);
     ex_iovec_single(&(ex_iov_table[i]), i*chunk_size, chunk_size);
     opque_add(q, segment_write(seg, da, 1, &(ex_iov_table[i]), &tbuf, 0, timeout));
  }
  assert(opque_waitall(q) == OP_STATE_SUCCESS);

  //** Read it back
  memset(buffer, 0, bufsize);
  assert(gop_sync_exec(segment_read(seg, da, 1, &ex_iov, &tbuf, 0, timeout)) == OP_STATE_SUCCESS);
  assert(compare_buffers_print(buffer, log1_data, bufsize, 0) == 0);

  //*************************************************************************
  //---------- Replace the clones base with seg(Log1) and verify ------------
  //*************************************************************************
  assert(gop_sync_exec(segment_remove(clone, da, timeout)) == OP_STATE_SUCCESS);
  segment_destroy(clone);
  assert(gop_sync_exec(segment_clone(seg, da, &clone, CLONE_STRUCTURE, NULL, timeout)) == OP_STATE_SUCCESS);

  s = (seglog_priv_t *)clone->priv;

  s->base_seg = seg;
  s->file_size = segment_size(seg);

  //** Read it back
  memset(buffer, 0, bufsize);
  assert(gop_sync_exec(segment_read(clone, da, 1, &ex_iov, &tbuf, 0, timeout)) == OP_STATE_SUCCESS);
  assert(compare_buffers_print(buffer, log1_data, bufsize, 0) == 0);

  //*************************************************************************
  //---------- Write to the clones log and verify (now have 2 logs) ---------
  //*************************************************************************
  memcpy(log2_data, log1_data, bufsize);
  memset(buffer, '3', 1.5*chunk_size);
  for (i=0; i<n_chunks; i+=4) {
     memcpy(&(log2_data[i*chunk_size]), buffer, 1.5*chunk_size);
     ex_iovec_single(&(ex_iov_table[i]), i*chunk_size, 1.5*chunk_size);
     opque_add(q, segment_write(clone, da, 1, &(ex_iov_table[i]), &tbuf, 0, timeout));
  }
  assert(opque_waitall(q) == OP_STATE_SUCCESS);

  //** Read it back
  memset(buffer, 0, bufsize);
  assert(gop_sync_exec(segment_read(clone, da, 1, &ex_iov, &tbuf, 0, timeout)) == OP_STATE_SUCCESS);
  assert(compare_buffers_print(buffer, log2_data, bufsize, 0) == 0);

  //*************************************************************************
  //---- clone2 = clone (structure and data). Verify the contents -----------
  //*************************************************************************
  assert(gop_sync_exec(segment_clone(clone, da, &clone2, CLONE_STRUCT_AND_DATA, NULL, timeout)) == OP_STATE_SUCCESS);
  memset(buffer, 0, bufsize);
  assert(gop_sync_exec(segment_read(clone2, da, 1, &ex_iov, &tbuf, 0, timeout)) == OP_STATE_SUCCESS);
  assert(compare_buffers_print(buffer, log2_data, bufsize, 0) == 0);

  //** We don't need this anymore so destroy it
  assert(gop_sync_exec(segment_remove(clone2, da, timeout)) == OP_STATE_SUCCESS);
  segment_destroy(clone2);

  //*************************************************************************
  //---------------- Clone2 = clone's structure *only* ----------------------
  //*************************************************************************
  assert(gop_sync_exec(segment_clone(clone, da, &clone2, CLONE_STRUCTURE, NULL, timeout)) == OP_STATE_SUCCESS);

  //*************************************************************************
  //-------------- Replace clone2's base with clone and verify --------------
  //*************************************************************************
  s = (seglog_priv_t *)clone2->priv;
  assert(gop_sync_exec(segment_remove(s->base_seg, da, timeout)) == OP_STATE_SUCCESS);
  segment_destroy(s->base_seg);

  s->base_seg = clone;
  s->file_size = segment_size(clone);

  //** Read it back
  memset(buffer, 0, bufsize);
  assert(gop_sync_exec(segment_read(clone2, da, 1, &ex_iov, &tbuf, 0, timeout)) == OP_STATE_SUCCESS);
  assert(compare_buffers_print(buffer, log2_data, bufsize, 0) == 0);

  //*************************************************************************
  //----------- Write to Clone2 and verify (now have 3 logs) ----------------
  //*************************************************************************
  memcpy(log3_data, log2_data, bufsize);
  memset(buffer, '4', chunk_size);
  for (i=0; i<n_chunks; i+=2) {
     memcpy(&(log3_data[i*chunk_size + chunk_size/3]), buffer, chunk_size);
     ex_iovec_single(&(ex_iov_table[i]), i*chunk_size + chunk_size/3, chunk_size);
     opque_add(q, segment_write(clone2, da, 1, &(ex_iov_table[i]), &tbuf, 0, timeout));
  }
  assert(opque_waitall(q) == OP_STATE_SUCCESS);

  //** Read it back
  memset(buffer, 0, bufsize);
  assert(gop_sync_exec(segment_read(clone2, da, 1, &ex_iov, &tbuf, 0, timeout)) == OP_STATE_SUCCESS);
  assert(compare_buffers_print(buffer, log3_data, bufsize, 0) == 0);

  //*************************************************************************
  // -- clone3 = clone2 structure and contents and verify
  //*************************************************************************
  assert(gop_sync_exec(segment_clone(clone2, da, &clone3, CLONE_STRUCT_AND_DATA, NULL, timeout)) == OP_STATE_SUCCESS);
  memset(buffer, 0, bufsize);
  assert(gop_sync_exec(segment_read(clone3, da, 1, &ex_iov, &tbuf, 0, timeout)) == OP_STATE_SUCCESS);
  assert(compare_buffers_print(buffer, log3_data, bufsize, 0) == 0);

  //*************************************************************************
  //--------------------- Testing Finished -------------------------------
  //*************************************************************************

  //** Clean up
  assert(gop_sync_exec(segment_remove(clone3, da, timeout)) == OP_STATE_SUCCESS);
  assert(gop_sync_exec(segment_remove(clone2, da, timeout)) == OP_STATE_SUCCESS);

  segment_destroy(clone3);
  segment_destroy(clone2);
  segment_destroy(seg);


  exnode_exchange_destroy(exp);

  exnode_system_destroy();
  cache_destroy(cache);
  cache_system_destroy();

  ds_attr_destroy(ds, da);
  ds_destroy_service(ds);
  ibp_destroy_context(ic);
  thread_pool_destroy_context(tpc_unlimited);
  thread_pool_destroy_context(tpc_cpu);

  return(0);
}
