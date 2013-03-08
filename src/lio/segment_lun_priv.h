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
// LUN segment support
//***********************************************************************

#ifndef _SEGMENT_LUN_PRIV_H_
#define _SEGMENT_LUN_PRIV_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  ex_off_t used_size;
  ex_off_t total_size;
  ex_off_t max_block_size;
  ex_off_t excess_block_size;
  ex_off_t max_row_size;
  ex_off_t chunk_size;
  ex_off_t stripe_size;
  apr_time_t grow_time;
  rs_query_t *rsq;
  thread_pool_context_t *tpc;
  int grow_count;
  int n_devices;
  int n_shift;
  int hard_errors;
  int grow_break;
  int map_version;
  int inprogress_count;
  rs_mapping_notify_t notify;
  interval_skiplist_t *isl;
  resource_service_fn_t *rs;
  data_service_fn_t *ds;
} seglun_priv_t;

#ifdef __cplusplus
}
#endif

#endif

