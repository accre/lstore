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

#ifndef _SEGMENT_LOG_PRIV_H_
#define _SEGMENT_LOG_PRIV_H_

#include "interval_skiplist.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  ex_off_t lo;
  ex_off_t hi;  //** On disk this is actually the length.  It's converted to an offset when loaded and a length when stored
  ex_off_t data_offset;
} slog_range_t;

typedef struct {
  segment_t *table_seg;
  segment_t *data_seg;
  segment_t *base_seg;
  data_service_fn_t *ds;
  interval_skiplist_t *mapping;
  thread_pool_context_t *tpc;
  ex_off_t file_size;
  ex_off_t log_size;
  ex_off_t data_size;
  int soft_errors;
  int hard_errors;
} seglog_priv_t;

#ifdef __cplusplus
}
#endif

#endif

