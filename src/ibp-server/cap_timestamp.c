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

#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include "allocation.h"
#include <tbx/log.h>
#include "ibp_time.h"

//*************************************************************************
// set_alloc_timestamp - Stores a timestamp
//*************************************************************************

void set_alloc_timestamp(Allocation_timestamp_t *ts, Allocation_address_t *add)
{
  memcpy(&(ts->host), add, sizeof(Allocation_address_t));
  ts->time = ibp_time_now();
}

//*************************************************************************
// set_alloc_timestamp - Stores a timestamp
//*************************************************************************

void set_rw_ts(Allocation_rw_ts_t *tsarray, int *slot, Allocation_address_t *add, uint64_t offset, uint64_t size, osd_id_t id)
{
  Allocation_rw_ts_t *ts;

  //** Make sure the initial slot is valid
  *slot = *slot % ALLOC_HISTORY;
  if (*slot < 0) *slot = 0;

  ts = &(tsarray[*slot]);
  set_alloc_timestamp(&(ts->ts), add);
  ts->offset = offset;
  ts->size = size;
  ts->id = id;

  (*slot) = (*slot + 1) % ALLOC_HISTORY;
}

//*************************************************************************
// set_manage_ts - Stores a timestamp
//*************************************************************************

void set_manage_ts(Allocation_manage_ts_t *tsarray, int *slot, Allocation_address_t *add, int cmd, int subcmd, 
        int reliability, uint32_t expiration, uint64_t size, osd_id_t pid)
{
  Allocation_manage_ts_t *ts;

  //** Make sure the initial slot is valid
  *slot = *slot % ALLOC_HISTORY;
  if (*slot < 0) *slot = 0;

  ts = &(tsarray[*slot]);
  set_alloc_timestamp(&(ts->ts), add);
  ts->cmd = cmd;
  ts->subcmd = subcmd;
  ts->reliability = reliability;
  ts->expiration = expiration;
  ts->size = size;
  ts->id = pid;

  (*slot) = (*slot + 1) % ALLOC_HISTORY;
}


