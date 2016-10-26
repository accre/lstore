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


