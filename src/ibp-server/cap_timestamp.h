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

#include "allocation.h"

#define set_read_timestamp(a, add, offset, size, id) set_rw_ts((a)->read_ts, &((a)->read_slot), add, offset, size, id)
#define set_write_timestamp(a, add, offset, size, id) set_rw_ts((a)->write_ts, &((a)->write_slot), add, offset, size, id)
#define set_manage_timestamp(a, add, cmd, subcmd, reliability, expire, size, pid) set_manage_ts((a)->manage_ts, &((a)->manage_slot), add, cmd, subcmd, reliability, expire, size, pid)

void set_alloc_timestamp(Allocation_timestamp_t *ts, Allocation_address_t *add);
void set_rw_ts(Allocation_rw_ts_t *ts, int *slot, Allocation_address_t *add, uint64_t offset, uint64_t size, osd_id_t id);
void set_manage_ts(Allocation_manage_ts_t *ts, int *slot, Allocation_address_t *add, int cmd, int subcmd, 
        int reliability, uint32_t expiration, uint64_t size, osd_id_t pid);


