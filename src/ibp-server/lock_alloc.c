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

//******************************************************************
//******************************************************************

#include <assert.h>
#include <time.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <apr_thread_mutex.h>
#include <apr_pools.h>
#include "osd_abstract.h"
#include <tbx/fmttypes.h>
#include <tbx/log.h>
#include "lock_alloc.h"

//******** These are for the directory splitting ****
#define LOCK_BITS    8
#define LOCK_BITMASK 0x00FF
#define LOCK_MAX     256

#define id_slot(id) (id & LOCK_BITMASK)

apr_thread_mutex_t *_lock_table[LOCK_MAX];
apr_pool_t  *_lock_pool;

//******************************************************************
//  lock_osd_id - Locks the ID.  Blocks until a lock can be acquired
//******************************************************************

void lock_osd_id(osd_id_t id)
{
   apr_thread_mutex_lock(_lock_table[id_slot(id)]);
}

//******************************************************************
//  unlock_osd_id - Unlocks the ID.
//******************************************************************

void unlock_osd_id(osd_id_t id)
{
   apr_thread_mutex_unlock(_lock_table[id_slot(id)]);
}

//******************************************************************
//  lock_osd_id_pair - Locks a pair of IDs.  Blocks until a lock can be acquired
//******************************************************************

void lock_osd_id_pair(osd_id_t id1, osd_id_t id2)
{
   int slot1 = id_slot(id1);
   int slot2 = id_slot(id2);

   if (slot1 == slot2) {
      apr_thread_mutex_lock(_lock_table[slot1]);
   } else if (slot1 > slot2) {
      apr_thread_mutex_lock(_lock_table[slot1]);
      apr_thread_mutex_lock(_lock_table[slot2]);
   } else {
      apr_thread_mutex_lock(_lock_table[slot2]);
      apr_thread_mutex_lock(_lock_table[slot1]);
   }
}

//******************************************************************
//  unlock_osd_id_pair - Unlocks the ID pair.
//******************************************************************

void unlock_osd_id_pair(osd_id_t id1, osd_id_t id2)
{
   int slot1 = id_slot(id1);
   int slot2 = id_slot(id2);

   if (slot1 == slot2) {
      apr_thread_mutex_unlock(_lock_table[slot1]);
   } else if (slot1 > slot2) {
      apr_thread_mutex_unlock(_lock_table[slot2]);
      apr_thread_mutex_unlock(_lock_table[slot1]);
   } else {
      apr_thread_mutex_unlock(_lock_table[slot1]);
      apr_thread_mutex_unlock(_lock_table[slot2]);
   }
}

//******************************************************************
//  lock_alloc_init - Initializes the allocation locking routines
//******************************************************************

void lock_alloc_init()
{
  int i;

  apr_pool_create(&_lock_pool, NULL);

  for (i=0; i<LOCK_MAX; i++) {
     apr_thread_mutex_create(&(_lock_table[i]), APR_THREAD_MUTEX_DEFAULT, _lock_pool);
  }
}

//******************************************************************
//  lock_alloc_destroy - Destroys the allocation locking data
//******************************************************************

void lock_alloc_destroy()
{
  int i;

  for (i=0; i<LOCK_MAX; i++) {
     apr_thread_mutex_destroy(_lock_table[i]);
  }

  apr_pool_destroy(_lock_pool);
}


