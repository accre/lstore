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


