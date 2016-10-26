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

//*******************************************

#define _XOPEN_SOURCE 600
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <apr_time.h>
#include <math.h>
#include <tbx/string_token.h>
#include "osd_abstract.h"
#include "osd_fs.h"
#include <tbx/fmttypes.h>
#include <tbx/log.h>
#include <tbx/type_malloc.h>
#ifdef _HAS_XFS
#include <xfs/xfs.h>
#endif

#define RANGE_INUSE   0
#define RANGE_REQUEST 1

#define FS_BUF_SIZE 262144
//#define FS_BUF_SIZE 65536
#define FS_MAGIC_HEADER sizeof(fs_header_t)
//#define FS_OBJ_COUNT 128
//#define FS_RANGE_COUNT 16
#define FS_OBJ_COUNT 64
#define FS_RANGE_COUNT 4

#define _id2fname(fs, id, fname, len) (fs)->id2fname(fs, id, fname, len)
#define _trashid2fname(fs, trash_type, trash_id, fname, len) (fs)->trashid2fname(fs, trash_type, trash_id, fname, len)

osd_off_t fs_offset_l2p(osd_fs_t *fs, osd_fs_fd_t *fsfd, osd_off_t l_offset);
osd_off_t fs_offset_p2l(osd_fs_t *fs, osd_fs_fd_t *fsfd, osd_off_t p_offset);
osd_fd_t *fs_open(osd_t *d, osd_id_t id, int mode);
int fs_close(osd_t *d, osd_fd_t *ofd);
osd_off_t fs_normal_read(osd_fs_t *fs, osd_fs_fd_t *fsfd, osd_off_t offset, osd_off_t len, buffer_t buffer);
osd_off_t fs_chksum_read(osd_fs_t *fs, osd_fs_fd_t *fsfd, osd_off_t block, osd_off_t len, buffer_t buffer);
osd_off_t fs_normal_write(osd_fs_t *fs, osd_fs_fd_t *fsfd, osd_off_t offset, osd_off_t len, buffer_t buffer);
osd_off_t fs_chksum_write(osd_fs_t *fs, osd_fs_fd_t *fsfd, osd_off_t block, osd_off_t len, buffer_t buffer);
int _fs_calculate_chksum(osd_fs_t *fs, osd_fs_fd_t *fsfd, tbx_chksum_t *cs, osd_off_t block, char *buffer, osd_off_t bufsize, char *calc_chksum, int correct_errors);
osd_off_t _chksum_buffered_read(osd_fs_t *fs, osd_fs_fd_t *fsfd, osd_off_t len, char *buffer, osd_off_t bufsize, tbx_chksum_t *cs1, tbx_chksum_t *cs2);

#define PTR2INT(p) (int)(long)(p)
#define INT2PTR(n) (void *)(long)(n)
#define fs_cache_lock(c) apr_thread_mutex_lock((c)->lock)
#define fs_cache_unlock(c) apr_thread_mutex_unlock((c)->lock)

//******************************************************************************
// fs_chksum_add - Add data to the ruhnning chksum
//******************************************************************************

int fs_chksum_add(tbx_chksum_t *cs, int size, char *data)
{
    tbx_tbuf_t tbuf;

    tbx_tbuf_single(&tbuf, size, data);
    return(tbx_chksum_add(cs, size, &tbuf, 0));
}

//******************************************************************************
// fs_cache_create - Creates an FS cache object
//******************************************************************************

fs_cache_table_t *fs_cache_create(apr_pool_t *pool, int n_blocks, int block_size, apr_time_t max_wait)
{
  int i;
  fs_cache_table_t *c;

  //** Check if block caching is disabled
  i = n_blocks * block_size;
  if (i == 0) return(NULL);

  tbx_type_malloc_clear(c, fs_cache_table_t, 1);

  apr_thread_mutex_create(&(c->lock), APR_THREAD_MUTEX_DEFAULT, pool);
  c->hash = apr_hash_make(pool);
  c->n_total = n_blocks * block_size;
  c->block_size = block_size;
  c->n_blocks = n_blocks;
  c->max_wait = max_wait;

  tbx_type_malloc_clear(c->block_free_count, int, n_blocks);
  tbx_type_malloc_clear(c->block_last_offset, int, n_blocks);
  for (i=0; i<n_blocks; i++) c->block_free_count[i] = block_size;

  tbx_type_malloc_clear(c->table, fs_cache_entry_t, c->n_total+1);

  return(c);
}

//******************************************************************************
// fs_cache_destroy - Destroys an FS cache object
//******************************************************************************

void fs_cache_destroy(fs_cache_table_t *c)
{

  if (c == NULL) return;

  //---There is no apr_hash_destroy() must use the pool which is handled via the fs
  apr_thread_mutex_destroy(c->lock);
  free(c->block_free_count);
  free(c->block_last_offset);
  free(c->table);
  free(c);
}

//******************************************************************************
//  _fs_cache_forced_purge -NOTE:  Assumes *all* slots are used!
//******************************************************************************

int _fs_cache_forced_purge(fs_cache_table_t *c, int n_drop)
{
  int i, j, slot, start, end, drop_block, count;
  fs_cache_entry_t *element;
 
  element = c->table;
  slot= 1;  //** This slot will be dropped based on the simple algorithm

  drop_block = n_drop/c->n_blocks;
  if (drop_block <= 0) drop_block = 1;

  for (i=0; i<c->n_blocks; i++) {
    start = i*c->block_size + 1;
    end = start + drop_block;
    count = 0;
    for (j=start; j<end; j++) {
       if (element[j].time != 0) {
          apr_hash_set(c->hash, &(element[j].key), sizeof(fs_cache_key_t), NULL);
          element[j].time = 0;
          count++;
       }
    }
    c->block_free_count[i] += count;
    c->block_last_offset[i] = 0;
  }

  return(slot);
}


//******************************************************************************
//  _fs_cache_purge_expired
//******************************************************************************

int _fs_cache_purge_expired(fs_cache_table_t *c)
{
  int i, j, slot, start, end, count;
  apr_time_t now, dt;
  fs_cache_entry_t *element;
  now = apr_time_now();

  element = c->table;
  slot= 0;

  for (i=0; i<c->n_blocks; i++) {
    start = i*c->block_size + 1;
    end = start + c->block_size;
    count = 0;
    for (j=start; j<end; j++) {
       dt = now - element[j].time;
       if (dt > c->max_wait) {
         slot = j;
         if (element[j].time != 0) {
            apr_hash_set(c->hash, &(element[j].key), sizeof(fs_cache_key_t), NULL);
            element[j].time = 0;
            count++;
         }
       }
    }
    c->block_free_count[i] += count;
    c->block_last_offset[i] = 0;
  }

  return(slot);
}


//******************************************************************************
//  _fs_cache_find_free
//******************************************************************************

int _fs_cache_find_free(fs_cache_table_t *c)
{
  int i, slot, j, start, off;
  apr_time_t now, dt;
  fs_cache_entry_t *element;
  now = apr_time_now();

  element = c->table;

  for (i=0; i<c->n_blocks; i++) {
     if (c->block_free_count[i] > 0) {
        start = i*c->block_size + 1;
        off = c->block_last_offset[i];
        for (j=0; j<c->block_size; j++) {
           slot = start + ((j+off) % c->block_size);
           dt = now - element[slot].time;
           if (dt > c->max_wait) {
              if (element[slot].time != 0) {
                 apr_hash_set(c->hash, &(element[slot].key), sizeof(fs_cache_key_t), NULL);
                 c->block_free_count[i]++;
              }
              c->block_last_offset[i] = (j + off) % c->block_size;
              return(slot);
           }
        }
     }
  }

  return(0);
}

//******************************************************************************
// _fs_cache_find_slot - Finds a free slot.  If none are available then an expiration
//   run is made to clean up.  If no slots are still available then slots are
//   freed at random.
//   NOTE : Locking is done via the calling routine
//******************************************************************************

int _fs_cache_find_slot(fs_cache_table_t *c)
{
  int slot, b;

  //** Find a slot from the free list
  slot = _fs_cache_find_free(c);
log_printf(15, "_fs_cache_find_slot: _fs_cache_find_free=%d\n", slot);
  if (slot == 0) {
     //** Expire old blocks
     slot = _fs_cache_purge_expired(c);
log_printf(15, "_fs_cache_find_slot: _fs_cache_purge_expired=%d\n", slot);
     if (slot == 0) {
        //** Force removal of some of the data
        slot = _fs_cache_forced_purge(c, 0.1*c->n_total);
log_printf(15, "_fs_cache_find_slot: _fs_cache_forced_pruge=%d\n", slot);
     }
  }

  //** Update the block free count
  b = (slot-1) / c->n_blocks;
  c->block_free_count[b]--;

  return(slot);
}

//******************************************************************************
// _fs_cache_remove - Removes a slot from the cache
//   NOTE : Locking is done via the calling routine
//******************************************************************************

void _fs_cache_remove(fs_cache_table_t *c, int slot)
{
  int b;

  b = (slot-1) / c->block_size;
  c->block_free_count[b]++;

  apr_hash_set(c->hash, &(c->table[slot].key), sizeof(fs_cache_key_t), NULL);
  c->table[slot].time = 0; //** This marks the slot as free
}

//******************************************************************************
// fs_direct_cache_read - Returns 1 if the block has been recently read
//    and is Ok to bypass chksum verification.
//******************************************************************************

int fs_direct_cache_read(fs_cache_table_t *c, osd_id_t id, int block)
{
  fs_cache_key_t key;
  int slot, err;
  apr_time_t dt, now;

  if (c == NULL) return(0);  //** Check if disabled

  err = 0;
  key.id = id; key.block = block;

log_printf(15, "fs_direct_cache_read: c->hash=%p\n", c->hash);
log_printf(15, "fs_direct_cache_read: key=%p id=" LU " block=%d\n", &key, id, block);
tbx_log_flush();

  fs_cache_lock(c);
  slot = PTR2INT(apr_hash_get(c->hash, &key, sizeof(fs_cache_key_t)));
log_printf(15, "fs_direct_cache_read: id=" LU " block=%d slot=%d\n", id, block, slot); tbx_log_flush();

//  void *v = apr_hash_get(c->hash, &key, sizeof(key));
//  slot = PTR2INT(v);
//log_printf(15, "fs_direct_cache_read: id=" LU " block=%d slot=%d \n", id, block, slot); tbx_log_flush();

  if (slot != 0) {
     now = apr_time_now();
     dt = now - c->table[slot].time;
     if (dt < c->max_wait) {
        err= 1;
        c->table[slot].time = now;
     } else {
       _fs_cache_remove(c, slot);
     }
  }

  fs_cache_unlock(c);

log_printf(15, "fs_direct_cache_read: id=" LU " block=%d slot=%d err=%d\n", id, block, slot, err); tbx_log_flush();
//return(0);

  return(err);
}

//******************************************************************************
//  fs_cache_add - Adds the id/block combo to the cache
//******************************************************************************

void fs_cache_add(fs_cache_table_t *c, osd_id_t id, int block)
{
  fs_cache_key_t key;
  int slot;

  if (c == NULL) return;  //** Check if disabled

log_printf(15, "fs_cache_add: id=" LU " block=%d\n", id, block);

  key.id = id; key.block = block;

  fs_cache_lock(c);
  slot = PTR2INT(apr_hash_get(c->hash, &key, sizeof(key)));

  if (slot == 0) {
    slot = _fs_cache_find_slot(c);
log_printf(15, "fs_cache_add: id=" LU " block=%d slot=%d\n", id, block, slot);
    c->table[slot].key = key;
    c->table[slot].time = apr_time_now();
    apr_hash_set(c->hash, &(c->table[slot].key), sizeof(key), INT2PTR(slot));
  }
  fs_cache_unlock(c);
}

//******************************************************************************
// fs_cache_remove - Removes teh id/block combo from the cache
//******************************************************************************

void fs_cache_remove(fs_cache_table_t *c, osd_id_t id, int block)
{
  fs_cache_key_t key;
  int slot;

  if (c == NULL) return;  //** Check if disabled

  key.id = id; key.block = block;

  fs_cache_lock(c);
  slot = PTR2INT(apr_hash_get(c->hash, &key, sizeof(key)));
log_printf(15, "fs_cache_remove: id=" LU " block=%d slot=%d\n", id, block, slot);

  if (slot != 0) {
    //apr_hash_set(c->hash, &key, sizeof(key), NULL);
    _fs_cache_remove(c, slot);
  }
  fs_cache_unlock(c);
}


//**************************************************
// _generate_key - Generates a random 64 bit number
//**************************************************

osd_id_t _generate_key()
{
   osd_id_t r;

   r = 0;
   tbx_random_get_bytes(&r, sizeof(osd_id_t));

   return(r);
}

//**************************************************
// id2fname - Converts an id to a filename
//     The filename is stored in fname. Additionally
//     the return is also fname.
//**************************************************

char *id2fname_normal(osd_fs_t *fs, osd_id_t id, char *fname, int len) 
{
   int dir = (id & DIR_BITMASK);

   snprintf(fname, len, "%s/%d/" LU "", fs->devicename, dir, id);

//   printf("_id2fname: fname=%s\n", fname);
 
   return(fname);
}

//**************************************************

char *id2fname_loopback(osd_fs_t *fs, osd_id_t id, char *fname, int len) 
{
   if (id >= FS_MAX_LOOPBACK) return(NULL);
   return(fs->id_map[id]);
}

//**************************************************
// trashid2fname - Converts a general id to a filename
//     The filename is stored in fname. Additionally
//     the return is also fname.
//**************************************************

char *trashid2fname_normal(osd_fs_t *fs, int trash_type, const char *trash_id, char *fname, int len) 
{
   switch (trash_type) {
      case OSD_EXPIRE_ID:
           snprintf(fname, len, "%s/expired_trash/%s", fs->devicename, trash_id);
           break;
      case OSD_DELETE_ID:
           snprintf(fname, len, "%s/deleted_trash/%s", fs->devicename, trash_id);
           break;
      default:
           log_printf(0, "_trashid2fname: Invalid trash_type=%d trash_id=%s\n", trash_type, trash_id);
           fname[0] = '\0';
   }

//   printf("_trashid2fname: fname=%s\n", fname);
 
   return(fname);
}

//**************************************************

char *trashid2fname_loopback(osd_fs_t *fs, int trash_type, const char *trash_id, char *fname, int len) 
{
//   return((const char *)trash_id);  //** A little sloppy....
  return(NULL);
}

//******************************************************************************
// fs_associate_id - Associates an ID with a local filename.  Only used with
//     loopback mounts.  id must be between 0..FS_MAX_LOOPBACK
//******************************************************************************

int fs_associate_id(osd_t *d, int id, char *name)
{
  osd_fs_t *fs = (osd_fs_t *)(d->private);

  if (id >= FS_MAX_LOOPBACK) return(-1);

  fs->id_map[id] = name;
  return(0);
}

//******************************************************************************
// fs_corrupt_count - Returns the number of corrupt objects encountered during
//    execution
//******************************************************************************

int fs_corrupt_count(osd_t *d)
{
  osd_fs_t *fs = (osd_fs_t *)(d->private);
  int n;

  apr_thread_mutex_lock(fs->obj_lock);
  n = apr_hash_count(fs->corrupt_hash);
  apr_thread_mutex_unlock(fs->obj_lock);

  return(n);
}

//******************************************************************************
//  _insert_corrupt_hash - Inserts a corrupt object into the table.
//     NOTE:  Assumes fs->obj_lock has been acquired exgternally
//******************************************************************************

void _insert_corrupt_hash(osd_fs_t *fs, osd_id_t id, const char *value)
{
  osd_id_t *id2;
  char *v = apr_hash_get(fs->corrupt_hash, &id, sizeof(osd_id_t));

  log_printf(1, "_insert_corrupt_hash: fs=%s id=" LU " v=%s\n", fs->devicename, id, value);

  if (v == NULL) {  //** Need to add it
     id2 = (osd_id_t *)apr_palloc(fs->pool, sizeof(osd_id_t));
     *id2 = id;
     apr_hash_set(fs->corrupt_hash, id2, sizeof(osd_id_t), value);
  }
}

//*************************************************************
//  fs_new_corrupt_iterator - Creates a new iterator to walk through the files
//*************************************************************

osd_iter_t *fs_new_corrupt_iterator(osd_t *d)
{
   osd_iter_t *oi = (osd_iter_t *)malloc(sizeof(osd_iter_t));
   osd_fs_corrupt_iter_t *ci = (osd_fs_corrupt_iter_t *)malloc(sizeof(osd_fs_corrupt_iter_t));

   if (oi == NULL) return(NULL);
   if (ci == NULL) return(NULL);

   apr_pool_create(&(ci->pool), NULL);
   ci->iter = NULL;
   ci->first_time = 1;
   ci->fs = (osd_fs_t *)d->private;

   oi->d = d;
   oi->arg = (void *)ci;

   return(oi);
}

//*************************************************************
//  fs_corrupt_destroy_iterator - Destroys an iterator
//*************************************************************

void fs_destroy_corrupt_iterator(osd_iter_t *oi)
{
  osd_fs_corrupt_iter_t *iter = (osd_fs_corrupt_iter_t *)oi->arg;

  if (iter == NULL) return; 

  apr_pool_destroy(iter->pool);

  free(iter);
  free(oi);
}

//*************************************************************
//  corrupt_iterator_next - Returns the next key for the iterator
//*************************************************************

int fs_corrupt_iterator_next(osd_iter_t *oi, osd_id_t *id)
{
  osd_fs_corrupt_iter_t *ci = (osd_fs_corrupt_iter_t *)oi->arg;
  osd_id_t *cid;
  apr_ssize_t klen;

  if (ci->first_time == 1) {
     ci->first_time = 0;
     ci->iter = apr_hash_first(ci->pool, ci->fs->corrupt_hash);
  } else {
     ci->iter = apr_hash_next(ci->iter);    
  }

  if (ci->iter == NULL) return(1);

  apr_hash_this(ci->iter, (const void **)&cid, &klen, NULL);
  *id = *cid;
  return(0); 
}

//******************************************************************************
// fs_mapping - Maps the given byte range to the chksum blocks
//******************************************************************************

int fs_mapping(osd_fs_fd_t *fsfd, osd_off_t offset, osd_off_t len, 
               osd_off_t *start_block, osd_off_t *end_block, osd_off_t *start_offset, 
               osd_off_t *start_size, osd_off_t *end_size)
{
  osd_fs_chksum_t *fcs = &(fsfd->obj->fd_chksum);
  osd_off_t n;

  //** Do the start block first
  if (offset < fcs->header_blocksize) {
     *start_block = 0;
     *start_offset = offset;
//     *start_size = ((len+offset) > fcs->header_blocksize) ? fcs->header_blocksize - offset : len;
     *start_size = ((len + *start_offset) > fcs->header_blocksize) ? fcs->header_blocksize - offset : len;
  } else {
     n = offset - fcs->header_blocksize;
     *start_block = (n / fcs->blocksize) + 1;
     *start_offset = n % fcs->blocksize;
     *start_size = ((len + *start_offset) > fcs->blocksize) ? fcs->blocksize - *start_offset : len;
  }

  //** Now handle the final block
  n = offset + len - 1;  //** This is the logical location for the final byte
  if (len == 0) n++;
  if (n < fcs->header_blocksize) {
     *end_block = 0;
     *end_size = len;
  } else {
     n = n - fcs->header_blocksize;  //** Do things relative to the 1st data block
     *end_block = (n / fcs->blocksize) + 1; //** +1 is for the offset block 0
     *end_size = (n % fcs->blocksize) + 1;  //** The +1 converts the modulo offset to a len
//     if (*end_size == 0) { (*end_block)--; *end_size = fcs->blocksize; }
//log_printf(10, "fs_mapping: fsfd=%p hbs=" I64T " bs=" I64T " n=" I64T "\n", fsfd, fcs->header_blocksize, fcs->blocksize, n);
  }

  return(0);
}

//******************************************************************************
// delete_range - Removes the range from the list
//******************************************************************************

int delete_range(osd_fs_fd_t *fsfd)
{
  return(tbx_pch_release(fsfd->my_range_coop, &(fsfd->my_range_slot)));
}

//******************************************************************************
// insert_range - Inserts the range into the coop
//******************************************************************************

int insert_range(tbx_pc_t *coop, osd_fs_fd_t *fsfd, int rtype, int64_t timestamp, osd_off_t start_block, osd_off_t end_block)
{
  osd_fs_range_t *r;

  fsfd->my_range_coop = coop;
  fsfd->my_range_slot = tbx_pch_reserve(coop);
  r = tbx_pch_data(&(fsfd->my_range_slot));

  if (r == NULL) {
     log_printf(0, "insert_range:  No slot acquired!!!\n");
     return(1);
  }

  r->fsfd = fsfd;
  r->type = rtype;
  r->timestamp = timestamp;
  r->lo = start_block;
  r->hi = end_block;
  
  return(0);
}

//******************************************************************************
// check_range_overlap - Attempts to secure a the given range if possible.
//     If the full range is not available a shortened range is returned
//     in end_block.  If upon success 0 is returned if no range is available 
//     1 is returned
//******************************************************************************

int check_range_overlap(tbx_pc_t *coop, int64_t timestamp, osd_off_t start_block, osd_off_t *end_block)
{
   tbx_pc_iter_t it;
   tbx_pch_t i;
   osd_fs_range_t *r;

   it = tbx_pc_iter_init(coop);
   i = tbx_pc_next(&it);
   while ((r = tbx_pch_data(&i)) != NULL) {
       if ((r->lo <= start_block) && (r->hi >= start_block)) { //** Range overlaps beginning of block so may need to return
           if ((r->type == RANGE_INUSE) || ((r->type == RANGE_REQUEST) && (r->timestamp < timestamp))) {
log_printf(10, "check_range_overlap: my_ts=" I64T " my_lo=" I64T " my_hi=" I64T " CONFLICT ts=" I64T " type=%d lo=" I64T " hi=" I64T "\n", 
    timestamp, start_block, *end_block, r->timestamp, r->type, r->lo, r->hi);
              return(1);
           }
       } else if ((r->lo > start_block) && (r->lo <= *end_block)) { //** Need to shrink range
         if ((r->type == RANGE_INUSE) || ((r->type == RANGE_REQUEST) && (r->timestamp < timestamp))) {
log_printf(10, "check_range_overlap: my_ts=" I64T " my_lo=" I64T " my_hi=" I64T " SHRINK ts=" I64T " type=%d lo=" I64T " hi=" I64T "\n", 
    timestamp, start_block, *end_block, r->timestamp, r->type, r->lo, r->hi);
            *end_block = r->lo-1;
         }
       }

       i = tbx_pc_next(&it);
   }

   return(0);
}

//******************************************************************************
// _range_request_wakeup - Wakes up any pending quests that can be fulfilled
//    NOTE:  Assumes the object (obj->lock) is currently locked!
//******************************************************************************

void _range_request_wakeup(tbx_pc_t *coop, osd_off_t lo, osd_off_t hi)
{
   tbx_pc_iter_t it;
   tbx_pch_t i;
   osd_fs_range_t *r;

   it = tbx_pc_iter_init(coop);
   i = tbx_pc_next(&it);
   while ((r = tbx_pch_data(&i)) != NULL) {
      if (r->type == RANGE_REQUEST) {
         if ((r->lo <= lo) && (r->hi >= lo)) {
            apr_thread_cond_signal(r->fsfd->cond);
         } else if ((r->lo >= lo) && (r->lo <= hi)) { 
            apr_thread_cond_signal(r->fsfd->cond);
         }
      }

      i = tbx_pc_next(&it);
   }

   return;   
}

//******************************************************************************
// fsfd_unlock - Releases a previously acquired lock
//******************************************************************************

void fsfd_unlock(osd_fs_t *fs, osd_fs_fd_t *fsfd)
{
  osd_fs_object_t *obj = fsfd->obj;
  osd_fs_range_t *r;

  apr_thread_mutex_lock(obj->lock);


  //** Get my range first
  r = tbx_pch_data(&(fsfd->my_range_slot));

  log_printf(10, "fsfd_unlock: fsfd=%p ts=%d  lo=" I64T " hi=" I64T "\n", fsfd, fsfd->timestamp, r->lo, r->hi); tbx_log_flush();

  delete_range(fsfd);  //** then delete it

  fflush(fsfd->fd);  //** Sync buffers

  //** Wake up everyone
  apr_thread_cond_broadcast(obj->cond);

//  //** Wake up everyone I overlap with
//  _range_request_wakeup(obj->write_range_list, my_lo, my_hi);  
//  _range_request_wakeup(obj->read_range_list, my_lo, my_hi);  

  apr_thread_mutex_unlock(obj->lock);
}


//******************************************************************************
// fsfd_lock - Attempts to lock the objects requested block range.  
//    If the entire range can't be locked then the largest block begininng
//    at the starting block is returned.
//******************************************************************************

osd_off_t fsfd_lock(osd_fs_t *fs, osd_fs_fd_t *fsfd, int mode, osd_off_t start_block, osd_off_t end_block, osd_off_t *got_block)
{
  int err;
  int64_t timestamp;
  osd_fs_object_t *obj = fsfd->obj;
  tbx_pc_t *coop;

  if (mode == OSD_READ_MODE) {
     coop = obj->read_range_list;
  } else {
     coop = obj->write_range_list;
  }

  apr_thread_mutex_lock(obj->lock);

  timestamp = obj->count;  //** Get my timestamp.  This is set only once
  fsfd->timestamp = timestamp;
  obj->count++;

  log_printf(10, "fsfd_lock: fsfd=%p ts=" I64T " id=" LU " lo=" I64T " hi=" I64T " mode=%d\n", fsfd, timestamp, obj->id, start_block, end_block, mode); tbx_log_flush();
  err = 1;
  while (err != 0) {
     *got_block = end_block;
     err = check_range_overlap(obj->write_range_list, timestamp, start_block, got_block);
  log_printf(10, "fsfd_lock: fsfd=%p  ts=" I64T " write check lo=" I64T " hi_got=" I64T " err=%d\n", fsfd, timestamp, start_block, *got_block, err); tbx_log_flush();
     if ((err == 0) && (mode == OSD_WRITE_MODE)) {
        err = check_range_overlap(obj->read_range_list, timestamp, start_block, got_block);
  log_printf(10, "fsfd_lock: fsfd=%p ts=" I64T " read check lo=" I64T " hi_got=" I64T " err=%d\n", fsfd, timestamp, start_block, *got_block, err); tbx_log_flush();
     }

    //** No success so insert myself in the queue and wait
    if (err != 0) {
  log_printf(10, "fsfd_lock: fsfd=%p Failed so inserting my request ts=" I64T "\n", fsfd, timestamp); tbx_log_flush();
       insert_range(coop, fsfd, RANGE_REQUEST, timestamp, start_block, end_block);
//       apr_thread_cond_wait(fsfd->cond, obj->lock);
       apr_thread_cond_wait(obj->cond, obj->lock);
  log_printf(10, "fsfd_lock: fsfd=%p Woken back so trying again ts=" I64T "\n", fsfd, timestamp); tbx_log_flush();
       delete_range(fsfd);  //** Delete the queue request and try again
    }
  }


  log_printf(10, "fsfd_lock: fsfd=%p SUCCESS ts=" I64T " lo=" I64T " hi=" I64T " hi_got=" I64T "\n", fsfd, timestamp, start_block, end_block, *got_block); tbx_log_flush();

  //** Register my range
  insert_range(coop, fsfd, RANGE_INUSE, timestamp, start_block, *got_block);
  fsfd->my_range_coop = coop;

  fflush(fsfd->fd);  //** Sync buffers

  log_printf(10, "fsfd_lock: fsfd=%p AFTER FINAL INSERT ts=" I64T " lo=" I64T " hi=" I64T " hi_got=" I64T "\n", fsfd, timestamp, start_block, end_block, *got_block); tbx_log_flush();

  apr_thread_mutex_unlock(fsfd->obj->lock);

  log_printf(10, "fsfd_lock: fsfd=%p AFTER UNLOCK ts=" I64T " lo=" I64T " hi=" I64T " hi_got=" I64T "\n", fsfd, timestamp, start_block, end_block, *got_block); tbx_log_flush();

  return(0);
}

//**************************************************
//  reserve - Preallocates space for allocation
//**************************************************

int fs_reserve(osd_t *d, osd_id_t id, osd_off_t len) {

log_printf(10, "osd_fs: reserve(" LU ", " ST ")\n", id, len);

  osd_fs_fd_t *fsfd = (osd_fs_fd_t *)fs_open(d, id, OSD_READ_MODE);
  if (fsfd == NULL) return(0);

  posix_fallocate(fileno(fsfd->fd), 0, len);

  fs_close(d, (osd_fd_t *)fsfd);

  return(0);
}


//**************************************************
// id_exists - Checks to see if the ID exists
//**************************************************

int fs_id_exists(osd_t *d, osd_id_t id) {
  osd_fs_t *fs = (osd_fs_t *)(d->private);

  char fname[fs->pathlen];

  FILE *fd = fopen(_id2fname(fs, id, fname, sizeof(fname)), "r");
  if (fd == NULL) return(0);

  fclose(fd);
  return(1);
}

//**************************************************
//  _fs_read_block_header - Reads the block header
//    Assumes the fpos is correctly located
//**************************************************

int _fs_read_block_header(osd_fs_fd_t *fsfd, uint32_t *block_bytes_used, char *cs_value, osd_off_t max_blocksize, int cs_len)
{
  osd_off_t n, m;
  int err;

  err = 0;

  n = fread(block_bytes_used, 1, sizeof(uint32_t), fsfd->fd);
  n = n + fread(cs_value, 1, cs_len, fsfd->fd);

  m = cs_len + sizeof(uint32_t);
  if (m != n) err = -1;
  if (*block_bytes_used > max_blocksize) { *block_bytes_used = max_blocksize; err = -2; }

  return(err);
}

//**************************************************
//  _fs_write_block_header - Writes the block header
//    Assumes the fpos is correctly located
//**************************************************

int _fs_write_block_header(osd_fs_fd_t *fsfd, uint32_t block_bytes_used, char *cs_value, int cs_len)
{
  osd_off_t n, m;

  n = fwrite(&block_bytes_used, 1, sizeof(uint32_t), fsfd->fd);
  n = n + fwrite(cs_value, 1, cs_len, fsfd->fd);
  
  m = cs_len + sizeof(uint32_t);
  if (m != n) return(-1);

  return(0);
}

//**************************************************
// create_id - Creates a new object id for use.
//**************************************************

osd_id_t fs_create_id(osd_t *d, int tbx_chksum_type, int header_size, int block_size, osd_id_t id) {
   osd_fs_t *fs = (osd_fs_t *)(d->private);
   char  fname[fs->pathlen];
   char  cs_value[CHKSUM_MAX_SIZE];
   int32_t dbytes;
   int n, cs_size;
   fs_header_t header;
   tbx_chksum_t chksum;

   apr_thread_mutex_lock(fs->lock);

   if (id == 0) {
      do {      //Generate a unique key
         id = _generate_key();
log_printf(15,"fs_create_id: id=" LU " tbx_chksum_type=%d\n", id, tbx_chksum_type);
      } while (fs_id_exists(d, id));
   }

   FILE *fd = fopen(_id2fname(fs, id, fname, sizeof(fname)), "w");   //Make sure and create the file so no one else can use it
   if (fd == NULL) {
      apr_thread_mutex_unlock(fs->lock);
      log_printf(1, "ERROR creating allocations! fname=%s\n", fname);
      return(0);
   }

   if (tbx_chksum_type_valid(tbx_chksum_type) == 1) {
      memcpy(header.magic, CHKSUM_MAGIC, sizeof(header.magic));
      header.state = 0;
      header.tbx_chksum_type = tbx_chksum_type;
      header.header_size = header_size;
      header.block_size = block_size;
      tbx_chksum_set(&chksum, tbx_chksum_type);
      tbx_chksum_get(&chksum, CHKSUM_DIGEST_BIN, cs_value);
      dbytes = 0;
      cs_size = tbx_chksum_size(&chksum, CHKSUM_DIGEST_BIN);
      memset(cs_value, 0, cs_size);
      n = fwrite(&header, 1, sizeof(header), fd); //** Store the header and also the initial chksum
      n = n + fwrite(&dbytes, 1, sizeof(int32_t), fd);
      n = n + fwrite(cs_value, 1, cs_size, fd);
      if (n != (sizeof(header) + sizeof(int32_t) + cs_size)) {
         log_printf(0, "fs_create_id:  Error storing magic header+chksum! id=" LU " \n", id);
      }
   }

   fclose(fd);

   apr_thread_mutex_unlock(fs->lock);

   return(id);
}

//**************************************************
//  physical_remove - Removes the id
//**************************************************

int fs_physical_remove(osd_t *d, osd_id_t id) {
//   log_printf(10, "physical_remove(" LU ")\n", id);
   osd_fs_t *fs = (osd_fs_t *)(d->private);

   char fname[fs->pathlen];

   return(remove(_id2fname(fs, id, fname, sizeof(fname))));
}

//**************************************************
//  trash_physical_remove - Removes the id from trash
//**************************************************

int fs_trash_physical_remove(osd_t *d, int trash_type, const char *trash_id) {
//log_printf(10, "trash_physical_remove(%d, %s)\n", trash_type, trash_id);
   osd_fs_t *fs = (osd_fs_t *)(d->private);

   char fname[fs->pathlen];

   return(remove(_trashid2fname(fs, trash_type, trash_id, fname, sizeof(fname))));
}

//**************************************************
//  delete_remove - Moved the ID to the deleted_trash dir
//**************************************************

int fs_delete_remove(osd_t *d, osd_id_t id) {
//log_printf(10,"delete_remove(" LU ")\n", id);
   osd_fs_t *fs = (osd_fs_t *)(d->private);

   char fname[fs->pathlen], tname[fs->pathlen];
   apr_time_t now = apr_time_now();
   now = apr_time_sec(now);
   snprintf(tname, sizeof(tname), "%s/deleted_trash/" TT "_" LU, fs->devicename, now, id);

   return(rename(_id2fname(fs, id, fname, sizeof(fname)), tname));
}

//**************************************************
//  expire_remove - Moved the ID to the expired_trash dir
//**************************************************

int fs_expire_remove(osd_t *d, osd_id_t id) {
//log_printf(10,"expire_remove(" LU ")\n", id);
   osd_fs_t *fs = (osd_fs_t *)(d->private);

   char fname[fs->pathlen], tname[fs->pathlen];
   apr_time_t now = apr_time_now();
   now = apr_time_sec(now);

   snprintf(tname, sizeof(tname), "%s/expired_trash/" TT "_" LU, fs->devicename, now, id);

   return(rename(_id2fname(fs, id, fname, sizeof(fname)), tname));
}

//**************************************************
//  remove - Wrapper for physical/expire/delete remove
//**************************************************

int fs_remove(osd_t *d, int rmode, osd_id_t id) {
   if (rmode == OSD_DELETE_ID) {
      return(fs_delete_remove(d, id));
   } else if (rmode == OSD_EXPIRE_ID) {
      return(fs_expire_remove(d, id));
   } else if (rmode == OSD_PHYSICAL_ID) {
      return(fs_physical_remove(d, id));
   }

   return(-1);
}

//**************************************************
// trash_undelete - Undeletes a trashed id
//**************************************************

osd_id_t fs_trash_undelete(osd_t *d, int trash_type, const char *trash_id) {
   osd_fs_t *fs = (osd_fs_t *)(d->private);

   char tname[fs->pathlen], wname[fs->pathlen];
   char dname[fs->pathlen];
   char *bstate;
   int n;
   osd_id_t id;

   _trashid2fname(fs, trash_type, trash_id, tname, sizeof(tname));

   //** Parse for the id
   strncpy(wname, trash_id, sizeof(wname));
   tbx_stk_string_token(wname, "_", &bstate, &n);  //** Throw away the time
//   sscanf(tbx_stk_string_token(NULL, "_", &bstate, &n), LU, &id);
char *dummy = tbx_stk_string_token(NULL, "_", &bstate, &n);
log_printf(15, "trash_undelete: text id=%s\n",dummy);
sscanf(dummy, LU, &id);

   _id2fname(fs, id, dname, sizeof(dname));

   n = rename(tname, dname);
log_printf(15, "trash_undelete: ttype=%d trash_id=%s tname=%s dname=%s id=" LU " rename()=%d\n", trash_type, trash_id, tname, dname, id, n);

   if (n != 0) id = 0;
   
   return(id);
}

//*************************************************************
//  fs_offset_l2p - Converts the Logical offset to a
//     physical offset
//*************************************************************

osd_off_t fs_offset_l2p(osd_fs_t *fs, osd_fs_fd_t *fsfd, osd_off_t l_offset)
{
  osd_off_t n, blocks, rem, len, hds;
  osd_fs_chksum_t *fcs;


  fcs = &(fsfd->obj->fd_chksum);
//log_printf(10, "fs_offset_l2p: id=" LU " l_offset=" I64T " is_valid=%d\n", fsfd->obj->id, l_offset, fcs->is_valid);
 
  if (fcs->is_valid == 0) return(l_offset);

  hds = fcs->hbs_with_chksum - fcs->header_blocksize;
  if (l_offset < fcs->header_blocksize) {
     n = l_offset + FS_MAGIC_HEADER + hds;
  } else {
     len = l_offset - fcs->header_blocksize;
     blocks = len / fcs->blocksize;
     rem = len % fcs->blocksize;
     n = FS_MAGIC_HEADER + fcs->hbs_with_chksum + blocks * fcs->bs_with_chksum + hds + rem;
  }

log_printf(10, "fs_offset_l2p: id=" LU " l_offset=" I64T " p_offset=" I64T " is_valid=%d\n", fsfd->obj->id, l_offset, n, fcs->is_valid);

  return(n);  
}


//*************************************************************
//  fs_offset_p2l - Converts the physical offset to a
//     logical offset
//*************************************************************

osd_off_t fs_offset_p2l(osd_fs_t *fs, osd_fs_fd_t *fsfd, osd_off_t p_offset)
{
  osd_off_t n, blocks, rem, hds;
  osd_fs_chksum_t *fcs;


  fcs = &(fsfd->obj->fd_chksum);
  if (fcs->is_valid == 0) return(p_offset);

  hds = fcs->hbs_with_chksum - fcs->header_blocksize;
  n = p_offset - FS_MAGIC_HEADER;
  if (n < fcs->hbs_with_chksum) {
     n = n - hds;
  } else {
     n = n - fcs->hbs_with_chksum;
     blocks = n / fcs->bs_with_chksum;
     rem = n % fcs->bs_with_chksum;
     if (rem > hds) rem = rem - hds;
     n = fcs->header_blocksize + blocks*fcs->blocksize + rem;
     if (rem < 0) {
        log_printf(0, "fs_offset_p2l: rem<cs_size! p_offset=" I64T " l_offset=" I64T " rem=" I64T " hds=" I64T "\n", 
           p_offset, n, rem, hds);
        n = n + rem;
     }

  }

  return(n);
}


//*************************************************************
// truncate - truncates a file to a logical fixed size
//*************************************************************

int fs_truncate(osd_t *d, osd_id_t id, osd_off_t l_size) {
   osd_fs_t *fs = (osd_fs_t *)(d->private);
   osd_off_t n, b, rem, boff;
   uint32_t bused;
   int err, cs_len;
   osd_fs_chksum_t *fcs;
   tbx_chksum_t cs;
   char buffer[FS_BUF_SIZE];
   char cs_value[CHKSUM_MAX_SIZE];

   osd_fs_fd_t *fsfd = (osd_fs_fd_t *)fs_open(d, id, OSD_READ_MODE);
   if (fsfd == NULL) return(0);

   if (l_size > 0) l_size--;  //** This is the final offset
   n = fs_offset_l2p(fs, fsfd, l_size);  //** Convert it to the physical offset
   if (l_size > 0) n++;   //** convert the physical offset to a length

   err = ftruncate(fileno(fsfd->fd), n);

   //** Now we need to recalculate the last blocks chksum if needed
   fcs = &(fsfd->obj->fd_chksum);
   if (tbx_chksum_type(&(fcs->chksum)) != CHKSUM_NONE) {
      cs = fcs->chksum;
      if (l_size < fcs->header_blocksize) {
         b = 0;
         rem = l_size + 1;
         boff = FS_MAGIC_HEADER;
      } else {
         b = l_size - fcs->header_blocksize;
         rem = (b % fcs->blocksize) + 1;
         b = (b / fcs->blocksize) + 1;
         boff = FS_MAGIC_HEADER + fcs->hbs_with_chksum + (b-1) * fcs->bs_with_chksum;
      }

      bused = rem;
      cs_len = tbx_chksum_size(&cs, CHKSUM_DIGEST_BIN);
      tbx_chksum_reset(&cs);
      n = boff + cs_len + sizeof(uint32_t);
      fseeko(fsfd->fd, n, SEEK_SET);      

      fsfd_lock(fs, fsfd, OSD_WRITE_MODE, b, b, &n);
      _chksum_buffered_read(fs, fsfd, rem, buffer, FS_BUF_SIZE, &cs, NULL);
      tbx_chksum_get(&cs, CHKSUM_DIGEST_BIN, cs_value);
      fseeko(fsfd->fd, boff, SEEK_SET);      
      _fs_write_block_header(fsfd, bused, cs_value, cs_len);
      fsfd_unlock(fs, fsfd);
   }
   
   fs_close(d, (osd_fd_t *)fsfd);
 
   return(err);
}

//*************************************************************
// size - Returns the fd size in bytes
//*************************************************************

osd_off_t fs_fd_size(osd_t *d, osd_fd_t *ofd) 
{
   osd_fs_t *fs = (osd_fs_t *)(d->private);
   osd_fs_fd_t *fsfd = (osd_fs_fd_t *)ofd;
   osd_off_t n;

   if (fsfd == NULL) return(0);

   fseeko(fsfd->fd, 0, SEEK_END);
   n = ftell(fsfd->fd);

   n = fs_offset_p2l(fs, fsfd, n);

   return(n);
}

//*************************************************************
// size - Returns the id size in bytes
//*************************************************************

osd_off_t fs_size(osd_t *d, osd_id_t id) {
   osd_fs_t *fs = (osd_fs_t *)(d->private);
   osd_off_t n;

   osd_fs_fd_t *fsfd = (osd_fs_fd_t *)fs_open(d, id, OSD_READ_MODE);
   if (fsfd == NULL) return(0);

   fseeko(fsfd->fd, 0, SEEK_END);
   n = ftell(fsfd->fd);

   n = fs_offset_p2l(fs, fsfd, n);

   fs_close(d, (osd_fd_t *)fsfd);

   return(n);
}

//*************************************************************
// fs_trash_size - Returns the trash file size in bytes
//*************************************************************

osd_off_t fs_trash_size(osd_t *d, int trash_type, const char *trash_id) {
   osd_fs_t *fs = (osd_fs_t *)(d->private);

   char fname[fs->pathlen];
   FILE *fout = fopen(_trashid2fname(fs, trash_type, trash_id, fname, sizeof(fname)), "r");

   if (fout == NULL) return(0);

   fseeko(fout, 0, SEEK_END);
   osd_off_t n = ftell(fout);

   fclose(fout);

   return(n);
}

//*************************************************************
//  fs_read_header - Retreives the file header 
//*************************************************************

int fs_read_header(FILE *fd, fs_header_t *header)
{
  fseeko(fd, 0, SEEK_SET);
  return(fread(header, 1, sizeof(fs_header_t), fd));
}

//*************************************************************
//  fs_write_header - Stores the file header 
//*************************************************************

int fs_write_header(FILE *fd, fs_header_t *header)
{
  fseeko(fd, 0, SEEK_SET);
  return(fwrite(header, 1, sizeof(fs_header_t), fd));
}

//*************************************************************
// fs_get_state - Returns the current state of the object
//*************************************************************

int fs_get_state(osd_t *d, osd_fd_t *ofd)
{
   osd_fs_fd_t *fsfd = (osd_fs_fd_t *)ofd;
   int state;

   if (ofd == NULL) return(OSD_STATE_BAD_HEADER);

   apr_thread_mutex_lock(fsfd->obj->lock);
   state = fsfd->obj->state;
   apr_thread_mutex_unlock(fsfd->obj->lock);

   return(state);
}

//*************************************************************
// object_open - Opens the object and determines the alloc type
//*************************************************************

int object_open(osd_fs_t *fs, osd_fs_object_t *obj)
{
   char  fname[fs->pathlen];
   int n, normal;
   osd_fs_chksum_t *fcs = &(obj->fd_chksum);

   obj->is_ok = 0;  //** Default is failure
   normal = 1;

   FILE *fd = fopen(_id2fname(fs, obj->id, fname, sizeof(fname)), "r+");
   if (fd != NULL) {
     n = fs_read_header(fd, &(obj->header));
     if (n == sizeof(fs_header_t)) {
        normal = memcmp(obj->header.magic, CHKSUM_MAGIC, sizeof(CHKSUM_MAGIC));
     }
     log_printf(10, "object_open: id=" LU " fs=%s normal=%d fread=%d\n", obj->id, fs->devicename, normal, n); tbx_log_flush();
   } else {
     log_printf(1, "ERROR with open id=" LU " fs=%s\n", obj->id, fs->devicename); tbx_log_flush();
     return(1);
  }

   log_printf(10, "object_open: id=" LU " fs=%s normal=%d n_opened=%d\n", obj->id, fs->devicename, normal, obj->n_opened); tbx_log_flush();

   if (normal != 0) { //** Normal allocation so init the obj header
      fcs->blocksize = 1;
      fcs->header_blocksize = 1;
      fcs->bs_with_chksum = 1;
      fcs->hbs_with_chksum = 1;
      fcs->is_valid = 0;
      tbx_chksum_clear(&(fcs->chksum));
      obj->read = fs_normal_read;
      obj->write = fs_normal_write;
      memset(&(obj->header), 0, sizeof(fs_header_t));
      obj->state = OSD_STATE_GOOD;
   } else {  //** Got a chksum type allocation
      tbx_chksum_set(&(fcs->chksum), obj->header.tbx_chksum_type);
      if (tbx_chksum_type_valid(obj->header.tbx_chksum_type) == 0) {
         n = obj->header.tbx_chksum_type;
         log_printf(0, "object_open:  Invalid chksum type! id=" LU " type=%d \n", obj->id, n);
         fclose(fd);
         return(-2);
      }

      obj->state = obj->header.state;
      if (obj->state != OSD_STATE_GOOD) {
         log_printf(0, "object_open:  Bad state! id=" LU " state=%d \n", obj->id, obj->state);
      }

      fcs->header_blocksize = obj->header.header_size;
      fcs->blocksize = obj->header.block_size;
      n = tbx_chksum_size(&(fcs->chksum), CHKSUM_DIGEST_BIN);
      fcs->hbs_with_chksum = fcs->header_blocksize + n + sizeof(uint32_t);
      fcs->bs_with_chksum = fcs->blocksize + n + sizeof(uint32_t);
      fcs->is_valid = 1;

      obj->read = fs_chksum_read;
      obj->write = fs_chksum_write;
   }

   if (fd != NULL) fclose(fd);

   if (obj->state != OSD_STATE_GOOD) _insert_corrupt_hash(fs, obj->id, "chksum");

   obj->is_ok = 1;  //** Successful open!

   return(0);
}


//*************************************************************
// fs_object_get - Retreives the next free object slot
//*************************************************************

osd_fs_object_t *fs_object_get(osd_fs_t *fs, osd_id_t id)
{
  tbx_pch_t pch;
  osd_fs_object_t *obj;
  int err;

  apr_thread_mutex_lock(fs->obj_lock);

  //** 1st see if it's already in use
  obj = (osd_fs_object_t *)apr_hash_get(fs->obj_hash, &id, sizeof(osd_id_t));
  if (obj == NULL) {  //** New object
     pch = tbx_pch_reserve(fs->obj_list);
     obj = (osd_fs_object_t *)tbx_pch_data(&pch);
     obj->id = id;
     obj->my_slot = pch;
     obj->n_opened = 1;  //** Include the caller
     obj->n_pending = 0; //** Reset the pending count
     obj->n_read = 0;
     obj->n_write = 0;
     obj->count = 0;
     obj->is_ok = -1;  //** Default to object_open failure
     obj->state = OSD_STATE_GOOD;

     apr_hash_set(fs->obj_hash, &(obj->id), sizeof(osd_id_t), obj);  //** Add it to the table

     apr_thread_mutex_lock(obj->lock);  //** Lock it so no one else can open/close it

     apr_thread_mutex_unlock(fs->obj_lock);   //** Unlock the list and do the open

     err = object_open(fs, obj);  //** Opens the object and stores the approp r/w routines

     apr_thread_mutex_unlock(obj->lock);   // Free the object lock

     if (err != 0) {  //** Failed on open so cleanup
        apr_thread_mutex_lock(fs->obj_lock);   //** Can only remove if no one is waiting
        if (obj->n_pending <= 0) {
           apr_hash_set(fs->obj_hash, &(obj->id), sizeof(osd_id_t), NULL);
           tbx_pch_release(fs->obj_list, &pch);
        }
        apr_thread_mutex_unlock(fs->obj_lock);
        obj = NULL;
     } else {
        log_printf(15, "fs_object_get(1): id=" LU " nopened=%d shelf=%d hole=%d\n", obj->id, obj->n_opened, obj->my_slot.shelf, obj->my_slot.hole);
     }
  } else {  //** Already opened so just increase the ref count
     apr_thread_mutex_lock(obj->lock);

     //** Make sure object_open didn't fail
     err = 0;
     obj->n_pending--;
     log_printf(15, "fs_object_get(2): want_id=" LU " id=" LU " nopened=%d npending=%d shelf=%d hole=%d err=%d\n", id, obj->id, obj->n_opened, obj->n_pending, obj->my_slot.shelf, obj->my_slot.hole, err);

     if ((obj->id == id) && (obj->is_ok == 1)) {  //** and id matches and is ok
        obj->n_opened++;
        apr_thread_mutex_unlock(obj->lock);
     } else {
        err = 1;

        if ((obj->n_pending <= 0) && (obj->n_opened <= 0)) {  //** Last person in failure chain so do the final clean up
           apr_thread_mutex_unlock(obj->lock);
           apr_hash_set(fs->obj_hash, &(obj->id), sizeof(osd_id_t), NULL);
           tbx_pch_release(fs->obj_list, &(obj->my_slot));
        } else {  //** Someone else is waiting so let them clean up
           apr_thread_mutex_unlock(obj->lock);
        }
     }

     log_printf(15, "fs_object_get(2): id=" LU "  err=%d\n", id, err);

     apr_thread_mutex_unlock(fs->obj_lock);

      if (err == 1) obj = NULL;
  }

  return(obj);
}

//*************************************************************
// fs_object_release - Releases the object
//*************************************************************

int fs_object_release(osd_fs_t *fs, osd_fs_object_t *obj)
{
  tbx_pch_t pch;

  apr_thread_mutex_lock(obj->lock);

log_printf(15, "id=" LU " nopened=%d npending=%d state=%d shelf=%d hole=%d\n", obj->id, obj->n_opened, obj->n_pending, obj->state, obj->my_slot.shelf, obj->my_slot.hole);

  if (obj->n_opened > 1) {  //** Early exit
     obj->n_opened--;
     apr_thread_mutex_unlock(obj->lock); //** release the original lock
     return(0);
  }

  //** Reacquire the locks in the proper order
  //** This is to eliminate a race condition on open/close
  apr_thread_mutex_unlock(obj->lock); //** release the original lock

  apr_thread_mutex_lock(fs->obj_lock);  //** Acquire it in the order it was created
  apr_thread_mutex_lock(obj->lock);

  obj->n_opened--;

log_printf(15, "Trying to REALLY close it... id=" LU " nopened=%d npending=%d shelf=%d hole=%d\n", obj->id, obj->n_opened, obj->n_pending, obj->my_slot.shelf, obj->my_slot.hole);

  if ((obj->n_opened <= 0) && (obj->n_pending == 0)) {  //** Now *really* close it
     apr_hash_set(fs->obj_hash, &(obj->id), sizeof(osd_id_t), NULL);
     apr_thread_mutex_unlock(obj->lock);   //** Free my lock since it's now removed from the hash and before the release

     //** Now actually free it
     pch = obj->my_slot;
     tbx_pch_release(fs->obj_list, &pch);
  } else {
     apr_thread_mutex_unlock(obj->lock);   //** Free my lock.  False alarm
  }

  apr_thread_mutex_unlock(fs->obj_lock);

  return(0);
}

//*************************************************************
// fs_object_fd_get - Retreives the next free object slot
//*************************************************************

osd_fs_fd_t *fs_object_fd_get(osd_fs_t *fs, osd_fs_object_t *obj, int mode)
{
  tbx_pch_t pch = tbx_pch_reserve(fs->fd_list);
  osd_fs_fd_t *fd = tbx_pch_data(&pch);
  fd->my_slot = pch;
  fd->obj = obj;
  fd->chksum = obj->fd_chksum.chksum;
  tbx_chksum_reset(&(fd->chksum));

log_printf(15, "fs_object_fd_get: id=" LU " shelf=%d hole=%d\n", obj->id, pch.shelf, pch.hole);

  return(fd);
}

//*************************************************************
// fs_object_fd_release - Releases the object
//*************************************************************

void fs_object_fd_release(osd_fs_t *fs, osd_fs_fd_t *fd)
{
  tbx_pch_t pch = fd->my_slot;
log_printf(15, "fs_object_fd_release: id=" LU " shelf=%d hole=%d\n", fd->obj->id, pch.shelf, pch.hole);

  tbx_pch_release(fs->fd_list, &pch);
}



//*************************************************************
// fsfd_get - Stores the object in the global table if needed
//*************************************************************

osd_fs_fd_t *fsfd_get(osd_fs_t *fs, osd_id_t id, int mode)
{
  osd_fs_object_t *obj;
  osd_fs_fd_t *fsfd;

  //** Find a free/existing object slot depending on if the file is currrently in use or not
  obj = fs_object_get(fs, id);
  if (obj == NULL) {
     log_printf(1, "fsfd_get: Can't get an object slot!  id=" LU "\n", id);
     return(NULL);
  }

  //** Find an fd slot
  fsfd = fs_object_fd_get(fs, obj, mode);
  if (fsfd == NULL) {
     log_printf(1, "fsfd_get: Can't get an fd slot!  id=" LU "\n", id);
     return(NULL);
  }

  return(fsfd);
}

//*************************************************************
// fsfd_remove - REmoves the object from the global table if needed
//*************************************************************

int fsfd_remove(osd_fs_t *fs, osd_fs_fd_t *fsfd)
{
  osd_fs_object_t *obj;
//  apr_thread_mutex_lock(fs->obj_lock);
//  apr_thread_mutex_lock(fsfs->obj->lock);

  obj = fsfd->obj;  //** Get the object 1st
  fs_object_fd_release(fs, fsfd);  //** Then release the fd
  fs_object_release(fs, obj);      //** and finally the object itself

  return(0);
}

//*************************************************************
// fs_open - Opens an object for R/W
//*************************************************************

osd_fd_t *fs_open(osd_t *d, osd_id_t id, int mode)
{
   osd_fs_t *fs = (osd_fs_t *)(d->private);
   char fname[fs->pathlen];

   osd_fs_fd_t *fsfd = fsfd_get(fs, id, mode);
   if (fsfd == NULL) return(NULL);

   fsfd->fd = fopen(_id2fname(fs, id, fname, sizeof(fname)), "r+");
   if (fsfd->fd == NULL) {
      if (mode == OSD_WRITE_MODE) {
         log_printf(10, "fs_open(" LU ", %d, w+) doesn't exist so creating the file\n", id, mode);
         fsfd->fd = fopen(_id2fname(fs, id, fname, sizeof(fname)), "w+");
         if (fsfd->fd == NULL) {
            log_printf(0, "fs_open(" LU ", %d) open error = %d\n", id, mode, errno);
            fsfd_remove(fs, fsfd);
            return(NULL);
         }
      } else {
         log_printf(0, "fs_open(" LU ", %d, r+)=NULL open error = %d\n", id, mode, errno);
         fsfd_remove(fs, fsfd);
         return(NULL);
      }
   }

   log_printf(10, "fs_open(" LU ", %d)=%p success\n", id, mode, fsfd->fd);

   return((osd_fd_t *)fsfd);
}

//*************************************************************
// fs_close - Opens an object for R/W
//*************************************************************

int fs_close(osd_t *d, osd_fd_t *ofd)
{
   osd_fs_t *fs = (osd_fs_t *)(d->private);
   osd_fs_fd_t *fsfd = (osd_fs_fd_t *)ofd;

   if (fsfd == NULL) return(0);

   log_printf(10, "fs_close(%p)\n", fsfd->fd);

   if (fsfd->fd != NULL) {
      fclose(fsfd->fd);
      fsfd->fd = NULL;
   }

   return(fsfd_remove(fs, fsfd));
}


//*************************************************************
//  fs_native_open - Opens the ID and positions the fd to len
//*************************************************************

int fs_native_open(osd_t *d, osd_id_t id, osd_off_t offset, int mode) 
{
   int flags;
   osd_fs_t *fs = (osd_fs_t *)(d->private);

   char fname[fs->pathlen];

   if (mode == OSD_WRITE_MODE) {
      flags = O_CREAT | O_WRONLY;
   } else if (mode == OSD_READ_MODE) {
      flags = O_CREAT | O_RDONLY;
   } else {
      flags = O_CREAT | O_RDWR;
   } 

   int fd = open(_id2fname(fs, id, fname, sizeof(fname)), flags, S_IRUSR|S_IWUSR); 
   if (fd == -1) {
      log_printf(0, "fs_native_open(" LU ", " OT ", %d) open error = %d\n", id, offset, mode, errno);
      return(-1);
   }

   lseek(fd, offset, SEEK_SET);

   return(fd);
}

//*************************************************************
//  fs_native_close - Closes the fd
//*************************************************************

int fs_native_close(osd_t *d, int fd) 
{
  return(close(fd));
}

//**************************************************************************************
// _chksum_buffered_read - Reads len bytes of data from fsfd and accumulates
//     the chksum information in cs1 and optionally cs2
//**************************************************************************************

osd_off_t _chksum_buffered_read(osd_fs_t *fs, osd_fs_fd_t *fsfd, osd_off_t len, char *buffer, osd_off_t bufsize, tbx_chksum_t *cs1, tbx_chksum_t *cs2)
{
  osd_off_t nblocks, i, n, nleft;

  if (len == 0) return(0);

  nblocks = len / bufsize;
  n = 0;
  for (i = 0; i < nblocks; i++) {
     n = n + fread(buffer, 1, bufsize, fsfd->fd);
     fs_chksum_add(cs1, bufsize, buffer);
     if (cs2 != NULL) fs_chksum_add(cs2, bufsize, buffer);
  }

  nleft = len % bufsize;
  if (nleft > 0) {
log_printf(10, "_chksum_buffered_read: len=" I64T " bufsize=" I64T " nblocks=" I64T " nleft=" I64T "\n", len, bufsize, nblocks, nleft);
//ftello(fsfd->fd);
     n = n + fread(buffer, 1, nleft, fsfd->fd);
     fs_chksum_add(cs1, nleft, buffer);
     if (cs2 != NULL) fs_chksum_add(cs2, nleft, buffer);
  }

  return(n);
}

//**************************************************************************************
// chksum_merged_write - Writes a PARTIAL block to disk with chksum information
//  NOTE:  Assumes the block lock has already been acquired
//**************************************************************************************

osd_off_t chksum_merged_write(osd_fs_t *fs, osd_fs_fd_t *fsfd, osd_off_t block, osd_off_t offset, osd_off_t len, buffer_t data) 
{
  char cs_value[CHKSUM_MAX_SIZE+2];
  char disk_value[CHKSUM_MAX_SIZE+2];
//  char dummy_value[CHKSUM_MAX_SIZE+2];
  char buffer[FS_BUF_SIZE];
  uint32_t block_bytes_used;
  osd_off_t n, n2, obj_offset, nbytes, bs, d, herr;
  tbx_chksum_t *cs = &(fsfd->chksum);
  osd_fs_chksum_t *ocs = &(fsfd->obj->fd_chksum);

  //** Move to the start of the block
  if (block == 0) {
     obj_offset = FS_MAGIC_HEADER;
     bs = ocs->header_blocksize;
  } else {
     obj_offset = FS_MAGIC_HEADER + ocs->hbs_with_chksum + (block-1) * ocs->bs_with_chksum;
     bs = ocs->blocksize;
  }

  n = 0;
  fseeko(fsfd->fd, obj_offset, SEEK_SET); //** Move to the start of the block

  //** Get the chksum and block bytes from disk
  nbytes = tbx_chksum_size(cs, CHKSUM_DIGEST_BIN);
  block_bytes_used = 0;
  herr = _fs_read_block_header(fsfd, &block_bytes_used, disk_value, bs, nbytes);

  if ((herr == 0) && (block_bytes_used > 0)) {  //** If it's a valid with data
    tbx_chksum_reset(cs);  //** Reset the chksum
    d = block_bytes_used;
    _chksum_buffered_read(fs, fsfd, d, buffer, FS_BUF_SIZE, cs, NULL);  //** Read the original
    tbx_chksum_get(cs, CHKSUM_DIGEST_BIN, cs_value);
 
    n2 = memcmp(disk_value, cs_value, nbytes);
    if (n2 != 0) {
       d = block_bytes_used;
       log_printf(0, "chksum_merged_write(%p, " I64T ", " I64T ") original chksum mismatch! memcmp = " I64T " bbu=" I64T " off=" I64T" len=" I64T "\n", 
           fsfd, obj_offset, ocs->blocksize, n2, d, offset, len);
       n2 = (block == 0) ? OSD_STATE_BAD_HEADER : OSD_STATE_BAD_BLOCK; 

       return(n2);
    }
  } else if (herr == -2) {  //** Invalid blocksize so it's a bad block
    log_printf(0, "chksum_merged_write(%p, " I64T ", " I64T ") invalid header size!\n", fsfd, obj_offset, ocs->blocksize);
    n2 = (block == 0) ? OSD_STATE_BAD_HEADER : OSD_STATE_BAD_BLOCK; 
    return(n2);
  }

  //** Move to the correct offset and write the data
  fseeko(fsfd->fd, obj_offset + sizeof(uint32_t) + nbytes + offset, SEEK_SET); //** Move to the start of the block
  n = fwrite(data, 1, len, fsfd->fd);
  if (n != len) {
     d = block_bytes_used;
     log_printf(0, "chksum_merged_write(%p, " I64T ", " I64T ") error writing data! off=" I64T" len=" I64T " n=" I64T " errno=%d\n", 
         fsfd, obj_offset, ocs->blocksize, offset, len, n, errno);
     n2 = (block == 0) ? OSD_STATE_BAD_HEADER : OSD_STATE_BAD_BLOCK; 
     return(n2);
  }

  //** Now read it back and calculate the chksum
  n2 = offset+len;
  if (n2 > block_bytes_used) block_bytes_used = n2;  //** check if we grow the block
  fseeko(fsfd->fd, obj_offset + sizeof(uint32_t) + nbytes, SEEK_SET); //** Move to the start of the block
  tbx_chksum_reset(cs);  //** Reset the chksum
  d = block_bytes_used;
  _chksum_buffered_read(fs, fsfd, d, buffer, FS_BUF_SIZE, cs, NULL);  //** Read the original
  tbx_chksum_get(cs, CHKSUM_DIGEST_BIN, cs_value);

  //** lastly store the new header
  fseeko(fsfd->fd, obj_offset, SEEK_SET); //** Move to the start of the block
  herr = _fs_write_block_header(fsfd, block_bytes_used, cs_value, nbytes) ;
  if (herr != 0) {
    log_printf(0, "chksum_merged_write(%p, " I64T ", " I64T ") error writing header!\n", fsfd, obj_offset, ocs->blocksize);
    n2 = (block == 0) ? OSD_STATE_BAD_HEADER : OSD_STATE_BAD_BLOCK; 
    return(n2);
  }

  return(OSD_STATE_GOOD);
}

//**************************************************************************************
// chksum_full_write - Writes a FULL block to disk with chksum information
//  NOTE:  Assumes the block lock has already been acquired
//         DO NOT USE for header block!  Use chksum_merged_write instead!
//**************************************************************************************

osd_off_t chksum_full_write(osd_fs_t *fs, osd_fs_fd_t *fsfd, osd_off_t block, buffer_t buffer) 
{
  char dummy_value[CHKSUM_MAX_SIZE+2];
  char cs_value[CHKSUM_MAX_SIZE+2];
  osd_off_t n, obj_offset, nbytes;
  int herr;
  uint32_t block_bytes_used;
  tbx_chksum_t *cs = &(fsfd->chksum);
  osd_fs_chksum_t *ocs = &(fsfd->obj->fd_chksum);

//apr_time_t start_time, dt;
//start_time = apr_time_now();

  //** Prepare the chksum **
  tbx_chksum_reset(cs);
  fs_chksum_add(cs, ocs->blocksize, buffer);
  tbx_chksum_get(cs, CHKSUM_DIGEST_BIN, cs_value);

  //** Move to the correct location
  obj_offset = FS_MAGIC_HEADER + ocs->hbs_with_chksum + (block-1) * ocs->bs_with_chksum;
  fseeko(fsfd->fd, obj_offset, SEEK_SET);
 
  //** Store the data and the chksum
  nbytes = tbx_chksum_size(cs, CHKSUM_DIGEST_BIN); //** Now the chksum
  block_bytes_used = ocs->blocksize;
  herr = _fs_write_block_header(fsfd, block_bytes_used, cs_value, nbytes);
  n = fwrite(buffer, 1, ocs->blocksize, fsfd->fd);     //** Lastly the data

//Q  posix_fadvise(fileno(fsfd->fd), obj_offset, ocs->bs_with_chksum, POSIX_FADV_DONTNEED);

  if ((n != ocs->blocksize) || (herr != 0)) {
     log_printf(0, "chksum_full_write(%p, " I64T ", " I64T ") write error = %d n=" I64T " should be=" I64T " herr=%d\n", fsfd, obj_offset, ocs->blocksize, errno, n, ocs->blocksize, herr);
     n = (block == 0) ? OSD_STATE_BAD_HEADER : OSD_STATE_BAD_BLOCK; 
     return(n);
  }

//dt = apr_time_now() - start_time;
tbx_chksum_get(cs, CHKSUM_DIGEST_HEX, dummy_value);
log_printf(15, "chksum_full_write(%p, " I64T ", " I64T ") cs=%s\n", fsfd, obj_offset, ocs->blocksize, dummy_value);

  return(OSD_STATE_GOOD);
}

//*************************************************************
//  fs_chksum_write - Stores data to an id given the offset and length with chksumming
//*************************************************************

osd_off_t fs_chksum_write(osd_fs_t *fs, osd_fs_fd_t *fsfd, osd_off_t offset, osd_off_t len, buffer_t buf) 
{
   osd_off_t start_block, end_block, start_offset, start_size, end_size, end_got;
   osd_off_t bpos, block, n, first_block, err; 
   osd_off_t start_off, end_off, doff; 
   osd_fs_chksum_t *ocs = &(fsfd->obj->fd_chksum);
   int got_last;
   char *buffer = (char *)buf;

   //** Map the byte range to the blocks
   fs_mapping(fsfd, offset, len, &start_block, &end_block, &start_offset, &start_size, &end_size);
   first_block = start_block;

   //** Calculate the physical disk byte range for posix_fadvise
    if (start_block == 0) {
       start_off = FS_MAGIC_HEADER;
    } else {
       start_off = FS_MAGIC_HEADER + ocs->hbs_with_chksum + (start_block-1) * ocs->bs_with_chksum;
    }    

    //** Only need to preload the 1st and last blocks.  The middle blocks are completely overwritten.
    //** But if I do this I get crappy performance...  Weird interaction with lunix IO scheduler...
    end_off = FS_MAGIC_HEADER + ocs->hbs_with_chksum + ((end_block+1)-1) * ocs->bs_with_chksum;
    doff = end_off - start_off + 1;
    posix_fadvise(fileno(fsfd->fd), start_off, doff, POSIX_FADV_WILLNEED);


   log_printf(10, "fs_chksum_write(%p, " I64T ", " I64T ")\n", fsfd, offset, len);
   log_printf(10, "fs_chksum_write(%p, " I64T ", " I64T "): sb=" I64T " eb=" I64T " so=" I64T " ss=" I64T " es=" I64T " hbs=" I64T " bs=" I64T "\n", 
      fsfd, offset, len, start_block, end_block, start_offset, start_size, end_size, fsfd->obj->fd_chksum.header_blocksize, fsfd->obj->fd_chksum.blocksize);


   bpos = 0;
   got_last = 0;
   n = 0;
   err = OSD_STATE_GOOD;
   do {
     //** Request the lock and see what we get back
     fsfd_lock(fs, fsfd, OSD_WRITE_MODE, start_block, end_block, &end_got);
     log_printf(10, "fs_chksum_write(%p, " I64T ", " I64T ") sb=" I64T " eb=" I64T " end_got=" I64T "\n", fsfd, offset, len, start_block, end_block, end_got);
   
     //** Now write the 1st block (if needed)
     if (bpos == 0) {
         n = chksum_merged_write(fs, fsfd, start_block, start_offset, start_size, buffer);
         fs_cache_remove(fs->cache, fsfd->obj->id, start_block);
         if (n != 0) {
            log_printf(0, "fs_write(%p, " I64T ", " I64T ") write error = %d n=" I64T "\n", fsfd, offset, len, errno, n);
            err = n;
         }
         start_block++;
         bpos += start_size;
     }

     //** Check if the last block is full.  If not store it with chksum_merged
     got_last = end_got;
     if (end_got == end_block) { got_last--; }

     for (block=start_block; block <= got_last; block++) {
         n = chksum_full_write(fs, fsfd, block, &(buffer[bpos]));
         fs_cache_remove(fs->cache, fsfd->obj->id, block);
         if (n != 0) {
            log_printf(0, "fs_chksum_write(%p, " I64T ", " I64T ") write error = %d n=" I64T "\n", fsfd, offset, len, errno, n);
            err = n;
         }
         bpos += fsfd->obj->fd_chksum.blocksize;
     }          
  

     //** handle the last block
     if ((end_got == end_block) && (end_block != first_block)) {
         n = chksum_merged_write(fs, fsfd, end_block, 0, end_size, &(buffer[bpos]));
         fs_cache_remove(fs->cache, fsfd->obj->id, end_block);
         if (n != 0) {
            log_printf(0, "fs_chksum_write(%p, " I64T ", " I64T ") write error = %d n=" I64T "\n", fsfd, offset, len, errno, n);
            err = n;
         }
     }

     start_block = end_got + 1;

//     fflush(fsfd->fd);  //** Make sure data is available for other threads  -- done in fsfd_unlock

     fsfd_unlock(fs, fsfd);
   } while (end_got < end_block);

   log_printf(10, "fs_chksum_write(%p, " I64T ", " I64T ")=" I64T " FINISHED\n", fsfd, offset, len, err);

   if (err != OSD_STATE_GOOD) { 
      apr_thread_mutex_lock(fsfd->obj->lock);    
      fsfd->obj->header.state = n; 
      fsfd->obj->state = n;
      fs_write_header(fsfd->fd, &(fsfd->obj->header)); 
      apr_thread_mutex_unlock(fsfd->obj->lock);    

      apr_thread_mutex_lock(fs->obj_lock);    
      _insert_corrupt_hash(fs, fsfd->obj->id, "chksum");
      apr_thread_mutex_unlock(fs->obj_lock);    
   }

   if (end_block != 0) posix_fadvise(fileno(fsfd->fd), start_off, doff, POSIX_FADV_DONTNEED);

   return(err);
}

//*************************************************************
//  fs_normal_write - Stores data to an id given the offset and length (no chksum)
//*************************************************************

osd_off_t fs_normal_write(osd_fs_t *fs, osd_fs_fd_t *fsfd, osd_off_t offset, osd_off_t len, buffer_t buffer) 
{
   osd_off_t n, err;
 
//   apr_thread_mutex_lock(fsfd->lock);

   fseeko(fsfd->fd, offset, SEEK_SET);

log_printf(15, "fs_normal_write(%s, %p, " I64T ", " I64T ", %p)\n", fs->devicename, fsfd, offset, len, buffer);

   err = 0;   
   n = fwrite(buffer, 1, len, fsfd->fd);
   if (n != len) {
      log_printf(0, "fs_normal_write(%p, " I64T ", " I64T ") write error = %d n=" I64T "\n", fsfd, offset, len, errno, n);
      err = 1;
   }

//   apr_thread_mutex_unlock(fsfd->lock);

   if (offset > 4095) posix_fadvise(fileno(fsfd->fd), offset, len, POSIX_FADV_DONTNEED);

   return(err);
}

//*************************************************************
//  write - Stores data to an id given the offset and length
//*************************************************************

osd_off_t fs_write(osd_t *d, osd_fd_t *ofd, osd_off_t offset, osd_off_t len, buffer_t buffer) 
{
   osd_off_t err;
   osd_fs_t *fs = (osd_fs_t *)(d->private);
   osd_fs_fd_t *fsfd = (osd_fs_fd_t *)ofd;
   if (fsfd == NULL) {
      log_printf(0, "fs_write(NULL, " I64T ", " I64T ", buffer) invalid fd!\n", offset, len);
      return(-1);
   }

   log_printf(10, "fs_write(%p, " I64T ", " I64T ", buffer) start!\n", fsfd, offset, len);
   
   err = fsfd->obj->write(fs, fsfd, offset, len, buffer);
   if (err == 0) err = len;
   log_printf(10, "fs_write(%p, " I64T ", " I64T ", buffer)=" I64T " end!\n", fsfd, offset, len, err);

   return(err);
}

//**************************************************************************************
//  chksum_direct_read - Reads a chksum block but bypasses the chksum validation.
//     Designed for use with caching of recently verified blocks
//**************************************************************************************

osd_off_t chksum_direct_read(osd_fs_t *fs, osd_fs_fd_t *fsfd, osd_off_t block, osd_off_t offset, osd_off_t len, buffer_t data_ptr) 
{
  char *data = (char *)data_ptr;
  osd_off_t obj_offset;
  osd_fs_chksum_t *ocs = &(fsfd->obj->fd_chksum);
  osd_off_t n;

  //** Move to the start of the block
  if (block == 0) {
     obj_offset = FS_MAGIC_HEADER;
  } else {
     obj_offset = FS_MAGIC_HEADER + ocs->hbs_with_chksum + (block-1) * ocs->bs_with_chksum;
  }

  obj_offset += offset + tbx_chksum_size(&(fsfd->chksum), CHKSUM_DIGEST_BIN) + sizeof(uint32_t);

log_printf(10, "chksum_direct_read(%p) off=" OT " len=" OT "\n", fsfd, offset, len);

  fseeko(fsfd->fd, obj_offset, SEEK_SET);

  n = fread(data, 1, len, fsfd->fd);

  if (n != len) return(OSD_STATE_BAD_BLOCK);

  return(OSD_STATE_GOOD);
}


//**************************************************************************************
// chksum_merged_read - Read a PARTIAL block to disk with chksum information
//  NOTE:  Assumes the block lock has already been acquired
//**************************************************************************************

osd_off_t chksum_merged_read(osd_fs_t *fs, osd_fs_fd_t *fsfd, osd_off_t block, osd_off_t offset, osd_off_t len, buffer_t data_ptr) 
{
  char cs_value[CHKSUM_MAX_SIZE], disk_value[CHKSUM_MAX_SIZE];
  char buffer[FS_BUF_SIZE];
  char *data = (char *)data_ptr;
  int herr;
  uint32_t block_bytes_used;
  osd_off_t n, obj_offset, nbytes, bs, nleft, off, len2;
  tbx_chksum_t *cs = &(fsfd->chksum);
  osd_fs_chksum_t *ocs = &(fsfd->obj->fd_chksum);

//memset(data, 0, len);

  //** Move to the start of the block
  if (block == 0) {
     obj_offset = FS_MAGIC_HEADER;
     bs = ocs->header_blocksize;
  } else {
     obj_offset = FS_MAGIC_HEADER + ocs->hbs_with_chksum + (block-1) * ocs->bs_with_chksum;
     bs = ocs->blocksize;
  }

  fseeko(fsfd->fd, obj_offset, SEEK_SET);

//log_printf(5, "chksum_merged_read: block=" I64T " fpos=" I64T " len=" I64T "\n", block, obj_offset, len); tbx_log_flush();

  tbx_chksum_reset(cs);  //** Reset the chksum

  //** Get the chksum and block bytes from disk
  block_bytes_used = 0;
  nbytes = tbx_chksum_size(cs, CHKSUM_DIGEST_BIN);
  herr = _fs_read_block_header(fsfd, &block_bytes_used, disk_value, bs, nbytes);

//  nblocks = block_bytes_used;
//  log_printf(10, "chksum_merged_read(%p, b=" I64T ", o=" I64T ", l=" I64T " oo=" I64T ", bs=" I64T ", csl=" I64T ") after initial chksum read herr=%d bytes_used=" I64T "\n", 
//       fsfd, block, offset, len, obj_offset, ocs->blocksize, nbytes, herr, nblocks);

  if (herr != 0) {  //** Either a disk error or more likely this block is empty
     log_printf(10, "chksum_merged_read(%p, " I64T ", " I64T ") ERROR after header read herr=%d\n", fsfd, obj_offset, ocs->blocksize, herr);
  }


  //** Determine the max offset based on amount of data in block
  if (bs > block_bytes_used) bs = block_bytes_used;
  off = (offset > block_bytes_used) ? block_bytes_used : offset;
  n = 0;

  //** Read in any data before the "read" start position
  n = n + _chksum_buffered_read(fs, fsfd, off, buffer, FS_BUF_SIZE, cs, NULL);

//log_printf(10, "chksum_merged_read(%p, " I64T ", " I64T ") after pre-read n=" I64T "\n", fsfd, obj_offset, ocs->blocksize, n);

  if (offset < block_bytes_used) {  //** Siphon the requested data if available
     off = offset + len;
     len2 = (off >= block_bytes_used) ? block_bytes_used - offset : len;
     n = n + fread(data, 1, len2, fsfd->fd);
     if (off >= block_bytes_used) {  //** Add blanks as needed
        off = len - len2;
        memset(&(data[len2]), 0, off);
     }
     fs_chksum_add(cs, len2, data);  //** Only chksum the available data
//log_printf(10, "chksum_merged_read(%p, " I64T ", " I64T ") after siphon n=" I64T "\n", fsfd, obj_offset, ocs->blocksize, n);
        
     //** Read in the rest of block if available
     off = offset + len;
     bs = (off > block_bytes_used) ? off : block_bytes_used; 
     nleft = bs - offset - len;
     n = n + _chksum_buffered_read(fs, fsfd, nleft, buffer, FS_BUF_SIZE, cs, NULL);
   
//log_printf(10, "chksum_merged_read(%p, " I64T ", " I64T ") after post-read n=" I64T "\n", fsfd, obj_offset, ocs->blocksize, n);

  } else { //** Nothing to read so fill with blanks
     memset(data, 0, len);
  }


  if (block_bytes_used > 0) { //** Valid chksum stored
     if ((n != bs) && (herr != 0)) {
        log_printf(0, "chksum_merged_read(%p, " I64T ", " I64T ") read error = %d n=" I64T " should be=" I64T " herr=%d\n", fsfd, obj_offset, ocs->blocksize, errno, n, bs, herr);
        n = (block == 0) ? OSD_STATE_BAD_HEADER : OSD_STATE_BAD_BLOCK; 
        return(n);
     }

     tbx_chksum_get(cs, CHKSUM_DIGEST_BIN, cs_value);
 
     n = memcmp(disk_value, cs_value, tbx_chksum_size(&(ocs->chksum), CHKSUM_DIGEST_BIN));
     if (n != 0) {
        log_printf(0, "chksum_merged_read(%p, " I64T ", " I64T ") chksum mismatch! memcmp = " I64T "\n", fsfd, obj_offset, ocs->blocksize, n);
        n = (block == 0) ? OSD_STATE_BAD_HEADER : OSD_STATE_BAD_BLOCK; 
        return(n);
     }
  }

  return(OSD_STATE_GOOD);
}

//**************************************************************************************
// chksum_full_read - Read a FULL block to disk with chksum information
//  NOTE:  Assumes the block lock has already been acquired
//         DO NOT USE for header block!  Use chksum_merged_read instead!
//**************************************************************************************

osd_off_t chksum_full_read(osd_fs_t *fs, osd_fs_fd_t *fsfd, osd_off_t block, buffer_t buf) 
{
  char cs_value[CHKSUM_MAX_SIZE+2], disk_value[CHKSUM_MAX_SIZE+2];
  char *buffer  =(char *)buf;
  int herr;
  uint32_t block_bytes_used;
  osd_off_t n, obj_offset, nbytes, nleft;
  tbx_chksum_t *cs = &(fsfd->chksum);
  osd_fs_chksum_t *ocs = &(fsfd->obj->fd_chksum);

//memset(buffer, 0, ocs->blocksize);

  //** Prepare the chksum **
  tbx_chksum_reset(cs);

  //** Move to the correct location
  obj_offset = FS_MAGIC_HEADER + ocs->hbs_with_chksum + (block-1) * ocs->bs_with_chksum;
  fseeko(fsfd->fd, obj_offset, SEEK_SET);

log_printf(0, "chksum_full_read: block=" I64T " fpos=" I64T "\n", block, obj_offset); tbx_log_flush();

  //** Read the data and the chksum
  block_bytes_used = 0;
  nbytes = tbx_chksum_size(cs, CHKSUM_DIGEST_BIN);
  herr = _fs_read_block_header(fsfd, &block_bytes_used, disk_value, ocs->blocksize, nbytes);
  
  n = 0;
  if (block_bytes_used > 0) n = fread(buffer, 1, block_bytes_used, fsfd->fd);
  nleft = ocs->blocksize - block_bytes_used;
  if (nleft > 0) memset(&(buffer[block_bytes_used]), 0, nleft);
  
  fs_chksum_add(cs, block_bytes_used, buffer);

  if ((n != block_bytes_used) || (herr != 0)) {
     nbytes = block_bytes_used;
     log_printf(10, "chksum_full_read(%p, " I64T ", " I64T ") read error = %d n=" I64T " should be=" I64T " herr=%d\n", fsfd, obj_offset, ocs->blocksize, errno, n, nbytes, herr);
     n = (block == 0) ? OSD_STATE_BAD_HEADER : OSD_STATE_BAD_BLOCK; 
     return(n);
  }

  if (block_bytes_used > 0) { //** Valid chksum stored
     tbx_chksum_get(cs, CHKSUM_DIGEST_BIN, cs_value);
 
     n = memcmp(disk_value, cs_value, nbytes);
     if (n != 0) {
        log_printf(10, "chksum_merged_read(%p, " I64T ", " I64T ") chksum mismatch! memcmp = " I64T "\n", fsfd, obj_offset, ocs->blocksize, n);
        n = (block == 0) ? OSD_STATE_BAD_HEADER : OSD_STATE_BAD_BLOCK; 
        return(n);
     }
  }

  return(OSD_STATE_GOOD);
}


//*************************************************************
//  fs_normal_read - Reads data from the id given at offset and length (no chksum)
//*************************************************************

osd_off_t fs_normal_read(osd_fs_t *fs, osd_fs_fd_t *fsfd, osd_off_t offset, osd_off_t len, buffer_t buffer) 
{
   osd_off_t n, err;
 
//   apr_thread_mutex_lock(fsfd->lock);

   posix_fadvise(fileno(fsfd->fd), offset, len, POSIX_FADV_WILLNEED);

   fseeko(fsfd->fd, offset, SEEK_SET);

log_printf(10, "fs_normal_read(%s, %p, " I64T ", " I64T ", %p)\n", fs->devicename, fsfd, offset, len, buffer);

   err = 0;   
   n = fread(buffer, 1, len, fsfd->fd);
   if (n != len) {
      log_printf(0, "fs_normal_read(%p, " I64T ", " I64T ") write error = %d n=" I64T "\n", fsfd, offset, len, errno, n);
      err = 1;
   }

//   apr_thread_mutex_unlock(fsfd->lock);

   return(err);
}

//*************************************************************
//  fs_chksum_read - Reads data from the id given at offset and length with chksumming
//*************************************************************

osd_off_t fs_chksum_read(osd_fs_t *fs, osd_fs_fd_t *fsfd, osd_off_t offset, osd_off_t len, buffer_t buf) 
{
   osd_off_t start_block, end_block, start_offset, start_size, end_size;
   osd_off_t bpos, block, end_got, n,  first_block, err;
   osd_off_t start_off, end_off, doff; 
   int got_last;
   char *buffer = (char *)buf;
   osd_fs_chksum_t *ocs = &(fsfd->obj->fd_chksum);

//memset(buffer, 0, len);

   n = 0;

   //** Map the byte range to the blocks
   fs_mapping(fsfd, offset, len, &start_block, &end_block, &start_offset, &start_size, &end_size);
   first_block = start_block;

   //** Calculate the physical disk byte range for posix_fadvise
    if (start_block == 0) {
       start_off = FS_MAGIC_HEADER;
    } else {
       start_off = FS_MAGIC_HEADER + ocs->hbs_with_chksum + (start_block-1) * ocs->bs_with_chksum;
    }    
    end_off = FS_MAGIC_HEADER + ocs->hbs_with_chksum + ((end_block+1)-1) * ocs->bs_with_chksum;
    doff = end_off - start_off + 1;
    posix_fadvise(fileno(fsfd->fd), start_off, doff, POSIX_FADV_WILLNEED);


   log_printf(10, "fs_chksum_read(%p, " I64T ", " I64T ")\n", fsfd->fd, offset, len);
   log_printf(10, "fs_chksum_read(%p, " I64T ", " I64T "): sb=" I64T " eb=" I64T " so=" I64T " ss=" I64T " es=" I64T "\n", 
      fsfd, offset, len, start_block, end_block, start_offset, start_size, end_size);

   bpos = 0;
   got_last = 0;
   err = OSD_STATE_GOOD;
   do {
     //** Request the lock and see what we get back
     fsfd_lock(fs, fsfd, OSD_READ_MODE, start_block, end_block, &end_got);
   
     log_printf(10, "fs_chksum_read(%p, " I64T ", " I64T ") sb=" I64T " eb=" I64T " end_got=" I64T "\n", fsfd, offset, len, start_block, end_block, end_got);

     //** Now read the 1st block (if needed)
     if (bpos == 0) {
         if (fs_direct_cache_read(fs->cache, fsfd->obj->id, start_block) == 1) {
            n = chksum_direct_read(fs, fsfd, start_block, start_offset, start_size, buffer);
            if (n != 0) fs_cache_remove(fs->cache, fsfd->obj->id, start_block);
         } else {
            n = chksum_merged_read(fs, fsfd, start_block, start_offset, start_size, buffer);
            if (n == 0) fs_cache_add(fs->cache, fsfd->obj->id, start_block);
         }
         if (n != 0) {
            log_printf(0, "fs_chksum_read(%p, " I64T ", " I64T ") read error = %d n=" I64T "\n", fsfd, offset, len, errno, n);
            err = n;
         }
         start_block++;
         bpos += start_size;
     }

     got_last = end_got;
     if (end_got == end_block)  { got_last--; }

     for (block=start_block; block <= got_last; block++) {
//log_printf(10, "11111111111111111111\n");
         if (fs_direct_cache_read(fs->cache, fsfd->obj->id, start_block) == 1) {
            n = chksum_direct_read(fs, fsfd, block, 0, ocs->blocksize, &(buffer[bpos]));
            if (n != 0) fs_cache_remove(fs->cache, fsfd->obj->id, start_block);
         } else {
            n = chksum_full_read(fs, fsfd, block, &(buffer[bpos]));
            if (n == 0) fs_cache_add(fs->cache, fsfd->obj->id, start_block);
         }
         if (n != 0) {
            log_printf(0, "fs_chksum_read(%p, " I64T ", " I64T ") read error = %d n=" I64T "\n", fsfd, offset, len, errno, n);
            err = n;
         }
         bpos += fsfd->obj->fd_chksum.blocksize;
     }          
  
     //** Check if the last block is full.  If not read it with chksum_merged
     //** handle the last block
     if ((end_got == end_block) && (end_block != first_block)) {
//log_printf(10, "2222222222222222\n");
// log_printf(10, "22222222222222fs_chksum_read(%p, " I64T ", " I64T ") sb=" I64T " eb=" I64T " end_got=" I64T "\n", fsfd->fd, offset, len, start_block, end_block, end_got);
         if (fs_direct_cache_read(fs->cache, fsfd->obj->id, start_block) == 1) {
            n = chksum_direct_read(fs, fsfd, end_block, 0, end_size, &(buffer[bpos]));
         } else {
            n = chksum_merged_read(fs, fsfd, end_block, 0, end_size, &(buffer[bpos]));
         }
         if (n != 0) {
            log_printf(0, "fs_chksum_read(%p, " I64T ", " I64T ") read error = %d n=" I64T "\n", fsfd, offset, len, errno, n);
            err = n;
            fs_cache_remove(fs->cache, fsfd->obj->id, end_block);
         }
     }

     start_block = end_got + 1;

     fsfd_unlock(fs, fsfd);
   } while (end_got < end_block);

   if (err != OSD_STATE_GOOD) {
      apr_thread_mutex_lock(fsfd->obj->lock);    
      fsfd->obj->header.state = n; 
      fsfd->obj->state = n;
      fs_write_header(fsfd->fd, &(fsfd->obj->header)); 
      apr_thread_mutex_unlock(fsfd->obj->lock);    

      apr_thread_mutex_lock(fs->obj_lock);    
      _insert_corrupt_hash(fs, fsfd->obj->id, "chksum");
      apr_thread_mutex_unlock(fs->obj_lock);    
   }

   return(err);
}


//*************************************************************
//  read - Reads data from the id given at offset and length
//*************************************************************

osd_off_t fs_read(osd_t *d, osd_fd_t *ofd, osd_off_t offset, osd_off_t len, buffer_t buffer) 
{
   osd_off_t err;
   osd_fs_t *fs = (osd_fs_t *)(d->private);
   osd_fs_fd_t *fsfd = (osd_fs_fd_t *)ofd;
   if (fsfd == NULL) {
      log_printf(0, "fs_write(NULL, " I64T ", " I64T ", buffer) invalid fd!\n", offset, len);
      return(-1);
   }

   log_printf(10, "fs_read(%p, " I64T ", " I64T ", buffer) start!\n", fsfd, offset, len);

   err = fsfd->obj->read(fs, fsfd, offset, len, buffer);
   if (err == 0) err = len;

   log_printf(10, "fs_read(%p, " I64T ", " I64T ", buffer)=" I64T " end!\n", fsfd, offset, len, err);

   return(err);
}

//*************************************************************
//  fs_chksum_info - Retreives the objects chksum info
//    If no chksum info is available 1 is returned
//*************************************************************

int fs_chksum_info(osd_t *d, osd_id_t id, int *cs_type, osd_off_t *header_blocksize, osd_off_t *blocksize)
{
   osd_fs_chksum_t *fcs;
   int err;
log_printf(15, "fs_chksum_info: start id=" LU "\n", id); tbx_log_flush();
   osd_fs_fd_t *fsfd = (osd_fs_fd_t *)fs_open(d, id, OSD_READ_MODE);

log_printf(15, "fs_chksum_info: after open id=" LU "\n", id); tbx_log_flush();

   *cs_type = CHKSUM_NONE;
   *header_blocksize = 0;
   *blocksize = 0;
   err = 1;

   if (fsfd == NULL) return(err);

   fcs = &(fsfd->obj->fd_chksum);

   if (fcs->is_valid == 1) {  //** No chksum available so return
     *cs_type = tbx_chksum_type(&(fcs->chksum));
     *header_blocksize = fcs->header_blocksize;
     *blocksize = fcs->blocksize;
     err = 0;
   }


   fs_close(d, (osd_fd_t *)fsfd);

log_printf(15, "fs_chksum_info: end id=" LU " cs_type=%d\n", id, *cs_type); tbx_log_flush();

   return(err);
}

//*************************************************************
//  fs_get_chksum - Retreives the chksum data and stores it in the provided buffer
//     If the buffer is too small a value of the needed buffer size is returned
//     as a negative number. Otherwise the number of bytes used is returned
//*************************************************************

osd_off_t fs_get_chksum(osd_t *d, osd_id_t id, char *disk_buffer, char *calc_buffer, osd_off_t buffer_size, osd_off_t *block_len,
    char *good_block, osd_off_t start_block, osd_off_t end_block)
{
  osd_fs_t *fs = (osd_fs_t *)(d->private);
  osd_fs_chksum_t *fcs;
  osd_off_t fpos, bpos, slot, n, end_got, block, got_block;
  int cs_size, state, flag_corrupt;
  uint32_t block_bytes_used;
  char disk_value[CHKSUM_MAX_SIZE], calc_value[CHKSUM_MAX_SIZE];
  char chksum_buffer[FS_BUF_SIZE];
//char hex_buffer[CHKSUM_MAX_SIZE];

//log_printf(0, "fs_get_chksum: 00000000000000000\n"); tbx_log_flush();

   osd_fs_fd_t *fsfd = (osd_fs_fd_t *)fs_open(d, id, OSD_READ_MODE);
   if (fsfd == NULL) return(0);

   fcs = &(fsfd->obj->fd_chksum);

   if (fcs->is_valid == 0) {  //** No chksum available so return
      fs_close(d, (osd_fd_t *)fsfd);
      return(0);
   }

   cs_size = tbx_chksum_size(&(fcs->chksum), CHKSUM_DIGEST_BIN);
   n = cs_size*(end_block - start_block + 1);
   if ((buffer_size < n) && ((disk_buffer != NULL) || (calc_buffer != NULL))) {
      log_printf(0, "fs_get_chksum: buffer too small size=" I64T " need=" I64T "\n", buffer_size, n);
      fs_close(d, (osd_fd_t *)fsfd);
      return(-n);
   }


   flag_corrupt =0;
   bpos = 0;
   slot = 0;

//log_printf(0, "fs_get_chksum: 1111111111111111111111\n"); tbx_log_flush();

   //** Get the header 1st if needed
   if (start_block == 0) {
//log_printf(0, "fs_get_chksum: 22222222222222222222\n"); tbx_log_flush();

      fpos = FS_MAGIC_HEADER;
      fseeko(fsfd->fd, fpos, SEEK_SET);

      block_bytes_used = -1;
      n = fread(&block_bytes_used, 1, sizeof(uint32_t), fsfd->fd);
      if (block_len != 0) block_len[slot] = block_bytes_used;
      if (n != sizeof(uint32_t)) {
         log_printf(0, "fs_get_chksum: Error reading header block_bytes_used! id=" LU " n=" I64T "\n", id, n);
      }

      fsfd_lock(fs, fsfd, OSD_READ_MODE, 0, 0, &got_block);
      n = fread(disk_value, 1, cs_size, fsfd->fd);
      if (disk_buffer != NULL) memcpy(&(disk_buffer[bpos]), disk_value, cs_size);
      fsfd_unlock(fs, fsfd);
      if (n != cs_size) {
         log_printf(0, "fs_get_chksum: Error reading header chksum! id=" LU "\n", id);
      }

//log_printf(0, "fs_get_chksum: 333333333333333333333333333\n"); tbx_log_flush();


//log_printf(0, "fs_get_chksum: 4444444444444444444444\n"); tbx_log_flush();

      //** Lastly see if we need to verify the chksum
      if ((good_block != NULL) || (calc_buffer != NULL)) {
         state = _fs_calculate_chksum(fs, fsfd, &(fsfd->chksum), start_block, chksum_buffer, FS_BUF_SIZE, calc_value, 0);
         if (good_block != NULL) good_block[slot] = state;
         if (calc_buffer != NULL) memcpy(&(calc_buffer[bpos]), calc_value, cs_size);
      }

      bpos = bpos + cs_size;
      slot++;
      start_block++;
   }

//log_printf(0, "fs_get_chksum: 555555555555555555555555555\n"); tbx_log_flush();

   if (end_block == 0) {   //** Exit if only reading the header
      fs_close(d, (osd_fd_t *)fsfd);
      return(bpos);
   }
log_printf(5, "fs_get_chksum: start_block=" I64T "\n", start_block); tbx_log_flush();

   //** Now cycle through the blocks
   fpos = FS_MAGIC_HEADER + fcs->hbs_with_chksum;  //** Get the starting pos
   fpos = fpos + (start_block-1) * fcs->bs_with_chksum;
   do {
     //** Request the lock and see what we get back
     fsfd_lock(fs, fsfd, OSD_READ_MODE, start_block, end_block, &end_got);

     for (block=start_block; block <= end_got; block++) {
        fseeko(fsfd->fd, fpos, SEEK_SET);
log_printf(5, "fs_get_chksum: block=" I64T " fpos=" I64T "\n", block, fpos); tbx_log_flush();

        block_bytes_used = -1;
        n = fread(&block_bytes_used, 1, sizeof(uint32_t), fsfd->fd);
        if (block_len != 0) block_len[slot] = block_bytes_used;
        if (n != sizeof(uint32_t)) {
           log_printf(0, "fs_get_chksum: Error reading block block_bytes_used! id=" LU " block=" I64T "\n", id, block);
        }

        n = fread(disk_value, 1, cs_size, fsfd->fd);
        if (disk_buffer != NULL) memcpy(&(disk_buffer[bpos]), disk_value, cs_size);
        if (n != cs_size) {
           log_printf(0, "fs_get_chksum: Error reading block chksum! id=" LU " block=" I64T " err=" I64T "\n", id, block, n);
        }

//tbx_chksum_bin2hex(cs_size, (unsigned char *)disk_value, hex_buffer);
//log_printf(5, "fs_get_chksum: block=" I64T " dcs=%s\n", block, hex_buffer);

        //** Lastly see if we need to verify the chksum
        if ((good_block != NULL) || (calc_buffer != NULL)) {
           state = _fs_calculate_chksum(fs, fsfd, &(fsfd->chksum), block, chksum_buffer, FS_BUF_SIZE, calc_value, 0);
           if (state != 0) flag_corrupt = 1;
//tbx_chksum_bin2hex(cs_size, (unsigned char *)calc_value, hex_buffer);
//log_printf(5, "fs_get_chksum: block=" I64T " ccs=%s\n", block, hex_buffer);
           if (good_block != NULL) good_block[slot] = state;
           if (calc_buffer != NULL) memcpy(&(calc_buffer[bpos]), calc_value, cs_size);
        }

        slot++;
        bpos = bpos + cs_size;
        fpos = fpos + fcs->bs_with_chksum;
     }
     start_block = end_got + 1;

     fsfd_unlock(fs, fsfd);
   } while (end_got < end_block);

   if (flag_corrupt != 0) {
      apr_thread_mutex_lock(fs->obj_lock);
      _insert_corrupt_hash(fs, fsfd->obj->id, "chksum");
      apr_thread_mutex_unlock(fs->obj_lock);
   }

   fs_close(d, (osd_fd_t *)fsfd);

   return(bpos);
}

//*************************************************************
//  _fs_calculate_block_chksum - Calculates the chksum for the given block
//     Assumes the caller has the appropriate lock on the data
//*************************************************************

int _fs_calculate_chksum(osd_fs_t *fs, osd_fs_fd_t *fsfd, tbx_chksum_t *cs, osd_off_t block,
    char *buffer, osd_off_t bufsize, char *calc_value, int correct_errors)
{
  char cs_value[CHKSUM_MAX_SIZE];
  char disk_value[CHKSUM_MAX_SIZE];
  uint32_t block_bytes_used, max_block;
  osd_off_t fpos, nblocks, nleft, n, nbytes;
  int cs_size, err;
  osd_fs_chksum_t *ocs = &(fsfd->obj->fd_chksum);

  err = 0;

  if (block == 0) {
     fpos = FS_MAGIC_HEADER;
     max_block = ocs->header_blocksize;
  } else {
     fpos = FS_MAGIC_HEADER + ocs->hbs_with_chksum + (block-1) * ocs->bs_with_chksum;
     max_block = ocs->blocksize;
  }

  fseeko(fsfd->fd, fpos, SEEK_SET); //** Move to the start of the block
  tbx_chksum_reset(cs);  //** Reset the chksum


  //** Get the chksum and block bytes from disk
  n = 0;
  n = fread(&block_bytes_used, 1, sizeof(uint32_t), fsfd->fd);
  cs_size = tbx_chksum_size(cs, CHKSUM_DIGEST_BIN);
  n = n + fread(disk_value, 1, cs_size, fsfd->fd);
  nbytes = cs_size + sizeof(uint32_t);
  if (n != nbytes) {
     log_printf(0, "_fs_calculate_chksum: Error reading chksum information:  fread=" I64T "\n", n);
     return(1);
  } else if (block_bytes_used > max_block) {
     block_bytes_used = max_block;
     err = 2;
  }

  //** Now cycle through the data
  n = 0;
  nblocks = block_bytes_used / bufsize;
  for (nleft = 0; nleft < nblocks; nleft++) {
     n = n + fread(buffer, 1, bufsize, fsfd->fd);
     fs_chksum_add(cs, bufsize, buffer);
  }
  nleft = block_bytes_used % bufsize;
  if (nleft > 0) {
     n = n + fread(buffer, 1, nleft, fsfd->fd);
     fs_chksum_add(cs, nleft, buffer);
  }

  if (n != block_bytes_used) {
     log_printf(0, "_fs_calculate_chksum: Error reading chksum information:  fread=" I64T "\n", n);
     return(1);
  }

  if (block_bytes_used > 0) { //** Valid chksum stored
     tbx_chksum_get(cs, CHKSUM_DIGEST_BIN, cs_value);

     if (calc_value != NULL) memcpy(calc_value, cs_value, cs_size);

     n = memcmp(disk_value, cs_value, cs_size);
     if (n != 0) {
        nblocks = block_bytes_used;
        log_printf(10, "_fs_calculate_chksum mismatch! memcmp = " I64T " id=" LU " offset=" I64T " block_bytes=" I64T "\n", n, fsfd->obj->id, fpos, nblocks);

        if (correct_errors == 1) { //** Correct it if they want
           fpos = fpos + sizeof(uint32_t);
           fseeko(fsfd->fd, fpos, SEEK_SET); //** Move to the start of the block
           n = fwrite(cs_value, 1, cs_size, fsfd->fd);
           if (n != cs_size) {
              log_printf(0, "_fs_calculate_chksum: Error storing corrected chksum information:  fwrite=" I64T "\n", n);
              if (err == 0) return(3);  //** err could be -1 due to a blocksize error so the chksum could be using the wrong size
           }

        }

        if (err == 0) return(3);
     }
  } else {
    if (calc_value != NULL) memset(calc_value, 0, cs_size);
  }

  return(err);
}


//*************************************************************
//  fs_validate_chksum - Reads the entire object verifying the chksum data.
//     If correct_errors = 1 any errors encountered are corrected.
//     The block error count is returned or 0 if no errors occured.
//     A negative value represents a disk or other internal error.
//*************************************************************

int fs_validate_chksum(osd_t *d, osd_id_t id, int correct_errors)
{
   osd_fs_t *fs = (osd_fs_t *)(d->private);
   osd_fs_chksum_t *fcs;
   char buffer[FS_BUF_SIZE];
   osd_off_t block, start_block, end_block, end_got, nbytes, n;
   int err, errcnt, mode;

   //** Open the allocation in the appropriate mode
   mode = (correct_errors == 0) ? OSD_READ_MODE : OSD_WRITE_MODE;
   osd_fs_fd_t *fsfd = (osd_fs_fd_t *)fs_open(d, id, mode);
   if (fsfd == NULL) return(0);

   fcs = &(fsfd->obj->fd_chksum);

   if (fcs->is_valid == 0) {  //** Chksum is disabled so return
      fs_close(d, (osd_fd_t *)fsfd);
      return(0);
   }

   //** Find out how many bytes there are and the corresponding number of blocks
   fseeko(fsfd->fd, 0, SEEK_END);
   nbytes = ftell(fsfd->fd);
   n = fs_offset_p2l(fs, fsfd, nbytes) - fcs->header_blocksize;
   end_block = n / fcs->blocksize;

   //** Now cycle through all the blocks
   start_block = 0;
   errcnt = 0;
   do {
     fsfd_lock(fs, fsfd, mode, start_block, end_block, &end_got);

     for (block=start_block; block <= end_got; block++) {
       err = _fs_calculate_chksum(fs, fsfd, &(fsfd->chksum), block, buffer, FS_BUF_SIZE, NULL, correct_errors);
       if (err != 0) {
          log_printf(0, "fs_validate_chksum: chksum error id=" LU " block=" I64T " err=%d\n", id, block, err);
          errcnt++;
       }
     }

     start_block = end_got + 1;

     fsfd_unlock(fs, fsfd);
   } while (end_block > end_got);


   //** If we corrected things store a clean state
   if (correct_errors > 0) {
      apr_thread_mutex_lock(fsfd->obj->lock);
      fsfd->obj->header.state = OSD_STATE_GOOD;
      fsfd->obj->state = n;
      fs_write_header(fsfd->fd, &(fsfd->obj->header));
      apr_thread_mutex_unlock(fsfd->obj->lock);
   }

   fs_close(d, (osd_fd_t *)fsfd);

   return(errcnt);
}

//*************************************************************
// statfs - Determine the file system stats
//*************************************************************

int fs_statfs(osd_t *d, struct statfs *buf)
{
  osd_fs_t *fs = (osd_fs_t *)(d->private);

  return(statfs(fs->devicename, buf));
}

//*************************************************************
//  fs_opendir - Opens a sub dir for the OSD
//*************************************************************

int fs_opendir(osd_fs_iter_t *iter)
{
   osd_fs_t *fs = iter->fs;

   char dname[fs->pathlen];

   snprintf(dname, sizeof(dname), "%s/%d", fs->devicename, iter->n);
   iter->cdir = opendir(dname);

   if (iter->cdir == NULL) return(1);
   return(0);
}

//*************************************************************
//  new_iterator - Creates a new iterator to walk through the files
//*************************************************************

osd_iter_t *fs_new_iterator(osd_t *d)
{
   osd_iter_t *oi = (osd_iter_t *)malloc(sizeof(osd_iter_t));
   osd_fs_iter_t *iter = (osd_fs_iter_t *)malloc(sizeof(osd_fs_iter_t));

   if (oi == NULL) return(NULL);
   if (iter == NULL) return(NULL);

   oi->d = d;
   oi->arg = (void *)iter;

   iter->n = 0;
   iter->fs = (osd_fs_t *)(d->private);

   if (fs_opendir(iter) != 0) { free(iter); free(oi); return(NULL); }

   return(oi);
}

//*************************************************************
//  destroy_iterator - Destroys an iterator
//*************************************************************

void fs_destroy_iterator(osd_iter_t *oi)
{
  osd_fs_iter_t *iter;

  if (oi == NULL) return;

  iter = (osd_fs_iter_t *)oi->arg;


  if (iter->cdir != NULL) closedir(iter->cdir);

  free(iter);
  free(oi);
}

//*************************************************************
//  iterator_next - Returns the next key for the iterator
//*************************************************************

int fs_iterator_next(osd_iter_t *oi, osd_id_t *id)
{
  osd_fs_iter_t *iter;
  struct dirent *result;
  int finished, n;

  if (oi == NULL) return(1);
  iter = (osd_fs_iter_t *)oi->arg;

  if (iter->n >= DIR_MAX) return(1);

  finished = 0;
  do {
    n = readdir_r(iter->cdir, &(iter->entry),  &result);

    if ((result == NULL) || (n != 0)) {   //** Change dir or we're finished
       iter->n++;
       if (iter->n == DIR_MAX) return(1);   //** Finished
       closedir(iter->cdir);
       if (fs_opendir(iter) != 0) return(1);   //*** Error opening the directory
    } else if ((strcmp(result->d_name, ".") != 0) && (strcmp(result->d_name, "..") != 0)) {
      finished = 1;               //** Found a valid file
    }
//if (result==NULL) {
//  log_printf(15, "osd_fs:iterator_next: result=NULL n=%d\n", iter->n); tbx_log_flush();
//} else {
//  log_printf(15, "osd_fs:iterator_next: d_name=%s\n", result->d_name); tbx_log_flush();
//}
  } while (finished == 0);

  sscanf(result->d_name, LU, id);
  return(0);
}

//*************************************************************
//  new_trash_iterator - Creates a new iterator to walk through trash
//*************************************************************

osd_iter_t *fs_new_trash_iterator(osd_t *d, int trash_type)
{
   osd_fs_t *fs = (osd_fs_t *)(d->private);
   osd_iter_t *oi = (osd_iter_t *)malloc(sizeof(osd_iter_t));
   osd_fs_iter_t *iter = (osd_fs_iter_t *)malloc(sizeof(osd_fs_iter_t));
   char dname[fs->pathlen];

   if (oi == NULL) return(NULL);
   if (iter == NULL) return(NULL);

   oi->d = d;
   oi->arg = (void *)iter;

   if (trash_type == OSD_EXPIRE_ID) {
      snprintf(dname, sizeof(dname), "%s/expired_trash", fs->devicename);
   } else if (trash_type == OSD_DELETE_ID) {
      snprintf(dname, sizeof(dname), "%s/deleted_trash", fs->devicename);
   } else {
      log_printf(0, "fs_new_trash_iterator: invalid trash_type=%d\n", trash_type);
      free(iter); free(oi);
      return(NULL);
   }

   iter->fs = fs;
   iter->cdir = opendir(dname);
   iter->n = -1;

   if (iter->cdir == NULL) {
      log_printf(0, "new_iterator: error with opendir(%s)\n", dname);
      free(iter); free(oi);
      return(NULL);
   }

   return(oi);
}

//*************************************************************
//  trash_iterator_next - Returns the next key for the trash iterator
//*************************************************************

int fs_trash_iterator_next(osd_iter_t *oi, osd_id_t *id, ibp_time_t *move_time, char *trash_id)
{
  osd_fs_iter_t *iter;
  struct dirent *result;
  int finished, n;
  apr_time_t mtime;

  if (oi == NULL) return(-1);  //** Bad iterator
  iter = (osd_fs_iter_t *)oi->arg;

  finished = 0;
  do {
    n = readdir_r(iter->cdir, &(iter->entry),  &result);

    if ((result == NULL) || (n != 0)) {   //** We're finished
       return(1);  //** Finished;
    } else if ((strcmp(result->d_name, ".") != 0) && (strcmp(result->d_name, "..") != 0)) {
       if (trash_id != NULL) strncpy(trash_id, result->d_name, iter->fs->pathlen-1);
       n = sscanf(result->d_name, TT "_" LU, &mtime, id);
       if (n == 2) {
          *move_time = mtime;
          finished = 1;
       } else {
          log_printf(15, "fs_trash_iterator_next: Invalid file so skipping: %s\n", result->d_name); tbx_log_flush();
       }
    }
//if (result==NULL) {
//  log_printf(15, "osd_fs:iterator_next: result=NULL n=%d\n", iter->n); tbx_log_flush();
//} else {
//  log_printf(15, "osd_fs:iterator_next: d_name=%s\n", result->d_name); tbx_log_flush();
//}
  } while (finished == 0);


  return(0);
}

//*************************************************************
// fs_umount - Unmounts the dir.
//*************************************************************

int fs_umount(osd_t *d)
{
  osd_fs_t *fs = (osd_fs_t *)(d->private);

log_printf(15, "fs_umount fs=%p rid=%s\n",fs, fs->devicename);

  tbx_pc_destroy(fs->fd_list);
  tbx_pc_destroy(fs->obj_list);

  fs_cache_destroy(fs->cache);

  //**NOTE: there is no apr_hash_destroy function:(  so it gets removed when the pool is destroyed
  apr_thread_mutex_destroy(fs->lock);
  apr_thread_mutex_destroy(fs->obj_lock);
  apr_pool_destroy(fs->pool);

  free(fs->devicename);
  free(fs);
  free(d);

  return(0);
}

//*************************************************************
// fs_shelf_fd_new - Creates a new shelf of osd_fs_fd_t.
//*************************************************************

void *fs_shelf_fd_new(void *arg, int size)
{
  osd_fs_t *fs = (osd_fs_t *)arg;
  osd_fs_fd_t *shelf = (osd_fs_fd_t *)malloc(sizeof(osd_fs_fd_t)*size);
  int i;

  memset(shelf, 0, sizeof(osd_fs_fd_t)*size);

log_printf(10, "fs_shelf_fd_new: called data=%p size=%d rid=%s\n", (void *)shelf, size, fs->devicename); tbx_log_flush();

  for (i=0; i<size; i++) {
    apr_thread_mutex_create(&(shelf[i].lock), APR_THREAD_MUTEX_DEFAULT, fs->pool);
    apr_thread_cond_create(&(shelf[i].cond), fs->pool);
  }


  return((void *)shelf);
}

//*************************************************************
// fs_shelf_fd_free - Deletes a shelf of osd_fs_fd_t.
//*************************************************************

void fs_shelf_fd_free(void *arg, int size, void *data)
{
  osd_fs_t *fs = (osd_fs_t *)arg;
  osd_fs_fd_t *shelf = (osd_fs_fd_t *)data;
  int i;

log_printf(10, "fs_shelf_fd_free: called data=%p rid=%s size=%d\n", data, fs->devicename, size); tbx_log_flush();
  for (i=0; i<size; i++) {
    apr_thread_mutex_destroy(shelf[i].lock);
    apr_thread_cond_destroy(shelf[i].cond);
  }

  free(shelf);
}

//*************************************************************
// fs_shelf_range_new - Creates a new shelf of fs_range structs
//*************************************************************

void *fs_shelf_range_new(void *arg, int size)
{
//  log_printf(15, "fs_shelf_range_new: size=%d\n", size); 
  void *ptr = malloc(sizeof(osd_fs_range_t)*size);
  memset(ptr, 0, sizeof(osd_fs_range_t)*size);
  return(ptr);
}

//*************************************************************
// fs_shelf_range_free - Frees an existing shelf of fs_range structs
//*************************************************************

void fs_shelf_range_free(void *arg, int size, void *data)
{
//  log_printf(15, "fs_shelf_range_free: size=%d\n", size); 
   free(data);
}


//*************************************************************
// fs_shelf_object_new - Creates a new shelf of osd_fs_object_t.
//*************************************************************

void *fs_shelf_object_new(void *arg, int size)
{
  osd_fs_object_t *shelf, *obj;
  int i;
//  apr_pool_t *pool = (apr_pool_t *)arg;

  shelf = (osd_fs_object_t *)malloc(sizeof(osd_fs_object_t)*size);
  assert(shelf != NULL);

log_printf(15, "fs_shelf_object_new: shelf=%p size=%d\n", shelf, size); 

  memset(shelf, 0, sizeof(osd_fs_object_t)*size);

  for (i=0; i<size; i++) {
     obj = &(shelf[i]);

     //** Make the read/write ranges for each object
     obj->read_range_list = tbx_pc_new("read_range", FS_RANGE_COUNT, sizeof(osd_fs_range_t), NULL, fs_shelf_range_new, fs_shelf_range_free);
     obj->write_range_list = tbx_pc_new("write_range", FS_RANGE_COUNT, sizeof(osd_fs_range_t), NULL, fs_shelf_range_new, fs_shelf_range_free);
     assert(obj->read_range_list != NULL);
     assert(obj->write_range_list != NULL);

     //** Make the locks
     apr_pool_create(&(obj->pool), NULL);
    apr_thread_cond_create(&(obj->cond), obj->pool);
     apr_thread_mutex_create(&(obj->lock), APR_THREAD_MUTEX_DEFAULT, obj->pool);
  }

  return((void *)shelf);
}

//*************************************************************
// fs_shelf_object_free - Deletes a shelf of osd_fs_object_t.
//*************************************************************

void fs_shelf_object_free(void *arg, int size, void *data)
{
  osd_fs_object_t *shelf = (osd_fs_object_t *)data;
  osd_fs_object_t *obj;
  int i; 

log_printf(15, "fs_shelf_object_free: shelf=%p size=%d\n", shelf, size); 
  for (i=0; i<size; i++) {
     obj = &(shelf[i]);

     //** Free the read/write ranges for each object
     tbx_pc_destroy(obj->read_range_list);
     tbx_pc_destroy(obj->write_range_list);

     //** Free the lock
     apr_thread_mutex_destroy(obj->lock);
     apr_thread_cond_destroy(obj->cond);
//     apr_thread_mutex_destroy(obj->obj_lock);
     apr_pool_destroy(obj->pool);
  }

  //** Lastly free the shelf itself
  free(shelf);

  return;
}


//*************************************************************
// osd_mount_fs - Mounts the device
//*************************************************************

osd_t *osd_mount_fs(const char *device, int n_cache, apr_time_t expire_time) 
{
   int i;
   osd_t *d = (osd_t *)malloc(sizeof(osd_t));
   assert(d != NULL);

   osd_fs_t *fs = (osd_fs_t *)malloc(sizeof(osd_fs_t));
   assert(fs != NULL);

   memset(fs, 0, sizeof(osd_fs_t));

   d->private = (void *)fs;
   fs->devicename = strdup(device);
   fs->pathlen = strlen(fs->devicename) + 100;

   if (strcmp(device, FS_LOOPBACK_DEVICE) != 0) {
      DIR *dir;
      if ((dir = opendir(fs->devicename)) == NULL) {
         log_printf(0, "osd_fs:  Directory does not exist!!!!!! checked: %s\n", device);
      } else {
         closedir(dir);
      }

      //*** Check and make sure all the directories exist ***
      int i;
      char fname[fs->pathlen];
      for (i=0; i<DIR_MAX; i++) {
         snprintf(fname, sizeof(fname), "%s/%d", fs->devicename, i);
         if (mkdir(fname, S_IRUSR | S_IWUSR | S_IXUSR) != 0) {
            if (errno != EEXIST) {
               printf("osd_fs: Error creating/checking sub directories!  errno=%d\n", errno);
               printf("osd_fs: Directory : %s\n", fname);
               abort();
            }
         }
      }

      //** Now make the "deleted_trash" and "expired_trash" directories
      snprintf(fname, sizeof(fname), "%s/deleted_trash", fs->devicename);
      if (mkdir(fname, S_IRUSR | S_IWUSR | S_IXUSR) != 0) {
         if (errno != EEXIST) {
            printf("osd_fs: Error creating deleted_trash directory!  errno=%d\n", errno);
            printf("osd_fs: Directory : %s\n", fname);
            abort();
         }
      }
      snprintf(fname, sizeof(fname), "%s/expired_trash", fs->devicename);
      if (mkdir(fname, S_IRUSR | S_IWUSR | S_IXUSR) != 0) {
         if (errno != EEXIST) {
            printf("osd_fs: Error creating expired_trash directory!  errno=%d\n", errno);
            printf("osd_fs: Directory : %s\n", fname);
            abort();
         }
      }
   }


   fs->mount_type = 0;

//#ifdef _HAS_XFS
//   fs->mount_type = platform_test_xfs_path(device);
//#endif

   log_printf(10, "osd_fs_mount: %s mount_type=%d\n", fs->devicename, fs->mount_type);
   log_printf(15, "fs_mount fs=%p rid=%s\n",fs, fs->devicename); 
 
   apr_pool_create(&(fs->pool), NULL);
   apr_thread_mutex_create(&(fs->lock), APR_THREAD_MUTEX_DEFAULT, fs->pool);
   apr_thread_mutex_create(&(fs->obj_lock), APR_THREAD_MUTEX_DEFAULT, fs->pool);

   //** Lastly set up all the pointers
   d->umount = fs_umount;
   d->create_id = fs_create_id;
   d->native_open = fs_native_open;
   d->native_close = fs_native_close;
   d->reserve = fs_reserve;
   d->remove = fs_remove;
   d->chksum_info = fs_chksum_info;
   d->get_chksum = fs_get_chksum;
   d->validate_chksum = fs_validate_chksum;
   d->delete_remove = fs_delete_remove;
   d->expire_remove = fs_expire_remove;
   d->physical_remove = fs_physical_remove;
   d->trash_physical_remove = fs_trash_physical_remove;
   d->trash_undelete = fs_trash_undelete;
   d->truncate = fs_truncate;
   d->size = fs_size;
   d->fd_size = fs_fd_size;
   d->trash_size = fs_trash_size;
   d->read = fs_read;
   d->write = fs_write;
   d->open = fs_open;
   d->get_state = fs_get_state;
   d->close = fs_close;
   d->id_exists = fs_id_exists;
   d->statfs = fs_statfs;
   d->get_corrupt_count = fs_corrupt_count;
   d->new_corrupt_iterator = fs_new_corrupt_iterator;
   d->destroy_corrupt_iterator = fs_destroy_corrupt_iterator;
   d->corrupt_iterator_next = fs_corrupt_iterator_next;
   d->new_iterator = fs_new_iterator;
   d->new_trash_iterator = fs_new_trash_iterator;
   d->destroy_iterator = fs_destroy_iterator;
   d->iterator_next = fs_iterator_next;
   d->trash_iterator_next = fs_trash_iterator_next;

   if (strcmp(device, FS_LOOPBACK_DEVICE) == 0) {
      fs->id2fname = id2fname_loopback;
      fs->trashid2fname = trashid2fname_loopback;
   } else {
      fs->id2fname = id2fname_normal;
      fs->trashid2fname = trashid2fname_normal;
   }

   //** Now make the object and Object FD buffers
   fs->obj_list = tbx_pc_new("obj_list", FS_OBJ_COUNT, sizeof(osd_fs_object_t), (void *)fs, fs_shelf_object_new, fs_shelf_object_free); assert(fs->obj_list != NULL);
   fs->fd_list = tbx_pc_new("fd_list", FS_OBJ_COUNT, sizeof(osd_fs_fd_t), (void *)fs, fs_shelf_fd_new, fs_shelf_fd_free); assert(fs->fd_list != NULL);
   fs->obj_hash = apr_hash_make(fs->pool); assert(fs->obj_hash != NULL);
   fs->corrupt_hash = apr_hash_make(fs->pool); assert(fs->corrupt_hash != NULL);

   //** Lastly make the cache buffers
   i = (n_cache > 0) ? sqrt(1.0*n_cache) : 0;
   fs->cache = fs_cache_create(fs->pool, i, i, expire_time);

   return(d);
}


