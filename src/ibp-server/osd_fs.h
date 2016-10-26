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

//**************************************************
//
//**************************************************

#ifndef __OSD_FS_H
#define __OSD_FS_H

#include <ibp-server/visibility.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <stdint.h>
#include <time.h>
#include <apr_thread_mutex.h>
#include <apr_thread_cond.h>
#include <apr_pools.h>
#include <apr_hash.h>
#include <openssl/rand.h>
#include <errno.h>
#include "osd_abstract.h"
#include "random.h"
#include "statfs.h"
#include <tbx/chksum.h>
#include "pigeon_coop.h"

//******** These are for the directory splitting ****
#define DIR_BITS    8
#define DIR_BITMASK 0x00FF
#define DIR_MAX     256

#define FS_MAX_LOOPBACK 100

#define CHKSUM_MAGIC  ".CHKSUM1."
#define FS_LOOPBACK_DEVICE "loopback"

#define XFS_MOUNT 1

typedef struct { 
  osd_id_t id;
  int      block;
} __attribute__((__packed__)) fs_cache_key_t;

typedef struct {
  fs_cache_key_t key;
  apr_time_t     time;
} fs_cache_entry_t;

typedef struct {
  apr_thread_mutex_t *lock;
  apr_hash_t *hash;
  fs_cache_entry_t *table;
  int *block_free_count;
  int *block_last_offset;
  apr_time_t max_wait;
  int n_blocks;
  int block_size;
  int n_total;
} fs_cache_table_t;

typedef struct {      //** This is used to determine if the allocation uses chksums
  uint32_t tbx_chksum_type;
  uint32_t header_size;
  uint32_t block_size;
  uint16_t state;
  char magic[10];
} __attribute__((__packed__)) fs_header_t;

typedef struct {
  tbx_chksum_t chksum;
  osd_off_t blocksize;
  osd_off_t header_blocksize;
  osd_off_t bs_with_chksum;
  osd_off_t hbs_with_chksum;
  int       is_valid;
} osd_fs_chksum_t;

typedef struct osd_fs_s osd_fs_t;
typedef struct osd_fs_object_s osd_fs_object_t;
typedef struct osd_fs_fd_s osd_fs_fd_t;

typedef struct {
  int64_t  timestamp;
  osd_off_t lo;
  osd_off_t hi;
  osd_fs_fd_t *fsfd;
  int type;       //** RANGE_INUSE or RANGE_REQUEST
} osd_fs_range_t;


struct osd_fs_fd_s {
  osd_fs_object_t *obj;
  FILE *fd;
  apr_thread_mutex_t *lock;  //** Used strictly for thread safety on the FD.
  apr_thread_cond_t *cond;
  tbx_chksum_t chksum;
  tbx_pc_t *my_range_coop;
  tbx_pch_t my_range_slot;
  tbx_pch_t my_slot;
  int timestamp;
};

struct osd_fs_object_s {
  int n_pending;                 //** Pending tasks trying to open the object.  This should only be accessed if holding the fs->obj_lock!!!
  int n_opened;                  //** Number of times the object is opened
  int n_read;                    //** Current number of read operations
  int n_write;                   //** Current number of write operations
  int is_ok;                     //** Determines if object_open completed successfully
  osd_id_t id;                   //** Object ID
  int     state;                 //** Object state
  int64_t count;                 //** Used for coordinating writes as a timestamp
  osd_fs_chksum_t fd_chksum;
  fs_header_t header;            //** Object header
  tbx_pch_t my_slot;
//  osd_off_t *read_range_list;  //** List of host Read block ranges
  tbx_pc_t *read_range_list;
//  osd_off_t *write_range_list;  //** List of host Write block ranges
  tbx_pc_t *write_range_list;
  osd_off_t (*read)(osd_fs_t *fs, osd_fs_fd_t *fsfd, osd_off_t offset, osd_off_t len, buffer_t buffer);   //Read data
  osd_off_t (*write)(osd_fs_t *fs, osd_fs_fd_t *fsfd, osd_off_t offset, osd_off_t len, buffer_t buffer);  //Store data to disk
  apr_thread_mutex_t *lock;
  apr_thread_cond_t *cond;
  apr_pool_t *pool;
};

struct osd_fs_s {
    char *devicename;
    int  pathlen;
    int  mount_type;
    int  max_objs;
    int  max_fd_per_obj;
//    osd_fs_object_t *obj_list;
    tbx_pc_t *obj_list;
//    osd_fs_fd_t     *fd_list;
    tbx_pc_t *fd_list;
    apr_hash_t *obj_hash;
    apr_hash_t *corrupt_hash;
    fs_cache_table_t *cache;
    apr_thread_mutex_t *lock;
    apr_thread_mutex_t *obj_lock;
    apr_pool_t *pool;
    char *id_map[FS_MAX_LOOPBACK];
    char *(*id2fname)(struct osd_fs_s *fs, osd_id_t id, char *fname, int len);
    char *(*trashid2fname)(struct osd_fs_s *fs, int trash_type, const char *trash_id, char *fname, int len);
};

typedef struct {
   osd_fs_t *fs;         //** Device pointer
   DIR *cdir;            //** DIR handle
   int  n;               //** Directory number
   struct dirent entry;  //** Used by readdir_r
} osd_fs_iter_t;

typedef struct {
   osd_fs_t *fs;
   apr_hash_index_t *iter;
   apr_pool_t *pool;
   int first_time;
} osd_fs_corrupt_iter_t;

IBPS_API osd_t *osd_mount_fs(const char *device, int n_cache, apr_time_t expire_time);
IBPS_API int fs_associate_id(osd_t *d, int id, char *fname);

#endif

