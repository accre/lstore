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
// lio_fuse.h - LIO Linux FUSE header file
//***********************************************************************

#ifndef _LIO_FUSE_H_
#define _LIO_FUSE_H_

#ifdef __cplusplus
extern "C" {
#endif

#define FUSE_USE_VERSION 26

#include <fuse_lowlevel.h>
#include "list.h"
#include "lio.h"

#define LFS_READ_MODE  1
#define LFS_WRITE_MODE 2

#define LFS_TAPE_ATTR "user.tape_system"

#define LFS_INODE_OK     0  //** Everythings fine
#define LFS_INODE_DROP   1  //** Drop the inode from the cache
#define LFS_INODE_DELETE 2  //** Remove it from cache and delete the file contents

typedef struct lio_fuse_file_handle_s lio_fuse_file_handle_t;

typedef struct lio_inode_s lio_inode_t;

typedef struct {
  char *fname;
  int name_start;
  lio_inode_t *inode;
  int flagged;
  fuse_req_t req;
} lio_dentry_t;

struct lio_inode_s {
  ex_id_t ino;
  int modify_data_ts;
  int modify_attr_ts;
//  char *fname;
//  int name_start;
  int ftype;
  ex_off_t size;
  int fuse_count;
  int lfs_count;
  int flagged_object;
  int pending_count;
  int pending_update;
  int nlinks;
//  fuse_req_t req;
  lio_dentry_t *entry1;
  Stack_t *dentry_stack;
  Stack_t *remove_stack;
  lio_fuse_file_handle_t *fh;
  apr_time_t recheck_time;
};

typedef struct {
  double attr_to;
  double entry_to;
  int inode_cache_size;
  int cond_count;
  int enable_tape;
  atomic_int_t counter;
  list_t *new_inode_list;
  list_t *ino_index;
  list_t *fname_index;
  lio_config_t *lc;
  apr_thread_t *rw_thread;
  apr_pool_t *mpool;
  apr_thread_mutex_t *lock;
  apr_thread_mutex_t *rw_lock;
  apr_thread_cond_t **inode_cond;
  struct fuse_lowlevel_ops llops;
  char *id;
  opque_t *q;
  Stack_t *rw_stack;
  int shutdown;
//  atomic_int_t ino_counter;
} lio_fuse_t;

struct lio_fuse_file_handle_s {  //** Shared file handle
  lio_inode_t *inode;
  exnode_t *ex;
  segment_t *seg;
  lio_fuse_t *lfs;
  int ref_count;
  atomic_int_t modified;
};

typedef struct {  //** Individual file descriptor
  lio_fuse_file_handle_t *fh;  //** Shared hnadle
  int mode;         //** R/W mode
} lio_fuse_fd_t;


extern lio_fuse_t *lfs_gc;
extern struct fuse_lowlevel_ops lfs_gc_llops;

lio_fuse_t *lio_fuse_init(lio_config_t *lc);
void lio_fuse_destroy(lio_fuse_t *lfs);


#ifdef __cplusplus
}
#endif

#endif

