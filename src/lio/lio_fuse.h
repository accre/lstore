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

#include <fuse.h>
#include <apr_time.h>
#include "list.h"
#include "lio.h"

#define LFS_READ_MODE  1
#define LFS_WRITE_MODE 2

//#define LFS_TAPE_ATTR "user.tape_system"
#define LFS_TAPE_ATTR "system.tape"

#define LFS_INODE_OK     0  //** Everythings fine
#define LFS_INODE_DROP   1  //** Drop the inode from the cache
#define LFS_INODE_DELETE 2  //** Remove it from cache and delete the file contents

typedef struct lio_fuse_file_handle_s lio_fuse_file_handle_t;

typedef struct {
  int enable_tape;
  int shutdown;
  int mount_point_len;
  atomic_int_t counter;
  list_t *ino_index;
  lio_config_t *lc;
  apr_pool_t *mpool;
  apr_thread_mutex_t *lock;
  apr_hash_t *open_files;
  struct fuse_operations fops;
  char *id;
  char *mount_point;
  segment_rw_hints_t *rw_hints;
} lio_fuse_t;

typedef struct {
  lio_config_t *lc;
  char *mount_point;
  int lio_argc;
  char **lio_argv;
} lio_fuse_init_args_t;

extern struct fuse_operations lfs_fops;

void *lfs_init(struct fuse_conn_info *conn);  // returns pointer to lio_fuse_t on success, otherwise NULL
void lfs_destroy(void *lfs); // expects a lio_fuse_t* as the argument

#ifdef __cplusplus
}
#endif

#endif

