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
  apr_time_t recheck_time;
  int v_size;
  char *val;
} lio_attr_t;

typedef struct {
  ex_id_t ino;
  ex_off_t size;
  int flagged;
  int modify_data_ts;
  int modify_attr_ts;
  int ftype;
  int nlinks;
  lio_fuse_file_handle_t *fh;
  char *link;
  apr_time_t recheck_time;
} lio_inode_t;

typedef struct {
  char *fname;
  ex_id_t ino;
  int name_start;
  int flagged;
  int ref_count;  //** Represents open files
  apr_time_t recheck_time;
} lio_dentry_t;

typedef struct {  //** Individual file descriptor
  lio_fuse_file_handle_t *fh;  //** Shared handle
  lio_dentry_t *entry;  //** Prointer to my dentry
  int mode;         //** R/W mode
} lio_fuse_fd_t;

typedef struct {
  double attr_to;
  double entry_to;
  int inode_cache_size;
  apr_time_t xattr_to;
  apr_time_t stale_dt;
  apr_time_t gc_interval;
  ex_off_t readahead;
  int file_count;
  int enable_tape;
  int shutdown;
  int mount_point_len;
  atomic_int_t counter;
  list_t *ino_index;
  list_t *fname_index;
  list_t *attr_index;
  lio_config_t *lc;
  apr_pool_t *mpool;
  apr_thread_t *gc_thread;
  apr_thread_mutex_t *lock;
  apr_thread_mutex_t **file_lock;
  struct fuse_operations fops;
  char *id;
  char *mount_point;
} lio_fuse_t;

struct lio_fuse_file_handle_s {  //** Shared file handle
  exnode_t *ex;
  segment_t *seg;
  lio_fuse_t *lfs;
  int ref_count;
  ex_off_t readahead_end;
  atomic_int_t modified;
};

typedef struct {
  lio_config_t *lc;
  char *mount_point;
  int lio_argc;
  char **lio_argv;
} lio_fuse_init_args_t;

extern struct fuse_operations lfs_fops;

void *lfs_init(struct fuse_conn_info *conn);  // returns pointer to lio_fuse_t on success, otherwise NULL
void lfs_destroy(void *lfs); // expects a lio_fuse_t* as the argument

// Forward declarations for non-FUSE consumers of this
void *lfs_init_real(struct fuse_conn_info *conn, const int argc, const char **argv, const char *mount_point);
int lfs_mkdir(const char *fname, mode_t mode);
int lfs_unlink_real(const char *fname, lio_fuse_t *lfs);
int lfs_release_real(const char *fname,  struct fuse_file_info *fi, lio_fuse_t *lfs);
int lfs_stat_real(const char *fname, struct stat *stat, lio_fuse_t *lfs);
int lfs_mknod_real(const char *fname, mode_t mode, dev_t rdev, lio_fuse_t *lfs);
int lfs_open_real(const char *fname, struct fuse_file_info *fi, lio_fuse_t *lfs);
int lfs_release_real(const char *fname, struct fuse_file_info *fi, lio_fuse_t *lfs);
int lfs_read(const char *fname, char *buf, size_t size, off_t off, struct fuse_file_info *fi);
int lfs_readv(const char *fname, iovec_t *iov, int n_iov, size_t size, off_t off, struct fuse_file_info *fi);
int lfs_write(const char *fname, const char *buf, size_t size, off_t off, struct fuse_file_info *fi);
int lfs_writev(const char *fname, iovec_t *iov, int n_iov, size_t size, off_t off, struct fuse_file_info *fi);
int lfs_setxattr_real(const char *fname, const char *name, const char *fval, size_t size, int flags, lio_fuse_t *lfs);
int lfs_getxattr_real(const char *fname, const char *name, char *buf, size_t size, lio_fuse_t *lfs);
int lfs_unlink_real(const char *fname, lio_fuse_t *lfs);
int lfs_opendir_real(const char *fname, struct fuse_file_info *fi, lio_fuse_t *lfs);
int lfs_readdir_real(const char *dname, void *buf, fuse_fill_dir_t filler, off_t off, struct fuse_file_info *fi, lio_fuse_t * lfs);
#ifdef __cplusplus
}
#endif

#endif

