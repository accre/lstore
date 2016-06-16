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

//***********************************************************************
// lio_fuse.h - LIO Linux FUSE header file
//***********************************************************************

#ifndef _LIO_FUSE_H_
#define _LIO_FUSE_H_

#define FUSE_USE_VERSION 26

#include <apr_hash.h>
#include <apr_pools.h>
#include <apr_thread_mutex.h>
#include <apr_time.h>
#include <fuse.h>
#include <fuse/fuse.h>
#include <tbx/atomic_counter.h>
#include <tbx/list.h>

#include "ex3_abstract.h"
#include "lio.h"
#include "lio/lio_visibility.h"
#include "lio_abstract.h"

#ifdef __cplusplus
extern "C" {
#endif


struct fuse_conn_info;
struct lio_fuse_init_args_t;
struct lio_fuse_t;

#define LFS_READ_MODE  1
#define LFS_WRITE_MODE 2

#define LFS_TAPE_ATTR "system.tape"

#define LFS_INODE_OK     0  //** Everythings fine
#define LFS_INODE_DROP   1  //** Drop the inode from the cache
#define LFS_INODE_DELETE 2  //** Remove it from cache and delete the file contents

typedef struct lio_fuse_file_handle_s lio_fuse_file_handle_t;

typedef struct lio_fuse_t lio_fuse_t;
struct lio_fuse_t {
int enable_tape;
int shutdown;
int mount_point_len;
tbx_atomic_unit32_t counter;
tbx_list_t *ino_index;
lio_config_t *lc;
apr_pool_t *mpool;
apr_thread_mutex_t *lock;
apr_hash_t *open_files;
struct fuse_operations fops;
char *id;
char *mount_point;
segment_rw_hints_t *rw_hints;
};

typedef struct lio_fuse_init_args_t lio_fuse_init_args_t;
struct lio_fuse_init_args_t {
lio_config_t *lc;
char *mount_point;
int lio_argc;
char **lio_argv;
};

LIO_API extern struct fuse_operations lfs_fops;

void *lfs_init(struct fuse_conn_info *conn);  // returns pointer to lio_fuse_t on success, otherwise NULL
void lfs_destroy(void *lfs); // expects a lio_fuse_t* as the argument

#ifdef __cplusplus
}
#endif

#endif


