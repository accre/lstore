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

#pragma once
#ifndef HDFS_LSTORE_H_INCLUDED
#define HDFS_LSTORE_H_INCLUDED

#include "visibility.h"
#include <inttypes.h>

#ifdef __cplusplus
extern "C" {
#endif

#define HDFSL_OPEN_READ   1
#define HDFSL_OPEN_WRITE  2
#define HDFSL_OPEN_APPEND 3

// Types
typedef struct hdfs_lstore_s hdfs_lstore_t;
typedef struct hdfsl_fd_s hdfsl_fd_t;

typedef struct {
    char *path;
    char *symlink;
    char *user;
    char *group;
    int  perms;
    int64_t len;
    int64_t blocksize;
    long modify_time_ms;
    int   objtype;
} hdfsl_fstat_t;

typedef struct hdfsl_fstat_iter_s hdfsl_fstat_iter_t;

HDFSL_API hdfs_lstore_t *lstore_activate(int *argc, char ***argv);
HDFSL_API void lstore_deactivate(hdfs_lstore_t *ctx);
HDFSL_API int lstore_delete(hdfs_lstore_t *ctx, char *path, int recurse_depth);
HDFSL_API int lstore_fstat(hdfs_lstore_t *ctx, char *path, hdfsl_fstat_t *fstat);
HDFSL_API hdfsl_fstat_iter_t *lstore_fstat_iter(hdfs_lstore_t *ctx, char *path, int recurse_depth);
HDFSL_API int lstore_fstat_iter_next(hdfsl_fstat_iter_t *it, hdfsl_fstat_t *fstat);
HDFSL_API void lstore_fstat_iter_destroy(hdfsl_fstat_iter_t *it);
HDFSL_API int lstore_mkdir(hdfs_lstore_t *ctx, char *path);
HDFSL_API int lstore_rename(hdfs_lstore_t *ctx, char *src_path, char *dest_path);
HDFSL_API hdfsl_fd_t *lstore_open(hdfs_lstore_t *ctx, char *path, int mode);
HDFSL_API void lstore_close(hdfsl_fd_t *fd);
HDFSL_API int lstore_read(hdfsl_fd_t *fd, char *buf, int64_t length);
HDFSL_API int lstore_write(hdfsl_fd_t *fd, char *buf, int64_t length);
HDFSL_API void lstore_seek(hdfsl_fd_t *fd, int64_t off);
HDFSL_API int64_t lstore_getpos(hdfsl_fd_t *fd);

#ifdef __cplusplus
}
#endif

#endif
