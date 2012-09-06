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

//**********************************************************
//**********************************************************

#ifndef __LOG_H_
#define __LOG_H_

#ifndef _DISABLE_LOG

#include <unistd.h>
#include <sys/types.h>
#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include <assert.h>
#include <apr_pools.h>
#include <apr_thread_mutex.h>

#ifdef __cplusplus
extern "C" {
#endif

#define _mlog_size 1024

typedef struct {
  apr_thread_mutex_t *lock;
  FILE *fd;
  int header_type;
  int level;
} info_fd_t;

#define INFO_HEADER_NONE   0
#define INFO_HEADER_THREAD 1
#define INFO_HEADER_FULL   2

info_fd_t *info_create(FILE *fd, int header_type, int level);
void info_destroy(info_fd_t *fd);
void flush_info(info_fd_t *fd);
//int info_printf(info_fd_t *fd, int level, const char *fmt, ...);
int minfo_printf(info_fd_t *ifd, int module_index, int level, const char *fn, const char *fname, int line, const char *fmt, ...);
void info_flush(info_fd_t *ifd);
#define info_printf(ifd, n, ...) minfo_printf(ifd, _log_module_index, n, __func__, _mlog_file_table[_log_module_index], __LINE__, __VA_ARGS__)
#define get_info_header_type(fd) fd->header_type
#define set_info_header_type(fd, new_type) fd->header_type = new_type
#define get_info_level(fd) fd->level
#define set_info_level(fd, new_level) fd->level = new_level


extern int _mlog_table[_mlog_size];
extern char *_mlog_file_table[_mlog_size];

extern FILE *_log_fd;
extern int _log_level;
extern long int _log_maxsize;
extern long int _log_currsize;
extern int _log_special;
extern apr_thread_mutex_t *_log_lock;
extern apr_pool_t *_log_mpool;
extern char _log_fname[1024];

void _open_log(char *fname, int dolock);
void _close_log();
void flush_log();
int mlog_printf(int suppress_header, int module_index, int level, const char *fn, const char *fname, int line, const char *fmt, ...);
void mlog_load(char *fname);

#ifndef _log_module_index
#define _log_module_index 0
#endif

#define _lock_log() apr_thread_mutex_lock(_log_lock)
#define _unlock_log() apr_thread_mutex_unlock(_log_lock)
#define log_code(a) a
#define log_level() _log_level
#define set_log_level(n) _log_level = n
#define set_log_maxsize(n) _log_maxsize = n
#define close_log()  _close_log()
#define log_fd()     _log_fd

#define open_log(fname) _open_log(fname, 1)
#define log_printf(n, ...) mlog_printf(0, _log_module_index, n, __func__, _mlog_file_table[_log_module_index], __LINE__, __VA_ARGS__)
#define slog_printf(n, ...) mlog_printf(1, _log_module_index, n, __func__, _mlog_file_table[_log_module_index], __LINE__, __VA_ARGS__)

#define assign_log_fd(fd) _log_fd = fd

#else

#define log_code(a)
#define set_log_level(n)
#define log_level() -1
#define open_log(fname)
#define close_log()
#define log_fd()     stdout
#define truncate_log()
#define assign_log_fd(fd)
#define flush_log()
#define log_printf(n, ...)
#define mlog_printf(mi, n, ...)

#endif

#ifdef __cplusplus
}
#endif

#endif

