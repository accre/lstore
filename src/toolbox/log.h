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

//**********************************************************
//**********************************************************

#ifndef __LOG_H_
#define __LOG_H_

#include "tbx/toolbox_visibility.h"
#ifndef _DISABLE_LOG

#include <unistd.h>
#include <sys/types.h>
#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include <assert.h>
#include "tbx/assert_result.h"
#include <apr_pools.h>
#include <apr_thread_mutex.h>

#ifdef __cplusplus
extern "C" {
#endif


struct tbx_log_fd_t {
    apr_thread_mutex_t *lock;
    FILE *fd;
    int header_type;
    int level;
};

void info_destroy(tbx_log_fd_t *ifd);
void flush_info(tbx_log_fd_t *fd);
//int info_printf(tbx_log_fd_t *fd, int level, const char *fmt, ...);
#define get_info_header_type(fd) fd->header_type
#define set_info_header_type(fd, new_type) fd->header_type = new_type
#define set_info_level(fd, new_level) fd->level = new_level


extern int _mlog_table[_mlog_size];

extern FILE *_log_fd;
extern long int _log_maxsize;
extern long int _log_currsize;
extern int _log_special;
extern apr_thread_mutex_t *_log_lock;
extern apr_pool_t *_log_mpool;
extern char _log_fname[1024];

void _close_log();

#define _lock_log() apr_thread_mutex_lock(_log_lock)
#define _unlock_log() apr_thread_mutex_unlock(_log_lock)
#define log_code(a) a
#define set_log_maxsize(n) _log_maxsize = n
#define close_log()  _close_log()
#define log_fd()     _log_fd

#define open_log(fname) tbx_log_open(fname, 1)

#define assign_log_fd(fd) _log_fd = fd

#else

#define log_code(a)
#define tbx_set_log_level(n)
#define open_log(fname)
#define close_log()
#define log_fd()     stdout
#define truncate_log()
#define assign_log_fd(fd)
#define tbx_log_flush()
#define log_printf(n, ...)
#define tbx_mlog_printf(mi, n, ...)

#endif

#ifdef __cplusplus
}
#endif

#endif

