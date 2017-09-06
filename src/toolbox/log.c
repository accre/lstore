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

//***************************************************************
//***************************************************************

#define _log_module_index 100

#include <apr_errno.h>
#include <apr_thread_mutex.h>
#include <string.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "log.h"
#include "tbx/assert_result.h"
#include "tbx/atomic_counter.h"
#include "tbx/iniparse.h"
#include "tbx/visibility.h"
#include "tbx/type_malloc.h"

TBX_API int tbx_stack_get_info_level(tbx_log_fd_t *fd) {
    return fd->level;
}

FILE *_log_fd = NULL;
int _log_level = 0;
long int _log_currsize = 0;
long int _log_maxsize = 100*1024*1024;
int _log_special = 0;

apr_thread_mutex_t *_log_lock = NULL;
apr_pool_t *_log_mpool = NULL;
char _log_fname[1024] = "stdout";
int _mlog_table[_mlog_size];
char *_mlog_file_table[_mlog_size];

//***************************************************************
// _log_init - Init the log routines
//***************************************************************

void _log_init()
{
    int n;

    tbx_atomic_startup();
    if (apr_pool_create(&_log_mpool, NULL) != APR_SUCCESS) {
        fprintf(stderr, "Could not make log memory pool\n");
        exit(1);
    }
    if (apr_thread_mutex_create(&_log_lock,
                                APR_THREAD_MUTEX_DEFAULT,
                                _log_mpool) != APR_SUCCESS) {
        fprintf(stderr, "Could not make log mutex\n");
        exit(1);
    }
    for (n=0; n<_mlog_size; n++) {
        _mlog_table[n]=20;
        _mlog_file_table[n] = "";
    }
}
//***************************************************************
// _open_log - Opens the log file for access
//***************************************************************

void tbx_log_open(char *fname, int dolock)
{
    if (dolock == 1) {
        if (_log_lock == NULL) _log_init();

        _lock_log();
    }

    _log_currsize = 0;

    if (fname != NULL) {    //** If NULL we'll just use the old name
        strncpy(_log_fname, fname, sizeof(_log_fname)-1);
        _log_fname[sizeof(_log_fname)-1]= '\0';
    }

    _log_special = 0;
    if (fname == NULL) {  //** Old file so just truncate
        fseek(_log_fd, SEEK_SET, 0L);
        if(ftruncate(fileno(_log_fd), 0L)){ perror("OPEN_LOG: truncate error ");}
    } else if (strcmp(_log_fname, "stdout") == 0) {
        _log_special = 1;
        _log_fd = stdout;
    } else if (strcmp(_log_fname, "stderr") == 0) {
        _log_special = 2;
        _log_fd = stderr;
    } else if ((_log_fd = fopen(_log_fname, "w")) == NULL) {
        fprintf(stderr, "OPEN_LOG failed! Attempted to us log file %s\n", _log_fname);
        perror("OPEN_LOG: ");
    }

    if (dolock == 1) _unlock_log();
}

void _close_log()
{
    if ((strcmp(_log_fname, "stdout") != 0) && (strcmp(_log_fname, "stderr") != 0)) {
        fclose(_log_fd);
    }
}

//***************************************************************
// mlog_printf - Prints data to the log file
//***************************************************************

__attribute__((format (printf, 7, 8)))
int tbx_mlog_printf(int suppress_header, int module_index, int level, const char *fn, const char *fname, int line, const char *fmt, ...)
{
    va_list args;
//  int err;
    int n = 0;

    if (level > _mlog_table[module_index]) return(0);
    if (level > _log_level) return(0);

    if (_log_lock == NULL) _log_init();

    _lock_log();
    if (_log_fd == NULL) {
        _log_fd = stderr;
        _log_special=2;
    }

    if (suppress_header == 0) n = fprintf(_log_fd, "[mi=%d tid=%d file=%s:%d fn=%s] ", module_index, tbx_atomic_thread_id, fname, line, fn);
    va_start(args, fmt);
    n += vfprintf(_log_fd, fmt, args);
    va_end(args);

    _log_currsize += n;
    if (_log_currsize > _log_maxsize) {
        if (_log_special==0) {
            tbx_log_open(NULL, 0);
        }
        _log_currsize = 0;
    }
    _unlock_log();

    return(n);
}

//***************************************************************
// flush_log - Flushes the log file
//***************************************************************

void tbx_log_flush()
{
    if (_log_lock == NULL) _log_init();

    _lock_log();
    fflush(_log_fd);
    _unlock_log();
}

//***************************************************************
// mlog_load - Loads the module log information
//***************************************************************

void tbx_mlog_load(tbx_inip_file_t *fd, char *output_override, int log_level_override)
{
    char *group_index, *group_level;
    char *name, *value, *logname;
    int n, default_level;
    tbx_inip_group_t *g;
    tbx_inip_element_t *ele;


    if (_log_lock == NULL) _log_init();

    group_index = "log_index";
    group_level = "log_level";

    if (fd == NULL) {
        log_printf(0, "Error loading module definitions!\n");
        return;
    }

    default_level = tbx_inip_get_integer(fd, group_level, "default", 0);
    _log_level = (log_level_override > -10) ? log_level_override : tbx_inip_get_integer(fd, group_level, "start_level", 0);
    for (n=0; n<_mlog_size; n++) _mlog_table[n] = default_level;
    logname = (output_override == NULL) ? tbx_inip_get_string(fd, group_level, "output", "stdout") : strdup(output_override);
    open_log(logname);
    free(logname);
    _log_maxsize = tbx_inip_get_integer(fd, group_level, "size", 100*1024*1024);

    //** Load the mappings
    g = tbx_inip_group_find(fd, group_index);
    if (g == NULL) {
        log_printf(1, "Missing %s group!\n", group_index);
        return;
    }

    ele = tbx_inip_ele_first(g);
    while (ele != NULL) {
        name = tbx_inip_ele_get_key(ele);
        value = tbx_inip_ele_get_value(ele);

        n = (value != NULL) ? atoi(value) : -1;

        if ((n>=0) && (n<_mlog_size)) {
            _mlog_file_table[n] = strdup(name);
            _mlog_table[n] = tbx_inip_get_integer(fd, group_level, name, _mlog_table[n]);
//printf("mlog_load: mi=%d key=%s val=%d\n", n, name, _mlog_table[n]);
        } else {
            log_printf(0, "Invalid index: %s=%d  should be between 0..%d!  Skipping option\n", name, n, _mlog_size);
        }

        //** Move to the next segmnet to load
        ele = tbx_inip_ele_next(ele);
    }
}


//***************************************************************
// minfo_printf -Does a normal printf
//***************************************************************

__attribute__((format (printf, 7, 8)))
int tbx_minfo_printf(tbx_log_fd_t *ifd, int module_index, int level, const char *fn, const char *fname, int line, const char *fmt, ...)
{
    va_list args;
    int n = 0;

    if (level > _mlog_table[module_index]) return(0);
    if (level > ifd->level) return(0);

    apr_thread_mutex_lock(ifd->lock);

    //** Print the the header
    switch (ifd->header_type) {
    case INFO_HEADER_NONE:
        break;
    case INFO_HEADER_THREAD:
        n = fprintf(ifd->fd, "[tid=%d] ", tbx_atomic_thread_id);
        break;
    case INFO_HEADER_FULL:
        n = fprintf(ifd->fd, "[mi=%d tid=%d file=%s:%d fn=%s] ", module_index, tbx_atomic_thread_id, fname, line, fn);
        break;
    }

    //** Print the user text
    va_start(args, fmt);
    n += vfprintf(ifd->fd, fmt, args);
    va_end(args);

    apr_thread_mutex_unlock(ifd->lock);

    return(n);
}

//***************************************************************
// info_flush - Flushes teh info device
//***************************************************************

void tbx_info_flush(tbx_log_fd_t *ifd)
{
    apr_thread_mutex_lock(ifd->lock);
    fflush(ifd->fd);
    apr_thread_mutex_unlock(ifd->lock);
}

//***************************************************************
// info_create - Creates an info FD device
//***************************************************************

tbx_log_fd_t *tbx_info_create(FILE *fd, int header_type, int level)
{
    tbx_log_fd_t *ifd;

    tbx_type_malloc(ifd, tbx_log_fd_t, 1);

    if (_log_lock == NULL) _log_init();  //** WE use the log mpool

    assert_result(apr_thread_mutex_create(&(ifd->lock), APR_THREAD_MUTEX_DEFAULT, _log_mpool), APR_SUCCESS);
    ifd->fd = fd;
    ifd->header_type = header_type;
    ifd->level = level;

    return(ifd);
}

//***************************************************************
// info_destroy - Destroys a previously created info device
//***************************************************************

void tbx_info_destroy(tbx_log_fd_t *ifd)
{
    apr_thread_mutex_destroy(ifd->lock);
    free(ifd);
}
