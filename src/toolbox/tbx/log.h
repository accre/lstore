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
#ifndef ACCRE_LOG_H_INCLUDED
#define ACCRE_LOG_H_INCLUDED

#include <stdio.h>
#include <tbx/iniparse.h>
#include <tbx/visibility.h>

#ifdef __cplusplus
extern "C" {
#endif

// Types
typedef struct tbx_log_fd_t tbx_log_fd_t;

// Functions
TBX_API tbx_log_fd_t *tbx_info_create(FILE *fd, int header_type, int level);
TBX_API void tbx_info_destroy(tbx_log_fd_t *ifd);
TBX_API void tbx_info_flush(tbx_log_fd_t *ifd);
TBX_API void tbx_log_flush();
TBX_API void tbx_log_open(char *fname, int dolock);
TBX_API int tbx_minfo_printf(tbx_log_fd_t *ifd, int module_index, int level, const char *fn, const char *fname, int line, const char *fmt, ...) __attribute__((format (printf, 7, 8)));
TBX_API void tbx_mlog_load(tbx_inip_file_t *fd, char *output_override, int log_level_override);
TBX_API int tbx_mlog_printf(int suppress_header, int module_index, int level, const char *fn, const char *fname, int line, const char *fmt, ...) __attribute__((format (printf, 7, 8)));
TBX_API int tbx_stack_get_info_level(tbx_log_fd_t *fd);

// Preprocessor macros
#define _mlog_size 1024
#define INFO_HEADER_NONE   0
#define INFO_HEADER_THREAD 1
#define INFO_HEADER_FULL   2

#define tbx_log_level() _log_level
#define tbx_set_log_level(n) _log_level = n
#define tbx_set_log_maxsize(n) _log_maxsize = n
#define log_printf(n, ...) tbx_mlog_printf(0, _log_module_index, n, __func__, _mlog_file_table[_log_module_index], __LINE__, __VA_ARGS__)
#define info_printf(ifd, n, ...) tbx_minfo_printf(ifd, _log_module_index, n, __func__, _mlog_file_table[_log_module_index], __LINE__, __VA_ARGS__)
#define slog_printf(n, ...) tbx_mlog_printf(1, _log_module_index, n, __func__, _mlog_file_table[_log_module_index], __LINE__, __VA_ARGS__)

#ifndef _log_module_index
#define _log_module_index 0
#endif

// Globals
extern TBX_API char *_mlog_file_table[_mlog_size];
extern TBX_API int _log_level;
extern TBX_API long int _log_maxsize;

#ifdef __cplusplus
}
#endif

#endif
