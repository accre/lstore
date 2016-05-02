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

//*************************************************************
// liblio_trace.h - Header file for lio_trace library
//*************************************************************

#ifndef __LIBLIO_TRACE_H_
#define __LILIO_TRACE_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    int (*open)(const char *pathname, int flags, ...);
    int (*close)(int fd);
    ssize_t (*read)(int fd, void *buf, size_t count);
    ssize_t (*write)(int fd, const void *buf, size_t count);
    off_t (*lseek)(int fd, off_t offset, int whence);
} lt_fn_t;

typedef struct {
    char *trace_name;
    char *trace_header;
    int fd;
    int max_fd;
    int logfd;
} lt_config_t;

typedef struct {
    char *fname;
    int fd;
    long int init_size;
    long int max_size;
    long int block_size;
    long int pos;
} fd_trace_t;

extern lt_config_t ltc;

#define LIBLIO_TRACE_SECTION "liblio_trace"

#ifdef __cplusplus
}
#endif


#endif

