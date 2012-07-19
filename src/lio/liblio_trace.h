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

