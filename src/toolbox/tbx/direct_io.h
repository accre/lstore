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
#ifndef ACCRE_DIRECT_IO_H_INCLUDED
#define ACCRE_DIRECT_IO_H_INCLUDED

#include <tbx/assert_result.h>
#include <tbx/visibility.h>
#include <apr_time.h>
#include <unistd.h>

TBX_API int tbx_dio_init(FILE *fd);
TBX_API void tbx_dio_finish(FILE *fd, int flags);
TBX_API ssize_t tbx_dio_read(FILE *fd, char *buf, ssize_t nbytes, ssize_t offset);
TBX_API ssize_t tbx_dio_write(FILE *fd, char *buf, ssize_t nbytes, ssize_t offset);

#endif
