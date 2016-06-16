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

#ifndef __INIPARSE_H
#define __INIPARSE_H

#include <stdio.h>
#include <stdint.h>

#include "tbx/iniparse.h"

#ifdef __cplusplus
extern "C" {
#endif

tbx_inip_file_t *inip_read_fd(FILE *fd);
uint64_t inip_get_unsigned_integer(tbx_inip_file_t *inip, const char *group, const char *key, uint64_t def);

#ifdef __cplusplus
}
#endif

#endif

