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
// OS file header file
//***********************************************************************

#include "object_service_abstract.h"
#include <tbx/fmttypes.h>

#ifndef _OS_FILE_H_
#define _OS_FILE_H_

#ifdef __cplusplus
extern "C" {
#endif

#define OS_TYPE_FILE "file"

typedef struct local_object_iter_t local_object_iter_t;
struct local_object_iter_t {
    object_service_fn_t *os;
    os_object_iter_t  *oit;
};

int local_next_object(local_object_iter_t *it, char **myfname, int *prefix_len);
local_object_iter_t *create_local_object_iter(os_regex_table_t *path, os_regex_table_t *object_regex, int object_types, int recurse_depth);
void destroy_local_object_iter(local_object_iter_t *it);

object_service_fn_t *object_service_file_create(service_manager_t *ess, tbx_inip_file_t *ifd, char *section);
int osf_store_val(void *src, int src_size, void **dest, int *v_size);

#ifdef __cplusplus
}
#endif

#endif

