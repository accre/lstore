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
#ifndef ACCRE_INIPARSE_H_INCLUDED
#define ACCRE_INIPARSE_H_INCLUDED

#include <inttypes.h>
#include "tbx/toolbox_visibility.h"

#ifdef __cplusplus
extern "C" {
#endif

// Types
typedef struct tbx_inip_element_t tbx_inip_element_t;

typedef struct tbx_inip_file_t tbx_inip_file_t;

typedef struct tbx_inip_group_t tbx_inip_group_t;

// Functions
TBX_API void tbx_inip_destroy(tbx_inip_file_t *inip);

TBX_API tbx_inip_group_t *tbx_inip_group_find(tbx_inip_file_t *inip, const char *name);

TBX_API char *tbx_inip_key_find(tbx_inip_group_t *group, const char *name);

TBX_API tbx_inip_element_t *tbx_inip_ele_first(tbx_inip_group_t *group);

TBX_API tbx_inip_group_t *tbx_inip_group_first(tbx_inip_file_t *inip);

TBX_API char *tbx_inip_group_get(tbx_inip_group_t *g);

TBX_API double tbx_inip_double_get(tbx_inip_file_t *inip, const char *group, const char *key, double def);

TBX_API char *tbx_inip_ele_key_get(tbx_inip_element_t *ele);

TBX_API char *tbx_inip_ele_value_get(tbx_inip_element_t *ele);

TBX_API int64_t tbx_inip_integer_get(tbx_inip_file_t *inip, const char *group, const char *key, int64_t def);

TBX_API char *tbx_inip_string_get(tbx_inip_file_t *inip, const char *group, const char *key, char *def);

TBX_API void tbx_inip_group_free(tbx_inip_group_t *g);

TBX_API void tbx_inip_group_set(tbx_inip_group_t *ig, char *value);

TBX_API int tbx_inip_n_groups(tbx_inip_file_t *inip);

TBX_API tbx_inip_element_t *tbx_inip_ele_next(tbx_inip_element_t *ele);

TBX_API tbx_inip_group_t *tbx_inip_group_next(tbx_inip_group_t *g);

TBX_API tbx_inip_file_t *tbx_inip_file_read(const char *fname);

TBX_API tbx_inip_file_t *tbx_inip_string_read(const char *text);

#ifdef __cplusplus
}
#endif

#endif
