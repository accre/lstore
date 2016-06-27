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
// Linear segment support
//***********************************************************************

#ifndef _SEGMENT_FILE_H_
#define _SEGMENT_FILE_H_

#include <gop/opque.h>
#include <gop/types.h>

#include "ds.h"
#include "ex3.h"
#include "ex3_types.h"

#ifdef __cplusplus
extern "C" {
#endif

#define SEGMENT_TYPE_FILE "file"

segment_t *segment_file_load(void *arg, ex_id_t id, exnode_exchange_t *ex);
segment_t *segment_file_create(void *arg);
op_generic_t *segment_file_make(segment_t *seg, data_attr_t *da, char *fname);

#ifdef __cplusplus
}
#endif

#endif
