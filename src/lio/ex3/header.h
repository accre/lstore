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
// Linear exnode3 support
//***********************************************************************

#ifndef _EX3_HEADER_H_
#define _EX3_HEADER_H_

#include <lio/ex3.h>
#include <tbx/list.h>

#include "ex3/types.h"

#ifdef __cplusplus
extern "C" {
#endif

lio_ex_header_t *ex_header_create();
void ex_header_release(lio_ex_header_t *h);
void ex_header_destroy(lio_ex_header_t *h);
void ex_header_init(lio_ex_header_t *h);
char *ex_header_get_name(lio_ex_header_t *h);
void ex_header_set_name(lio_ex_header_t *h, char *name);
ex_id_t ex_header_get_id(lio_ex_header_t *h);
void ex_header_set_id(lio_ex_header_t *h, ex_id_t id);
char  *ex_header_get_type(lio_ex_header_t *h);
void ex_header_set_type(lio_ex_header_t *h, char *type);
tbx_list_t *ex_header_get_attributes(lio_ex_header_t *h);
void ex_header_set_attributes(lio_ex_header_t *h, tbx_list_t *attr);

#ifdef __cplusplus
}
#endif

#endif
