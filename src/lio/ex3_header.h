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


#include <tbx/list.h>
#include "ex3_types.h"

#ifndef _EX3_HEADER_H_
#define _EX3_HEADER_H_

#ifdef __cplusplus
extern "C" {
#endif


typedef struct ex_header_t ex_header_t;
struct ex_header_t {
    char *name;
    ex_id_t id;
    char *type;
    tbx_list_t *attributes;  //should be a key/value pair struct?
};

ex_header_t *ex_header_create();
void ex_header_release(ex_header_t *h);
void ex_header_destroy(ex_header_t *h);
void ex_header_init(ex_header_t *h);
char *ex_header_get_name(ex_header_t *h);
void ex_header_set_name(ex_header_t *h, char *name);
ex_id_t ex_header_get_id(ex_header_t *h);
void ex_header_set_id(ex_header_t *h, ex_id_t id);
char  *ex_header_get_type(ex_header_t *h);
void ex_header_set_type(ex_header_t *h, char *type);
tbx_list_t *ex_header_get_attributes(ex_header_t *h);
void ex_header_set_attributes(ex_header_t *h, tbx_list_t *attr);

#ifdef __cplusplus
}
#endif

#endif

