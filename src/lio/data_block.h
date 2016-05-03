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
// Data block definition
//***********************************************************************

#include "ex3_types.h"
#include "data_service_abstract.h"
#include "atomic_counter.h"
#include "stack.h"

#ifndef _DATA_BLOCK_H_
#define _DATA_BLOCK_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef void data_block_cap_t;

typedef struct {
    data_block_cap_t *cap;
    data_service_fn_t *ds;
} data_block_warm_t;

typedef struct {
    char *key;
    char *value;
} data_block_attr_t;

typedef struct {
    ex_id_t  id;
    ex_off_t size;
    ex_off_t max_size;
    tbx_atomic_unit32_t ref_count;
    tbx_atomic_unit32_t initial_ref_count;
    data_block_cap_t *cap;
    data_service_fn_t *ds;
    char *rid_key;
    tbx_stack_t *attr_stack;
    data_block_warm_t *warm;
} data_block_t;

#define data_block_id(db) (db)->id

//** Block related functions
int data_block_serialize(data_block_t *d, exnode_exchange_t *exp);
data_block_t *data_block_deserialize(service_manager_t *dsm, ex_id_t id, exnode_exchange_t *exp);
data_block_t *data_block_create(data_service_fn_t *ds);
char *data_block_get_attr(data_block_t *d, char *key);
int data_block_set_attr(data_block_t *d, char *key, char *val);
void data_block_destroy(data_block_t *b);
void data_block_auto_warm(data_block_t *b);
void data_block_stop_warm(data_block_t *b);

#ifdef __cplusplus
}
#endif

#endif

