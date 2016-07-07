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
// Basic Resource Query interface
//***********************************************************************

#ifndef _RS_QUERY_BASE_H_
#define _RS_QUERY_BASE_H_

#include "rs.h"

#ifdef __cplusplus
extern "C" {
#endif

    struct lio_rsq_base_ele_t;
struct lio_rsq_base_t;

#define RSQ_BASE_OP_MAX_VAL 4
#define RSQ_BASE_OP_OR      4

#define RSQ_BASE_KV_MAX_VAL 3
#define RSQ_BASE_KV_OP_BITMASK 7
#define RSQ_BASE_KV_PREFIX  2

//** Mutually exclusive options to be &'ed with the KV options above
#define RSQ_BASE_KV_UNIQUE   64
#define RSQ_BASE_KV_PICKONE 128

struct lio_rsq_base_ele_t {
    int op;          //** Query operation
    char *key;
    char *val;
    int key_op;
    int val_op;
    struct lio_rsq_base_ele_t *next;
};

struct lio_rsq_base_t {
    lio_resource_service_fn_t *rs;
    lio_rsq_base_ele_t *head;
    lio_rsq_base_ele_t *tail;
};

int rs_query_base_add(lio_resource_service_fn_t *rs, rs_query_t **q, int op, char *key, int key_op, char *val, int val_op);
void rs_query_base_append(lio_resource_service_fn_t *rs, rs_query_t *rsq, rs_query_t *rsq_append);
void rs_query_base_destroy(lio_resource_service_fn_t *rs, rs_query_t *q);
char *rs_query_base_print(lio_resource_service_fn_t *rs, rs_query_t *q);
rs_query_t *rs_query_base_new(lio_resource_service_fn_t *rs);
rs_query_t *rs_query_base_dup(lio_resource_service_fn_t *rs, rs_query_t *query);
rs_query_t *rs_query_base_parse(lio_resource_service_fn_t *rs, char *value);
void rs_query_base_destroy(lio_resource_service_fn_t *rs, rs_query_t *q);
void rs_query_count(lio_resource_service_fn_t *rs, rs_query_t *q, int *n_ele, int *n_unique, int *n_pickone);

#ifdef __cplusplus
}
#endif

#endif
