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

//***********************************************************************
// Basic Resource Query interface
//***********************************************************************


#include "resource_service_abstract.h"

#ifndef _RS_QUERY_BASE_H_
#define _RS_QUERY_BASE_H_

#ifdef __cplusplus
extern "C" {
#endif

#define RSQ_BASE_OP_MAX_VAL 4
#define RSQ_BASE_OP_KV      1
#define RSQ_BASE_OP_NOT     2
#define RSQ_BASE_OP_AND     3
#define RSQ_BASE_OP_OR      4

#define RSQ_BASE_KV_MAX_VAL 3
#define RSQ_BASE_KV_OP_BITMASK 7
#define RSQ_BASE_KV_EXACT   1
#define RSQ_BASE_KV_PREFIX  2
#define RSQ_BASE_KV_ANY     3

  //** Mutually exclusive options to be &'ed with the KV options above
#define RSQ_BASE_KV_UNIQUE   64
#define RSQ_BASE_KV_PICKONE 128

struct rsq_base_ele_s {
  int op;          //** Query operation
  char *key;
  char *val;
  int key_op;
  int val_op;
  struct rsq_base_ele_s *next;
};

typedef struct rsq_base_ele_s rsq_base_ele_t;

typedef struct {
  resource_service_fn_t *rs;
  rsq_base_ele_t *head;
  rsq_base_ele_t *tail;
} rsq_base_t;

int rs_query_base_add(resource_service_fn_t *rs, rs_query_t **q, int op, char *key, int key_op, char *val, int val_op);
void rs_query_base_append(resource_service_fn_t *rs, rs_query_t *rsq, rs_query_t *rsq_append);
void rs_query_base_destroy(resource_service_fn_t *rs, rs_query_t *q);
char *rs_query_base_print(resource_service_fn_t *rs, rs_query_t *q);
rs_query_t *rs_query_base_new(resource_service_fn_t *rs);
rs_query_t *rs_query_base_dup(resource_service_fn_t *rs, rs_query_t *query);
rs_query_t *rs_query_base_parse(resource_service_fn_t *rs, char *value);
void rs_query_base_destroy(resource_service_fn_t *rs, rs_query_t *q);
void rs_query_count(resource_service_fn_t *rs, rs_query_t *q, int *n_ele, int *n_unique, int *n_pickone);

#ifdef __cplusplus
}
#endif

#endif

