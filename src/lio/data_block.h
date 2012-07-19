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
 char *key;
 char *value;
} data_block_attr_t;

typedef struct {
  ex_id_t  id;
  ex_off_t size;
  ex_off_t max_size;
  atomic_int_t ref_count;
  atomic_int_t initial_ref_count;
  data_block_cap_t *cap;
  data_service_fn_t *ds;
  char *rid_key;
  Stack_t *attr_stack;
} data_block_t;

#define data_block_id(db) (db)->id

//** Block related functions
int data_block_serialize(data_block_t *d, exnode_exchange_t *exp);
data_block_t *data_block_deserialize(ex_id_t id, exnode_exchange_t *exp);
data_block_t *data_block_create(data_service_fn_t *ds);
char *data_block_get_attr(data_block_t *d, char *key);
int data_block_set_attr(data_block_t *d, char *key, char *val);
void data_block_destroy(data_block_t *b);

#ifdef __cplusplus
}
#endif

#endif

