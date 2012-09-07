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

#define _log_module_index 158

#include <assert.h>
#include <stdlib.h>
#include "resource_service_abstract.h"
#include "rs_query_base.h"
#include "string_token.h"
#include "log.h"
#include "type_malloc.h"
#include "append_printf.h"
#include "service_manager.h"

//***********************************************************************
//  rs_query_count - Returns info about the number of query elements
//***********************************************************************

void rs_query_count(resource_service_fn_t *rs, rs_query_t *rsq, int *n_ele, int *n_unique, int *n_pickone)
{
  rsq_base_t *query = (rsq_base_t *)rsq;
  rsq_base_ele_t *q = query->head;

  *n_ele = 0;
  *n_unique = 0;
  *n_pickone = 0;

  while (q != NULL) {
    (*n_ele)++;

    if ((q->key_op & RSQ_BASE_KV_UNIQUE) || (q->val_op & RSQ_BASE_KV_UNIQUE)) (*n_unique)++;
    if ((q->key_op & RSQ_BASE_KV_PICKONE) || (q->val_op & RSQ_BASE_KV_PICKONE)) (*n_pickone)++;

    q = q->next;
  }

  return;
}

//***********************************************************************
// rs_query_base_destroy - Destroys a query list
//   NOTE:  priv is NOT used!
//***********************************************************************

void rs_query_base_destroy(resource_service_fn_t *rs, rs_query_t *rsq)
{
  rsq_base_t *query = (rsq_base_t *)rsq;
  rsq_base_ele_t *q, *prev;

  q = query->head;
  while (q != NULL) {
     prev = q;
     if (q->key != NULL) free(q->key);
     if (q->val != NULL) free(q->val);
     q = q->next;
     free(prev);
  }

  free(query);
}



//***********************************************************************
// rs_query_base_add - Adds a query to the list
//   NOTE:  priv is NOT used!
//***********************************************************************

int rs_query_base_add(resource_service_fn_t *rs, rs_query_t **rsq, int op, char *key, int key_op, char *val, int val_op)
{
  rsq_base_t **query = (rsq_base_t **)rsq;
  rsq_base_ele_t *q;

  //** Sanity check
  if (op > RSQ_BASE_OP_MAX_VAL) return(1);
  if (key_op > RSQ_BASE_KV_MAX_VAL) return(2);
  if (val_op > RSQ_BASE_KV_MAX_VAL) return(3);
  if ((key_op & RSQ_BASE_KV_UNIQUE) && (key_op & RSQ_BASE_KV_UNIQUE)) return(4);
  if ((val_op & RSQ_BASE_KV_UNIQUE) && (val_op & RSQ_BASE_KV_UNIQUE)) return(5);

  //** Ok make the query
  type_malloc_clear(q, rsq_base_ele_t, 1);
  q->op = op;
  q->key_op = key_op; q->key = (key == NULL) ? NULL : strdup(key);
  q->val_op = key_op; q->val = (val == NULL) ? NULL : strdup(val);
  q->next = NULL;

  //** and append it;
  if (*query == NULL) {
     type_malloc_clear(*query, rsq_base_t, 1);
     (*query)->rs = rs;

     (*query)->head = q;
     (*query)->tail = q;
  } else {
     (*query)->tail->next = q;
     (*query)->tail = q;
  }

  return(0);
}

//***********************************************************************
//  rs_query_base_dup - Duplicates a query structure
//***********************************************************************

rs_query_t *rs_query_base_dup(resource_service_fn_t *rs, rs_query_t *rsq)
{
  rsq_base_t *new_query;
  rsq_base_t *query = (rsq_base_t *)rsq;
  rsq_base_ele_t *q = query->head;
  rsq_base_ele_t *qn = NULL;
  rsq_base_ele_t *prev = NULL;

log_printf(15, "rs_query_phase_dup: START\n");

  if (query == NULL) return(NULL);

  type_malloc_clear(new_query, rsq_base_t, 1);
  for (q = query->head; q != NULL; q = q->next) {
log_printf(15, "rs_query_phase_dup: Adding element\n");
     type_malloc_clear(qn, rsq_base_ele_t, 1);
     if (new_query->head == NULL) new_query->head = qn;
     if (prev != NULL) prev->next = qn;

     qn->op = q->op;
     qn->key = (q->key == NULL) ? NULL : strdup(q->key);
     qn->key_op = q->key_op;
     qn->val = (q->val == NULL) ? NULL : strdup(q->val);
     qn->val_op = q->val_op;
     qn->next = NULL;
     prev = qn;
  }

  new_query->tail = qn;

log_printf(15, "rs_query_phase_dup: END\n");

  return((rs_query_t *)new_query);
}

//***********************************************************************
//  rs_query_append - Appends a query structure
//***********************************************************************

void rs_query_base_append(resource_service_fn_t *rs, rs_query_t *rsq, rs_query_t *rsq_append)
{
  rsq_base_t *query = (rsq_base_t *)rsq;
  rsq_base_t *query_append = (rsq_base_t *)rsq_append;
  rsq_base_ele_t *q;
  rsq_base_ele_t *qn = NULL;
  rsq_base_ele_t *prev = query->tail;

log_printf(15, "START\n");

  if (query_append == NULL) return;

  for (q = query_append->head; q != NULL; q = q->next) {
log_printf(15, "Adding element\n");
     type_malloc_clear(qn, rsq_base_ele_t, 1);
     if (query->head == NULL) query->head = qn;
     if (prev != NULL) prev->next = qn;

     qn->op = q->op;
     qn->key = (q->key == NULL) ? NULL : strdup(q->key);
     qn->key_op = q->key_op;
     qn->val = (q->val == NULL) ? NULL : strdup(q->val);
     qn->val_op = q->val_op;
     qn->next = NULL;
     prev = qn;
  }

  query->tail = qn;

log_printf(15, "END\n");

  return;
}

//***********************************************************************
//  rs_query_base_print -Converts the query to a string
//***********************************************************************

char *rs_query_base_print(resource_service_fn_t *rs, rs_query_t *rsq)
{
  int bufsize = 10*1024;
  char buffer[bufsize], *ekey, *eval;
  char *key, *val;
  rsq_base_t *query = (rsq_base_t *)rsq;
  rsq_base_ele_t *q;
  int used;

log_printf(15, "rs_query_base_print: START\n");

  if (query == NULL) return(NULL);

  used = 0;
  append_printf(buffer, &used, bufsize, "%s:", rs->type);

  q = query->head;
  while (q != NULL) {
    key = (q->key == NULL) ? "" : q->key;
    val = (q->val == NULL) ? "" : q->val;
    ekey = escape_text(":", '\\', key);
    eval = escape_text(":", '\\', val);
    append_printf(buffer, &used, bufsize, "%d:%s:%d:%s:%d", q->op, ekey, q->key_op, eval, q->val_op);

log_printf(15, "rs_query_base_print: Adding element\n");

    free(ekey); free(eval);

    q = q->next;
    if (q != NULL) append_printf(buffer, &used, bufsize, ";");
  }

log_printf(15, "rs_query_base_print: END rsq=%s\n", buffer);

  return(strdup(buffer));
}

//***********************************************************************
//  rs_query_base_parse - Parses the query string to a query struct
//  NOTE : rs is ignored
//***********************************************************************

rs_query_t *rs_query_base_parse(resource_service_fn_t *rs, char *qstring)
{
  char *buffer, *token, *t2, *ekey, *bstate, *tstate;
  int fin, bfin;
  rsq_base_ele_t *root, *tail, *q;
  rsq_base_t *query = NULL;

  buffer = strdup(qstring);

log_printf(15, "rs_query_base_parse: Parsing=%s!\n", buffer);

  token = escape_string_token(buffer, ";", '\\', 0, &bstate, &bfin);
log_printf(15, "rs_query_base_parse: initial token=%s!\n", token);
  t2 = escape_string_token(token, ":", '\\', 0, &tstate, &fin);
log_printf(15, "rs_query_base_parse: rs_type=%s\n", t2);
  ekey = unescape_text('\\', t2);
log_printf(15, "rs_query_base_parse: ekey=%s\n", ekey);
//  rs = lookup_resource_service(ekey);
  if (strcmp(rs->type, ekey) != 0) {
     log_printf(0, "rs_query_base_parse: Mismatch RS types  parent=%s got=%s!\n", rs->type, ekey);
     free(buffer);
     return(NULL);
  }
  free(ekey);

  type_malloc(query, rsq_base_t, 1);
  query->rs = rs;

log_printf(15, "rs_query_base_parse: bfin=%d!\n", bfin);

  root = NULL; tail = NULL;
  do {
    type_malloc_clear(q, rsq_base_ele_t, 1);
    if (root == NULL) root = q;

    t2 = escape_string_token(NULL, ":", '\\', 0, &tstate, &fin);

    q->op = atoi(t2);
    t2 = escape_string_token(NULL, ":", '\\', 0, &tstate, &fin);
    q->key = unescape_text('\\', t2);
    t2 = escape_string_token(NULL, ":", '\\', 0, &tstate, &fin);
    q->key_op = atoi(t2);
    t2 = escape_string_token(NULL, ":", '\\', 0, &tstate, &fin);
    q->val = unescape_text('\\', t2);
    t2 = escape_string_token(NULL, ":", '\\', 0, &tstate, &fin);
    q->val_op = atoi(t2);

    log_printf(15, "rs_query_base_parse: element=OP(%d):KEY(%s):KEY_OP(%d):VAL(%s):VAL_OP(%d)\n", q->op, q->key, q->key_op, q->val, q->val_op);

    if (tail != NULL) tail->next = q;
    tail = q;

    token = escape_string_token(NULL, ";", '\\', 0, &bstate, &bfin);  tstate = token;
  } while (strlen(token) > 0);

  free(buffer);

  query->head = root;
  query->tail = tail;

  return((rs_query_t *)query);
}


