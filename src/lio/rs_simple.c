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
// Simple resource managment implementation
//***********************************************************************

#define _log_module_index 159

#include <assert.h>
#include "list.h"
#include "ex3_system.h"
#include "resource_service_abstract.h"
#include "rs_query_base.h"
#include "rs_simple.h"
#include "rs_simple_priv.h"
#include "iniparse.h"
#include "log.h"
#include "stack.h"
#include "type_malloc.h"
#include "random.h"

typedef struct {
  char *key;
  char *value;
}  kvq_ele_t;

typedef struct {
  int n_max;
  int n_used;
  int n_snap;
  kvq_ele_t *list;
} kvq_list_t;

typedef struct {
  int n_unique;
  int n_pickone;
  kvq_ele_t **unique;
  kvq_ele_t *pickone;
} kvq_table_t;


//***********************************************************************
// rss_test - Tests the current RID entry for query compatibility
//***********************************************************************

int rss_test(rsq_base_ele_t *q, rss_rid_entry_t *rse, int n_match, kvq_ele_t *uniq, kvq_ele_t *pickone)
{
  int found;
  int k_unique, k_pickone, k_op;
  int v_unique, v_pickone, v_op;
  int err, i, nlen;
  char *key, *val, *str_tomatch;
  list_iter_t it;
  list_compare_t cmp_fn;

  //** Factor the ops
  k_unique = q->key_op & RSQ_BASE_KV_UNIQUE;
  k_pickone = q->key_op & RSQ_BASE_KV_PICKONE;
  k_op = q->key_op & RSQ_BASE_KV_OP_BITMASK;
  v_unique = q->val_op & RSQ_BASE_KV_UNIQUE;
  v_pickone = q->val_op & RSQ_BASE_KV_PICKONE;
  v_op = q->val_op & RSQ_BASE_KV_OP_BITMASK;

//log_printf(15, "key=%s val=%s\n", q->key, q->val);

  str_tomatch = (q->key != NULL) ? q->key : "";
  nlen = strlen(str_tomatch);
  list_strncmp_set(&cmp_fn, nlen);
  it = list_iter_search_compare(rse->attr, str_tomatch, &cmp_fn, 0);
  found = 0;
  while ((found==0) && ((err=list_next(&it, (list_key_t **)&key, (list_data_t **)&val)) == 0)) {
     //** First check the key for a comparison
     str_tomatch = (q->key != NULL) ? q->key : "";
     switch (k_op) {
       case (RSQ_BASE_KV_EXACT):
            if (strcmp(key, str_tomatch) == 0) found = 1;
            break;
       case (RSQ_BASE_KV_PREFIX):
            if (strncmp(key, str_tomatch, nlen) == 0) found = 1;
            break;
       case (RSQ_BASE_KV_ANY):
            found = 1;
            break;
     }

     //** If got a match then compare the unique or pickone
     if (found == 1) {
        if (n_match > 0) {
           if (k_unique > 0) {
              for (i=0; i<n_match; i++) {
                  if (strcmp(key, uniq[i].key) == 0) {
                     found = 0;
                     break;
                  }
              }
           } else if (k_pickone > 0) {
              if (strcmp(key, pickone->key) != 0) {
                 found = 0;
              }
           }
        }

        //** If still got a match do the same for the value
        //** Compare the value based on the op
        str_tomatch = (q->val != NULL) ? q->val : "";
        switch (v_op) {
          case (RSQ_BASE_KV_EXACT):
               if (strcmp(val, str_tomatch) != 0) found = 0;
               break;
          case (RSQ_BASE_KV_PREFIX):
               if (strncmp(val, str_tomatch, nlen) != 0) found = 0;
               break;
          case (RSQ_BASE_KV_ANY):
               break;
        }


        //** If still a match then do the uniq/pickone check if needed on the value
        if (found == 1) {
           if (n_match > 0) {
              if (v_unique > 0) {
                 for (i=0; i<n_match; i++) {
                     if (strcmp(val, uniq[i].value) == 0) {
                        found = 0;
                        break;
                     }
                 }
              } else if (v_pickone > 0) {
                 if (strcmp(val, pickone->value) != 0) {
                    found = 0;
                 }
              }
           }
        }
     }
  }

  //** Got a match so store it if needed
  if (found == 1) {
     if (k_unique > 0) uniq[n_match].key = key;
     if (v_unique > 0) uniq[n_match].value = val;
     if (k_pickone > 0) pickone->key = key;
     if (v_pickone > 0) pickone->value = val;
  }

  return(found);
}

//***********************************************************************
// rs_simple_request - Processes a simple RS request
//***********************************************************************

op_generic_t *rs_simple_request(resource_service_fn_t *arg, data_attr_t *da, rs_query_t *rsq, data_cap_set_t **caps, rs_request_t *req, int req_size, rs_hints_t *hints_list, int fixed_size, int n_rid, int timeout)
{
  rs_simple_priv_t *rss = (rs_simple_priv_t *)arg->priv;
  rsq_base_t *query_global = (rsq_base_t *)rsq;
  rsq_base_t *query_local;
  kvq_table_t kvq_global, kvq_local, *kvq;
  op_status_t status;
  opque_t *que;
  rss_rid_entry_t *rse;
  rsq_base_ele_t *q;
  int slot, rnd_off, i, j, k, i_unique, i_pickone, found, err_cnt, loop, loop_end;
  int state, *a, *b, *op_state;
  Stack_t *stack;

  log_printf(15, "rs_simple_request: START n_rid=%d req_size=%d\n", n_rid, req_size);

  //** Determine the query sizes and make the processing arrays
  memset(&kvq, 0, sizeof(kvq));
  rs_query_count(arg, rsq, &i, &(kvq_global.n_unique), &(kvq_global.n_pickone));

  log_printf(15, "rs_simple_request: n_unique=%d n_pickone=%d\n", kvq_global.n_unique, kvq_global.n_pickone);

  //** Make space the for the uniq and pickone fields.
  //** Make sure we have space for at least 1 of each to pass to the routines even though they aren't used
  j = (kvq_global.n_pickone == 0) ? 1 : kvq_global.n_pickone;
  type_malloc_clear(kvq_global.pickone, kvq_ele_t, j);

  j = (kvq_global.n_unique == 0) ? 1 : kvq_global.n_unique;
  type_malloc_clear(kvq_global.unique, kvq_ele_t *, j);
  for (i=0; i<j; i++) {
      type_malloc_clear(kvq_global.unique[i], kvq_ele_t, n_rid);
  }

  //** We don't allow these on the local but make a temp space anyway
  kvq_local.n_pickone = 0;
  type_malloc_clear(kvq_local.pickone, kvq_ele_t, 1);
  kvq_global.n_unique = 0;
  type_malloc_clear(kvq_local.unique, kvq_ele_t *, 1);
  type_malloc_clear(kvq_local.unique[0], kvq_ele_t, n_rid);

  status = op_success_status;

  que = new_opque();
  stack = new_stack();

  err_cnt = 0;
  found = 0;
//  max_size = (req_size > fixed_size) ? req_size : fixed_size;

  for (i=0; i < n_rid; i++) {
     found = 0;
     loop_end = 1;
     query_local = NULL;
     rnd_off = random_int(0, rss->n_rids-1);
//rnd_off = 0;  //FIXME

     if (hints_list != NULL) {
        query_local = (rsq_base_t *)hints_list[i].local_rsq;
        if (query_local != NULL) {
           loop_end = 2;
           rs_query_count(arg, query_local, &j, &(kvq_local.n_unique), &(kvq_local.n_pickone));
           if ((kvq_local.n_unique != 0) && (kvq_local.n_pickone != 0)) {
              log_printf(1, "Unsupported use of pickone/unique in local RSQ hints_list[%d]=%s!\n", i, hints_list[i]);
              status.op_status = OP_STATE_FAILURE;
              status.error_code = RS_ERROR_FIXED_NOT_FOUND;
              hints_list[i].status = RS_ERROR_HINTS_INVALID_LOCAL;
              err_cnt++;
              break;
           }
        }

        if (i<fixed_size) {  //** Use the fixed list for assignment
           rse = list_search(rss->rid_table, hints_list[i].fixed_rid_key);
           if (rse == NULL) {
              log_printf(1, "Missing element in hints list[%d]=%s!\n", i, hints_list[i]);
              status.op_status = OP_STATE_FAILURE;
              status.error_code = RS_ERROR_FIXED_NOT_FOUND;
              hints_list[i].status = RS_ERROR_FIXED_NOT_FOUND;
              err_cnt++;
              break;
           }
           rnd_off = rse->slot;
        }
     }

     for (j=0; j<rss->n_rids; j++) {
        slot = (rnd_off+j) % rss->n_rids;
        rse = rss->random_array[slot];

        if ((rse->status != RS_STATUS_ON) && (i>=fixed_size)) continue;  //** Skip this if disabled and not in the fixed list

        empty_stack(stack, 1);
        q = query_global->head;
        kvq = &kvq_global;
        for (loop=0; loop<loop_end; loop++) {
           i_unique = 0;  i_pickone = 0;
           while (q != NULL) {
              state = -1;
              switch (q->op) {
                case RSQ_BASE_OP_KV:
                   //log_printf(0, "KV: key=%s val=%s\n", q->key, q->val);
                   state = rss_test(q, rse, i, kvq->unique[i_unique], &(kvq->pickone[i_pickone]));
                   if ((q->key_op & RSQ_BASE_KV_UNIQUE) || (q->val_op & RSQ_BASE_KV_UNIQUE)) i_unique++;
                   if ((q->key_op & RSQ_BASE_KV_PICKONE) || (q->val_op & RSQ_BASE_KV_PICKONE)) i_pickone++;
                   break;
                case RSQ_BASE_OP_NOT:
                   a = (int *)pop(stack);
                   state = (*a == 0) ? 1 : 0;
                   //log_printf(0, "NOT(%d)=%d\n", *a, state);
                   free(a);
                   break;
                case RSQ_BASE_OP_AND:
                   a = (int *)pop(stack);
                   b = (int *)pop(stack);
                   state = (*a) && (*b);
                   //log_printf(0, "%d AND %d = %d\n", *a, *b, state);
                   free(a); free(b);
                   break;
                case RSQ_BASE_OP_OR:
                   a = (int *)pop(stack);
                   b = (int *)pop(stack);
                   state = a || b;
                   //log_printf(0, "%d OR %d = %d\n", *a, *b, state);
                   free(a); free(b);
                   break;
              }

              type_malloc(op_state, int, 1);
              *op_state = state;
              push(stack, (void *)op_state);
              //log_printf(15, " stack_size=%d loop=%d push state=%d\n",stack_size(stack), loop, state);
              q = q->next;
           }

           if (query_local != NULL) {
              q = query_local->head;
              kvq = &kvq_local;
           }
        }

        op_state = (int *)pop(stack);
        state = -1;
        if (op_state != NULL) {
           state = *op_state;
           free(op_state);
        }

        if (op_state == NULL) {
           log_printf(1, "rs_simple_request: ERROR processing i=%d EMPTY STACK\n", i);
           found = 0;
           status.op_status = OP_STATE_FAILURE;
           status.error_code = RS_ERROR_EMPTY_STACK;
        } else if  (state == 1) { //** Got one
           log_printf(15, "rs_simple_request: processing i=%d ds_key=%s\n", i, rse->ds_key);
           found = 1;
           if (i<fixed_size) hints_list[i].status = RS_ERROR_OK;

           for (k=0; k<req_size; k++) {
              if (req[k].rid_index == i) {
                 log_printf(15, "rs_simple_request: i=%d ds_key=%s, rid_key=%s size=" XOT "\n", i, rse->ds_key, rse->rid_key, req[k].size);
                 req[k].rid_key = rse->rid_key;
                 req[k].gop = ds_allocate(rss->ds, rse->ds_key, da, req[k].size, caps[k], timeout);
                 opque_add(que, req[k].gop);
              }
           }

           break;  //** Got one so exit the RID scan and start the next one
        } else if (i<fixed_size) {  //** This should have worked so flag an error
           log_printf(1, "Match fail in fixed list[%d]=%s!\n", i, hints_list[i]);
           status.op_status = OP_STATE_FAILURE;
           status.error_code = RS_ERROR_FIXED_MATCH_FAIL;
           hints_list[i].status = RS_ERROR_FIXED_NOT_FOUND;
           err_cnt++;
           break;  //** Skip to the next in the list
        } else {
           found = 0;
        }
     }

     if ((found == 0) && (i>=fixed_size)) break;

  }


  //** Clean up
  j = (kvq_global.n_unique == 0) ? 1 : kvq_global.n_unique;
  for (i=0; i<j; i++) {
      free(kvq_global.unique[i]);
  }
  free(kvq_global.unique);
  free(kvq_global.pickone);

  free(kvq_local.unique[0]);
  free(kvq_local.unique);
  free(kvq_local.pickone);

  free_stack(stack, 1);

  log_printf(15, "rs_simple_request: END n_rid=%d\n", n_rid);

//callback_t *cb = (callback_t *)que->qd.list->top->data;
//op_generic_t *gop = (op_generic_t *)cb->priv;
//log_printf(15, "top gid=%d reg=%d\n", gop_id(gop), gop_id(req[0].gop));

  if ((found == 0) || (err_cnt>0)) {
     opque_free(que, OP_DESTROY);

     if (status.error_code == 0) {
        log_printf(1, "rs_simple_request: Can't find enough RIDs! requested=%d found=%d err_cnt=%d\n", n_rid, found, err_cnt);
        status.op_status = OP_STATE_FAILURE;
        status.error_code = RS_ERROR_NOT_ENOUGH_RIDS;
     }

     return(gop_dummy(status));
  }

  return(opque_get_gop(que));
}

//***********************************************************************
// rs_simple_get_rid_value - Returns the value associated with ther RID key
//    provided
//***********************************************************************

char *rs_simple_get_rid_value(resource_service_fn_t *arg, char *rid_key, char *key)
{
  rs_simple_priv_t *rss = (rs_simple_priv_t *)arg->priv;
  rss_rid_entry_t *rse;
  char *value = NULL;


  rse = list_search(rss->rid_table, rid_key);
  if (rse != NULL) {
     value = list_search(rse->attr, key);
  }

  return(value);
}


//***********************************************************************
// rse_free - Frees the memory associated with the RID entry
//***********************************************************************

void rs_simple_rid_free(list_data_t *arg)
{
  rss_rid_entry_t *rse = (rss_rid_entry_t *)arg;

log_printf(15, "START\n"); flush_log();

  if (rse == NULL) return;

log_printf(15, "removing ds_key=%s attr=%p\n", rse->ds_key, rse->attr);

  list_destroy(rse->attr);

  if (rse->rid_key != NULL) free(rse->rid_key);
  if (rse->ds_key != NULL) free(rse->ds_key);

  free(rse);
}

//***********************************************************************
//  rs_load_entry - Loads an RID entry fro mthe file
//***********************************************************************

rss_rid_entry_t *rss_load_entry(inip_group_t *grp)
{
  rss_rid_entry_t *rse;
  inip_element_t *ele;
  char *key, *value;

  //** Create the new RS list
  type_malloc_clear(rse, rss_rid_entry_t, 1);
  rse->status = RS_STATUS_ON;
  assert(rse != NULL);
  rse->rid_key = NULL;
  rse->ds_key = NULL;
  rse->attr = list_create(1, &list_string_compare, list_string_dup, list_simple_free, list_simple_free);

  //** Now cycle through the attributes
  ele = inip_first_element(grp);
  while (ele != NULL) {
     key = inip_get_element_key(ele);
     value = inip_get_element_value(ele);
     if (strcmp(key, "rid_key") == 0) {  //** This is the RID so store it separate
        rse->rid_key = strdup(value);
        list_insert(rse->attr, key, strdup(value));
     } else if (strcmp(key, "ds_key") == 0) {  //** This is what gets passed to the data service
        rse->ds_key = strdup(value);
     } else if ((key != NULL) && (value != NULL)) {
        list_insert(rse->attr, key, strdup(value));
     }

     log_printf(15, "rss_load_entry: key=%s value=%s\n", key, value);

     ele = inip_next_element(ele);
  }

//log_printf(0, "rse->ds_key=%s rse->attr=%p\n", rse->ds_key, rse->attr);

  //** Make sure we have an RID and DS link
  if ((rse->rid_key == NULL) || (rse->ds_key == NULL)) {
     log_printf(1, "rss_load_entry: missing RID or ds_key! rid=%s ds_key=%s\n", rse->rid_key, rse->ds_key);
     rs_simple_rid_free(rse);
     rse = NULL;
  }

  return(rse);
}

//***********************************************************************
// rs_simple_destroy - Destroys the simple RS service
//***********************************************************************

void rs_simple_destroy(resource_service_fn_t *rs)
{
  rs_simple_priv_t *rss = (rs_simple_priv_t *)rs->priv;

log_printf(15, "rs_simple_destroy: sl=%p\n", rss->rid_table); flush_log();

  free(rss->random_array);
  list_destroy(rss->rid_table);

  free(rss->random_array);
  free(rss);
  free(rs);
}


//***********************************************************************
// rs_simple_create - Creates a simple resource management service from
//    the given file.
//***********************************************************************

resource_service_fn_t *rs_simple_create(void *arg, char *fname, char *section)
{
  exnode_abstract_set_t *ess = (exnode_abstract_set_t *)arg;
  inip_file_t *kf;
  inip_group_t *ig;
  char *key;
  rss_rid_entry_t *rse;
  rs_simple_priv_t *rss;
  resource_service_fn_t *rs;
  list_iter_t it;
  int i, n;

  //* Load the config file
  kf = inip_read(fname);
  if (kf == NULL) {
    log_printf(0, "rs_simple_create:  Error parsing config file! file=%s\n", fname);
    return(NULL);
  }

  //** Create the new RS list
  type_malloc_clear(rss, rs_simple_priv_t, 1);
//  rss->rid_table = list_create(0, &list_string_compare, list_string_dup, list_simple_free, rs_simple_rid_free);
  rss->rid_table = list_create(0, &list_string_compare, NULL, NULL, rs_simple_rid_free);
log_printf(15, "rs_simple_create: sl=%p ds=%p\n", rss->rid_table, ess->ds);

  rss->ds = ess->ds;

  //** And load it
  ig = inip_first_group(kf);
  while (ig != NULL) {
    key = inip_get_group(ig);
    if (strcmp("rid", key) == 0) {  //** Found a resource
       rse = rss_load_entry(ig);
       if (rse != NULL) {
          list_insert(rss->rid_table, rse->rid_key, rse);
       }
    }
    ig = inip_next_group(ig);
  }

  //** Make the randomly permuted table
  rss->n_rids = list_key_count(rss->rid_table);
  type_malloc_clear(rss->random_array, rss_rid_entry_t *, rss->n_rids);
  it = list_iter_search(rss->rid_table, (list_key_t *)NULL, 0);
  for (i=0; i < rss->n_rids; i++) {
     list_next(&it, (list_key_t **)&key, (list_data_t **)&rse);

     n = random_int(0, rss->n_rids-1);
//n = i;  //FIXME
     while (rss->random_array[n] != NULL) {
        n = (n+1) % rss->n_rids;
     }
     rse->slot = n;
     rss->random_array[n] = rse;
  }

  inip_destroy(kf);

  //** Lastly create the resource service
  type_malloc_clear(rs, resource_service_fn_t, 1);
  rs->priv = rss;
  rs->query_dup = rs_query_base_dup;
  rs->query_add = rs_query_base_add;
  rs->query_append = rs_query_base_append;
  rs->query_destroy = rs_query_base_destroy;
  rs->query_print = rs_query_base_print;
  rs->query_parse = rs_query_base_parse;
  rs->get_rid_value = rs_simple_get_rid_value;
  rs->data_request = rs_simple_request;
  rs->destroy_service = rs_simple_destroy;
  rs->type = RS_TYPE_SIMPLE;

  return(rs);
}



