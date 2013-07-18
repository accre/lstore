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
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
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
#include "append_printf.h"
#include "string_token.h"

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

int _rs_simple_refresh(resource_service_fn_t *rs);

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

log_printf(15, "key=%s val=%s n_attr=%d\n", q->key, q->val, list_key_count(rse->attr));

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

log_printf(15, "ckey=%s found=%d\n", key, found);
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
                     if (uniq[i].value != NULL) {  //** This could be NULL if a previous RID wasn't in the config table
                        if (strcmp(val, uniq[i].value) == 0) {
                           found = 0;
                           break;
                        }
                     }
                 }
              } else if (v_pickone > 0) {
                 if (pickone->value != NULL) {  //** Same as above for uniq check
                    if (strcmp(val, pickone->value) != 0) {
                       found = 0;
                    }
                 }
              }
           }
        }
     }
  }

log_printf(15, "last err=%d\n", err);

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
  int state, *a, *b, *op_state, unique_size;
  Stack_t *stack;

  log_printf(15, "rs_simple_request: START rss->n_rids=%d n_rid=%d req_size=%d fixed_size=%d\n", rss->n_rids, n_rid, req_size, fixed_size);

  for (i=0; i<req_size; i++) req[i].rid_key = NULL;  //** Clear the result in case of an error

  apr_thread_mutex_lock(rss->lock);
  i = _rs_simple_refresh(arg);  //** Check if we need to refresh the data
  if (i != 0) { apr_thread_mutex_unlock(rss->lock); return(gop_dummy(op_failure_status)); }

  //** Determine the query sizes and make the processing arrays
  memset(&kvq, 0, sizeof(kvq));
  rs_query_count(arg, rsq, &i, &(kvq_global.n_unique), &(kvq_global.n_pickone));

  log_printf(15, "rs_simple_request: n_unique=%d n_pickone=%d\n", kvq_global.n_unique, kvq_global.n_pickone);
flush_log();

  //** Make space the for the uniq and pickone fields.
  //** Make sure we have space for at least 1 more than we need of each to pass to the routines even though they aren't used
  j = (kvq_global.n_pickone == 0) ? 1 : kvq_global.n_pickone + 1;
  type_malloc_clear(kvq_global.pickone, kvq_ele_t, j);

  unique_size = kvq_global.n_unique + 1;
  type_malloc_clear(kvq_global.unique, kvq_ele_t *, unique_size);
log_printf(15, "MALLOC j=%d\n", unique_size);
  for (i=0; i<unique_size; i++) {
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
              log_printf(0, "Unsupported use of pickone/unique in local RSQ hints_list[%d]=%s!\n", i, hints_list[i]);
              status.op_status = OP_STATE_FAILURE;
              status.error_code = RS_ERROR_FIXED_NOT_FOUND;
              hints_list[i].status = RS_ERROR_HINTS_INVALID_LOCAL;
              err_cnt++;
              continue;
           }
        }

        if (i<fixed_size) {  //** Use the fixed list for assignment
           rse = list_search(rss->rid_table, hints_list[i].fixed_rid_key);
           if (rse == NULL) {
              log_printf(0, "Missing element in hints list[%d]=%s! Ignoring check.\n", i, hints_list[i]);
              hints_list[i].status = RS_ERROR_FIXED_NOT_FOUND;
              continue;   //** Skip the check
           }
           rnd_off = rse->slot;
        }
     }

     for (j=0; j<rss->n_rids; j++) {
        slot = (rnd_off+j) % rss->n_rids;
        rse = rss->random_array[slot];

log_printf(15, "i=%d j=%d slot=%d rse->rid_key=%s rse->status=%d\n", i, j, slot, rse->rid_key, rse->status);
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
                   state = rss_test(q, rse, i, kvq->unique[i_unique], &(kvq->pickone[i_pickone]));
                   log_printf(0, "KV: key=%s val=%s i_unique=%d i_pickone=%d loop=%d rss_test=%d rse->rid_key=%s\n", q->key, q->val, i_unique, i_pickone, loop, state, rse->rid_key); flush_log();
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
              log_printf(15, " stack_size=%d loop=%d push state=%d\n",stack_size(stack), loop, state); flush_log();
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
                 req[k].rid_key = strdup(rse->rid_key);
                 req[k].gop = ds_allocate(rss->ds, rse->ds_key, da, req[k].size, caps[k], timeout);
                 opque_add(que, req[k].gop);
              }
           }

           break;  //** Got one so exit the RID scan and start the next one
        } else if (i<fixed_size) {  //** This should have worked so flag an error
           log_printf(1, "Match fail in fixed list[%d]=%s!\n", i, hints_list[i].fixed_rid_key);
           status.op_status = OP_STATE_FAILURE;
           status.error_code = RS_ERROR_FIXED_MATCH_FAIL;
           hints_list[i].status = RS_ERROR_FIXED_MATCH_FAIL;
           err_cnt++;
           break;  //** Skip to the next in the list
        } else {
           found = 0;
        }
     }

     if ((found == 0) && (i>=fixed_size)) break;

  }


  //** Clean up
log_printf(15, "FREE j=%d\n", unique_size);
  for (i=0; i<unique_size; i++) {
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

  apr_thread_mutex_unlock(rss->lock);

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


  apr_thread_mutex_lock(rss->lock);
  rse = list_search(rss->rid_table, rid_key);
  if (rse != NULL) {
     value = list_search(rse->attr, key);
     if (value != NULL)  value = strdup(value);
  }
  apr_thread_mutex_unlock(rss->lock);

  return(strdup(value));
}


//***********************************************************************
// rse_free - Frees the memory associated with the RID entry
//***********************************************************************

void rs_simple_rid_free(list_data_t *arg)
{
  rss_rid_entry_t *rse = (rss_rid_entry_t *)arg;

log_printf(15, "START\n"); flush_log();

  if (rse == NULL) return;

log_printf(15, "removing rid_key=%s ds_key=%s attr=%p\n", rse->rid_key, rse->ds_key, rse->attr);

  list_destroy(rse->attr);

//QWERTY  if (rse->rid_key != NULL) free(rse->rid_key);
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

log_printf(0, "loading\n");
  //** Create the new RS list
  type_malloc_clear(rse, rss_rid_entry_t, 1);
  rse->status = RS_STATUS_ON;
  rse->attr = list_create(1, &list_string_compare, list_string_dup, list_simple_free, list_simple_free);

  //** Now cycle through the attributes
  ele = inip_first_element(grp);
  while (ele != NULL) {
     key = inip_get_element_key(ele);
     value = inip_get_element_value(ele);
     if (strcmp(key, "rid_key") == 0) {  //** This is the RID so store it separate
        rse->rid_key = strdup(value);
        list_insert(rse->attr, key, rse->rid_key);
//QWERTY        list_insert(rse->attr, key, strdup(value));
     } else if (strcmp(key, "ds_key") == 0) {  //** This is what gets passed to the data service
        rse->ds_key = strdup(value);
     } else if (strcmp(key, "status") == 0) {  //** Current status
        rse->status = atoi(value);
     } else if (strcmp(key, "space_free") == 0) {  //** Free space
        rse->space_free = string_get_integer(value);
     } else if (strcmp(key, "space_used") == 0) {  //** Used bytes
        rse->space_used = string_get_integer(value);
     } else if (strcmp(key, "space_total") == 0) {  //** Total bytes
        rse->space_total = string_get_integer(value);
     } else if ((key != NULL) && (value != NULL)) {  //** Anything else is an attribute
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
// rss_get_rid_config - Gets the rid configuration
//***********************************************************************

char *rss_get_rid_config(resource_service_fn_t *rs)
{
  rs_simple_priv_t *rss = (rs_simple_priv_t *)rs->priv;
  char *buffer, *key, *val;
  int bufsize = 5*1024;
  apr_hash_index_t *hi;
  rss_check_entry_t *ce;
  int used;
  apr_ssize_t klen;
  list_iter_t ait;

  buffer = NULL;

  apr_thread_mutex_lock(rss->lock);
  do {
     if (buffer != NULL) free(buffer);
     bufsize = 2 * bufsize;
     type_malloc_clear(buffer, char, bufsize);

     used = 0;
     for (hi = apr_hash_first(NULL, rss->rid_mapping); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, (const void **)&key, &klen, (void **)&ce);

        //** Print the Standard fields
        append_printf(buffer, &used, bufsize, "[rid]\n");
        append_printf(buffer, &used, bufsize, "rid_key=%s\n", ce->rid_key);
        append_printf(buffer, &used, bufsize, "ds_key=%s\n", ce->ds_key);
        append_printf(buffer, &used, bufsize, "status=%d\n", ce->re->status);
        append_printf(buffer, &used, bufsize, "space_used=" XOT "\n", ce->re->space_used);
        append_printf(buffer, &used, bufsize, "space_free=" XOT "\n", ce->re->space_free);
        append_printf(buffer, &used, bufsize, "space_total=" XOT "\n", ce->re->space_total);

        //** Now cycle through printing the attributes
        ait = list_iter_search(ce->re->attr, (list_key_t *)NULL, 0);
        while (list_next(&ait, (list_key_t **)&key, (list_data_t **)&val) == 0) {
           //if ((strcmp("rid_key", key) == 0) || (strcmp("ds_key", key) == 0)) append_printf(buffer, &used, bufsize, "%s=%s-BAD\n", key, val);
           if ((strcmp("rid_key", key) != 0) && (strcmp("ds_key", key) != 0)) append_printf(buffer, &used, bufsize, "%s=%s\n", key, val);
        }

        append_printf(buffer, &used, bufsize, "\n");
     }
  } while (used >= bufsize);
  apr_thread_mutex_unlock(rss->lock);

  log_printf(5, "config=%s\n", buffer);

  return(buffer);
}

//***********************************************************************
// _rss_clear_check_table - Clears the check table
//   NOTE:  Assumes rs is already loacked!
//***********************************************************************

void _rss_clear_check_table(data_service_fn_t *ds, apr_hash_t *table, apr_pool_t *mpool)
{
  apr_hash_index_t *hi;
  rss_check_entry_t *entry;
  const void *rid;
  apr_ssize_t klen;

  for (hi = apr_hash_first(NULL, table); hi != NULL; hi = apr_hash_next(hi)) {
     apr_hash_this(hi, &rid, &klen, (void **)&entry);
     apr_hash_set(table, rid, klen, NULL);

     ds_inquire_destroy(ds, entry->space);
     free(entry->ds_key);
     free(entry->rid_key);
     free(entry);
  }

  apr_hash_clear(table);
}

//***********************************************************************
// rss_mapping_register - Registration for mapping updates
//***********************************************************************

void rss_mapping_register(resource_service_fn_t *rs, rs_mapping_notify_t *map_version)
{
  rs_simple_priv_t *rss = (rs_simple_priv_t *)rs->priv;

  apr_thread_mutex_lock(rss->update_lock);
  apr_hash_set(rss->mapping_updates, map_version, sizeof(rs_mapping_notify_t *), map_version);
  apr_thread_mutex_unlock(rss->update_lock);
}

//***********************************************************************
// rss_mapping_unregister - DE-Register for mapping updates
//***********************************************************************

void rss_mapping_unregister(resource_service_fn_t *rs, rs_mapping_notify_t *map_version)
{
  rs_simple_priv_t *rss = (rs_simple_priv_t *)rs->priv;

  apr_thread_mutex_lock(rss->update_lock);
  apr_hash_set(rss->mapping_updates, map_version, sizeof(rs_mapping_notify_t *), NULL);
  apr_thread_mutex_unlock(rss->update_lock);
}

//***********************************************************************
// rss_mapping_noitfy - Notifies all registered entities
//***********************************************************************

void rss_mapping_notify(resource_service_fn_t *rs, int new_version, int status_change)
{
  rs_simple_priv_t *rss = (rs_simple_priv_t *)rs->priv;
  apr_hash_index_t *hi;
  rs_mapping_notify_t *rsn;
  apr_ssize_t klen;
  void *rid;

  if (status_change > 0) status_change = random_int(0, 100000);

  apr_thread_mutex_lock(rss->update_lock);
  for (hi = apr_hash_first(NULL, rss->mapping_updates); hi != NULL; hi = apr_hash_next(hi)) {
     apr_hash_this(hi, (const void **)&rid, &klen, (void **)&rsn);
     apr_thread_mutex_lock(rsn->lock);
     rsn->map_version = new_version;
     if (status_change > 0) rsn->status_version = status_change;
     apr_thread_mutex_unlock(rsn->lock);
  }
  apr_thread_mutex_unlock(rss->update_lock);
}

//***********************************************************************
// rss_translate_cap_set - Translates the cap set based o nthe latest RID mappings
//***********************************************************************

void rss_translate_cap_set(resource_service_fn_t *rs, char *rid_key, data_cap_set_t *cs)
{
  rs_simple_priv_t *rss = (rs_simple_priv_t *)rs->priv;
  rss_check_entry_t *rce;

  apr_thread_mutex_lock(rss->lock);
  rce = apr_hash_get(rss->rid_mapping, rid_key, APR_HASH_KEY_STRING);
  if (rce != NULL) ds_translate_cap_set(rss->ds, rid_key, rce->ds_key, cs);
  apr_thread_mutex_unlock(rss->lock);
}

//***********************************************************************
// rss_perform_check - Checks the RIDs and updates their status
//***********************************************************************

int rss_perform_check(resource_service_fn_t *rs)
{
  rs_simple_priv_t *rss = (rs_simple_priv_t *)rs->priv;
  apr_hash_index_t *hi;
  rss_check_entry_t *ce;
  int prev_status, status_change;
  char *rid;
  apr_ssize_t klen;
  op_status_t status;
  opque_t *q;
  op_generic_t *gop;

  log_printf(5, "START\n");

  //** Generate the task list
  q = new_opque();

  status_change = 0;
  apr_thread_mutex_lock(rss->lock);
  for (hi = apr_hash_first(NULL, rss->rid_mapping); hi != NULL; hi = apr_hash_next(hi)) {
     apr_hash_this(hi, (const void **)&rid, &klen, (void **)&ce);
     if (ce->re->status != RS_STATUS_IGNORE) {
        gop = ds_res_inquire(rss->ds, ce->ds_key, rss->da, ce->space, rss->check_timeout);
        gop_set_private(gop, ce);
        opque_add(q, gop);
     }
  }
  apr_thread_mutex_unlock(rss->lock);

  //** Wait for them all to complete
  opque_waitall(q);

  //** Process the results
  apr_thread_mutex_lock(rss->lock);
  if (rss->modify_time == rss->current_check) {  //** Only process the results if not updated
     for (gop = opque_get_next_finished(q); gop != NULL; gop = opque_get_next_finished(q)) {
         status = gop_get_status(gop);
         ce = gop_get_private(gop);
         prev_status = ce->re->status;
         if (status.op_status == OP_STATE_SUCCESS) {  //** Got a valid response
            ce->re->space_free = ds_res_inquire_get(rss->ds, DS_INQUIRE_FREE, ce->space);
            ce->re->space_used = ds_res_inquire_get(rss->ds, DS_INQUIRE_USED, ce->space);
            ce->re->space_total = ds_res_inquire_get(rss->ds, DS_INQUIRE_TOTAL, ce->space);
            if (ce->re->space_free <= rss->min_free) {
               ce->re->status = 1;
            } else {
               ce->re->status = 0;
            }
         } else {  //** No response so mark it as down
            ce->re->status = 1;
         }
         if (prev_status != ce->re->status) status_change = 1;

log_printf(15, "ds_key=%s prev_status=%d new_status=%d\n", ce->ds_key, prev_status, ce->re->status);
         gop_free(gop, OP_DESTROY);
     }

  }

  opque_free(q, OP_DESTROY);
  apr_thread_mutex_unlock(rss->lock);

  log_printf(5, "END status_change=%d\n", status_change);

  return(status_change);
}

//***********************************************************************
// _rss_make_check_table - Makes the RID->Resource mapping table
//   NOTE:  Assumes rs is already loacked!
//***********************************************************************

void _rss_make_check_table(resource_service_fn_t *rs)
{
  rs_simple_priv_t *rss = (rs_simple_priv_t *)rs->priv;
  rss_check_entry_t *ce, *ce2;
  rss_rid_entry_t *re;
  int i;

  //** Clear out the old one
  _rss_clear_check_table(rss->ds, rss->rid_mapping, rss->mpool);

  //** Now make the new one
  rss->unique_rids = 1;
  for (i=0; i<rss->n_rids; i++) {
     re = rss->random_array[i];
     type_malloc(ce, rss_check_entry_t, 1);
     ce->ds_key = strdup(re->ds_key);
     ce->rid_key = strdup(re->rid_key);
     ce->space = ds_inquire_create(rss->ds);
     ce->re = re;

     //** Check for dups.  If so we only keep the 1st entry and spew a log message
     ce2 = apr_hash_get(rss->rid_mapping, ce->rid_key, APR_HASH_KEY_STRING);
     if (ce2 == NULL) {  //** Unique so add it
        apr_hash_set(rss->rid_mapping, ce->rid_key, APR_HASH_KEY_STRING, ce);
     } else {  //** Dup so disable dynamic mapping by unsetting unique_rids
        log_printf(0, "WARNING duplicate RID found.  Dropping dynamic mapping.  res=%s ---  new res=%s\n", ce2->ds_key, ce->ds_key);
        rss->unique_rids = 0;
        ds_inquire_destroy(rss->ds, ce->space);
        free(ce->rid_key);
        free(ce->ds_key);
        free(ce);
     }
  }

  return;
}

//***********************************************************************
//  rss_check_thread - checks for availabilty on all the RIDS
//***********************************************************************

void *rss_check_thread(apr_thread_t *th, void *data)
{
  resource_service_fn_t *rs = (resource_service_fn_t *)data;
  rs_simple_priv_t *rss = (rs_simple_priv_t *)rs->priv;
  int do_notify, map_version, status_change;
  apr_interval_time_t dt;

  dt = apr_time_from_sec(rss->check_interval);

  apr_thread_mutex_lock(rss->lock);
  rss->current_check = 0;  //** Triggers a reload
  do {
    log_printf(5, "LOOP START\n");
    _rs_simple_refresh(rs);  //** Do a quick check and see if the file has changed

    do_notify = 0;
    if (rss->current_check != rss->modify_time) { //** Need to reload
       rss->current_check = rss->modify_time;
       do_notify = 1;
      // _rss_make_check_table(rs);
    }
    map_version = rss->modify_time;
    apr_thread_mutex_unlock(rss->lock);

    status_change = (rss->check_timeout <= 0) ? 0 : rss_perform_check(rs);

    if (((do_notify == 1) && (rss->dynamic_mapping == 1)) || (status_change != 0))  rss_mapping_notify(rs, map_version, status_change);

    log_printf(5, "LOOP END\n");

    apr_thread_mutex_lock(rss->lock);
    if (rss->shutdown == 0) apr_thread_cond_timedwait(rss->cond, rss->lock, dt);
  } while (rss->shutdown == 0);

  //** Clean up
  _rss_clear_check_table(rss->ds, rss->rid_mapping, rss->mpool);
  apr_thread_mutex_unlock(rss->lock);

  return(NULL);
}


//***********************************************************************
// _rs_simple_load - Loads the config file
//   NOTE:  No locking is performed!
//***********************************************************************

int _rs_simple_load(resource_service_fn_t *res, char *fname)
{
  inip_group_t *ig;
  char *key;
  rss_rid_entry_t *rse;
  rs_simple_priv_t *rss = (rs_simple_priv_t *)res->priv;
  list_iter_t it;
  int i, n;
  inip_file_t *kf;

  log_printf(5, "START fname=%s n_rids=%d\n", fname, rss->n_rids);

  //** Open the file
  assert(kf = inip_read(fname));

  //** Create the new RS list
  rss->rid_table = list_create(0, &list_string_compare, NULL, NULL, rs_simple_rid_free);
log_printf(15, "rs_simple_load: sl=%p\n", rss->rid_table);

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

  log_printf(5, "END n_rids=%d\n", rss->n_rids);

  return(0);
}

//***********************************************************************
// _rs_simple_refresh - Refreshes the RID table if needed
//   NOTE: No Locking is performed
//***********************************************************************

int _rs_simple_refresh(resource_service_fn_t *rs)
{
  rs_simple_priv_t *rss = (rs_simple_priv_t *)rs->priv;
  struct stat sbuf;
  int err;

//log_printf(0, "SKIPPING refresh\n");
//return(0);

  if (stat(rss->fname, &sbuf) != 0) {
     log_printf(1, "RS file missing!!! Using old definition. fname=%s\n", rss->fname);
     return(0);
  }

  if (rss->modify_time != sbuf.st_mtime) {  //** File changed so reload it
     log_printf(5, "RELOADING data\n");
     rss->modify_time = sbuf.st_mtime;
     if (rss->rid_table != NULL) list_destroy(rss->rid_table);
     if (rss->random_array != NULL) free(rss->random_array);
     err = _rs_simple_load(rs, rss->fname);  //** Load the new file
     _rss_make_check_table(rs);  //** and make the new inquiry table
     apr_thread_cond_signal(rss->cond);  //** Notify the check thread that we made a change
     return(err);
  }

  return(0);
}

//***********************************************************************
// rs_simple_destroy - Destroys the simple RS service
//***********************************************************************

void rs_simple_destroy(resource_service_fn_t *rs)
{
  rs_simple_priv_t *rss = (rs_simple_priv_t *)rs->priv;
  apr_status_t value;

log_printf(15, "rs_simple_destroy: sl=%p\n", rss->rid_table); flush_log();

  //** Notify the depot check thread
  apr_thread_mutex_lock(rss->lock);
  rss->shutdown = 1;
  apr_thread_cond_broadcast(rss->cond);
  apr_thread_mutex_unlock(rss->lock);

  //** Wait for it to shutdown
  apr_thread_join(&value, rss->check_thread);

  //** Now we can free up all the space
  apr_thread_mutex_destroy(rss->lock);
  apr_thread_cond_destroy(rss->cond);
  apr_pool_destroy(rss->mpool);  //** This also frees the hash tables

  list_destroy(rss->rid_table);

  free(rss->random_array);
  free(rss->fname);
  free(rss);
  free(rs);
}

//***********************************************************************
// rs_simple_create - Creates a simple resource management service from
//    the given file.
//***********************************************************************

resource_service_fn_t *rs_simple_create(void *arg, inip_file_t *kf, char *section)
{
  service_manager_t *ess = (service_manager_t *)arg;
  rs_simple_priv_t *rss;
  resource_service_fn_t *rs;
  struct stat sbuf;

  //** Create the new RS list
  type_malloc_clear(rss, rs_simple_priv_t, 1);

  assert(apr_pool_create(&(rss->mpool), NULL) == APR_SUCCESS);
  apr_thread_mutex_create(&(rss->lock), APR_THREAD_MUTEX_DEFAULT, rss->mpool);
  apr_thread_mutex_create(&(rss->update_lock), APR_THREAD_MUTEX_DEFAULT, rss->mpool);
  apr_thread_cond_create(&(rss->cond), rss->mpool);
  rss->rid_mapping = apr_hash_make(rss->mpool);
  rss->mapping_updates = apr_hash_make(rss->mpool);

  rss->ds = lookup_service(ess, ESS_RUNNING, ESS_DS);
  rss->da = lookup_service(ess, ESS_RUNNING, ESS_DA);

  //** Set the resource service fn ptrs
  type_malloc_clear(rs, resource_service_fn_t, 1);
  rs->priv = rss;
  rs->get_rid_config = rss_get_rid_config;
  rs->register_mapping_updates = rss_mapping_register;
  rs->unregister_mapping_updates = rss_mapping_unregister;
  rs->translate_cap_set = rss_translate_cap_set;
  rs->query_new = rs_query_base_new;
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

  //** This is the file to use for loading the RID table
  rss->fname = inip_get_string(kf, section, "fname", NULL);
  rss->dynamic_mapping = inip_get_integer(kf, section, "dynamic_mapping", 0);
  rss->check_interval = inip_get_integer(kf, section, "check_interval", 300);
  rss->check_timeout = inip_get_integer(kf, section, "check_timeout", 60);
  rss->min_free = inip_get_integer(kf, section, "min_free", 100*1024*1024);

  //** Get the modify time to detect changes
//  assert(stat(rss->fname, &sbuf) == 0);
//  rss->modify_time = sbuf.st_mtime;
  rss->modify_time = 0;

  //** Load the RID table
  assert(_rs_simple_refresh(rs) == 0);
//  assert(_rs_simple_load(rs, rss->fname) == 0);

  //** Launch the check thread
  apr_thread_create(&(rss->check_thread), NULL, rss_check_thread, (void *)rs, rss->mpool);

  return(rs);
}

