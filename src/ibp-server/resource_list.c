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

//****************************************************************
//****************************************************************

#include <assert.h>
#include <string.h>
#include <tbx/type_malloc.h>
#include "resource_list.h"
#include "resource.h"

//****************************************************************
// resource_list_n_used - Returns the number of resources
//****************************************************************

int resource_list_n_used(Resource_list_t *rl)
{
  int n;

  apr_thread_mutex_lock(rl->lock);
  n = rl->n;
  apr_thread_mutex_unlock(rl->lock);
  
  return(n);
}

//****************************************************************
// resource_list_iterator - Creates a resource list iterator
//****************************************************************

resource_list_iterator_t resource_list_iterator(Resource_list_t *rl)
{
  return(0);
}

//****************************************************************
// resource_list_iterator_destroy - Destroys a resource list iterator
//****************************************************************

void resource_list_iterator_destroy(Resource_list_t *rl, resource_list_iterator_t *it)
{
  return;
}

//****************************************************************
// resource_list_iterator_next - Returns the next resource
//****************************************************************

Resource_t *resource_list_iterator_next(Resource_list_t *rl, resource_list_iterator_t *it)
{
  int i;
  Resource_t *r;

log_printf(15, "resource_list_iterator_next: start it=%d max=%d n=%d\n", *it, rl->max_res, rl->n); tbx_log_flush();

  apr_thread_mutex_lock(rl->lock);

  for (i=*it; i<rl->max_res; i++) {
     if (rl->res[i].used == 1) {
        *it = i + 1;
        r = rl->res[i].r;
        apr_thread_mutex_unlock(rl->lock);
log_printf(15, "resource_list_iterator_next: i=%d r=%s\n", i, r->name); tbx_log_flush();
        return(r);
     }
  }
log_printf(15, "resource_list_iterator_next: r=NULL\n"); tbx_log_flush();

  apr_thread_mutex_unlock(rl->lock);

  return(NULL);
}

//****************************************************************
// _resource_list_insert - Adds a resource to the list and returns
//  the array index
//      NOTE:  NO Locking is performed!!!!!
//****************************************************************

int _resource_list_insert(Resource_list_t *rl, Resource_t *r)
{
  int i, n;
  char *crid = NULL;
  char str[RID_LEN];

  n = 0;
  if (rl->n == rl->max_res) {  //** See if we need to grow the array
     n = rl->max_res + 1;
     assert((rl->res = (rl_ele_t *)realloc(rl->res, sizeof(rl_ele_t)*n)) != NULL);
     for (i=rl->max_res; i<n; i++) { rl->res[i].used = 0; }
     rl->max_res = n;
  }

  //** Find a slot
  for (i=0; i<rl->max_res; i++) {
     n = (rl->n+i) % rl->max_res;
     if (rl->res[n].used == 0) break;
  }

  //** Finally add it
  ibp_rid2str(r->rid, str);
  assert((crid = strdup(str)) != NULL);
  rl->res[n].crid = crid;
  rl->res[n].r = r;
  rl->res[n].used = 1;
  apr_hash_set(rl->table, crid, APR_HASH_KEY_STRING, r);
  rl->n++;

  return(n);
}

//****************************************************************
// resource_list_insert - Adds a resource to the list and returns
//  the array index
//****************************************************************

int resource_list_insert(Resource_list_t *rl, Resource_t *r)
{
  int err;

  apr_thread_mutex_lock(rl->lock);
  err = _resource_list_insert(rl, r);
  apr_thread_mutex_unlock(rl->lock);

  return(err);
}

//****************************************************************
// resource_list_delete - Removes a resource from the list
//****************************************************************

int resource_list_delete(Resource_list_t *rl, Resource_t *r)
{
  int i, notfound;

  apr_thread_mutex_lock(rl->lock);
  notfound = 1;

  for (i=0; i<rl->max_res; i++) {
     if (rl->res[i].used == 1) {
        log_printf(15, "checking res[%d]=%s for %s\n", i, rl->res[i].r->name, r->name);
        if (rl->res[i].r == r) {  //** Found it
           log_printf(15, "REMOVING res[%d]=%s for %s\n", i, rl->res[i].r->name, r->name);
           rl->n--;
           rl->res[i].r = NULL;
           rl->res[i].used = 0;
           apr_hash_set(rl->table, rl->res[i].crid, APR_HASH_KEY_STRING, NULL);
           free(rl->res[i].crid);
           notfound = 0;
           break;
        }
     }
  }

  apr_thread_mutex_unlock(rl->lock);

  return(notfound);
}


//****************************************************************
//  create_resource_list - Takes a list of resources and
//     creates a hash map from RID -> resource
//** NOT thread safe for inserst!
//****************************************************************

Resource_list_t *create_resource_list(int n)
{
  int i;
  Resource_list_t *rl;

  tbx_type_malloc_clear(rl, Resource_list_t, 1);
  tbx_type_malloc_clear(rl->res, rl_ele_t, n);
  assert(apr_pool_create(&(rl->mpool), NULL) == APR_SUCCESS);
  assert((rl->table = apr_hash_make(rl->mpool)) != NULL);
  apr_thread_mutex_create(&(rl->lock), APR_THREAD_MUTEX_DEFAULT, rl->mpool);
  rl->pending = tbx_stack_new();

  rl->n = 0;
  rl->max_res = n;
  rl->pick_index = 0;
  resource_set_pick_policy(rl, RL_PICK_ROUND_ROBIN);

  for (i=0; i<n; i++) {
    rl->res[i].used = 0;
    rl->res[i].r = NULL;
  }

  return(rl);
}

//****************************************************************
// free_resource_list - Frees the resource list
//****************************************************************

void free_resource_list(Resource_list_t *rl)
{
  int i;

  apr_thread_mutex_destroy(rl->lock);
  apr_pool_destroy(rl->mpool);

  for (i=0; i<rl->max_res; i++) {
     if (rl->res[i].used == 1)  free(rl->res[i].crid);
  }

  tbx_stack_free(rl->pending, 0);
  free(rl->res);
  free(rl);
}

//****************************************************************
//  resource_lookup - Returns the resource associated with the RID
//****************************************************************

Resource_t *resource_lookup(Resource_list_t *rl, char *rid)
{
  Resource_t *r;

//log_printf(15, "resource_lookup: looking up rid=!%s! len=%lu\n", rid, strlen(rid));

  apr_thread_mutex_lock(rl->lock);
  r = (Resource_t *)apr_hash_get(rl->table, rid, APR_HASH_KEY_STRING);
  apr_thread_mutex_unlock(rl->lock);

  return(r);
}

//****************************************************************
//  resource_pending_insert - Inserts the RID as a pending insert
//****************************************************************

int resource_list_pending_insert(Resource_list_t *rl, char *rid)
{
  int err;
  char *prid;

  apr_thread_mutex_lock(rl->lock);
  tbx_stack_move_to_top(rl->pending);
  err = 0;
  while ((prid = tbx_stack_get_current_data(rl->pending)) != NULL) {
log_printf(0, "rid=%s prid=%s\n", rid, prid);
    if (strcmp(rid, prid) == 0) {
       log_printf(0, "Attempting to insert an already pending RID=%s!\n", rid);
       err = 1;
       break;
    }

    tbx_stack_move_down(rl->pending);
  }

  if (err == 0) {  //** Safe to insert it
    tbx_stack_push(rl->pending, rid);
  }
  apr_thread_mutex_unlock(rl->lock);

  return(err);
}

//****************************************************************
//  resource_pending_activate - Activates a pending RID
//****************************************************************

int resource_list_pending_activate(Resource_list_t *rl, char *rid, Resource_t *r)
{
  int slot;
  char *prid;

  apr_thread_mutex_lock(rl->lock);
  tbx_stack_move_to_top(rl->pending);
  slot = -1;
  while ((prid = tbx_stack_get_current_data(rl->pending)) != NULL) {
log_printf(0, "rid=%s prid=%s\n", rid, prid);
    if (strcmp(rid, prid) == 0) {
       tbx_stack_delete_current(rl->pending, 0, 0);
       slot = _resource_list_insert(rl, r);
       break;
    }

    tbx_stack_move_down(rl->pending);
  }
  apr_thread_mutex_unlock(rl->lock);

  if (slot == -1) {
     log_printf(0, "No pending RID matches %s\n", rid);
  }

  return(slot);
}

//****************************************************************
//  resource_pick_random - Returns a random resource
//****************************************************************

Resource_t *resource_pick_random(Resource_list_t *rl, rid_t *rid)
{
  unsigned int ui;
  int slot, i, k, mode;

  tbx_random_get_bytes(&ui, sizeof(ui));
  double d = (1.0 * ui) / (UINT_MAX + 1.0);

  slot = rl->n * d;

  k = slot;
  for (i=0; i<rl->max_res; i++) {
     slot = (k + i) % rl->max_res;
     if (rl->res[slot].used == 1) {
        mode = resource_get_mode(rl->res[slot].r);
        if ((mode & RES_MODE_WRITE) > 0) break;  //** Found a match
     }
  }

  if (rid != NULL) *rid = rl->res[slot].r->rid;
  return(rl->res[slot].r);
}

//****************************************************************
//  resource_pick_round_robin - Rounds robin through the resources
//****************************************************************

Resource_t *resource_pick_round_robin(Resource_list_t *rl, rid_t *rid)
{
  int i, k, mode;
  int slot;

  slot = rl->pick_index % rl->max_res;

  k = slot;
  for (i=0; i<rl->max_res; i++) {
     slot = (k + i) % rl->max_res;
log_printf(15, "resource_pick_round_robin: i=%d slot=%d used=%d n=%d max=%d\n", i, slot, rl->res[slot].used, rl->n, rl->max_res);
     if (rl->res[slot].used == 1) {
        mode = resource_get_mode(rl->res[slot].r);
        if ((mode & RES_MODE_WRITE) > 0) break;  //** Found a match
     }
  }

  rl->pick_index = (slot + 1) % rl->max_res;

  if (rid != NULL) *rid = rl->res[slot].r->rid;
  return(rl->res[slot].r);
}

//****************************************************************
//  resource_pick_most_free - Returns the least used resource
//****************************************************************

Resource_t *resource_pick_most_free(Resource_list_t *rl, rid_t *rid)
{
  int i, free_index, mode;
  int64_t free_space, space;

  free_space = -1;
  free_index = -1;

  for (i=0; i<rl->max_res; i++) {
    if (rl->res[i].used == 1) {
       mode = resource_get_mode(rl->res[i].r);
       if ((mode & RES_MODE_WRITE) > 0) {
          space = resource_allocable(rl->res[i].r, 0);
          if (space > free_space) {
             free_space = space;
             free_index = i;
          }
       }
    }
  }

  if (rid != NULL) *rid = rl->res[free_index].r->rid;
  return(rl->res[free_index].r);
}

//****************************************************************
//  resource_pick - Returns a resource based o nthe current policy
//****************************************************************

Resource_t *resource_pick(Resource_list_t *rl, rid_t *rid)
{
  Resource_t *r;

  apr_thread_mutex_lock(rl->lock);

  if (rl->n > 0) {
     r = rl->pick_routine(rl, rid);
  } else {
     ibp_empty_rid(rid);
     apr_thread_mutex_unlock(rl->lock);
     return(NULL);
  }
  apr_thread_mutex_unlock(rl->lock);

  return(r);
}

//****************************************************************
//  resource_set_pick_policy - Sets the pick policy
//****************************************************************

void resource_set_pick_policy(Resource_list_t *rl, int policy)
{
  switch (policy) {
    case RL_PICK_RANDOM:      rl->pick_routine = resource_pick_random;  break; 
    case RL_PICK_ROUND_ROBIN: rl->pick_routine = resource_pick_round_robin;  break; 
    case RL_PICK_MOST_FREE:   rl->pick_routine = resource_pick_most_free;  break; 
    default:
     rl->pick_routine = resource_pick_round_robin;  break; 
  }
}
