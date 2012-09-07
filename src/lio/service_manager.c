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
// Routines for managing the automatic data service framework
//***********************************************************************

#define _log_module_index 191

#include <apr_strings.h>
#include "service_manager.h"
#include "type_malloc.h"
#include "log.h"

//***********************************************************************
//  set_service_type_arg - Sets the generic priv arg for the service type
//***********************************************************************

int set_service_type_arg(service_manager_t *sm, int sm_type, void *arg)
{
  apr_thread_mutex_lock(sm->lock);

  if (sm_type >= sm->n) {
     log_printf(0, "Index out of bounds!  sm_type=%d n_max=%d\n", sm_type, sm->n);
     apr_thread_mutex_unlock(sm->lock);
     return(1);
  }

  sm->arg[sm_type] = arg;

  apr_thread_mutex_unlock(sm->lock);

  return(0);
}

//***********************************************************************
//  set_service_type_arg - Sets the generic priv arg for the service type
//***********************************************************************

void *get_service_type_arg(service_manager_t *sm, int sm_type)
{
  void *arg;

  apr_thread_mutex_lock(sm->lock);

  if (sm_type >= sm->n) {
     log_printf(0, "Index out of bounds!  sm_type=%d n_max=%d\n", sm_type, sm->n);
     apr_thread_mutex_unlock(sm->lock);
     return(NULL);
  }

  arg = sm->arg[sm_type];

  apr_thread_mutex_unlock(sm->lock);

  return(arg);
}


//***********************************************************************
//  add_service - Adds a service to the appropriate list
//***********************************************************************

int add_service(service_manager_t *sm, int sm_type, char *service_name, void *service)
{
  char *key;

  apr_thread_mutex_lock(sm->lock);

  if (sm_type >= sm->n) {
     log_printf(0, "Index out of bounds!  sm_type=%d n_max=%d\n", sm_type, sm->n);
     apr_thread_mutex_unlock(sm->lock);
     return(1);
  }

  key = apr_pstrdup(sm->pool, service_name);
  apr_hash_set(sm->queue[sm_type], key, APR_HASH_KEY_STRING, service);

  apr_thread_mutex_unlock(sm->lock);

  return(0);
}

//***********************************************************************
// lookup_data_service - returns the current DS for the given type
//***********************************************************************

void *lookup_service(service_manager_t *sm, int sm_type, char *service_name)
{
  void *s;

  apr_thread_mutex_lock(sm->lock);

  if (sm_type >= sm->n) {
     log_printf(0, "Index out of bounds!  sm_type=%d n_max=%d\n", sm_type, sm->n);
     apr_thread_mutex_unlock(sm->lock);
     return(NULL);
  }

  s = apr_hash_get(sm->queue[sm_type], service_name, APR_HASH_KEY_STRING);
  if (s == NULL) {
    log_printf(0, "No matching driver for type=%s\n", service_name);
  }
  apr_thread_mutex_unlock(sm->lock);

  return(s);
}

//***********************************************************************
//  destroy_service_manager - Destroys an existing SM.
//***********************************************************************

void destroy_service_manager(service_manager_t *sm)
{
   apr_pool_destroy(sm->pool);
   free(sm->queue);
   free(sm->arg);
   free(sm);
}


//***********************************************************************
// create_service_manager - Creates a new SM for use
//***********************************************************************

service_manager_t *create_service_manager(int n)
{
  int i;
  service_manager_t *sm;

  type_malloc_clear(sm, service_manager_t, 1);

  apr_pool_create(&sm->pool, NULL);
  apr_thread_mutex_create(&sm->lock, APR_THREAD_MUTEX_DEFAULT, sm->pool);
  sm->n = n;

  type_malloc(sm->queue, apr_hash_t *, n);
  type_malloc_clear(sm->arg, void *, n);
  for (i=0; i<n; i++) {
     sm->queue[i] = apr_hash_make(sm->pool);
  }

  return(sm);
}


