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

//*************************************************************************
//*************************************************************************

#define _log_module_index 103

#include "atomic_counter.h"
#include "apr_thread_proc.h"
#include "stdlib.h"

atomic_int_t _atomic_global_counter = 0;

static apr_threadkey_t *atomic_thread_id_key;
atomic_int_t _atomic_times_used = 0;
apr_pool_t *_atomic_mpool = NULL;

//*************************************************************************
// atomic_global_counter - Returns the global counter and inc's it as well
//*************************************************************************

inline int atomic_global_counter()
{
  int n;
  n = atomic_inc(_atomic_global_counter);
  if (n > 1073741824) atomic_set(_atomic_global_counter, 0);
  return(n);
}

//*************************************************************************
// _a_thread_id_ptr - Returns the pointer to the thread unique id
//*************************************************************************

int *_a_thread_id_ptr()
{
  int *ptr = NULL;

  apr_threadkey_private_get((void *)&ptr, atomic_thread_id_key);
  if (ptr == NULL ){
     ptr = (int *)malloc(sizeof(int));
     apr_threadkey_private_set(ptr, atomic_thread_id_key);
     *ptr = atomic_global_counter();
  }

  return(ptr);
}

//***************************************************************************

void _atomic_destructor(void *ptr) 
{ 
  free(ptr);   
}

//*************************************************************************
//  atomic_init - initializes the atomic routines. Only needed if using the
//     thread_id or global counter routines
//*************************************************************************

void atomic_init()
{
  if (atomic_inc(_atomic_times_used) != 0) return;

  apr_pool_create(&_atomic_mpool, NULL);
  apr_threadkey_private_create(&atomic_thread_id_key,_atomic_destructor, _atomic_mpool);
}

//*************************************************************************
//  atomic_destroy - Destroys the atomic routines. Only needed if using the
//     thread_id or global counter routines
//*************************************************************************

void atomic_destroy()
{
  if (atomic_dec(_atomic_times_used) > 0) return;
  
  apr_pool_destroy(_atomic_mpool);
}


