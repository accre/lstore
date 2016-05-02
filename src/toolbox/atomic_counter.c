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

//*************************************************************************
//*************************************************************************

#define _log_module_index 103

#include "atomic_counter.h"
#include "apr_thread_proc.h"
#include "stdlib.h"

static tbx_atomic_unit32_t _atomic_global_counter = 0;

static apr_threadkey_t *atomic_thread_id_key;
tbx_atomic_unit32_t _atomic_times_used = 0;
apr_pool_t *_atomic_mpool = NULL;

//*************************************************************************
// atomic_global_counter - Returns the global counter and inc's it as well
//*************************************************************************

inline int atomic_counter(tbx_atomic_unit32_t *counter)
{
    int n;
    n = atomic_inc(*counter);
    if (n > 1073741824) atomic_set(*counter, 0);
    return(n);
}

//*************************************************************************
// atomic_global_counter - Returns the global counter and inc's it as well
//*************************************************************************

inline int atomic_global_counter()
{
    return(atomic_counter(&_atomic_global_counter));
}

//*************************************************************************
// _a_thread_id_ptr - Returns the pointer to the thread unique id
//*************************************************************************

int *_a_thread_id_ptr()
{
    int *ptr = NULL;

    apr_threadkey_private_get((void *)&ptr, atomic_thread_id_key);
    if (ptr == NULL ) {
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

    _atomic_mpool = NULL;
    _atomic_times_used = 0;
}


