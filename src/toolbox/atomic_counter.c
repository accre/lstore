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

#include <apr_thread_proc.h>
#include <apr_pools.h>
#include <stdlib.h>

#include "tbx/atomic_counter.h"

static tbx_atomic_int_t _tbx_atomic_global_counter = 0;

static apr_threadkey_t *tbx_atomic_thread_id_key;
tbx_atomic_int_t _atomic_times_used = 0;
apr_pool_t *_atomic_mpool = NULL;

//*************************************************************************
// tbx_atomic_global_counter - Returns the global counter and inc's it as well
//*************************************************************************

inline int tbx_atomic_counter(tbx_atomic_int_t *counter)
{
    int n;
    n = tbx_atomic_inc(*counter);
    if (n > 1073741824) tbx_atomic_set(*counter, 0);
    return(n);
}

//*************************************************************************
// tbx_atomic_global_counter - Returns the global counter and inc's it as well
//*************************************************************************

inline int tbx_atomic_global_counter()
{
    return(tbx_atomic_counter(&_tbx_atomic_global_counter));
}

//*************************************************************************
// tbx_a_thread_id_ptr - Returns the pointer to the thread unique id
//*************************************************************************

int *tbx_a_thread_id_ptr()
{
    int *ptr = NULL;

    apr_threadkey_private_get((void *)&ptr, tbx_atomic_thread_id_key);
    if (ptr == NULL ) {
        ptr = (int *)malloc(sizeof(int));
        apr_threadkey_private_set(ptr, tbx_atomic_thread_id_key);
        *ptr = tbx_atomic_global_counter();
    }

    return(ptr);
}

//***************************************************************************

void _atomic_destructor(void *ptr)
{
    free(ptr);
}

//*************************************************************************
//  tbx_atomic_startup - initializes the atomic routines. Only needed if using the
//     thread_id or global counter routines
//*************************************************************************

void tbx_atomic_startup()
{
    if (tbx_atomic_inc(_atomic_times_used) != 0) return;

    apr_pool_create(&_atomic_mpool, NULL);
    apr_threadkey_private_create(&tbx_atomic_thread_id_key,_atomic_destructor, _atomic_mpool);
}

//*************************************************************************
//  tbx_atomic_shutdown - Destroys the atomic routines. Only needed if using the
//     thread_id or global counter routines
//*************************************************************************

void tbx_atomic_shutdown()
{
    if (tbx_atomic_dec(_atomic_times_used) > 0) return;

    apr_pool_destroy(_atomic_mpool);

    _atomic_mpool = NULL;
    _atomic_times_used = 0;
}


