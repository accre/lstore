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

//*******************************************************************
//*******************************************************************

#define _log_module_index 110

#include "tbx/random.h"
#include <apr_thread_mutex.h>
#include <apr_pools.h>
#include <openssl/rand.h>
#include <assert.h>
#include "tbx/assert_result.h"

// Forward declarations
void random_seed(const void *buf, int nbytes);
double random_double(double lo, double hi);
apr_thread_mutex_t  *_rnd_lock = NULL;
apr_pool_t *_rnd_pool = NULL;
int _rnd_count = 0;

//*******************************************************************
//  init_random - Inititalizes the random number generator for use
//*******************************************************************

int tbx_random_startup()
{
    long max_bytes = 1024;

    _rnd_count++;
    if (_rnd_lock != NULL) return(0);

    assert_result(RAND_load_file("/dev/urandom", max_bytes), max_bytes);

    apr_pool_create(&_rnd_pool, NULL);
    apr_thread_mutex_create(&_rnd_lock, APR_THREAD_MUTEX_DEFAULT,_rnd_pool);

    return(0);
}

//*******************************************************************
//  destroy_random - Destroys the random number generator for use
//*******************************************************************

int tbx_random_shutdown()
{
    _rnd_count--;
    if (_rnd_count > 0) return(0);

    apr_thread_mutex_destroy(_rnd_lock);
    apr_pool_destroy(_rnd_pool);

    return(0);
}

//*******************************************************************
// random_seed - Sets the random number seed
//*******************************************************************

void random_seed(const void *buf, int nbytes)
{
    apr_thread_mutex_lock(_rnd_lock);
    RAND_seed(buf, nbytes);
    apr_thread_mutex_unlock(_rnd_lock);

    return;
}

//*******************************************************************
// get_random - Gets nbytes  of random data and placed it in buf.
//*******************************************************************

int tbx_random_get_bytes(void *buf, int nbytes)
{
    int err;

    if (_rnd_lock == NULL) tbx_random_startup();

    apr_thread_mutex_lock(_rnd_lock);
    err = RAND_bytes((unsigned char *)buf, nbytes);
    apr_thread_mutex_unlock(_rnd_lock);

    return(err);
}

//*******************************************************************
// random_double Returns a random double precision number withing
//  given range.
//*******************************************************************

double random_double(double lo, double hi)
{
    double dn, n;
    uint64_t rn;

    rn = 0;
    tbx_random_get_bytes(&rn, sizeof(rn));
    dn = (1.0 * rn) / (UINT64_MAX + 1.0);

    n = lo + (hi - lo) * dn;

    return(n);
}

//*******************************************************************
// Returns a random integer within the given range
//*******************************************************************

int64_t tbx_random_get_int64(int64_t lo, int64_t hi)
{
    int64_t n, dn;

    dn = hi - lo + 1;
    n = lo + dn * random_double(0, 1);

    return(n);
}
