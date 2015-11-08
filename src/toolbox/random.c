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

//*******************************************************************
//*******************************************************************

#define _log_module_index 110

#include "random.h"
#include <apr_thread_mutex.h>
#include <apr_pools.h>
#include <openssl/rand.h>
#include <assert.h>
#include "assert_result.h"

apr_thread_mutex_t  *_rnd_lock = NULL;
apr_pool_t *_rnd_pool = NULL;
int _rnd_count = 0;

//*******************************************************************
//  init_random - Inititalizes the random number generator for use
//*******************************************************************

int init_random()
{
    long max_bytes = 1024;

    _rnd_count++;
    if (_rnd_lock != NULL) return(0);

    assert (RAND_load_file("/dev/urandom", max_bytes) == max_bytes);

    apr_pool_create(&_rnd_pool, NULL);
    apr_thread_mutex_create(&_rnd_lock, APR_THREAD_MUTEX_DEFAULT,_rnd_pool);

    return(0);
}

//*******************************************************************
//  destroy_random - Destroys the random number generator for use
//*******************************************************************

int destroy_random()
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

int get_random(void *buf, int nbytes)
{
    int err;

    if (_rnd_lock == NULL) init_random();

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
    get_random(&rn, sizeof(rn));
    dn = (1.0 * rn) / (UINT64_MAX + 1.0);

    n = lo + (hi - lo) * dn;

    return(n);
}

//*******************************************************************
// Returns a random integer within the given range
//*******************************************************************

int64_t random_int(int64_t lo, int64_t hi)
{
    int64_t n, dn;

    dn = hi - lo + 1;
    n = lo + dn * random_double(0, 1);

    return(n);
}
