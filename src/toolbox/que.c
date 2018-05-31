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

#include <apr_pools.h>
#include <apr_time.h>
#include <apr_thread_cond.h>
#include <apr_thread_mutex.h>
#include <fcntl.h>
#include <sys/select.h>
#include <unistd.h>
#include <tbx/log.h>
#include <tbx/que.h>
#include <tbx/type_malloc.h>

struct tbx_que_s {
    apr_pool_t *mpool;
    apr_thread_cond_t *get_cond;
    apr_thread_cond_t *put_cond;
    apr_thread_mutex_t *lock;
    char *array;
    int n_objects;
    int object_size;
    int slot;
    int n_used;
    int get_waiting;
    int put_waiting;
};

//************************************************************************************

void tbx_que_destroy(tbx_que_t *q)
{

    apr_thread_mutex_destroy(q->lock);
    apr_thread_cond_destroy(q->get_cond);
    apr_thread_cond_destroy(q->put_cond);
    apr_pool_destroy(q->mpool);

    free(q->array);
    free(q);
    return;
}

//************************************************************************************

tbx_que_t *tbx_que_create(int n_objects, int object_size)
{
    tbx_que_t *q;

    tbx_type_malloc_clear(q, tbx_que_t, 1);
    tbx_type_malloc_clear(q->array, char, n_objects*object_size);

    assert_result(apr_pool_create(&(q->mpool), NULL), APR_SUCCESS);
    apr_thread_mutex_create(&(q->lock), APR_THREAD_MUTEX_DEFAULT, q->mpool);
    apr_thread_cond_create(&(q->get_cond), q->mpool);
    apr_thread_cond_create(&(q->put_cond), q->mpool);

    q->n_objects = n_objects;
    q->object_size = object_size;
    q->slot = 0;
    q->n_used = 0;
    q->get_waiting = 0;
    q->put_waiting = 0;

    return(q);
}

//************************************************************************************

int tbx_que_put(tbx_que_t *q, void *object, apr_time_t dt)
{
    int err, slot;
    apr_time_t stime;

    err = 0;
    stime = apr_time_now();
    apr_thread_mutex_lock(q->lock);

    while (1) {
        if (q->n_used < q->n_objects) {  //** Got space
            if (object != NULL) {
                slot = (q->n_used + q->slot) % q->n_objects;
                memcpy(q->array + slot*q->object_size, object, q->object_size);
                q->n_used++;
            }

            //** Check if someone is waiting
            if (q->get_waiting > 0) apr_thread_cond_broadcast(q->get_cond);
            if (q->put_waiting > 0) apr_thread_cond_broadcast(q->put_cond);

            break;
        }

        //** See if we timed out
        if ((apr_time_now()-stime) > dt) {
            err = 1;
            break;
        }

        //** See if we always block
        if (dt == TBX_QUE_BLOCK) stime = apr_time_now();

        //** If we made it here we have to wait for a slot to become free
        q->put_waiting++;
        apr_thread_cond_timedwait(q->put_cond, q->lock, dt);
        q->put_waiting--;
    }

    apr_thread_mutex_unlock(q->lock);

    return(err);
}

//************************************************************************************

int tbx_que_get(tbx_que_t *q, void *object, apr_time_t dt)
{
    int err;
    apr_time_t stime;

    err = 0;
    stime = apr_time_now();
    apr_thread_mutex_lock(q->lock);

    while (1) {
        if (q->n_used > 0) {  //** Got an object
            if (object != NULL) {
                q->slot = q->slot % q->n_objects;
                memcpy(object, q->array + q->slot*q->object_size, q->object_size);
                q->n_used--;
                q->slot++;
            }

            //** Check if someone is waiting
            if (q->get_waiting > 0) apr_thread_cond_broadcast(q->get_cond);
            if (q->put_waiting > 0) apr_thread_cond_broadcast(q->put_cond);

            break;
        }

        //** See if we timed out
        if ((apr_time_now()-stime) > dt) {
            err = 1;
            break;
        }

        //** See if we always block
        if (dt == TBX_QUE_BLOCK) stime = apr_time_now();

        //** If we made it here we have to wait for some data
        q->get_waiting++;
        apr_thread_cond_timedwait(q->get_cond, q->lock, dt);
        q->get_waiting--;
    }

    apr_thread_mutex_unlock(q->lock);

    return(err);
}