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

//******************************************************************
//******************************************************************

#define _log_module_index 114

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "pigeon_hole.h"
#include "tbx/assert_result.h"
#include "tbx/log.h"

//***************************************************************************
// tbx_ph_iter_init - Initializes an iterator
//***************************************************************************

tbx_ph_iter_t tbx_ph_iter_init(tbx_ph_t *ph)
{
    tbx_ph_iter_t pi;

    apr_thread_mutex_lock(ph->lock);

    pi.ph = ph;
    pi.start_slot = ph->next_slot - ph->nused;
    if (pi.start_slot < 0) pi.start_slot = pi.start_slot + ph->nholes;
    pi.found = 0;
    pi.count = 0;

    apr_thread_mutex_unlock(ph->lock);

    return(pi);
}

//***************************************************************************
// tbx_ph_next - Returns the next used hole
//***************************************************************************

int tbx_ph_next(tbx_ph_iter_t *pi)
{
    int i, slot;
    tbx_ph_t *ph = pi->ph;

    if (pi->count == -1) return(-1);

    apr_thread_mutex_lock(ph->lock);
    for (i=pi->count; i<ph->nholes; i++) {
        slot = (pi->start_slot + i) % ph->nholes;
        if (ph->hole[slot] == 1) {
            apr_thread_mutex_unlock(ph->lock);

            pi->count = i+1;;
            pi->found++;
            return(slot);
        }
    }

    apr_thread_mutex_unlock(ph->lock);

    pi->count = -1;
    return(-1);
}


//***************************************************************************
// tbx_ph_used - Returns the number of holes used
//***************************************************************************

int tbx_ph_used(tbx_ph_t *ph)
{
    int n;

    apr_thread_mutex_lock(ph->lock);
    n = ph->nused;
    apr_thread_mutex_unlock(ph->lock);

    return(n);
}

//***************************************************************************
// tbx_ph_free - Returns the number of holes free
//***************************************************************************

int tbx_ph_free(tbx_ph_t *ph)
{
    int n;

    apr_thread_mutex_lock(ph->lock);
    n = ph->nholes - ph->nused;
    apr_thread_mutex_unlock(ph->lock);

    return(n);
}

//***************************************************************************
//  tbx_ph_release - releases a pigeon hole for use
//***************************************************************************

void tbx_ph_release(tbx_ph_t *ph, int slot)
{
    apr_thread_mutex_lock(ph->lock);
    log_printf(15, "tbx_ph_release: ph=%s nholes=%d start nused=%d slot=%d\n", ph->name, ph->nholes, ph->nused, slot);

    //** Check for valid range
    if ((slot<0) || (slot>=ph->nholes)) {
        log_printf(15, "tbx_ph_release: ERROR ph=%p slot=%d is invalid\n", ph, slot);
        apr_thread_mutex_unlock(ph->lock);
        return;
    }

    if (ph->hole[slot] == 1) {
        ph->hole[slot] = 0;
        ph->nused--;
    } else {
        log_printf(15, "tbx_ph_release: ERROR ph=%s nholes=%d nused=%d slot=%d is NOT USED!!!\n", ph->name, ph->nholes, ph->nused, slot);
//abort();
    }
    apr_thread_mutex_unlock(ph->lock);
}

//***************************************************************************
//  tbx_ph_reserve - Allocates a pigeon hole
//***************************************************************************

int tbx_ph_reserve(tbx_ph_t *ph)
{
    int i, slot;

    apr_thread_mutex_lock(ph->lock);

//log_printf(15, "tbx_ph_reserve: ph=%s nholes=%d nused=%d\n", ph->name, ph->nholes, ph->nused);

    if (ph->nused == ph->nholes) { //** All holes used so return
        apr_thread_mutex_unlock(ph->lock);
        return(-1);
    }

    for (i=0; i < ph->nholes; i++) {
        slot = (ph->next_slot + i) % ph->nholes; //** Start scanning at the last hole
        if (ph->hole[slot] == 0) {
            ph->hole[slot] = 1;
            ph->nused++;
            ph->next_slot = (slot+1) % ph->nholes;
            log_printf(10, "tbx_ph_reserve: ph=%s slot=%d\n", ph->name, slot);
            apr_thread_mutex_unlock(ph->lock);
            return(slot);
        }
    }

    apr_thread_mutex_unlock(ph->lock);

    abort();

    return(-1);
}


//***************************************************************************
// tbx_ph_destroy - Destroys a pigeon hole structure
//***************************************************************************

void tbx_ph_destroy(tbx_ph_t *ph)
{
    apr_thread_mutex_destroy(ph->lock);
    apr_pool_destroy(ph->pool);

    free(ph->hole);
    free(ph);
}

//***************************************************************************
// tbx_ph_new - Creates a new pigeon hole structure
//***************************************************************************

tbx_ph_t *tbx_ph_new(const char *name, int size)
{
    tbx_ph_t *ph = (tbx_ph_t *)malloc(sizeof(tbx_ph_t));
    memset((void *)ph, 0, sizeof(tbx_ph_t));
   FATAL_UNLESS(ph != NULL);

    ph->hole = (char *)malloc(size);
   FATAL_UNLESS(ph->hole != NULL);

    memset(ph->hole, 0, size);
    ph->name = name;
    apr_pool_create(&(ph->pool), NULL);
    apr_thread_mutex_create(&(ph->lock), APR_THREAD_MUTEX_DEFAULT, ph->pool);

    ph->nholes = size;
    ph->nused = 0;
    ph->next_slot = 0;

    return(ph);
}


