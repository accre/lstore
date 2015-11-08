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

//******************************************************************
//******************************************************************

#define _log_module_index 114

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include "assert_result.h"
#include "log.h"
#include "pigeon_hole.h"

//***************************************************************************
// pigeon_hole_iterator_init - Initializes an iterator
//***************************************************************************

pigeon_hole_iter_t pigeon_hole_iterator_init(pigeon_hole_t *ph)
{
    pigeon_hole_iter_t pi;

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
// pigeon_hole_iterator_next - Returns the next used hole
//***************************************************************************

int pigeon_hole_iterator_next(pigeon_hole_iter_t *pi)
{
    int i, slot;
    pigeon_hole_t *ph = pi->ph;

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
// pigeon_holes_used - Returns the number of holes used
//***************************************************************************

int pigeon_holes_used(pigeon_hole_t *ph)
{
    int n;

    apr_thread_mutex_lock(ph->lock);
    n = ph->nused;
    apr_thread_mutex_unlock(ph->lock);

    return(n);
}

//***************************************************************************
// pigeon_holes_free - Returns the number of holes free
//***************************************************************************

int pigeon_holes_free(pigeon_hole_t *ph)
{
    int n;

    apr_thread_mutex_lock(ph->lock);
    n = ph->nholes - ph->nused;
    apr_thread_mutex_unlock(ph->lock);

    return(n);
}

//***************************************************************************
//  release_pigeon_hole - releases a pigeon hole for use
//***************************************************************************

void release_pigeon_hole(pigeon_hole_t *ph, int slot)
{
    apr_thread_mutex_lock(ph->lock);
    log_printf(15, "release_pigeon_hole: ph=%s nholes=%d start nused=%d slot=%d\n", ph->name, ph->nholes, ph->nused, slot);

    //** Check for valid range
    if ((slot<0) || (slot>=ph->nholes)) {
        log_printf(15, "release_pigeon_hole: ERROR ph=%p slot=%d is invalid\n", ph, slot);
        apr_thread_mutex_unlock(ph->lock);
        return;
    }

    if (ph->hole[slot] == 1) {
        ph->hole[slot] = 0;
        ph->nused--;
    } else {
        log_printf(15, "release_pigeon_hole: ERROR ph=%s nholes=%d nused=%d slot=%d is NOT USED!!!\n", ph->name, ph->nholes, ph->nused, slot);
//abort();
    }
    apr_thread_mutex_unlock(ph->lock);
}

//***************************************************************************
//  reserve_pigeon_hole - Allocates a pigeon hole
//***************************************************************************

int reserve_pigeon_hole(pigeon_hole_t *ph)
{
    int i, slot;

    apr_thread_mutex_lock(ph->lock);

//log_printf(15, "reserve_pigeon_hole: ph=%s nholes=%d nused=%d\n", ph->name, ph->nholes, ph->nused);

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
            log_printf(10, "reserve_pigeon_hole: ph=%s slot=%d\n", ph->name, slot);
            apr_thread_mutex_unlock(ph->lock);
            return(slot);
        }
    }

    apr_thread_mutex_unlock(ph->lock);

    abort();

    return(-1);
}


//***************************************************************************
// destroy_pigeon_hole - Destroys a pigeon hole structure
//***************************************************************************

void destroy_pigeon_hole(pigeon_hole_t *ph)
{
    apr_thread_mutex_destroy(ph->lock);
    apr_pool_destroy(ph->pool);

    free(ph->hole);
    free(ph);
}

//***************************************************************************
// new_pigeon_hole - Creates a new pigeon hole structure
//***************************************************************************

pigeon_hole_t *new_pigeon_hole(const char *name, int size)
{
    pigeon_hole_t *ph = (pigeon_hole_t *)malloc(sizeof(pigeon_hole_t));
    assert(ph != NULL);

    ph->hole = (char *)malloc(size);
    assert(ph->hole != NULL);

//log_printf(0, "new_pigeon_hole: size=%d\n", size);
    memset(ph->hole, 0, size);
    ph->name = name;
    apr_pool_create(&(ph->pool), NULL);
    apr_thread_mutex_create(&(ph->lock), APR_THREAD_MUTEX_DEFAULT, ph->pool);

    ph->nholes = size;
    ph->nused = 0;
    ph->next_slot = 0;

    return(ph);
}


