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

#define _log_module_index 113

#include <stdio.h>
#include <stdlib.h>

#include "pigeon_coop.h"
#include "pigeon_hole.h"
#include "tbx/assert_result.h"
#include "tbx/log.h"
#include "tbx/visibility.h"
#include "tbx/type_malloc.h"

//***************************************************************************
// pigeon_coop_hole_data - Returns a pointer to the data
//***************************************************************************

void *tbx_pch_data(tbx_pch_t *pch)
{
    return(pch->data);
}

//***************************************************************************
// tbx_pc_iter_init - Initializes an iterator
//***************************************************************************

tbx_pc_iter_t tbx_pc_iter_init(tbx_pc_t *pc)
{
    tbx_pc_iter_t pci;

    apr_thread_mutex_lock(pc->lock);

//log_printf(10, "tbx_pc_iter_init: pc=%s nshelves=%d nused=%d\n", pc->name, pc->nshelves, pc->nused);
    pci.pc = pc;
    pci.shelf = 0;
    pci.pi = tbx_ph_iter_init(pc->ph_shelf[pci.shelf]);

    apr_thread_mutex_unlock(pc->lock);

    return(pci);
}

//***************************************************************************
// tbx_pc_next - Returns the next used hole
//***************************************************************************

tbx_pch_t tbx_pc_next(tbx_pc_iter_t *pci)
{
    int i, slot;
    tbx_pch_t pch;
    tbx_ph_t *shelf;
    tbx_pc_t *pc = pci->pc;

    if (pci->shelf == -1) {
        pch.shelf = -1;
        pch.hole = -1;
        pch.data = NULL;

//log_printf(10, "tbx_pc_next: pc=%s nothing left 1\n", pc->name); tbx_log_flush();

        return(pch);
    }

    apr_thread_mutex_lock(pc->lock);

//log_printf(10, "tbx_pc_next: pc=%s nshelves=%d nused=%d\n", pc->name, pc->nshelves, pc->nused); tbx_log_flush();

    //** Check if coop contracted since last call
    if (pci->shelf >= pc->nshelves) {
        apr_thread_mutex_unlock(pc->lock);
        pch.shelf = -1;
        pch.hole = -1;
        pch.data = NULL;

//log_printf(10, "tbx_pc_next: pc=%s nothing left 2\n", pc->name); tbx_log_flush();
        return(pch);
    }

    //** Check if the current shelf has anything used
    slot = tbx_ph_next(&(pci->pi));
    if (slot >= 0) {
        pch.shelf = pci->shelf;
        pch.hole = slot;
        pch.data = &(pc->data_shelf[pch.shelf][pch.hole*pc->item_size]);

//log_printf(10, "tbx_pc_next: pc=%s FOUND-1 shelf=%d slot=%d\n", pc->name, pch.shelf, pch.hole); tbx_log_flush();

        apr_thread_mutex_unlock(pc->lock);

        return(pch);
    } else {  //** Nope got to check the next shelf
        pci->shelf++;
        for (i=pci->shelf; i<pc->nshelves; i++)  {
            shelf = pc->ph_shelf[i];
            if (tbx_ph_used(shelf) > 0) {
                pci->pi = tbx_ph_iter_init(shelf);
                slot = tbx_ph_next(&(pci->pi));
                if (slot >= 0) {
                    pch.shelf = pci->shelf;
                    pch.hole = slot;
                    pch.data = (void *)&(pc->data_shelf[pch.shelf][pch.hole*pc->item_size]);
//log_printf(10, "tbx_pc_next: pc=%s FOUND-2 shelf=%d slot=%d\n", pc->name, pch.shelf, pch.hole); tbx_log_flush();
                    apr_thread_mutex_unlock(pc->lock);
                    return(pch);
                }
            }
        }
    }

//log_printf(10, "tbx_pc_next: pc=%s nothing left 3\n", pc->name);

    apr_thread_mutex_unlock(pc->lock);

    //** Nothing left if we made it here
    pci->shelf = -1;

    pch.shelf = -1;
    pch.hole = -1;
    pch.data = NULL;

    return(pch);
}




//***************************************************************************
//  tbx_ph_release - releases a pigeon hole for use
//
//***************************************************************************

int tbx_pch_release(tbx_pc_t *pc, tbx_pch_t *pch)
{
    int i, n;

    apr_thread_mutex_lock(pc->lock);

    log_printf(10, "release_pigeon_coop_hole: pc=%s nused=%d nshelves=%d shelf=%d slot=%d\n", pc->name, pc->nused, pc->nshelves, pch->shelf, pch->hole);

//** Check for valid range
    if ((pch->shelf<0) || (pch->shelf>=pc->nshelves)) {
        log_printf(15, "release_pigeon_coop_hole: ERROR pc=%s shelf=%d is invalid\n", pc->name, pch->shelf);
//abort();
        apr_thread_mutex_unlock(pc->lock);
        return(-1);
    }

    tbx_ph_release(pc->ph_shelf[pch->shelf], pch->hole);
    n = tbx_ph_used(pc->ph_shelf[pch->shelf]);

    pc->check_shelf = pch->shelf;  //** Look here for the next free slot
    pc->nused--;

    //** Invalidate the hole
    pch->hole = -1;
    pch->shelf = -1;
    pch->data = NULL;


    if ((n == 0) && (pc->nshelves > 1)) {  //** Empty shelf so see if we can free up the shelf
        //** Can only free up empty shelves on the top due to pch mapping issues
        i = pc->nshelves-1;
        while ((tbx_ph_used(pc->ph_shelf[i]) == 0) && (i>0)) {
            i--;
        }

        log_printf(10, "release_pigeon_coop_hole: pc=%s Attempting to free shelves.  nshelves=%d last_used=%d nused=%d\n", pc->name, pc->nshelves, n, pc->nused);
        tbx_log_flush();
        n = i+1;

        if (n < pc->nshelves) {
            //** Free up the shelves
            for (i=n; i < pc->nshelves; i++) {
                tbx_ph_destroy(pc->ph_shelf[i]);
                pc->free(pc->new_arg, pc->shelf_size, pc->data_shelf[i]);
            }


            //** Adjust the array sizes
            pc->nshelves = n;
            pc->ph_shelf = (tbx_ph_t **)realloc(pc->ph_shelf, pc->nshelves*sizeof(tbx_ph_t *));
           FATAL_UNLESS(pc->ph_shelf != NULL);
            pc->data_shelf = realloc(pc->data_shelf, pc->nshelves*sizeof(char *));
           FATAL_UNLESS(pc->data_shelf != NULL);

            log_printf(10, "release_pigeon_coop_hole: pc=%s after free shelves.  nshelves=%d nused=%d\n", pc->name, pc->nshelves, pc->nused);
            tbx_log_flush();

        }
    }

    apr_thread_mutex_unlock(pc->lock);

    return(0);
}

//***************************************************************************
//  reserve_pigeon_coop_hole - Allocates a pigeon hole from the coop
//***************************************************************************

tbx_pch_t tbx_pch_reserve(tbx_pc_t *pc)
{
    int i, slot, n, start_shelf;
    tbx_pch_t pch;

    apr_thread_mutex_lock(pc->lock);

    //** Check for a free slot **
    n = pc->nused / pc->shelf_size;
    start_shelf = (n > pc->check_shelf) ? pc->check_shelf : n;
    for (n=0; n < pc->nshelves; n++) {
        i = (start_shelf + n) % pc->nshelves;
        slot = tbx_ph_reserve(pc->ph_shelf[i]);
//log_printf(10, "reserve_pigeon_coop_hole: pc=%s nshelves=%d i=%d slot=%d\n", pc->name, pc->nshelves, i, slot);
        if (slot != -1) {
            pc->nused++;
            pch.shelf = i;
            pch.hole = slot;
            pc->check_shelf = i;
            pch.data = (void *)&(pc->data_shelf[i][slot*pc->item_size]);
            log_printf(15, "reserve_pigeon_coop_hole: shelf=%d slot=%d\n", i, slot);
            apr_thread_mutex_unlock(pc->lock);
            return(pch);
        }
    }

    //** No free slots so have to grow the coop by adding a shelf **
    pc->nshelves++;
    pc->ph_shelf = (tbx_ph_t **)realloc(pc->ph_shelf, pc->nshelves*sizeof(tbx_ph_t *));
   FATAL_UNLESS(pc->ph_shelf != NULL);
    pc->data_shelf = realloc(pc->data_shelf, pc->nshelves*sizeof(char *));
   FATAL_UNLESS(pc->data_shelf != NULL);

    i = pc->nshelves-1;
    pc->ph_shelf[i] = tbx_ph_new(pc->name, pc->shelf_size);
    pc->data_shelf[i] = pc->new(pc->new_arg, pc->shelf_size);

    //** get the slot **
    slot = tbx_ph_reserve(pc->ph_shelf[i]);
    pc->check_shelf = i;
    pc->nused++;
    pch.shelf = i;
    pch.hole = slot;
    pch.data = &(pc->data_shelf[i][slot]);
    log_printf(15, "reserve_pigeon_coop_hole: shelf=%d slot=%d\n", i, slot);


    apr_thread_mutex_unlock(pc->lock);

    return(pch);
}


//***************************************************************************
// destroy_pigeon_coop - Destroys a pigeon coop structure
//***************************************************************************

void tbx_pc_destroy(tbx_pc_t *pc)
{
    int i;
    apr_thread_mutex_destroy(pc->lock);
    apr_pool_destroy(pc->pool);

    for (i=0; i<pc->nshelves; i++) {
//log_printf(10, "destroy_pigeon_coop: pc=%s nshelves=%d i=%d\n", pc->name, pc->nshelves, i); tbx_log_flush();
        tbx_ph_destroy(pc->ph_shelf[i]);
        pc->free(pc->new_arg, pc->shelf_size, pc->data_shelf[i]);
    }

    free(pc->ph_shelf);
    free(pc->data_shelf);
    free(pc);
}

//***************************************************************************
// new_pigeon_coop - Creates a new pigeon coop structure
//
//   name - Tag to use when printing log messages
//   size - Number of items to store in each shelf
//   item_size - Size of each item.
//***************************************************************************
TBX_API tbx_pc_t *tbx_pc_new(const char *name, int size, int item_size,
                                void *new_arg,
                                tbx_pc_new_fn_t new_fn,
                                tbx_pc_free_fn_t free)
{
    int i;
    int default_shelves = 1;
    tbx_pc_t *pc = (tbx_pc_t *)malloc(sizeof(tbx_pc_t));
   FATAL_UNLESS(pc != NULL);

    pc->name = name;
    pc->new = new_fn;
    pc->free = free;
    pc->new_arg = new_arg;
    pc->shelf_size = size;
    pc->item_size = item_size;
    pc->check_shelf = 0;
    pc->nused = 0;
    pc->nshelves = default_shelves;
    tbx_type_malloc(pc->ph_shelf, tbx_ph_t *, default_shelves);
   FATAL_UNLESS(pc->ph_shelf != NULL);
    tbx_type_malloc(pc->data_shelf, char *, default_shelves);
   FATAL_UNLESS(pc->data_shelf != NULL);

    for (i=0; i<default_shelves; i++) {
        pc->ph_shelf[i] = tbx_ph_new(pc->name, size);
        pc->data_shelf[i] = pc->new(new_arg, size);
    }

    apr_pool_create(&(pc->pool), NULL);
    apr_thread_mutex_create(&(pc->lock), APR_THREAD_MUTEX_DEFAULT, pc->pool);

    return(pc);
}


