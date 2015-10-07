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

#define _log_module_index 113

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include "log.h"
#include "pigeon_hole.h"
#include "pigeon_coop.h"

//***************************************************************************
// pigeon_coop_hole_data - Returns a pointer to the data
//***************************************************************************

void *pigeon_coop_hole_data(pigeon_coop_hole_t *pch)
{
  return(pch->data);
}

//***************************************************************************
// pigeon_coop_iterator_init - Initializes an iterator
//***************************************************************************

pigeon_coop_iter_t pigeon_coop_iterator_init(pigeon_coop_t *pc)
{
  pigeon_coop_iter_t pci;

  apr_thread_mutex_lock(pc->lock);

//log_printf(10, "pigeon_coop_iterator_init: pc=%s nshelves=%d nused=%d\n", pc->name, pc->nshelves, pc->nused);
  pci.pc = pc;
  pci.shelf = 0;
  pci.pi = pigeon_hole_iterator_init(pc->ph_shelf[pci.shelf]);

  apr_thread_mutex_unlock(pc->lock);

  return(pci);
}

//***************************************************************************
// pigeon_coop_iterator_next - Returns the next used hole
//***************************************************************************

pigeon_coop_hole_t pigeon_coop_iterator_next(pigeon_coop_iter_t *pci)
{
  int i, slot;
  pigeon_coop_hole_t pch;
  pigeon_hole_t *shelf;
  pigeon_coop_t *pc = pci->pc;

  if ((pci->shelf == -1)) {
     pch.shelf = -1;
     pch.hole = -1;
     pch.data = NULL;

//log_printf(10, "pigeon_coop_iterator_next: pc=%s nothing left 1\n", pc->name); flush_log();

     return(pch);    
  }

  apr_thread_mutex_lock(pc->lock);

//log_printf(10, "pigeon_coop_iterator_next: pc=%s nshelves=%d nused=%d\n", pc->name, pc->nshelves, pc->nused); flush_log();

  //** Check if coop contracted since last call
  if (pci->shelf >= pc->nshelves) {
     apr_thread_mutex_unlock(pc->lock);
     pch.shelf = -1;
     pch.hole = -1;
     pch.data = NULL;

//log_printf(10, "pigeon_coop_iterator_next: pc=%s nothing left 2\n", pc->name); flush_log();
     return(pch);    
  }

  //** Check if the current shelf has anything used
  slot = pigeon_hole_iterator_next(&(pci->pi));
  if (slot >= 0) {
     pch.shelf = pci->shelf;
     pch.hole = slot;
     pch.data = &(pc->data_shelf[pch.shelf][pch.hole*pc->item_size]);

//log_printf(10, "pigeon_coop_iterator_next: pc=%s FOUND-1 shelf=%d slot=%d\n", pc->name, pch.shelf, pch.hole); flush_log();

     apr_thread_mutex_unlock(pc->lock);

     return(pch);    
  } else {  //** Nope got to check the next shelf
    pci->shelf++;
    for (i=pci->shelf; i<pc->nshelves; i++)  {
        shelf = pc->ph_shelf[i];
        if (pigeon_holes_used(shelf) > 0) {
            pci->pi = pigeon_hole_iterator_init(shelf);
            slot = pigeon_hole_iterator_next(&(pci->pi));
            if (slot >= 0) {
               pch.shelf = pci->shelf;
               pch.hole = slot;
               pch.data = (void *)&(pc->data_shelf[pch.shelf][pch.hole*pc->item_size]);
//log_printf(10, "pigeon_coop_iterator_next: pc=%s FOUND-2 shelf=%d slot=%d\n", pc->name, pch.shelf, pch.hole); flush_log();
               apr_thread_mutex_unlock(pc->lock);
               return(pch);    
            }
        }
    }
  }

//log_printf(10, "pigeon_coop_iterator_next: pc=%s nothing left 3\n", pc->name);

  apr_thread_mutex_unlock(pc->lock);

  //** Nothing left if we made it here
  pci->shelf = -1;

  pch.shelf = -1;
  pch.hole = -1;
  pch.data = NULL;

  return(pch);
}




//***************************************************************************
//  release_pigeon_hole - releases a pigeon hole for use
//   
//***************************************************************************

int release_pigeon_coop_hole(pigeon_coop_t *pc, pigeon_coop_hole_t *pch)
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

  release_pigeon_hole(pc->ph_shelf[pch->shelf], pch->hole);
  n = pigeon_holes_used(pc->ph_shelf[pch->shelf]);

  pc->check_shelf = pch->shelf;  //** Look here for the next free slot
  pc->nused--;

  //** Invalidate the hole
  pch->hole = -1;
  pch->shelf = -1;
  pch->data = NULL;


  if ((n == 0) && (pc->nshelves > 1)) {  //** Empty shelf so see if we can free up the shelf
     //** Can only free up empty shelves on the top due to pch mapping issues
     i = pc->nshelves-1;
     while ((pigeon_holes_used(pc->ph_shelf[i]) == 0) && (i>0)) {
        i--;
     }    

     log_printf(10, "release_pigeon_coop_hole: pc=%s Attempting to free shelves.  nshelves=%d last_used=%d nused=%d\n", pc->name, pc->nshelves, n, pc->nused); flush_log();
     n = i+1;      

     if (n < pc->nshelves) {
        //** Free up the shelves
        for (i=n; i < pc->nshelves; i++) {
            destroy_pigeon_hole(pc->ph_shelf[i]);
            pc->free(pc->new_arg, pc->shelf_size, pc->data_shelf[i]);
        }


        //** Adjust the array sizes
        pc->nshelves = n;
        pc->ph_shelf = (pigeon_hole_t **)realloc(pc->ph_shelf, pc->nshelves*sizeof(pigeon_hole_t *));
        assert(pc->ph_shelf != NULL);
        pc->data_shelf = realloc(pc->data_shelf, pc->nshelves*sizeof(char *));
        assert(pc->data_shelf != NULL);

     log_printf(10, "release_pigeon_coop_hole: pc=%s after free shelves.  nshelves=%d nused=%d\n", pc->name, pc->nshelves, pc->nused); flush_log();

     }
  }
   
  apr_thread_mutex_unlock(pc->lock);

  return(0);
}

//***************************************************************************
//  reserve_pigeon_coop_hole - Allocates a pigeon hole from the coop
//***************************************************************************

pigeon_coop_hole_t reserve_pigeon_coop_hole(pigeon_coop_t *pc)
{
  int i, slot, n, start_shelf;
  pigeon_coop_hole_t pch;

  apr_thread_mutex_lock(pc->lock);

  //** Check for a free slot **
  n = pc->nused / pc->shelf_size;
  start_shelf = (n > pc->check_shelf) ? pc->check_shelf : n;
  for (n=0; n < pc->nshelves; n++) {  
     i = (start_shelf + n) % pc->nshelves;
     slot = reserve_pigeon_hole(pc->ph_shelf[i]);
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
  pc->ph_shelf = (pigeon_hole_t **)realloc(pc->ph_shelf, pc->nshelves*sizeof(pigeon_hole_t *));
  assert(pc->ph_shelf != NULL);
  pc->data_shelf = realloc(pc->data_shelf, pc->nshelves*sizeof(char *));
  assert(pc->data_shelf != NULL);

  i = pc->nshelves-1;
  pc->ph_shelf[i] = new_pigeon_hole(pc->name, pc->shelf_size);
  pc->data_shelf[i] = pc->new(pc->new_arg, pc->shelf_size);

  //** get the slot **
  slot = reserve_pigeon_hole(pc->ph_shelf[i]);
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

void destroy_pigeon_coop(pigeon_coop_t *pc)
{
  int i;
  apr_thread_mutex_destroy(pc->lock);
  apr_pool_destroy(pc->pool);

  for (i=0; i<pc->nshelves; i++) {
//log_printf(10, "destroy_pigeon_coop: pc=%s nshelves=%d i=%d\n", pc->name, pc->nshelves, i); flush_log();
    destroy_pigeon_hole(pc->ph_shelf[i]);
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

pigeon_coop_t *new_pigeon_coop(const char *name, int size, int item_size, void *new_arg, void *(*new)(void *arg, int size), 
   void (*free)(void *arg, int size, void *dshelf))
{
  int i;
  int default_shelves = 1;
  pigeon_coop_t *pc = (pigeon_coop_t *)malloc(sizeof(pigeon_coop_t));
  assert(pc != NULL);

  pc->name = name;
  pc->new = new;
  pc->free = free;
  pc->new_arg = new_arg;
  pc->shelf_size = size;
  pc->item_size = item_size;
  pc->check_shelf = 0;
  pc->nused = 0;
  pc->nshelves = default_shelves;
  pc->ph_shelf = (pigeon_hole_t **)malloc(default_shelves*sizeof(pigeon_hole_t *));
  assert(pc->ph_shelf != NULL);
  pc->data_shelf = malloc(default_shelves*sizeof(char *));
  assert(pc->data_shelf != NULL);

  for (i=0; i<default_shelves; i++) {
     pc->ph_shelf[i] = new_pigeon_hole(pc->name, size);
     pc->data_shelf[i] = pc->new(new_arg, size);
  }

//log_printf(0, "new_pigeon_coop: size=%d\n", size);

  apr_pool_create(&(pc->pool), NULL);
  apr_thread_mutex_create(&(pc->lock), APR_THREAD_MUTEX_DEFAULT, pc->pool);

  return(pc);
}


