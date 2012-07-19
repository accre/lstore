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

#ifndef __PIGEON_COOP_H_
#define __PIGEON_COOP_H_

#include <apr_thread_mutex.h>
#include <apr_pools.h>
#include "pigeon_hole.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
   apr_thread_mutex_t *lock;
   apr_pool_t *pool;
   int nshelves;
   int shelf_size;
   int item_size;
   int check_shelf;
   int nused;
   const char *name;
   void *new_arg;
   pigeon_hole_t **ph_shelf;
   char **data_shelf;
   void *(*new)(void *arg, int size);
   void (*free)(void *arg, int size, void *dshelf);
}  pigeon_coop_t;

typedef struct {
   int shelf;
   int hole;
   void *data;
} pigeon_coop_hole_t;

typedef struct {
  pigeon_coop_t *pc;
  int shelf;
  pigeon_hole_iter_t pi;
} pigeon_coop_iter_t;

pigeon_coop_iter_t pigeon_coop_iterator_init(pigeon_coop_t *ph);
pigeon_coop_hole_t pigeon_coop_iterator_next(pigeon_coop_iter_t *pci);
void *pigeon_coop_hole_data(pigeon_coop_hole_t *pch);
int release_pigeon_coop_hole(pigeon_coop_t *ph, pigeon_coop_hole_t *pch);
pigeon_coop_hole_t reserve_pigeon_coop_hole(pigeon_coop_t *pc);
void destroy_pigeon_coop(pigeon_coop_t *ph);
pigeon_coop_t *new_pigeon_coop(const char *name, int size, int item_size, void *new_arg, void *(*new)(void *arg, int size), 
   void (*free)(void *arg, int size, void *dshelf));

#ifdef __cplusplus
}
#endif

#endif


