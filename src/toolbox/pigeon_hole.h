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

#ifndef __PIGEON_HOLE_H_
#define __PIGEON_HOLE_H_

#include "tbx/toolbox_visibility.h"
#include <apr_thread_mutex.h>
#include <apr_pools.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tbx_ph_t tbx_ph_t;
struct tbx_ph_t {
    apr_thread_mutex_t *lock;
    apr_pool_t *pool;
    int nholes;
    int nused;
    int next_slot;
    char *hole;
    const char *name;
};

typedef struct tbx_ph_iter_t tbx_ph_iter_t;
struct tbx_ph_iter_t {
    tbx_ph_t *ph;
    int start_slot;
    int count;
    int found;
};

tbx_ph_iter_t pigeon_hole_iterator_init(tbx_ph_t *ph);
int pigeon_hole_iterator_next(tbx_ph_iter_t *pi);
int pigeon_holes_used(tbx_ph_t *ph);
int pigeon_holes_free(tbx_ph_t *ph);
void release_pigeon_hole(tbx_ph_t *ph, int slot);
int reserve_pigeon_hole(tbx_ph_t *ph);
void destroy_pigeon_hole(tbx_ph_t *ph);
tbx_ph_t *new_pigeon_hole(const char *name, int size);

#ifdef __cplusplus
}
#endif

#endif

