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

//***********************************************************************
// Routines for managing the view loading framework
//***********************************************************************

#define _log_module_index 166

#include "ex3_abstract.h"
#include "list.h"
#include "random.h"
#include "type_malloc.h"
#include "log.h"


//***********************************************************************
// view_insert - Inserts a view into an existing exnode
//***********************************************************************

int view_insert(exnode_t *ex, segment_t *seg)
{
    atomic_inc(seg->ref_count);

    return(list_insert(ex->view, &segment_id(seg), seg));
}

//***********************************************************************
// view_remove - Removes a view into an existing exnode
//***********************************************************************

int view_remove(exnode_t *ex, segment_t *seg)
{
    atomic_dec(seg->ref_count);

    list_remove(ex->view, &segment_id(seg), seg);

    return(0);
}

