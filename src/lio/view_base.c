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

//***********************************************************************
// Routines for managing the view loading framework
//***********************************************************************

#define _log_module_index 166

#include "ex3_abstract.h"
#include <tbx/list.h>
#include <tbx/random.h>
#include <tbx/type_malloc.h>
#include <tbx/log.h>


//***********************************************************************
// lio_view_insert - Inserts a view into an existing exnode
//***********************************************************************

int lio_view_insert(exnode_t *ex, segment_t *seg)
{
    tbx_atomic_inc(seg->ref_count);

    return(tbx_list_insert(ex->view, &segment_id(seg), seg));
}

//***********************************************************************
// view_remove - Removes a view into an existing exnode
//***********************************************************************

int view_remove(exnode_t *ex, segment_t *seg)
{
    tbx_atomic_dec(seg->ref_count);

    tbx_list_remove(ex->view, &segment_id(seg), seg);

    return(0);
}

