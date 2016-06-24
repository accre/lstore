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
// Dummy test for testing dynamic loading of modules
//***********************************************************************

#define _log_module_index 162

#include <libgen.h>
#include <tbx/atomic_counter.h>
#include <tbx/iniparse.h>
#include <tbx/interval_skiplist.h>
#include <tbx/log.h>

#include "ex3.h"
#include "ex3_compare.h"
#include "ex3_system.h"
#include "segment_file.h"

#define SEGMENT_TYPE_DYNFILE "dynfile"

//***********************************************************************
// segment_dynfile_create - Creates a file segment
//***********************************************************************

segment_t *segment_dynfile_create(void *arg)
{
    segment_t *seg;

    log_printf(0, "START\n");

    seg = segment_file_create(arg);
    seg->header.type = SEGMENT_TYPE_DYNFILE;

    log_printf(0, "END\n");

    return(seg);
}

//***********************************************************************
// segment_dynfile_load - Loads a file segment from ini/ex3
//***********************************************************************

segment_t *segment_dynfile_load(void *arg, ex_id_t id, exnode_exchange_t *ex)
{

    log_printf(0, "START\n");
    segment_t *seg = segment_dynfile_create(arg);
    segment_deserialize(seg, id, ex);
    seg->header.type = SEGMENT_TYPE_DYNFILE;
    log_printf(0, "END\n");

    return(seg);
}
