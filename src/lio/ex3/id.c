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
// Generates a a unique id
//***********************************************************************

#define _log_module_index 150

#include <tbx/random.h>

#include "ex3/types.h"

void generate_ex_id(ex_id_t *id)
{
    //** Fill with random data
    tbx_random_get_bytes(id, sizeof(ex_id_t));

    //** Mask the sign bit cause JAVA doesn't like unsigned numbers
    *id = (*id) << 1;
    *id = (*id) >> 1;
}
