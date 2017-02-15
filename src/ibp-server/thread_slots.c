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

#include "ibp_server.h"
#include "pigeon_hole.h"

tbx_ph_t *_ph;

void release_thread_slot(int slot) { tbx_ph_release(_ph, slot); }
int reserve_thread_slot() { return(tbx_ph_reserve(_ph)); }
void destroy_thread_slots() { tbx_ph_destroy(_ph); }
void init_thread_slots(int size) { _ph = tbx_ph_new("thread_slots", size); }


