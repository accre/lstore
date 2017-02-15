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


#ifndef _LOCK_ALLOC_H
#define _LOCK_ALLOC_H

#include "visibility.h"

IBPS_API void lock_osd_id(osd_id_t id);
IBPS_API void unlock_osd_id(osd_id_t id);
IBPS_API void lock_osd_id_pair(osd_id_t id1, osd_id_t id2);
IBPS_API void unlock_osd_id_pair(osd_id_t id1, osd_id_t id2);
IBPS_API void lock_alloc_init();
IBPS_API void lock_alloc_destroy();

#endif
