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

#include "visibility.h"
#include "allocation.h"

#ifndef _IBPS_PRINT_ALLOC_H_
#define _IBPS_PRINT_ALLOC_H_

IBPS_API void print_manage_history(char *buffer, int *used, int nbytes, Allocation_manage_ts_t *ts_list, int start);
IBPS_API void print_rw_history(char *buffer, int *used, int nbytes, Allocation_rw_ts_t *ts_list, int start);
IBPS_API void print_allocation(char *buffer, int *used, int nbytes, Allocation_t *a, Allocation_history_t *h,
                  int state, int cs_type, osd_off_t hbs, osd_off_t bs);

#endif


