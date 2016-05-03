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

//**************************************************************
//  Random number interface
//**************************************************************

#ifndef _RANDOM_H_
#define _RANDOM_H_

#include "tbx/toolbox_visibility.h"
#include <inttypes.h>

TBX_API int init_random();
TBX_API int destroy_random();
TBX_API int get_random(void *buf, int nbytes);
void random_seed(const void *buf, int nbytes);
double random_double(double lo, double hi);
TBX_API int64_t random_int(int64_t lo, int64_t hi);

#endif



