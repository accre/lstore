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
//  Header file to simplify using "?printf" routines and different
//  data types, like size_t.
//******************************************************************

#ifndef __FMTTYPES_
#define __FMTTYPES_

#include <inttypes.h>
#include <apr_time.h>
#define I64T "%" PRId64    //int64_t
#define LU   "%" PRIu64    //uint64_t
#define OT   I64T          // ibp_off_t
#define ST   "%zu"          // size_t
#define TT   "%" APR_TIME_T_FMT  // time format

#endif

