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

//************************************************************************
//************************************************************************

#ifndef _IBP_TIME_H_
#define _IBP_TIME_H_

#include <time.h>
#include <apr_time.h>

typedef time_t ibp_time_t;

#define ibp_time_now() time(NULL)
#define ibp2apr_time(a) apr_time_make((a), 0)
#define apr2ibp_time(i) apr_time_sec((i))


#endif



