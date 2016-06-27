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
// Exnode3 abstract class
//***********************************************************************

#ifndef _EX3_TYPES_H_
#define _EX3_TYPES_H_

#include <ibp/ibp.h>
#include <inttypes.h>
#include <lio/ex3.h>

#ifdef __cplusplus
extern "C" {
#endif




ex_tbx_iovec_t *ex_iovec_create();
void ex_iovec_destroy(ex_tbx_iovec_t *iov);



#ifdef __cplusplus
}
#endif

#endif
