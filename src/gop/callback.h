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
//*************************************************************
//  Generic Callback implementation
//*************************************************************


#ifndef __CALLBACK_H_
#define __CALLBACK_H_

#include <stdarg.h>

#include "gop/callback.h"

#ifdef __cplusplus
extern "C" {
#endif
#include "gop/visibility.h"

void callback_append(gop_callback_t **root_cb, gop_callback_t *cb);
void callback_destroy(gop_callback_t *root_cb);
void callback_execute(gop_callback_t *cb, int value);
void callback_single_execute(gop_callback_t *cb, int value);

#ifdef __cplusplus
}
#endif


#endif

