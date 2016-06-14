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
#include "gop/callback.h"
//*************************************************************
//  Generic Callback implementation
//*************************************************************

#include <stdarg.h>

#ifndef __CALLBACK_H_
#define __CALLBACK_H_

#ifdef __cplusplus
extern "C" {
#endif
#include "gop/gop_visibility.h"

void callback_append(callback_t **root_cb, callback_t *cb);
void callback_destroy(callback_t *root_cb);
void callback_execute(callback_t *cb, int value);
void callback_single_execute(callback_t *cb, int value);

#ifdef __cplusplus
}
#endif


#endif

