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
// Dummy AuthN implementation
//***********************************************************************

#include "authn_abstract.h"

#ifndef _AUTHN_FAKE_H_
#define _AUTHN_FAKE_H_

#ifdef __cplusplus
extern "C" {
#endif

#define AUTHN_TYPE_FAKE "fake"

//*** HAck for getting userid@hosntam passed around to be romved with proper authn/z
typedef struct {
    char *handle;
    int len;
} authn_fake_priv_t;
//--------------------------

authn_t *authn_fake_create(service_manager_t *ess, tbx_inip_file_t *ifd, char *section);

#ifdef __cplusplus
}
#endif

#endif

