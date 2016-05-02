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

#ifndef _IBP_MISC_H_
#define _IBP_MISC_H_

#include "ibp/ibp_visibility.h"
#include "ibp.h"

#ifdef __cplusplus
extern "C" {
#endif

void ibp_configure_signals();
//char *string_token(char *str, const char *sep, char **last, int *finished);
int parse_cap(ibp_context_t *ic, ibp_cap_t *cap, char *host, int *port, char *key, char *typekey);
int parse_cmpstr(char *str, char *host, int *port, int *size);
void ibppc_form_host(ibp_context_t *ic, char *hoststr, int n_host, char *host, rid_t rid);
//void sort_oplist(oplist_t *iolist);

#ifdef __cplusplus
}
#endif

#endif

