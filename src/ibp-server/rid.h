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


#ifndef _RID_H_
#define _RID_H_

#ifdef __cplusplus
extern "C" {
#endif

#define RID_LEN          128

typedef struct {      //**IBP resource ID data type
  char name[RID_LEN];
} rid_t;


char *ibp_rid2str(rid_t rid, char *buffer);
int ibp_str2rid(char *rid_str, rid_t *rid);
void ibp_empty_rid(rid_t *rid);
int ibp_rid_is_empty(rid_t rid);
int ibp_compare_rid(rid_t rid1, rid_t rid2);

#ifdef __cplusplus
}
#endif

#endif
