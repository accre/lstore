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
// View layoput support
//***********************************************************************

#ifndef _VIEW_LAYOUT_H_
#define _VIEW_LAYOUT_H_

#ifdef __cplusplus
extern "C" {
#endif

#define VIEW_TYPE_LAYOUT "layout"

view_t *view_layout_create(void *arg);
view_t *view_layout_load(void *arg, ex_id_t id, lio_exnode_exchange_t *ex);
int view_layout_set(view_t *v, layout_t *lay);

#ifdef __cplusplus
}
#endif

#endif
