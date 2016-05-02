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

#ifndef __STACK_H__
#define __STACK_H__

#ifdef __cplusplus
extern "C" {
#endif

struct tbx_stack_ele_t {
    void *data;
    struct tbx_stack_ele_t *down, *up;
};

struct tbx_stack_t {
    tbx_stack_ele_t *top, *bottom, *curr;
    int n;
};

void set_stack_ele_data(tbx_stack_ele_t *ele, void *data);
tbx_stack_ele_t *pop_link(tbx_stack_t *stack);
int move_to_link(tbx_stack_t *stack, tbx_stack_ele_t *ele);
int insert_link_below(tbx_stack_t *stack, tbx_stack_ele_t *ele);

#ifdef __cplusplus
}
#endif

#endif
