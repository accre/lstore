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

#ifndef __RAID4_H_
#define __RAID4_H_

#ifdef __cplusplus
extern "C" {
#endif

void raid4_encode(int data_strips, char **data, char **parity, int block_size);
int raid4_decode(int data_strips, int *erasures, char **data, char **parity, int block_size);

#ifdef __cplusplus
}
#endif

#endif
