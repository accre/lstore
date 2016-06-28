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

#pragma once
#ifndef ACCRE_VARINT_H_INCLUDED
#define ACCRE_VARINT_H_INCLUDED

#include <inttypes.h>
#include <tbx/visibility.h>

#ifdef __cplusplus
extern "C" {
#endif


// Functions
TBX_API int tbx_varint_test();

TBX_API int tbx_zigzag_decode(uint8_t *buffer, int bufsize, int64_t *value);

TBX_API int tbx_zigzag_encode(int64_t value, uint8_t *buffer);

// Precompiler macros
#define tbx_varint_need_more(B) ((B) & 0x80)

#ifdef __cplusplus
}
#endif

#endif
