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

#include <tbx/toolbox_visibility.h>

#pragma once
#ifndef ACCRE_TBX_DECL_H_INCLUDED
#define ACCRE_TBX_DECL_H_INCLUDED

/*!
 * @brief Declares a toolbox type with its default contructors/destructors
 * @param TYPENAME Type name with trailing '_t', useful for searching codebase
 * @param FUNCNAME Type name without trailing '_t'
 */
#define TBX_TYPE(TYPENAME, FUNCNAME) \
    /*! Opaque definition of TYPENAME \
     */ \
    typedef struct TYPENAME TYPENAME; \
    TBX_API TBX_MALLOC TYPENAME * FUNCNAME ## _new(); \
    TBX_API void FUNCNAME ## _del(); \
    TBX_API TBX_NONNULL int FUNCNAME ## _init(TYPENAME * self); \
    TBX_API void FUNCNAME ## _fini(TYPENAME * self); \
    TBX_API size_t FUNCNAME ## _size()

#define TBX_TYPE_INIT(TYPENAME, FUNCNAME, ...) \
    TBX_API TBX_NONNULL_SOME(1) int FUNCNAME(__VA_ARGS__)

#define TBX_TYPE_NEW(TYPENAME, FUNCNAME, ...) \
    TBX_API TBX_MALLOC TYPENAME * FUNCNAME(__VA_ARGS__)


#ifdef __GNUC__
#   define TBX_NONNULL __attribute__((nonnull))
#   define TBX_NONNULL_SOME(...) __attribute__((nonnull(__VA_ARGS__)))
#   define TBX_MALLOC __attribute__((malloc))
#else
#   error "Does this platform support function attributes?"
#endif

#endif
