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
#ifndef ACCRE_PACKER_H_INCLUDED
#define ACCRE_PACKER_H_INCLUDED

#include "tbx/toolbox_visibility.h"

#ifdef __cplusplus
extern "C" {
#endif

// Types
typedef struct tbx_pack_raw_t tbx_pack_raw_t;

typedef struct tbx_pack_t tbx_pack_t;

typedef struct tbx_pack_zlib_t tbx_pack_zlib_t;

// Functions
TBX_API void tbx_pack_consumed(tbx_pack_t *pack);
TBX_API tbx_pack_t *tbx_pack_create(int type, int mode, unsigned char *buffer, unsigned int bufsize);
TBX_API void tbx_pack_destroy(tbx_pack_t *pack);
TBX_API int tbx_pack_read(tbx_pack_t *pack, unsigned char *data, int nbytes);
TBX_API int tbx_pack_read_new_data(tbx_pack_t *pack, unsigned char *buffer, unsigned int bufsize);
TBX_API int tbx_pack_used(tbx_pack_t *pack);
TBX_API int tbx_pack_write(tbx_pack_t *pack, unsigned char *data, int nbytes);
TBX_API int tbx_pack_write_flush(tbx_pack_t *pack);
TBX_API void tbx_pack_write_resized(tbx_pack_t *pack, unsigned char *buffer, unsigned int bufsize);

// Preprocessor Macros
#define PACK_FINISHED -3
#define PACK_ERROR   -2
#define PACK_FULL    -1
#define PACK_NONE     0
#define PACK_COMPRESS 1
#define PACK_READ     0
#define PACK_WRITE    1

#define tbx_pack_end(p) (p)->end(p)

#ifdef __cplusplus
}
#endif

#endif
