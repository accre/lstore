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
// Exnode3 abstract class
//***********************************************************************

#ifndef _EX3_TYPES_H_
#define _EX3_TYPES_H_

#include <inttypes.h>

#include "ibp.h"

#ifdef __cplusplus
extern "C" {
#endif

#define XIDT "%" PRIu64    //uint64_t
#define XOT  "%" PRId64    //int64_t
#define PXOT     PRId64    // Drop the % for formatting ..int64_t
#define XOTC PRId64


typedef int64_t ex_off_t;
typedef uint64_t ex_id_t;

typedef ibp_tbx_iovec_t ex_tbx_iovec_t;


#define ex_iovec_single(iov, oset, nbytes) (iov)->offset = oset; (iov)->len = nbytes
ex_tbx_iovec_t *ex_iovec_create();
void ex_iovec_destroy(ex_tbx_iovec_t *iov);

typedef struct exnode_text_t exnode_text_t;
struct exnode_text_t {
    char *text;
    tbx_inip_file_t *fd;
};

typedef struct exnode_exchange_t exnode_exchange_t;
struct exnode_exchange_t {
    int type;
    exnode_text_t text;
};

#ifdef __cplusplus
}
#endif

#endif

