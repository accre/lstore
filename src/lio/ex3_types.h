/*
Advanced Computing Center for Research and Education Proprietary License
Version 1.0 (April 2006)

Copyright (c) 2006, Advanced Computing Center for Research and Education,
 Vanderbilt University, All rights reserved.

This Work is the sole and exclusive property of the Advanced Computing Center
for Research and Education department at Vanderbilt University.  No right to
disclose or otherwise disseminate any of the information contained herein is
granted by virtue of your possession of this software except in accordance with
the terms and conditions of a separate License Agreement entered into with
Vanderbilt University.

THE AUTHOR OR COPYRIGHT HOLDERS PROVIDES THE "WORK" ON AN "AS IS" BASIS,
WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, TITLE, FITNESS FOR A PARTICULAR
PURPOSE, AND NON-INFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Vanderbilt University
Advanced Computing Center for Research and Education
230 Appleton Place
Nashville, TN 37203
http://www.accre.vanderbilt.edu
*/ 

//***********************************************************************
// Exnode3 abstract class
//***********************************************************************

#ifndef _EX3_TYPES_H_
#define _EX3_TYPES_H_

#include <inttypes.h>

#ifdef __cplusplus
extern "C" {
#endif

#define XIDT "%" PRIu64    //uint64_t
#define XOT  "%" PRId64    //int64_t
#define XOTC PRId64


typedef int64_t ex_off_t;
typedef uint64_t ex_id_t;

typedef struct {    //** I/O Vec array
  ex_off_t offset;
  ex_off_t len;
} ex_iovec_t;

#define ex_iovec_single(iov, oset, nbytes) (iov)->offset = oset; (iov)->len = nbytes
ex_iovec_t *ex_iovec_create();
void ex_iovec_destroy(ex_iovec_t *iov);

typedef struct {
  int type;
  union {
    char *text;
  };
} exnode_exchange_t;

#ifdef __cplusplus
}
#endif

#endif

