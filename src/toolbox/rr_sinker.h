/*
Advanced Computing Center for Research and Education Proprietary License
Version 1.0 (September 2012)

Copyright (c) 2012, Advanced Computing Center for Research and Education,
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

//*************************************************************************
//*************************************************************************

#ifndef _RR_SINKER_H_
#define _RR_SINKER_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "rr_base.h"

//** User callback function to process results
typedef rrtask_data_t *(sinker_task_fn)(rrtask_data_t *input);

typedef struct {
    zctx_t *ctx;
    void *sinker;
    void *pipe;
    char *pattern;
    char *endpoint;
    sinker_task_fn *cb;
} rrsinker_t;

rrsinker_t *rrsinker_new();
void rrsinker_destroy(rrsinker_t **self_p);
void rrsinker_start(rrsinker_t *self, sinker_task_fn *cb);
void rrsinker_task_manager(void *args, zctx_t *ctx, void *pipe);
void rrsinker_load_config(rrsinker_t *self, char *fname);

#ifdef __cplusplus
}
#endif

#endif
