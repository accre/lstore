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

//*************************************************************
//  Generic Callback implementation
//*************************************************************

#include <stdarg.h>

#ifndef __CALLBACK_H_
#define __CALLBACK_H_

#ifdef __cplusplus
extern "C" {
#endif

struct callback_s {   //** Used for application level callback
    void *priv;
    void (*fn)(void *priv, int value);
    struct callback_s *next;
    struct callback_s *tail;
};

typedef struct callback_s callback_t;

void callback_set(callback_t *cb, void (*fn)(void *priv, int value), void *priv);
void callback_append(callback_t **root_cb, callback_t *cb);
void callback_destroy(callback_t *root_cb);
void callback_execute(callback_t *cb, int value);
void callback_single_execute(callback_t *cb, int value);

#ifdef __cplusplus
}
#endif


#endif

