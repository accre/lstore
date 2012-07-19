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

#define _log_module_index 127

#include <stdlib.h>
#include "log.h"
#include "callback.h"

//*************************************************************

void callback_set(callback_t *cb, void (*fn)(void *, int), void *priv)
{
   cb->priv = priv;
   cb->fn = fn;
   cb->next = NULL;
   cb->tail = NULL;
}

//*************************************************************
// callback_append - Append the callback to the tail
//*************************************************************

void callback_append(callback_t **root_cb, callback_t *cb)
{
   callback_t *last;
   if (cb == NULL) return;  //** Nothing to add so exit
   
   if (*root_cb == NULL) {
      (*root_cb) = cb;
      cb->next = NULL;
      cb->tail = cb;
   } else {
      last = (*root_cb)->tail;
      last->next = cb;
      (*root_cb)->tail = cb;
      cb->next = NULL;
   }

}

//*************************************************************

void callback_destroy(callback_t *root_cb)
{
   callback_t *cb;

//log_printf(15, "callback_destroy:  START root_cb=%d\n", root_cb);
   while (root_cb != NULL) {
      cb = root_cb;
      root_cb = root_cb->next;
//log_printf(15, "callback_destroy:  freeing cb=%p\n", cb);
      free(cb);
   }
//log_printf(15, "callback_destroy:  END\n");
}

//*************************************************************

void callback_single_execute(callback_t *cb, int value)
{
  if (cb != NULL) {
     if (cb->fn != NULL) {
        cb->fn(cb->priv, value);
     }
  }
}

//*************************************************************
//  callback_execute - Exectutes all cb's in the order they were added
//*************************************************************

void callback_execute(callback_t *root_cb, int value)
{
  callback_t *cb_next;
  callback_t *cb = root_cb;

  while (cb != NULL) {
     cb_next = cb->next;  //** Need this to handle the opque_free() race condition of the current cb
log_printf(15, " cb_exec:  cb=%p cb->next=%p\n", cb, cb_next);
     callback_single_execute(cb, value);
     cb = cb_next;
  }
}


