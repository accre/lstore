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

#define _log_module_index 127

#include <stdlib.h>
#include <tbx/log.h>
#include "callback.h"

//*************************************************************

void gop_cb_set(callback_t *cb, void (*fn)(void *, int), void *priv)
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
        cb_next = cb->next;  //** Need this to handle the gop_opque_free() race condition of the current cb
        log_printf(15, " cb_exec:  cb=%p cb->next=%p\n", cb, cb_next);
        callback_single_execute(cb, value);
        cb = cb_next;
    }
}


