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
//  Routines to provide frame support for MQ layer
//*************************************************************

#include "mq_portal.h"
#include "type_malloc.h"
#include "log.h"
#include <stdlib.h>

//**************************************************************
//  mq_get_frame - Returns the frame data
//**************************************************************

int mq_get_frame(mq_frame_t *f, void **data, int *size)
{
    if (f == NULL) {
        *data = NULL;
        *size = 0;
        return(0);
    }

    *size = f->len;
    *data = f->data;

    return(f->auto_free);
}

//*************************************************************
//  mq_frame_strdup - Converts the contents ofthe frame to a NULL
//    terminated string and returns the pointer.  The
//    caller is responsible for freeing the data.
//*************************************************************

char *mq_frame_strdup(mq_frame_t *f)
{
    char *data, *str;
    int n;

    mq_get_frame(f, (void **)&data, &n);

    if (data == NULL) return(NULL);

    type_malloc(str, char, n+1);
    str[n] = 0;
    memcpy(str, data, n);

    return(str);
}

//*************************************************************
// quick stack related msg routines
//*************************************************************

mq_msg_t *mq_msg_new()
{
    return(new_stack());
}
mq_frame_t *mq_msg_first(mq_msg_t *msg)
{
    move_to_top(msg);
    return((mq_frame_t *)get_ele_data(msg));
}
mq_frame_t *mq_msg_last(mq_msg_t *msg)
{
    move_to_bottom(msg);
    return((mq_frame_t *)get_ele_data(msg));
}
mq_frame_t *mq_msg_next(mq_msg_t *msg)
{
    move_down(msg);
    return((mq_frame_t *)get_ele_data(msg));
}
mq_frame_t *mq_msg_prev(mq_msg_t *msg)
{
    move_up(msg);
    return((mq_frame_t *)get_ele_data(msg));
}
mq_frame_t *mq_msg_current(mq_msg_t *msg)
{
    return((mq_frame_t *)get_ele_data(msg));
}
mq_frame_t *mq_msg_pluck(mq_msg_t *msg, int move_up)
{
    mq_frame_t *f = get_ele_data(msg);
    delete_current(msg, move_up, 0);
    return(f);
}
void mq_msg_insert_above(mq_msg_t *msg, mq_frame_t *f)
{
    insert_above(msg, f);
}
void mq_msg_insert_below(mq_msg_t *msg, mq_frame_t *f)
{
    insert_below(msg, f);
}
void mq_msg_push_frame(mq_msg_t *msg, mq_frame_t *f)
{
    push(msg, f);
}
void mq_msg_append_frame(mq_msg_t *msg, mq_frame_t *f)
{
    move_to_bottom(msg);
    insert_below(msg, f);
}

void mq_frame_set(mq_frame_t *f, void *data, int len, int auto_free)
{
    f->data = data;
    f->len = len;
    f->auto_free = auto_free;
}

mq_frame_t *mq_frame_new(void *data, int len, int auto_free)
{
    mq_frame_t *f;

    type_malloc(f, mq_frame_t, 1);
    mq_frame_set(f, data, len, auto_free);

    return(f);
}

mq_frame_t *mq_frame_dup(mq_frame_t *f)
{
    void *data, *copy;
    int size;

    mq_get_frame(f, &data, &size);
    if (size == 0) {
        copy = NULL;
    } else {
        type_malloc(copy, void, size);
        memcpy(copy, data, size);
    }

    return(mq_frame_new(copy, size, MQF_MSG_AUTO_FREE));
}

void mq_frame_destroy(mq_frame_t *f)
{
    if ((f->auto_free == MQF_MSG_AUTO_FREE) && (f->data)) {
        free(f->data);
    } else if (f->auto_free == MQF_MSG_INTERNAL_FREE) {
        zmq_msg_close(&(f->zmsg));
    }
    free(f);
}

void mq_msg_destroy(mq_msg_t *msg)
{
    mq_frame_t *f;

    while ((f = pop(msg)) != NULL) {
        mq_frame_destroy(f);
    }

    free_stack(msg, 0);
}

void mq_msg_push_mem(mq_msg_t *msg, void *data, int len, int auto_free)
{
    push(msg, mq_frame_new(data, len, auto_free));
}
void mq_msg_append_mem(mq_msg_t *msg, void *data, int len, int auto_free)
{
    move_to_bottom(msg);
    insert_below(msg, mq_frame_new(data, len, auto_free));
}

void mq_msg_append_msg(mq_msg_t *msg, mq_msg_t *extra, int mode)
{
    Stack_ele_t *curr;
    mq_frame_t *f;
    char *data;

    move_to_top(msg);
    for (curr = extra->top; curr != NULL; curr = curr->down) {
        f = (mq_frame_t *)curr->data;
        if (mode == MQF_MSG_AUTO_FREE) {
            type_malloc(data, char, f->len);
            memcpy(data, f->data, f->len);
            insert_below(msg, mq_frame_new(data, f->len, MQF_MSG_AUTO_FREE));
        } else {
            insert_below(msg, mq_frame_new(f->data, f->len, MQF_MSG_KEEP_DATA));
        }
    }
}

mq_msg_hash_t mq_msg_hash(mq_msg_t *msg)
{
    Stack_ele_t *curr;
    mq_frame_t *f;
    unsigned char *data;
    unsigned char *p;
    mq_msg_hash_t h;
    int size, n;

    n = 0;
    h.full_hash = h.even_hash = 0;
    for (curr = msg->top; curr != NULL; curr = curr->down) {
        f = (mq_frame_t *)curr->data;
        mq_get_frame(f, (void **)&data, &size);
        for (p = data; size > 0; p++, size--) {
            h.full_hash = h.full_hash * 33 + *p;
            if ((n%2) == 0) h.even_hash = h.even_hash * 33 + *p;

            n++;
        }
    }

    return(h);
}

//*************************************************************
// mq_msg_total_size - Total size of mesg
//*************************************************************

int mq_msg_total_size(mq_msg_t *msg)
{
    mq_frame_t *f;
    int n;

    n = 0;
    move_to_top(msg);
    while ((f = get_ele_data(msg)) != NULL) {
        n += f->len;
        move_down(msg);
    }

    return(n);
}
