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
// Routines for managing a data block
//***********************************************************************

#define _log_module_index 144

#include <stdlib.h>
#include "ex3_abstract.h"
#include "service_manager.h"
#include "data_service_abstract.h"
#include "interval_skiplist.h"
#include "append_printf.h"
#include "string_token.h"
#include "type_malloc.h"
#include "iniparse.h"
#include "log.h"
#include "random.h"

//***********************************************************************
// db_find_key - Scans the stack for the key
//***********************************************************************

data_block_attr_t *db_find_key(Stack_t *stack, char *key)
{
    data_block_attr_t *attr;

    if (stack == NULL) return(NULL);

    move_to_top(stack);
    while ((attr = (data_block_attr_t *)get_ele_data(stack)) != NULL) {
        if (strcmp(attr->key, key) == 0) {
            return(attr);
        }
        move_down(stack);
    }

    return(NULL);
}

//***********************************************************************
// data_block_auto_warm - Adds the data block to the auto warm list
//***********************************************************************

void data_block_auto_warm(data_block_t *b)
{
    b->warm = ds_cap_auto_warm(b->ds, b->cap);
}

//***********************************************************************
// data_block_stop_warm - Disables the data block from being warmed
//***********************************************************************

void data_block_stop_warm(data_block_t *b)
{
    if (b->warm == NULL) return;

    ds_cap_stop_warm(b->ds, b->warm);
    b->warm = NULL;
}

//***********************************************************************
// data_block_set_attr - Sets a data block attribute
//***********************************************************************

int data_block_set_attr(data_block_t *b, char *key, char *val)
{
    data_block_attr_t *attr;

    //** See if the key exists
    attr = db_find_key(b->attr_stack, key);

    if ((attr == NULL) && (val != NULL)) {
        type_malloc_clear(attr, data_block_attr_t, 1);
        attr->key = strdup(key);
    }

    if (attr->value != NULL) free(attr->value);
    if (val != NULL) attr->value = strdup(val);  //** IF val == NULL then this will never get returned and on deserial it will get dropped

    if (b->attr_stack == NULL) b->attr_stack = new_stack();
    push(b->attr_stack, attr);

    return(0);
}

//***********************************************************************
// data_block_get_attr - Gets a data block attribute or returns NULL if not found
//***********************************************************************

char *data_block_get_attr(data_block_t *b, char *key)
{
    data_block_attr_t *attr;

    //** See if the key exists
    attr = db_find_key(b->attr_stack, key);

    if (attr == NULL) {
        return(NULL);
    }

    return(attr->value);
}

//***********************************************************************
//  data_block_serialize_text - Stores the data block in the exnode
//***********************************************************************


int data_block_serialize_text(data_block_t *b, exnode_exchange_t *exp)
{
    static int bufsize=2048;
    char capsbuf[bufsize];
    char *etext, *ekey;
    int cused, refcount;
    data_block_attr_t *attr;

    cused = 0;

    append_printf(capsbuf, &cused, bufsize, "[block-" XIDT "]\n", b->id);
    append_printf(capsbuf, &cused, bufsize, "type=%s\n", ds_type(b->ds));
    append_printf(capsbuf, &cused, bufsize, "rid_key=%s\n", b->rid_key);
    append_printf(capsbuf, &cused, bufsize, "size=" XOT "\n", b->size);
    append_printf(capsbuf, &cused, bufsize, "max_size=" XOT "\n", b->max_size);
    refcount = atomic_get(b->ref_count);
    append_printf(capsbuf, &cused, bufsize, "ref_count=%d\n", refcount);

    etext = escape_text("=#[]", '\\', ds_get_cap(b->ds, b->cap, DS_CAP_READ));
    append_printf(capsbuf, &cused, bufsize, "read_cap=%s\n", etext);
    free(etext);
    etext = escape_text("=#[]", '\\', ds_get_cap(b->ds, b->cap, DS_CAP_WRITE));
    append_printf(capsbuf, &cused, bufsize, "write_cap=%s\n", etext);
    free(etext);
    etext = escape_text("=#[]", '\\', ds_get_cap(b->ds, b->cap, DS_CAP_MANAGE));
    append_printf(capsbuf, &cused, bufsize, "manage_cap=%s\n", etext);
    free(etext);

    if (b->attr_stack != NULL) {  //** Deserialze the other attributes
        while ((attr = (data_block_attr_t *)pop(b->attr_stack)) != NULL) {
            if (attr->value != NULL) {
                ekey = escape_text("=#[]", '\\', attr->key);
                etext = escape_text("=#[]", '\\', attr->value);
                append_printf(capsbuf, &cused, bufsize, "%s=%s\n", ekey, etext);
                free(etext);
                free(ekey);
                free(attr->value);
            }

            free(attr->key);
            free(attr);
        }
    }

    append_printf(capsbuf, &cused, bufsize, "\n");

    //** Merge everything together and return it
    exnode_exchange_t cexp;
    cexp.text.text = capsbuf;
    exnode_exchange_append(exp, &cexp);

    return(0);
}

//***********************************************************************
// data_block_serialize_proto -Convert the data block to a protocol buffer
//***********************************************************************

int data_block_serialize_proto(data_block_t *b, exnode_exchange_t *exp)
{
    return(-1);
}

//***********************************************************************
// data_block_serialize -Convert the data block to a more portable format
//***********************************************************************

int data_block_serialize(data_block_t *d, exnode_exchange_t *exp)
{
    if (exp->type == EX_TEXT) {
        return(data_block_serialize_text(d, exp));
    } else if (exp->type == EX_PROTOCOL_BUFFERS) {
        return(data_block_serialize_proto(d, exp));
    }

    return(-1);
}

//***********************************************************************
// data_block_deserialize_text -Read the text based data block
//***********************************************************************

data_block_t *data_block_deserialize_text(service_manager_t *sm, ex_id_t id, exnode_exchange_t *exp)
{
    int bufsize=1024;
    char capgrp[bufsize];
    char *text, *etext;
    int i;
    data_block_t *b;
    data_service_fn_t *ds;
    inip_file_t *cfd;
    inip_group_t *cg;
    inip_element_t *ele;
    char *key;
    data_block_attr_t *attr;

    //** Parse the ini text
    cfd = exp->text.fd;

    //** Find the cooresponding cap
    snprintf(capgrp, bufsize, "block-" XIDT, id);
    cg = inip_find_group(cfd, capgrp);
    if (cg == NULL) {
        log_printf(0, "data_block_deserialize_text: id=" XIDT " not found!\n", id);
        return(NULL);
    }

    //** Determine the type and make a blank block
    text = inip_get_string(cfd, capgrp, "type", "");
    ds = lookup_service(sm, DS_SM_RUNNING, text);
    if (ds == NULL) {
        log_printf(0, "data_block_deserialize_text: b->id=" XIDT " Unknown data service tpye=%s!\n", id, text);
        return(NULL);;
    }
    free(text);

    //** Make the space
    type_malloc_clear(b, data_block_t, 1);
    b->id = id;
    b->ds = ds;
    b->cap = ds_cap_set_create(b->ds);

    //** and parse the fields
    b->rid_key = inip_get_string(cfd, capgrp, "rid_key", "");
    b->size = inip_get_integer(cfd, capgrp, "size", b->size);
    b->max_size = inip_get_integer(cfd, capgrp, "max_size", b->max_size);
    i = inip_get_integer(cfd, capgrp, "ref_count", b->ref_count);
    atomic_set(b->ref_count, 0);
    atomic_set(b->initial_ref_count, i);
    etext = inip_get_string(cfd, capgrp, "read_cap", "");
    ds_set_cap(b->ds, b->cap, DS_CAP_READ, unescape_text('\\', etext));
    free(etext);
    etext = inip_get_string(cfd, capgrp, "write_cap", "");
    ds_set_cap(b->ds, b->cap, DS_CAP_WRITE, unescape_text('\\', etext));
    free(etext);
    etext = inip_get_string(cfd, capgrp, "manage_cap", "");
    ds_set_cap(b->ds, b->cap, DS_CAP_MANAGE, unescape_text('\\', etext));
    free(etext);

    //** Now cycle through any misc attributes set
    ele = inip_first_element(inip_find_group(cfd, capgrp));
    while (ele != NULL) {
        key = inip_get_element_key(ele);

        //** Ignore the builtin commands
        if ((strcmp("rid_key", key) != 0) && (strcmp("size", key) != 0) && (strcmp("max_size", key) != 0) && (strcmp("type", key) != 0) &&
                (strcmp("ref_count", key) != 0) && (strcmp("read_cap", key) != 0) && (strcmp("write_cap", key) != 0) && (strcmp("manage_cap", key) != 0)) {
            type_malloc(attr, data_block_attr_t, 1);
            attr->key = unescape_text('\\', inip_get_element_key(ele));
            attr->value = unescape_text('\\', inip_get_element_value(ele));
            if (b->attr_stack == NULL) b->attr_stack = new_stack();
            push(b->attr_stack, attr);
        }

        ele = inip_next_element(ele);
    }

    return(b);
}

//***********************************************************************
// data_block_deserialize_proto - Read the prot formatted data block
//***********************************************************************

data_block_t *data_block_deserialize_proto(service_manager_t *sm, ex_id_t id, exnode_exchange_t *exp)
{
    return(NULL);
}

//***********************************************************************
// data_block_deserialize -Convert from the portable to internal format
//***********************************************************************

data_block_t *data_block_deserialize(service_manager_t *sm, ex_id_t id, exnode_exchange_t *exp)
{
    if (exp->type == EX_TEXT) {
        return(data_block_deserialize_text(sm, id, exp));
    } else if (exp->type == EX_PROTOCOL_BUFFERS) {
        return(data_block_deserialize_proto(sm, id, exp));
    }

    return(NULL);
}

//***********************************************************************
// data_block_create - Creates an empty data block
//***********************************************************************

data_block_t *data_block_create(data_service_fn_t *ds)
{
    data_block_t *b;

    type_malloc_clear(b, data_block_t, 1);

    b->ds = ds;
    b->cap = ds_cap_set_create(b->ds);
    generate_ex_id(&(b->id));

    log_printf(15, "b->id=" XIDT " ref_count=%d b=%p\n", b->id, b->ref_count, b);

    return(b);
}

//***********************************************************************
// data_block_destroy - Destroys the data block struct (not the data)
//***********************************************************************

void data_block_destroy(data_block_t *b)
{
    if (b == NULL) return;

    log_printf(15, "b->id=" XIDT " ref_count=%d\n", b->id, b->ref_count);

    if (b->ref_count > 0) return;

    if (b->attr_stack != NULL) free_stack(b->attr_stack, 1);

    ds_cap_set_destroy(b->ds, b->cap, 1);
    if (b->rid_key != NULL) free(b->rid_key);
    log_printf(15, "b->id=" XIDT " ref_count=%d p=%p\n", b->id, b->ref_count, b);

    if (b->warm != NULL) data_block_stop_warm(b);

    free(b);
}





