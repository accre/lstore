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
// Routines for managing a data block
//***********************************************************************

#define _log_module_index 144

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/append_printf.h>
#include <tbx/assert_result.h>
#include <tbx/atomic_counter.h>
#include <tbx/iniparse.h>
#include <tbx/log.h>
#include <tbx/stack.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>

#include "data_block.h"
#include "ds.h"
#include "ex3.h"
#include "ex3/types.h"
#include "service_manager.h"

//***********************************************************************
// db_find_key - Scans the stack for the key
//***********************************************************************

lio_data_block_attr_t *db_find_key(tbx_stack_t *stack, char *key)
{
    lio_data_block_attr_t *attr;

    if (stack == NULL) return(NULL);

    tbx_stack_move_to_top(stack);
    while ((attr = (lio_data_block_attr_t *)tbx_stack_get_current_data(stack)) != NULL) {
        if (strcmp(attr->key, key) == 0) {
            return(attr);
        }
        tbx_stack_move_down(stack);
    }

    return(NULL);
}

//***********************************************************************
// data_block_auto_warm - Adds the data block to the auto warm list
//***********************************************************************

void data_block_auto_warm(lio_data_block_t *b)
{
    b->warm = ds_cap_auto_warm(b->ds, b->cap);
}

//***********************************************************************
// data_block_stop_warm - Disables the data block from being warmed
//***********************************************************************

void data_block_stop_warm(lio_data_block_t *b)
{
    if (b->warm == NULL) return;

    ds_cap_stop_warm(b->ds, b->warm);
    b->warm = NULL;
}

//***********************************************************************
// data_block_set_attr - Sets a data block attribute
//***********************************************************************

int data_block_set_attr(lio_data_block_t *b, char *key, char *val)
{
    lio_data_block_attr_t *attr;

    //** See if the key exists
    attr = db_find_key(b->attr_stack, key);

    if (attr == NULL) {  //** See if we need to add the attribute
        tbx_type_malloc_clear(attr, lio_data_block_attr_t, 1);
        attr->key = strdup(key);
    }

    if (attr->value != NULL) free(attr->value);  //** Free the old value
    attr->value = (val != NULL) ? strdup(val) : NULL;  //** Store the new one

    if (b->attr_stack == NULL) b->attr_stack = tbx_stack_new();
    tbx_stack_push(b->attr_stack, attr);

    return(0);
}

//***********************************************************************
// data_block_get_attr - Gets a data block attribute or returns NULL if not found
//***********************************************************************

char *data_block_get_attr(lio_data_block_t *b, char *key)
{
    lio_data_block_attr_t *attr;

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


int data_block_serialize_text(lio_data_block_t *b, lio_exnode_exchange_t *exp)
{
    static int bufsize=2048;
    char capsbuf[bufsize];
    char *etext, *ekey;
    int cused, refcount;
    lio_data_block_attr_t *attr;

    cused = 0;

    tbx_append_printf(capsbuf, &cused, bufsize, "[block-" XIDT "]\n", b->id);
    tbx_append_printf(capsbuf, &cused, bufsize, "type=%s\n", ds_type(b->ds));
    tbx_append_printf(capsbuf, &cused, bufsize, "rid_key=%s\n", b->rid_key);
    tbx_append_printf(capsbuf, &cused, bufsize, "size=" XOT "\n", b->size);
    tbx_append_printf(capsbuf, &cused, bufsize, "max_size=" XOT "\n", b->max_size);
    refcount = tbx_atomic_get(b->ref_count);
    tbx_append_printf(capsbuf, &cused, bufsize, "ref_count=%d\n", refcount);

    etext = tbx_stk_escape_text("=#[]", '\\', ds_get_cap(b->ds, b->cap, DS_CAP_READ));
    tbx_append_printf(capsbuf, &cused, bufsize, "read_cap=%s\n", etext);
    free(etext);
    etext = tbx_stk_escape_text("=#[]", '\\', ds_get_cap(b->ds, b->cap, DS_CAP_WRITE));
    tbx_append_printf(capsbuf, &cused, bufsize, "write_cap=%s\n", etext);
    free(etext);
    etext = tbx_stk_escape_text("=#[]", '\\', ds_get_cap(b->ds, b->cap, DS_CAP_MANAGE));
    tbx_append_printf(capsbuf, &cused, bufsize, "manage_cap=%s\n", etext);
    free(etext);

    if (b->attr_stack != NULL) {  //** Deserialze the other attributes
        while ((attr = (lio_data_block_attr_t *)tbx_stack_pop(b->attr_stack)) != NULL) {
            if (attr->value != NULL) {
                ekey = tbx_stk_escape_text("=#[]", '\\', attr->key);
                etext = tbx_stk_escape_text("=#[]", '\\', attr->value);
                tbx_append_printf(capsbuf, &cused, bufsize, "%s=%s\n", ekey, etext);
                free(etext);
                free(ekey);
                free(attr->value);
            }

            free(attr->key);
            free(attr);
        }
    }

    tbx_append_printf(capsbuf, &cused, bufsize, "\n");

    //** Merge everything together and return it
    lio_exnode_exchange_t cexp;
    cexp.text.text = capsbuf;
    exnode_exchange_append(exp, &cexp);

    return(0);
}

//***********************************************************************
// data_block_serialize_proto -Convert the data block to a protocol buffer
//***********************************************************************

int data_block_serialize_proto(lio_data_block_t *b, lio_exnode_exchange_t *exp)
{
    return(-1);
}

//***********************************************************************
// data_block_serialize -Convert the data block to a more portable format
//***********************************************************************

int data_block_serialize(lio_data_block_t *d, lio_exnode_exchange_t *exp)
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

lio_data_block_t *data_block_deserialize_text(lio_service_manager_t *sm, ex_id_t id, lio_exnode_exchange_t *exp)
{
    int bufsize=1024;
    char capgrp[bufsize];
    char *text, *etext;
    int i;
    lio_data_block_t *b;
    lio_data_service_fn_t *ds;
    tbx_inip_file_t *cfd;
    tbx_inip_group_t *cg;
    tbx_inip_element_t *ele;
    char *key;
    lio_data_block_attr_t *attr;

    //** Parse the ini text
    cfd = exp->text.fd;

    //** Find the cooresponding cap
    snprintf(capgrp, bufsize, "block-" XIDT, id);
    cg = tbx_inip_group_find(cfd, capgrp);
    if (cg == NULL) {
        log_printf(0, "data_block_deserialize_text: id=" XIDT " not found!\n", id);
        return(NULL);
    }

    //** Determine the type and make a blank block
    text = tbx_inip_get_string(cfd, capgrp, "type", "");
    ds = lio_lookup_service(sm, DS_SM_RUNNING, text);
    if (ds == NULL) {
        log_printf(0, "data_block_deserialize_text: b->id=" XIDT " Unknown data service tpye=%s!\n", id, text);
        return(NULL);;
    }
    free(text);

    //** Make the space
    tbx_type_malloc_clear(b, lio_data_block_t, 1);
    b->id = id;
    b->ds = ds;
    b->cap = ds_cap_set_create(b->ds);

    //** and parse the fields
    b->rid_key = tbx_inip_get_string(cfd, capgrp, "rid_key", "");
    b->size = tbx_inip_get_integer(cfd, capgrp, "size", b->size);
    b->max_size = tbx_inip_get_integer(cfd, capgrp, "max_size", b->max_size);
    i = tbx_inip_get_integer(cfd, capgrp, "ref_count", b->ref_count);
    tbx_atomic_set(b->ref_count, 0);
    tbx_atomic_set(b->initial_ref_count, i);
    etext = tbx_inip_get_string(cfd, capgrp, "read_cap", "");
    ds_set_cap(b->ds, b->cap, DS_CAP_READ, tbx_stk_unescape_text('\\', etext));
    free(etext);
    etext = tbx_inip_get_string(cfd, capgrp, "write_cap", "");
    ds_set_cap(b->ds, b->cap, DS_CAP_WRITE, tbx_stk_unescape_text('\\', etext));
    free(etext);
    etext = tbx_inip_get_string(cfd, capgrp, "manage_cap", "");
    ds_set_cap(b->ds, b->cap, DS_CAP_MANAGE, tbx_stk_unescape_text('\\', etext));
    free(etext);

    //** Now cycle through any misc attributes set
    ele = tbx_inip_ele_first(tbx_inip_group_find(cfd, capgrp));
    while (ele != NULL) {
        key = tbx_inip_ele_get_key(ele);

        //** Ignore the builtin commands
        if ((strcmp("rid_key", key) != 0) && (strcmp("size", key) != 0) && (strcmp("max_size", key) != 0) && (strcmp("type", key) != 0) &&
                (strcmp("ref_count", key) != 0) && (strcmp("read_cap", key) != 0) && (strcmp("write_cap", key) != 0) && (strcmp("manage_cap", key) != 0)) {
            tbx_type_malloc(attr, lio_data_block_attr_t, 1);
            attr->key = tbx_stk_unescape_text('\\', tbx_inip_ele_get_key(ele));
            attr->value = tbx_stk_unescape_text('\\', tbx_inip_ele_get_value(ele));
            if (b->attr_stack == NULL) b->attr_stack = tbx_stack_new();
            tbx_stack_push(b->attr_stack, attr);
        }

        ele = tbx_inip_ele_next(ele);
    }

    return(b);
}

//***********************************************************************
// data_block_deserialize_proto - Read the prot formatted data block
//***********************************************************************

lio_data_block_t *data_block_deserialize_proto(lio_service_manager_t *sm, ex_id_t id, lio_exnode_exchange_t *exp)
{
    return(NULL);
}

//***********************************************************************
// data_block_deserialize -Convert from the portable to internal format
//***********************************************************************

lio_data_block_t *data_block_deserialize(lio_service_manager_t *sm, ex_id_t id, lio_exnode_exchange_t *exp)
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

lio_data_block_t *data_block_create(lio_data_service_fn_t *ds)
{
    lio_data_block_t *b;

    tbx_type_malloc_clear(b, lio_data_block_t, 1);

    b->ds = ds;
    b->cap = ds_cap_set_create(b->ds);
    generate_ex_id(&(b->id));

    log_printf(15, "b->id=" XIDT " ref_count=%d b=%p\n", b->id, b->ref_count, b);

    return(b);
}

//***********************************************************************
// data_block_destroy - Destroys the data block struct (not the data)
//***********************************************************************

void data_block_destroy(lio_data_block_t *b)
{
    if (b == NULL) return;

    log_printf(15, "b->id=" XIDT " ref_count=%d\n", b->id, b->ref_count);

    if (b->ref_count > 0) return;

    if (b->attr_stack != NULL) tbx_stack_free(b->attr_stack, 1);

    ds_cap_set_destroy(b->ds, b->cap, 1);
    if (b->rid_key != NULL) free(b->rid_key);
    log_printf(15, "b->id=" XIDT " ref_count=%d p=%p\n", b->id, b->ref_count, b);

    if (b->warm != NULL) data_block_stop_warm(b);

    free(b);
}





