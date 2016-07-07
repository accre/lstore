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

#define _log_module_index 151

#include <assert.h>
#include <gop/gop.h>
#include <gop/opque.h>
#include <gop/tp.h>
#include <gop/types.h>
#include <lio/segment.h>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <tbx/append_printf.h>
#include <tbx/atomic_counter.h>
#include <tbx/iniparse.h>
#include <tbx/list.h>
#include <tbx/log.h>
#include <tbx/skiplist.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>

#include "data_block.h"
#include "ds.h"
#include "ex3.h"
#include "ex3/compare.h"
#include "ex3/header.h"
#include "ex3/types.h"
#include "service_manager.h"

typedef struct {
    lio_exnode_t *src_ex;
    lio_exnode_t *dest_ex;
    data_attr_t *da;
    void *arg;
    int mode;
    int timeout;
} lio_exnode_clone_t;

//*************************************************************************
// ex_iovec_create
//*************************************************************************

ex_tbx_iovec_t *ex_iovec_create()
{
    ex_tbx_iovec_t *iov;
    tbx_type_malloc_clear(iov, ex_tbx_iovec_t, 1);
    return(iov);
}

//*************************************************************************
// ex_iovec_destroy
//*************************************************************************

void ex_iovec_destroy(ex_tbx_iovec_t *iov)
{
    free(iov);
}

//*************************************************************************
// exnode_[g|s]et_default - Retreives/Sets the default segment
//
//  NOTE: The default segment should also be placed in the view list!
//*************************************************************************

lio_segment_t *lio_exnode_default_get(lio_exnode_t *ex)
{
    return(ex->default_seg);
}
void exnode_set_default(lio_exnode_t *ex, lio_segment_t *seg)
{
    ex->default_seg = seg;
}

//*************************************************************************
//  exnode_get_header - Returns a pointer to the exnode header
//*************************************************************************

lio_ex_header_t *exnode_get_header(lio_exnode_t *ex)
{
    return(&(ex->header));
}

//*************************************************************************
// lio_exnode_create - Returns an empty exnode
//*************************************************************************

lio_exnode_t *lio_exnode_create()
{
    lio_exnode_t *ex;

    tbx_type_malloc_clear(ex, lio_exnode_t, 1);

    ex_header_init(&(ex->header));

    ex->block = tbx_list_create(0, &skiplist_compare_ex_id, NULL, NULL, NULL);
    ex->view = tbx_list_create(0, &skiplist_compare_ex_id, NULL, NULL, NULL);

    return(ex);
}

//*************************************************************************
// exnode_remove_func - Resmoves the exnode data
//*************************************************************************

gop_op_status_t exnode_remove_func(void *arg, int gid)
{
    lio_exnode_clone_t *op = (lio_exnode_clone_t *)arg;
    lio_exnode_t *ex = op->src_ex;
    tbx_list_iter_t it;
    lio_segment_t *seg;
    ex_id_t *id;
    int i, n;
    gop_op_generic_t *gop;
    gop_opque_t *q;
    gop_op_status_t status;

    n = tbx_list_key_count(ex->view);
    if (n == 0) return(gop_success_status);

    q = gop_opque_new();
    opque_start_execution(q);

    //** Start the cloning process
    it = tbx_list_iter_search(ex->view, NULL, 0);
    tbx_list_next(&it, (tbx_list_key_t **)&id, (tbx_list_data_t **)&seg);
    while (seg != NULL) {
        gop = segment_remove(seg, op->da, op->timeout);
        gop_opque_add(q, gop);
        tbx_list_next(&it, (tbx_list_key_t **)&id, (tbx_list_data_t **)&seg);
    }

    //** Wait for everything to complete
    i = opque_waitall(q);

    gop_opque_free(q, OP_DESTROY);

    status = (i != OP_STATE_SUCCESS) ? gop_failure_status : gop_success_status;
    return(status);
}

//*************************************************************************
// exnode_remove - Removes the exnode data
//*************************************************************************

gop_op_generic_t *exnode_remove(gop_thread_pool_context_t *tpc, lio_exnode_t *ex, data_attr_t *da, int timeout)
{
    lio_exnode_clone_t *exc;
    gop_op_generic_t *gop;

    tbx_type_malloc_clear(exc, lio_exnode_clone_t, 1);
    exc->src_ex = ex;
    exc->da = da;
    exc->timeout = timeout;

    gop = gop_tp_op_new(tpc, NULL, exnode_remove_func, (void *)exc, free, 1);
    return(gop);
}


//*************************************************************************
// lio_exnode_clone_func - Clones the exnode structure and optionally data
//*************************************************************************

gop_op_status_t lio_exnode_clone_func(void *arg, int gid)
{
    lio_exnode_clone_t *exc = (lio_exnode_clone_t *)arg;
    lio_exnode_t *sex = exc->src_ex;
    lio_exnode_t *ex = exc->dest_ex;
    tbx_list_iter_t it;
    lio_segment_t *src_seg, **new_seg, **segptr;
    ex_id_t *id, did;
    int i, n, nfailed;
    gop_op_generic_t *gop;
    gop_opque_t *q;
    gop_op_status_t status;

    n = tbx_list_key_count(sex->view);
    if (n == 0) return(gop_success_status);

    //** make space to store the segments as they are creted
    tbx_type_malloc(new_seg, lio_segment_t *, 2*n);
    q = gop_opque_new();
    opque_start_execution(q);

    //** Start the cloning process
    it = tbx_list_iter_search(sex->view, NULL, 0);
    tbx_list_next(&it, (tbx_list_key_t **)&id, (tbx_list_data_t **)&src_seg);
    i = 0;
    while (src_seg != NULL) {
        new_seg[i] = src_seg;
        new_seg[i+1] = NULL;  //** Need to do this soit doesn't try and use it
        gop = segment_clone(src_seg, exc->da, &(new_seg[i+1]), exc->mode, exc->arg, exc->timeout);
        gop_set_private(gop, &(new_seg[i]));
        gop_opque_add(q, gop);
        tbx_list_next(&it, (tbx_list_key_t **)&id, (tbx_list_data_t **)&src_seg);
        i += 2;
    }

    did = segment_id(sex->default_seg);

    //** Wait for everything to complete
    nfailed = 0;
    n = gop_opque_task_count(q);
    for (i=0; i<n; i++) {
        gop = opque_waitany(q);
        segptr = gop_get_private(gop);

        if (gop_completed_successfully(gop) != OP_STATE_SUCCESS) {
            nfailed++;
            segment_destroy(segptr[1]);
        } else {
            if (did == segment_id(segptr[0])) ex->default_seg = segptr[1];
            tbx_list_insert(ex->view, &segment_id(segptr[1]), segptr[1]);
            tbx_atomic_inc(segptr[1]->ref_count);
        }

        gop_free(gop, OP_DESTROY);
    }

    free(new_seg);

    gop_opque_free(q, OP_DESTROY);

    status = (nfailed > 0) ? gop_failure_status : gop_success_status;
    return(status);
}

//*************************************************************************
// lio_exnode_clone - Clones the exnode structure and optionally data
//*************************************************************************

gop_op_generic_t *lio_exnode_clone(gop_thread_pool_context_t *tpc, lio_exnode_t *src_ex, data_attr_t *da, lio_exnode_t **ex, void *arg, int mode, int timeout)
{
    lio_exnode_clone_t *exc;
    gop_op_generic_t *gop;

    *ex = lio_exnode_create();

    //** Copy the header
    if (src_ex->header.name != NULL) (*ex)->header.name = strdup(src_ex->header.name);
    if (src_ex->header.type != NULL) (*ex)->header.type = strdup(src_ex->header.type);
    generate_ex_id(&((*ex)->header.id));

    tbx_type_malloc(exc, lio_exnode_clone_t, 1);
    exc->src_ex = src_ex;
    exc->dest_ex = *ex;
    exc->da = da;
    exc->mode = mode;
    exc->timeout = timeout;
    exc->arg = arg;

    gop = gop_tp_op_new(tpc, NULL, lio_exnode_clone_func, (void *)exc, free, 1);
    return(gop);
}

//*************************************************************************
// exnode_exchange_free - Frees the internal memroy in the exported exnode
//     but not the exp itself
//*************************************************************************

void exnode_exchange_free(lio_exnode_exchange_t *exp)
{
    if (exp->text.text != NULL) {
        free(exp->text.text);
        exp->text.text= NULL;
    }
    if (exp->text.fd != NULL) {
        tbx_inip_destroy(exp->text.fd);
        exp->text.fd = NULL;
    }
}

//*************************************************************************
// lio_exnode_exchange_destroy - Same as exnode_exchange_free but also frees exp
//     as well
//*************************************************************************

void lio_exnode_exchange_destroy(lio_exnode_exchange_t *exp)
{
    exnode_exchange_free(exp);
    free(exp);
}

//*************************************************************************
// exnode_exchange_init - Initializes an empty exportable exnode
//*************************************************************************

void exnode_exchange_init(lio_exnode_exchange_t *exp, int type)
{
    memset(exp, 0, sizeof(lio_exnode_exchange_t));
    exp->type = type;
}

//*************************************************************************
// lio_exnode_exchange_create - Returns an empty exportable exnode
//*************************************************************************

lio_exnode_exchange_t *lio_exnode_exchange_create(int type)
{
    lio_exnode_exchange_t *exp;

    tbx_type_malloc(exp, lio_exnode_exchange_t, 1);
    exnode_exchange_init(exp, type);

    return(exp);
}

//*************************************************************************
// exnode_exchange_get_default_view_id - Returns the default View/Segment ID
//*************************************************************************

ex_id_t exnode_exchange_get_default_view_id(lio_exnode_exchange_t *exp)
{
    return(tbx_inip_get_integer(exp->text.fd, "view", "default", 0));
}

//*************************************************************************
// lio_lio_exnode_exchange_text_parse - Parses a text based exnode and returns it
//*************************************************************************

lio_exnode_exchange_t *lio_lio_exnode_exchange_text_parse(char *text)
{
    lio_exnode_exchange_t *exp;

    exp = lio_exnode_exchange_create(EX_TEXT);

    exp->text.text = text;
    exp->text.fd = tbx_inip_string_read(text);

    return(exp);
}


//*************************************************************************
// lio_exnode_exchange_create - Returns an empty exportable exnode
//*************************************************************************

lio_exnode_exchange_t *lio_exnode_exchange_load_file(char *fname)
{
    FILE *fd;
    char *text;
    int i;

    fd = fopen(fname, "r");
    assert(fd != NULL);
    fseek(fd, 0, SEEK_END);
    i = ftell(fd);
    tbx_type_malloc(text, char, i + 2);
    fseek(fd, 0, SEEK_SET);
    fread(text, i, 1, fd);
    text[i] = '\n';
    text[i+1] = '\0';
    fclose(fd);

    return(lio_lio_exnode_exchange_text_parse(text));
}

//*************************************************************************
// exnode_exchange_append_text - Appends the text to the text based exnode
//     proto object.  NOT used for the goole protobuf version
//*************************************************************************

void exnode_exchange_append_text(lio_exnode_exchange_t *exp, char *buffer)
{
    char *text;
    int n;

    if (buffer == NULL) return;

    n = (exp->text.text == NULL) ? 0 : strlen(exp->text.text);

    tbx_type_malloc_clear(text, char, n + strlen(buffer) + 3);
    if (n == 0) {
        sprintf(text, "%s", buffer);
        exp->text.text = text;
    } else {
        sprintf(text, "%s\n%s", exp->text.text, buffer);
        free(exp->text.text);
        exp->text.text = text;
    }

}

//*************************************************************************
// exnode_exchange_append - Appends the text to the text based exnode
//     proto object.  NOT used for the goole protobuf version
//*************************************************************************

void exnode_exchange_append(lio_exnode_exchange_t *exp, lio_exnode_exchange_t *exp_append)
{
    if (exp_append->text.text == NULL) return;
    exnode_exchange_append_text(exp, exp_append->text.text);
}

//*************************************************************************
//  lio_exnode_deserialize_text - Storea a text based exnode
//*************************************************************************

int lio_exnode_deserialize_text(lio_exnode_t *ex, lio_exnode_exchange_t *exp, lio_service_manager_t *ess)
{
    tbx_inip_group_t *g;
    tbx_inip_element_t *ele;
    tbx_inip_file_t *fd;
    lio_segment_t *seg = NULL;
    ex_id_t id;
    int fin;
    char *key, *value, *token, *bstate;
    char *exgrp = "exnode";

    fd = exp->text.fd;

    //** Load the header
    g = tbx_inip_group_find(fd, exgrp);
    if (g != NULL) {
        ex->header.name =  tbx_inip_get_string(fd, exgrp, "name", "");
        ex->header.id = tbx_inip_get_integer(fd, exgrp, "id", 0);
    }

    //** and the views
    g = tbx_inip_group_find(fd, "view");
    if (g == NULL) {
        log_printf(1, "lio_exnode_deserialize_text: No views found!\n");
        return(1);
    }

    ele = tbx_inip_ele_first(g);
    while (ele != NULL) {
        key = tbx_inip_ele_get_key(ele);
        if (strcmp(key, "segment") == 0) {

            //** Parse the segment line
            value = tbx_inip_ele_get_value(ele);
            token = strdup(value);
            id = 0;
            sscanf(tbx_stk_string_token(token, ":", &bstate, &fin), XIDT, &id);
            free(token);

            //** and load it
            log_printf(15, "exnode_load_text: Loading view segment " XIDT "\n", id);
            seg = load_segment(ess, id, exp);
            if (seg != NULL) {
                tbx_atomic_inc(seg->ref_count);
                tbx_list_insert(ex->view, &segment_id(seg), seg);
            } else {
                log_printf(0, "Bad segment!  sid=" XIDT "\n", id);
            }
        }

        //** Move to the next segmnet to load
        ele = tbx_inip_ele_next(ele);
    }

    //** Now get the default segment to use
    id = tbx_inip_get_integer(fd, "view", "default", 0);
    if (id == 0) {   //** No default so use the last one loaded
        ex->default_seg = seg;
    } else {
        ex->default_seg = tbx_list_search(ex->view, &id);
    }

    return((ex->default_seg == NULL) ? 1 : 0);
}

//*************************************************************************
// lio_exnode_deserialize_proto - Deserializes the exnode from a google protobuf
//*************************************************************************

int lio_exnode_deserialize_proto(lio_exnode_t *ex, lio_exnode_exchange_t *exp, lio_service_manager_t *ess)
{
    return(-1);
}

//*************************************************************************
// lio_exnode_deserialize - Deserializes the exnode
//*************************************************************************

int lio_exnode_deserialize(lio_exnode_t *ex, lio_exnode_exchange_t *exp, lio_service_manager_t *ess)
{
    if (exp->type == EX_TEXT) {
        return(lio_exnode_deserialize_text(ex, exp, ess));
    } else if (exp->type == EX_PROTOCOL_BUFFERS) {
        return(lio_exnode_deserialize_proto(ex, exp, ess));
    }

    return(-1);
}


//*************************************************************************
//  lio_exnode_serialize_text - Exports a text based exnode
//*************************************************************************

int lio_exnode_serialize_text(lio_exnode_t *ex, lio_exnode_exchange_t *exp)
{
    int bufsize = 1024;
    char buffer[bufsize];
    char *etext;
    int used = 0;
    lio_segment_t *seg;
    ex_id_t *id;
    tbx_sl_iter_t it;

    //** Store the header
    tbx_append_printf(buffer, &used, bufsize, "[exnode]\n");
    if (ex->header.name != NULL) {
        if (strcmp(ex->header.name, "") != 0) {
            etext = tbx_stk_escape_text("=", '\\', ex->header.name);
            tbx_append_printf(buffer, &used, bufsize, "name=%s\n", etext);
            free(etext);
        }
    }
    tbx_append_printf(buffer, &used, bufsize, "id=" XIDT "\n\n", ex->header.id);

    exnode_exchange_append_text(exp, buffer);

    //** and all the views
    used = 0;
    tbx_append_printf(buffer, &used, bufsize, "\n[view]\n");
    if (ex->default_seg != NULL) tbx_append_printf(buffer, &used, bufsize, "default=" XIDT "\n", segment_id(ex->default_seg));
    it = tbx_list_iter_search(ex->view, (tbx_sl_key_t *)NULL, 0);
    while (tbx_list_next(&it, (tbx_sl_key_t **)&id, (tbx_sl_data_t **)&seg) == 0) {
        log_printf(15, "lio_exnode_serialize_text: Storing view segment " XIDT "\n", segment_id(seg));
        tbx_append_printf(buffer, &used, bufsize, "segment=" XIDT "\n", *id);

        segment_serialize(seg, exp);
    }

    tbx_append_printf(buffer, &used, bufsize, "\n");

    exnode_exchange_append_text(exp, buffer);

    return(0);
}

//*************************************************************************
// lio_exnode_serialize_proto - Serializes the exnode to a google protobuf
//*************************************************************************

int lio_exnode_serialize_proto(lio_exnode_t *ex, lio_exnode_exchange_t *exp)
{
    return(-1);
}

//*************************************************************************
// lio_exnode_serialize - Serializes the exnode
//*************************************************************************

int lio_exnode_serialize(lio_exnode_t *ex, lio_exnode_exchange_t *exp)
{
    if (exp->type == EX_TEXT) {
        return(lio_exnode_serialize_text(ex, exp));
    } else if (exp->type == EX_PROTOCOL_BUFFERS) {
        return(lio_exnode_serialize_proto(ex, exp));
    }

    return(-1);
}


//*************************************************************************
// lio_exnode_destroy - Frees the memory associated with an exnode
//*************************************************************************

void lio_exnode_destroy(lio_exnode_t *ex)
{
    tbx_list_iter_t it;
    lio_segment_t *seg;
    lio_data_block_t *b;
    ex_id_t id;

    //** Remove the views
    it = tbx_list_iter_search(ex->view, (tbx_sl_key_t *)NULL, 0);
    while (tbx_list_next(&it, (tbx_sl_key_t *)&id, (tbx_sl_data_t *)&seg) == 0) {
        tbx_atomic_dec(seg->ref_count);
        log_printf(15, "lio_exnode_destroy: seg->id=" XIDT " ref_count=%d\n", segment_id(seg), seg->ref_count);
        segment_destroy(seg);
        tbx_list_next(&it, (tbx_sl_key_t *)&id, (tbx_sl_data_t *)&seg);
    }

    //** And any blocks
    it = tbx_list_iter_search(ex->block, (tbx_sl_key_t *)NULL, 0);
    while (tbx_list_next(&it, (tbx_sl_key_t *)&id, (tbx_sl_data_t *)&b) == 0) {
        data_block_destroy(b);
        tbx_list_next(&it, (tbx_sl_key_t *)&id, (tbx_sl_data_t *)&b);
    }

    tbx_list_destroy(ex->view);
    tbx_list_destroy(ex->block);

    ex_header_release(&(ex->header));

    free(ex);

    return;
}


