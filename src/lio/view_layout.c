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

#include <string.h>
#include "exnode.h"
#include "append_printf.h"
#include "string_token.h"
#include "iniparse.h"
#include "type_malloc.h"
#include "random.h"
#include "log.h"

typedef struct {
    layout_t *lay;
} view_layout_t;

//*************************************************************************
// vl_read - Reads data from the layout view
//*************************************************************************

op_generic_t *vl_read(view_t *v, data_attr_t *da, ex_off_t off, tbuffer_t *buffer, ex_off_t boff, ex_off_t len, int timeout)
{
    view_layout_t *vl = (view_layout_t *)v->priv;

    if (vl == NULL)  return(gop_dummy(0));

    return(layout_read(vl->lay, da, off, buffer, boff, len, timeout));
}

//*************************************************************************
// vl_write - Writes data to the layout view
//*************************************************************************

op_generic_t *vl_write(view_t *v, data_attr_t *da, ex_off_t off, tbuffer_t *buffer, ex_off_t boff, ex_off_t len, int timeout)
{
    view_layout_t *vl = (view_layout_t *)v->priv;

    if (vl == NULL)  return(gop_dummy(0));

    return(layout_write(vl->lay, da, off, buffer, boff, len, timeout));
}

//*************************************************************************
// vl_check - Check data in the layout view
//*************************************************************************

op_generic_t *vl_check(view_t *v, data_attr_t *da, int timeout)
{
    view_layout_t *vl = (view_layout_t *)v->priv;

    if (vl == NULL)  return(gop_dummy(0));

    return(layout_check(vl->lay, da, timeout));
}

//*************************************************************************
// vl_repair - Attempts to repair the view layout's data
//*************************************************************************

op_generic_t *vl_repair(view_t *v, data_attr_t *da, int timeout)
{
    view_layout_t *vl = (view_layout_t *)v->priv;

    if (vl == NULL)  return(gop_dummy(0));

    return(layout_repair(vl->lay, da, timeout));
}

//*************************************************************************
// vl_truncate - Attempts to Grow/shrink the view layout's data
//*************************************************************************

op_generic_t *vl_truncate(view_t *v, data_attr_t *da, ex_off_t new_size, int timeout)
{
    view_layout_t *vl = (view_layout_t *)v->priv;

    if (vl == NULL)  return(gop_dummy(0));

    return(layout_truncate(vl->lay, da, new_size, timeout));
}

//*************************************************************************
// vl_size - Returns the views size
//*************************************************************************

ex_off_t vl_size(view_t *v)
{
    view_layout_t *vl = (view_layout_t *)v->priv;

    if (vl == NULL) return(-1);

    return(layout_size(vl->lay));
}

//***********************************************************************
// vl_serialize_text -Converts the view to a text representation
//***********************************************************************

int vl_serialize_text(view_t *v, exnode_exchange_t *exp)
{
    view_layout_t *vl = (view_layout_t *)v->priv;
    int bufsize = 10*1024;
    char buffer[bufsize];
    int used = 0;

    //** Serialize the layout
    layout_serialize(vl->lay, exp);

    //** and my view
    append_printf(buffer, &used, bufsize, "\n[view-" XIDT "]\n", v->header.id);
    append_printf(buffer, &used, bufsize, "type=%s\n", view_type(v));
    append_printf(buffer, &used, bufsize, "layout=" XIDT "\n",layout_id(vl->lay));
    append_printf(buffer, &used, bufsize, "ref_count=" XIDT "\n\n",v->ref_count);

    //** Merge everything together and return it
    exnode_exchange_append_text(exp, buffer);

    return(0);
}


//***********************************************************************
// vl_serialize_proto -Converts the view to a protocol buffer
//***********************************************************************

int vl_serialize_proto(view_t *v, exnode_exchange_t *exp)
{
    return(-1);
}

//***********************************************************************
// vl_serialize -Convert the view to a more portable format
//***********************************************************************

int vl_serialize(view_t *v, exnode_exchange_t *exp)
{
//  view_layout_t *vl = (view_layout_t *)v->priv;

    if (exp->type == EX_TEXT) {
        return(vl_serialize_text(v, exp));
    } else if (exp->type == EX_PROTOCOL_BUFFERS) {
        return(vl_serialize_proto(v, exp));
    }

    return(-1);
}

//***********************************************************************
// vl_deserialize_text - Reads the view from a a text format
//***********************************************************************

int vl_deserialize_text(view_t *v, ex_id_t id, exnode_exchange_t *exp)
{
    view_layout_t *vl = (view_layout_t *)v->priv;
    int bufsize = 1024;
    char grp[bufsize];
    ex_id_t lay_id;
    inip_file_t *fd;

    //** Parse the ini text
    fd = inip_read_text(exp->text);

    //** Make the layout section name
    snprintf(grp, bufsize, "view-" XIDT, id);

    //** Get the header info
    v->header.id = id;
    v->header.type = VIEW_TYPE_LAYOUT;
    v->header.name = inip_get_string(fd, grp, "name", "");

    //** and the layout to use
    lay_id = inip_get_unsigned_integer(fd, grp, "layout", 0);
    if (lay_id == 0) {
        log_printf(0, "vl_deserialize_text: ERROR No layout specified!\n");
        return(1);
    }

    inip_destroy(fd);

    log_printf(0, "vl_deserialize_text: Attempting to load layot id=" XIDT "\n", lay_id);

    //** load the layout
    vl->lay = load_layout(lay_id, exp);
    atomic_inc(vl->lay->ref_count);

    if (vl->lay == NULL) return(-1);
    if (vl->lay == NULL) return(-1);

    return(0);
}


//***********************************************************************
// vl_deserialize_proto - Loads the view from a proto struct
//***********************************************************************

int vl_deserialize_proto(view_t *v, ex_id_t id, exnode_exchange_t *exp)
{
    return(-1);
}

//***********************************************************************
// vl_deserialize -Convert the view to a more portable format
//***********************************************************************

int vl_deserialize(view_t *v, ex_id_t id, exnode_exchange_t *exp)
{
    if (exp->type == EX_TEXT) {
        return(vl_deserialize_text(v, id, exp));
    } else if (exp->type == EX_PROTOCOL_BUFFERS) {
        return(vl_deserialize_proto(v, id, exp));
    }

    return(-1);
}



//*************************************************************************
// view_layout_set - Sets the view's layout
//*************************************************************************

int view_layout_set(view_t *v, layout_t *lay)
{
    view_layout_t *vl = (view_layout_t *)v->priv;

    vl->lay = lay;
    atomic_inc(lay->ref_count);

    return(0);
}

//*************************************************************************
// vl_destroy - Destroys the view struct without touching the data
//*************************************************************************

void vl_destroy(view_t *v)
{
    view_layout_t *vl = (view_layout_t *)v->priv;

    log_printf(15, "vl_destroy: view->id=" XIDT " ref_count=%d\n", view_id(v), v->ref_count);
    if (v->ref_count > 0) return;

    ex_header_release(&(v->header));
    atomic_dec(vl->lay->ref_count);
    layout_destroy(vl->lay);
    free(vl);
    free(v);
}


//*************************************************************************
// view_layout_create - Creates a View from a layout
//*************************************************************************

view_t *view_layout_create(void *arg)
{
    view_t *v;
    view_layout_t *vl;

    type_malloc_clear(v, view_t, 1);
    type_malloc_clear(vl, view_layout_t, 1);

    v->priv = (void *)vl;
    generate_ex_id(&(v->header.id));
    v->header.type = VIEW_TYPE_LAYOUT;
    atomic_set(v->ref_count, 0);

    v->fn.read = vl_read;
    v->fn.write = vl_write;
    v->fn.check = vl_check;
    v->fn.repair = vl_repair;
    v->fn.truncate = vl_truncate;
    v->fn.size = vl_size;
    v->fn.serialize = vl_serialize;
    v->fn.deserialize = vl_deserialize;
    v->fn.destroy = vl_destroy;

    return(v);
}

//*************************************************************************
// view_layout_load - Loads a View from disk
//*************************************************************************

view_t *view_layout_load(void *arg, ex_id_t id, exnode_exchange_t *ex)
{
    view_t *v = view_layout_create(arg);

    view_deserialize(v, id, ex);
    return(v);
}
