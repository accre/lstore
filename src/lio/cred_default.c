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
// Default cred setup
//***********************************************************************

#define _log_module_index 185

#include "list.h"
#include "type_malloc.h"
#include "log.h"
#include "authn_abstract.h"

//***********************************************************************

char *cdef_get_type(creds_t *c)
{
    return("DEFAULT");
}

//***********************************************************************

void *cdef_get_type_field(creds_t *c, int index, int *len)
{
    *len = 0;
    return(NULL);
}

//***********************************************************************

char *cdef_get_id(creds_t *c)
{
    return(c->id);
//  if (c->id != NULL) {
//     return(strdup(c->id));
//  }
//  return(NULL);
}

//***********************************************************************

void cdef_set_id(creds_t *c, char *id)
{
    c->id = strdup(id);
    return;
}

//***********************************************************************

void cdef_set_private_handle(creds_t *c, void *handle, void (*destroy)(void *))
{
    c->handle = handle;
    c->handle_destroy = destroy;
    return;
}

//***********************************************************************

void *cdef_get_private_handle(creds_t *c)
{
    return(c->handle);
}


//***********************************************************************

void cdef_destroy(creds_t *c)
{
    if (c->handle_destroy != NULL) c->handle_destroy(c);
    if (c->id != NULL) free(c->id);
    free(c);
}

//***********************************************************************

creds_t *cred_default_create()
{
    creds_t *c;
    type_malloc_clear(c, creds_t, 1);

    c->get_type = cdef_get_type;
    c->get_type_field = cdef_get_type_field;
    c->get_id = cdef_get_id;
    c->set_id = cdef_set_id;
    c->set_private_handle = cdef_set_private_handle;
    c->get_private_handle = cdef_get_private_handle;
    c->destroy = cdef_destroy;

    return(c);
}

