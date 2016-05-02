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

#define _log_module_index 149

#include <string.h>
#include <stdlib.h>
#include "ex3_header.h"
#include <assert.h>
#include "assert_result.h"
#include "list.h"

//*************************************************************************
// ex_header_init - Initializes an existing exnode header
//*************************************************************************

void ex_header_init(ex_header_t *eh)
{
    assert(eh != NULL);
    memset(eh, 0, sizeof(ex_header_t));
//  eh->attributes = list_create(1, &list_string_compare, list_string_dup, list_simple_free, list_simple_free);
}

//*************************************************************************
// ex_header_release - Releases the header componenets
//   NOTE:  Just releases fields and NOT the header itself
//*************************************************************************

void ex_header_release(ex_header_t *h)
{
    if (h->name != NULL) free(h->name);
//  if (h->type != NULL) free(h->type);
//  list_destroy(h->attributes);
}


//*************************************************************************
// ex_header_create - Creates a new exnode header
//*************************************************************************

ex_header_t *ex_header_create()
{
    ex_header_t *eh = (ex_header_t *)malloc(sizeof(ex_header_t));

    return(eh);
}

//*************************************************************************
// ex_header_destroy - Destroys an exnode header
//*************************************************************************

void ex_header_destroy(ex_header_t *eh)
{
    ex_header_release(eh);
    free(eh);
}

//*************************************************************************
// name - Manipulates the header name
//*************************************************************************

char *ex_header_get_name(ex_header_t *h)
{
    return(h->name);
}
void ex_header_set_name(ex_header_t *h, char *name)
{
    if (h->name != NULL) free(h->name);
    h->name = strdup(name);
}


//*************************************************************************
// id routines
//*************************************************************************

ex_id_t ex_header_get_id(ex_header_t *h)
{
    return(h->id);
}
void ex_header_set_id(ex_header_t *h, ex_id_t id)
{
    h->id = id;
}

//*************************************************************************
// type routines
//*************************************************************************

char *ex_header_get_type(ex_header_t *h)
{
    return(h->type);
}
void ex_header_set_type(ex_header_t *h, char *type)
{
    h->type = strdup(type);
}

//*************************************************************************
// Attribute routines
//*************************************************************************

tbx_list_t *ex_header_get_attributes(ex_header_t *h)
{
    return(h->attributes);
}
void ex_header_set_attributes(ex_header_t *h, tbx_list_t *attr)
{
    h->attributes = attr;
}


