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

#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/list.h>

#include "ex3/header.h"

//*************************************************************************
// ex_header_init - Initializes an existing exnode header
//*************************************************************************

void ex_header_init(lio_ex_header_t *eh)
{
    assert(eh != NULL);
    memset(eh, 0, sizeof(lio_ex_header_t));
}

//*************************************************************************
// ex_header_release - Releases the header componenets
//   NOTE:  Just releases fields and NOT the header itself
//*************************************************************************

void ex_header_release(lio_ex_header_t *h)
{
    if (h->name != NULL) free(h->name);
}


//*************************************************************************
// ex_header_create - Creates a new exnode header
//*************************************************************************

lio_ex_header_t *ex_header_create()
{
    lio_ex_header_t *eh = (lio_ex_header_t *)malloc(sizeof(lio_ex_header_t));

    return(eh);
}

//*************************************************************************
// ex_header_destroy - Destroys an exnode header
//*************************************************************************

void ex_header_destroy(lio_ex_header_t *eh)
{
    ex_header_release(eh);
    free(eh);
}

//*************************************************************************
// name - Manipulates the header name
//*************************************************************************

char *ex_header_get_name(lio_ex_header_t *h)
{
    return(h->name);
}
void ex_header_set_name(lio_ex_header_t *h, char *name)
{
    if (h->name != NULL) free(h->name);
    h->name = strdup(name);
}


//*************************************************************************
// id routines
//*************************************************************************

ex_id_t ex_header_get_id(lio_ex_header_t *h)
{
    return(h->id);
}
void ex_header_set_id(lio_ex_header_t *h, ex_id_t id)
{
    h->id = id;
}

//*************************************************************************
// type routines
//*************************************************************************

char *ex_header_get_type(lio_ex_header_t *h)
{
    return(h->type);
}
void ex_header_set_type(lio_ex_header_t *h, char *type)
{
    h->type = strdup(type);
}

//*************************************************************************
// Attribute routines
//*************************************************************************

tbx_list_t *ex_header_get_attributes(lio_ex_header_t *h)
{
    return(h->attributes);
}
void ex_header_set_attributes(lio_ex_header_t *h, tbx_list_t *attr)
{
    h->attributes = attr;
}
