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

#define _log_module_index 149

#include <string.h>
#include <stdlib.h>
#include "ex3_header.h"
#include "assert.h"
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

char *ex_header_get_name(ex_header_t *h) { return(h->name); }
void ex_header_set_name(ex_header_t *h, char *name) 
{ 
  if (h->name != NULL) free(h->name);
  h->name = strdup(name); 
}


//*************************************************************************
// id routines
//*************************************************************************

ex_id_t ex_header_get_id(ex_header_t *h) { return(h->id); }
void ex_header_set_id(ex_header_t *h, ex_id_t id) { h->id = id; }

//*************************************************************************
// type routines
//*************************************************************************

char *ex_header_get_type(ex_header_t *h) { return(h->type); }
void ex_header_set_type(ex_header_t *h, char *type) { h->type = strdup(type); }

//*************************************************************************
// Attribute routines
//*************************************************************************

list_t *ex_header_get_attributes(ex_header_t *h) { return(h->attributes); }
void ex_header_set_attributes(ex_header_t *h, list_t *attr) { h->attributes = attr; }


