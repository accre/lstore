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
// Routines for managing the automatic data service framework
//***********************************************************************

#define _log_module_index 145

#include "ex3_abstract.h"
#include "list.h"
#include "random.h"
#include "type_malloc.h"
#include "log.h"

typedef struct {
  data_service_fn_t *(*ds_create)(char *fname);
} data_service_driver_t;

typedef struct {
  list_t *table;
} data_service_table_t;

data_service_table_t *data_service_driver = NULL;

list_t *data_service_available_table = NULL;


//***********************************************************************
// install_data_service- Installs a data_service driver into the table
//***********************************************************************

int install_data_service(char *type, data_service_fn_t *(*ds_create)(char *fname))
{
  data_service_driver_t *d;

  //** 1st time so create the struct
  if (data_service_driver == NULL) {
     type_malloc_clear(data_service_driver, data_service_table_t, 1);
     data_service_driver->table = list_create(0, &list_string_compare, list_string_dup, list_simple_free, list_no_data_free);
  }

  d = list_search(data_service_driver->table, type);
  if (d != NULL) {
    log_printf(0, "install_data_service: Matching driver for type=%s already exists!\n", type);
    return(1);
  }
  
  type_malloc_clear(d, data_service_driver_t, 1);
  d->ds_create = ds_create;
  list_insert(data_service_driver->table, type, (void *)d);

  return(0);
}

//***********************************************************************
// load_data_service - Creates a new DS based on the type
//***********************************************************************

data_service_fn_t *load_data_service(char *type, char *fname)
{
  data_service_driver_t *d;

  d = list_search(data_service_driver->table, type);
  if (d == NULL) {
    log_printf(0, "No matching driver for type=%s\n", type);
    return(NULL);
  }

  return(d->ds_create(fname));
}


//***********************************************************************
// lookup_data_service - returns the current DS for the given type
//***********************************************************************

int add_data_service(data_service_fn_t *ds)
{
  data_service_fn_t *d;

  if (data_service_available_table == NULL) {
     data_service_available_table = list_create(0, &list_string_compare, list_string_dup, list_simple_free, list_no_data_free);
  }

  d = list_search(data_service_available_table, ds_type(ds));
  if (d != NULL) {
    log_printf(0, "Matching driver for type=%s\n", ds_type(ds));
    return(1);
  }

  list_insert(data_service_available_table, ds_type(ds), (void *)ds);

  return(0);
}

//***********************************************************************
// lookup_data_service - returns the current DS for the given type
//***********************************************************************

data_service_fn_t *lookup_data_service(char *type)
{
  data_service_fn_t *ds;

  if (data_service_available_table == NULL) return(NULL);

  ds = list_search(data_service_available_table, type);
  if (ds == NULL) {
    log_printf(0, "No matching driver for type=%s\n", type);
    return(NULL);
  }

  return(ds);
}

