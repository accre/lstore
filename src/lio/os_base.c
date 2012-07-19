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
// Routines for managing the segment loading framework
//***********************************************************************

#define _log_module_index 154

#include "ex3_abstract.h"
#include "list.h"
#include "type_malloc.h"
#include "log.h"
#include "object_service_abstract.h"
#include "string_token.h"
#include "log.h"

typedef struct {
  object_service_fn_t *(*create)(void *arg, char *fname);
  void *arg;
} os_driver_t;

typedef struct {
  list_t *table;
} os_driver_table_t;

os_driver_table_t *os_driver_table = NULL;
os_regex_table_t *os_regex_table_create(int n);

//***********************************************************************
// install_object_service- Installs an OS driver into the table
//***********************************************************************

int install_object_service(char *type, object_service_fn_t *(*create)(void *arg, char *fname), void *arg)
{
  os_driver_t *d;

  //** 1st time so create the struct
  if (os_driver_table == NULL) {
     type_malloc_clear(os_driver_table, os_driver_table_t, 1);
     os_driver_table->table = list_create(0, &list_string_compare, list_string_dup, list_simple_free, list_no_data_free);
  }

  d = list_search(os_driver_table->table, type);
  if (d != NULL) {
    log_printf(0, "install_object_service: Matching driver for type=%s already exists!\n", type);
    return(1);
  }

  type_malloc_clear(d, os_driver_t, 1);
  d->create = create;
  d->arg = arg;
  list_insert(os_driver_table->table, type, (void *)d);

  return(0);
}

//***********************************************************************
// create_object_service - Creates a segment of the given type
//***********************************************************************

object_service_fn_t *create_object_service(char *type, char *fname)
{
  os_driver_t *d;

  d = list_search(os_driver_table->table, type);
  if (d == NULL) {
    log_printf(0, "create_object_service:  No matching driver for type=%s\n", type);
    return(NULL);
  }

  return(d->create(d->arg, fname));
}

//***********************************************************************
// os_glob2regex - Converts a string in shell glob notation to regex
//***********************************************************************

char *os_glob2regex(char *glob)
{
  char *reg;
  int i, j, n;

  n = strlen(glob);
  type_malloc(reg, char, n);

  j = 0;
  reg[j] = '^'; j++;

  switch (glob[0]) {
    case ('.') : reg[j] = '\\'; reg[j+1] = '.'; j=+2;  break;
    case ('*') : reg[j] = '\\'; reg[j+1] = '*'; j=+2;  break;
    case ('?') : reg[j] = '.'; j++; break;
    default  : reg[j] = glob[0]; j++; break;
  }

  for (i=1; i<n; i++) {
     switch (glob[i]) {
        case ('.') : reg[j] = '\\'; reg[j+1] = '.'; j=+2;  break;
        case ('*') : if (glob[i-1] == '\\') {
                       reg[j] = '*'; j++;
                     } else {
                       reg[j] = '\\'; reg[j+1] = '*'; j=+2;
                     }
                     break;
        case ('?') : reg[j] = (glob[i-1] == '\\') ? '?' : '.';
                     j++;
                     break;
        default    : reg[j] = glob[i]; j++;
                     break;
     }
  }

  reg[j] = '$';
  reg[j+1] = 0;
  j =+ 2;

  return(realloc(reg, j));
}

//***********************************************************************
// os_path_glob2regex - Converts a path glob to a regex table
//***********************************************************************

os_regex_table_t *os_path_glob2regex(char *path)
{
  os_regex_table_t *table;
  char *bstate, *p2, *frag;
  int i, j, n, fin, err;

  p2 = strdup(path);

  //** Determine the max number of path fragments
  j = 1;
  n = strlen(p2);
  for (i=0; i<n; i++) { if (p2[i] == '/') j++; }

  //** Make the  table space
  table = os_regex_table_create(n);

  //** Cycle through the fragments converting them
  i = 0;
  frag = escape_string_token(p2, "/", '\\', 1, &bstate, &fin);
  while (frag != NULL) {
     table->regex_entry[i].expression = os_glob2regex(frag);
     err = regcomp(&(table->regex_entry[i].compiled), table->regex_entry[i].expression, REG_NOSUB);
     if (err != 0) {
        os_regex_table_destroy(table);
        log_printf(0, "os_path_glob2regex: Error with fragment %s err=%d tid=%d\n", table->regex_entry[i].expression, err, atomic_thread_id);
        return(NULL);
     }

     frag = escape_string_token(NULL, "/", '\\', 1, &bstate, &fin);
  }

  table->n = i; //** Adjust the table size

  return(table);
}

//***********************************************************************
// os_regex_table_create - Creates a regex table
//***********************************************************************

os_regex_table_t *os_regex_table_create(int n)
{
  os_regex_table_t *table;

  type_malloc_clear(table, os_regex_table_t, 1);
  type_malloc_clear(table->regex_entry, os_regex_entry_t, n);

  table->n = n;
  return(table);
}

//***********************************************************************
// os_regex_table_destroy - Destroys a regex table
//***********************************************************************

void os_regex_table_destroy(os_regex_table_t *table)
{
  int i;
  os_regex_entry_t *re;

  if (table->regex_entry != NULL) {
     for (i=0; i<table->n; i++) {
       re = &(table->regex_entry[i]);
       if (re->expression != NULL) {
           free(re->expression);
           regfree(&(re->compiled));
       }
     }

     free(table->regex_entry);
  }

  free(table);
}
