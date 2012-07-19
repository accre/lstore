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
// Comparison routines for lists
//***********************************************************************

#define _log_module_index 147

#include "ex3_abstract.h"
#include "skiplist.h"
#include "log.h"

int skiplist_compare_fn_ex_off(void *arg, skiplist_key_t *k1, skiplist_key_t *k2);
skiplist_compare_t skiplist_compare_ex_off={skiplist_compare_fn_ex_off, NULL};

int skiplist_compare_fn_ex_id(void *arg, skiplist_key_t *k1, skiplist_key_t *k2);
skiplist_compare_t skiplist_compare_ex_id={skiplist_compare_fn_ex_id, NULL};

int skiplist_compare_fn_ex_off(void *arg, skiplist_key_t *k1, skiplist_key_t *k2)
{
  ex_off_t *a = (ex_off_t *)k1;
  ex_off_t *b = (ex_off_t *)k2;
  int cmp = 1;

  if (*a < *b) {
    cmp = -1;
  } else if ( *a == *b) {
    cmp = 0;
  }

  log_printf(15, "skiplist_compare_fn_ex_off: cmp(" XOT ", " XOT ")=%d\n", *a, *b, cmp);
  return(cmp);
}

//*************************************************************************************

int skiplist_compare_fn_ex_id(void *arg, skiplist_key_t *k1, skiplist_key_t *k2)
{
  ex_id_t *a = (ex_id_t *)k1;
  ex_id_t *b = (ex_id_t *)k2;
  int cmp = 1;

  if (*a < *b) {
    cmp = -1;
  } else if ( *a == *b) {
    cmp = 0;
  }

//  log_printf(15, "skiplist_compare_fn_ex_id: cmp(" XIDT ", " XIDT ")=%d\n", *a, *b, cmp);
  return(cmp);
}

