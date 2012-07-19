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
// Linear exnode3 support
//***********************************************************************


#include "list.h"
#include "ex3_types.h"

#ifndef _EX3_HEADER_H_
#define _EX3_HEADER_H_

#ifdef __cplusplus
extern "C" {
#endif


typedef struct {
  char *name;
  ex_id_t id;
  char *type;
  list_t *attributes;  //should be a key/value pair struct?
} ex_header_t;

ex_header_t *ex_header_create();
void ex_header_release(ex_header_t *h);
void ex_header_destroy(ex_header_t *h);
void ex_header_init(ex_header_t *h);
char *ex_header_get_name(ex_header_t *h);
void ex_header_set_name(ex_header_t *h, char *name);
ex_id_t ex_header_get_id(ex_header_t *h);
void ex_header_set_id(ex_header_t *h, ex_id_t id);
char  *ex_header_get_type(ex_header_t *h);
void ex_header_set_type(ex_header_t *h, char *type);
list_t *ex_header_get_attributes(ex_header_t *h);
void ex_header_set_attributes(ex_header_t *h, list_t *attr);

#ifdef __cplusplus
}
#endif

#endif

