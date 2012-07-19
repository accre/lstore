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

#ifndef __INIPARSE_H
#define __INIPARSE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>

struct inip_element_s {  //** Key/Value pair
   char *key;
   char *value;
   struct inip_element_s *next;
};
typedef struct inip_element_s inip_element_t;

struct inip_group_s {  //** Group
   char *group;
   inip_element_t *list;
   struct inip_group_s *next;
};
typedef struct inip_group_s inip_group_t;

typedef struct {  //File
   inip_group_t *tree;
   int  n_groups;
} inip_file_t;


#define inip_n_groups(inip)    (inip)->n_groups
#define inip_first_group(inip) (inip)->tree
#define inip_get_group(g)  (g)->group
#define inip_next_group(g) ((g) == NULL) ? NULL : (g)->next
#define inip_first_element(group)  (group)->list
#define inip_next_element(ele) ((ele) == NULL) ? NULL : (ele)->next
#define inip_get_element_key(ele) ((ele) == NULL) ? NULL : (ele)->key
#define inip_get_element_value(ele) ((ele) == NULL) ? NULL : (ele)->value

inip_file_t *inip_read(const char *fname);
inip_file_t *inip_read_text(const char *text);
void inip_destroy(inip_file_t *inip);
char *inip_get_string(inip_file_t *inip, const char *group, const char *key, char *def);
int64_t inip_get_integer(inip_file_t *inip, const char *group, const char *key, int64_t def);
uint64_t inip_get_unsigned_integer(inip_file_t *inip, const char *group, const char *key, uint64_t def);
double inip_get_double(inip_file_t *inip, const char *group, const char *key, double def);
inip_group_t *inip_find_group(inip_file_t *inip, const char *name);


#ifdef __cplusplus
}
#endif

#endif

