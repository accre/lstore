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

#include "tbx/toolbox_visibility.h"
#include <stdio.h>
#include <stdint.h>
typedef struct tbx_inip_element_t tbx_inip_element_t;
struct tbx_inip_element_t {  //** Key/Value pair
    char *key;
    char *value;
    struct tbx_inip_element_t *next;
};

typedef struct tbx_inip_group_t tbx_inip_group_t;
struct tbx_inip_group_t {  //** Group
    char *group;
    tbx_inip_element_t *list;
    struct tbx_inip_group_t *next;
};

typedef struct tbx_inip_file_t tbx_inip_file_t;
struct tbx_inip_file_t {  //File
    tbx_inip_group_t *tree;
    int  n_groups;
};


#define inip_n_groups(inip)    (inip)->n_groups
#define inip_first_group(inip) (inip)->tree
#define inip_get_group(g)  (g)->group
#define inip_next_group(g) ((g) == NULL) ? NULL : (g)->next
#define inip_first_element(group)  (group)->list
#define inip_next_element(ele) ((ele) == NULL) ? NULL : (ele)->next
#define inip_get_element_key(ele) ((ele) == NULL) ? NULL : (ele)->key
#define inip_get_element_value(ele) ((ele) == NULL) ? NULL : (ele)->value

tbx_inip_file_t *inip_read_fd(FILE *fd);
TBX_API tbx_inip_file_t *inip_read(const char *fname);
TBX_API tbx_inip_file_t *inip_read_text(const char *text);
TBX_API void inip_destroy(tbx_inip_file_t *inip);
TBX_API char *inip_get_string(tbx_inip_file_t *inip, const char *group, const char *key, char *def);
TBX_API int64_t inip_get_integer(tbx_inip_file_t *inip, const char *group, const char *key, int64_t def);
uint64_t inip_get_unsigned_integer(tbx_inip_file_t *inip, const char *group, const char *key, uint64_t def);
TBX_API double inip_get_double(tbx_inip_file_t *inip, const char *group, const char *key, double def);
TBX_API tbx_inip_group_t *inip_find_group(tbx_inip_file_t *inip, const char *name);
TBX_API char *inip_find_key(tbx_inip_group_t *group, const char *name);


#ifdef __cplusplus
}
#endif

#endif

