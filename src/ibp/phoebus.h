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


#ifndef __PHOEBUS_H
#define __PHOEBUS_H

#include <stdio.h>
#include "config.h"
#include "iniparse.h"

#ifdef _ENABLE_PHOEBUS
#include "liblsl_client.h"
#else 
  typedef void liblslSess;
#endif


#define LSL_DEPOTID_LEN 60

#ifdef __cplusplus
extern "C" {
#endif
   
typedef struct {
   char *key;  
   char *path_string;
   char **path;
   int p_count;
   int free_path;
} phoebus_t;
   
extern phoebus_t *global_phoebus;

void phoebus_init(void);
void phoebus_destroy(void);
int phoebus_print(char *buffer, int *used, int nbytes);
void phoebus_load_config(inip_file_t *kf);
void phoebus_path_set(phoebus_t *p, const char *path);
void phoebus_path_destroy(phoebus_t *p);
void phoebus_path_to_string(char *string, int max_size, phoebus_t *p);
char *phoebus_get_key(phoebus_t *p);

//char **split(char*, char*, int *);

#ifdef __cplusplus
}
#endif

#endif
