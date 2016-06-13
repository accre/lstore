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


#ifndef __PHOEBUS_H
#define __PHOEBUS_H

#include "ibp/ibp_visibility.h"
#include <stdio.h>
#include "config.h"
#include <tbx/iniparse.h>

#ifdef _ENABLE_PHOEBUS
#include "liblsl_client.h"
#else
typedef void liblslSess;
#endif


#define LSL_DEPOTID_LEN 60

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tbx_phoebus_t tbx_phoebus_t;
struct tbx_phoebus_t {
    char *key;
    char *path_string;
    char **path;
    int p_count;
    int free_path;
};

extern tbx_phoebus_t *global_phoebus;

void phoebus_init(void);
void phoebus_destroy(void);
int phoebus_print(char *buffer, int *used, int nbytes);
void phoebus_load_config(tbx_inip_file_t *kf);
IBP_API void ibp_phoebus_path_set(tbx_phoebus_t *p, const char *path);
void phoebus_path_destroy(tbx_phoebus_t *p);
void phoebus_path_to_string(char *string, int max_size, tbx_phoebus_t *p);
char *phoebus_get_key(tbx_phoebus_t *p);

#ifdef __cplusplus
}
#endif

#endif
