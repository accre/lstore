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

#ifndef _IBP_TYPES_H_
#define _IBP_TYPES_H_

#include <time.h>
#include <stdint.h>

#include "ibp/ibp_types.h"
#include "ibp/ibp_visibility.h"


#ifdef __cplusplus
extern "C" {
#endif
/*********************************************************
 * Constant definition
 *********************************************************/

# define CMD_BUF_SIZE      1024



/*********************************************************
 * Data sturcture definition.
 *********************************************************/

/*
 * Definition of the timer structure
 */





struct ibp_ridlist_t {  //** RID list structure
    int n;
    rid_t *rl;
};

struct ibp_alias_capstatus_t {  //** Alias cap status
    int read_refcount;
    int write_refcount;
    ibp_off_t offset;
    ibp_off_t size;
    long int duration;
};

//*** ibp_types.c **
ibp_depot_t *new_ibp_depot();
void destroy_ibp_depot(ibp_depot_t *d);
ibp_attributes_t *new_ibp_attributes();
void destroy_ibp_attributes(ibp_attributes_t *attr);
ibp_timer_t *new_ibp_timer();
void destroy_ibp_timer(ibp_timer_t *t);
void get_ibp_timer(ibp_timer_t *t, int *client_timeout, int *server_timeout);
ibp_cap_t *dup_ibp_cap(ibp_cap_t *src);
void copy_ibp_capset(ibp_capset_t *src, ibp_capset_t *dest);
ibp_capstatus_t *new_ibp_capstatus();
void ibp_cap_destroystatus(ibp_capstatus_t *cs);
void copy_ibp_capstatus(ibp_capstatus_t *src, ibp_capstatus_t *dest);
ibp_alias_capstatus_t *new_ibp_alias_capstatus();
void destroy_ibp_alias_capstatus(ibp_alias_capstatus_t *cs);
void copy_ibp_alias_capstatus(ibp_alias_capstatus_t *src, ibp_alias_capstatus_t *dest);
void get_ibp_alias_capstatus(ibp_alias_capstatus_t *cs, int *readcount, int *writecount,
                             ibp_off_t *offset, ibp_off_t *size, int *duration);
void ridlist_init(ibp_ridlist_t *rlist, int size);

#ifdef __cplusplus
}
#endif

#endif
