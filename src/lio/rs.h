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
//***********************************************************************
// Generic resource managment service
//***********************************************************************


#ifndef _RESOURCE_SERVICE_H_
#define _RESOURCE_SERVICE_H_

#include <apr_hash.h>
#include <apr_thread_cond.h>
#include <lio/rs.h>

#include "ds.h"
#include "ex3/types.h"

#ifdef __cplusplus
extern "C" {
#endif

#define RS_SM_AVAILABLE "rs_available"
#define RS_SM_RUNNING   "rs_running"

#define RS_ERROR_OK                  -100 //** No error everything is good
#define RS_ERROR_NOT_ENOUGH_RIDS     -101 //** Didn't find enough RIDs for the request
#define RS_ERROR_FIXED_MATCH_FAIL    -102 //** The fixed RID failed to match the request
#define RS_ERROR_FIXED_NOT_FOUND     -103 //** The fixed RID couldn't be located in the RID table
#define RS_ERROR_HINTS_INVALID_LOCAL -104 //** The fixed RID's local RSQ contained a pickone or unique which isn't supported
#define RS_ERROR_EMPTY_STACK         -105 //** The Query stack is empty so the query is probably malformed

#define RS_STATUS_UP           0  //** The RID is enabled and available for use
#define RS_STATUS_IGNORE       1  //** Disabled via config file
#define RS_STATUS_OUT_OF_SPACE 2  //** The RID is disabled due to space
#define RS_STATUS_DOWN         3  //** Can't connect to RID



struct rs_space_t {
    int n_rids_total;
    int n_rids_free;
    int n_rids_status[3];
    int64_t used_total;   //** *_total is for everything in the config file.  Ignoring state
    int64_t free_total;
    int64_t total_total;
    int64_t used_up;;     //** Just totals for depot up, i.e. RS_STATUS_ON
    int64_t free_up;
    int64_t total_up;
};


struct  rs_hints_t {  //** Used for passing existing RID's and individual queries to the RS requestor and validation or repair
    char *fixed_rid_key;  //** RID key for existing/fixed index
    int  status;    //** Status of the fixed match or INVALID_LOCAL if a problem occurs with the local_rsq.  Returns one of the error codes above
    rs_query_t *local_rsq;  //** Local query appended to the global queury just for this allocation  used for both fixed and new
    apr_hash_t *pick_from;  //** List of resources to pick from
};

struct rs_request_t {
    op_generic_t *gop;
    int rid_index;
    ex_off_t size;
    char *rid_key;
};




typedef resource_service_fn_t *(rs_create_t)(void *arg, tbx_inip_file_t *ifd, char *section);


#define rs_type(rs)  (rs)->type
#define rs_get_rid_value(rs, rid_key, key) (rs)->get_rid_value(rs, rid_key, key)
#define rs_translate_cap_set(rs, rid_key, cs) (rs)->translate_cap_set(rs, rid_key, cs)
#define rs_query_dup(rs, q) (rs)->query_dup(rs, q)
#define rs_data_request(rs, da, q, caps, req, n_req, hints_list, fixed_size, n_rid, ignore_fixed_err, to) (rs)->data_request(rs, da, q, caps, req, n_req, hints_list, fixed_size, n_rid, ignore_fixed_err, to)
#define rs_destroy_service(rs) (rs)->destroy_service(rs)

rs_space_t rs_space(char *config);

void resource_service_destroy(resource_service_fn_t *rsf);

#ifdef __cplusplus
}
#endif

#endif
