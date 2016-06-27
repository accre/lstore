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
// OS file header file
//***********************************************************************

#ifndef _OS_FILE_H_
#define _OS_FILE_H_

#include <openssl/md5.h>
#include <tbx/chksum.h>
#include <tbx/fmttypes.h>
#include <tbx/iniparse.h>

#include "authn.h"
#include "os.h"
#include "service_manager.h"

#ifdef __cplusplus
extern "C" {
#endif

#define OS_TYPE_FILE "file"

struct local_object_iter_t {
    object_service_fn_t *os;
    os_object_iter_t  *oit;
};

int local_next_object(local_object_iter_t *it, char **myfname, int *prefix_len);
local_object_iter_t *create_local_object_iter(os_regex_table_t *path, os_regex_table_t *object_regex, int object_types, int recurse_depth);
void destroy_local_object_iter(local_object_iter_t *it);

object_service_fn_t *object_service_file_create(service_manager_t *ess, tbx_inip_file_t *ifd, char *section);
int osf_store_val(void *src, int src_size, void **dest, int *v_size);

#define SAFE_MIN_LEN 2

#define FILE_ATTR_PREFIX "_^FA^_"
#define FILE_ATTR_PREFIX_LEN 6

#define DIR_PERMS S_IRWXU|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH
#define OSF_LOCK_CHKSUM CHKSUM_MD5
#define OSF_LOCK_CHKSUM_SIZE MD5_DIGEST_LENGTH

struct osfile_priv_t {
    int base_path_len;
    int file_path_len;
    int hardlink_path_len;
    int internal_lock_size;
    int hardlink_dir_size;
    tbx_atomic_unit32_t hardlink_count;
    char *base_path;
    char *file_path;
    char *hardlink_path;
    char *host_id;
    thread_pool_context_t *tpc;
    apr_thread_mutex_t **internal_lock;
    os_authz_t *osaz;
    authn_t *authn;
    apr_pool_t *mpool;
    tbx_list_t *fobj_table;
    apr_hash_t *vattr_hash;
    tbx_list_t *vattr_prefix;
    apr_thread_mutex_t *fobj_lock;
    tbx_pc_t *fobj_pc;
    tbx_pc_t *task_pc;
    os_virtual_attr_t lock_va;
    os_virtual_attr_t link_va;
    os_virtual_attr_t link_count_va;
    os_virtual_attr_t type_va;
    os_virtual_attr_t create_va;
    os_virtual_attr_t attr_link_pva;
    os_virtual_attr_t attr_type_pva;
    os_virtual_attr_t timestamp_pva;
    os_virtual_attr_t append_pva;
    int max_copy;
};


#ifdef __cplusplus
}
#endif

#endif
