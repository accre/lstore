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
// OS file header file
//***********************************************************************

#include "object_service_abstract.h"
#include "authn_abstract.h"
#include "chksum.h"
#include <openssl/md5.h>

#ifndef _OS_FILE_PRIV_H_
#define _OS_FILE_PRIV_H_

#ifdef __cplusplus
extern "C" {
#endif

#define SAFE_MIN_LEN 2

//#define FILE_ATTR_PREFIX "##FILE_ATTRIBUTES##"
#define FILE_ATTR_PREFIX "_^FA^_"
#define FILE_ATTR_PREFIX_LEN 6

#define DIR_PERMS S_IRWXU|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH
#define OSF_LOCK_CHKSUM CHKSUM_MD5
#define OSF_LOCK_CHKSUM_SIZE MD5_DIGEST_LENGTH

typedef struct {
    int base_path_len;
    int file_path_len;
    int hardlink_path_len;
    int internal_lock_size;
    int hardlink_dir_size;
    atomic_int_t hardlink_count;
    char *base_path;
    char *file_path;
    char *hardlink_path;
    char *host_id;
    thread_pool_context_t *tpc;
    apr_thread_mutex_t **internal_lock;
    os_authz_t *osaz;
    authn_t *authn;
    apr_pool_t *mpool;
    list_t *fobj_table;
    apr_hash_t *vattr_hash;
    list_t *vattr_prefix;
    apr_thread_mutex_t *fobj_lock;
    pigeon_coop_t *fobj_pc;
    pigeon_coop_t *task_pc;
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
} osfile_priv_t;


#ifdef __cplusplus
}
#endif

#endif

