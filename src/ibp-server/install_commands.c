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

#include <ibp-server/ibp_server.h>

//*************************************************************************
//  install_commands - Install all the depot commans in the global function table
//*************************************************************************

void install_commands(tbx_inip_file_t *kf) {

  //** Load the default ACLs
  global_config->server.default_acl = tbx_inip_get_string(kf, "access_control", "default", "open");

  //** Default commands **
  add_command(IBP_ALLOCATE, "ibp_allocate", kf, NULL, NULL, NULL, NULL, read_allocate, handle_allocate);
  add_command(IBP_SPLIT_ALLOCATE, "ibp_split_allocate", kf, NULL, NULL, NULL, NULL, read_allocate, handle_allocate);
  add_command(IBP_MERGE_ALLOCATE, "ibp_merge_allocate", kf, NULL, NULL, NULL, NULL, read_merge_allocate, handle_merge);
  add_command(IBP_STATUS, "ibp_status", kf, NULL, NULL, NULL, NULL, read_status, handle_status);
  add_command(IBP_MANAGE, "ibp_manage", kf, NULL, NULL, NULL, NULL, read_manage, handle_manage);
  add_command(IBP_WRITE, "ibp_write", kf, NULL, NULL, NULL, NULL, read_write, handle_write);
  add_command(IBP_STORE, "ibp_store", kf, NULL, NULL, NULL, NULL, read_write, handle_write);
  add_command(IBP_LOAD, "ibp_load", kf, NULL, NULL, NULL, NULL, read_read, handle_read);
  add_command(IBP_SEND, "ibp_send", kf, NULL, NULL, NULL, NULL, read_read, handle_copy);
  add_command(IBP_PHOEBUS_SEND, "ibp_phoebus_send", kf, phoebus_load_config, phoebus_init, phoebus_destroy, phoebus_print, read_read, handle_copy);
  add_command(IBP_RENAME, "ibp_rename", kf, NULL, NULL, NULL, NULL, read_rename, handle_rename);
  add_command(IBP_ALIAS_ALLOCATE, "ibp_alias_allocate", kf, NULL, NULL, NULL, NULL, read_alias_allocate, handle_alias_allocate);
  add_command(IBP_ALIAS_MANAGE, "ibp_alias_manage", kf, NULL, NULL, NULL, NULL, read_manage, handle_manage);
  add_command(IBP_PUSH, "ibp_tbx_stack_push", kf, NULL, NULL, NULL, NULL, read_read, handle_copy);
  add_command(IBP_PULL, "ibp_pull", kf, NULL, NULL, NULL, NULL, read_read, handle_copy);
  add_command(IBP_VEC_WRITE, "ibp_write", kf, NULL, NULL, NULL, NULL, read_write, handle_write);
  add_command(IBP_VEC_READ, "ibp_load", kf, NULL, NULL, NULL, NULL, read_read, handle_read);

  //** Chksum version of commands
  add_command(IBP_ALLOCATE_CHKSUM, "ibp_allocate", kf, NULL, NULL, NULL, NULL, read_allocate, handle_allocate);
  add_command(IBP_SPLIT_ALLOCATE_CHKSUM, "ibp_split_allocate", kf, NULL, NULL, NULL, NULL, read_allocate, handle_allocate);
  add_command(IBP_WRITE_CHKSUM, "ibp_write", kf, NULL, NULL, NULL, NULL, read_write, handle_write);
  add_command(IBP_STORE_CHKSUM, "ibp_store", kf, NULL, NULL, NULL, NULL, read_write, handle_write);
  add_command(IBP_LOAD_CHKSUM, "ibp_load", kf, NULL, NULL, NULL, NULL, read_read, handle_read);
  add_command(IBP_SEND_CHKSUM, "ibp_send", kf, NULL, NULL, NULL, NULL, read_read, handle_copy);
  add_command(IBP_PHOEBUS_SEND_CHKSUM, "ibp_phoebus_send", kf, phoebus_load_config, phoebus_init, phoebus_destroy, phoebus_print, read_read, handle_copy);
  add_command(IBP_PUSH_CHKSUM, "ibp_tbx_stack_push", kf, NULL, NULL, NULL, NULL, read_read, handle_copy);
  add_command(IBP_PULL_CHKSUM, "ibp_pull", kf, NULL, NULL, NULL, NULL, read_read, handle_copy);
  add_command(IBP_VALIDATE_CHKSUM, "ibp_validate_chksum", kf, NULL, NULL, NULL, NULL, read_validate_get_chksum, handle_validate_chksum);
  add_command(IBP_GET_CHKSUM, "ibp_get_chksum", kf, NULL, NULL, NULL, NULL, read_validate_get_chksum, handle_get_chksum);
  add_command(IBP_VEC_WRITE_CHKSUM, "ibp_write", kf, NULL, NULL, NULL, NULL, read_write, handle_write);
  add_command(IBP_VEC_READ_CHKSUM, "ibp_load", kf, NULL, NULL, NULL, NULL, read_read, handle_read);

  //*** Extra commands go below ****
  add_command(INTERNAL_GET_CORRUPT, "internal_get_corrupt", kf, NULL, NULL, NULL, NULL, read_internal_get_corrupt, handle_internal_get_corrupt);
  add_command(INTERNAL_GET_CONFIG, "internal_get_config", kf, NULL, NULL, NULL, NULL, read_internal_get_config, handle_internal_get_config);
  add_command(INTERNAL_GET_ALLOC, "internal_get_alloc", kf, NULL, NULL, NULL, NULL, read_internal_get_alloc, handle_internal_get_alloc);
  add_command(INTERNAL_DATE_FREE, "internal_date_free", kf, NULL, NULL, NULL, NULL, read_internal_date_free, handle_internal_date_free);
  add_command(INTERNAL_EXPIRE_LIST, "internal_expire_list", kf, NULL, NULL, NULL, NULL, read_internal_expire_list, handle_internal_expire_list);
  add_command(INTERNAL_UNDELETE, "internal_undelete", kf, NULL, NULL, NULL, NULL, read_internal_undelete, handle_internal_undelete);
  add_command(INTERNAL_RESCAN, "internal_rescan", kf, NULL, NULL, NULL, NULL, read_internal_rescan, handle_internal_rescan);
  add_command(INTERNAL_RID_MOUNT, "internal_rid_mount", kf, NULL, NULL, NULL, NULL, read_internal_mount, handle_internal_mount);
  add_command(INTERNAL_RID_UMOUNT, "internal_rid_umount", kf, NULL, NULL, NULL, NULL, read_internal_umount, handle_internal_umount);
  add_command(INTERNAL_RID_SET_MODE, "internal_rid_set_mode", kf, NULL, NULL, NULL, NULL, read_internal_set_mode, handle_internal_set_mode);

}

