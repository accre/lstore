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

/**
 * @file test.c
 * Simple test program to call the stubs and verify their correctness
 */

#include <stdio.h>

#include "lstore_dsi.h"

#define CHECK_EQUAL(V1,V2) if ((V1) != (V2)) { printf( #V1 " != " #V2 "\n" ); return 1; }

int main() {
    printf("Hello, World!\n");

    CHECK_EQUAL(globus_gridftp_server_lstore_module.activation_func(), 0);
    lstore_handle_t handle;
    globus_gfs_operation_t op;
    memset(&op, 42, sizeof(op));
    CHECK_EQUAL(user_connect(&handle, op), 0);


    // Verify stat works
    globus_gfs_stat_info_t stat_info;
    memset(&stat_info, 42, sizeof(stat_info));
    stat_info.pathname = "/lio/lfs/ONLYATVANDY-LFS.txt";
    stat_info.file_only = 1;
    globus_gfs_stat_t *ret = NULL;
    int ret_count = 0;
    tbx_stack_t *stack = tbx_stack_new();

    CHECK_EQUAL(plugin_stat(&handle, stack, "/lio/lfs/ONLYATVANDY-LFS.txt", 0), 0);
    CHECK_EQUAL(user_stat(&handle, &stat_info, &ret, &ret_count), 0);
    free(ret);
    free(stack);

    printf("Found %d matching files\n", ret_count);

    CHECK_EQUAL(user_close(&handle), 0);
    CHECK_EQUAL(globus_gridftp_server_lstore_module.deactivation_func(), 0);
    return 0;
}
