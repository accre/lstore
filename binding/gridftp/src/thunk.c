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
 * @file thunk.c
 * Entry point(s) from the GridFTP side to LStore
 */

#include <lio/lio.h>
#include <stdio.h>

#include "lstore_dsi.h"

int activate() {
    printf("Loaded\n");

    int argc = 2;
    char **argv = malloc(sizeof(char *)*argc);
    argv[0] = "lio_gridftp";
    argv[1] = "-c";
    argv[2] = "/etc/lio/lio-gridftp.cfg";

    char **argvp = argv;
    lio_init(&argc, &argvp);
    if (!lio_gc) {
        printf("Failed to load LStore\n");
        return 1;
    }
    free(argv);

    return 0;
}

int deactivate() {
    printf("Unloaded\n");
    lio_shutdown();
    return 0;
}

int user_connect(lstore_handle_t *h, globus_gfs_operation_t op) {
    printf("Connect\n");
    memcpy(h->op, &op, sizeof(op));
    return 0;
}

int user_close(lstore_handle_t *h) {
    printf("Close\n");
    return 0;
}
