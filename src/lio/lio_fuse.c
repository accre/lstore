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

#define _log_module_index 211


#include <stdio.h>
#include <string.h>
#include <sys/stat.h>

#include "lio_fuse.h"

//*************************************************************************
//*************************************************************************

void print_usage(void)
{
    printf("\n"
           "lio_fuse mount_point [FUSE_OPTIONS] [--lio LIO_COMMON_OPTIONS]\n");
    lio_print_options(stdout);
    printf("    FUSE_OPTIONS:\n"
           "       -h   --help            print this help\n"
           "       -ho                    print FUSE mount options help\n"
           "       -V   --version         print FUSE version\n"
           "       -d   -o debug          enable debug output (implies -f)\n"
           "       -s                     disable multi-threaded operation\n"
           "       -f                     foreground operation\n"
           "                                (REQUIRED unless  '-c /absolute/path/lio.cfg' is specified and all included files are absolute paths)"
           "       -o OPT[,OPT...]        mount options\n"
           "                                (for possible values of OPT see 'man mount.fuse' or see 'lio_fuse -ho')\n");
}

int main(int argc, char **argv)
{
    int err = -1;
    lio_fuse_init_args_t lio_args;
    int fuse_argc;
    char **fuse_argv;

    if (argc < 2 || strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0) {
        print_usage();
        return(1);
    }

    // ** split lio and fuse arguments **

    int idx;

    // these defaults hold if --lio is not used on the commandline
    fuse_argc = argc;
    fuse_argv = argv;
    lio_args.lio_argc = 1;
    lio_args.lio_argv = argv;
    lio_args.mount_point = argv[1];

    for (idx=1; idx<argc; idx++) {
        if(strcmp(argv[idx], "--lio") == 0) {
            fuse_argc = idx;
            lio_args.mount_point = argv[fuse_argc-1];  //** The last FUSE argument is the mount point

            lio_args.lio_argc = argc - idx;
            lio_args.lio_argv = &argv[idx];
            lio_args.lio_argv[0] = argv[0]; //replace "--lio" with the executable name because the parser may reasonably expect the zeroth argument to be the program name
        }
    }

    umask(0);

    err = fuse_main(fuse_argc, fuse_argv, &lfs_fops, &lio_args /* <- stored to fuse's ctx->private_data*/);

    return(err);
}
