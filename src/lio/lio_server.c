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

#define _log_module_index 218

#include <apr_errno.h>
#include <apr_pools.h>
#include <apr_signal.h>
#include <apr_thread_cond.h>
#include <apr_thread_mutex.h>
#include <apr_time.h>
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/assert_result.h>
#include <tbx/log.h>
#include <unistd.h>

#include "lio.h"

int shutdown_now = 0;
apr_thread_mutex_t *shutdown_lock;
apr_thread_cond_t *shutdown_cond;
apr_pool_t *mpool;


void signal_shutdown(int sig)
{
    char date[128];
    apr_ctime(date, apr_time_now());

    log_printf(0, "Shutdown requested on %s\n", date);

    apr_thread_mutex_lock(shutdown_lock);
    shutdown_now = 1;
    apr_thread_cond_signal(shutdown_cond);
    apr_thread_mutex_unlock(shutdown_lock);

    return;
}

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int background = 1;
    int i, start_option;

    for (i=0; i<argc; i++) {
        if (strcmp(argv[i], "-f") == 0) {
            background = 0;
            break;
        }
    }

    if (background == 1) {
        if (fork() == 0) {    //** This is the daemon
            fclose(stdin);     //** Need to close all the std* devices **
            fclose(stdout);
            fclose(stderr);
        } else {           //** Parent exits and doesn't close anything
            printf("Running as a daemon.\n");
            exit(0);
        }
    }

    if (argc < 2) {
        printf("\n");
        printf("lio_server LIO_COMMON_OPTIONS [-f] [-C cwd]\n");
        lio_print_options(stdout);
        printf("    -f                 - Run in foreground instead of as a daemon\n");
        printf("    -C cwd             - Change the current workding directory for execution\n");
        return(1);
    }

    lio_init(&argc, &argv);

    if (background == 1) {
        log_printf(0, "Running as a daemon.\n");
    }


    //** NOTE:  The "-f" option has already been handled but it will still appear in the list below cause lio_init() won't handle it
    i=1;
    if (i<argc) {
        do {
            start_option = i;

            if (strcmp(argv[i], "-C") == 0) {  //** Change the CWD
                i++;
                if (chdir(argv[i]) != 0) {
                    fprintf(stderr, "ERROR setting CWD=%s.  errno=%d\n", argv[i], errno);
                    log_printf(0, "ERROR setting CWD=%s.  errno=%d\n", argv[i], errno);
                } else {
                    log_printf(0, "Setting CWD=%s\n", argv[i]);
                }
                i++;
            } else if (strcmp(argv[i], "-f") == 0) {  //** Foreground process already handled
                i++;
            }
        } while ((start_option < i) && (i<argc));
    }

    //***Attach the signal handler for shutdown
    apr_signal_unblock(SIGQUIT);
    apr_signal(SIGQUIT, signal_shutdown);

    //** Want everyone to ignore SIGPIPE messages
#ifdef SIGPIPE
    apr_signal_block(SIGPIPE);
#endif

    //** Make the APR stuff
    assert_result(apr_pool_create(&mpool, NULL), APR_SUCCESS);
    apr_thread_mutex_create(&shutdown_lock, APR_THREAD_MUTEX_DEFAULT, mpool);
    apr_thread_cond_create(&shutdown_cond, mpool);

    //** Wait until a shutdown request is received
    apr_thread_mutex_lock(shutdown_lock);
    while (shutdown_now == 0) {
        apr_thread_cond_wait(shutdown_cond, shutdown_lock);
    }
    apr_thread_mutex_unlock(shutdown_lock);

    //** Cleanup
    apr_pool_destroy(mpool);

    lio_shutdown();

    return(0);
}
