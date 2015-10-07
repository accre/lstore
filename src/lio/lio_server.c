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

#define _log_module_index 218


#include <assert.h>
#include <apr_signal.h>
#include "lio_fuse.h"
#include "exnode.h"
#include "log.h"
#include "iniparse.h"
#include "type_malloc.h"
#include "thread_pool.h"
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
    int i, start_option, start_index;

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

    start_index = i;

    //***Attach the signal handler for shutdown
    apr_signal_unblock(SIGQUIT);
    apr_signal(SIGQUIT, signal_shutdown);

    //** Want everyone to ignore SIGPIPE messages
#ifdef SIGPIPE
    apr_signal_block(SIGPIPE);
#endif

    //** Make the APR stuff
    assert(apr_pool_create(&mpool, NULL) == APR_SUCCESS);
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
