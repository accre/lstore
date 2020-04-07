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

//*******************************************************************
//*******************************************************************

#define _log_module_index 110


#include <apr_pools.h>
#include <apr_thread_mutex.h>
#include <apr_signal.h>
#include <apr_thread_proc.h>
#include <unistd.h>

#include "tbx/fmttypes.h"
#include "tbx/siginfo.h"
#include "tbx/stack.h"
#include "tbx/apr_wrapper.h"
#include "tbx/type_malloc.h"

#define MAX_SIG 65
char *_siginfo_name[MAX_SIG];
tbx_stack_t *_si_list[MAX_SIG];

apr_thread_mutex_t *_si_lock = NULL;
apr_pool_t *_si_pool = NULL;
apr_thread_t *_si_thread = NULL;

typedef struct {  // ** info handler task
    tbx_siginfo_fn_t fn;
    void *arg;
} si_task_t;

// ***************************************************************
// tbx_siginfo_handler_add - Adds a routine to run when info is requested
// ***************************************************************

void tbx_siginfo_handler_add(int signal, tbx_siginfo_fn_t fn, void *arg)
{
    si_task_t *t;

    apr_thread_mutex_lock(_si_lock);

    tbx_type_malloc_clear(t, si_task_t, 1);
    t->fn = fn;
    t->arg = arg;
    tbx_stack_push(_si_list[signal], t);
    apr_thread_mutex_unlock(_si_lock);
}

// ***************************************************************
// tbx_siginfo_handler_remove - Removes a routine to run when info is requested
// ***************************************************************

void tbx_siginfo_handler_remove(int signal, tbx_siginfo_fn_t fn, void *arg)
{
    si_task_t *t;

    if (_si_list == NULL) return;

    apr_thread_mutex_lock(_si_lock);
    tbx_stack_move_to_top(_si_list[signal]);
    while ((t = tbx_stack_get_current_data(_si_list[signal])) != NULL) {
        if ((t->fn == fn) && (t->arg == arg)) {
            tbx_stack_delete_current(_si_list[signal], 0, 1);
            break;
        }

        tbx_stack_move_down(_si_list[signal]);
    }
    apr_thread_mutex_unlock(_si_lock);

    return;
}

// ***************************************************************
// siginfo_thread - Runs the siginfo handlers. This is done
//     in a separate thead to avoid any interrupt deadlocks in
//     case a handler needs a lock
// ***************************************************************

void *siginfo_thread(apr_thread_t *th, void *data)
{
    FILE *fd;
    si_task_t *t;
    char sname[100];
    apr_status_t ret = 0;
    long signal = (long)data;

    if ((signal < 1) || (signal > MAX_SIG)) return(NULL);

    snprintf(sname, sizeof(sname), "/tmp/siginfo-%ld.log", signal);

    apr_thread_detach(th);  //** Detach us

    apr_thread_mutex_lock(_si_lock);
    fd = fopen((_siginfo_name[signal]) ? _siginfo_name[signal] : sname, "w");
    if (fd == NULL) {
        log_printf(0, "Unable to dump info to %s for signal %ld\n", _siginfo_name[signal], signal);
        goto failed;
    }

    tbx_stack_move_to_top(_si_list[signal]);
    while ((t = tbx_stack_get_current_data(_si_list[signal])) != NULL) {
        t->fn(t->arg, fd);
        tbx_stack_move_down(_si_list[signal]);
    }
    fclose(fd);

failed:
    apr_thread_mutex_unlock(_si_lock);

    apr_thread_exit(th, ret);
    return(NULL);
}

// ***************************************************************
// tbx_siginfo_handler - Just launches the handler into a different
//    thread to avoid any potential INT handler deadlocks
// ***************************************************************

void tbx_siginfo_handler(int sig)
{
    long signal = sig;
    apr_thread_mutex_lock(_si_lock);
    tbx_thread_create_assert(&_si_thread, NULL, siginfo_thread, (void *)signal, _si_pool);
    apr_thread_mutex_unlock(_si_lock);
}

// ***************************************************************
// tbx_siginfo_install - Installs the information signal handler
//    Can be called multiple times to change the signal and file name
// ***************************************************************

void tbx_siginfo_install(char *fname, int signal)
{
    if (!_si_pool) {
        apr_pool_create(&_si_pool, NULL);
        apr_thread_mutex_create(&_si_lock, APR_THREAD_MUTEX_DEFAULT, _si_pool);
    }

    if ((signal < 1) || (signal > MAX_SIG)) return;

    apr_thread_mutex_lock(_si_lock);

    if (!_si_list[signal])  _si_list[signal] = tbx_stack_new();

    if (_siginfo_name[signal]) free(_siginfo_name[signal]);
    _siginfo_name[signal] = fname;

    apr_signal_unblock(signal);
    apr_signal(signal, tbx_siginfo_handler);
    apr_thread_mutex_unlock(_si_lock);
}


// ***************************************************************
// tbx_siginfo_shutdown - Uninstalls and shuts down the siginfo
//     handler
// ***************************************************************

void tbx_siginfo_shutdown()
{
    int i;
    if (!_si_pool) return;

    for (i=1; i<MAX_SIG; i++) {
        if (_si_list[i] != NULL) apr_signal_block(i);
        if(_siginfo_name[i] != NULL) free(_siginfo_name[i]);
    }

    apr_pool_destroy(_si_pool);
}

// ***************************************************************
// siginfo_sample_fn - Sample info handler
// ***************************************************************

void siginfo_sample_fn(void *arg, FILE *fd)
{
    fprintf(fd, "This is a test\n");
}
