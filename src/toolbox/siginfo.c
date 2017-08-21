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


char *_siginfo_name = NULL;
tbx_stack_t *_si_list = NULL;
apr_thread_mutex_t *_si_lock = NULL;
apr_thread_t *_si_thread = NULL;
apr_thread_t *_si_thread_join = NULL;
apr_pool_t *_si_pool = NULL;
int _signal = -1234;

typedef struct {  // ** info handler task
    tbx_siginfo_fn_t fn;
    void *arg;
} si_task_t;

// ***************************************************************
// tbx_siginfo_handler_add - Adds a routine to run when info is requested
// ***************************************************************

void tbx_siginfo_handler_add(tbx_siginfo_fn_t fn, void *arg)
{
    si_task_t *t;

    apr_thread_mutex_lock(_si_lock);

    tbx_type_malloc_clear(t, si_task_t, 1);
    t->fn = fn;
    t->arg = arg;
    tbx_stack_push(_si_list, t);
    apr_thread_mutex_unlock(_si_lock);
}

// ***************************************************************
// tbx_siginfo_handler_remove - Removes a routine to run when info is requested
// ***************************************************************

void tbx_siginfo_handler_remove(tbx_siginfo_fn_t fn, void *arg)
{
    si_task_t *t;

    if (_si_list == NULL) return;

    apr_thread_mutex_lock(_si_lock);
    tbx_stack_move_to_top(_si_list);
    while ((t = tbx_stack_get_current_data(_si_list)) != NULL) {
        if ((t->fn == fn) && (t->arg == arg)) {
            tbx_stack_delete_current(_si_list, 0, 1);
            break;
        }

        tbx_stack_move_down(_si_list);
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
    apr_status_t ret = 0;

    apr_thread_mutex_lock(_si_lock);
    if (_si_thread_join) apr_thread_join(&ret, _si_thread_join);  //** Cleanup old invocation if needed

    fd = fopen((_siginfo_name) ? _siginfo_name : "/tmp/siginfo.log", "w");
    if (fd == NULL) {
        log_printf(0, "Unable to dump info to %s\n", _siginfo_name);
        return(NULL);
    }

    tbx_stack_move_to_top(_si_list);
    while ((t = tbx_stack_get_current_data(_si_list)) != NULL) {
        t->fn(t->arg, fd);
        tbx_stack_move_down(_si_list);
    }
    apr_thread_mutex_unlock(_si_lock);

    fclose(fd);

    _si_thread_join = th;  //** Track us for cleanup

    apr_thread_exit(th, ret);
    return(NULL);
}

// ***************************************************************
// tbx_siginfo_handler - Just launches the handler into a different
//    thread to avoid any potential INT handler deadlocks
// ***************************************************************

void tbx_siginfo_handler(int sig)
{
    apr_status_t status;

    if (_si_thread) apr_thread_join(&status, _si_thread);
    tbx_thread_create_assert(&_si_thread, NULL, siginfo_thread, NULL, _si_pool);
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
        _si_list = tbx_stack_new();
    }

    apr_thread_mutex_lock(_si_lock);

    if (_siginfo_name) free(_siginfo_name);
    _siginfo_name = fname;

    if (_signal != -1234) apr_signal_block(_signal);
    _signal = signal;
    apr_signal_unblock(_signal);
    apr_signal(_signal, tbx_siginfo_handler);
    apr_thread_mutex_unlock(_si_lock);
}


// ***************************************************************
// tbx_siginfo_shutdown - Uninstalls and shuts down the siginfo
//     handler
// ***************************************************************

void tbx_siginfo_shutdown()
{
    apr_status_t ret;

    if (!_si_pool) return;

    if (_si_thread_join) apr_thread_join(&ret, _si_thread_join);  //** Cleanup old invocation if needed

    if (_signal != -1234) apr_signal_block(_signal);
    if (_siginfo_name) free(_siginfo_name);
    apr_pool_destroy(_si_pool);
}

// ***************************************************************
// siginfo_sample_fn - Sample info handler
// ***************************************************************

void siginfo_sample_fn(void *arg, FILE *fd)
{
    fprintf(fd, "This is a test\n");
}
