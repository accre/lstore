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

#define _log_module_index 153

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <dlfcn.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <apr_time.h>
#include <stdarg.h>
#include "apr_wrapper.h"
#include "fmttypes.h"
#include "log.h"
#include "type_malloc.h"
#include "liblio_trace.h"
#include "iniparse.h"
#include "append_printf.h"
#include "atomic_counter.h"

lt_config_t ltc;
lt_fn_t lt_fn;

atomic_int_t _trace_count = 0;

fd_trace_t *fd_table = NULL;
fd_trace_t *fd_stats = NULL;

apr_time_t start_time;
atomic_int_t curr_fd_slot = 0;

//*************************************************************
//*************************************************************


//** Declare the constructor and destructor
void  __attribute__ ((constructor)) liblio_trace_init(void);
void  __attribute__ ((destructor)) liblio_trace_fini(void);

//*************************************************************
// lt_default_config - sets the default config
//*************************************************************

void lt_default_config()
{
    set_log_level(15);
    open_log("trace.log");

    ltc.trace_name = "output.trace";
    ltc.trace_header = "output.trh";
    ltc.max_fd = 100;
}

//*************************************************************
// lt_load_config - Loads a config
//*************************************************************

void lt_load_config(char *fname)
{
    inip_file_t *fd;
    char *str;
    int n;

    fd = inip_read(fname);

    n = inip_get_integer(fd, LIBLIO_TRACE_SECTION, "log_level", log_level());
    set_log_level(n);

    str = inip_get_string(fd, LIBLIO_TRACE_SECTION, "log_file", NULL);
    if (str != NULL) open_log(str);

    str = inip_get_string(fd, LIBLIO_TRACE_SECTION, "output", NULL);
    if (str != NULL) ltc.trace_name = str;

    str = inip_get_string(fd, LIBLIO_TRACE_SECTION, "header", NULL);
    if (str != NULL) ltc.trace_header = str;

    ltc.max_fd = inip_get_integer(fd, LIBLIO_TRACE_SECTION, "max_fd", ltc.max_fd);

    ltc.logfd = STDERR_FILENO;

    inip_destroy(fd);
}

//*************************************************************
// liblio_trace_init - Initilaizes the tracing system
//*************************************************************

void __attribute__((constructor)) liblio_trace_init()
{
    int i;
    char *config = NULL;
    void *handle;
    apr_wrapper_start();

    log_printf(10, "_liblio_trace_init: Initializing system\n");

    lt_default_config();

    config = getenv("LIBLIO_TRACE");
    if (config != NULL) {
        log_printf(10, "liblio_trace_init: Loading config file %s\n", config);
        lt_load_config(config);
    } else {
        log_printf(10, "liblio_trace_init: Using default config\n");
    }

    log_printf(10, "liblio_trace_init: LOG Storing output in %s\n", ltc.trace_name);

    //** Initialize fn ptrs
    handle = dlopen("libc.so.6", RTLD_LAZY);
    log_printf(10, "liblio_trace_init: handle=%p\n",handle);
    lt_fn.open = dlsym(handle, "open");
    log_printf(10, "liblio_trace_init: open=%p\n",lt_fn.open);
    lt_fn.close = dlsym(handle, "close");
    log_printf(10, "liblio_trace_init: close=%p\n",lt_fn.close);

    lt_fn.read = dlsym(handle, "read");
    log_printf(10, "liblio_trace_init: read=%p\n",lt_fn.read);
    lt_fn.write = dlsym(handle, "write");
    log_printf(10, "liblio_trace_init: write=%p\n",lt_fn.write);
    lt_fn.lseek = dlsym(handle, "lseek");
    log_printf(10, "liblio_trace_init: lseek=%p\n",lt_fn.lseek);


    type_malloc_clear(fd_stats, fd_trace_t, ltc.max_fd);
    type_malloc_clear(fd_table, fd_trace_t, ltc.max_fd);
    for (i=0; i<ltc.max_fd; i++) {
        fd_table[i].fd = -1;
    }

    ltc.fd = lt_fn.open(ltc.trace_name, O_WRONLY|O_TRUNC);
    if (ltc.fd == -1) {
        ltc.fd = lt_fn.open(ltc.trace_name, O_WRONLY|O_CREAT, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    }
//  sprintf(logstr, "liblio_trace_destroy: trace fd=%d\n", ltc.fd);
//  lt_fn.write(ltc.logfd, logstr, strlen(logstr));

    start_time = apr_time_now();

    log_printf(10, "liblio_trace_init: LOG END\n");

    return;
}

//*************************************************************
// liblio_trace_fini - Destroys the tracing system
//*************************************************************

void __attribute__((destructor)) liblio_trace_fini()

{
    int bufsize=10*1024;
    char header[bufsize+1];
    int used, n, i;
    fd_trace_t *fdt;
    int n_ops;

    log_printf(10, "liblio_trace_destroy: LOG Shutting down system fd=%d\n", ltc.fd);

    lt_fn.close(ltc.fd);

    //** Construct the header
    used = 0;
    header[0] = '\0';

    append_printf(header, &used, bufsize, "[trace]\n");
    append_printf(header, &used, bufsize, "n_files=%d\n", curr_fd_slot);
    append_printf(header, &used, bufsize, "trace=%s\n", ltc.trace_name);
    n_ops = atomic_get(_trace_count);
    append_printf(header, &used, bufsize, "n_ops=%d\n", n_ops);
    append_printf(header, &used, bufsize, "\n");

    n = atomic_get(curr_fd_slot);
    for (i=0; i<n; i++) {
        fdt = &(fd_stats[i]);

        append_printf(header, &used, bufsize, "[file-%d]\n", fdt->fd);
        append_printf(header, &used, bufsize, "path=%s\n", fdt->fname);
        append_printf(header, &used, bufsize, "init_size=%ld\n", fdt->init_size);
        append_printf(header, &used, bufsize, "max_size=%ld\n", fdt->max_size);
        append_printf(header, &used, bufsize, "block_size=%ld\n", fdt->block_size);
        append_printf(header, &used, bufsize, "\n");
    }

    log_printf(10, "liblio_trace_destroy: LOG Storing header in %s:\n%s\n", ltc.trace_header, header);

    //** Store the header **
    n = lt_fn.open(ltc.trace_header, O_WRONLY|O_TRUNC);
    if (n == -1) {
        n = lt_fn.open(ltc.trace_header, O_WRONLY|O_CREAT, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    }
    lt_fn.write(n, header, strlen(header));
    lt_fn.close(n);

    free(fd_table);
    free(fd_stats);

    apr_wrapper_stop();

    return;
}


//*************************************************************
// open - Open stub call
//*************************************************************

int open(const char *pathname, int flags, ...)
{
    va_list args;
    int fd;

    va_start(args, flags);
    fd = lt_fn.open(pathname, flags, args);
    va_end(args);

    log_printf(10, "liblio_trace: LOG open(%s,%d)=%d\n", pathname, flags, fd);

    if (fd == -1) return(-1);

    fd_table[fd].fd = atomic_inc(curr_fd_slot);
    fd_table[fd].init_size = lt_fn.lseek(fd, 0L, SEEK_END);
    lt_fn.lseek(fd, 0L, SEEK_SET);  //** Reposition to the beginning
    fd_table[fd].pos = 0;
    fd_table[fd].max_size = 0;
    fd_table[fd].block_size = 1;
    fd_table[fd].fname = strdup(pathname);

    return(fd);

}

//*************************************************************
// close - Close stub call
//*************************************************************

int close(int fd)
{
    int err;

    fd_table[fd].max_size = lt_fn.lseek(fd, 0L, SEEK_END);

    fd_stats[fd_table[fd].fd] = fd_table[fd]; //** Store the stats

    err = lt_fn.close(fd);
    log_printf(10, "liblio_trace: LOG close(%d)=%d\n", fd, err);

    return(err);

}

//*************************************************************
// read - read stub call
//*************************************************************

ssize_t read(int fd, void *buf, size_t count)
{
    ssize_t result;
    long int nbytes=count;
    long int err;
    int n;
    char text[1024];
    double dt;

    atomic_inc(_trace_count);

    result = lt_fn.read(fd, buf, count);
    if (fd_table[fd].fd < 0) return(result);

    err = result;

    dt = apr_time_now() - start_time;
    dt = dt / APR_USEC_PER_SEC;

    n = sprintf(text, "%d, %ld, %ld, R, %lf\n", fd_table[fd].fd, fd_table[fd].pos, err, dt);
    lt_fn.write(ltc.fd, text, n);

    fd_table[fd].pos += result;

    log_printf(10, "liblio_trace: read(%d, buf, %ld)=%ld\n", fd, nbytes, err);

    return(result);

}


//*************************************************************
// write - write stub call
//*************************************************************

ssize_t write(int fd, const void *buf, size_t count)
{
    ssize_t result;
    long int nbytes=count;
    long int err;
    int n;
    char text[1024];
    double dt;

    atomic_inc(_trace_count);

    result = lt_fn.write(fd, buf, count);
    err = result;

    log_printf(10, "liblio_trace: LOG EARLY write(%d, buf, %ld)=%ld\n", fd, nbytes, err);

    if (fd_table[fd].fd < 0) return(result);


    dt = apr_time_now() - start_time;
    dt = dt / APR_USEC_PER_SEC;

    n = sprintf(text, "%d, %ld, %ld, W, %lf\n", fd_table[fd].fd, fd_table[fd].pos, err, dt);
    lt_fn.write(ltc.fd, text, n);

    fd_table[fd].pos += result;

    log_printf(10, "liblio_trace: LOG write(%d, buf, %ld)=%ld\n", fd, nbytes, err);

    return(result);

}

//*************************************************************
// lseek - lseek stub call
//*************************************************************

off_t lseek(int fd, off_t offset, int whence)
{
    off_t result;

    result = lt_fn.lseek(fd, offset, whence);

    return(result);
}
