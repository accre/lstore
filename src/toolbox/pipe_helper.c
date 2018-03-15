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

#include <apr_time.h>
#include <fcntl.h>
#include <sys/select.h>
#include <unistd.h>
#include <tbx/pipe_helper.h>
#include <tbx/log.h>



//************************************************************************************

void tbx_pipe_close(int fd[2])
{
    if (fd[0] != -1) close(fd[0]);
    if (fd[1] != -1) close(fd[1]);
    return;
}

//************************************************************************************

int tbx_pipe_open(int fd[2])
{
    int err;

    err = pipe(fd);
    if (err != 0) return(err);

    err = fcntl(fd[0], F_SETFL, O_NONBLOCK|O_DIRECT);
    if (err != 0) goto oops;
    err = fcntl(fd[1], F_SETFL, O_NONBLOCK|O_DIRECT);
    if (err != 0) goto oops;

    return(0);

oops:
    close(fd[0]);
    close(fd[1]);

    return(err);
}

//************************************************************************************

int tbx_pipe_put(int fd[2], void *object, int object_size, apr_time_t dt)
{
    int err;
    fd_set set;
    struct timeval tv;

    if (object_size > PIPE_BUF) return(-2);

again:
    tv.tv_sec = dt / APR_USEC_PER_SEC;
    tv.tv_usec = dt % APR_USEC_PER_SEC;
    FD_ZERO(&set);
    FD_SET(fd[1], &set);
    err = select(fd[1]+1, NULL, &set, NULL, &tv);
    if (err != 1) return(-2);

    err = write(fd[1], object, object_size);
    if (err == object_size) {
        err = 0;
    } else if (errno == EAGAIN) {  //** Somebody beat us to the pipe so try again
        goto again;
    } else {
        err = -1;
    }

    return(err);
}

//************************************************************************************

int tbx_pipe_get(int fd[2], void *object, int object_size, apr_time_t dt)
{
    int err;
    fd_set set;
    struct timeval tv;

    if (object_size > PIPE_BUF) return(-2);

again:
    tv.tv_sec = dt / APR_USEC_PER_SEC;
    tv.tv_usec = dt % APR_USEC_PER_SEC;
    FD_ZERO(&set);
    FD_SET(fd[0], &set);
    err = select(fd[0]+1, &set, NULL, NULL, &tv);
    if (err != 1) return(-2);

    err = read(fd[0], object, object_size);
    if (err == object_size) {
        err = 0;
    } else if (errno == EAGAIN) {  //** Somebody beat us to the object so try again
        goto again;
    } else {
        err = -1;
    }


    return(err);
}