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

//*************************************************************
//  ZeroMQ implementation of the MQ subsystem.
//  This is mainly a wrapper around 0MQ calls with an
//  extension of the 0MQ socket types
//*************************************************************

#include <apr_signal.h>
#include <assert.h>
#include <czmq.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/errno.h>
#include <sys/signal.h>
#include <tbx/fmttypes.h>
#include <tbx/log.h>
#include <tbx/random.h>
#include <tbx/stack.h>
#include <tbx/type_malloc.h>
#include <unistd.h>

#include "mq_portal.h"

//*************************************************************
//   Native routines
//*************************************************************

//*************************************************************

void zero_native_destroy(gop_mq_socket_context_t *ctx, gop_mq_socket_t *socket)
{
    zsocket_destroy((zctx_t *)ctx->arg, socket->arg);
    free(socket);
}

//*************************************************************

int zero_native_bind(gop_mq_socket_t *socket, const char *format, ...)
{
    va_list args;
    int err, n;
    char id[255];

    if (socket->type != MQ_PAIR) {
        va_start(args, format);
        snprintf(id, 255, format, args);
        zsocket_set_identity(socket->arg, id);
    } else {
        id[0] = 0;
    }
    err = zsocket_bind(socket->arg, format, args);
    n = errno;
    va_end(args);

    log_printf(0, "id=!%s! err=%d errno=%d\n", id, err, n);

    return((err == -1) ? -1 : 0);
}

//*************************************************************

int zero_native_connect(gop_mq_socket_t *socket, const char *format, ...)
{
    va_list args;
    int err;
    char buf[255], id[255];

    if (socket->type != MQ_PAIR) zsocket_set_router_mandatory(socket->arg, 1);

    va_start(args, format);
    //** Set the ID
    if (socket->type != MQ_PAIR) {
        snprintf(buf, 255, format, args);
        snprintf(id, 255, "%s:" I64T , buf, tbx_random_get_int64(1, 1000000));
        zsocket_set_identity(socket->arg, id);
        log_printf(4, "Unique hostname created = %s\n", id);
    }

    err = zsocket_connect(socket->arg, format, args);
    va_end(args);

    return(err);
}

//*************************************************************

int zero_native_disconnect(gop_mq_socket_t *socket, const char *format, ...)
{
    va_list args;
    int err;

    va_start(args, format);
    err = zsocket_disconnect(socket->arg, format, args);
    va_end(args);

    return(err);
}

//*************************************************************

void *zero_native_poll_handle(gop_mq_socket_t *socket)
{
    return(socket->arg);
}

//*************************************************************

int zero_native_monitor(gop_mq_socket_t *socket, char *address, int events)
{
    return(zmq_socket_monitor(socket->arg, address, events));
}

//*************************************************************

int zero_native_send(gop_mq_socket_t *socket, mq_msg_t *msg, int flags)
{
    gop_mq_frame_t *f, *fn;
    int n, loop, bytes;

    int count = 0;

    n = 0;
    f = gop_mq_msg_first(msg);
    if (f->len > 1) {
        log_printf(5, "dest=!%.*s! nframes=%d\n", f->len, (char *)(f->data), tbx_stack_count(msg));
    } else {
        log_printf(5, "dest=(single byte) nframes=%d\n", tbx_stack_count(msg));
    }

    while ((fn = gop_mq_msg_next(msg)) != NULL) {
        loop = 0;
        do {
            bytes = zmq_send(socket->arg, f->data, f->len, ZMQ_SNDMORE);
            if (bytes == -1) {
                if (errno == EHOSTUNREACH) {
                    usleep(100);
                } else {
                    FATAL_UNLESS(errno == EHOSTUNREACH);
                }
            }
            loop++;
            log_printf(15, "sending frame=%d len=%d bytes=%d errno=%d loop=%d\n", count, f->len, bytes, errno, loop);
            if (f->len>0) {
                log_printf(15, "byte=%uc\n", (unsigned char)f->data[0]);
            }
        } while ((bytes == -1) && (loop < 10));
        n += bytes;
        count++;
        f = fn;
    }

    if (f != NULL) n += zmq_send(socket->arg, f->data, f->len, 0);

    if (f != NULL) {
        log_printf(5, "last frame frame=%d len=%d ntotal=%d\n", count, f->len, n);
    } else {
        log_printf(0, "ERROR: missing last frame!\n");
    }
    return((n>0) ? 0 : -1);
}


//*************************************************************

int zero_native_recv(gop_mq_socket_t *socket, mq_msg_t *msg, int flags)
{
    gop_mq_frame_t *f;
    int n, nframes, rc;
    int64_t more = 0;
    size_t msize = sizeof(more);

    if ((flags & MQ_DONTWAIT) > 0) {
        more = 0;
        rc = zmq_getsockopt (socket->arg, ZMQ_EVENTS, &more, &msize);
        log_printf(5, "more=" I64T "\n", more);
        FATAL_UNLESS(rc == 0);
        if ((more & ZMQ_POLLIN) == 0) return(-1);
    }

    n = 0;
    nframes = 0;
    do {
        tbx_type_malloc(f, gop_mq_frame_t, 1);
        gop_mq_frame_t *prevent_overwrite = f;
        FATAL_UNLESS(prevent_overwrite == f);

        rc = zmq_msg_init(&(f->zmsg));
        FATAL_UNLESS(rc == 0);
        rc = zmq_msg_recv(&(f->zmsg), socket->arg, flags);
        FATAL_UNLESS(rc != -1);

        rc = zmq_getsockopt (socket->arg, ZMQ_RCVMORE, &more, &msize);
        FATAL_UNLESS(rc == 0);

        f->len = zmq_msg_size(&(f->zmsg));
        f->data = zmq_msg_data(&(f->zmsg));
        f->auto_free = MQF_MSG_INTERNAL_FREE;

        gop_mq_msg_append_frame(msg, f);
        n += f->len;
        nframes++;
        log_printf(5, "more=" I64T "\n", more);
        FATAL_UNLESS(prevent_overwrite == f);
    } while (more > 0);

    log_printf(5, "total bytes=%d nframes=%d\n", n, nframes);

    return((n>0) ? 0 : -1);
}

//*************************************************************

gop_mq_socket_t *zero_create_native_socket(gop_mq_socket_context_t *ctx, int stype)
{
    gop_mq_socket_t *s;

    tbx_type_malloc_clear(s, gop_mq_socket_t, 1);

    s->type = stype;
    s->arg = zsocket_new((zctx_t *)ctx->arg, stype);
    zsocket_set_linger(s->arg, 0);
    zsocket_set_sndhwm(s->arg, 100000);
    zsocket_set_rcvhwm(s->arg, 100000);
    s->destroy = zero_native_destroy;
    s->bind = zero_native_bind;
    s->connect = zero_native_connect;
    s->disconnect = zero_native_disconnect;
    s->poll_handle = zero_native_poll_handle;
    s->monitor = zero_native_monitor;
    s->send = zero_native_send;
    s->recv = zero_native_recv;

    return(s);
}


//*************************************************************
//   TRACE_ROUTER routines
//      Send: Pop and route
//      Recv: Append sender
//*************************************************************

//*************************************************************

int zero_trace_router_recv(gop_mq_socket_t *socket, mq_msg_t *msg, int flags)
{
    gop_mq_frame_t *f;

    int n = zero_native_recv(socket, msg, flags);

    if (n != -1) {  //** Move the sender from the top to the bottom
        f = mq_msg_pop(msg);
        gop_mq_msg_append_frame(msg, f);
    }
    return(n);
}

//*************************************************************

gop_mq_socket_t *zero_create_trace_router_socket(gop_mq_socket_context_t *ctx)
{
    gop_mq_socket_t *s;

    tbx_type_malloc_clear(s, gop_mq_socket_t, 1);

    s->type = MQ_TRACE_ROUTER;
    s->arg = zsocket_new((zctx_t *)ctx->arg, ZMQ_ROUTER);
   FATAL_UNLESS(s->arg);
    zsocket_set_linger(s->arg, 0);
    zsocket_set_sndhwm(s->arg, 100000);
    zsocket_set_rcvhwm(s->arg, 100000);
    s->destroy = zero_native_destroy;
    s->bind = zero_native_bind;
    s->connect = zero_native_connect;
    s->disconnect = zero_native_disconnect;
    s->poll_handle = zero_native_poll_handle;
    s->monitor = zero_native_monitor;
    s->send = zero_native_send;
    s->recv = zero_trace_router_recv;

    return(s);
}

//*************************************************************
//   SIMPLE_ROUTER routines
//      Send: Pop and route
//      Recv: pass thru
//*************************************************************

//*************************************************************

int zero_simple_router_recv(gop_mq_socket_t *socket, mq_msg_t *msg, int flags)
{
    gop_mq_frame_t *f;

    int n = zero_native_recv(socket, msg, flags);

    if (n != -1) {  //** Remove sender which 0MQ router added
        f = mq_msg_pop(msg);
        gop_mq_frame_destroy(f);
    }
    return(n);
}

//*************************************************************

gop_mq_socket_t *zero_create_simple_router_socket(gop_mq_socket_context_t *ctx)
{
    gop_mq_socket_t *s;

    tbx_type_malloc_clear(s, gop_mq_socket_t, 1);

    s->type = MQ_SIMPLE_ROUTER;
    s->arg = zsocket_new((zctx_t *)ctx->arg, ZMQ_ROUTER);
    zsocket_set_linger(s->arg, 0);
    zsocket_set_sndhwm(s->arg, 100000);
    zsocket_set_rcvhwm(s->arg, 100000);
    s->destroy = zero_native_destroy;
    s->bind = zero_native_bind;
    s->connect = zero_native_connect;
    s->disconnect = zero_native_disconnect;
    s->poll_handle = zero_native_poll_handle;
    s->monitor = zero_native_monitor;
    s->send = zero_native_send;
    s->recv = zero_simple_router_recv;

    return(s);
}


//*************************************************************
// zero_create_socket  - Creates an MQ socket based o nthe given type
//*************************************************************

gop_mq_socket_t *zero_create_socket(gop_mq_socket_context_t *ctx, int stype)
{
    gop_mq_socket_t *s = NULL;
    log_printf(15, "\t\tstype=%d\n", stype);
    switch (stype) {
    case MQ_DEALER:
    case MQ_PAIR:
        s = zero_create_native_socket(ctx, stype);
        break;
    case MQ_TRACE_ROUTER:
        s = zero_create_trace_router_socket(ctx);
        break;
    case MQ_SIMPLE_ROUTER:
        s = zero_create_simple_router_socket(ctx);
        break;
    default:
        log_printf(0, "Unknown socket type: %d\n", stype);
        free(s);
        s = NULL;
    }

    return(s);
}

//*************************************************************
//  zero_socket_context_destroy - Destroys the 0MQ based context
//*************************************************************

void zero_socket_context_destroy(gop_mq_socket_context_t *ctx)
{
    //** Kludge to get around race issues in 0mq when closing sockets manually vs letting
    //** zctx_destroy() close them
    zctx_destroy((zctx_t **)&(ctx->arg));
    free(ctx);
}

//*************************************************************
//  zero_socket_context_new - Creates a new MQ context based on 0MQ
//*************************************************************

gop_mq_socket_context_t *zero_socket_context_new()
{
    gop_mq_socket_context_t *ctx;

    tbx_type_malloc_clear(ctx, gop_mq_socket_context_t, 1);

    ctx->arg = zctx_new();
   FATAL_UNLESS(ctx->arg != NULL);
    zctx_set_linger(ctx->arg, 0);
    ctx->create_socket = zero_create_socket;
    ctx->destroy = zero_socket_context_destroy;

    //** Disable the CZMQ SIGINT/SIGTERM signale handler
    apr_signal(SIGINT, NULL);
    apr_signal(SIGTERM, NULL);

    return(ctx);
}
