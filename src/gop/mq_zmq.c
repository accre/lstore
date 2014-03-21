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

//*************************************************************
//  ZeroMQ implementation of the MQ subsystem.
//  This is mainly a wrapper around 0MQ calls with an
//  extension of the 0MQ socket types
//*************************************************************

#include "mq_portal.h"
#include "mq_roundrobin.h"
#include "type_malloc.h"
#include "log.h"
#include "apr_signal.h"
#include "random.h"
#include <stdlib.h>

//*************************************************************
//   Native routines
//*************************************************************

//*************************************************************

void zero_native_destroy(mq_socket_context_t *ctx, mq_socket_t *socket)
{
  zsocket_destroy((zctx_t *)ctx->arg, socket->arg);
  free(socket);
}

//*************************************************************

int zero_native_bind(mq_socket_t *socket, const char *format, ...)
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

int zero_native_connect(mq_socket_t *socket, const char *format, ...)
{
  va_list args;
  int err;
  char buf[255], id[255];

if (socket->type != MQ_PAIR) zsocket_set_router_mandatory(socket->arg, 1);

  va_start(args, format);
  //** Set the ID
  if (socket->type != MQ_PAIR) {
     snprintf(buf, 255, format, args);
     snprintf(id, 255, "%s:%ld", buf, random_int(1, 1000000));
     zsocket_set_identity(socket->arg, id);
  }

  err = zsocket_connect(socket->arg, format, args);
  va_end(args);

  return(err);
}

//*************************************************************

int zero_native_disconnect(mq_socket_t *socket, const char *format, ...)
{
  va_list args;
  int err;

  va_start(args, format);
  err = zsocket_disconnect(socket->arg, format, args);
  va_end(args);

  return(err);
}

//*************************************************************

void *zero_native_poll_handle(mq_socket_t *socket)
{
  return(socket->arg);
}

//*************************************************************

int zero_native_monitor(mq_socket_t *socket, char *address, int events)
{
  return(zmq_socket_monitor(socket->arg, address, events));
}

//*************************************************************

int zero_native_send(mq_socket_t *socket, mq_msg_t *msg, int flags)
{
  mq_frame_t *f, *fn;
  int n, loop, bytes;

int count = 0;

  n = 0;
  f = mq_msg_first(msg);
if (f->len > 1) {
   log_printf(5, "dest=!%.*s! nframes=%d\n", f->len, (char *)(f->data), stack_size(msg)); flush_log();
} else {
   log_printf(5, "dest=(single byte) nframes=%d\n", stack_size(msg)); flush_log();
}

  while ((fn = mq_msg_next(msg)) != NULL) {
    loop = 0;
    do {
       bytes = zmq_send(socket->arg, f->data, f->len, ZMQ_SNDMORE);
       if (bytes == -1) {
          if (errno == EHOSTUNREACH) usleep(100);
       }
       loop++;
log_printf(5, "sending frame=%d len=%d bytes=%d errno=%d loop=%d\n", count, f->len, bytes, errno, loop); flush_log();
if (f->len>0) { log_printf(5, "byte=%uc\n", (unsigned char)f->data[0]); flush_log(); }
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

int zero_native_recv(mq_socket_t *socket, mq_msg_t *msg, int flags)
{
  mq_frame_t *f;
  int n, nframes, rc;
  int64_t more = 0;
  size_t msize = sizeof(more);

  if ((flags & MQ_DONTWAIT) > 0) {
    more = 0;
    rc = zmq_getsockopt (socket->arg, ZMQ_EVENTS, &more, &msize);
log_printf(5, "more=" I64T "\n", more);
    assert (rc == 0);
    if ((more & ZMQ_POLLIN) == 0) return(-1);
  }

  n = 0;
  nframes = 0;
  do {
    type_malloc(f, mq_frame_t, 1);

    rc = zmq_msg_init(&(f->zmsg));
    assert (rc == 0);
    rc = zmq_msg_recv(&(f->zmsg), socket->arg, flags);
log_printf(15, "rc=%d errno=%d\n", rc, errno);
    assert (rc != -1);

    rc = zmq_getsockopt (socket->arg, ZMQ_RCVMORE, &more, &msize);
    assert (rc == 0);

    f->len = zmq_msg_size(&(f->zmsg));
    f->data = zmq_msg_data(&(f->zmsg));
    f->auto_free = MQF_MSG_INTERNAL_FREE;

    mq_msg_append_frame(msg, f);
    n += f->len;
    nframes++;
log_printf(5, "more=" I64T "\n", more);
  } while (more > 0);

log_printf(5, "total bytes=%d nframes=%d\n", n, nframes);

  return((n>0) ? 0 : -1);
}

//*************************************************************

mq_socket_t *zero_create_native_socket(mq_socket_context_t *ctx, int stype)
{
  mq_socket_t *s;

  type_malloc_clear(s, mq_socket_t, 1);

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

int zero_trace_router_recv(mq_socket_t *socket, mq_msg_t *msg, int flags)
{
  mq_frame_t *f;

  int n = zero_native_recv(socket, msg, flags);

  if (n != -1) {  //** Move the sender from the top to the bottom
     f = mq_msg_pop(msg);
     mq_msg_append_frame(msg, f);
  }
  return(n);
}

//*************************************************************

mq_socket_t *zero_create_trace_router_socket(mq_socket_context_t *ctx)
{
  mq_socket_t *s;

  type_malloc_clear(s, mq_socket_t, 1);

  s->type = MQ_TRACE_ROUTER;
  s->arg = zsocket_new((zctx_t *)ctx->arg, ZMQ_ROUTER);
  assert(s->arg);
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
// ROUND ROBIN socket
//*************************************************************
mq_socket_t *zero_create_round_robin_socket(mq_socket_context_t *ctx)
{
  mq_socket_t *s;

  type_malloc_clear(s, mq_socket_t, 1);

  s->type = MQ_ROUND_ROBIN;
  s->arg = zsocket_new((zctx_t *)ctx->arg, ZMQ_ROUTER);
  assert(s->arg);
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

int zero_simple_router_recv(mq_socket_t *socket, mq_msg_t *msg, int flags)
{
  mq_frame_t *f;

  int n = zero_native_recv(socket, msg, flags);

  if (n != -1) {  //** Remove sender which 0MQ router added
     f = mq_msg_pop(msg);
     mq_frame_destroy(f);
  }
  return(n);
}

//*************************************************************

mq_socket_t *zero_create_simple_router_socket(mq_socket_context_t *ctx)
{
  mq_socket_t *s;

  type_malloc_clear(s, mq_socket_t, 1);

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

mq_socket_t *zero_create_socket(mq_socket_context_t *ctx, int stype)
{
  mq_socket_t *s = NULL;
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
    case MQ_ROUND_ROBIN:
	     s = zero_create_round_robin_socket(ctx);
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

void zero_socket_context_destroy(mq_socket_context_t *ctx)
{
  //** Kludge to get around race issues in 0mq when closing sockets manually vs letting
  //** zctx_destroy() close them
//  sleep(1);
log_printf(5, "after sleep\n"); flush_log();
  zctx_destroy((zctx_t **)&(ctx->arg));
log_printf(5, "after zctx_destroy\n"); flush_log();
  free(ctx);
}

//*************************************************************
//  zero_socket_context_new - Creates a new MQ context based on 0MQ
//*************************************************************

mq_socket_context_t *zero_socket_context_new()
{
  mq_socket_context_t *ctx;

  type_malloc_clear(ctx, mq_socket_context_t, 1);

  ctx->arg = zctx_new();
  assert(ctx->arg != NULL);
  zctx_set_linger(ctx->arg, 0);
  ctx->create_socket = zero_create_socket;
  ctx->destroy = zero_socket_context_destroy;

  //** Disable the CZMQ SIGINT/SIGTERM signale handler
  apr_signal(SIGINT, NULL);
  apr_signal(SIGTERM, NULL);

  return(ctx);
}
