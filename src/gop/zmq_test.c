#include "type_malloc.h"
#include "log.h"
#include "apr_signal.h"
#include "apr_wrapper.h"
#include "opque.h"
#include "random.h"
#include <stdlib.h>
#include <zmq.h>
#include <czmq.h>

char *host = "tcp://127.0.0.1:6713";

#define MODE_CLIENT 1
#define MODE_SERVER 2

//***************************************************************************

void destroy_context(zctx_t *ctx) {
    zctx_destroy(&ctx);
}

//***************************************************************************

zctx_t *new_context() {
    zctx_t *ctx;

    ctx = zctx_new();
    assert(ctx != NULL);
    zctx_set_linger(ctx, 0);

    //** Disable the CZMQ SIGINT/SIGTERM signale handler
    apr_signal(SIGINT, NULL);
    apr_signal(SIGTERM, NULL);

    return(ctx);
}

//*************************************************************

void *new_socket(zctx_t *ctx, int stype) {
    void *s;

    s = zsocket_new(ctx, stype);
    assert(s);
    zsocket_set_linger(s, 0);
    zsocket_set_sndhwm(s, 100000);
    zsocket_set_rcvhwm(s, 100000);

    return(s);
}

//*************************************************************

void destroy_socket(zctx_t *ctx, void *socket) {
    zsocket_destroy(ctx, socket);
}

//*************************************************************

int socket_connect(void *socket, const char *format, ...) {
    va_list args;
    int err, n;
    char buf[255], id[255];

    //zsocket_set_router_mandatory(socket, 1);

    va_start(args, format);

    //** Set the ID
    snprintf(buf, 255, format, args);
//  snprintf(id, 255, "%s:%ld", buf, random());

    snprintf(id, 255, "%ld", random_int(1, 1000));
    zsocket_set_identity(socket, strdup(id));

    err = zsocket_connect(socket, format, args);
    n = errno;
    va_end(args);

    log_printf(0, "id=!%s! err=%d errno=%d\n", id, err, n);

    assert(err == 0);
    return(err);
}

//*************************************************************

int socket_bind(void *socket, const char *format, ...) {
    va_list args;
    int err, n;
    char id[255];

    va_start(args, format);
    snprintf(id, 255, format, args);
    zsocket_set_identity(socket, strdup(id));
    err = zsocket_bind(socket, format, args);
    n = errno;
    va_end(args);

    log_printf(0, "id=!%s! err=%d errno=%d\n", id, err, n);

    return((err == -1) ? -1 : 0);
}

//***************************************************************************
// run_client
//***************************************************************************

void run_client() {
    zctx_t *ctx;
    void *socket;
    int i, n;
    char buf[256];
    char *me;

    ctx = new_context();
    socket = new_socket(ctx, ZMQ_REQ);
    socket_connect(socket, host);
//  zmq_connect(socket, host);

    me = zsocket_identity(socket);
    log_printf(0, "my identity=%s\n", me);
    i = 0;
    for (;;) {
        log_printf(0, "Sending %d\n", i);
//     zmq_send(socket, host, strlen(host), ZMQ_SNDMORE);
//     zmq_send(socket, me, strlen(me), ZMQ_SNDMORE);

        snprintf(buf, sizeof(buf), "Hello %d", i);
        zmq_send(socket, buf, strlen(buf)+1, 0);

//     zmq_recv(socket, buf, sizeof(buf), 0);
//     log_printf(0, "From %s\n", buf);
        n = zmq_recv(socket, buf, sizeof(buf), 0);
        buf[n] = 0;
        log_printf(0, "Got %s\n", buf);

        sleep(1);
        i++;
    }

    destroy_context(ctx);
}

//***************************************************************************
// run_server
//***************************************************************************

void run_server() {
    zctx_t *ctx;
    void *socket;
    int i, n, nclient;
    char buf[256];
    char data[256];
    char client[256];

    ctx = new_context();
    socket = new_socket(ctx, ZMQ_ROUTER);
//  socket = new_socket(ctx, ZMQ_REP);
    assert(socket_bind(socket, host) == 0);
//  assert(zmq_bind(socket, host) == 0);

    log_printf(0, "my identity=%s\n", zsocket_identity(socket));

    i = 0;
    for (;;) {
        log_printf(0, "Waiting %d\n", i);
        nclient = zmq_recv(socket, client, sizeof(client), 0);
        client[nclient] = 0;
        log_printf(0, "From %s [%d]\n", client, nclient);
        n = zmq_recv(socket, buf, sizeof(buf), 0);
        buf[n] = 0;
        if (n != 0) log_printf(0, "Missing EMPTY frame! buf=%s\n", buf);
        n = zmq_recv(socket, buf, sizeof(buf), 0);
        buf[n] = 0;
        log_printf(0, "Got %s\n", buf);

        zmq_send(socket, client, nclient, ZMQ_SNDMORE);
        zmq_send(socket, NULL, 0, ZMQ_SNDMORE);
        snprintf(data, sizeof(buf), "(%s) World %d", buf, i);
        zmq_send(socket, data, strlen(data)+1, 0);
        i++;
    }

    destroy_context(ctx);
}

//***************************************************************************
//***************************************************************************
//***************************************************************************

int main(int argc, char **argv) {
    int i, start_option, test_mode;

    if (argc < 2) {
        printf("mq_test [-d log_level] [-c|-s] [-q]\n");
        printf("-c      Client mode\n");
        printf("-s      Server mode\n");
        printf("-h      Host\n");
        printf("**NOTE:  Defaults to launching both client and server for internal testing\n");
        return(0);
    }

    i = 1;

    do {
        start_option = i;

        if (strcmp(argv[i], "-d") == 0) { //** Enable debugging
            i++;
            set_log_level(atol(argv[i]));
            i++;
        } else if (strcmp(argv[i], "-c") == 0) { //** Client mode
            i++;
            test_mode = MODE_CLIENT;
        } else if (strcmp(argv[i], "-s") == 0) { //** Server mode
            i++;
            test_mode = MODE_SERVER;
        } else if (strcmp(argv[i], "-s") == 0) { //** Server mode
            i++;
            host = argv[i];
            i++;
        }
    } while ((start_option < i) && (i<argc));


    apr_wrapper_start();
    init_opque_system();
    init_random();

    if (test_mode == MODE_CLIENT) {
        run_client();
    } else {
        run_server();
    }

    return(0);
}
