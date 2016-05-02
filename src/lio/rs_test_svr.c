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
#define _log_module_index 182

#include <assert.h>
#include "assert_result.h"
#include "exnode.h"
#include "log.h"
#include "iniparse.h"
#include "type_malloc.h"
#include "thread_pool.h"
#include "lio.h"
#include "rs_zmq_base.h"
#include "rs_zmq.h"

//*************************************************************************
//*************************************************************************

static int s_interrupted = 0;
static sigset_t signal_mask; //** signals to block
static void signal_handler(int signal_value)
{
    s_interrupted = 1;
}

static void catch_signals(void)
{
    struct sigaction action;
    action.sa_handler = signal_handler;
    action.sa_flags = 0;
    sigemptyset(&action.sa_mask);
    sigaction(SIGINT, &action, NULL);
    sigaction(SIGTERM, &action, NULL);
}

int main(int argc, char **argv)
{
    if (argc < 2) {
        printf("\n");
        printf("rs_test LIO_COMMON_OPTIONS\n");
        lio_print_options(stdout);
        return(1);
    }

    int thread_nbr;

    lio_init(&argc, &argv);

    thread_nbr = lio_parallel_task_count;

    //*** Parses the args
    char *svr_proto, *svr_addr, *svr_port, *zmq_svr;

    //** Retrieves remote zmq server name, transport protocol, and lisenting port
    svr_proto = inip_get_string(lio_gc->ifd, "zmq_server", "protocol", RS_ZMQ_DFT_PROTO);
    svr_addr = inip_get_string(lio_gc->ifd, "zmq_server", "server", NULL);
    svr_port = inip_get_string(lio_gc->ifd, "zmq_server", "port", RS_ZMQ_DFT_PORT);
    asprintf(&zmq_svr, "%s://%s:%s", string_trim(svr_proto), string_trim(svr_addr), string_trim(svr_port));


    //** Creates zmq context
    void *context = zmq_ctx_new();
    assert(context != NULL);

    //** Creates zmq router and binds it to tcp://*:5555
    //** It talks to rs client
    void *router = zmq_socket(context, ZMQ_ROUTER);
    assert(router != NULL);
    int rc = zmq_bind(router, zmq_svr);
    assert(rc != -1);
    printf("ZMQ router socket created.\n");

    // Creates and binds DEALER socket to inproc://worker
    // It talks to workers
    void *dealer = zmq_socket(context, ZMQ_DEALER);
    assert(dealer != NULL);
    rc = zmq_bind(dealer, "inproc://worker");
    assert(rc != -1);
    printf("ZMQ dealer socket created.\n");

    //** Blocks the SIGINT, SIGTERM signals
    sigemptyset(&signal_mask);
    sigaddset(&signal_mask, SIGINT);
    sigaddset(&signal_mask, SIGTERM);
    rc = pthread_sigmask(SIG_BLOCK, &signal_mask, NULL);
    assert(rc == 0);

    //** Launches thread pool
    printf("Launching threads...\n");
    int thread_count;
    rs_zmq_thread_arg_t **arg;
    type_malloc_clear(arg, rs_zmq_thread_arg_t *, thread_nbr);
    pthread_t *workers;
    type_malloc_clear(workers, pthread_t, thread_nbr);
    for (thread_count = 0; thread_count < thread_nbr; thread_count++) {
        type_malloc_clear(arg[thread_count], rs_zmq_thread_arg_t, 1);
        arg[thread_count]->zmq_context = context;
        arg[thread_count]->rs = lio_gc->rs;
        arg[thread_count]->da = ds_attr_create(lio_gc->ds);
        arg[thread_count]->ds = lio_gc->ds;
        arg[thread_count]->timeout = lio_gc->timeout;
        pthread_create(&workers[thread_count], NULL, rs_zmq_worker_routine, (void *)arg[thread_count]);
    }
    printf("Launched all %d threads.\n", thread_nbr);

    //** Unblocks the SIGINT, SIGTERM signals
    rc = pthread_sigmask(SIG_UNBLOCK, &signal_mask, NULL);
    assert(rc == 0);

    //** Catches the SIGNIT, SIGTERM signals
    catch_signals();

    //** Uses a QUEUE device to connect router and dealer
    zmq_device(ZMQ_QUEUE, router, dealer);

    if (s_interrupted == 1)
        printf("Interrupt received, killing server...\n");

    //** Shutdown zmq should go before cleaning thread resources
    zmq_close(router);
    zmq_close(dealer);
    printf("Destroied ZMQ router and dealer\n");
    zmq_ctx_destroy(context); //** This "trigers" the exit of all threads, because it makes all blocking operations on sockets return
    printf("Destroied ZMQ context\n");
    fflush(stdout);

    //** Waits for all threads to exit
    for (thread_count = 0; thread_count < thread_nbr; thread_count++) {
        pthread_join(workers[thread_count], NULL);
    }

    //** Destroys allocations for threads
    for (thread_count = 0; thread_count < thread_nbr; thread_count++) {
        ds_attr_destroy(lio_gc->ds, arg[thread_count]->da);
        free(arg[thread_count]);
    }
    free(arg);
    free(workers);

    free(svr_proto);
    free(svr_addr);
    free(svr_port);
    free(zmq_svr);

    lio_shutdown();

    return(0);
}

