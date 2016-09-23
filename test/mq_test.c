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

//#include "mq_portal.h"
#include <unistd.h>
#include <apr_thread_cond.h>
#include <gop/gop.h>
#include <gop/mq.h>
#include <gop/opque.h>
#include <tbx/assert_result.h>
#include <tbx/apr_wrapper.h>
#include <tbx/fmttypes.h>
#include <tbx/iniparse.h>
#include <tbx/log.h>
#include <tbx/stack.h>
#include <tbx/type_malloc.h>
#include <tbx/atomic_counter.h>

#define CMD_PING 1
#define CMD_PONG 2

typedef struct {
    int command;
    uint64_t id;
    int delay;
    int address_reply;
    int ping_count;
    int success_value;
    int dt;
} test_data_t;

typedef struct {
    mq_msg_t *msg;
    test_data_t *td;
    apr_time_t expire;
} defer_t;

apr_thread_mutex_t *lock = NULL;
apr_thread_cond_t  *cond = NULL;
char *handle = NULL;
tbx_stack_t *deferred_ready = NULL;
tbx_stack_t *deferred_pending = NULL;
gop_mq_command_stats_t server_stats;
gop_mq_portal_t *server_portal = NULL;

char *host = "tcp://127.0.0.1:6714";
mq_pipe_t control_efd[2];
mq_pipe_t server_efd[2];
int shutdown_everything = 0;
tbx_atomic_unit32_t ping_count = 0;

//***************************************************************************
// pack_msg - Packs a message for sending
//***************************************************************************

mq_msg_t *pack_msg(int dotrack, test_data_t *td)
{
    mq_msg_t *msg;

    msg = gop_mq_msg_new();
    gop_mq_msg_append_mem(msg, host, strlen(host), MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
    if (dotrack == 1) {
        gop_mq_msg_append_mem(msg, MQF_TRACKEXEC_KEY, MQF_TRACKEXEC_SIZE, MQF_MSG_KEEP_DATA);
    } else {
        gop_mq_msg_append_mem(msg, MQF_EXEC_KEY, MQF_EXEC_SIZE, MQF_MSG_KEEP_DATA);
    }
    gop_mq_msg_append_mem(msg, &(td->id), sizeof(uint64_t), MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, MQF_PING_KEY, MQF_PING_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, td, sizeof(test_data_t), MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    return(msg);
}


//***************************************************************************
// unpack_msg - unPacks a message received
//***************************************************************************

int unpack_msg(mq_msg_t *msg, test_data_t *td)
{
    gop_mq_frame_t *f;
    char *data;
    uint64_t *id;
    int n, status;

    status = 0;

    //** Parse the response
    f = gop_mq_msg_first(msg);
    gop_mq_get_frame(f, (void **)&data, &n);
    if (n != 0) {  //** SHould be an empty frame
        log_printf(0, " ERROR:  Missing initial empty frame!\n");
        status = 1;
        goto fail;
    }
    f = gop_mq_msg_next(msg);
    gop_mq_get_frame(f, (void **)&data, &n);
    if (mq_data_compare(data, n, MQF_VERSION_KEY, MQF_VERSION_SIZE) != 0) {
        log_printf(0, "ERROR:  Missing version frame!\n");
        status = 1;
        goto fail;
    }
    f = gop_mq_msg_next(msg);
    gop_mq_get_frame(f, (void **)&data, &n);
    if (mq_data_compare(data, n, MQF_RESPONSE_KEY, MQF_RESPONSE_SIZE) != 0) {
        log_printf(0, " ERROR: Bad RESPONSE command frame\n");
        status = 1;
        goto fail;
    }
    f = gop_mq_msg_next(msg);
    gop_mq_get_frame(f, (void **)&id, &n);
    if (n != sizeof(uint64_t)) {
        log_printf(0, " ERROR: Bad ID size!  Got %d should be sizeof(uint64_t)=" ST "\n", n, sizeof(uint64_t));
        status = 1;
        goto fail;
    }

    f = gop_mq_msg_next(msg);
    gop_mq_get_frame(f, (void **)&data, &n);
    if (n != sizeof(test_data_t)) {
        log_printf(0, " ERROR: Bad test data size!  Got %d should be sizeof(test_data_t)=" ST "\n", n, sizeof(test_data_t));
        status = 1;
        goto fail;
    }
    *td = *(test_data_t *)data;

    if (td->id != *id) {
        log_printf(0, " ERROR: ID mismatch! id=" LU " td->id=" LU "\n", *id, td->id);
        status = 1;
        goto fail;
    }

    f = gop_mq_msg_next(msg);
    gop_mq_get_frame(f, (void **)&data, &n);
    if (n != 0) {  //** SHould be an empty frame
        log_printf(0, " ERROR:  Final initial empty frame!\n");
        status = 1;
        goto fail;
    }

fail:
    return(status);
}

//***************************************************************************
//  client_direct - Direct send/recv bypasses mq_portal fn
//***************************************************************************

int client_direct()
{
    gop_mq_socket_context_t *ctx;
    gop_mq_socket_t *sock;
    mq_msg_t *msg = NULL;
    gop_mq_frame_t *f;
    gop_mq_pollitem_t pfd;
    char *data;
    int err, status, n;

    log_printf(0, "TEST: (START) client_direct()\n");

    status = 0;

    sleep(1);  //** Wait for the server to start up and bind to the port
    ctx = gop_mq_socket_context_new();
    sock = gop_mq_socket_new(ctx, MQ_TRACE_ROUTER);
    err = gop_mq_connect(sock, host);
    if (err != 0) {
        log_printf(0, "ERROR:  Failed connecting to host=%s error=%d\n", host, err);
        status = 1;
        goto fail;
    }

    sleep(1);

    //** Compose the PING message
    msg = gop_mq_msg_new();
    gop_mq_msg_append_mem(msg, host, strlen(host), MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, MQF_PING_KEY, MQF_PING_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, "HANDLE", 6, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    //** Send the message
    err = gop_mq_send(sock, msg, 0);
    if (err != 0) {
        log_printf(0, "ERROR:  Failed sending PING message to host=%s error=%d\n", host, err);
        status = 1;
        goto fail;
    }

    gop_mq_msg_destroy(msg);

    //** Wait for the response
    msg = gop_mq_msg_new();

    pfd.socket = gop_mq_poll_handle(sock);
    pfd.events = MQ_POLLIN;
    err = gop_mq_poll(&pfd, 1, 5000);  //** Wait for 5 secs
    if (err != 1) {
        log_printf(0, "ERROR:  Failed polling for PING message response to host=%s\n", host);
        status = 1;
        goto fail;
    }
    err = gop_mq_recv(sock, msg, MQ_DONTWAIT);
//  err = gop_mq_recv(sock, msg, 0);
    if (err != 0) {
        log_printf(0, "ERROR:  Failed recving PONG response message from host=%s error=%d\n", host, err);
        status = 1;
        goto fail;
    }

    //** Parse the response
    f = gop_mq_msg_first(msg);
    gop_mq_get_frame(f, (void **)&data, &n);
    if (n != 0) {  //** SHould be an empty frame
        log_printf(0, " ERROR:  Missing initial PONG empty frame!\n");
        status = 1;
        goto fail;
    }
    f = gop_mq_msg_next(msg);
    gop_mq_get_frame(f, (void **)&data, &n);
    if (mq_data_compare(data, n, MQF_VERSION_KEY, MQF_VERSION_SIZE) != 0) {
        log_printf(0, "ERROR:  Missing version frame!\n");
        status = 1;
        goto fail;
    }
    f = gop_mq_msg_next(msg);
    gop_mq_get_frame(f, (void **)&data, &n);
    if (mq_data_compare(data, n, MQF_PONG_KEY, MQF_PONG_SIZE) != 0) {
        log_printf(0, " ERROR: Bad PONG command frame\n");
        status = 1;
        goto fail;
    }
    f = gop_mq_msg_next(msg);
    gop_mq_get_frame(f, (void **)&data, &n);
    if (mq_data_compare("HANDLE", 6, data, n) != 0) {
        log_printf(0, " ERROR: Bad PONG ID should be HANDLE got=%s\n", data);
        status = 1;
        goto fail;
    }

fail:
    if (msg) gop_mq_msg_destroy(msg);
    gop_mq_socket_destroy(ctx, sock);
    gop_mq_socket_context_destroy(ctx);

    if (status == 0) {
        log_printf(0, "TEST: (END) client_direct() = SUCCESS\n");
    } else {
        log_printf(0, "TEST: (END) client_direct() = FAIL\n");
    }
    return(status);
}

//***************************************************************************
//***************************************************************************

int client_exec_ping_test(gop_mq_context_t *mqc)
{
    mq_msg_t *msg;
    gop_op_generic_t *gop;
    int err, status;
    log_printf(0, "TEST: (START) client_exec_ping_test(mqc)\n");

    status = 0;

    //** Compose the PING message
    msg = gop_mq_msg_new();
    gop_mq_msg_append_mem(msg, host, strlen(host), MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, MQF_EXEC_KEY, MQF_EXEC_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, MQF_PING_KEY, MQF_PING_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, "HANDLE", 6, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    gop = gop_mq_op_new(mqc, msg, NULL, NULL, NULL, 5);

    //** Send the message
    handle = NULL;  //** This is where I get my response
    err = gop_waitall(gop);
    gop_free(gop, OP_DESTROY);
    if (err != OP_STATE_SUCCESS) {
        log_printf(0, "ERROR:  Failed sending PING message to host=%s error=%d\n", host, err);
        status = 1;
        goto fail;
    }

    //** Wait for it to be processed by the server
    apr_thread_mutex_lock(lock);
    log_printf(5, "handle=%p\n", handle);
    while (handle == NULL) {
        apr_thread_cond_wait(cond, lock);
        log_printf(5, "--handle=%p\n", handle);
    }
    apr_thread_mutex_unlock(lock);

    //** Check that the handle made it through
    if (mq_data_compare(handle, 6, "HANDLE", 6) != 0) {
        log_printf(0, "ERROR: Bad handle\n");
        status = 1;
        goto fail;
    }

    free(handle);

fail:
    if (status == 0) {
        log_printf(0, "TEST: (END) client_exec_ping_test(mqc) = SUCCESS\n");
    } else {
        log_printf(0, "TEST: (END) client_exec_ping_test(mqc) = FAIL\n");
    }
    return(status);
}

//***************************************************************************
//  client_response_pong - Handles the client pong response
//***************************************************************************

gop_op_status_t client_response_pong(void *arg, int id)
{
    gop_mq_task_t *task = (gop_mq_task_t *)arg;
    test_data_t td;
    char b64[1024];
    int err;

    log_printf(1, "Processing response gid=%d\n", gop_id(task->gop));

    err = unpack_msg(task->response, &td);
    if (err != 0) return(gop_failure_status);

    log_printf(1, "delay=%d ping_count=%d address_reply=%d sid=%s\n", td.delay, td.ping_count, td.address_reply, gop_mq_id2str((char *)&(td.id), sizeof(td.id), b64, sizeof(b64)));
    if ((td.delay > 0) && (td.address_reply ==1)) {
        if (td.ping_count == 0) {
            if (server_portal == NULL) log_printf(0, "ERROR:  No trackexec heartbeat detected!  delay=%d ping_count=%d\n", td.delay, td.ping_count);
//        return(gop_failure_status);
        }
    }

    return(gop_success_status);
}

//***************************************************************************
//  client_trackexec_ping_test - Simple ping with response
//***************************************************************************

int client_trackexec_ping_test(gop_mq_context_t *mqc, int delay, int address_reply, int dt)
{
    mq_msg_t *msg;
    gop_op_generic_t *gop;
    int err, status, success_value;
    test_data_t td;

    log_printf(0, "TEST: (START) client_trackexec_ping_test(mqc, delay=%d, reply=%d, dt=%d)\n", delay, address_reply, dt);

    status = 0;

    //** Figure out the edge case being checked.
    success_value = OP_STATE_SUCCESS;
    if ((delay < 0) || (delay > dt)) success_value = OP_STATE_FAILURE;

    //** Compose the PING message
    memset(&td, 0, sizeof(td));
    td.id = tbx_atomic_global_counter();
    td.command = CMD_PING;
    td.delay = delay;
    td.address_reply = address_reply;
    msg = pack_msg(1, &td);

    gop = gop_mq_op_new(mqc, msg, client_response_pong, NULL, NULL, dt);
    gop_set_private(gop, &td);

    //** Send the message and wait for the response
    err = gop_waitall(gop);
    gop_free(gop, OP_DESTROY);
    if (err != success_value) {
        log_printf(0, "ERROR: Recving PONG message to host=%s error=%d\n", host, err);
        status = 1;
        goto fail;
    }

fail:

    if (status == 0) {
        log_printf(0, "TEST: (END) client_trackexec_ping_test(mqc, delay=%d, reply=%d, dt=%d) = SUCCESS (g=%d, s=%d)\n", delay, address_reply, dt, err, success_value);
    } else {
        log_printf(0, "TEST: (END) client_trackexec_ping_test(mqc, delay=%d, reply=%d, dt=%d) = FAIL (g=%d, s=%d)\n", delay, address_reply, dt, err, success_value);
    }
    tbx_log_flush();
    return(status);
}

//***************************************************************************
//  test_data_set - Fills a test data structure
//***************************************************************************

void test_data_set(test_data_t *td, int delay, int address_reply, int dt)
{
    //** Figure out the edge case being checked.

    //** Compose the PING message
    memset(td, 0, sizeof(test_data_t));
    td->id = tbx_atomic_global_counter();
    td->command = CMD_PING;
    td->delay = delay;
    td->address_reply = address_reply;
    td->dt = dt;

    td->success_value = OP_STATE_SUCCESS;
    if ((delay < 0) || (delay > dt)) td->success_value = OP_STATE_FAILURE;

}

//***************************************************************************
//  generate_tasks - Generates the individual ops for the bulk task
//***************************************************************************

void generate_tasks(gop_mq_context_t *mqc, gop_opque_t *q, int count, test_data_t *td_base)
{
    int i;
    gop_op_generic_t *gop;
    test_data_t *td;
    mq_msg_t *msg;

    for (i=0; i< count; i++) {
        tbx_type_malloc(td, test_data_t, 1);
        *td = *td_base;

        msg = pack_msg(1, td);
        gop = gop_mq_op_new(mqc, msg, client_response_pong, td, free, td->dt);
        td->id = gop_id(gop);
        gop_set_private(gop, td);
        gop_opque_add(q, gop);
    }
}

//***************************************************************************
//  bulk_test - Does a bulk ping/pong test
//***************************************************************************

int bulk_test(gop_mq_context_t *mqc)
{
    int err, n, expire, quick;
    gop_opque_t *q = gop_opque_new();
    gop_op_generic_t *gop;
    test_data_t tdc, *td;
    gop_op_status_t status;
    apr_time_t dt, start_time;
    char b64[1024];
    double ttime;

    quick = 0;
    //**delay, address_reply, expire
    test_data_set(&tdc, 5, 1, 3);
    if (quick == 0) {
        generate_tasks(mqc, q, 100, &tdc);
    } else {
        generate_tasks(mqc, q, 10, &tdc);
    }
    expire = 30;
    test_data_set(&tdc, 2, 1, expire);
    if (quick == 0) {
        generate_tasks(mqc, q, 1000, &tdc);
    } else {
        generate_tasks(mqc, q, 10, &tdc);
    }
    test_data_set(&tdc, 0, 0, expire);
    if (quick == 0) {
        generate_tasks(mqc, q, 10000, &tdc);
    } else {
        generate_tasks(mqc, q, 10, &tdc);
    }

    log_printf(0, "TEST: (START) %d tasks in bulk test\n", gop_opque_task_count(q));
    err = 0;
    n = 0;
    start_time = apr_time_now();
    while ((gop = opque_waitany(q)) != NULL) {
        n++;
        td = (test_data_t *)gop_get_private(gop);
        status = gop_get_status(gop);
        if (td->success_value != (int)status.op_status) {
            err++;
            dt = apr_time_now() - start_time;
            ttime = (1.0*dt) / APR_USEC_PER_SEC;
            log_printf(0, "TEST: (ERROR) gid=%d bulk_ping_test(delay=%d, reply=%d, dt=%d) = FAIL (g=%d, s=%d) dt=%lf sid=%s\n", gop_id(gop), td->delay, td->address_reply, td->dt, status.op_status, td->success_value, ttime, gop_mq_id2str((char *)&(td->id), sizeof(td->id), b64, sizeof(b64)));
        }

        dt = apr_time_now() - start_time;
        ttime = (1.0*dt) / APR_USEC_PER_SEC;
        log_printf(0, "BULK n=%d err=%d gid=%d dt=%lf sid=%s (delay=%d reply=%d dt=%d) got=%d shouldbe=%d\n", n, err, gop_id(gop), ttime, gop_mq_id2str((char *)&(td->id), sizeof(td->id), b64, sizeof(b64)), td->delay, td->address_reply, td->dt, status.op_status, td->success_value);
        tbx_log_flush();
        dt = apr_time_now();
        gop_free(gop, OP_DESTROY);
        dt = apr_time_now() - dt;
        ttime = (1.0*dt) / APR_USEC_PER_SEC;
        log_printf(0, "BULK gop_free dt=%lf\n", ttime);
        tbx_log_flush();
    }

    dt = apr_time_now() - start_time;
    ttime = (1.0*dt) / APR_USEC_PER_SEC;
    log_printf(0, "TEST: (END) Completed %d tasks in bulk test failed=%d dt=%lf\n", gop_opque_task_count(q), err, ttime);
    tbx_log_flush();
    if (ttime > expire) {
        log_printf(0, "TEST: (END) !!WARNING!! Execution time %lf > %d sec!!!! The dt=%d was used for the commands so I would expect failures!\n", ttime, expire, expire);
        log_printf(0, "TEST: (END) !!WARNING!! This probably means you are running under valgrind and is to be expected.\n");
    }

    log_printf(0, "Sleeping to let other threads close\n");
    sleep(5);
    log_printf(0, "Waking up\n");

    gop_opque_free(q, OP_DESTROY);
    return(err);
}


//***************************************************************************
// client_edge_tests -  Client edges tests
//***************************************************************************

int client_edge_tests(gop_mq_context_t *mqc)
{
    int nfail;

    log_printf(0, "START\n");

    nfail = 0;

    nfail += client_trackexec_ping_test(mqc, 0, 0, 5);  //** Ping test without tracking and no delay
    nfail += client_trackexec_ping_test(mqc, 2, 1, 5);  //** Ping test with tracking and delay
    nfail += client_trackexec_ping_test(mqc, -1, 0, 2);  //** Ping test no tracking and drop the message
    nfail += client_trackexec_ping_test(mqc, -1, 1, 2);  //** Ping with tracking and drop the message
    nfail += client_trackexec_ping_test(mqc, 5, 1, 2);  //** Ping with tracking but defer until after timeout
    log_printf(0, "Waiting for expired ping to hit\n");
    sleep(5);
    nfail += client_trackexec_ping_test(mqc, 5, 0, 2);  //** Ping without tracking but defer until after timeout
    log_printf(0, "Waiting for expired ping to hit\n");
    sleep(5);

    log_printf(0, "END nfail=%d\n", nfail);

    return(nfail);
}

//***************************************************************************
// client_make_context - Makes the MQ portal context
//***************************************************************************

gop_mq_context_t *client_make_context()
{
    char *text_params = "[mq_context]\n"
                        "  min_conn=1\n"
                        "  max_conn=4\n"
                        "  min_threads=2\n"
                        "  max_threads=10\n"
                        "  backlog_trigger=1000\n"
                        "  heartbeat_dt=1\n"
                        "  heartbeat_failure=10\n"
                        "  min_ops_per_sec=100\n";
    tbx_inip_file_t *ifd;
    gop_mq_context_t *mqc;

    ifd = tbx_inip_string_read(text_params);
    mqc = gop_mq_create_context(ifd, "mq_context");
    assert(mqc != NULL);
    tbx_inip_destroy(ifd);

    return(mqc);
}


//***************************************************************************
// client_test_thread -  Client test thread
//***************************************************************************

void *client_test_thread(apr_thread_t *th, void *arg)
{
    char v;
    int nfail, nfail_total, i, min;
    gop_mq_context_t *mqc;

    log_printf(0, "START\n");

    nfail_total = 0;

    //** perform ths simple base direct client test
    nfail_total += client_direct();
    tbx_log_flush();

    //** The rest of the tests all go through the mq_portal so we need to configure that now
    //** Make the portal
    mqc = client_make_context();

    nfail_total += client_exec_ping_test(mqc);  //** Simple exec ping test.  No tracking
    tbx_log_flush();

    min = 0;
    if (min == 1) {
        v = 1;
        log_printf(0, "Skipping raw tests.\n");
        WARN_UNLESS(1 == gop_mq_pipe_write(control_efd[1], &v));
        sleep(1);
        log_printf(0, "Continuing....\n");
    }

    for (i=min; i<2; i++) {
        nfail = 0;

        //** Check edge cases
        log_printf(0, "Checking edge cases (ROUND=%d)\n", i);
        tbx_log_flush();
//     nfail += client_edge_tests(mqc);

        log_printf(0, "Switching to bulk tests (ROUND=%d)\n", i);
        tbx_log_flush();
        nfail += bulk_test(mqc);

        log_printf(0, "Completed round %d of tests. failed:%d\n", i, nfail);
        tbx_log_flush();

        nfail_total += nfail;

        if (i==0) { //** Switch the server to using the MQ loop
            v = 1;
            log_printf(0, "Telling server to switch and use the MQ event loop\n");
            WARN_UNLESS( 1 == gop_mq_pipe_write(control_efd[1], &v));
            sleep(10);
            log_printf(0, "Continuing....\n");
        }
    }


    //** Destroy the portal
    gop_mq_destroy_context(mqc);

    log_printf(0, "END\n");

    printf("Total Tasks failed: %d\n", nfail);

    return(NULL);
}

//***************************************************************************
// proc_ping - Processes a ping request
//***************************************************************************

int proc_ping(gop_mq_socket_t *sock, mq_msg_t *msg)
{
    mq_msg_t *pong;
    gop_mq_frame_t *f, *pid;
    int err;

//void *data;
//for (f = gop_mq_msg_first(msg), i=0; f != NULL; f = gop_mq_msg_next(msg), i++) {
//  gop_mq_get_frame(f, &data, &err);
//  log_printf(5, "fsize[%d]=%d\n", i, err);
//}

    apr_thread_mutex_lock(lock);
    server_stats.incoming[MQS_PING_INDEX]++;
    apr_thread_mutex_unlock(lock);

    //** Peel off the top frames and just leave the return address
    f = gop_mq_msg_first(msg);
    gop_mq_frame_destroy(gop_mq_msg_pluck(msg, 0));  //blank
    gop_mq_frame_destroy(gop_mq_msg_pluck(msg, 0));  //version
    gop_mq_frame_destroy(gop_mq_msg_pluck(msg,0));  //command

    pid = gop_mq_msg_pluck(msg, 0);  //Ping ID

    tbx_atomic_inc(ping_count);

    pong = gop_mq_msg_new();

    //** Push the address in reverse order (including the empty frame)
    while ((f = mq_msg_pop(msg)) != NULL) {
//i=gop_mq_get_frame(f, &data, &err);
//log_printf(5, "data=%s len=%d i=%d\n", (char *)data, err, i); tbx_log_flush();
//log_printf(5, "add=%d\n", err);
        gop_mq_msg_frame_push(pong, f);
    }

    gop_mq_msg_destroy(msg);

    //** Now add the command
    gop_mq_msg_append_mem(pong, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(pong, MQF_PONG_KEY, MQF_PONG_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_frame(pong, pid);
    gop_mq_msg_append_mem(pong, NULL, 0, MQF_MSG_KEEP_DATA);

//f = gop_mq_msg_first(pong);
//gop_mq_get_frame(f, &data, &err);
//log_printf(5, "1.data=%s len=%d\n", (char *)data, err);

    apr_thread_mutex_lock(lock);
    server_stats.outgoing[MQS_PONG_INDEX]++;
    apr_thread_mutex_unlock(lock);

    err = gop_mq_send(sock, pong, MQ_DONTWAIT);

    gop_mq_msg_destroy(pong);

    return(err);
}

//***************************************************************************
// proc_app_ping - Processes a ping request from the application
//***************************************************************************

int proc_exec_ping(gop_mq_socket_t *sock, mq_msg_t *msg)
{
    gop_mq_frame_t *f, *pid;
    char *data;
    int err;

    int i;
    for (f = gop_mq_msg_first(msg), i=0; f != NULL; f = gop_mq_msg_next(msg), i++) {
        gop_mq_get_frame(f, (void **)&data, &err);
        log_printf(5, "fsize[%d]=%d\n", i, err);
    }

    apr_thread_mutex_lock(lock);
    server_stats.incoming[MQS_PING_INDEX]++;
    apr_thread_mutex_unlock(lock);

    //** Peel off the top frames and just leave the return address
    f = gop_mq_msg_first(msg);
    gop_mq_frame_destroy(gop_mq_msg_pluck(msg, 0));  //blank
    gop_mq_frame_destroy(gop_mq_msg_pluck(msg, 0));  //version
    gop_mq_frame_destroy(gop_mq_msg_pluck(msg,0));  //command(exec)
    gop_mq_frame_destroy(gop_mq_msg_pluck(msg,0));  //app command(ping)

    pid = gop_mq_msg_current(msg);  //Ping ID

    //** Notify the sender;
    gop_mq_get_frame(pid, (void **)&data, &err);
    apr_thread_mutex_lock(lock);
    tbx_type_malloc_clear(handle, char, err+1);
    memcpy(handle, data, err);
    log_printf(5, "setting handle=%s nbytes=%d\n", handle, err);
    apr_thread_cond_broadcast(cond);
    apr_thread_mutex_unlock(lock);

    gop_mq_msg_destroy(msg);

    return(0);
}

//***************************************************************************
// sever_handle_deferred - Handles sending deferred responses
//***************************************************************************

int server_handle_deferred(gop_mq_socket_t *sock)
{
    defer_t *defer;
    int err;
    char v;

    log_printf(5, "deferred responses to handle %d (server_portal=%p)\n", tbx_stack_count(deferred_ready), server_portal);

    err = 0;
    while ((defer = tbx_stack_pop(deferred_ready)) != NULL) {
        if (server_portal == NULL) WARN_UNLESS(1 == gop_mq_pipe_read(server_efd[0], &v));
        log_printf(5, "Processing deferred response\n");

        defer->td->ping_count = tbx_atomic_get(ping_count) - defer->td->ping_count;  //** Send back the ping count since it was sent

        //** Send the response
        if (server_portal == NULL) apr_thread_mutex_lock(lock);
        server_stats.outgoing[MQS_RESPONSE_INDEX]++;
        if (server_portal == NULL) apr_thread_mutex_unlock(lock);

        if (server_portal == NULL) {
            err += gop_mq_send(sock, defer->msg, 0);
            gop_mq_msg_destroy(defer->msg);
        } else {
            err = gop_mq_submit(server_portal, gop_mq_task_new(gop_mq_portal_mq_context(server_portal), defer->msg, NULL, NULL, 5));
        }

        if (err != 0) {
            log_printf(0, "ERROR:  Failed sending deferred message to host=%s error=%d\n", host, err);
        }

        free(defer);
    }

    return(err);
}


//***************************************************************************
// proc_trackexec_ping - Processes a ping request from the application and returns a response
//***************************************************************************

int proc_trackexec_ping(gop_mq_portal_t *p, gop_mq_socket_t *sock, mq_msg_t *msg)
{
    gop_mq_frame_t *pid, *tdf;
    mq_msg_t *response, *track_response;
    defer_t *defer;
    char *data, b64[1024];
    int err, size;
    test_data_t *td;

    log_printf(5, "TEXEC START\n");

    apr_thread_mutex_lock(lock);
    server_stats.incoming[MQS_TRACKEXEC_INDEX]++;
    apr_thread_mutex_unlock(lock);

    //** Peel off the top frames and just leave the return address
    gop_mq_msg_first(msg);
    gop_mq_frame_destroy(gop_mq_msg_pluck(msg, 0));  //blank
    gop_mq_frame_destroy(gop_mq_msg_pluck(msg, 0));  //version
    gop_mq_frame_destroy(gop_mq_msg_pluck(msg,0));  //command(trackexec)

    pid = gop_mq_msg_pluck(msg, 0);  //Ping ID
    gop_mq_get_frame(pid, (void **)&data, &size);
    log_printf(1, "TEXEC sid=%s\n", gop_mq_id2str(data, size, b64, sizeof(b64)));
    gop_mq_frame_destroy(gop_mq_msg_pluck(msg,0));  //PING command

    tdf = gop_mq_msg_pluck(msg, 0);   //Arg
    gop_mq_get_frame(tdf, (void **)&td, &err);
    if (err != sizeof(test_data_t)) {
        log_printf(0, "ERROR: test_data data argument incorrect size!  size=%d\n", err);
    }

    //** What's left in msg is the tracking address ***

    //** Form the core of the response messeage
    response = gop_mq_msg_new();
    gop_mq_msg_append_mem(response, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(response, MQF_RESPONSE_KEY, MQF_RESPONSE_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_frame(response, pid);
    gop_mq_msg_append_frame(response, tdf);
    gop_mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);

    //** Add the address
    gop_mq_msg_apply_return_address(response, msg, 1);

    //** Make the trackaddress response if needed
    log_printf(5, "address_reply=%d\n", td->address_reply);
    track_response = (td->address_reply == 1) ? gop_mq_msg_trackaddress(host, msg, pid, 1) : NULL;

    if (td->delay == 0) {
        gop_mq_get_frame(pid, (void **)&data, &size);;
        log_printf(3, "delay=0.  Sending response. gid=" LU "\n", *(uint64_t *)data);
        //** Send the response
        apr_thread_mutex_lock(lock);
        server_stats.outgoing[MQS_RESPONSE_INDEX]++;
        apr_thread_mutex_unlock(lock);

        if (p == NULL) {
            err = gop_mq_send(sock, response, 0);
            gop_mq_msg_destroy(response);
        } else {
            err = gop_mq_submit(server_portal, gop_mq_task_new(gop_mq_portal_mq_context(server_portal), response, NULL, NULL, 5));
        }

        if (err != 0) {
            log_printf(0, "ERROR:  Failed sending PONG message to host=%s error=%d\n", host, err);
        }

    } else if (td->delay > 0) {
        log_printf(3, "delay>0.  Deferring response.\n");
        tbx_type_malloc(defer, defer_t, 1);
        defer->msg = response;
        defer->td = td;
        defer->td->ping_count = tbx_atomic_get(ping_count);
        defer->expire = apr_time_now() + apr_time_from_sec(td->delay);
        apr_thread_mutex_lock(lock);
        tbx_stack_push(deferred_pending, defer);
        apr_thread_cond_signal(cond);
        apr_thread_mutex_unlock(lock);
    } else {
        log_printf(3, "delay<0.  Dropping response.\n");
        gop_mq_msg_destroy(response);
    }

    if (track_response != NULL) {  //** Send a TRACKADDESS response
        apr_thread_mutex_lock(lock);
        server_stats.outgoing[MQS_TRACKADDRESS_INDEX]++;
        apr_thread_mutex_unlock(lock);

        if (p == NULL) {
            err = gop_mq_send(sock, track_response, 0);
            gop_mq_msg_destroy(track_response);
        } else {
            err = gop_mq_submit(server_portal, gop_mq_task_new(gop_mq_portal_mq_context(server_portal), track_response, NULL, NULL, 5));
        }


        if (err != 0) {
            log_printf(0, "ERROR:  Failed sending TRACKADDRESS message to host=%s error=%d\n", host, err);
        }
    }

    gop_mq_msg_destroy(msg);

    log_printf(5, "TEXEC END\n");

    return(0);
}

//***************************************************************************
// server_handle_request - Processes a request
//***************************************************************************

int server_handle_request(gop_mq_socket_t *sock)
{
    mq_msg_t *msg;
    gop_mq_frame_t *f;
    char *data;
    int size, status;

    status = 0;
    msg = gop_mq_msg_new();
    if (gop_mq_recv(sock, msg, MQ_DONTWAIT) != 0) {
        log_printf(0, "ERROR recving message!\n");
        return(1);
    }

    f = gop_mq_msg_first(msg);
    gop_mq_get_frame(f, (void **)&data, &size);
    if (size != 0) {  //** SHould be an empty frame
        log_printf(0, " ERROR:  Missing initial empty frame!\n");
        status = 1;
        goto fail;
    }

    //** This is the version frame
    f = gop_mq_msg_next(msg);
    gop_mq_get_frame(f, (void **)&data, &size);
    if (mq_data_compare(data, size, MQF_VERSION_KEY, MQF_VERSION_SIZE) != 0) {
        log_printf(0, "ERROR:  Missing version frame!\n");
        status = 1;
        goto fail;
    }

    //** This is the command frame
    f = gop_mq_msg_next(msg);
    gop_mq_get_frame(f, (void **)&data, &size);

    if (mq_data_compare(data, size, MQF_PING_KEY, MQF_PING_SIZE) == 0) {
        status = proc_ping(sock, msg);
    } else if (mq_data_compare(data, size, MQF_EXEC_KEY, MQF_EXEC_SIZE) == 0) {
        status = proc_exec_ping(sock, msg);
    } else if (mq_data_compare(data, size, MQF_TRACKEXEC_KEY, MQF_TRACKEXEC_SIZE) == 0) {
        status = proc_trackexec_ping(NULL, sock, msg);
    } else {
        status = (unsigned char)data[0];
        log_printf(0, "ERROR:  Unknown Command! size=%d c=%d\n", size, status);
        status = 1;
    }

fail:
    return(status);
}

//***************************************************************************
// server_test_raw_socket -  SErver test using mq_socket directly
//***************************************************************************

void *server_test_raw_socket()
{
    gop_mq_socket_context_t *ctx;
    gop_mq_socket_t *sock;
    gop_mq_pollitem_t pfd[3];
    int dt = 1 * 1000;
    int err, n, finished;
    char v;

    log_printf(0, "START\n");

    ctx = gop_mq_socket_context_new();
    sock = gop_mq_socket_new(ctx, MQ_TRACE_ROUTER);
    err = gop_mq_bind(sock, host);
    if (err != 0) {
        log_printf(0, "ERROR:  Failed connecting to host=%s error=%d errno=%d\n", host, err, errno);
        goto fail;
    }

    //**Make the poll structure
    memset(pfd, 0, sizeof(gop_mq_pollitem_t)*3);
    gop_mq_pipe_poll_store(&(pfd[0]), control_efd[0], MQ_POLLIN);
    pfd[1].socket = gop_mq_poll_handle(sock);
    pfd[1].events = MQ_POLLIN;
    gop_mq_pipe_poll_store(&(pfd[2]), server_efd[0], MQ_POLLIN);

    //** Main processing loop
    finished = 0;
    do {
        log_printf(5, "Before poll dt=%d\n", dt);
        n = gop_mq_poll(pfd, 3, dt);
        log_printf(5, "pfd[control]=%d pfd[socket]=%d pdf[deferred]=%d\n", pfd[0].revents, pfd[1].revents, pfd[2].revents);
        if (n > 0) {  //** Got an event so process it
            if (pfd[0].revents != 0) {
                finished = 1;
                WARN_UNLESS(1 == gop_mq_pipe_read(control_efd[0], &v));
            }
            if (pfd[1].revents != 0) finished = server_handle_request(sock);
            if (pfd[2].revents != 0) finished = server_handle_deferred(sock);
        }
    } while (finished == 0);

fail:

    //** Wake up the deferred thread and tell it to shut down
//  apr_thread_mutex_lock(lock);
//  shutdown_everything = 1;
//  apr_thread_cond_broadcast(cond);
//  apr_thread_mutex_unlock(lock);

    gop_mq_stats_print(0, "Server RAW", &server_stats);
    log_printf(0, "END\n");
    tbx_log_flush();
    gop_mq_socket_destroy(ctx, sock);


    log_printf(0, "before ctx destroy\n");
    tbx_log_flush();
    gop_mq_socket_context_destroy(ctx);

    log_printf(0, "after ctx destroy\n");
    tbx_log_flush();

    return(NULL);
}

//***************************************************************************
// cb_ping - Handles the PING response
//***************************************************************************

void cb_ping(void *arg, gop_mq_task_t *task)
{
    log_printf(3, "START\n");
    tbx_log_flush();
    proc_trackexec_ping(server_portal, NULL, task->msg);
    log_printf(3, "END\n");
    tbx_log_flush();
    task->msg = NULL;  //** The proc routine free's this
}

//***************************************************************************
// server_make_context - Makes the MQ portal context
//***************************************************************************

gop_mq_context_t *server_make_context()
{
    char *text_params = "[mq_context]\n"
                        "  min_conn=1\n"
                        "  max_conn=2\n"
                        "  min_threads=2\n"
                        "  max_threads=100\n"
                        "  backlog_trigger=1000\n"
                        "  heartbeat_dt=1\n"
                        "  heartbeat_failure=5\n"
                        "  min_ops_per_sec=100\n";
    tbx_inip_file_t *ifd;
    gop_mq_context_t *mqc;

    ifd = tbx_inip_string_read(text_params);
    mqc = gop_mq_create_context(ifd, "mq_context");
    assert(mqc != NULL);
    tbx_inip_destroy(ifd);

    return(mqc);
}

//***************************************************************************
// server_test_mq_loop
//***************************************************************************

void server_test_mq_loop()
{
    gop_mq_context_t *mqc;
    gop_mq_command_table_t *table;
    char v;
    log_printf(0, "START\n");

    //** Make the server portal
    mqc = server_make_context();

    //** Make the server portal
    server_portal = gop_mq_portal_create(mqc, host, MQ_CMODE_SERVER);
    table = gop_mq_portal_command_table(server_portal);
    gop_mq_command_set(table, MQF_PING_KEY, MQF_PING_SIZE, NULL, cb_ping);

    gop_mq_portal_install(mqc, server_portal);

    //** Wait for a shutdown
    WARN_UNLESS(1 == gop_mq_pipe_read(control_efd[0], &v));

    //** Destroy the portal
    gop_mq_destroy_context(mqc);

    log_printf(0, "END\n");
}


//***************************************************************************
// server_test_thread -  Server test thread
//***************************************************************************

void *server_test_thread(apr_thread_t *th, void *arg)
{
    log_printf(0, "START\n");

    memset(&server_stats, 0, sizeof(server_stats));

    //** Do the raw socket tests
    log_printf(0, "Using raw socket for event loop\n");
    tbx_log_flush();
    server_test_raw_socket();

    //** Now do the same but usingthe MQ event loop.
    log_printf(0, "Switching to using MQ event loop\n");
    tbx_log_flush();
    server_test_mq_loop();

    //** Wake up the deferred thread and tell it to shut down
    apr_thread_mutex_lock(lock);
    shutdown_everything = 1;
    apr_thread_cond_broadcast(cond);
    apr_thread_mutex_unlock(lock);

    return(NULL);
}

//***************************************************************************
// server_deferred_thread - Handles deferred/delayed responses
//***************************************************************************

void *server_deferred_thread(apr_thread_t *th, void *arg)
{
    defer_t *defer;
    apr_time_t dt, now, max_wait;
    uint64_t n;
    int i;
    char v;

    max_wait = apr_time_make(1, 0);
    dt = max_wait;

    apr_thread_mutex_lock(lock);
    while (shutdown_everything == 0) {
        if (dt > max_wait) dt = max_wait;
        apr_thread_cond_timedwait(cond, lock, dt);

        log_printf(5, "checking deferred_pending tbx_stack_count=%d\n", tbx_stack_count(deferred_pending));
        //** Move anything expired to the ready queue
        //** Keeping track of the next wakeup call
        dt = apr_time_make(100, 0);
        now = apr_time_now();
        tbx_stack_move_to_top(deferred_pending);
        n = 0;
        while ((defer = tbx_stack_get_current_data(deferred_pending)) != NULL) {
            if (defer->expire < now) {  //** Expired so move it for sending
                tbx_stack_delete_current(deferred_pending, 0, 0);
                tbx_stack_push(deferred_ready, defer);
                n++;
            } else if (dt > defer->expire) {  //** Keep track of when to wake up next
                dt = defer->expire;
                tbx_stack_move_down(deferred_pending);
            } else {
                tbx_stack_move_down(deferred_pending);
            }
        }

        log_printf(5, "deferred_ready=%d deferred_pending=%d n= " LU " server_portal=%p\n", tbx_stack_count(deferred_ready), tbx_stack_count(deferred_pending), n, server_portal);

        if (n > 0) { //** Got tasks to send
            if (server_portal == NULL) {
                v = 1;
                for (i=0; i<(int)n; i++) WARN_UNLESS(1 == gop_mq_pipe_write(server_efd[1], &v));
            } else {
                server_handle_deferred(NULL);
            }
        }
    }
    apr_thread_mutex_unlock(lock);

    return(NULL);
}

//***************************************************************************
//***************************************************************************
//***************************************************************************

int main(int argc, char **argv)
{
    apr_pool_t *mpool;
    apr_thread_t *server_thread, *client_thread, *deferred_thread;
    apr_status_t dummy;
    gop_mq_socket_context_t *ctx;
    int i;
    int volatile start_option;  //** This disables optimizing the arg loop and getting a warning. 
    char v;

    if (argc < 2) {
        printf("mq_test [-d log_level]\n");
        return(0);
    }

    i = 1;
    do {
        start_option = i;

        if (strcmp(argv[i], "-d") == 0) { //** Enable debugging
            i++;
            tbx_set_log_level(atol(argv[i]));
            i++;
        } else if (strcmp(argv[i], "-h") == 0) { //** Print help
            printf("mq_test [-d log_level]\n");
            return(0);
        }
    } while ((start_option < i) && (i<argc));

    printf("log_level=%d\n", _log_level);

    gop_init_opque_system();

    apr_pool_create(&mpool, NULL);
    assert_result(apr_thread_mutex_create(&lock, APR_THREAD_MUTEX_DEFAULT, mpool), APR_SUCCESS);
    assert_result(apr_thread_cond_create(&cond, mpool), APR_SUCCESS);

    //** Make the pipe for controlling the server
    ctx = gop_mq_socket_context_new();
    gop_mq_pipe_create(ctx, control_efd);

    //** Make the server pipe for delayed responses
    gop_mq_pipe_create(ctx, server_efd);

    //** Make the stacks for controlling deferred replies
    deferred_ready = tbx_stack_new();
    deferred_pending = tbx_stack_new();

    tbx_thread_create_assert(&client_thread, NULL, client_test_thread, NULL, mpool);
    tbx_thread_create_assert(&server_thread, NULL, server_test_thread, NULL, mpool);
    tbx_thread_create_assert(&deferred_thread, NULL, server_deferred_thread, NULL, mpool);

    apr_thread_join(&dummy, client_thread);

    //** Trigger the server to shutdown
    v = 1;
    WARN_UNLESS(1 == gop_mq_pipe_write(control_efd[1], &v));
    apr_thread_join(&dummy, server_thread);
    apr_thread_join(&dummy, deferred_thread);

    tbx_stack_del(deferred_ready, 0);
    tbx_stack_del(deferred_pending, 0);

    gop_mq_pipe_destroy(ctx, control_efd);
    gop_mq_pipe_destroy(ctx, server_efd);

    gop_mq_socket_context_destroy(ctx);

    apr_thread_mutex_destroy(lock);
    apr_thread_cond_destroy(cond);

    apr_pool_destroy(mpool);

    gop_shutdown();

    return(0);
}

