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

#include "mq_portal.h"
#include "apr_wrapper.h"
#include "log.h"
#include "type_malloc.h"
#include "apr_wrapper.h"

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
Stack_t *deferred_ready = NULL;
Stack_t *deferred_pending = NULL;
mq_command_stats_t server_stats;
mq_portal_t *server_portal = NULL;

char *host = "tcp://127.0.0.1:6714";
mq_pipe_t control_efd[2];
mq_pipe_t server_efd[2];
int shutdown_everything = 0;
atomic_int_t ping_count = 0;

//***************************************************************************
// pack_msg - Packs a message for sending
//***************************************************************************

mq_msg_t *pack_msg(int dotrack, test_data_t *td)
{
    mq_msg_t *msg;

    msg = mq_msg_new();
    mq_msg_append_mem(msg, host, strlen(host), MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
    if (dotrack == 1) {
        mq_msg_append_mem(msg, MQF_TRACKEXEC_KEY, MQF_TRACKEXEC_SIZE, MQF_MSG_KEEP_DATA);
    } else {
        mq_msg_append_mem(msg, MQF_EXEC_KEY, MQF_EXEC_SIZE, MQF_MSG_KEEP_DATA);
    }
    mq_msg_append_mem(msg, &(td->id), sizeof(uint64_t), MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, MQF_PING_KEY, MQF_PING_SIZE, MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, td, sizeof(test_data_t), MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    return(msg);
}


//***************************************************************************
// unpack_msg - unPacks a message received
//***************************************************************************

int unpack_msg(mq_msg_t *msg, test_data_t *td)
{
    mq_frame_t *f;
    char *data;
    uint64_t *id;
    int n, status;

    status = 0;

    //** Parse the response
    f = mq_msg_first(msg);
    mq_get_frame(f, (void **)&data, &n);
    if (n != 0) {  //** SHould be an empty frame
        log_printf(0, " ERROR:  Missing initial empty frame!\n");
        status = 1;
        goto fail;
    }
    f = mq_msg_next(msg);
    mq_get_frame(f, (void **)&data, &n);
    if (mq_data_compare(data, n, MQF_VERSION_KEY, MQF_VERSION_SIZE) != 0) {
        log_printf(0, "ERROR:  Missing version frame!\n");
        status = 1;
        goto fail;
    }
    f = mq_msg_next(msg);
    mq_get_frame(f, (void **)&data, &n);
    if (mq_data_compare(data, n, MQF_RESPONSE_KEY, MQF_RESPONSE_SIZE) != 0) {
        log_printf(0, " ERROR: Bad RESPONSE command frame\n");
        status = 1;
        goto fail;
    }
    f = mq_msg_next(msg);
    mq_get_frame(f, (void **)&id, &n);
    if (n != sizeof(uint64_t)) {
        log_printf(0, " ERROR: Bad ID size!  Got %d should be sizeof(uint64_t)=%d\n", n, sizeof(uint64_t));
        status = 1;
        goto fail;
    }

    f = mq_msg_next(msg);
    mq_get_frame(f, (void **)&data, &n);
    if (n != sizeof(test_data_t)) {
        log_printf(0, " ERROR: Bad test data size!  Got %d should be sizeof(test_data_t)=%d\n", n, sizeof(test_data_t));
        status = 1;
        goto fail;
    }
    *td = *(test_data_t *)data;

    if (td->id != *id) {
        log_printf(0, " ERROR: ID mismatch! id=" LU " td->id=" LU "\n", *id, td->id);
        status = 1;
        goto fail;
    }

    f = mq_msg_next(msg);
    mq_get_frame(f, (void **)&data, &n);
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
    mq_socket_context_t *ctx;
    mq_socket_t *sock;
    mq_msg_t *msg;
    mq_frame_t *f;
    mq_pollitem_t pfd;
    char *data;
    int err, status, n;

    log_printf(0, "TEST: (START) client_direct()\n");

    status = 0;

    sleep(1);  //** Wait for the server to start up and bind to the port
    ctx = mq_socket_context_new();
    sock = mq_socket_new(ctx, MQ_TRACE_ROUTER);
    err = mq_connect(sock, host);
    if (err != 0) {
        log_printf(0, "ERROR:  Failed connecting to host=%s error=%d\n", host, err);
        status = 1;
        goto fail;
    }

    sleep(1);

    //** Compose the PING message
    msg = mq_msg_new();
    mq_msg_append_mem(msg, host, strlen(host), MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, MQF_PING_KEY, MQF_PING_SIZE, MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, "HANDLE", 6, MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    //** Send the message
    err = mq_send(sock, msg, 0);
    if (err != 0) {
        log_printf(0, "ERROR:  Failed sending PING message to host=%s error=%d\n", host, err);
        status = 1;
        goto fail;
    }

    mq_msg_destroy(msg);

    //** Wait for the response
    msg = mq_msg_new();

    pfd.socket = mq_poll_handle(sock);
    pfd.events = MQ_POLLIN;
    err = mq_poll(&pfd, 1, 5000);  //** Wait for 5 secs
    if (err != 1) {
        log_printf(0, "ERROR:  Failed polling for PING message response to host=%s\n", host);
        status = 1;
        goto fail;
    }
    err = mq_recv(sock, msg, MQ_DONTWAIT);
//  err = mq_recv(sock, msg, 0);
    if (err != 0) {
        log_printf(0, "ERROR:  Failed recving PONG response message from host=%s error=%d\n", host, err);
        status = 1;
        goto fail;
    }

    //** Parse the response
    f = mq_msg_first(msg);
    mq_get_frame(f, (void **)&data, &n);
    if (n != 0) {  //** SHould be an empty frame
        log_printf(0, " ERROR:  Missing initial PONG empty frame!\n");
        status = 1;
        goto fail;
    }
    f = mq_msg_next(msg);
    mq_get_frame(f, (void **)&data, &n);
    if (mq_data_compare(data, n, MQF_VERSION_KEY, MQF_VERSION_SIZE) != 0) {
        log_printf(0, "ERROR:  Missing version frame!\n");
        status = 1;
        goto fail;
    }
    f = mq_msg_next(msg);
    mq_get_frame(f, (void **)&data, &n);
    if (mq_data_compare(data, n, MQF_PONG_KEY, MQF_PONG_SIZE) != 0) {
        log_printf(0, " ERROR: Bad PONG command frame\n");
        status = 1;
        goto fail;
    }
    f = mq_msg_next(msg);
    mq_get_frame(f, (void **)&data, &n);
    if (mq_data_compare("HANDLE", 6, data, n) != 0) {
        log_printf(0, " ERROR: Bad PONG ID should be HANDLE got=%s\n", data);
        status = 1;
        goto fail;
    }

fail:
    mq_msg_destroy(msg);
    mq_socket_destroy(ctx, sock);
    mq_socket_context_destroy(ctx);

    if (status == 0) {
        log_printf(0, "TEST: (END) client_direct() = SUCCESS\n");
    } else {
        log_printf(0, "TEST: (END) client_direct() = FAIL\n");
    }
    return(status);
}

//***************************************************************************
//***************************************************************************

int client_exec_ping_test(mq_context_t *mqc)
{
    mq_msg_t *msg;
    op_generic_t *gop;
    int err, status;
    log_printf(0, "TEST: (START) client_exec_ping_test(mqc)\n");

    status = 0;

    //** Compose the PING message
    msg = mq_msg_new();
    mq_msg_append_mem(msg, host, strlen(host), MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, MQF_EXEC_KEY, MQF_EXEC_SIZE, MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, MQF_PING_KEY, MQF_PING_SIZE, MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, "HANDLE", 6, MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    gop = new_mq_op(mqc, msg, NULL, NULL, NULL, 5);

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

op_status_t client_response_pong(void *arg, int id)
{
    mq_task_t *task = (mq_task_t *)arg;
    test_data_t td;
    char b64[1024];
    int err;

    log_printf(1, "Processing response gid=%d\n", gop_id(task->gop));

    err = unpack_msg(task->response, &td);
    if (err != 0) return(op_failure_status);

    log_printf(1, "delay=%d ping_count=%d address_reply=%d sid=%s\n", td.delay, td.ping_count, td.address_reply, mq_id2str((char *)&(td.id), sizeof(td.id), b64, sizeof(b64)));
    if ((td.delay > 0) && (td.address_reply ==1)) {
        if (td.ping_count == 0) {
            if (server_portal == NULL) log_printf(0, "ERROR:  No trackexec heartbeat detected!  delay=%d ping_count=%d\n", td.delay, td.ping_count);
//        return(op_failure_status);
        }
    }

    return(op_success_status);
}

//***************************************************************************
//  client_trackexec_ping_test - Simple ping with response
//***************************************************************************

int client_trackexec_ping_test(mq_context_t *mqc, int delay, int address_reply, int dt)
{
    mq_msg_t *msg;
    op_generic_t *gop;
    int err, status, success_value;
    test_data_t td;

    log_printf(0, "TEST: (START) client_trackexec_ping_test(mqc, delay=%d, reply=%d, dt=%d)\n", delay, address_reply, dt);

    status = 0;

    //** Figure out the edge case being checked.
    success_value = OP_STATE_SUCCESS;
    if ((delay < 0) || (delay > dt)) success_value = OP_STATE_FAILURE;

    //** Compose the PING message
    memset(&td, 0, sizeof(td));
    td.id = atomic_global_counter();
    td.command = CMD_PING;
    td.delay = delay;
    td.address_reply = address_reply;
    msg = pack_msg(1, &td);

    gop = new_mq_op(mqc, msg, client_response_pong, NULL, NULL, dt);
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
    flush_log();
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
    td->id = atomic_global_counter();
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

void generate_tasks(mq_context_t *mqc, opque_t *q, int count, test_data_t *td_base)
{
    int i;
    op_generic_t *gop;
    test_data_t *td;
    mq_msg_t *msg;

    for (i=0; i< count; i++) {
        type_malloc(td, test_data_t, 1);
        *td = *td_base;

        msg = pack_msg(1, td);
        gop = new_mq_op(mqc, msg, client_response_pong, td, free, td->dt);
        td->id = gop_id(gop);
        gop_set_private(gop, td);
        opque_add(q, gop);
    }
}

//***************************************************************************
//  bulk_test - Does a bulk ping/pong test
//***************************************************************************

int bulk_test(mq_context_t *mqc)
{
    int err, n, expire, quick;
    opque_t *q = new_opque();
    op_generic_t *gop;
    test_data_t tdc, *td;
    op_status_t status;
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

    log_printf(0, "TEST: (START) %d tasks in bulk test\n", opque_task_count(q));
    err = 0;
    n = 0;
    start_time = apr_time_now();
    while ((gop = opque_waitany(q)) != NULL) {
        n++;
        td = (test_data_t *)gop_get_private(gop);
        status = gop_get_status(gop);
        if (td->success_value != status.op_status) {
            err++;
            dt = apr_time_now() - start_time;
            ttime = (1.0*dt) / APR_USEC_PER_SEC;
            log_printf(0, "TEST: (ERROR) gid=%d bulk_ping_test(delay=%d, reply=%d, dt=%d) = FAIL (g=%d, s=%d) dt=%lf sid=%s\n", gop_id(gop), td->delay, td->address_reply, td->dt, status.op_status, td->success_value, ttime, mq_id2str((char *)&(td->id), sizeof(td->id), b64, sizeof(b64)));
        }

        dt = apr_time_now() - start_time;
        ttime = (1.0*dt) / APR_USEC_PER_SEC;
        log_printf(0, "BULK n=%d err=%d gid=%d dt=%lf sid=%s (delay=%d reply=%d dt=%d) got=%d shouldbe=%d\n", n, err, gop_id(gop), ttime, mq_id2str((char *)&(td->id), sizeof(td->id), b64, sizeof(b64)), td->delay, td->address_reply, td->dt, status.op_status, td->success_value);
        flush_log();
        dt = apr_time_now();
        gop_free(gop, OP_DESTROY);
        dt = apr_time_now() - dt;
        ttime = (1.0*dt) / APR_USEC_PER_SEC;
        log_printf(0, "BULK gop_free dt=%lf\n", ttime);
        flush_log();
    }

    dt = apr_time_now() - start_time;
    ttime = (1.0*dt) / APR_USEC_PER_SEC;
    log_printf(0, "TEST: (END) Completed %d tasks in bulk test failed=%d dt=%lf\n", opque_task_count(q), err, ttime);
    flush_log();
    if (ttime > expire) {
        log_printf(0, "TEST: (END) !!WARNING!! Execution time %lf > %d sec!!!! The dt=%d was used for the commands so I would expect failures!\n", ttime, expire, expire);
        log_printf(0, "TEST: (END) !!WARNING!! This probably means you are running under valgrind and is to be expected.\n");
    }

    log_printf(0, "Sleeping to let other threads close\n");
    sleep(5);
    log_printf(0, "Waking up\n");

    opque_free(q, OP_DESTROY);
    return(err);
}


//***************************************************************************
// client_edge_tests -  Client edges tests
//***************************************************************************

int client_edge_tests(mq_context_t *mqc)
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

mq_context_t *client_make_context()
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
    inip_file_t *ifd;
    mq_context_t *mqc;

    ifd = inip_read_text(text_params);
    mqc = mq_create_context(ifd, "mq_context");
    assert(mqc != NULL);
    inip_destroy(ifd);

    return(mqc);
}


//***************************************************************************
// client_test_thread -  Client test thread
//***************************************************************************

void *client_test_thread(apr_thread_t *th, void *arg)
{
    char v;
    int nfail, nfail_total, i, min;
    mq_context_t *mqc;

    log_printf(0, "START\n");

    nfail_total = 0;

    //** perform ths simple base direct client test
    nfail_total += client_direct();
    flush_log();

    //** The rest of the tests all go through the mq_portal so we need to configure that now
    //** Make the portal
    mqc = client_make_context();

    nfail_total += client_exec_ping_test(mqc);  //** Simple exec ping test.  No tracking
    flush_log();

    min = 0;
    if (min == 1) {
        v = 1;
        log_printf(0, "Skipping raw tests.\n");
        mq_pipe_write(control_efd[1], &v);
        sleep(1);
        log_printf(0, "Continuing....\n");
    }

    for (i=min; i<2; i++) {
        nfail = 0;

        //** Check edge cases
        log_printf(0, "Checking edge cases (ROUND=%d)\n", i);
        flush_log();
//     nfail += client_edge_tests(mqc);

        log_printf(0, "Switching to bulk tests (ROUND=%d)\n", i);
        flush_log();
        nfail += bulk_test(mqc);

        log_printf(0, "Completed round %d of tests. failed:%d\n", i, nfail);
        flush_log();

        nfail_total += nfail;

        if (i==0) { //** Switch the server to using the MQ loop
            v = 1;
            log_printf(0, "Telling server to switch and use the MQ event loop\n");
            mq_pipe_write(control_efd[1], &v);
            sleep(10);
            log_printf(0, "Continuing....\n");
        }
    }


    //** Destroy the portal
    mq_destroy_context(mqc);

    log_printf(0, "END\n");

    printf("Total Tasks failed: %d\n", nfail);

    return(NULL);
}

//***************************************************************************
// proc_ping - Processes a ping request
//***************************************************************************

int proc_ping(mq_socket_t *sock, mq_msg_t *msg)
{
    mq_msg_t *pong;
    mq_frame_t *f, *pid;
    int err;

//void *data;
//for (f = mq_msg_first(msg), i=0; f != NULL; f = mq_msg_next(msg), i++) {
//  mq_get_frame(f, &data, &err);
//  log_printf(5, "fsize[%d]=%d\n", i, err);
//}

    apr_thread_mutex_lock(lock);
    server_stats.incoming[MQS_PING_INDEX]++;
    apr_thread_mutex_unlock(lock);

    //** Peel off the top frames and just leave the return address
    f = mq_msg_first(msg);
    mq_frame_destroy(mq_msg_pluck(msg, 0));  //blank
    mq_frame_destroy(mq_msg_pluck(msg, 0));  //version
    mq_frame_destroy(mq_msg_pluck(msg,0));  //command

    pid = mq_msg_pluck(msg, 0);  //Ping ID

    atomic_inc(ping_count);

    pong = mq_msg_new();

    //** Push the address in reverse order (including the empty frame)
    while ((f = mq_msg_pop(msg)) != NULL) {
//i=mq_get_frame(f, &data, &err);
//log_printf(5, "data=%s len=%d i=%d\n", (char *)data, err, i); flush_log();
//log_printf(5, "add=%d\n", err);
        mq_msg_push_frame(pong, f);
    }

    mq_msg_destroy(msg);

    //** Now add the command
    mq_msg_append_mem(pong, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(pong, MQF_PONG_KEY, MQF_PONG_SIZE, MQF_MSG_KEEP_DATA);
    mq_msg_append_frame(pong, pid);
    mq_msg_append_mem(pong, NULL, 0, MQF_MSG_KEEP_DATA);

//f = mq_msg_first(pong);
//mq_get_frame(f, &data, &err);
//log_printf(5, "1.data=%s len=%d\n", (char *)data, err);

    apr_thread_mutex_lock(lock);
    server_stats.outgoing[MQS_PONG_INDEX]++;
    apr_thread_mutex_unlock(lock);

    err = mq_send(sock, pong, MQ_DONTWAIT);

    mq_msg_destroy(pong);

    return(err);
}

//***************************************************************************
// proc_app_ping - Processes a ping request from the application
//***************************************************************************

int proc_exec_ping(mq_socket_t *sock, mq_msg_t *msg)
{
    mq_frame_t *f, *pid;
    char *data;
    int err;

    int i;
    for (f = mq_msg_first(msg), i=0; f != NULL; f = mq_msg_next(msg), i++) {
        mq_get_frame(f, (void **)&data, &err);
        log_printf(5, "fsize[%d]=%d\n", i, err);
    }

    apr_thread_mutex_lock(lock);
    server_stats.incoming[MQS_PING_INDEX]++;
    apr_thread_mutex_unlock(lock);

    //** Peel off the top frames and just leave the return address
    f = mq_msg_first(msg);
    mq_frame_destroy(mq_msg_pluck(msg, 0));  //blank
    mq_frame_destroy(mq_msg_pluck(msg, 0));  //version
    mq_frame_destroy(mq_msg_pluck(msg,0));  //command(exec)
    mq_frame_destroy(mq_msg_pluck(msg,0));  //app command(ping)

    pid = mq_msg_current(msg);  //Ping ID

    //** Notify the sender;
    mq_get_frame(pid, (void **)&data, &err);
    apr_thread_mutex_lock(lock);
    type_malloc_clear(handle, char, err+1);
    memcpy(handle, data, err);
    log_printf(5, "setting handle=%s nbytes=%d\n", handle, err);
    apr_thread_cond_broadcast(cond);
    apr_thread_mutex_unlock(lock);

    mq_msg_destroy(msg);

    return(0);
}

//***************************************************************************
// sever_handle_deferred - Handles sending deferred responses
//***************************************************************************

int server_handle_deferred(mq_socket_t *sock)
{
    defer_t *defer;
    int err;
    char v;

    log_printf(5, "deferred responses to handle %d (server_portal=%p)\n", stack_size(deferred_ready), server_portal);

    err = 0;
    while ((defer = pop(deferred_ready)) != NULL) {
        if (server_portal == NULL) mq_pipe_read(server_efd[0], &v);
        log_printf(5, "Processing deferred response\n");

        defer->td->ping_count = atomic_get(ping_count) - defer->td->ping_count;  //** Send back the ping count since it was sent

        //** Send the response
        if (server_portal == NULL) apr_thread_mutex_lock(lock);
        server_stats.outgoing[MQS_RESPONSE_INDEX]++;
        if (server_portal == NULL) apr_thread_mutex_unlock(lock);

        if (server_portal == NULL) {
            err += mq_send(sock, defer->msg, 0);
            mq_msg_destroy(defer->msg);
        } else {
            err = mq_submit(server_portal, mq_task_new(server_portal->mqc, defer->msg, NULL, NULL, 5));
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

int proc_trackexec_ping(mq_portal_t *p, mq_socket_t *sock, mq_msg_t *msg)
{
    mq_frame_t *pid, *tdf;
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
    mq_msg_first(msg);
    mq_frame_destroy(mq_msg_pluck(msg, 0));  //blank
    mq_frame_destroy(mq_msg_pluck(msg, 0));  //version
    mq_frame_destroy(mq_msg_pluck(msg,0));  //command(trackexec)

    pid = mq_msg_pluck(msg, 0);  //Ping ID
    mq_get_frame(pid, (void **)&data, &size);
    log_printf(1, "TEXEC sid=%s\n", mq_id2str(data, size, b64, sizeof(b64)));
    mq_frame_destroy(mq_msg_pluck(msg,0));  //PING command

    tdf = mq_msg_pluck(msg, 0);   //Arg
    mq_get_frame(tdf, (void **)&td, &err);
    if (err != sizeof(test_data_t)) {
        log_printf(0, "ERROR: test_data data argument incorrect size!  size=%d\n", err);
    }

    //** What's left in msg is the tracking address ***

    //** Form the core of the response messeage
    response = mq_msg_new();
    mq_msg_append_mem(response, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(response, MQF_RESPONSE_KEY, MQF_RESPONSE_SIZE, MQF_MSG_KEEP_DATA);
    mq_msg_append_frame(response, pid);
    mq_msg_append_frame(response, tdf);
    mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);

    //** Add the address
    mq_apply_return_address_msg(response, msg, 1);

    //** Make the trackaddress response if needed
    log_printf(5, "address_reply=%d\n", td->address_reply);
    track_response = (td->address_reply == 1) ? mq_trackaddress_msg(host, msg, pid, 1) : NULL;

    if (td->delay == 0) {
        mq_get_frame(pid, (void **)&data, &size);;
        log_printf(3, "delay=0.  Sending response. gid=" LU "\n", *(uint64_t *)data);
        //** Send the response
        apr_thread_mutex_lock(lock);
        server_stats.outgoing[MQS_RESPONSE_INDEX]++;
        apr_thread_mutex_unlock(lock);

        if (p == NULL) {
            err = mq_send(sock, response, 0);
            mq_msg_destroy(response);
        } else {
            err = mq_submit(server_portal, mq_task_new(server_portal->mqc, response, NULL, NULL, 5));
        }

        if (err != 0) {
            log_printf(0, "ERROR:  Failed sending PONG message to host=%s error=%d\n", host, err);
        }

    } else if (td->delay > 0) {
        log_printf(3, "delay>0.  Deferring response.\n");
        type_malloc(defer, defer_t, 1);
        defer->msg = response;
        defer->td = td;
        defer->td->ping_count = atomic_get(ping_count);
        defer->expire = apr_time_now() + apr_time_from_sec(td->delay);
        apr_thread_mutex_lock(lock);
        push(deferred_pending, defer);
        apr_thread_cond_signal(cond);
        apr_thread_mutex_unlock(lock);
    } else {
        log_printf(3, "delay<0.  Dropping response.\n");
        mq_msg_destroy(response);
    }

    if (track_response != NULL) {  //** Send a TRACKADDESS response
        apr_thread_mutex_lock(lock);
        server_stats.outgoing[MQS_TRACKADDRESS_INDEX]++;
        apr_thread_mutex_unlock(lock);

        if (p == NULL) {
            err = mq_send(sock, track_response, 0);
            mq_msg_destroy(track_response);
        } else {
            err = mq_submit(server_portal, mq_task_new(server_portal->mqc, track_response, NULL, NULL, 5));
        }


        if (err != 0) {
            log_printf(0, "ERROR:  Failed sending TRACKADDRESS message to host=%s error=%d\n", host, err);
        }
    }

    mq_msg_destroy(msg);

    log_printf(5, "TEXEC END\n");

    return(0);
}

//***************************************************************************
// server_handle_request - Processes a request
//***************************************************************************

int server_handle_request(mq_socket_t *sock)
{
    mq_msg_t *msg;
    mq_frame_t *f;
    char *data;
    int size, status;

    status = 0;
    msg = mq_msg_new();
    if (mq_recv(sock, msg, MQ_DONTWAIT) != 0) {
        log_printf(0, "ERROR recving message!\n");
        return(1);
    }

    f = mq_msg_first(msg);
    mq_get_frame(f, (void **)&data, &size);
    if (size != 0) {  //** SHould be an empty frame
        log_printf(0, " ERROR:  Missing initial empty frame!\n");
        status = 1;
        goto fail;
    }

    //** This is the version frame
    f = mq_msg_next(msg);
    mq_get_frame(f, (void **)&data, &size);
    if (mq_data_compare(data, size, MQF_VERSION_KEY, MQF_VERSION_SIZE) != 0) {
        log_printf(0, "ERROR:  Missing version frame!\n");
        status = 1;
        goto fail;
    }

    //** This is the command frame
    f = mq_msg_next(msg);
    mq_get_frame(f, (void **)&data, &size);

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
    mq_socket_context_t *ctx;
    mq_socket_t *sock;
    mq_pollitem_t pfd[3];
    int dt = 1 * 1000;
    int err, n, finished;
    char v;

    log_printf(0, "START\n");

    ctx = mq_socket_context_new();
    sock = mq_socket_new(ctx, MQ_TRACE_ROUTER);
    err = mq_bind(sock, host);
    if (err != 0) {
        log_printf(0, "ERROR:  Failed connecting to host=%s error=%d errno=%d\n", host, err, errno);
        goto fail;
    }

    //**Make the poll structure
    memset(pfd, 0, sizeof(mq_pollitem_t)*3);
    mq_pipe_poll_store(&(pfd[0]), control_efd[0], MQ_POLLIN);
    pfd[1].socket = mq_poll_handle(sock);
    pfd[1].events = MQ_POLLIN;
    mq_pipe_poll_store(&(pfd[2]), server_efd[0], MQ_POLLIN);

    //** Main processing loop
    finished = 0;
    do {
        log_printf(5, "Before poll dt=%d\n", dt);
        n = mq_poll(pfd, 3, dt);
        log_printf(5, "pfd[control]=%d pfd[socket]=%d pdf[deferred]=%d\n", pfd[0].revents, pfd[1].revents, pfd[2].revents);
        if (n > 0) {  //** Got an event so process it
            if (pfd[0].revents != 0) {
                finished = 1;
                mq_pipe_read(control_efd[0], &v);
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

    mq_stats_print(0, "Server RAW", &server_stats);
    log_printf(0, "END\n");
    flush_log();
    mq_socket_destroy(ctx, sock);


    log_printf(0, "before ctx destroy\n");
    flush_log();
    mq_socket_context_destroy(ctx);

    log_printf(0, "after ctx destroy\n");
    flush_log();

    return(NULL);
}

//***************************************************************************
// cb_ping - Handles the PING response
//***************************************************************************

void cb_ping(void *arg, mq_task_t *task)
{
    log_printf(3, "START\n");
    flush_log();
    proc_trackexec_ping(server_portal, NULL, task->msg);
    log_printf(3, "END\n");
    flush_log();
    task->msg = NULL;  //** The proc routine free's this
}

//***************************************************************************
// server_make_context - Makes the MQ portal context
//***************************************************************************

mq_context_t *server_make_context()
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
    inip_file_t *ifd;
    mq_context_t *mqc;

    ifd = inip_read_text(text_params);
    mqc = mq_create_context(ifd, "mq_context");
    assert(mqc != NULL);
    inip_destroy(ifd);

    return(mqc);
}

//***************************************************************************
// server_test_mq_loop
//***************************************************************************

void server_test_mq_loop()
{
    mq_context_t *mqc;
    mq_command_table_t *table;
    char v;
    log_printf(0, "START\n");

    //** Make the server portal
    mqc = server_make_context();

    //** Make the server portal
    server_portal = mq_portal_create(mqc, host, MQ_CMODE_SERVER);
    table = mq_portal_command_table(server_portal);
    mq_command_set(table, MQF_PING_KEY, MQF_PING_SIZE, NULL, cb_ping);

    mq_portal_install(mqc, server_portal);

    //** Wait for a shutdown
    mq_pipe_read(control_efd[0], &v);

    //** Destroy the portal
    mq_destroy_context(mqc);

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
    flush_log();
    server_test_raw_socket();

    //** Now do the same but usingthe MQ event loop.
    log_printf(0, "Switching to using MQ event loop\n");
    flush_log();
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

        log_printf(5, "checking deferred_pending stack_size=%d\n", stack_size(deferred_pending));
        //** Move anything expired to the ready queue
        //** Keeping track of the next wakeup call
        dt = apr_time_make(100, 0);
        now = apr_time_now();
        move_to_top(deferred_pending);
        n = 0;
        while ((defer = get_ele_data(deferred_pending)) != NULL) {
            if (defer->expire < now) {  //** Expired so move it for sending
                delete_current(deferred_pending, 0, 0);
                push(deferred_ready, defer);
                n++;
            } else if (dt > defer->expire) {  //** Keep track of when to wake up next
                dt = defer->expire;
                move_down(deferred_pending);
            } else {
                move_down(deferred_pending);
            }
        }

        log_printf(5, "deferred_ready=%d deferred_pending=%d n= " LU " server_portal=%p\n", stack_size(deferred_ready), stack_size(deferred_pending), n, server_portal);

        if (n > 0) { //** Got tasks to send
            if (server_portal == NULL) {
                v = 1;
                for (i=0; i<n; i++) mq_pipe_write(server_efd[1], &v);
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
    mq_socket_context_t *ctx;
    int i, start_option;
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
            set_log_level(atol(argv[i]));
            i++;
        } else if (strcmp(argv[i], "-h") == 0) { //** Print help
            printf("mq_test [-d log_level]\n");
            return(0);
        }
    } while ((start_option < i) && (i<argc));

    printf("log_level=%d\n", _log_level);

    apr_wrapper_start();
    init_opque_system();

    apr_pool_create(&mpool, NULL);
    { int result = apr_thread_mutex_create(&lock, APR_THREAD_MUTEX_DEFAULT, mpool); assert(result == APR_SUCCESS); }
    { int result = apr_thread_cond_create(&cond, mpool); assert(result == APR_SUCCESS); }

    //** Make the pipe for controlling the server
    ctx = mq_socket_context_new();
    mq_pipe_create(ctx, control_efd);

    //** Make the server pipe for delayed responses
    mq_pipe_create(ctx, server_efd);

    //** Make the stacks for controlling deferred replies
    deferred_ready = new_stack();
    deferred_pending = new_stack();

    thread_create_assert(&client_thread, NULL, client_test_thread, NULL, mpool);
    thread_create_assert(&server_thread, NULL, server_test_thread, NULL, mpool);
    thread_create_assert(&deferred_thread, NULL, server_deferred_thread, NULL, mpool);

    apr_thread_join(&dummy, client_thread);

    //** Trigger the server to shutdown
    v = 1;
    mq_pipe_write(control_efd[1], &v);
    apr_thread_join(&dummy, server_thread);
    apr_thread_join(&dummy, deferred_thread);

    free_stack(deferred_ready, 0);
    free_stack(deferred_pending, 0);

    mq_pipe_destroy(ctx, control_efd);
    mq_pipe_destroy(ctx, server_efd);

    mq_socket_context_destroy(ctx);

    apr_thread_mutex_destroy(lock);
    apr_thread_cond_destroy(cond);

    apr_pool_destroy(mpool);

    destroy_opque_system();
    apr_wrapper_stop();

    return(0);
}

