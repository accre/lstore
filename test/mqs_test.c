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

#include <gop/mq_helpers.h>
#include <gop/mq_ongoing.h>
#include <gop/mq_stream.h>
#include <gop/opque.h>
#include <gop/portal.h>
#include <tbx/apr_wrapper.h>
#include <tbx/log.h>
#include <tbx/random.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>
#include <unistd.h>

#define MQS_TEST_KEY  "mqs_test"
#define MQS_TEST_SIZE 8

typedef struct {
    int launch_flusher;
    int delay;
    int client_delay;
    int max_packet;
    int send_bytes;
    int stop_reading_bytes;
    int timeout;
    int shouldbe;
    int gid;
} test_gop_t;

apr_thread_mutex_t *lock = NULL;
apr_thread_cond_t  *cond = NULL;
char *handle = NULL;
gop_mq_command_stats_t server_stats;
gop_mq_portal_t *server_portal = NULL;
gop_mq_ongoing_t *server_ongoing = NULL;
gop_mq_ongoing_t *client_ongoing = NULL;
mq_msg_t *host = NULL;

char *test_data = NULL;
int test_size = 1024*1024;
int packet_min = 1024;
int packet_max = 1024*1024;
int send_min = 1024;
int send_max = 10*1024*1024;
int nparallel = 100;
int ntotal = 2000;
int do_compress = MQS_PACK_RAW;
int timeout = 10;
int stream_max_size = 4096;
int launch_flusher = 0;
int delay_response = 0;
int in_process = 0;

int ongoing_server_interval = 5;
int ongoing_client_interval = 1;

char *server_host_string = "SERVER|tcp://127.0.0.1:6714";
char *client_host_string = "SERVER|tcp://localhost:6714";
char *host_id = "30:Random_Id";
int host_id_len = 13;

mq_pipe_t control_efd[2];
int shutdown_everything = 0;

//***********************************************************************
// client_read_stream - Reads the incoming data stream
//***********************************************************************

gop_op_status_t client_read_stream(void *task_arg, int tid)
{
    gop_mq_task_t *task = (gop_mq_task_t *)task_arg;
    gop_mq_context_t *mqc = (gop_mq_context_t *)task->arg;
    gop_mq_stream_t *mqs;
    gop_op_status_t status;
    int err, nread, nleft, offset, nbytes;
    int client_delay;
    char *buffer;
    test_gop_t *op;

    op = gop_get_private(task->gop);

    log_printf(0, "START: gid=%d test_gop(f=%d, cd=%d sd=%d, mp=%d, sb=%d, to=%d) = %d\n", op->gid, op->launch_flusher, op->client_delay, op->delay, op->max_packet, op->send_bytes, op->timeout, op->shouldbe);

    client_delay = op->client_delay;
    tbx_type_malloc(buffer, char, test_size);

    status = gop_success_status;

    //** Parse the response
    gop_mq_remove_header(task->response, 1);

    mqs = gop_mq_stream_read_create(mqc, client_ongoing, host_id, host_id_len, gop_mq_msg_first(task->response), host, op->timeout);

    log_printf(0, "gid=%d msid=%d\n", op->gid, gop_mqs_id(mqs));

    nread = 0;
    nleft = op->send_bytes;
    while (nleft > 0) {
        offset=-1;
        err = gop_mq_stream_read(mqs, &offset, sizeof(int));
        if (err != 0) {
            log_printf(0, "ERROR reading offset!  nread=%d err=%d\n", nread, err);
            status = gop_failure_status;
            goto fail;
        }
        if (offset > test_size) {
            log_printf(0, "ERROR invalid offset=%d > %d! nread=%d\n", offset, test_size, nread);
            status = gop_failure_status;
            goto fail;
        }

        err = gop_mq_stream_read(mqs, &nbytes, sizeof(int));
        if (err != 0) {
            log_printf(0, "ERROR reading nbytes!  nread=%d err=%d\n", nread, err);
            status = gop_failure_status;
            goto fail;
        }
        err = offset + nbytes - 1;
        if ((err > test_size) && (err >= 0)) {
            log_printf(0, "ERROR invalid offset+nbytes offset=%d test=%d max is %d! nread=%d\n", offset, nbytes, test_size, nread);
            status = gop_failure_status;
            goto fail;
        }

        if ((nread+nbytes) > op->stop_reading_bytes) {
            log_printf(1, "Kicking out! gid=%d nread=%d stop_reading=%d n_buf=%d\n", op->gid, nread, op->stop_reading_bytes, nbytes);
            nbytes = op->stop_reading_bytes - nread;
            nleft = -1;
        }

        memset(buffer, 0, nbytes);
        err = gop_mq_stream_read(mqs, buffer, nbytes);
        if (err != 0) {
            log_printf(0, "ERROR reading data! nbytes=%d but got %d nread=%d\n", nbytes, err, nread);
            status = gop_failure_status;
            goto fail;
        }

        if (memcmp(buffer, &(test_data[offset]), nbytes) != 0) {
            log_printf(0, "ERROR data mismatch! offset=%d nbytes=%d nread=%d\n", offset, nbytes, nread);
            status = gop_failure_status;
            goto fail;
        }

        if (nleft < 0) goto fail;  //** Kick out
        nread += nbytes;
        nleft -= nbytes;

        if ((nleft > 0) && (client_delay > 0)) {
            log_printf(0, "gid=%d msid=%d sleep(%d)\n", op->gid, gop_mqs_id(mqs), client_delay);
            sleep(client_delay);
            log_printf(0, "gid=%d msid=%d Awake!\n", op->gid, gop_mqs_id(mqs));
            client_delay = 0;
        }

        log_printf(2, "nread=%d nleft=%d\n", nread, nleft);
    }

    if (nleft != -1) {  //** Attempt to do a read beyond the EOS is we didn't kickout early
        nbytes = 1;
        log_printf(1, "gid=%d msid=%d Before read after EOS\n", op->gid, gop_mqs_id(mqs));
        err = gop_mq_stream_read(mqs, buffer, nbytes);
        log_printf(1, "gid=%d msid=%d Attempt to read beyond EOS err=%d\n", op->gid, gop_mqs_id(mqs), err);
        if (err != -1) { // We want gop_mq_stream_read to return -1 here. It's an error otherwise
            log_printf(0, "ERROR Attempt to read after EOS succeeded! err=%d gid=%d msid=%d\n", err, op->gid, gop_mqs_id(mqs));
            status = gop_failure_status;
        }
    }

fail:
    err = gop_mqs_id(mqs);
    gop_mq_stream_destroy(mqs);
    free(buffer);

    log_printf(5, "END msid=%d status=%d %d\n", err, status.op_status, status.error_code);

    return(status);
}


//***************************************************************************
// client_make_context - Makes the MQ portal context
//***************************************************************************

gop_mq_context_t *client_make_context()
{
    char *text_params = "[mq_context]\n"
                        "  min_conn=1\n"
                        "  max_conn=1\n"
                        "  min_threads=2\n"
                        "  max_threads=%d\n"
                        "  backlog_trigger=1000\n"
                        "  heartbeat_dt=1\n"
                        "  heartbeat_failure=10\n"
                        "  min_ops_per_sec=100\n";

    char buffer[1024];
    tbx_inip_file_t *ifd;
    gop_mq_context_t *mqc;

    snprintf(buffer, sizeof(buffer), text_params, 100*nparallel);
    ifd = tbx_inip_string_read(buffer);
    mqc = gop_mq_create_context(ifd, "mq_context");
    assert(mqc != NULL);
    tbx_inip_destroy(ifd);

    return(mqc);
}

//***************************************************************************
// test_gop - Generates a MQS test GOP for execution
//***************************************************************************

gop_op_generic_t *test_gop(gop_mq_context_t *mqc, int flusher, int client_delay, int delay, int max_packet, int send_bytes, int stop_reading_bytes, int to, int myid)
{
    mq_msg_t *msg;
    gop_op_generic_t *gop;
    test_gop_t *op;

    log_printf(0, "START\n");

    //** Fill in the structure
    tbx_type_malloc_clear(op, test_gop_t, 1);
    op->launch_flusher = flusher;
    op->delay = delay;
    op->client_delay = client_delay;
    op->max_packet = max_packet;
    op->send_bytes = send_bytes;
    op->stop_reading_bytes = stop_reading_bytes;
    op->timeout = to;

    op->shouldbe = OP_STATE_SUCCESS;
    if (flusher == 0) {
        if (delay > to) op->shouldbe = OP_STATE_FAILURE;
    }

    //** Make the gop
    msg = gop_mq_make_exec_core_msg(host, 1);
    gop_mq_msg_append_mem(msg, MQS_TEST_KEY, MQS_TEST_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, host_id, host_id_len, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, op, sizeof(test_gop_t), MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
    gop = gop_mq_op_new(mqc, msg, client_read_stream, mqc, NULL, to);
    gop_set_private(gop, op);
    op->gid = gop_id(gop);
    gop_set_myid(gop, myid);

    log_printf(0, "CREATE: gid=%d myid=%d test_gop(f=%d, cd=%d, sd=%d, mp=%d, sb=%d, to=%d) = %d\n", gop_id(gop), myid, op->launch_flusher, op->client_delay, op->delay, op->max_packet, op->send_bytes, op->timeout, op->shouldbe);
    tbx_log_flush();

    return(gop);
}

//***************************************************************************
// new_bulk_task - Generates a new bulk task
//***************************************************************************

gop_op_generic_t *new_bulk_task(gop_mq_context_t *mqc, int myid)
{
    int transfer_bytes, packet_bytes, delay, client_delay, flusher, to;
    int to_min, to_max, dt, min_dt, stop_reading_bytes;

    min_dt = 3;
    to_min = 10;
    to_max = 20;

    transfer_bytes = tbx_random_get_int64(send_min, send_max);
    packet_bytes = tbx_random_get_int64(packet_min, packet_max);
    to = tbx_random_get_int64(to_min, to_max);
    delay = tbx_random_get_int64(1, 10);
    if (delay == 1) {
        delay = tbx_random_get_int64(to_min, to_max);
        if (delay == to) delay += 2;
        dt = delay-to;
        if (dt < 0) {
            if (dt > -min_dt) delay = to - min_dt;
        } else {
            if (dt < min_dt) delay = to + min_dt;
        }
    } else {
        delay = 0;
    }

    flusher = tbx_random_get_int64(1, 3);
    if (flusher != 1) {
        flusher = 0;
    }

    client_delay = tbx_random_get_int64(1, 10);
    if (client_delay == 1) {
        client_delay = tbx_random_get_int64(to_min, to_max);
        dt = client_delay - to;
        if (dt < 0) {
            if (dt > -min_dt) client_delay = to - min_dt;
        } else {
            if (dt < min_dt) client_delay = to + min_dt;
        }
    } else {
        client_delay = 0;
    }

    stop_reading_bytes = tbx_random_get_int64(1, 10);
    if (stop_reading_bytes <= 1) {
        stop_reading_bytes = tbx_random_get_int64(0, transfer_bytes);
        client_delay = 0;
        delay = 0;
    } else {
        stop_reading_bytes = transfer_bytes+1;
    }

    return(test_gop(mqc, flusher, client_delay, delay, packet_bytes, transfer_bytes, stop_reading_bytes, to, myid));
}

//***************************************************************************
// client_consume_result - Evaluates the result and frees the GOP
//   On success 0 is returned.  Otherwise 1 is returned indicating an error
//***************************************************************************

int client_consume_result(gop_op_generic_t *gop)
{
    int n;
    test_gop_t *op;
    gop_op_status_t status;

    op = gop_get_private(gop);
    status = gop_get_status(gop);

    if ((int)status.op_status != op->shouldbe) {
        n = 1;
        log_printf(0, "ERROR with stream test! gid=%d myid=%d test_gop(f=%d, cd=%d sd=%d, mp=%d, sb=%d, sr=%d, to=%d) = %d got=%d\n", gop_id(gop), gop_get_myid(gop), op->launch_flusher, op->client_delay, op->delay, op->max_packet, op->send_bytes, op->stop_reading_bytes, op->timeout, op->shouldbe, status.op_status);
    } else {
        n = 0;
        log_printf(0, "SUCCESS with stream test! gid=%d myid=%d test_gop(f=%d, cd=%d sd=%d, mp=%d, sb=%d, sr=%d, to=%d) = %d got=%d\n", gop_id(gop), gop_get_myid(gop), op->launch_flusher, op->client_delay, op->delay, op->max_packet, op->send_bytes, op->stop_reading_bytes, op->timeout, op->shouldbe, status.op_status);
    }
    tbx_log_flush();

    gop_free(gop, OP_DESTROY);
    free(op);

    return(n);
}

//***************************************************************************
// client_test_thread -  Client test thread
//***************************************************************************

void *client_test_thread(apr_thread_t *th, void *arg)
{
    int n, i;
    int single_max, single_send;
    gop_mq_context_t *mqc;
    gop_op_generic_t *gop;
    gop_opque_t *q = NULL;

    log_printf(0, "START\n");

    //** The rest of the tests all go through the mq_portal so we need to configure that now
    //** Make the portal
    mqc = client_make_context();

    //** Make the ongoing checker
    client_ongoing = gop_mq_ongoing_create(mqc, NULL, ongoing_client_interval, ONGOING_CLIENT);
    assert(client_ongoing != NULL);

    log_printf(0, "START basic stream tests\n");
    n = 0;
    single_max = 8192;
    single_send = 1024 * 1024;

    //** Kickout during a normal read
    gop = test_gop(mqc, 0, 0, 0, single_max, 10*single_send, 8*single_max, 10, 101);
    gop_waitall(gop);
    n += client_consume_result(gop);

//goto skip;

    //** Normal valid usage pattern
    gop = test_gop(mqc, 0, 0, 0, single_max, single_send, single_send+1, 10, 100);
    gop_waitall(gop);
    n += client_consume_result(gop);

//goto skip;

    //** Launch the flusher but no delay sending data
    gop = test_gop(mqc, 1, 0, 0, single_max, single_send, single_send+1, 10, 101);
    gop_waitall(gop);
    n += client_consume_result(gop);

    //** Response will come after the timeout
    gop = test_gop(mqc, 0, 0, 10, single_max, single_send, single_send+1, 5, 102);
    gop_waitall(gop);
    n += client_consume_result(gop);

    //** Launch the flusher but delay sending data forcing the heartbeat to handle it
    gop = test_gop(mqc, 1, 0, 15, single_max, single_send, single_send+1, 8, 103);
    gop_waitall(gop);
    n += client_consume_result(gop);

    //** Valid use pattern but the client pauses after reading
    gop = test_gop(mqc, 0, 30, 0, single_max, single_send, single_send+1, 5, 104);
    gop_waitall(gop);
    n += client_consume_result(gop);

    if (n != 0) {
        log_printf(0, "END:  ERROR with %d basic tests\n", n);
    } else {
        log_printf(0, "END:  SUCCESS! No problems with any basic stream test\n");
    }

//  goto skip;

    log_printf(0, "START bulk stream tests nparallel=%d\n", nparallel);
    n = 0;

    q = gop_opque_new();
    opque_start_execution(q);
    for (i=0; i<ntotal; i++) {
        gop = new_bulk_task(mqc, i);
        gop_opque_add(q, gop);
        if (i>=nparallel-1) {
            gop = opque_waitany(q);
            n += client_consume_result(gop);
        }
    }

    log_printf(0, "FINISHED job submission!\n");

    while ((gop = opque_waitany(q)) != NULL) {
        n += client_consume_result(gop);
    }

    if (n != 0) {
        log_printf(0, "END:  ERROR with %d bulk tests\n", n);
    } else {
        log_printf(0, "END:  SUCCESS! No problems with any bulk stream test\n");
    }

//skip:
    gop_mq_ongoing_destroy(client_ongoing);

    //** Destroy the portal
    gop_mq_destroy_context(mqc);

    if (q != NULL) gop_opque_free(q, OP_DESTROY);

    log_printf(0, "END\n");
    tbx_log_flush();

    return(NULL);
}

//***************************************************************************
// cb_write_stream - Handles the stream write response
//***************************************************************************

void cb_write_stream(void *arg, gop_mq_task_t *task)
{
    int nbytes, offset, nsent, nleft, err, kickout;
    gop_mq_frame_t *fid, *hid, *fop;
    mq_msg_t *msg;
    gop_mq_stream_t *mqs;
    test_gop_t *op;

    apr_thread_mutex_lock(lock);
    in_process++;
    apr_thread_mutex_unlock(lock);

    //** Parse the command.
    msg = task->msg;
    gop_mq_remove_header(msg, 0);

    fid = mq_msg_pop(msg);  //** This is the command ID
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame
    hid = mq_msg_pop(msg);  //** This is the Host ID for ongoing tracking

    fop = mq_msg_pop(msg);  //** Contains the op structure
    gop_mq_get_frame(fop, (void **)&op, &nbytes);
    assert(nbytes == sizeof(test_gop_t));

    log_printf(0, "START: gid=%d test_gop(f=%d, cd=%d, sd=%d, mp=%d, sb=%d, sr=%d, to=%d) = %d\n", op->gid, op->launch_flusher, op->client_delay, op->delay, op->max_packet, op->send_bytes, op->stop_reading_bytes, op->timeout, op->shouldbe);

    //** Create the stream so we can get the heartbeating while we work
    mqs = gop_mq_stream_write_create(task->ctx, server_portal, server_ongoing, do_compress, op->max_packet, op->timeout, msg, fid, hid, op->launch_flusher);

    log_printf(0, "gid=%d msid=%d\n", op->gid, gop_mqs_id(mqs));

    if (op->delay > 0) {
        log_printf(5, "Sleeping for %d sec gid=%d\n", op->delay, op->gid);
        sleep(op->delay);
        log_printf(5, "Woken up from sleep gid=%d\n", op->gid);
    }

    nleft = op->send_bytes;
    nsent = 0;
    kickout = 0;
    do {
        nbytes = tbx_random_get_int64(1, test_size);
        if (nbytes > nleft) nbytes = nleft;
        offset = tbx_random_get_int64(0, test_size-nbytes);

        log_printf(0, "nsent=%d  offset=%d nbytes=%d\n", nsent, offset, nbytes);
        err = gop_mq_stream_write(mqs, &offset, sizeof(int));
        if (err != 0) {
            kickout = 1;
            goto fail;
        }

        err = gop_mq_stream_write(mqs, &nbytes, sizeof(int));
        if (err != 0) {
            kickout = 2;
            goto fail;
        }

        err = gop_mq_stream_write(mqs, &(test_data[offset]), nbytes);
        if (err != 0) {
            kickout = 3;
            goto fail;
        }

        nsent += nbytes;
        nleft -= nbytes;
    } while (nleft > 0);

fail:
    if (kickout) {
        if (op->stop_reading_bytes >= op->send_bytes) {
            log_printf(0, "ERROR writing! nsent=%d gid=%d kickout=%d\n", nsent, op->gid, kickout);
        } else {
            log_printf(0, "ABORTED write nsent=%d stop_reading=%d gid=%d kickout=%d\n", nsent, op->stop_reading_bytes, op->gid, kickout);
        }
    }

    err = op->gid;
    gop_mq_frame_destroy(fop);
    gop_mq_stream_destroy(mqs);

    log_printf(0, "END gid=%d\n", err);
    tbx_log_flush();

    apr_thread_mutex_lock(lock);
    in_process--;
    apr_thread_cond_broadcast(cond);
    apr_thread_mutex_unlock(lock);

}

//***************************************************************************
// server_make_context - Makes the MQ portal context
//***************************************************************************

gop_mq_context_t *server_make_context()
{
    char *text_params = "[mq_context]\n"
                        "  min_conn=1\n"
                        "  max_conn=1\n"
                        "  min_threads=2\n"
                        "  max_threads=%d\n"
                        "  backlog_trigger=10000\n"
                        "  heartbeat_dt=1\n"
                        "  heartbeat_failure=5\n"
                        "  min_ops_per_sec=100\n";
    char buffer[1024];
    tbx_inip_file_t *ifd;
    gop_mq_context_t *mqc;

    snprintf(buffer, sizeof(buffer), text_params, 100*nparallel);
    ifd = tbx_inip_string_read(buffer);
    mqc = gop_mq_create_context(ifd, "mq_context");
    assert(mqc != NULL);
    tbx_inip_destroy(ifd);

    return(mqc);
}

//***************************************************************************
// server_test_mq_loop
//***************************************************************************

void *server_test_thread(apr_thread_t *th, void *arg)
{
    gop_mq_context_t *mqc;
    gop_mq_command_table_t *table;
    char c;

    log_printf(0, "START\n");

    //** Make the server portal
    mqc = server_make_context();

    //** Make the server portal
    server_portal = gop_mq_portal_create(mqc, server_host_string, MQ_CMODE_SERVER);

    //** Make the ongoing checker
    server_ongoing = gop_mq_ongoing_create(mqc, server_portal, ongoing_server_interval, ONGOING_SERVER);
    assert(server_ongoing != NULL);

    //** Install the commands
    table = gop_mq_portal_command_table(server_portal);
    gop_mq_command_set(table, MQS_TEST_KEY, MQS_TEST_SIZE, mqc, cb_write_stream);
    gop_mq_command_set(table, MQS_MORE_DATA_KEY, MQS_MORE_DATA_SIZE, server_ongoing, gop_mqs_server_more_cb);
//ADded in ongoing_create  gop_mq_command_set(table, ONGOING_KEY, ONGOING_SIZE, server_ongoing, mq_ongoing_cb);

    gop_mq_portal_install(mqc, server_portal);

    //** Wait for a shutdown
    gop_mq_pipe_read(control_efd[0], &c);

    apr_thread_mutex_lock(lock);
    while (in_process != 0) {
        apr_thread_cond_wait(cond, lock);
    }
    apr_thread_mutex_unlock(lock);

    gop_mq_ongoing_destroy(server_ongoing);

    //** Destroy the portal
    gop_mq_destroy_context(mqc);

    log_printf(0, "END\n");
    tbx_log_flush();

    return(NULL);
}


//***************************************************************************
//***************************************************************************
//***************************************************************************

int main(int argc, char **argv)
{
    apr_pool_t *mpool;
    apr_thread_t *server_thread, *client_thread;
    apr_status_t dummy;
    int i, start_option, do_random, ll;
    int64_t lsize = 0;
    char buf1[256], buf2[256], c;
    char *logfile = NULL;
    char *host_string_converted;
    gop_mq_socket_context_t *ctx;

    ll = 0;

    if (argc < 2) {
        printf("mqs_test [-d log_level] [-log log_file] [-log_size size] [-t min max] [-p min max] [-np nparalle] [-nt ntotal] [-z] [-0] \n");
        printf("\n");
        printf("-d log_level\n");
        printf("-log log_file  Log file for storing output.  Defaults to stdout\n");
        printf("-log_size size Log file size.  Can use unit abbreviations.\n");
        printf("-t min max     Range of total bytes to transfer for bulk tests. Defaults is %s to %s\n", tbx_stk_pretty_print_int_with_scale(send_min, buf1), tbx_stk_pretty_print_int_with_scale(send_max, buf2));
        printf("-p min max     Range of max stream packet sizes for bulk tests. Defaults is %s to %s\n", tbx_stk_pretty_print_int_with_scale(packet_min, buf1), tbx_stk_pretty_print_int_with_scale(packet_max, buf2));
        printf("-np nparallel  Number of parallel streams to execute.  Default is %d\n", nparallel);
        printf("-nt ntotal     Total number of bulk operations to perform.  Default is %d\n", ntotal);
        printf("-z             Enable data compression\n");
        printf("-0             Use test data filled with zeros.  Defaults to using random data.\n");
        printf("\n");
        return(0);
    }

    i = 1;
    do_random = 1;
    do {
        start_option = i;

        if (strcmp(argv[i], "-d") == 0) { //** Enable debugging
            i++;
            ll = atol(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-h") == 0) { //** Print help
            printf("mq_test [-d log_level]\n");
            return(0);
        } else if (strcmp(argv[i], "-t") == 0) { //** Change number of total bytes transferred
            i++;
            send_min = tbx_stk_string_get_integer(argv[i]);
            i++;
            send_max = tbx_stk_string_get_integer(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-p") == 0) { //** Max number of bytes to transfer / packet
            i++;
            packet_min = tbx_stk_string_get_integer(argv[i]);
            i++;
            packet_max = tbx_stk_string_get_integer(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-np") == 0) { //** Parallel transfers
            i++;
            nparallel = tbx_stk_string_get_integer(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-nt") == 0) { //** Total number of transfers
            i++;
            ntotal = tbx_stk_string_get_integer(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-log") == 0) { //** Log file
            i++;
            logfile = argv[i];
            i++;
        } else if (strcmp(argv[i], "-log_size") == 0) { //** Log file size
            i++;
            lsize = tbx_stk_string_get_integer(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-z") == 0) { //** Enable compression
            i++;
            do_compress = MQS_PACK_COMPRESS;
        } else if (strcmp(argv[i], "-0") == 0) { //** Used 0 filled data
            i++;
            do_random = 0;
        }
    } while ((start_option < i) && (i<argc));

    printf("log_level=%d\n", _log_level);

    printf("Settings packet=(%d,%d) send=(%d,%d) np=%d nt=%d\n", packet_min, packet_max, send_min, send_max, nparallel, ntotal);

    gop_init_opque_system();
    tbx_random_startup();

    if (logfile != NULL) tbx_log_open(logfile, 0);
    if (lsize != 0) tbx_set_log_maxsize(lsize);

    tbx_set_log_level(ll);

    //** Make the test_data to pluck info from
    tbx_type_malloc_clear(test_data, char, test_size);
    if (do_random == 1) tbx_random_get_bytes(test_data, test_size);

    //** Make the locking structures for client/server communication
    apr_pool_create(&mpool, NULL);
    assert_result(apr_thread_mutex_create(&lock, APR_THREAD_MUTEX_DEFAULT, mpool), APR_SUCCESS);
    assert_result(apr_thread_cond_create(&cond, mpool), APR_SUCCESS);

    host_string_converted = strdup(client_host_string);
    host = gop_mq_string_to_address(host_string_converted);

    //** Make the pipe for controlling the server
    ctx = gop_mq_socket_context_new();
    gop_mq_pipe_create(ctx, control_efd);

    tbx_thread_create_assert(&server_thread, NULL, server_test_thread, NULL, mpool);
    sleep(5); //** Make surethe server gets fired up
    tbx_thread_create_assert(&client_thread, NULL, client_test_thread, NULL, mpool);

    apr_thread_join(&dummy, client_thread);

    //** Trigger the server to shutdown
    c = 1;
    gop_mq_pipe_write(control_efd[1], &c);
    apr_thread_join(&dummy, server_thread);

    gop_mq_pipe_destroy(ctx, control_efd);
    gop_mq_socket_context_destroy(ctx);

    apr_thread_mutex_destroy(lock);
    apr_thread_cond_destroy(cond);

    apr_pool_destroy(mpool);

    gop_shutdown();

    free(test_data);
    return(0);
}

