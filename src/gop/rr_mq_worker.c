/*****************************
 * Round robin worker
 *****************************/

#include "mq_portal.h"
#include "mq_stream.h"
#include "mq_ongoing.h"
#include "mq_roundrobin.h"
#include "mqs_roundrobin.h"
#include "random.h"
#include "apr_wrapper.h"
#include "log.h"
#include "type_malloc.h"
#include <sys/eventfd.h>

#define NUM_WORKERS 9

typedef struct {
    apr_thread_t *thread;
    mq_context_t *mqc;
    mq_portal_t *portal;
    mq_command_table_t *table;
    int id;

} bulk_worker_t;

mq_portal_t *parent_portal = NULL;
mq_command_table_t *parent_table = NULL;
mq_ongoing_t *parent_ongoing = NULL;

char *server_string = "tcp://127.0.0.1:6714";
mq_msg_t *server = NULL;

bulk_worker_t *bulk_workers = NULL;

int complete = 0;
char *user_command = NULL;

int bulk_control = 0;

int running = 0;

mq_command_table_t *table;
mq_portal_t *worker_portal;

mq_msg_t *pack_increment_msg()
{
    mq_msg_t *msg = mq_make_exec_core_msg(server, 0);
    mq_msg_append_mem(msg, MQF_INCREMENT_KEY, MQF_INCREMENT_SIZE, MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
    return msg;
}

// build_send_increment()
// Sends an increment message to the server. This is done after processing a message
void build_send_increment(mq_portal_t *portal)
{
    mq_task_t *task;
    mq_msg_t *msg;

    msg = pack_increment_msg();
    task = mq_task_new(portal->mqc, msg, NULL, NULL, 5);

    log_printf(1, "WORKER: Sending INCREMENT message...\n");
    int status = mq_submit(portal, task);

    if(status != 0)
        log_printf(0, "WORKER: Failed sending INCREMENT request to server at %s - error = %d\n", server, status);
    else
        log_printf(10, "WORKER: Successfully sent INCREMENT request to server!\n");

    task = NULL;
}

// process_trackexec_ping()
// Respond to a PING
void process_trackexec_ping(mq_portal_t *p, mq_socket_t *sock, mq_msg_t *msg)
{
    mq_frame_t *id_frame;
    char *data, b64[1024];
    int err;

    log_printf(15, "WORKER: Building response message...\n");

    mq_remove_header(msg, 0);
    id_frame = mq_msg_pluck(msg, 0); // random uint64_t
    mq_frame_destroy(mq_msg_pluck(msg, 0)); // ping
    //leave the null frame, mq_apply_return_address_msg() will put it on top of response
    //There should be two addresses left in the message

    mq_get_frame(id_frame, (void **)&data, &err);
    log_printf(1, "WORKER: PING id = %s\n", mq_id2str(data, err, b64, sizeof(b64)));

    mq_msg_t *response = mq_make_response_core_msg(msg, id_frame);
    mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);

    log_printf(10, "WORKER: Response message frames:\n");
    display_msg_frames(response);

    mq_task_t *pass_task = mq_task_new(p->mqc, response, NULL, NULL, 5);
    pass_task->pass_through = 1;
    err = mq_submit(p, pass_task);

    if(err != 0) {
        log_printf(0, "WORKER: Failed to send response\n");
    } else {
        log_printf(10, "WORKER: Successfully sent response!\n");
    }
    mq_msg_destroy(msg);
    pass_task = NULL;
}

// process_exec_ping()
// Receive a PING
void process_exec_ping(mq_portal_t *p, mq_socket_t *sock, mq_msg_t *msg)
{
    mq_frame_t *pid, *addr;
    char *data, b64[1024];
    int err;

    log_printf(10, "WORKER: Frames before destroying:\n");
    display_msg_frames(msg);

    mq_remove_header(msg, 0);
    pid = mq_msg_pluck(msg, 0); //Message ID frame

    mq_frame_destroy(mq_msg_pluck(msg, 0)); //ping
    mq_frame_destroy(mq_msg_pluck(msg, 0)); //null
    addr = mq_msg_pluck(msg, 0); //client address

    mq_get_frame(pid, (void **)&data, &err);
    log_printf(5, "WORKER: PING ID %s\n", mq_id2str(data, err, b64, sizeof(b64)));

    log_printf(15, "WORKER: Destroying message...\n");
    mq_msg_destroy(msg);
    mq_frame_destroy(pid);
    mq_frame_destroy(addr);
}

// ping()
// Processes the PING according to its exec frame and sends an INCREMENT to the server
void ping(void *arg, mq_task_t *task)
{
    log_printf(10, "WORKER: Executing PING response...\n");
    mq_msg_t *msg = task->msg;
    mq_portal_t *p = (mq_portal_t *)arg;
    mq_frame_t *f;
    char *data;
    int size;

    for(f = mq_msg_first(msg); f != NULL; f = mq_msg_next(msg)) {
        mq_get_frame(f, (void **)&data, &size);
        if( mq_data_compare(data, size, MQF_EXEC_KEY, MQF_EXEC_SIZE) == 0 ) {
            log_printf(5, "WORKER: PING request is EXEC\n");
            process_exec_ping(p, NULL, msg);
            break;
        }
        if( mq_data_compare(data, size, MQF_TRACKEXEC_KEY, MQF_TRACKEXEC_SIZE) == 0 ) {
            log_printf(5, "WORKER: PING request is TRACKEXEC\n");
            process_trackexec_ping(p, NULL, msg);
            break;
        }
    }

    log_printf(15, "SERVER: Completed PING response.\n");
    task->msg = NULL;
    build_send_increment(p);
}

// process_stream()
// Sends data back to the client
void process_stream(mq_portal_t *p, mq_task_t *task, mq_msg_t *msg)
{

    log_printf(10, "WORKER: Received an RR_STREAM request\n");

    mq_frame_t *f, *h_id, *m_id;
    mq_stream_t *mqs;
    char *data, *id;
    int err, size_to_send;

    log_printf(10, "WORKER: Message frames BEFORE destroying:\n");
    display_msg_frames(msg);

    log_printf(15, "WORKER: Destroying frames...\n");
    mq_remove_header(msg, 0);
    m_id = mq_msg_pop(msg); //message ID, auto-generated
    mq_frame_destroy(mq_msg_pop(msg)); //rr_stream
    h_id = mq_msg_pop(msg); //host id
    f = mq_msg_pop(msg); //data size

    //remaining frames are addresses

    mq_get_frame(f, (void **)&data, &err);
    size_to_send = atol(data);
    if(size_to_send != TEST_SIZE) {
        log_printf(0, "WORKER: ERROR - Client requested %d bytes, supposed to request %d\n", size_to_send, TEST_SIZE);
        return;
    }
    log_printf(10, "WORKER: Client requested %d bytes\n", size_to_send);
    mq_frame_destroy(f);

    mq_get_frame(h_id, (void **)&id, &err);
    char *clients_id = malloc(err + 1);
    strncpy(clients_id, id, err);
    clients_id[err] = '\0';
    log_printf(5, "WORKER: Host ID %s size = %d\n", clients_id, err);

    mqs = mq_stream_write_create(task->ctx, p, parent_ongoing, MQS_PACK_RAW, 8192, 5, msg, m_id, h_id, 3); //these values: sure, whatever

    int n_left = size_to_send;
    int n_sent = 0;
    int offset = 0;
    int n_bytes = 0;
    char *test_data = malloc(size_to_send);
    memset(test_data, TEST_DATA, size_to_send);

    while(n_left > 0) {
        n_bytes = random_int(1, TEST_SIZE);
        if(n_bytes > n_left)
            n_bytes = n_left;
        offset = random_int(0, size_to_send - n_bytes);

        log_printf(5, "WORKER: n_sent = %d  offset = %d n_bytes = %d\n", n_sent, offset, n_bytes);
        err = mq_stream_write(mqs, &offset, sizeof(int));
        if(err != 0) {
            log_printf(0, "WORKER: ERROR writing offset!  n_sent=%d\n", n_sent);
            break;
        }

        err = mq_stream_write(mqs, &n_bytes, sizeof(int));
        if (err != 0) {
            log_printf(0, "WORKER: ERROR writing n_bytes!  n_sent=%d\n", n_sent);
            break;
        }

        err = mq_stream_write(mqs, &(test_data[offset]), n_bytes);
        if (err != 0) {
            log_printf(0, "WORKER: ERROR writing test_data!  n_sent=%d\n", n_sent);
            break;
        }

        n_sent += n_bytes;
        n_left -= n_bytes;
    }


    if(err != 0)
        log_printf(0, "WORKER: ERROR - Did not send %d bytes successfully, err = %d\n", size_to_send, err);
    else
        log_printf(10, "WORKER: Successfully sent %d bytes to client\n", size_to_send);

    mq_stream_destroy(mqs);
    mq_msg_destroy(msg);
    free(clients_id);
    free(test_data);
}

void stream(void *arg, mq_task_t *task)
{
    mq_portal_t *p = (mq_portal_t *)arg;

    if(task->msg == NULL) {
        log_printf(0, "WORKER: NULL msg\n");
        return;
    }

    log_printf(15, "WORKER: Executing RR_STREAM response...\n");
    process_stream(p, task, task->msg);
    log_printf(15, "WORKER: Completed RR_STREAM response.\n");
    task->msg = NULL;
    build_send_increment(p);
}

mq_msg_t *pack_register_msg()
{
    mq_msg_t *msg;
    //int free_slots = 10;
    char *free_slots = "10";

    /*
     * Not sending the bound address anymore
     * The worker will use the outgoing (client) connection/portal that it automatically
     * creates when sending this message. That portal allows two-way communication
     *
     * The server will just save the address that it gets in the message. The worker
     * only needs to send its number of free slots
     */

    msg = mq_make_exec_core_msg(server, 1);
    mq_msg_append_mem(msg, MQF_REGISTER_KEY, MQF_REGISTER_SIZE, MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, free_slots, strlen(free_slots), MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    return msg;
}

mq_msg_t *pack_deregister_msg()
{
    mq_msg_t *msg = mq_msg_new();

    //not sending address anymore

    mq_msg_append_msg(msg, server, MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, MQF_EXEC_KEY, MQF_EXEC_SIZE, MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, "WORKER", 6, MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, MQF_DEREGISTER_KEY, MQF_DEREGISTER_SIZE, MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    return msg;
}

op_status_t confirm_register(void *arg, int n)
{
    op_status_t status;
    mq_task_t *task;
    mq_msg_t *msg;

    status = op_success_status;
    task = (mq_task_t *)arg;
    msg = task->msg;

    log_printf(10, "WORKER: Received response to REGISTER request...\n");
    log_printf(10, "WORKER: Message received:\n");
    display_msg_frames(msg);

    return status;
}

void build_send_register(mq_context_t *mqc, mq_portal_t *wp, mq_command_table_t *tbl)
{
    mq_msg_t *msg;
    op_generic_t *gop;

    log_printf(15, "WORKER: Building message...\n");
    msg = pack_register_msg();

    log_printf(15, "WORKER: Creating new generic operation...\n");
    gop = new_mq_op(mqc, msg, confirm_register, NULL, NULL, 5);

    log_printf(15, "WORKER: Sending single REGISTER message...\n");
    int status = gop_waitall(gop);

    gop_free(gop, OP_DESTROY);

    if(status != OP_STATE_SUCCESS) {
        log_printf(0, "WORKER: Failed sending REGISTER request to server at %s - error = %d\n", server, status);
    } else {
        log_printf(15, "WORKER: Successfully sent REGISTER request to server!\n");

        log_printf(15, "WORKER: Grabbing new portal...\n");
        wp = mq_portal_lookup(mqc, server_string, MQ_CMODE_CLIENT); //this should be the portal that was just created
        assert(wp != NULL);
        log_printf(15, "WORKER: Grabbing new table...\n");
        tbl = mq_portal_command_table(wp);
        log_printf(5, "WORKER: Installing commands...\n");
        mq_command_set(tbl, MQF_PING_KEY, MQF_PING_SIZE, wp, ping);
        mq_command_set(tbl, MQF_RR_STREAM_KEY, MQF_RR_STREAM_SIZE, wp, stream);

        //Starting this here since the worker_portal is now created here
        if(parent_ongoing == NULL)
            parent_ongoing = mq_ongoing_create(mqc, wp, 5, ONGOING_SERVER);

        log_printf(5, "WORKER: Registration completed.\n");
    }
}

void build_send_deregister(mq_context_t *mqc, mq_portal_t *wp)
{
    mq_msg_t *msg;
    op_generic_t *gop;
    int status;

    log_printf(15, "WORKER: Building message...\n");
    msg = pack_deregister_msg();

    gop = new_mq_op(mqc, msg, NULL, NULL, NULL, 5);

    status = gop_waitall(gop);
    gop_free(gop, OP_DESTROY);

    if(status != OP_STATE_SUCCESS)
        log_printf(0, "WORKER: Failed sending DEREGISTER request to server at %s - error = %d\n", server, status);
    else
        log_printf(15, "WORKER: Successfully sent DEREGISTER request to server!\n");
}

mq_context_t *worker_make_context()
{
    mq_context_t *mqc;
    inip_file_t *ifd;
    char *text_parameters = "[mq_context]\n"
                            "min_conn = 1\n"
                            "max_conn = 4\n"
                            "min_threads = 2\n"
                            "max_threads = 10\n"
                            "backlog_trigger = 1000\n"
                            "heartbeat_dt = 120\n"
                            "heartbeat_failure = 10000\n"
                            "min_ops_per_sec = 100\n";
    // omitting socket_type for workers

    ifd = inip_read_text(text_parameters);

    mqc = mq_create_context(ifd, "mq_context");
    inip_destroy(ifd);

    return mqc;
}

// start_bulk_worker_test_thread()
// Many of these threads get created for the bulk test
// Each thread acts like an independent worker, but you can't interact with them
// on command line like the parent worker
void *start_bulk_worker_test_thread(apr_thread_t *th, void *arg)
{
    int id = *((int *)arg);
    printf("WORKER #%05d REPORTING IN\n", id);
    bulk_workers[id].mqc = worker_make_context();

    build_send_register(bulk_workers[id].mqc, bulk_workers[id].portal, bulk_workers[id].table);

    while(bulk_control != 1);

    build_send_deregister(bulk_workers[id].mqc, bulk_workers[id].portal);
    mq_destroy_context(bulk_workers[id].mqc);

    return NULL;
}

// start_bulk_worker_test()
// Start the bulk worker threads
void start_bulk_worker_test(mq_context_t *mqc, int n)
{

    bulk_workers = malloc(n * sizeof(bulk_worker_t));

    log_printf(1, "WORKER: Creating %d worker threads for bulk test...\n", n);

    int i;
    for(i = 0; i < n; i++) {
        bulk_workers[i].id = i;
        thread_create_assert(&bulk_workers[i].thread, NULL, start_bulk_worker_test_thread, &(bulk_workers[i].id), mqc->mpool);
    }
    log_printf(1, "WORKER: Minions created!\n", n);
}

// end_bulk_worker_test()
// Shut down all the bulk worker_threads
void end_bulk_worker_test(mq_context_t *mqc, int n)
{
    apr_status_t dummy;
    bulk_control = 1;

    log_printf(1, "WORKER: Shutting down all bulk worker threads...\n");
    int i;
    for(i = 0; i < n; i++) {
        apr_thread_join(&dummy, bulk_workers[i].thread);
    }

    free(bulk_workers);
    log_printf(1, "WORKER: Done with bulk test.\n");
}

void worker_test()
{
    mq_context_t *mqc;
    //mq_command_table_t *table;
    //mq_portal_t *worker_portal;
    flush_log();
    log_printf(1, "WORKER: Starting...\n");

    log_printf(10, "WORKER: Creating context...\n");
    mqc = worker_make_context();
    parent_portal = malloc(sizeof(mq_portal_t*));

    log_printf(1, "WORKER: Up and running!\n");
    type_malloc(user_command, char, 20);
    while(!complete) {
        printf("> ");
        scanf("%s", user_command);
        if(strcmp(user_command, "quit") == 0)
            complete = 1;
        else if(strcmp(user_command, "register") == 0) { //Parent worker register
            build_send_register(mqc, parent_portal, parent_table); // Parent portal, table, and ongoing get filled in here
            assert(parent_portal != NULL);
        } else if(strcmp(user_command, "deregister") == 0) { //Parent worker deregister
            build_send_deregister(mqc, parent_portal);
        } else if(strcmp(user_command, "bulk") == 0) { //Child workers register
            if(parent_ongoing == NULL) {
                printf("Parent worker needs to be registered first.\n");
                continue;
            }
            start_bulk_worker_test(mqc, NUM_WORKERS); //Children only use the global mqc for its memory pool
        } else if(strcmp(user_command, "endbulk") == 0) { //Child workers deregister
            end_bulk_worker_test(mqc, NUM_WORKERS);
        }
    }

    log_printf(1, "WORKER: Shutting down...\n");
    if(parent_ongoing != NULL)
        mq_ongoing_destroy(parent_ongoing);
    mq_destroy_context(mqc);
    free(user_command);
    free(parent_portal);

    log_printf(0, "WORKER: Finished.\n");
}

void *worker_test_thread(apr_thread_t *th, void *arg)
{
    worker_test();
    return NULL;
}

/*
 * main
 */
int main(int argc, char **argv)
{

    if (argc > 2) {
        if(strcmp(argv[1], "-d") == 0)
            set_log_level(atol(argv[2]));
        else {
            printf("%s -d <log_level>\n", argv[0]);
            return 1;
        }
    } else {
        printf("%s -d <log_level>\n", argv[0]);
        return 1;
    }

    srand(time(NULL));

    // Thread pool, threads, and control fd
    apr_pool_t *mpool;
    apr_thread_t *worker_thread;
    apr_status_t dummy;

    // Start the background systems
    apr_wrapper_start();
    init_opque_system();

    apr_pool_create(&mpool, NULL);

    server = mq_string_to_address(server_string);

    // Create threads for server
    thread_create_assert(&worker_thread, NULL, worker_test_thread, NULL, mpool);

    apr_thread_join(&dummy, worker_thread);

    // Clean up
    apr_pool_destroy(mpool);

    destroy_opque_system();
    apr_wrapper_stop();

    return 0;
}
