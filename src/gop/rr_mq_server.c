/*****************************
 * Round robin server
 *****************************/

#include "mq_portal.h"
#include "mq_roundrobin.h"
#include "mqs_roundrobin.h"
#include "apr_wrapper.h"
#include "log.h"
#include "type_malloc.h"
#include <sys/eventfd.h>

char *host_string = "tcp://127.0.0.1:6714";
mq_msg_t *host = NULL;

int complete = 0;
char *user_command = NULL;

mq_processing_queue *processing_queue = NULL;
mq_portal_t *server_portal = NULL;
apr_thread_mutex_t *table_lock = NULL;
apr_thread_mutex_t *queue_lock = NULL;

// process_round_robin_pass()
// Passes message to a worker using round robin (least recently used worker) scheme
// Messages already in the processing queue take priority over the current message
void process_round_robin_pass(mq_portal_t *p, mq_socket_t *sock, mq_msg_t *msg)
{
    mq_worker_table_t *worker_table;

    if(p->implementation_arg == NULL) {
        apr_thread_mutex_lock(queue_lock);
        processing_queue_add(processing_queue, msg);
        apr_thread_mutex_unlock(queue_lock);
        log_printf(0, "SERVER: ERROR - No worker table found. Message added to queue. Current queue length = %d\n", processing_queue_length(processing_queue));
        return;
    }

    worker_table = (mq_worker_table_t *)p->implementation_arg;

    apr_thread_mutex_lock(queue_lock);
    if(mq_worker_table_length(worker_table) == 0) {
        processing_queue_add(processing_queue, msg);
        apr_thread_mutex_unlock(queue_lock);
        log_printf(0, "SERVER: ERROR - No workers registered. Message added to queue. Current queue length = %d\n", processing_queue_length(processing_queue));
        return;
    }
    apr_thread_mutex_unlock(queue_lock);

    int n_messages;

    if((n_messages = processing_queue_length(processing_queue)) > 0) {
        mq_msg_t *m;

        apr_thread_mutex_lock(queue_lock);
        processing_queue_add(processing_queue, msg);

        while((m = processing_queue_get(processing_queue)) != NULL) {
            apr_thread_mutex_lock(table_lock);
            int err = mq_send_message(m, worker_table, p);
            apr_thread_mutex_unlock(table_lock);

            if(err == -1) {
                // All workers are busy. Push this message back on top and exit
                processing_queue_push(processing_queue, m);
                apr_thread_mutex_unlock(queue_lock);
                return;
            }
        }
        apr_thread_mutex_unlock(queue_lock);
    }

    apr_thread_mutex_lock(table_lock);
    int err = mq_send_message(msg, worker_table, p);
    apr_thread_mutex_unlock(table_lock);

    if(err == -1) {
        // All workers are busy. Add this message and exit
        apr_thread_mutex_lock(queue_lock);
        processing_queue_add(processing_queue, msg);
        apr_thread_mutex_unlock(queue_lock);
    }

}

// process_register_worker()
// Adds a worker to the worker table and sends a response back to notify
void process_register_worker(mq_portal_t *p, mq_socket_t *sock, mq_msg_t *msg)
{
    int worker_free_slots;
    char *worker_address;
    mq_msg_t *response;
    mq_frame_t *id;
    mq_frame_t *f;
    char *data;
    int err;

    log_printf(10, "SERVER: Contents of REGISTER message:\n");
    display_msg_frames(msg);

    mq_remove_header(msg, 0);
    id = mq_msg_pluck(msg, 0); // ID frame
    mq_frame_destroy(mq_msg_pluck(msg, 0)); // argument frame (REGISTER)

    // Get the number of free slots
    f = mq_msg_current(msg);
    mq_get_frame(f, (void **)&data, &err);
    worker_free_slots = atol(data);
    mq_frame_destroy(mq_msg_pluck(msg, 0)); //slots frame

    // message has a blank frame and the worker's address remaining
    // Address frame at the bottom is the auto-generated client portal host

    f = mq_msg_last(msg);
    mq_get_frame(f, (void **)&data, &err);
    type_malloc(worker_address, char, err + 1);
    memcpy(worker_address, data, err);
    worker_address[err] = '\0';

    log_printf(4, "SERVER: Received REGISTER from address %s\n", worker_address);
    log_printf(4, "SERVER: Worker at %s has free_slots = %d, size = %d\n", worker_address, worker_free_slots, err);

    // Add this worker to the table
    apr_thread_mutex_lock(table_lock);
    mq_register_worker(p->implementation_arg, worker_address, worker_free_slots);
    apr_thread_mutex_unlock(table_lock);

    log_printf(10, "SERVER: Sending REGISTER confirmation to worker...\n");

    // Build the response using the ID grabbed earlier
    response = mq_make_response_core_msg(msg, id);
    mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);

    log_printf(10, "SERVER: Response message frames:\n");
    display_msg_frames(response);

    mq_task_t *pass_task = mq_task_new(p->mqc, response, NULL, NULL, 5);
    // No need to set this flag here, since this is an actual response to a TRACKEXEC message
    //pass_task->pass_through = 1;
    err = mq_submit(p, pass_task);
    if(err != 0) {
        log_printf(0, "SERVER: Failed to send response, err = %d\n", err);
    } else {
        log_printf(10, "SERVER: Successfully sent response!\n");
    }

    mq_msg_destroy(msg);
}

// process_deregister_worker()
// Removes a worker from the worker table
void process_deregister_worker(mq_portal_t *p, mq_socket_t *sock, mq_msg_t *msg)
{
    mq_worker_table_t *table;
    char *worker_address;
    mq_frame_t *f;
    char *data;
    int err;

    // Grab the worker's address
    f = mq_msg_last(msg);
    mq_get_frame(f, (void **)&data, &err);
    printf("err = %d\n", err);
    type_malloc(worker_address, char, err + 1);
    memcpy(worker_address, data, err);
    worker_address[err] = '\0';
    log_printf(10, "SERVER: Attempting to delete worker with address %s\n", worker_address);

    table = (mq_worker_table_t *) p->implementation_arg;

    apr_thread_mutex_lock(table_lock);
    mq_deregister_worker(table, worker_address);
    apr_thread_mutex_unlock(table_lock);

    log_printf(5, "SERVER: Deleted worker with address %s\n", worker_address);
    free(worker_address);
    mq_msg_destroy(msg);
}

void process_increment_worker(mq_portal_t *p, mq_socket_t *sock, mq_msg_t *msg)
{
    mq_worker_table_t *table;
    mq_worker_t *worker;
    char *worker_address;
    mq_frame_t *f;
    char *data;
    int err;

    // Grab the worker's address
    f = mq_msg_last(msg);
    mq_get_frame(f, (void **)&data, &err);
    printf("err = %d\n", err);
    type_malloc(worker_address, char, err + 1);
    memcpy(worker_address, data, err);
    worker_address[err] = '\0';
    log_printf(10, "SERVER: Attempting to increment worker with address %s\n", worker_address);

    apr_thread_mutex_lock(table_lock);
    table = (mq_worker_table_t *) p->implementation_arg;
    worker = mq_get_worker(table, worker_address);
    if(worker != NULL) {
        worker->free_slots++;
        mq_worker_table_add(table, worker);
        log_printf(15, "SERVER: Worker with address %s now has %d free slots\n", worker_address, worker->free_slots);
    } else {
        log_printf(0, "SERVER: ERROR - Could not find worker with address %s\n", worker_address);
    }
    apr_thread_mutex_unlock(table_lock);

    free(worker_address);
    mq_msg_destroy(msg);
}

// pass_through()
// Either passes a message on to a worker via process_round_robin_pass,
// or ignores it entirely and passes it to the next address in the message
void pass_through(void *arg, mq_task_t *task)
{
    mq_portal_t *p;
    mq_frame_t *f;
    mq_msg_t *msg;
    char *data;
    int size;

    p = (mq_portal_t *)arg;
    msg = task->msg;

    log_printf(10, "SERVER: Beginning pass_through(). Message received:\n");
    display_msg_frames(msg);

    f = mq_msg_first(msg);
    mq_get_frame(f, (void **)&data, &size);

    if(size == 0) {
        // Top frame is a null frame - message terminated here and should be passed to a worker
        log_printf(10, "SERVER: Deferring message to worker...\n");
        process_round_robin_pass(p, NULL, msg);
    } else {
        // Top frame is an address - ignore and pass on
        log_printf(10, "SERVER: Passing message to %s\n", data);
        mq_task_t *pass_task = mq_task_new(p->mqc, msg, NULL, NULL, 5);
        //Set this flag so the task isn't added to the heartbeat table
        pass_task->pass_through = 1;
        int err = mq_submit(p, pass_task);

        if(err != 0) {
            log_printf(0, "SERVER: Failed to pass message, err = %d\n", err);
        } else {
            log_printf(10, "SERVER: Successfully passed message!\n");
        }
    }

    // So that msg doesn't disappear when this task is destroyed
    task->msg = NULL;

    log_printf(10, "SERVER: End of pass_through()\n");
}

void register_worker(void *arg, mq_task_t *task)
{
    log_printf(15, "SERVER: Received REGISTER command!\n");
    process_register_worker((mq_portal_t *)arg, NULL, task->msg);
    log_printf(15, "SERVER: Completed REGISTER command!\n");
    task->msg = NULL;
}

void deregister_worker(void *arg, mq_task_t *task)
{
    log_printf(15, "SERVER: Received DEREGISTER command!\n");
    process_deregister_worker((mq_portal_t *)arg, NULL, task->msg);
    log_printf(15, "SERVER: Completed DEREGISTER command!\n");
    task->msg = NULL;
}

void increment_worker(void *arg, mq_task_t *task)
{
    log_printf(15, "SERVER: Received INCREMENT command!\n");
    process_increment_worker((mq_portal_t *)arg, NULL, task->msg);
    log_printf(15, "SERVER: Completed INCREMENT command!\n");
    task->msg = NULL;
}

// queue_checker()
// Thread running in background periodically checking the processing queue for
// backlogged messages
void *queue_checker(apr_thread_t *thread, void *arg)
{
    mq_worker_table_t *worker_table = (mq_worker_table_t *)arg;
    while(1) {
        sleep(1);

        if(complete != 0)
            return(NULL);

        log_printf(15, "SERVER: Checking processing queue...\n");
        int n_messages;
        int n_processed = 0;

        if((n_messages = processing_queue_length(processing_queue)) > 0) {
            mq_msg_t *m;

            apr_thread_mutex_lock(queue_lock);
            while((m = processing_queue_get(processing_queue)) != NULL) {

                apr_thread_mutex_lock(table_lock);
                int err = mq_send_message(m, worker_table, server_portal);
                apr_thread_mutex_unlock(table_lock);

                if(err == -1) {
                    // All workers are busy. Push this message back on top
                    processing_queue_push(processing_queue, m);
                    break;
                }
                if(err == 0) {
                    n_processed++;
                }
            }
            apr_thread_mutex_unlock(queue_lock);
        }
        log_printf(15, "SERVER: Done checking, processed %d messages from queue. Current queue length = %d\n", n_processed, processing_queue_length(processing_queue));
    }
}

mq_context_t *server_make_context()
{
    inip_file_t *ifd;
    mq_context_t *mqc;
    char *text_parameters = "[mq_context]\n"
                            "min_conn=1\n"
                            "max_conn=2\n"
                            "min_threads=2\n"
                            "max_threads=100\n"
                            "backlog_trigger=1000\n"
                            "heartbeat_dt=120\n"
                            "heartbeat_failure=1000\n"
                            "min_ops_per_sec=100\n"
                            "socket_type=1002\n"; // Set socket type to MQF_ROUND_ROBIN

    flush_log();
    ifd = inip_read_text(text_parameters);

    //log_printf(15, "SERVER: Creating context...\n");
    mqc = mq_create_context(ifd, "mq_context");
    assert(mqc != NULL);
    inip_destroy(ifd);

    return mqc;
}

void server_test()
{
    mq_context_t *mqc;
    mq_command_table_t *table;
    mq_worker_table_t *worker_table;
    apr_pool_t *pqueue_pool;
    apr_thread_t *queue_checker_thread;
    apr_status_t s;

    log_printf(1, "SERVER: Starting...\n");

    log_printf(15, "SERVER: Creating context...\n");
    mqc = server_make_context();

    log_printf(15, "SERVER: Creating portal...\n");
    server_portal = mq_portal_create(mqc, host_string, MQ_CMODE_SERVER);

    log_printf(15, "SERVER: Creating command table...\n");
    table = mq_portal_command_table(server_portal);

    log_printf(15, "SERVER: Installing commands...\n");
    mq_command_set(table, MQF_REGISTER_KEY, MQF_REGISTER_SIZE, server_portal, register_worker);
    mq_command_set(table, MQF_DEREGISTER_KEY, MQF_DEREGISTER_SIZE, server_portal, deregister_worker);
    mq_command_set(table, MQF_INCREMENT_KEY, MQF_INCREMENT_SIZE, server_portal, increment_worker);
    mq_command_table_set_default(table, server_portal, pass_through);

    log_printf(15, "SERVER: Creating worker table...\n");
    worker_table = mq_worker_table_create();

    log_printf(15, "SERVER: Installing worker table...\n");
    mq_worker_table_install(worker_table, server_portal);

    log_printf(15, "SERVER: Installing portal...\n");
    mq_portal_install(mqc, server_portal);

    log_printf(15, "SERVER: Creating processing queue...\n");
    processing_queue = processing_queue_new();

    log_printf(15, "SERVER: Starting processing queue thread...\n");
    apr_pool_create(&pqueue_pool, NULL);
    thread_create_assert(&queue_checker_thread, NULL, queue_checker, worker_table, pqueue_pool);

    log_printf(1, "SERVER: Up and running!\n");
    type_malloc(user_command, char, 20);
    while(!complete) {
        printf("> ");
        scanf("%s", user_command);
        if(strcmp(user_command, "quit") == 0)
            complete = 1;
        else if(strcmp(user_command, "workers") == 0)
            display_worker_table(worker_table);
    }

    log_printf(1, "SERVER: Shutting down...\n");
    apr_thread_join(&s, queue_checker_thread);
    apr_pool_destroy(pqueue_pool);
    processing_queue_destroy(processing_queue);
    mq_worker_table_destroy(worker_table);
    mq_destroy_context(mqc);
    free(user_command);

    log_printf(1, "SERVER: Finished.\n");
}

void *server_test_thread(apr_thread_t *th, void *arg)
{
    server_test();
    return NULL;
}

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

    // Thread pool and threads
    apr_pool_t *mpool;
    apr_thread_t *server_thread;
    apr_status_t dummy;

    // Start the background systems
    apr_wrapper_start();
    //init_opque_system();

    apr_pool_create(&mpool, NULL);

    host = mq_string_to_address(host_string);

    // Create the lock
    assert_result(apr_thread_mutex_create(&table_lock, APR_THREAD_MUTEX_DEFAULT, mpool), APR_SUCCESS);
    assert_result(apr_thread_mutex_create(&queue_lock, APR_THREAD_MUTEX_DEFAULT, mpool), APR_SUCCESS);

    // Create thread for server
    thread_create_assert(&server_thread, NULL, server_test_thread, NULL, mpool);

    apr_thread_join(&dummy, server_thread);

    // Clean up
    apr_thread_mutex_destroy(table_lock);
    apr_thread_mutex_destroy(queue_lock);
    apr_pool_destroy(mpool);

    //destroy_opque_system();
    apr_wrapper_stop();

    return 0;
}
