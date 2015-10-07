#include "mq_portal.h"
#include "mq_roundrobin.h"
#include "type_malloc.h"
#include "log.h"

mq_worker_t *mq_worker_table_first(mq_worker_table_t *table)
{
    move_to_top(table);
    return (mq_worker_t *)get_ele_data(table);
}

mq_worker_t *mq_worker_table_next(mq_worker_table_t *table)
{
    move_down(table);
    return (mq_worker_t *)get_ele_data(table);
}

mq_worker_t *mq_worker_create(char *address, int free_slots)
{

    mq_worker_t *worker;
    type_malloc(worker, mq_worker_t, 1);

    worker->address = address;
    worker->free_slots = free_slots;
    log_printf(5, "worker created with address = %s\tslots = %d\n", address, free_slots);

    return worker;
}

mq_worker_table_t *mq_worker_table_create()
{
    return (new_stack());
}

void mq_worker_table_destroy(mq_worker_table_t *table)
{
    log_printf(5, "Destroying worker table\n");
    move_to_top(table);
    mq_worker_t *worker;
    while( (worker = (mq_worker_t *)pop(table)) != NULL) {
        free(worker->address);
        free(worker);
    }
    free_stack(table, 0);
    log_printf(5, "Successfully destroyed worker table\n");
}

// mq_get_worker
// Returns a worker by specific address (used for adding/removing workers),
// or just returns the top worker (in which case it pops)
mq_worker_t *mq_get_worker(mq_worker_table_t *table, char *address)
{
    if(address == NULL) {
        return (mq_worker_t *)pop(table);
    } else {
        mq_worker_t *worker;
        log_printf(10, "Address %s requested\n", address);
        for(worker = mq_worker_table_first(table); worker != NULL; worker = mq_worker_table_next(table)) {
            if(mq_data_compare(worker->address, strlen(worker->address), address, strlen(address)) == 0) {
                log_printf(10, "Found worker!\n");
                delete_current(table, 0, 0);
                return worker;
            }
        }
        log_printf(5, "Did not find worker with address %s\n", address);
        return NULL;
    }
}

// mq_get_available_worker
// Returns the first worker with at least 1 free slot
mq_worker_t *mq_get_available_worker(mq_worker_table_t *table)
{
    //int i, n = mq_worker_table_length(table);
    mq_worker_t *worker;
    for(worker = mq_worker_table_first(table); worker != NULL; worker = mq_worker_table_next(table)) {
        if(worker->free_slots > 0) {
            delete_current(table, 0, 0);
            return worker;
        }
    }
    log_printf(0, "WARNING - All workers are busy!\n");
    return NULL;
}

void mq_worker_table_add(mq_worker_table_t *table, mq_worker_t *worker)
{
    move_to_bottom(table);
    insert_below(table, worker);
    log_printf(15, "Added worker with address = %s\tslots = %d\n", worker->address, worker->free_slots);
}

void mq_register_worker(mq_worker_table_t *table, char *address, int free_slots)
{

    mq_worker_t *worker;
    worker = mq_get_worker(table, address);
    if(worker != NULL) { //overwriting an existing worker
        log_printf(15, "Clearing existing worker with address = %s\n", address);
        worker->free_slots = free_slots;
        log_printf(10, "Worker with address %s now has %d free slots\n", address, free_slots);
    } else { //registering a new worker
        worker = mq_worker_create(address, (free_slots < 0) ? 0 : free_slots);
    }
    mq_worker_table_add(table, worker);
}

void mq_deregister_worker(mq_worker_table_t *table, char *address)
{
    mq_worker_t *worker;
    for(worker = mq_worker_table_first(table); worker != NULL; worker = mq_worker_table_next(table)) {
        if(mq_data_compare(worker->address, strlen(worker->address), address, strlen(address)) == 0) {
            log_printf(5, "Removing worker with address %s, free slots %d\n", worker->address, worker->free_slots);
            delete_current(table, 0, 0);
            free(worker->address);
            free(worker);
            return;
        }
    }
    log_printf(0, "ERROR - No worker with address %s\n", address);
}

void mq_worker_table_install(mq_worker_table_t *table, mq_portal_t *portal)
{

    if(portal->implementation_arg != NULL) {
        log_printf(15, "Portal has existing worker table, destroying\n");
        mq_worker_table_destroy(portal->implementation_arg);
    }
    if(table == NULL) {
        // suppose for some reason you tried to install a portal's existing table into itself
        // the above block would have destroyed it, so now it's null
        // it's your fault for doing something weird
        log_printf(5, "ERROR: Worker table is null!\n");
        return;
    }
    portal->implementation_arg = table;
    log_printf(15, "Installed worker table into portal's socket context\n");
}

void display_worker_table(mq_worker_table_t *table)
{
    mq_worker_t* worker;
    int i;

    for(worker = mq_worker_table_first(table), i = 0; worker != NULL; worker = mq_worker_table_next(table), i++) {
        log_printf(1, "worker_table[%2d] = { address = %s free_slots = %d }\n", i, worker->address, worker->free_slots);
    }
}

int mq_worker_table_length(mq_worker_table_t *table)
{
    mq_worker_t *w;
    int i;
    for(w = mq_worker_table_first(table), i = 0; w != NULL; w = mq_worker_table_next(table), i++);
    return i;
}

// mq_send_message()
// Sends the given message to the first available worker
// If no worker is free, the messages needs to be added to (or pushed back on) the processing queue
int mq_send_message(mq_msg_t *msg, mq_worker_table_t *table, mq_portal_t *portal)
{
    if(msg == NULL) {
        log_printf(0, "SERVER: ERROR - Null message\n");
        return -2;
    }
    mq_worker_t *worker = mq_get_available_worker(table);
    if(worker == NULL) {
        log_printf(0, "SERVER: WARNING - All workers busy\n");
        return -1;
    }

    worker->free_slots--;
    mq_frame_t *addr_frame = mq_frame_new(worker->address, strlen(worker->address), MQF_MSG_KEEP_DATA);
    mq_worker_table_add(table, worker); //add the worker back once we use its address, since get_available_worker popped it
    mq_msg_push_frame(msg, addr_frame);

    mq_task_t *pass_task = mq_task_new(portal->mqc, msg, NULL, NULL, 5);
    // Set pass_through so that this task isn't added the heartbeat table
    pass_task->pass_through = 1;

    int status = mq_submit(portal, pass_task);

    if(status != 0) {
        log_printf(0, "SERVER: mq_submit() failed! status = %d\n", status);
    } else {
        log_printf(10, "SERVER: mq_submit() successfully sent message!\n");
    }

    return 0;
}

mq_processing_queue *processing_queue_new()
{
    return new_stack();
}

void processing_queue_destroy(mq_processing_queue *queue)
{
    mq_msg_t *msg;
    move_to_top(queue);
    while((msg = (mq_msg_t *)pop(queue)) != NULL) {
        mq_msg_destroy(msg);
    }
    free(queue);
}

void processing_queue_add(mq_processing_queue *queue, mq_msg_t *msg)
{
    move_to_bottom(queue);
    insert_below(queue, msg);
    //printf("test: length = %d after adding\n", processing_queue_length(queue));
}

void processing_queue_push(mq_processing_queue *queue, mq_msg_t *msg)
{
    move_to_top(queue);
    insert_above(queue, msg);
}

mq_msg_t *processing_queue_get(mq_processing_queue *queue)
{
    //move_to_top(queue);
    return (mq_msg_t *)pop(queue);
}

int processing_queue_length(mq_processing_queue *queue)
{
    mq_msg_t *m;
    int i = 0;
    move_to_top(queue);
    m = (mq_msg_t *)get_ele_data(queue);
    while(m != NULL) {
        move_down(queue);
        m = (mq_msg_t *)get_ele_data(queue);
        i++;
    }
    return i;
}

// display_msg_frames()
// Print out the sizes of frames in this message, from which you can infer
// the contents.
// Assumes only addresses are 20+ chars and prints them
void display_msg_frames(mq_msg_t *msg)
{
    // This prints a lot of text and chokes up valgrind, so only print if needed
    if(_log_level < 10)
        return;

    mq_frame_t *f;
    char *data, *toprint;
    int size, i;
    for(f = mq_msg_first(msg), i = 0; f != NULL; f = mq_msg_next(msg), i++) {
        mq_get_frame(f, (void **)&data, &size);
        log_printf(0, "msg[%2d]:\t%d\n", i, size);
        if(size != 0) {
            toprint = malloc(size + 1);
            strncpy(toprint, data, size);
            toprint[size] = '\0';
            if(size >= 20) {
                log_printf(0, "        \t%s\n", toprint);
            }
            free(toprint);
        }
    }
}





