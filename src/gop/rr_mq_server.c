/*****************************
 * Round robin server
 *****************************/

#include "mq_portal.h"
#include "mq_roundrobin.h"
#include "apr_wrapper.h"
#include "log.h"
#include "type_malloc.h"
#include <sys/eventfd.h>

char *host = "tcp://127.0.0.1:6714";
int complete = 0;
char *user_command = NULL;

/*
 * Processing functions
 */

/*
 * process_round_robin_pass()
 * 
 * Grab a worker from the worker table using round robin and pass it the message
 */
void process_round_robin_pass(mq_portal_t *p, mq_socket_t *sock, mq_msg_t *msg) {
	mq_worker_table_t *worker_table;
	mq_frame_t *f, *address_frame;
	unsigned int num_workers;
	mq_worker_t *worker;
	char *worker_address;
	op_generic_t *gop;	
	mq_msg_t *pass;
	int status;
	
	if(p->implementation_arg != NULL)
		worker_table = (mq_worker_table_t *)p->implementation_arg;
	else {
		log_printf(1, "SERVER: ERROR - no worker table found\n");
		return;
	}
	
	log_printf(0, "SERVER: Worker table before retrieving worker:\n");
	display_worker_table(worker_table);
	
	int done = 0;
	while( !done ) {
		worker = pop(worker_table);
		log_printf(0, "SERVER: Worker table after popping top worker:\n");
		display_worker_table(worker_table);
		if( worker == NULL ) {
			log_printf(0, "SERVER: ERROR - no workers\n");
			log_printf(0, "SERVER: Destroying message and cancelling pass\n");
			mq_msg_destroy(msg);
			return;
		}
		log_printf(0, "SERVER: Grabbed worker with address %s\n", worker->address);
		if(worker->free_slots <= 0) {
			log_printf(0, "SERVER: This worker is too busy. Trying again...\n");
			mq_add_worker(worker_table, worker);
		}
		else {
			log_printf(0, "SERVER: This worker is free\n");
			worker->free_slots--;
			mq_add_worker(worker_table, worker);
			done = 1;
		}
		log_printf(0, "SERVER: Worker table after readding worker:\n");
		display_worker_table(worker_table);
	}
	// For some reason, I'm segfaulting if I use mq_get_worker() here
	
	worker_address = worker->address;
	//log_printf(1, "SERVER: Grabbed worker with address %s\n", worker_address);
	address_frame = mq_frame_new(worker_address, strlen(worker_address), MQF_MSG_KEEP_DATA); //new frame with this address
	
	mq_msg_push_frame(msg, address_frame); //add the worker on top
	
	log_printf(0, "SERVER: Message being passed:\n");
	display_msg_frames(msg);
	
	log_printf(0, "SERVER: Submitting msg via mq_submit()...\n");
	mq_task_t *pass_task = mq_task_new(p->mqc, msg, NULL, NULL, 5);
	// Set pass_through so that this task isn't added the heartbeat table
	pass_task->pass_through = 1;
	
	status = mq_submit(p, pass_task);
	
	if(status != 0) {
		log_printf(0, "SERVER: mq_submit() failed! status = %d\n", status);
	} else {
		log_printf(0, "SERVER: mq_submit() successfully sent message!\n");
	}
	
}

void process_trackexec_ping(mq_portal_t *p, mq_socket_t *sock, mq_msg_t *msg) {
	
	mq_frame_t *f, *id_frame;
	//char *data;
	int err;
	
	log_printf(0, "SERVER: Building response message...\n");
	
	f = mq_msg_first(msg);
	mq_frame_destroy(mq_msg_pluck(msg, 0)); // null
	mq_frame_destroy(mq_msg_pluck(msg, 0)); // version
	mq_frame_destroy(mq_msg_pluck(msg, 0)); // trackexec
	id_frame = mq_msg_pluck(msg, 0); // "HANDLE"
	mq_frame_destroy(mq_msg_pluck(msg, 0)); // ping
	//leave the null frame, mq_apply_return_address_msg() will put it on top of response
	//address = mq_msg_pluck(msg, 0); //client address
	
	mq_msg_t *response = mq_msg_new();//mq_make_response_core_msg(msg, id_frame);
	mq_msg_append_mem(response, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(response, MQF_RESPONSE_KEY, MQF_RESPONSE_SIZE, MQF_MSG_KEEP_DATA);
	mq_msg_append_frame(response, id_frame);
	mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);
	
	mq_apply_return_address_msg(response, msg, 0);
	
	log_printf(0, "SERVER: Response message frames:\n");
	display_msg_frames(response);
	
	err = mq_submit(p, mq_task_new(p->mqc, response, NULL, NULL, 5));
	if(err != 0) {
		log_printf(0, "SERVER: Failed to send response\n");
	} else {
		log_printf(0, "SERVER: Successfully sent response!\n");
	}
	mq_msg_destroy(msg);
}

void process_exec_ping(mq_portal_t *p, mq_socket_t *sock, mq_msg_t *msg) {
	mq_frame_t *pid;
	mq_frame_t *f;
	char *data;
	int err;
	
	log_printf(4, "SERVER: Message frames BEFORE destroying:\n");
	display_msg_frames(msg);
	
	f = mq_msg_first(msg);
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //blank frame
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //version frame
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //command frame (exec)
	
	pid = mq_msg_first(msg); //Message ID frame
	mq_get_frame(pid, (void **)&data, &err); // data should be "HANDLE"
	if(mq_data_compare(data, 6, "HANDLE", 6) == 0) {
		log_printf(4, "SERVER: Received correct message ID!\n");
	} else {
		log_printf(4, "SERVER: Received INCORRECT message ID!\n");
		log_printf(4, "SERVER: ID should be \"HANDLE\", ID = %s\n", data);
	}	
	
	log_printf(4, "SERVER: Destroying message...\n");
	mq_msg_destroy(msg);
}

/*
 * process_register_worker()
 * 
 * Extract a worker's client portal address and number of free slots from a
 * REGISTER message and add it in the worker table
 */
void process_register_worker(mq_portal_t *p, mq_socket_t *sock, mq_msg_t *msg) {
	int worker_free_slots;
	char *worker_address;
	mq_msg_t *response;
	mq_frame_t *id;
	mq_frame_t *f;
	char *data;
	int err;
	
	log_printf(0, "SERVER: Contents of REGISTER message:\n");
	display_msg_frames(msg);
	
	f = mq_msg_first(msg);
	mq_frame_destroy(mq_msg_pluck(msg, 0)); // blank frame
	mq_frame_destroy(mq_msg_pluck(msg, 0)); // version frame
	mq_frame_destroy(mq_msg_pluck(msg, 0)); // command frame (EXEC)
	id = mq_msg_pluck(msg, 0); // ID frame ("WORKER")
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
	mq_register_worker(p->implementation_arg, worker_address, worker_free_slots);
	
	log_printf(0, "SERVER: Sending REGISTER confirmation to worker...\n");
	
	// Build the response using the ID grabbed earlier
	response = mq_msg_new();
	mq_msg_append_mem(response, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(response, MQF_RESPONSE_KEY, MQF_RESPONSE_SIZE, MQF_MSG_KEEP_DATA);
	mq_msg_append_frame(response, id);
	mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);
	mq_apply_return_address_msg(response, msg, 0);
	
	log_printf(0, "SERVER: Response message frames:\n");
	display_msg_frames(response);
	
	mq_task_t *pass_task = mq_task_new(p->mqc, response, NULL, NULL, 5);
	// No need to set this flag here, since this is an actual response to a TRACKEXEC message
	//pass_task->pass_through = 1;
	err = mq_submit(p, pass_task);
	if(err != 0) {
		log_printf(0, "SERVER: Failed to send response, err = %d\n", err);
	} else {
		log_printf(0, "SERVER: Successfully sent response!\n");
	}
	
	mq_msg_destroy(msg);
}

/*
 * process_deregister_worker()
 * 
 * Remove the worker that sent the message from the worker table
 */
void process_deregister_worker(mq_portal_t *p, mq_socket_t *sock, mq_msg_t *msg) {
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
	log_printf(1, "SERVER: Attempting to delete worker with address %s\n", worker_address);
	
	table = (mq_worker_table_t *) p->implementation_arg;
	
	mq_deregister_worker(table, worker_address);
	
	log_printf(1, "SERVER: Deleted worker with address %s\n", worker_address);
	free(worker_address);
	mq_msg_destroy(msg);
}

void process_increment_worker(mq_portal_t *p, mq_socket_t *sock, mq_msg_t *msg) {
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
	log_printf(1, "SERVER: Attempting to increment worker with address %s\n", worker_address);
	
	table = (mq_worker_table_t *) p->implementation_arg;
	for(worker = mq_worker_table_first(table); worker != NULL; worker = mq_worker_table_next(table)) {
		if(mq_data_compare(worker->address, strlen(worker->address), worker_address, strlen(worker_address)) == 0) {
			log_printf(0, "Found worker!\n");
			worker->free_slots++;
			log_printf(0, "SERVER: Worker at address %s now has %d free slots\n", worker->address, worker->free_slots);
			free(worker_address);
			mq_msg_destroy(msg);
			return;
		}
	}
	
	log_printf(0, "SERVER: ERROR - Could not find worker with address %s\n", worker_address);
	free(worker_address);
	mq_msg_destroy(msg);
}

/*
 * Hooks
 */
 
/*
 * pass_through()
 * 
 * Default function for messages the server won't process itself.
 * Decide whether to ignore and pass forward, or send to a worker
 */
void pass_through(void *arg, mq_task_t *task) {
	mq_portal_t *p;
	mq_frame_t *f;
	mq_msg_t *msg;
	char *data;
	int size;
	
	p = (mq_portal_t *)arg;
	msg = task->msg;
	
	log_printf(0, "SERVER: Beginning pass_through(). Message received:\n");
	display_msg_frames(msg);
	
	f = mq_msg_first(msg);
	mq_get_frame(f, (void **)&data, &size);
	
	if(size == 0) {		
		// Top frame is a null frame - message terminated here and should be passed to a worker
		log_printf(4, "SERVER: Deferring message to worker...\n");
		process_round_robin_pass(p, NULL, msg);
		log_printf(4, "SERVER: Sent message to worker.\n");
	}	
	else {
		// Top frame is an address - ignore and pass on
		log_printf(4, "SERVER: Passing message to %s\n", data);
		mq_task_t *pass_task = mq_task_new(p->mqc, msg, NULL, NULL, 5);
		//Set this flag so the task isn't added to the heartbeat table in case it's a TRACKEXEC
		pass_task->pass_through = 1;
		int err = mq_submit(p, pass_task);
		
		if(err != 0) {
			log_printf(0, "SERVER: Failed to pass message, err = %d\n", err);
		} else {
			log_printf(0, "SERVER: Successfully passed message!\n");
		}
	}
	
	// So that msg doesn't disappear when this task is destroyed
	task->msg = NULL;
	
	log_printf(0, "SERVER: End of pass_through()\n");
}

void ping(void *arg, mq_task_t *task) {
	log_printf(4, "SERVER: Executing PING response...\n");
	//determine if this is exec or trackexec
	mq_msg_t *msg = task->msg;
	mq_frame_t *exec_frame, *f;
	char *data; int size;
	
	for(f = mq_msg_first(msg); f != NULL; f = mq_msg_next(msg)) {
		mq_get_frame(f, (void **)&data, &size);
		if( mq_data_compare(data, size, MQF_EXEC_KEY, MQF_EXEC_SIZE) == 0 ) {
			log_printf(4, "SERVER: PING request is EXEC\n");
			process_exec_ping((mq_portal_t *)arg, NULL, msg);
			break;
		}
		if( mq_data_compare(data, size, MQF_TRACKEXEC_KEY, MQF_TRACKEXEC_SIZE) == 0 ) {
			log_printf(4, "SERVER: PING request is TRACKEXEC\n");
			process_trackexec_ping((mq_portal_t *)arg, NULL, msg);
			break;
		}
	}
		
	log_printf(4, "SERVER: Completed PING response.\n");
	task->msg = NULL;
}

void register_worker(void *arg, mq_task_t *task) {
	log_printf(4, "SERVER: Received REGISTER command!\n");
	process_register_worker((mq_portal_t *)arg, NULL, task->msg);
	log_printf(4, "SERVER: Completed REGISTER command!\n");
	task->msg = NULL;
}

void deregister_worker(void *arg, mq_task_t *task) {
	log_printf(4, "SERVER: Received DEREGISTER command!\n");
	process_deregister_worker((mq_portal_t *)arg, NULL, task->msg);
	log_printf(4, "SERVER: Completed DEREGISTER command!\n");
	task->msg = NULL;
}

void increment_worker(void *arg, mq_task_t *task) {
	log_printf(4, "SERVER: Received INCREMENT command!\n");
	process_increment_worker((mq_portal_t *)arg, NULL, task->msg);
	log_printf(4, "SERVER: Completed INCREMENT command!\n");
	task->msg = NULL;
}

/*
 * Thread functions
 */

mq_context_t *server_make_context() {
	inip_file_t *ifd;
	mq_context_t *mqc;	
	char *text_parameters = "[mq_context]\n\t"
		"min_conn=1\n\t"
		"max_conn=2\n\t"
		"min_threads=2\n\t"
		"max_threads=100\n\t"
		"backlog_trigger=1000\n\t"
		"heartbeat_dt=120\n\t"
		"heartbeat_failure=1000\n\t"
		"min_ops_per_sec=100\n\t"
		"socket_type=1002\n"; // Set socket type to MQF_ROUND_ROBIN
	
	flush_log();
	ifd = inip_read_text(text_parameters);
	
	//log_printf(15, "SERVER: Creating context...\n");
	mqc = mq_create_context(ifd, "mq_context");
	assert(mqc != NULL);
	inip_destroy(ifd);
	
	return mqc;
}

void server_test() {
	mq_context_t *mqc;
	mq_portal_t *server_portal;
	mq_command_table_t *table;
	mq_worker_table_t *worker_table;
	
	log_printf(1, "SERVER: Starting...\n");
	
	log_printf(1, "SERVER: Creating context...\n");
	mqc = server_make_context();
	
	log_printf(1, "SERVER: Creating portal...\n");
	server_portal = mq_portal_create(mqc, host, MQ_CMODE_SERVER);
	
	log_printf(1, "SERVER: Creating command table...\n");
	table = mq_portal_command_table(server_portal);
	
	log_printf(1, "SERVER: Installing commands...\n");
	// PING messages go to the worker
	//mq_command_set(table, MQF_PING_KEY, MQF_PING_SIZE, server_portal, ping);
	mq_command_set(table, MQF_REGISTER_KEY, MQF_REGISTER_SIZE, server_portal, register_worker);
	mq_command_set(table, MQF_DEREGISTER_KEY, MQF_DEREGISTER_SIZE, server_portal, deregister_worker);
	mq_command_set(table, MQF_INCREMENT_KEY, MQF_INCREMENT_SIZE, server_portal, increment_worker);
	mq_command_table_set_default(table, server_portal, pass_through);
	
	log_printf(1, "SERVER: Creating worker table...\n");
	worker_table = mq_worker_table_create();
	
	log_printf(1, "SERVER: Installing worker table...\n");
	mq_worker_table_install(worker_table, server_portal);
	
	log_printf(1, "SERVER: Installing portal...\n");
	mq_portal_install(mqc, server_portal);
	
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
	mq_worker_table_destroy(worker_table);
	mq_destroy_context(mqc);
	free(user_command);
	
	log_printf(1, "SERVER: Finished.\n");
}

void *server_test_thread(apr_thread_t *th, void *arg) {
	server_test();
	return NULL;
}

/*
 * main
 */
int main(int argc, char **argv) {
	
	if (argc > 2) {
		if(strcmp(argv[1], "-d") == 0)
			set_log_level(atol(argv[2]));
		else {
			printf("%s -d <log_level>\n", argv[0]);
			return 1;
		}
	}
	else {
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
	
	// Create thread for server
	thread_create_assert(&server_thread, NULL, server_test_thread, NULL, mpool);
	
	apr_thread_join(&dummy, server_thread);
	
	// Clean up
	apr_pool_destroy(mpool);
	
	//destroy_opque_system();
	apr_wrapper_stop();
	
	return 0;
}
