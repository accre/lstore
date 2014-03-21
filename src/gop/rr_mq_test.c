#include "mq_portal.h"
#include "mq_roundrobin.h"
#include "apr_wrapper.h"
#include "log.h"
#include "type_malloc.h"
#include <sys/eventfd.h>

char *host = "tcp://127.0.0.1:6714";
char *worker_host = "tcp://127.0.0.1:6715";
int control_efd = -1; //Used to signal the server to shutdown
int worker_efd = -1; //Used to signal the worker to shutdown

int register_efd = -1; //Used to signal real/test REGISTER

int count = 0;
char *handle = NULL;

/****************************************
 * Server message handling functions    *
 ****************************************/

void process_ping(mq_portal_t *p, mq_socket_t *sock, mq_msg_t *msg) {
	mq_frame_t *f, *pid;
	char *data;
	int err;
	
	log_printf(15, "SERVER: Message frames BEFORE destroying:\n");
	int i;
	for (f = mq_msg_first(msg), i=0; f != NULL; f = mq_msg_next(msg), i++) {
		mq_get_frame(f, (void **)&data, &err);
		log_printf(5, "SERVER:\tfsize[%d] = %d\n", i, err);
	}
	
	//** Peel off the top frames and just leave the return address
	f = mq_msg_first(msg);
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //blank frame
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //version frame
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //command frame (exec)
	
	pid = mq_msg_current(msg); //Message ID frame
	mq_get_frame(pid, (void **)&data, &err); // data should be "HANDLE"
	if(mq_data_compare(data, 6, "HANDLE", 6) == 0) {
		log_printf(15, "SERVER: Received correct message ID!\n");
	} else {
		log_printf(15, "SERVER: Received INCORRECT message ID!\n");
		log_printf(15, "SERVER: ID should be \"HANDLE\", ID = %s\n", data);
	}	
	
	log_printf(15, "SERVER: Destroying message...\n");
	mq_msg_destroy(msg); //Can't use this function in "thread" version of the handler, i.e., the method that is linked in the command table
	count++; //lol
}

void ping(void *arg, mq_task_t *task) {
	log_printf(15, "SERVER: Executing PING response...\n");
	process_ping((mq_portal_t *)arg, NULL, task->msg);
	log_printf(15, "SERVER: Completed PING response.\n");
	task->msg = NULL;
}

void unpack_register_msg(mq_portal_t *p, mq_msg_t *msg) {
	// This is a function to check that REGISTER messages are coming in properly
	// it's only used to test a message coming from worker_host
	mq_frame_t *pid;
	char *data;
	char *worker_address;
	int *worker_free_slots;
	int err;
	int test_slots = 10;
	
	pid = mq_msg_first(msg);
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //blank frame
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //version frame
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //command frame (exec)
	
	pid = mq_msg_current(msg); //Message ID frame
	mq_get_frame(pid, (void **)&data, &err); // data should be "WORKER"
	if(mq_data_compare(data, 6, "WORKER", 6) == 0) {
		log_printf(15, "SERVER: Received correct message ID!\n");
	} else {
		log_printf(15, "SERVER: Received INCORRECT message ID!\n");
		log_printf(15, "SERVER: ID should be \"WORKER\", ID = %s\n", data);
		log_printf(15, "SERVER: Failed! Destroying message...\n");
		mq_msg_destroy(msg);
		return;
	}	
	mq_frame_destroy(mq_msg_pluck(msg, 0));
	
	pid = mq_msg_current(msg); // MQF_REGISTER_KEY frame
	mq_get_frame(pid, (void **)&data, &err); // data should be MQF_REGISTER_KEY, err should be MQF_REGISTER_SIZE
	if(mq_data_compare(data, err, MQF_REGISTER_KEY, MQF_REGISTER_SIZE) == 0) {
		log_printf(15, "SERVER: Received REGISTER command!\n");
	} else {
		log_printf(5, "SERVER: Received incorrect command key, key = %s\n", data);
		log_printf(15, "SERVER: Failed! Destroying message...\n");
		mq_msg_destroy(msg);
		return;
	}
	mq_frame_destroy(mq_msg_pluck(msg, 0));
	
	pid = mq_msg_current(msg); // worker_host frame
	mq_get_frame(pid, (void **)&data, &err); //should be worker_host
	if(mq_data_compare(data, err, worker_host, strlen(worker_host)) == 0) {
		type_malloc(worker_address, char, err+1);
		memcpy(worker_address, data, err);
		log_printf(15, "SERVER: Received correct worker host address = %s, size = %d\n", worker_address, err);
	} else {
		log_printf(5, "SERVER: Received incorrect address, address = %s\n", worker_host);
		log_printf(15, "SERVER: Failed! Destroying message...\n");
		mq_msg_destroy(msg);
		return;
	}
	mq_frame_destroy(mq_msg_pluck(msg, 0));
	
	pid = mq_msg_current(msg); // free_slots frame
	mq_get_frame(pid, (void **)&worker_free_slots, &err); // should be free_slots (10)
	if(mq_data_compare(worker_free_slots, err, &test_slots, 4) == 0) {
		log_printf(15, "SERVER: Received correct worker free slots = %d, size = %d\n", *worker_free_slots, err);
	} else {
		log_printf(5, "SERVER: Received incorrect number of free slots, free_slots = %d, size = %d\n", *worker_free_slots, err);
		log_printf(15, "SERVER: Failed! Destroying message...\n");
		mq_msg_destroy(msg);
		return;
	}
	
	mq_msg_destroy(msg);
	
	//Now worker_address and worker_free_slots should be filled with the correct values
	log_printf(5, "SERVER: Registering new worker...\n");
	mq_register_worker(p->implementation_arg, worker_address, *worker_free_slots);
	
	log_printf(15, "SERVER: Destroying message...\n");
}

void process_register_worker(mq_portal_t *p, mq_socket_t *sock, mq_msg_t *msg) {
	mq_frame_t *f;
	char *data;
	char *worker_address;
	int *worker_free_slots;
	int err;
	
	log_printf(15, "SERVER: Message frames BEFORE destroying:\n");
	int i;
	for (f = mq_msg_first(msg), i=0; f != NULL; f = mq_msg_next(msg), i++) {
		mq_get_frame(f, (void **)&data, &err);
		log_printf(5, "SERVER:\tfsize[%d] = %d\n", i, err);
	}
	
	if(register_efd == -1) {
		unpack_register_msg(p, msg);
	}
	else {
		f = mq_msg_first(msg);
		//Assume these are good
		mq_frame_destroy(mq_msg_pluck(msg, 0)); //blank frame
		mq_frame_destroy(mq_msg_pluck(msg, 0)); //version frame
		mq_frame_destroy(mq_msg_pluck(msg, 0)); //command frame (EXEC)
		mq_frame_destroy(mq_msg_pluck(msg, 0)); //ID frame ("WORKER")
		mq_frame_destroy(mq_msg_pluck(msg, 0)); //argument frame (REGISTER)
		
		f = mq_msg_current(msg);
		type_malloc(worker_address, char, 21);
		mq_get_frame(f, (void **)worker_address, &err);
		mq_frame_destroy(mq_msg_pluck(msg, 0)); //address frame
		
		f = mq_msg_current(msg);
		mq_get_frame(f, (void **)&worker_free_slots, &err);
		mq_frame_destroy(mq_msg_pluck(msg, 0)); //slots frame
		
		mq_msg_destroy(msg);
		
		mq_register_worker(p->implementation_arg, worker_address, *worker_free_slots);
	}
	
	count++; //lol
}

void register_worker(void *arg, mq_task_t *task) {
	log_printf(15, "SERVER: Received REGISTER command!\n");
	process_register_worker((mq_portal_t *)arg, NULL, task->msg);
	log_printf(15, "SERVER: Completed REGISTER command!\n");
	task->msg = NULL;
}

/****************************************
 * Server thread functions				*
 ****************************************/

mq_context_t *server_make_context() {
	inip_file_t *ifd;
	mq_context_t *mqc;	
	char *text_parameters = "[mq_context]\n\t"
		"min_conn=1\n\t"
		"max_conn=2\n\t"
		"min_threads=2\n\t"
		"max_threads=100\n\t"
		"backlog_trigger=1000\n\t"
		"heartbeat_dt=1\n\t"
		"heartbeat_failure=5\n\t"
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
	/*
	 * Server needs a way of managing registered workers
	 * Try using an apr_hash_t
	 */
	mq_context_t *mqc;
	mq_portal_t *server_portal;
	mq_command_table_t *table;
	mq_worker_table_t *worker_table;
	uint64_t n;
	
	log_printf(15, "SERVER: Starting...\n");
	
	log_printf(15, "SERVER: Creating context...\n");
	mqc = server_make_context();
	
	log_printf(15, "SERVER: Creating portal...\n");
	server_portal = mq_portal_create(mqc, host, MQ_CMODE_SERVER);
	
	log_printf(15, "SERVER: Creating command table...\n");
	table = mq_portal_command_table(server_portal);
	
	log_printf(15, "SERVER: Installing ping command...\n");
	mq_command_set(table, MQF_PING_KEY, MQF_PING_SIZE, server_portal, ping);
	mq_command_set(table, MQF_REGISTER_KEY, MQF_REGISTER_SIZE, server_portal, register_worker);
	
	log_printf(15, "SERVER: Creating worker table...\n");
	worker_table = mq_worker_table_create();
	
	log_printf(15, "SERVER: Installing worker table...\n");
	mq_worker_table_install(worker_table, server_portal);
	
	log_printf(15, "SERVER: Installing portal...\n");
	mq_portal_install(mqc, server_portal);
	
	log_printf(15, "SERVER: Up and running!\n");
	read(control_efd, &n, sizeof(n));
	
	log_printf(15, "SERVER: Shutting down...\n");
	mq_worker_table_destroy(worker_table);
	mq_destroy_context(mqc);
	
	log_printf(15, "SERVER: Finished.\n");
}

void *server_test_thread(apr_thread_t *th, void *arg) {
	// Functions for threads need to be of the form
	// void name_of_func(apr_thread_t *th, void *arg)
	server_test();
	return NULL;
}

/****************************************
 * Client thread functions				*
 ****************************************/

mq_context_t *client_make_context() {
	mq_context_t *mqc;
	inip_file_t *ifd;
	char *text_parameters = "[mq_context]\n\t"
		"min_conn=1\n\t"
		"max_conn=4\n\t"
		"min_threads=2\n\t"
		"max_threads=10\n\t"
		"backlog_trigger=1000\n\t"
		"heartbeat_dt=1\n\t"
		"heartbeat_failure=10\n\t"
		"min_ops_per_sec=100\n\t"
		"socket_type=1002\n"; // Set socket type to MQF_ROUND_ROBIN
	
	ifd = inip_read_text(text_parameters);
	
	mqc = mq_create_context(ifd, "mq_context");
	inip_destroy(ifd);
	
	return mqc;
}

void client_ping_test() {
	mq_context_t *mqc;
	mq_msg_t *msg;
	op_generic_t *gop;
	
	log_printf(15, "CLIENT: Starting...\n");
	
	log_printf(15, "CLIENT: Creating context...\n");
	mqc = client_make_context();
	
	log_printf(15, "CLIENT: Building message...\n");
	msg = mq_msg_new();
	mq_msg_append_mem(msg, host, strlen(host), MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, MQF_EXEC_KEY, MQF_EXEC_SIZE, MQF_MSG_KEEP_DATA); // MQF_EXEC_KEY is the messages's command
	mq_msg_append_mem(msg, "HANDLE", 6, MQF_MSG_KEEP_DATA); // "HANDLE" is the message's ID
	mq_msg_append_mem(msg, MQF_PING_KEY, MQF_PING_SIZE, MQF_MSG_KEEP_DATA); // MQF_PING_KEY is the message's first argument under MQF_EXEC_KEY
	mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
	
	log_printf(15, "CLIENT: Creating new generic operation...\n");
	gop = new_mq_op(mqc, msg, NULL, NULL, NULL, 5);
	
	log_printf(15, "CLIENT: Sending message...\n");
	int status = gop_waitall(gop);
	gop_free(gop, OP_DESTROY);
	
	if(status != OP_STATE_SUCCESS)
		log_printf(15, "CLIENT: Failed sending PING request to server at %s - error = %d\n", host, status);
	else
		log_printf(15, "CLIENT: Successfully sent PING request to server!\n");
	
	log_printf(15, "CLIENT: Shutting down...\n");
	mq_destroy_context(mqc);
	
	log_printf(15, "CLIENT: Finished.\n");
}

void *client_ping_test_thread(apr_thread_t *th, void *arg) {
	client_ping_test();
	return NULL;
}

/****************************************
 * Worker thread functions				*
 ****************************************/

mq_msg_t *pack_register_msg(char *address) {
	mq_msg_t *msg = mq_msg_new();
	int free_slots = 10;
	
	mq_msg_append_mem(msg, host, strlen(host), MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, MQF_EXEC_KEY, MQF_EXEC_SIZE, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, "WORKER", 6, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, MQF_REGISTER_KEY, MQF_REGISTER_SIZE, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, address, strlen(address), MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, &free_slots, 4, MQF_MSG_KEEP_DATA); // THIS 4 IS HARDCODED
	mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
	
	return msg;
}

void bulk_worker_test(mq_context_t *mqc) {
	
	/*
	 * Problem - Server uses the process_register_worker function to check if the REGISTER message
	 * is correct. This is fine with the single message, since the address is supposed to be a known
	 * address that I can check against.
	 * With the bulk test, though, the addresses are different, so I either need a proper generic
	 * registration handling function, or some sort of logic to tell the server we're in bulk test
	 * mode.
	 */
	
	opque_t *queue;
	op_generic_t *gop;
	mq_msg_t *msg;
	int current, success, failure;
	char *address;
	
	queue = new_opque();
	type_malloc(address, char, 21); // address is 20 char long, +1 for \0
	
	log_printf(5, "WORKER: Starting bulk REGISTER test...\n");
	
	register_efd = 0; // this will tell server that we're not testing the known worker
	
	int i, start = 716;
	for(i = 0; i < 100; i++) {
		sprintf(address, "tcp://127.0.0.1:6%d", (start+i)); //build a new address so each REGISTER is unique
		log_printf(20, "WORKER: Address created = %s\n", address);
		msg = pack_register_msg(address);
		gop = new_mq_op(mqc, msg, NULL, NULL, NULL, 5);
		opque_add(queue, gop);
	}
	
	current = 0; success = 0; failure = 0;
	while((gop = opque_waitany(queue)) != NULL) {
		current++;
		int status = gop_get_status(gop).op_status;
		gop_free(gop, OP_DESTROY);
		if(status != OP_STATE_SUCCESS) {
			log_printf(5, "WORKER: FAILURE! n = %d, status = %d\n", current, status);
			failure++;
		} else {
			log_printf(5, "WORKER: Success: n = %d, status = %d\n", current, status);
			success++;
		}
		flush_log();
	}
	
	log_printf(5, "WORKER: Sleeping...\n");
	sleep(5);
	log_printf(5, "WORKER: Waking up\n");
	
	log_printf(5, "WORKER: Completed bulk REGISTER test, results:\n");
	log_printf(5, "WORKER:\ttotal = %d, fails = %d, success = %d\n", current, failure, success);
	
	opque_free(queue, OP_DESTROY);
}

mq_context_t *worker_make_context() {
	mq_context_t *mqc;
	inip_file_t *ifd;
	char *text_parameters = "[mq_context]\n\t"
		"min_conn=1\n\t"
		"max_conn=4\n\t"
		"min_threads=2\n\t"
		"max_threads=10\n\t"
		"backlog_trigger=1000\n\t"
		"heartbeat_dt=1\n\t"
		"heartbeat_failure=10\n\t"
		"min_ops_per_sec=100\n";
		// omitting socket_type for workers
	
	ifd = inip_read_text(text_parameters);
	
	mqc = mq_create_context(ifd, "mq_context");
	inip_destroy(ifd);
	
	return mqc;
}

void worker_register_test() {
	/*
	 * A worker behaves like a client AND a server.
	 * It only needs one thread, and will send/receive messages
	 * through that. It will use the round robin socket type.
	 * 
	 * First, send a REGISTER command to the server.
	 * This should create a mq_worker_t object in a list on the server
	 * with an address and a number of free slots. Whenever a worker
	 * completes a task, it will send an INCREMENT command to the server.
	 * The server will internally keep track of the remaining free slots
	 * for each worker by decrementing when sending a command, and incrementing
	 * when receiving the INCREMENT command.
	 * 
	 * Actually, try using a general SET command instead of INCREMENT.
	 * When a worker REGISTERs, it can also SET
	 * 
	 * Finally, a worker can also DEREGISTER
	 */
	 
	mq_context_t *mqc;
	mq_portal_t *worker_portal;
	mq_msg_t *msg;
	op_generic_t *gop;
	int free_slots = 10;
	uint64_t n;
	
	log_printf(15, "WORKER: Starting...\n");
	
	log_printf(15, "WORKER: Creating context...\n");
	mqc = worker_make_context();
	
	log_printf(15, "WORKER: Creating portal...\n");
	worker_portal = mq_portal_create(mqc, worker_host, MQ_CMODE_SERVER);
	
	log_printf(15, "WORKER: Installing portal...\n");
	mq_portal_install(mqc, worker_portal);
	
	log_printf(15, "WORKER: Building message...\n");
	msg = mq_msg_new();
	mq_msg_append_mem(msg, host, strlen(host), MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, MQF_EXEC_KEY, MQF_EXEC_SIZE, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, "WORKER", 6, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, MQF_REGISTER_KEY, MQF_REGISTER_SIZE, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, worker_host, strlen(worker_host), MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, &free_slots, 4, MQF_MSG_KEEP_DATA); // THIS 4 IS HARDCODED
	mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
	
	log_printf(15, "WORKER: Creating new generic operation...\n");
	gop = new_mq_op(mqc, msg, NULL, NULL, NULL, 5);
	
	log_printf(15, "WORKER: Sending single REGISTER message...\n");
	int status = gop_waitall(gop);
	
	gop_free(gop, OP_DESTROY);
	
	if(status != OP_STATE_SUCCESS)
		log_printf(15, "WORKER: Failed sending REGISTER request to server at %s - error = %d\n", host, status);
	else
		log_printf(15, "WORKER: Successfully sent REGISTER request to server!\n");
	
	log_printf(15, "WORKER: Up and running!\n");
	
	register_efd = 0;
	bulk_worker_test(mqc);
	
	read(worker_efd, &n, sizeof(n));
	
	log_printf(15, "WORKER: Shutting down...\n");
	mq_destroy_context(mqc);
	
	log_printf(15, "WORKER: Finished.\n");
}

void *worker_test_thread(apr_thread_t *th, void *arg) {
	worker_register_test();
	return NULL;
}

/****************************************
 * Main	+ misc functions				*
 ****************************************/

int main(int argc, char **argv) {
	
	if (argc > 2) {
		if(strcmp(argv[1], "-d") == 0)
			set_log_level(atol(argv[2]));
		else {
			printf("rr_mq_test -d <log_level>\n");
			return 1;
		}
	}
	else {
		printf("rr_mq_test -d <log_level>\n");
		return 1;
	}
	
	// Thread pool, threads, and control fd
	apr_pool_t *mpool;
	apr_thread_t *client_thread, *server_thread, *worker_thread;
	apr_status_t dummy;
	uint64_t server_shutdown = -1;
	uint64_t worker_shutdown = -1;
	
	// Start the background systems
	apr_wrapper_start();
	init_opque_system();
	
	apr_pool_create(&mpool, NULL);
	
	// Initialize control fd
	control_efd = eventfd(0, 0);
	assert(control_efd != -1);
	
	// Create threads for server, client, and worker
	thread_create_assert(&server_thread, NULL, server_test_thread, NULL, mpool);
	thread_create_assert(&client_thread, NULL, client_ping_test_thread, NULL, mpool);
	thread_create_assert(&worker_thread, NULL, worker_test_thread, NULL, mpool);
	
	// Join the threads when they complete
	apr_thread_join(&dummy, client_thread);
	
	// Send a stop signal to the server and worker so the threads complete
	server_shutdown = 1;
	worker_shutdown = 1;
	write(control_efd, &server_shutdown, sizeof(server_shutdown));
	write(worker_efd, &worker_shutdown, sizeof(worker_shutdown));
	apr_thread_join(&dummy, server_thread);
	apr_thread_join(&dummy, worker_thread);
	
	// Clean up
	apr_pool_destroy(mpool);
	
	destroy_opque_system();
	apr_wrapper_stop();
	
	close(control_efd);
	
	printf("DEBUG: count = %d\n", count);
	
	return 0;
}
