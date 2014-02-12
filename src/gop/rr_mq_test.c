#include "mq_portal.h"
#include "mq_roundrobin.h"
#include "apr_wrapper.h"
#include "log.h"
#include "type_malloc.h"
#include <sys/eventfd.h>

char *host = "tcp://127.0.0.1:6714";
int control_efd = -1;

int count = 0;
char *handle = NULL;

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
	mq_msg_destroy(msg); //Can't use this function in "thread" version of the handler. I.e., the method that is linked in the command table
	count++;
}

void ping(void *arg, mq_task_t *task) {
	log_printf(15, "SERVER: Executing PING response...\n");
	process_ping((mq_portal_t *)arg, NULL, task->msg);
	log_printf(15, "SERVER: Completed PING response.\n");
	task->msg = NULL;
}

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
	mq_command_set(table, MQF_PING_KEY, MQF_PING_SIZE, server_portal, ping); // passing the server portal to PING command
	
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

void worker_test() {
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
}

void *server_test_thread(apr_thread_t *th, void *arg) {
	// Functions for threads need to be of the form
	// void name_of_func(apr_thread_t *th, void *arg)
	server_test();
	return NULL;
}

void *client_ping_test_thread(apr_thread_t *th, void *arg) {
	client_ping_test();
	return NULL;
}

void *worker_test_thread(apr_thread_t *th, void *arg) {
	
	return NULL;
}

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
	apr_thread_t *client_thread, *server_thread;
	apr_status_t dummy;
	uint64_t n = -1;
	
	// Start the background systems
	apr_wrapper_start();
	init_opque_system();
	
	apr_pool_create(&mpool, NULL);
	
	// Initialize control fd
	control_efd = eventfd(0, 0);
	assert(control_efd != -1);
	
	// Create the two threads; server_test_thread just calls server_test in this case. Likewise for client
	thread_create_assert(&server_thread, NULL, server_test_thread, NULL, mpool);
	thread_create_assert(&client_thread, NULL, client_ping_test_thread, NULL, mpool);
	
	// Join the threads when they complete
	apr_thread_join(&dummy, client_thread);
	
	// Send a stop signal to the server so the thread completes
	n = 1;
	write(control_efd, &n, sizeof(n));
	apr_thread_join(&dummy, server_thread);
	
	// Clean up
	apr_pool_destroy(mpool);
	
	destroy_opque_system();
	apr_wrapper_stop();
	
	close(control_efd);
	
	printf("DEBUG: count = %d\n", count);
	
	return 0;
}
