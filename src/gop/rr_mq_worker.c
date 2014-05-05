/*****************************
 * Round robin worker
 *****************************/

#include "mq_portal.h"
#include "mq_stream.h"
#include "mq_ongoing.h"
#include "mq_roundrobin.h"
#include "apr_wrapper.h"
#include "log.h"
#include "type_malloc.h"
#include <sys/eventfd.h>

char *server = "tcp://127.0.0.1:6714";
char *worker_host = "tcp://127.0.0.1:6715";
char **bulk_addresses = NULL;
apr_thread_t **bulk_workers = NULL;
int num_workers = 10;
int complete = 0;
char *user_command = NULL;

int bulk_control = 0;

mq_ongoing_t *ongoing = NULL;

mq_command_table_t *table;
mq_portal_t *worker_portal;

mq_msg_t *pack_increment_msg() {
	mq_msg_t *msg = mq_msg_new();
	mq_msg_append_mem(msg, server, strlen(server), MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, MQF_EXEC_KEY, MQF_EXEC_SIZE, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, "WORKER", 6, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, MQF_INCREMENT_KEY, MQF_INCREMENT_SIZE, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
	return msg;
}

void build_send_increment(mq_context_t *mqc) {
	mq_msg_t *msg;
	mq_task_t *task;
	int err;
	op_generic_t *gop;
	
	msg = pack_increment_msg();
	gop = new_mq_op(mqc, msg, NULL, NULL, NULL, 5);
	
	log_printf(1, "WORKER: Sending INCREMENT message...\n");
	int status = gop_waitall(gop);
	
	gop_free(gop, OP_DESTROY);
	
	if(status != OP_STATE_SUCCESS)
		log_printf(1, "WORKER: Failed sending INCREMENT request to server at %s - error = %d\n", server, status);
	else
		log_printf(1, "WORKER: Successfully sent INCREMENT request to server!\n");
}

void process_trackexec_ping(mq_portal_t *p, mq_socket_t *sock, mq_msg_t *msg) {
	
	mq_frame_t *f, *id_frame;
	//char *data;
	int err;
	
	log_printf(0, "WORKER: Building response message...\n");
	
	f = mq_msg_first(msg);
	mq_frame_destroy(mq_msg_pluck(msg, 0)); // null
	mq_frame_destroy(mq_msg_pluck(msg, 0)); // version
	mq_frame_destroy(mq_msg_pluck(msg, 0)); // trackexec
	id_frame = mq_msg_pluck(msg, 0); // "HANDLE"
	mq_frame_destroy(mq_msg_pluck(msg, 0)); // ping
	//leave the null frame, mq_apple_return_address_msg() will put it on top of response
	//There should be two addresses left in the message
	
	log_printf(0, "WORKER: Address frames (should be null + 2 addresses):\n");
	display_msg_frames(msg);
	
	mq_msg_t *response = mq_msg_new();//mq_make_response_core_msg(msg, id_frame);
	mq_msg_append_mem(response, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(response, MQF_RESPONSE_KEY, MQF_RESPONSE_SIZE, MQF_MSG_KEEP_DATA);
	mq_msg_append_frame(response, id_frame);
	mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);
	
	mq_apply_return_address_msg(response, msg, 0);
	
	log_printf(0, "WORKER: Response message frames:\n");
	display_msg_frames(response);
	
	//log_printf(0, "Sleeping for a bit to see if the server fails...\n");
	//sleep(8);
	
	mq_task_t *pass_task = mq_task_new(p->mqc, response, NULL, NULL, 5);
	pass_task->pass_through = 1;
	err = mq_submit(p, pass_task);
	
	if(err != 0) {
		log_printf(0, "WORKER: Failed to send response\n");
	} else {
		log_printf(0, "WORKER: Successfully sent response!\n");
	}
	mq_msg_destroy(msg);
	
	build_send_increment(p->mqc);
}

void process_exec_ping(mq_portal_t *p, mq_socket_t *sock, mq_msg_t *msg) {
	mq_frame_t *f, *pid;
	char *data;
	int err;
	
	log_printf(4, "WORKER: Frames before destroying:\n");
	display_msg_frames(msg);
	
	f = mq_msg_first(msg);
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //blank frame
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //version frame
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //command frame (exec)
	
	pid = mq_msg_current(msg); //Message ID frame
	mq_get_frame(pid, (void **)&data, &err); // data should be "HANDLE"
	if(mq_data_compare(data, 6, "HANDLE", 6) == 0) {
		log_printf(4, "WORKER: Received correct message ID!\n");
	} else {
		log_printf(4, "WORKER: Received INCORRECT message ID!\n");
		log_printf(4, "WORKER: ID should be \"HANDLE\", ID = %s\n", data);
	}	
	
	log_printf(4, "WORKER: Destroying message...\n");
	mq_msg_destroy(msg);
	build_send_increment(p->mqc);
}

void ping(void *arg, mq_task_t *task) {
	log_printf(4, "WORKER: Executing PING response...\n");
	//determine if this is exec or trackexec
	mq_msg_t *msg = task->msg;
	mq_frame_t *exec_frame, *f;
	char *data; int size;
	
	for(f = mq_msg_first(msg); f != NULL; f = mq_msg_next(msg)) {
		mq_get_frame(f, (void **)&data, &size);
		if( mq_data_compare(data, size, MQF_EXEC_KEY, MQF_EXEC_SIZE) == 0 ) {
			log_printf(4, "WORKER: PING request is EXEC\n");
			process_exec_ping((mq_portal_t *)arg, NULL, msg);
			break;
		}
		if( mq_data_compare(data, size, MQF_TRACKEXEC_KEY, MQF_TRACKEXEC_SIZE) == 0 ) {
			log_printf(4, "WORKER: PING request is TRACKEXEC\n");
			process_trackexec_ping((mq_portal_t *)arg, NULL, msg);
			break;
		}
	}
		
	log_printf(4, "SERVER: Completed PING response.\n");
	task->msg = NULL;
}

void process_stream(mq_portal_t *p, mq_task_t *task, mq_msg_t *msg) {
	
	log_printf(0, "WORKER: Received an RR_STREAM request\n");
	log_printf(0, "WORKER: Sending back a dummy message through the server\n");
	
	mq_frame_t *f;
	char *data, *address;
	int err;
	
	log_printf(0, "WORKER: Message frames BEFORE destroying:\n");
	int i;
	for (f = mq_msg_first(msg), i=0; f != NULL; f = mq_msg_next(msg), i++) {
		mq_get_frame(f, (void **)&data, &err);
		log_printf(0, "WORKER:\tfsize[%d] = %d\n", i, err);
	}
	
	log_printf(0, "WORKER: Destroying frames...\n");
	mq_msg_first(msg);
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //blank
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //version
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //exec
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //id
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //rr_stream
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //data size
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //session id
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //blank
	
	//remaining frames are addresses
	
	log_printf(0, "WORKER: Building response...\n");
	mq_msg_t *res = mq_msg_new();
	
	f = mq_msg_first(msg); // client address
	mq_get_frame(f, (void **)&data, &err);
	address = malloc(20);   //************************************ why is this hardcoded wtf
	strncpy(address, data, 20);
	log_printf(0, "WORKER: address pulled from msg - %s\n", address);
	mq_msg_append_mem(res, address, strlen(address), MQF_MSG_KEEP_DATA);
	mq_frame_destroy(mq_msg_pop(msg));
	
	f = mq_msg_first(msg); //server address
	mq_get_frame(f, (void **)&data, &err);
	address = malloc(20);
	strncpy(address, data, 20);
	log_printf(0, "WORKER: address pulled from msg - %s\n", address);
	mq_msg_push_mem(res, address, strlen(address), MQF_MSG_KEEP_DATA); //Push server address on top
	mq_frame_destroy(mq_msg_pop(msg));
	
	mq_msg_destroy(msg); //don't care about worker address
	
	mq_msg_append_mem(res, NULL, 0, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(res, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(res, MQF_EXEC_KEY, MQF_EXEC_SIZE, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(res, "RESPONSE", 8, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(res, MQF_PING_KEY, MQF_PING_SIZE, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(res, NULL, 0, MQF_MSG_KEEP_DATA);
	
	log_printf(0, "WORKER: New message frames:\n");
	for (f = mq_msg_first(res), i=0; f != NULL; f = mq_msg_next(res), i++) {
		mq_get_frame(f, (void **)&data, &err);
		log_printf(0, "WORKER:\tfsize[%d] = %d\n", i, err);
	}
	
	log_printf(0, "WORKER: Submitting response gop...\n");
	op_generic_t *gop = new_mq_op(p->mqc, res, NULL, NULL, NULL, 5);
	int status = gop_waitall(gop);
	if(status != OP_STATE_SUCCESS) {
		log_printf(0, "WORKER: ERROR sending response message - status = %d\n", status);
	} else {
		log_printf(0, "WORKER: Sent response message successfully\n");
	}
	//gop_free(gop, OP_DESTROY);
	
	//wtf am I even doing
	
	/* 
	log_printf(0, "Artificial delay, 5 sec...\n");
	sleep(5);
	
	mq_stream_t *mqs;
	mq_frame_t *fid, *hid, *f;
	char *data, *msg_id, *msg_address;
	char *data_to_send;
	int send_size;
	int err;
	
	log_printf(0, "WORKER: Message frames BEFORE destroying:\n");
	int i;
	for (f = mq_msg_first(msg), i=0; f != NULL; f = mq_msg_next(msg), i++) {
		mq_get_frame(f, (void **)&data, &err);
		log_printf(0, "WORKER:\tfsize[%d] = %d\n", i, err);
	}
	
	log_printf(0, "WORKER: removing top frames...\n");
	f = mq_msg_first(msg);
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //blank frame
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //version frame
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //command frame (exec)
	
	log_printf(0, "WORKER: retrieving ID frame...\n");
	fid = mq_msg_first(msg); //contains ID = "HANDLE"
	mq_get_frame(fid, (void **)&data, &err);
	type_malloc(msg_id, char, err);
	strcpy(msg_id, data);
	log_printf(0, "WORKER:\tRR_STREAM request message ID: %s\n", msg_id);
	
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //ID
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //RR_STREAM
	
	f = mq_msg_first(msg);
	mq_get_frame(f, (void **)&data, &err);
	send_size = atol(data);
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //send size frame
	log_printf(4, "WORKER: Client requested %d bytes\n", send_size);
	type_malloc_clear(data_to_send, char, 0);
	
	hid = mq_msg_first(msg); //contains host ID
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //host ID
	//now top frame is null
	while((f = mq_msg_next(msg)) != NULL) {
		mq_get_frame(f, (void **)&data, &err);
		type_malloc(msg_address, char, err);
		strcpy(msg_address, data);
		log_printf(0, "WORKER:\tRR_STREAM response address: %s\n", msg_address);
		free(msg_address);
	}
	
	log_printf(0, "WORKER: Message frames AFTER destroying:\n");
	for (f = mq_msg_first(msg), i=0; f != NULL; f = mq_msg_next(msg), i++) {
		mq_get_frame(f, (void **)&data, &err);
		log_printf(0, "WORKER:\tfsize[%d] = %d\n", i, err);
	}
	
	mqs = mq_stream_write_create(task->ctx, p, ongoing, MQS_PACK_RAW, send_size*2, 10, msg, fid, hid, 0);
	
	data_to_send = malloc(send_size);
	err = mq_stream_write(mqs, data_to_send, send_size);
	if(err != 0)
		log_printf(0, "WORKER: ERROR - sending data unsuccessful!\n");
	else
		log_printf(0, "WORKER: Successfully sent %d bytes!\n", send_size);
	//mq_stream_destroy(mqs);
	*/
}

void stream(void *arg, mq_task_t *task) {	
	if(task->msg == NULL) {
		log_printf(0, "WORKER: NULL msg\n");
		return;
	}
	
	log_printf(4, "WORKER: Executing RR_STREAM response...\n");
	process_stream((mq_portal_t *)arg, task, task->msg);
	log_printf(4, "WORKER: Completed RR_STREAM response.\n");
	task->msg = NULL;
}

mq_msg_t *pack_register_msg() {
	mq_msg_t *msg = mq_msg_new();
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
	
	mq_msg_append_mem(msg, server, strlen(server), MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, MQF_TRACKEXEC_KEY, MQF_TRACKEXEC_SIZE, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, "WORKER", 6, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, MQF_REGISTER_KEY, MQF_REGISTER_SIZE, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, free_slots, strlen(free_slots), MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
	
	return msg;
}

mq_msg_t *pack_deregister_msg() {
	mq_msg_t *msg = mq_msg_new();
	
	//not sending address anymore
	
	mq_msg_append_mem(msg, server, strlen(server), MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, MQF_EXEC_KEY, MQF_EXEC_SIZE, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, "WORKER", 6, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, MQF_DEREGISTER_KEY, MQF_DEREGISTER_SIZE, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
	
	return msg;
}

op_status_t confirm_register(void *arg, int tid) {
	op_status_t status;
	mq_frame_t *frame;
	mq_task_t *task;
	mq_msg_t *msg;
	char *data;
	int err;
	
	status = op_success_status;
	task = (mq_task_t *)arg;
	msg = task->msg;
	
	log_printf(0, "WORKER: Received response to REGISTER request...\n");
	log_printf(0, "WORKER: Message received:\n");
	display_msg_frames(msg);
	
	return status;
}

void build_bulk_addresses(int n) {
	bulk_addresses = malloc(n * sizeof(char *));
	int i;
	char *base = "tcp://127.0.0.1:";
	int port = 6716;
	for(i = 0; i < n; i++) {
		type_malloc(bulk_addresses[i], char, 21);
		sprintf(bulk_addresses[i], "%s%d", base, (port + i));
	}
}

void destroy_bulk_addresses(int n) {
	int i;
	for(i = 0; i < n; i++)
		free(bulk_addresses[i]);
	free(bulk_addresses);
}

void build_send_register(mq_context_t *mqc) {
	mq_msg_t *msg;
	op_generic_t *gop;
	
	log_printf(1, "WORKER: Building message...\n");
	msg = pack_register_msg();
	
	log_printf(1, "WORKER: Creating new generic operation...\n");
	gop = new_mq_op(mqc, msg, confirm_register, NULL, NULL, 5);
	
	log_printf(1, "WORKER: Sending single REGISTER message...\n");
	int status = gop_waitall(gop);
	
	gop_free(gop, OP_DESTROY);
	
	if(status != OP_STATE_SUCCESS) {
		log_printf(1, "WORKER: Failed sending REGISTER request to server at %s - error = %d\n", server, status);
	}
	else {
		log_printf(1, "WORKER: Successfully sent REGISTER request to server!\n");
		
		log_printf(1, "WORKER: Grabbing new portal...\n");
		worker_portal = mq_portal_lookup(mqc, server, MQ_CMODE_CLIENT); //this should be the portal that was just created
		log_printf(1, "WORKER: Grabbing new table...\n");
		table = mq_portal_command_table(worker_portal);
		log_printf(1, "WORKER: Installing ping command...\n");
		mq_command_set(table, MQF_PING_KEY, MQF_PING_SIZE, worker_portal, ping);
		
		log_printf(1, "WORKER: Registration completed.\n");
	}
}

void build_send_deregister(mq_context_t *mqc) {
	mq_msg_t *msg;
	op_generic_t *gop;
	
	log_printf(1, "WORKER: Building message...\n");
	msg = pack_deregister_msg();
	
	log_printf(1, "WORKER: Creating new generic operation...\n");
	gop = new_mq_op(mqc, msg, NULL, NULL, NULL, 5);
	
	log_printf(1, "WORKER: Sending DEREGISTER message...\n");
	int status = gop_waitall(gop);
	
	gop_free(gop, OP_DESTROY);
	
	if(status != OP_STATE_SUCCESS)
		log_printf(1, "WORKER: Failed sending DEREGISTER request to server at %s - error = %d\n", server, status);
	else
		log_printf(1, "WORKER: Successfully sent DEREGISTER request to server!\n");
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
		"heartbeat_dt=120\n\t"
		"heartbeat_failure=10000\n\t"
		"min_ops_per_sec=100\n";
		// omitting socket_type for workers
	
	ifd = inip_read_text(text_parameters);
	
	mqc = mq_create_context(ifd, "mq_context");
	inip_destroy(ifd);
	
	return mqc;
}

void *start_bulk_worker_test_thread(apr_thread_t *th, void *arg) {
	int my_id = *((int *)arg);
	log_printf(0, "WORKER #%05d REPORTING IN\n", my_id);
	mq_context_t *mqc = worker_make_context();
	
	build_send_register(mqc);
	
	while(bulk_control != 1);
	
	build_send_deregister(mqc);
	
	mq_destroy_context(mqc);
	
	return NULL;
}

void start_bulk_worker_test(mq_context_t *mqc, int n) {	
	
	bulk_workers = malloc(n * sizeof(apr_thread_t*));
	
	log_printf(0, "WORKER: Creating %d worker threads for bulk test...\n", n);
	
	int i, id;
	for(i = 0; i < n; i++) {
		id = rand() % 100000;
		bulk_workers[i] = malloc(sizeof(apr_thread_t*));
		thread_create_assert(&bulk_workers[i], NULL, start_bulk_worker_test_thread, &id, mqc->mpool);
	}
	log_printf(0, "WORKER: Minions created!\n", n);
}

void end_bulk_worker_test(mq_context_t *mqc, int n) {
	apr_status_t dummy;
	bulk_control = 1;
	
	log_printf(0, "WORKER: Shutting down all bulk worker threads...\n");
	int i;
	for(i = 0; i < n; i++) {
		apr_thread_join(&dummy, bulk_workers[i]);
	}
	
	free(bulk_workers);
	log_printf(0, "WORKER: Done with bulk test.\n");
}

void worker_test() {
	mq_context_t *mqc;
	//mq_command_table_t *table;
	//mq_portal_t *worker_portal;
	flush_log();
	log_printf(1, "WORKER: Starting...\n");
	
	log_printf(1, "WORKER: Creating context...\n");
	mqc = worker_make_context();
	
	/*
	 * No longer creating a portal
	 * The worker will just use the automatically generated client portal when
	 * it sends its registration message
	 * see pack_register_msg()
	 */
	
	//Removed this 'til I know what I'm doing
	//ongoing = mq_ongoing_create(mqc, worker_portal, 5, ONGOING_SERVER);
	
	log_printf(1, "WORKER: Up and running!\n");
	type_malloc(user_command, char, 20);
	while(!complete) {
		printf("> ");
		scanf("%s", user_command);
		if(strcmp(user_command, "quit") == 0)
			complete = 1;
		else if(strcmp(user_command, "register") == 0)
			build_send_register(mqc);
		else if(strcmp(user_command, "deregister") == 0)
			build_send_deregister(mqc);
		else if(strcmp(user_command, "bulk") == 0)
			start_bulk_worker_test(mqc, num_workers);
		else if(strcmp(user_command, "endbulk") == 0)
			end_bulk_worker_test(mqc, num_workers);
	}
		
	log_printf(1, "WORKER: Shutting down...\n");
	mq_destroy_context(mqc);
	free(user_command);
	
	log_printf(1, "WORKER: Finished.\n");
}

void *worker_test_thread(apr_thread_t *th, void *arg) {
	worker_test();
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
	
	srand(time(NULL));
	
	// Thread pool, threads, and control fd
	apr_pool_t *mpool;
	apr_thread_t *worker_thread;
	apr_status_t dummy;
	
	// Start the background systems
	apr_wrapper_start();
	init_opque_system();
	
	apr_pool_create(&mpool, NULL);
	
	// Create threads for server
	thread_create_assert(&worker_thread, NULL, worker_test_thread, NULL, mpool);
	
	apr_thread_join(&dummy, worker_thread);
	
	// Clean up
	apr_pool_destroy(mpool);
	
	destroy_opque_system();
	apr_wrapper_stop();
	
	return 0;
}
