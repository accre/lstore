/*****************************
 * Round robin client
 *****************************/

#include "mq_portal.h"
#include "mq_stream.h"
#include "mq_helpers.h"
#include "mq_ongoing.h"
#include "mq_roundrobin.h"
#include "apr_wrapper.h"
#include "log.h"
#include "type_malloc.h"
#include <sys/eventfd.h>

#define NUM_TEST 100

char *server = "tcp://127.0.0.1:6714";
char *id = "99:Random_Id";
int complete = 0;
char *user_command = NULL;
mq_ongoing_t *ongoing = NULL;
apr_thread_t **bulk_clients = NULL;

int send_size = 8192;

typedef struct {
	int id;
	int results;
	int track;
	// Can later add values for timeouts, should_fail, etc
} test_param_t;

op_status_t read_stream(void *arg, int tid) {
	
	mq_task_t *task = (mq_task_t *)arg;
	mq_context_t *mqc = task->arg;
	mq_msg_t *msg = task->response;
	mq_stream_t *mqs;
	op_status_t status;
	int err;
	char *buffer, *data;
	mq_frame_t *f;
	
	flush_log();
	log_printf(4, "CLIENT: Message frames BEFORE destroying:\n");
	display_msg_frames(msg);
	
	status = op_success_status;
	mq_remove_header(msg, 0);
	f = mq_msg_first(msg);
	mq_get_frame(f, (void **)&data, &err);
	char *handle = malloc(err + 1);
	strncpy(handle, data, err);
	handle[err] = '\0';
	if( strcmp(data, "RESPONSE") != 0 )
		status = op_failure_status;
	
	/*
	status = op_success_status;
	mq_remove_header(msg, 1);
	mqs = mq_stream_read_create(mqc, ongoing, id, strlen(id), mq_msg_first(msg), server, 10);
	
	buffer = malloc(send_size);
	err = mq_stream_read(mqs, buffer, send_size);
	if(err != 0) {
		log_printf(0, "CLIENT: ERROR receiving data stream, err = %d\n", err);
		status = op_failure_status;
	}
	else
		log_printf(0, "CLIENT: Retrieved %d bytes of data!\n", send_size);
	
	mq_stream_destroy(mqs);
	free(buffer);
	*/
	return status;
}

void build_send_data(mq_context_t *mqc) {
	op_generic_t *gop;
	mq_msg_t *msg;
	char *s = malloc(5);
	sprintf(s, "%d", send_size);
	
	log_printf(1, "CLIENT: Building message...\n");
	msg = mq_msg_new();
	mq_msg_append_mem(msg, server, strlen(server), MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, MQF_EXEC_KEY, MQF_EXEC_SIZE, MQF_MSG_KEEP_DATA); 
	mq_msg_append_mem(msg, "HANDLE", 6, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, MQF_RR_STREAM_KEY, MQF_RR_STREAM_SIZE, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, s, strlen(s), MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, id, strlen(id) + 1, MQF_MSG_KEEP_DATA);  // Should switch this with prev frame
	mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
	
	log_printf(1, "CLIENT: Creating new generic operation...\n");
	gop = new_mq_op(mqc, msg, read_stream, mqc, NULL, 10);
	
	log_printf(1, "CLIENT: Sending message...\n");
	int status = gop_waitall(gop);
	
	if(status != OP_STATE_SUCCESS)
		log_printf(1, "CLIENT: Failed sending RR_STREAM request to server at %s - error = %d\n", server, status);
	else
		log_printf(1, "CLIENT: Successfully sent RR_STREAM request to server!\n");
	
	//gop_free(gop, OP_DESTROY);
}

op_status_t receive_pong(void *arg, int tid) {
	op_status_t status;
	mq_task_t *task;
	mq_msg_t *msg;
	mq_frame_t *f;
	char *data;
	int size;
	
	log_printf(0, "CLIENT: Received response...\n");
	
	status = op_success_status;
	task = (mq_task_t *)arg;
	msg = task->msg;
	
	//Parse the message and double check it's right
	
	log_printf(4, "CLIENT: Reponse message frames:\n");
	display_msg_frames(msg);
	
	mq_msg_first(msg);
	mq_frame_destroy(mq_msg_pluck(msg, 0)); // server address
	mq_frame_destroy(mq_msg_pluck(msg, 0)); // null
	mq_frame_destroy(mq_msg_pluck(msg, 0)); // version
	mq_frame_destroy(mq_msg_pluck(msg, 0)); // trackexec
	f = mq_msg_pluck(msg, 0); // ID
	mq_get_frame(f, (void **)&data, &size);
	if( mq_data_compare(data, size, "HANDLE", 6) != 0 ) {
		//this should never happen, because the ID frame needs to match
		//for this function to have been called in the first place
		log_printf(0, "CLIENT: ID in response is incorrect! This shouldn't have happened! ID = %s, size = %d\n", data, size);
		status = op_failure_status;
	}
	else {
		log_printf(0, "CLIENT: Successfully received ID: %s\n", data);
	}
	
	return status;
}

int send_ping(mq_context_t *mqc, int track) {
	op_generic_t *gop;
	mq_msg_t *msg;
	int result;
	
	log_printf(1, "CLIENT: Building message...\n");
	msg = mq_msg_new();
	mq_msg_append_mem(msg, server, strlen(server), MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
	
	if(track == 0)
		mq_msg_append_mem(msg, MQF_EXEC_KEY, MQF_EXEC_SIZE, MQF_MSG_KEEP_DATA);
	else
		mq_msg_append_mem(msg, MQF_TRACKEXEC_KEY, MQF_TRACKEXEC_SIZE, MQF_MSG_KEEP_DATA);
	
	mq_msg_append_mem(msg, "HANDLE", 6, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, MQF_PING_KEY, MQF_PING_SIZE, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
	
	log_printf(0, "CLIENT: Message to send:\n");
	display_msg_frames(msg);
	
	log_printf(1, "CLIENT: Creating new gop...\n");
	if(track == 0)
		gop = new_mq_op(mqc, msg, NULL, NULL, NULL, 15); //EXEC, no callback
	else
		gop = new_mq_op(mqc, msg, receive_pong, NULL, NULL, 15); //TRACKEXEC, callback is receive_pong
	
	log_printf(1, "CLIENT: Sending message...\n");
	result = 0;
	int status = gop_waitall(gop);
	
	if(status != OP_STATE_SUCCESS)
		log_printf(1, "CLIENT: Failed sending PING request to server at %s - error = %d\n", server, status);
	else {
		log_printf(1, "CLIENT: Successfully sent PING request to server!\n");
		result = 1;
	}
		
	gop_free(gop, OP_DESTROY);
	return result;
}

void stream_data(mq_context_t *mqc) {
	ongoing = mq_ongoing_create(mqc, NULL, 1, ONGOING_CLIENT);
	build_send_data(mqc);
	mq_ongoing_destroy(ongoing);	
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
		"heartbeat_dt=120\n\t"
		"heartbeat_failure=1000\n\t"
		"min_ops_per_sec=100\n\t"
		"socket_type=1002\n"; // Set socket type to MQF_ROUND_ROBIN
	
	ifd = inip_read_text(text_parameters);
	
	mqc = mq_create_context(ifd, "mq_context");
	inip_destroy(ifd);
	
	return mqc;
}

void *bulk_client_test_thread(apr_thread_t *th, void *arg) {
	test_param_t *tp = (test_param_t *)arg;
	mq_context_t *mqc = client_make_context();
	
	log_printf("BULK CLIENT: id = %d\n", tp->id);
	
	tp->results = send_ping(mqc, tp->track);
	
	mq_destroy_context(mqc);
	
	return NULL;
}

void bulk_test(mq_context_t *mqc) {
	apr_status_t dummy;
	bulk_clients = malloc(NUM_TEST * sizeof(apr_thread_t*));
	
	test_param_t **bulk_params = malloc(NUM_TEST * sizeof(test_param_t*));
	int results = 0;
	
	log_printf(0, "CLIENT: Creating %d client threads for bulk test...\n", NUM_TEST);
	
	int i;
	for(i = 0; i < NUM_TEST; i++) {
		bulk_clients[i] = malloc(sizeof(apr_thread_t*));
		bulk_params[i] = malloc(sizeof(test_param_t*));
		bulk_params[i]->id = i;
		bulk_params[i]->results = 0;
		bulk_params[i]->track = 0;
		thread_create_assert(&bulk_clients[i], NULL, bulk_client_test_thread, bulk_params[i], mqc->mpool);
	}
	
	log_printf(0, "CLIENT: Joining client threads...\n");
	for(i = 0; i < NUM_TEST; i++) {
		apr_thread_join(&dummy, bulk_clients[i]);
		results += bulk_params[i]->results;
	}
	
	free(bulk_clients);
	free(bulk_params);
	
	log_printf(0, "CLIENT: Done with bulk test\n");
	log_printf(0, "CLIENT: EXEC PING requests:\n");
	log_printf(0, "CLIENT:\tsent: %d\tsuccess: %d\n");
	
}

void client_test() {
	mq_context_t *mqc;
	
	flush_log();
	log_printf(1, "CLIENT: Starting...\n");
	
	log_printf(1, "CLIENT: Creating context...\n");
	mqc = client_make_context();
	
	//ongoing = mq_ongoing_create(mqc, NULL, 1, ONGOING_CLIENT);
	
	log_printf(1, "CLIENT: Up and running!\n");
	type_malloc(user_command, char, 20);
	while(!complete) {
		printf("> ");
		scanf("%s", user_command);
		if(strcmp(user_command, "quit") == 0)
			complete = 1;
		else if(strcmp(user_command, "pingt") == 0)
			send_ping(mqc, 1);
		else if(strcmp(user_command, "ping") == 0)
			send_ping(mqc, 0);
		else if(strcmp(user_command, "data") == 0)
			stream_data(mqc);
		else if(strcmp(user_command, "bulk") == 0)
			bulk_test(mqc);
	}
	
	log_printf(1, "CLIENT: Shutting down...\n");
	mq_destroy_context(mqc);
	free(user_command);
	
	log_printf(1, "CLIENT: Finished.\n");
}

void *client_test_thread(apr_thread_t *th, void *arg) {
	client_test();
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
	apr_thread_t *client_thread;
	apr_status_t dummy;
	
	// Start the background stuff
	apr_wrapper_start();
	init_opque_system();
	
	apr_pool_create(&mpool, NULL);
	
	// Start the main thread
	thread_create_assert(&client_thread, NULL, client_test_thread, NULL, mpool);
	
	apr_thread_join(&dummy, client_thread);
	
	// Clean up
	apr_pool_destroy(mpool);
	
	destroy_opque_system();
	apr_wrapper_stop();
	
	return 0;
}
