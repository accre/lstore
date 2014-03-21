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

char *server = "tcp://127.0.0.1:6714";
char *address = "tcp://127.0.0.1:6713";
char *worker = "tcp://127.0.0.1:6715";
char *id = "99:Random_Id";
int complete = 0;
char *user_command = NULL;
mq_ongoing_t *ongoing = NULL;

int send_size = 8192;

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
	int i;
	for (f = mq_msg_first(msg), i=0; f != NULL; f = mq_msg_next(msg), i++) {
		mq_get_frame(f, (void **)&data, &err);
		log_printf(4, "CLIENT:\tfsize[%d] = %d\n", i, err);
	}
	
	status = op_success_status;
	mq_remove_header(msg, 0);
	f = mq_msg_first(msg);
	mq_get_frame(f, (void **)&data, &err);
	char *handle = malloc(err);
	strncpy(handle, data, err);
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
	mq_msg_append_mem(msg, address, strlen(address), MQF_MSG_KEEP_DATA);
	
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

void build_send_ping(mq_context_t *mqc) {
	op_generic_t *gop;
	mq_msg_t *msg;
	
	log_printf(1, "CLIENT: Building message...\n");
	msg = mq_msg_new();
	mq_msg_append_mem(msg, server, strlen(server), MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, MQF_EXEC_KEY, MQF_EXEC_SIZE, MQF_MSG_KEEP_DATA); // MQF_EXEC_KEY is the messages's command
	mq_msg_append_mem(msg, "HANDLE", 6, MQF_MSG_KEEP_DATA); // "HANDLE" is the message's ID
	mq_msg_append_mem(msg, MQF_PING_KEY, MQF_PING_SIZE, MQF_MSG_KEEP_DATA); // MQF_PING_KEY is the message's first argument under MQF_EXEC_KEY
	mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
	
	log_printf(1, "CLIENT: Creating new generic operation...\n");
	gop = new_mq_op(mqc, msg, NULL, NULL, NULL, 5);
	
	log_printf(1, "CLIENT: Sending message...\n");
	int status = gop_waitall(gop);
	
	if(status != OP_STATE_SUCCESS)
		log_printf(1, "CLIENT: Failed sending PING request to server at %s - error = %d\n", server, status);
	else
		log_printf(1, "CLIENT: Successfully sent PING request to server!\n");
		
	gop_free(gop, OP_DESTROY);
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
		"heartbeat_dt=1\n\t"
		"heartbeat_failure=10\n\t"
		"min_ops_per_sec=100\n\t"
		"socket_type=1002\n"; // Set socket type to MQF_ROUND_ROBIN
	
	ifd = inip_read_text(text_parameters);
	
	mqc = mq_create_context(ifd, "mq_context");
	inip_destroy(ifd);
	
	return mqc;
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
		else if(strcmp(user_command, "ping") == 0)
			build_send_ping(mqc);
		else if(strcmp(user_command, "data") == 0)
			stream_data(mqc);
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
	
	// Thread pool, threads, and control fd
	apr_pool_t *mpool;
	apr_thread_t *client_thread;
	apr_status_t dummy;
	
	// Start the background systems
	apr_wrapper_start();
	init_opque_system();
	
	apr_pool_create(&mpool, NULL);
	
	// Create threads for server
	thread_create_assert(&client_thread, NULL, client_test_thread, NULL, mpool);
	
	apr_thread_join(&dummy, client_thread);
	
	// Clean up
	apr_pool_destroy(mpool);
	
	destroy_opque_system();
	apr_wrapper_stop();
	
	return 0;
}
