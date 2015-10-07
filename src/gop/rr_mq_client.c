/*****************************
 * Round robin client
 *****************************/

#include "mq_portal.h"
#include "mq_stream.h"
#include "mq_helpers.h"
#include "mq_ongoing.h"
#include "mq_roundrobin.h"
#include "mqs_roundrobin.h"
#include "apr_wrapper.h"
#include "log.h"
#include "type_malloc.h"
#include <sys/eventfd.h>

#define NUM_TEST		1
#define NUM_PARALLEL	10

char *server_string = "tcp://127.0.0.1:6714";
mq_msg_t *server = NULL;
char *id = "99:Random_Id";
int complete = 0;
char *user_command = NULL;
mq_ongoing_t *ongoing = NULL;

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
	mq_frame_t *fdata;
	int err;
	char *buffer;
	
	int n_read, n_left, offset, n_bytes;
	
	flush_log();
	log_printf(10, "CLIENT: Message frames BEFORE destroying:\n");
	display_msg_frames(msg);
	
	type_malloc(buffer, char, test_size);
	
	mq_remove_header(msg, 1);
	fdata = mq_msg_pop(msg);
	// Remaining frames are address frames
	
	log_printf(10, "CLIENT: Remaining message frames:\n");
	display_msg_frames(msg);
	
	mqs = mq_stream_read_create(mqc, ongoing, "STREAM_ID", 9, fdata, msg, 3);
	status = op_success_status;
	
	n_read = 0;
	n_left = TEST_SIZE;
	char *test_data = malloc(TEST_SIZE);
	memset(test_data, TEST_DATA, TEST_SIZE);
	
	while (n_left > 0) {
		
		offset=-1;
		err = mq_stream_read(mqs, &offset, sizeof(int));
		if(err != 0) {
			log_printf(0, "CLIENT: ERROR reading offset!  n_read=%d err=%d\n", n_read, err);
			status = op_failure_status;
			break;
		}
		
		if(offset > TEST_SIZE) {
			log_printf(0, "CLIENT: ERROR invalid offset=%d > %d! n_read=%d\n", offset, test_size, n_read);
			status = op_failure_status;
			break;
		}
		
		err = mq_stream_read(mqs, &n_bytes, sizeof(int));
		if(err != 0) {
			log_printf(0, "CLIENT: ERROR reading n_bytes!  n_read=%d err=%d\n", n_read, err);
			status = op_failure_status;
			break;
		}
		
		err = offset + n_bytes - 1;
		if((err > test_size) && (err >= 0)) {
			log_printf(0, "CLIENT: ERROR invalid offset+n_bytes offset=%d test=%d max is %d! n_read=%d\n", offset, n_bytes, test_size, n_read);
			status = op_failure_status;
			break;
		}
		
		memset(buffer, 0, n_bytes);
		err = mq_stream_read(mqs, buffer, n_bytes);
		if(err != 0) {
			log_printf(0, "CLIENT: ERROR reading data! n_bytes=%d but got %d n_read=%d\n", n_bytes, err, n_read);
			status = op_failure_status;
			break;
		}
		
		if(memcmp(buffer, &(test_data[offset]), n_bytes) != 0) {
			log_printf(0, "CLIENT: ERROR data mismatch! offset=%d nbytes=%d nread=%d\n", offset, n_bytes, n_read);
			status = op_failure_status;
			break;
		}
		
		n_read += n_bytes;
		n_left -= n_bytes;

		log_printf(2, "CLIENT: n_read=%d n_left=%d\n", n_read, n_left);
	}
	
	n_bytes = 1;
	log_printf(1, "CLIENT: msid = %d Before read after EOS\n", mqs->msid);
	err = mq_stream_read(mqs, buffer, n_bytes);
	log_printf(1, "CLIENT: msid = %d Attempt to read beyond EOS err = %d\n", mqs->msid, err);
	if (err == 0) {
		log_printf(0, "CLIENT: ERROR Attempt to read after EOS succeeded! err=%d msid=%d\n", err, mqs->msid);
		status = op_failure_status;
	}
	
	if(status.op_status == OP_STATE_FAILURE) {
		log_printf(0, "CLIENT: ERROR - Did not receive %d bytes successfully, err = %d\n", TEST_SIZE, err);
	}
	else
		log_printf(10, "CLIENT: Successfully received %d bytes!\n", TEST_SIZE);
		
	err = mqs->msid;
	mq_stream_destroy(mqs);
	mq_frame_destroy(fdata);
	free(buffer);
	free(test_data);
	
	return status;
}

mq_msg_t *pack_stream_msg() {
	char *data_size = malloc(5);
	sprintf(data_size, "%d", TEST_SIZE);
	
	log_printf(15, "CLIENT: Building message...\n");
	mq_msg_t *msg = mq_make_exec_core_msg(server, 1);
	mq_msg_append_mem(msg, MQF_RR_STREAM_KEY, MQF_RR_STREAM_SIZE, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, "STREAM_ID", 9, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, data_size, strlen(data_size), MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
	
	return msg;
}

void build_send_data(mq_context_t *mqc) {
	op_generic_t *gop;
	mq_msg_t *msg;
	
	msg = pack_stream_msg();
	
	log_printf(15, "CLIENT: Creating new generic operation...\n");
	gop = new_mq_op(mqc, msg, read_stream, mqc, NULL, 10);
	
	log_printf(15, "CLIENT: Sending message...\n");
	int status = gop_waitall(gop);
	
	if(status != OP_STATE_SUCCESS)
		log_printf(0, "CLIENT: Failed sending RR_STREAM request to server at %s - error = %d\n", server, status);
	else
		log_printf(10, "CLIENT: Successfully sent RR_STREAM request to server!\n");
	
	gop_free(gop, OP_DESTROY);
}

op_status_t receive_pong(void *arg, int tid) {
	op_status_t status;
	mq_task_t *task;
	mq_msg_t *msg;
	mq_frame_t *f;
	char *data, b64[1024];
	int size;
	
	log_printf(15, "CLIENT: Received response...\n");
	
	status = op_success_status;
	task = (mq_task_t *)arg;
	msg = task->msg;
	
	log_printf(10, "CLIENT: Reponse message frames:\n");
	display_msg_frames(msg);
	
	mq_remove_header(msg, 1);
	f = mq_msg_pluck(msg, 0); // ID
	mq_get_frame(f, (void **)&data, &size);
	mq_frame_destroy(f);
	log_printf(1, "CLIENT: Successfully received ID: %s\n", mq_id2str(data, size, b64, sizeof(b64)));
	
	return status;
}

mq_msg_t *pack_ping_msg(int track) {
	mq_msg_t *msg;
	uint64_t *id;
	
	msg = mq_msg_new();
	mq_msg_append_msg(msg, server, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
	
	if(track == 0)
		mq_msg_append_mem(msg, MQF_EXEC_KEY, MQF_EXEC_SIZE, MQF_MSG_KEEP_DATA);
	else
		mq_msg_append_mem(msg, MQF_TRACKEXEC_KEY, MQF_TRACKEXEC_SIZE, MQF_MSG_KEEP_DATA);
	
	id = malloc(sizeof(uint64_t));
	*id = atomic_global_counter();
	mq_msg_append_mem(msg, id, sizeof(uint64_t), MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, MQF_PING_KEY, MQF_PING_SIZE, MQF_MSG_KEEP_DATA);
	mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
	
	return msg;
}

int send_ping(mq_context_t *mqc, int track) {
	op_generic_t *gop;
	mq_msg_t *msg;
	int result;
	
	log_printf(15, "CLIENT: Building message...\n");
	msg = pack_ping_msg(track);
	log_printf(15, "CLIENT: Message to send:\n");
	display_msg_frames(msg);
	
	log_printf(15, "CLIENT: Creating new gop...\n");
	if(track == 0)
		gop = new_mq_op(mqc, msg, NULL, NULL, NULL, 15); //EXEC, no callback
	else
		gop = new_mq_op(mqc, msg, receive_pong, NULL, NULL, 15); //TRACKEXEC, callback is receive_pong
	
	log_printf(15, "CLIENT: Sending message...\n");
	result = 0;
	int status = gop_waitall(gop);
	
	if(status != OP_STATE_SUCCESS)
		log_printf(0, "CLIENT: Failed sending PING request to server at %s - error = %d\n", server, status);
	else {
		log_printf(15, "CLIENT: Successfully sent PING request to server!\n");
		result = 1;
	}
		
	gop_free(gop, OP_DESTROY);
	return result;
}

void stream_data(mq_context_t *mqc) {
	build_send_data(mqc);	
}

mq_context_t *client_make_context() {
	mq_context_t *mqc;
	inip_file_t *ifd;
	char *text_parameters = "[mq_context]\n"
		"min_conn=1\n"
		"max_conn=4\n"
		"min_threads=2\n"
		"max_threads=%d\n"
		"backlog_trigger=1000\n"
		"heartbeat_dt=120\n"
		"heartbeat_failure=1000\n"
		"min_ops_per_sec=100\n"
		"socket_type=1002\n"; // Set socket type to MQF_ROUND_ROBIN
	
	char buffer[1024];
	snprintf(buffer, sizeof(buffer), text_parameters, nparallel);
	ifd = inip_read_text(buffer);
	
	mqc = mq_create_context(ifd, "mq_context");
	inip_destroy(ifd);
	
	return mqc;
}

op_generic_t *create_bulk_ping(mq_context_t *mqc, int id) {
	op_generic_t *gop;
	mq_msg_t *msg;
	
	msg = pack_ping_msg(1);
	
	gop = new_mq_op(mqc, msg, receive_pong, NULL, NULL, 5);
	
	return gop;
}

op_generic_t *create_bulk_stream(mq_context_t *mqc, int id) {
	op_generic_t *gop;
	mq_msg_t *msg;
	
	msg = pack_stream_msg();
	
	gop = new_mq_op(mqc, msg, read_stream, NULL, NULL, 5);
	
	return gop;
}

void bulk_stream_test(mq_context_t *mqc, int num) {
	int stream_result, i, n_done;
	opque_t *q = NULL;
	op_generic_t *gop = NULL;
	
	log_printf(1, "CLIENT: Starting bulk STREAM test\n");
	log_printf(1, "CLIENT: %d tests\n", num);
	
	q = new_opque();
	stream_result = 0; n_done = 0;
	
	stream_result = 0; n_done = 0;
	for(i = 0; i < num; i++) {
		gop = create_bulk_stream(mqc, i);
		opque_add(q, gop);
	}
	
	log_printf(1, "CLIENT: Submitted all STREAM jobs; opque holds %d\n", opque_task_count(q));
	
	while((gop = opque_waitany(q)) != NULL) {
		stream_result += (gop_get_status(gop).op_status == OP_STATE_SUCCESS) ? 1 : 0;
		gop_free(gop, OP_DESTROY);
		n_done++;
		log_printf(1, "CLIENT: STREAM %d of %d completed------------------\n", n_done, num);
	}
	
	opque_free(q, OP_DESTROY);
	
	log_printf(0, "CLIENT: BULK TEST DONE - RESULTS:\n");
	log_printf(0, "type\t# total\t# success\n");
	log_printf(0, "----\t-------\t---------\n");
	log_printf(0, "STREAM\t%7d\t%9d\n", num, stream_result);
}

void bulk_ping_test(mq_context_t *mqc, int num) {
	int ping_result, i, n_done;
	opque_t *q = NULL;
	op_generic_t *gop = NULL;
	
	log_printf(1, "CLIENT: Starting bulk PING test\n");
	log_printf(1, "CLIENT: %d tests\n", num);
	
	q = new_opque();
	ping_result = 0; n_done = 0;
	for(i = 0; i < num; i++) {
		gop = create_bulk_ping(mqc, i); //only sends TRACKEXEC PING
		opque_add(q, gop);
		if(i >= NUM_PARALLEL - 1) {
			gop = opque_waitany(q);
			ping_result += (gop_get_status(gop).op_status == OP_STATE_SUCCESS) ? 1 : 0;
			gop_free(gop, OP_DESTROY);
			n_done++;
			log_printf(1, "CLIENT: %d of %d completed--------------------\n", n_done, num);
		}
	}
	
	log_printf(1, "CLIENT: Submitted all PING jobs; opque holds %d\n", opque_task_count(q));
	
	while((gop = opque_waitany(q)) != NULL) {
		ping_result += (gop_get_status(gop).op_status == OP_STATE_SUCCESS) ? 1 : 0;
		gop_free(gop, OP_DESTROY);
		n_done++;
		log_printf(1, "CLIENT: PING %d of %d completed--------------------\n", n_done, num);
	}
	
	
	
	opque_free(q, OP_DESTROY);
	
	log_printf(0, "CLIENT: BULK TEST DONE - RESULTS:\n");
	log_printf(0, "type\t# total\t# success\n");
	log_printf(0, "----\t-------\t---------\n");
	log_printf(0, "PING\t%7d\t%9d\n", num, ping_result);
	
}

void client_test() {
	mq_context_t *mqc;
	
	flush_log();
	log_printf(1, "CLIENT: Starting...\n");
	
	log_printf(15, "CLIENT: Creating context...\n");
	mqc = client_make_context();
	
	ongoing = mq_ongoing_create(mqc, NULL, ongoing_client_interval, ONGOING_CLIENT);
	
	log_printf(1, "CLIENT: Up and running!\n");
	type_malloc(user_command, char, 20);
	int num;
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
		else if(strcmp(user_command, "bulkping") == 0) {
			scanf("%d", &num);
			bulk_ping_test(mqc, num);
		}
		else if(strcmp(user_command, "bulkdata") == 0) {
			scanf("%d", &num);
			bulk_stream_test(mqc, num);
		}
	}
	
	log_printf(1, "CLIENT: Shutting down...\n");
	mq_ongoing_destroy(ongoing);
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
	
	srand(time(NULL));
	
	// Thread pool and threads
	apr_pool_t *mpool;
	apr_thread_t *client_thread;
	apr_status_t dummy;
	
	// Start the background stuff
	apr_wrapper_start();
	init_opque_system();
	
	apr_pool_create(&mpool, NULL);

        server = mq_string_to_address(server_string);
	
	// Start the main thread
	thread_create_assert(&client_thread, NULL, client_test_thread, NULL, mpool);
	
	apr_thread_join(&dummy, client_thread);
	
	// Clean up
	apr_pool_destroy(mpool);
	
	destroy_opque_system();
	apr_wrapper_stop();
	
	return 0;
}
