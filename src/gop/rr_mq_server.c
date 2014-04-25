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

void process_round_robin_pass(mq_portal_t *p, mq_socket_t *sock, mq_msg_t *msg) {
	unsigned int num_workers;
	mq_worker_t *worker;
	char *worker_address;
	mq_frame_t *address_frame;
	op_generic_t *gop;
	mq_worker_table_t *worker_table;
	
	// Just for testing. Don't keep this
	if(p->implementation_arg != NULL)
		worker_table = (mq_worker_table_t *)p->implementation_arg;
	else {
		log_printf(1, "SERVER: ERROR - no worker table found\n");
		return;
	}
	num_workers = apr_hash_count(worker_table->table);
	if(num_workers != 1) {
		log_printf(1, "SERVER: ERROR - number of workers != 1, num_workers = %d\n", num_workers);
		log_printf(1, "SERVER: Destroying message and quitting response.\n");
		mq_msg_destroy(msg);
		return;
	}
	
	apr_hash_index_t *index = apr_hash_first(worker_table->mpool, worker_table->table);
	apr_hash_this(index, NULL, NULL, (void **)&worker);
	worker_address = worker->address;
	log_printf(1, "SERVER: Grabbed worker with address %s, length = %d\n", worker_address, strlen(worker_address));
	address_frame = mq_frame_new(worker_address, strlen(worker_address), MQF_MSG_KEEP_DATA); //new frame with this address
	mq_msg_push_frame(msg, address_frame);
	
	log_printf(0, "SERVER: Message before sending\n");
	mq_frame_t *f; int size, i; char *data;
	for (f = mq_msg_first(msg), i=0; f != NULL; f = mq_msg_next(msg), i++) {
		mq_get_frame(f, (void **)&data, &size);
		log_printf(4, "SERVER:\tfsize[%d] = %d\n", i, size);
	}
	
	gop = new_mq_op(p->mqc, msg, NULL, NULL, NULL, 5);
	int status = gop_waitall(gop);
	
	if(status != OP_STATE_SUCCESS) {
		log_printf(1, "SERVER: FAILED to send message to worker!\n");
	} else {
		log_printf(1, "SERVER: Successfully sent message to worker!\n");
	}
	
	//freeing gop or deleting msg results in a segfault
	//I guess msg is being deleted automatically later on...
	//but I feel like leaving the gop here is a memory leak
	//gop_free(gop, OP_DESTROY);
	//log_printf(0, "SERVER: Destroyed gop\n");
}

void process_ping(mq_portal_t *p, mq_socket_t *sock, mq_msg_t *msg) {
	mq_frame_t *f, *pid;
	char *data;
	int err;
	
	log_printf(5, "SERVER: Message frames BEFORE destroying:\n");
	int i;
	for (f = mq_msg_first(msg), i=0; f != NULL; f = mq_msg_next(msg), i++) {
		mq_get_frame(f, (void **)&data, &err);
		log_printf(5, "SERVER:\tfsize[%d] = %d\n", i, err);
	}
	
	f = mq_msg_first(msg);
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //blank frame
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //version frame
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //command frame (exec)
	
	pid = mq_msg_current(msg); //Message ID frame
	mq_get_frame(pid, (void **)&data, &err); // data should be "HANDLE"
	if(mq_data_compare(data, 6, "HANDLE", 6) == 0) {
		log_printf(10, "SERVER: Received correct message ID!\n");
	} else {
		log_printf(10, "SERVER: Received INCORRECT message ID!\n");
		log_printf(10, "SERVER: ID should be \"HANDLE\", ID = %s\n", data);
	}	
	
	log_printf(15, "SERVER: Destroying message...\n");
	mq_msg_destroy(msg); //Can't use this function in "thread" version of the handler, i.e., the method that is linked in the command table
}

void process_register_worker(mq_portal_t *p, mq_socket_t *sock, mq_msg_t *msg) {
	mq_frame_t *f;
	char *data;
	char *worker_address;
	int worker_free_slots;
	int err;
	
	log_printf(15, "SERVER: Message frames BEFORE destroying:\n");
	int i;
	for (f = mq_msg_first(msg), i=0; f != NULL; f = mq_msg_next(msg), i++) {
		mq_get_frame(f, (void **)&data, &err);
		log_printf(15, "SERVER:\tfsize[%d] = %d\n", i, err);
	}
	
	f = mq_msg_first(msg);
	//Assume these are good
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //blank frame
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //version frame
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //command frame (EXEC)
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //ID frame ("WORKER")
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //argument frame (REGISTER)
	
	f = mq_msg_current(msg);
	mq_get_frame(f, (void **)&data, &err);
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //address frame
	type_malloc(worker_address, char, err + 1);
	memcpy(worker_address, data, err);
	log_printf(4, "SERVER: Received REGISTER from address %s\n", worker_address);
	
	f = mq_msg_current(msg);
	mq_get_frame(f, (void **)&data, &err);
	worker_free_slots = atol(data);
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //slots frame
	log_printf(4, "SERVER: Worker at %s has free_slots = %d, size = %d\n", worker_address, worker_free_slots, err);
	
	mq_msg_destroy(msg);
	
	mq_register_worker(p->implementation_arg, worker_address, worker_free_slots);
}

void process_deregister_worker(mq_portal_t *p, mq_socket_t *sock, mq_msg_t *msg) {
	mq_frame_t *f;
	char *data;
	char *worker_address;
	int err;
	mq_worker_table_t *table;
	mq_worker_t *worker;
	
	f = mq_msg_first(msg);
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //blank frame
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //version frame
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //command frame (EXEC)
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //ID frame ("WORKER")
	mq_frame_destroy(mq_msg_pluck(msg, 0)); //argument frame (DEREGISTER)
	f = mq_msg_current(msg);
	
	mq_get_frame(f, (void **)&data, &err);
	type_malloc(worker_address, char, err+1);
	memcpy(worker_address, data, err);
	log_printf(1, "SERVER: Attempting to delete worker with address %s\n", worker_address);
	
	table = p->implementation_arg;
	if(!table->table) {
		log_printf(1, "SERVER: ERROR - no worker table exists!\n");
		return;
	}
	if((worker = apr_hash_get(table->table, worker_address, err)) == NULL) {
		log_printf(1, "SERVER: ERROR - no worker found with address %s\n", worker_address);
		return;
	}
	apr_hash_set(table->table, worker_address, err, NULL);
	log_printf(1, "SERVER: Deleted worker with address %s\n", worker_address);
}

void display_worker_table(mq_worker_table_t *table) {
	apr_hash_index_t *hi;
	mq_worker_t* worker;
	void *val;
	int i;
	
	for(hi = apr_hash_first(table->mpool, table->table), i = 0; hi != NULL; hi = apr_hash_next(hi), i++) {
		apr_hash_this(hi, NULL, NULL, &val);
		worker = (mq_worker_t *)val;
		log_printf(1, "SERVER: worker_table[%d] = { address = %s free_slots = %d }\n", i, worker->address, worker->free_slots);
	}
}

/*
 * Hooks
 */

void pass_through(void *arg, mq_task_t *task) {
	mq_portal_t *p = (mq_portal_t *)arg;
	mq_msg_t *msg = task->msg;
	mq_frame_t *f = mq_msg_first(msg);
	char *data; int size;
	mq_get_frame(f, (void **)&data, &size);
	/*
	int i;
	for (f = mq_msg_first(msg), i=0; f != NULL; f = mq_msg_next(msg), i++) {
		mq_get_frame(f, (void **)&data, &size);
		log_printf(4, "SERVER:\tfsize[%d] = %d\n", i, size);
		if(size >= 20)
			log_printf(0, "SERVER: \t\taddress found: %s\n", data);
	}
	*/
	if(size == 0) {
		log_printf(4, "SERVER: Deferring message to worker...\n");
		process_round_robin_pass(p, NULL, msg);
		log_printf(4, "SERVER: Sent message to worker.\n");
	}	
	else {
		int status;
		if(task->gop != NULL) {
			status = gop_waitall(task->gop);
			//gop_free(gop, OP_DESTROY);
			//msg = NULL;
		}
		else {
			int i;
			for (f = mq_msg_first(msg), i=0; f != NULL; f = mq_msg_next(msg), i++) {
				mq_get_frame(f, (void **)&data, &size);
				log_printf(4, "SERVER:\tfsize[%d] = %d\n", i, size);
				if(size >= 20)
					log_printf(0, "SERVER: \t\taddress found: %s\n", data);
			}
			op_generic_t *gop = new_mq_op(p->mqc, msg, NULL, NULL, NULL, 5);
			status = gop_waitall(gop);
		}
		
		//not enough if/else statements!
		//if (this works) { awesome; } else { goddamnit; }
		if(status != OP_STATE_SUCCESS)
			log_printf(1, "SERVER: FAILED to pass message. status = %d\n", status);
		else
			log_printf(1, "SERVER: Successfully passed message\n");
	}
	
	log_printf(0, "SERVER: End of pass_through()\n");
}

void ping(void *arg, mq_task_t *task) {
	log_printf(4, "SERVER: Executing PING response...\n");
	process_ping((mq_portal_t *)arg, NULL, task->msg);
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
	
	log_printf(1, "SERVER: Starting...\n");
	
	log_printf(1, "SERVER: Creating context...\n");
	mqc = server_make_context();
	
	log_printf(1, "SERVER: Creating portal...\n");
	server_portal = mq_portal_create(mqc, host, MQ_CMODE_SERVER);
	
	log_printf(1, "SERVER: Creating command table...\n");
	table = mq_portal_command_table(server_portal);
	
	log_printf(1, "SERVER: Installing commands...\n");
	//mq_command_set(table, MQF_PING_KEY, MQF_PING_SIZE, server_portal, ping); // If I skip this, PING should end up going to a worker
	mq_command_set(table, MQF_REGISTER_KEY, MQF_REGISTER_SIZE, server_portal, register_worker);
	mq_command_set(table, MQF_DEREGISTER_KEY, MQF_DEREGISTER_SIZE, server_portal, deregister_worker);
	mq_command_table_set_default(table, server_portal, pass_through); // default function to call when a message isn't one of the above
	
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
	// Functions for threads need to be of the form
	// void name_of_func(apr_thread_t *th, void *arg)
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
	
	// Thread pool, threads, and control fd
	apr_pool_t *mpool;
	apr_thread_t *server_thread;
	apr_status_t dummy;
	
	// Start the background systems
	apr_wrapper_start();
	init_opque_system();
	
	apr_pool_create(&mpool, NULL);
	
	// Create threads for server
	thread_create_assert(&server_thread, NULL, server_test_thread, NULL, mpool);
	
	apr_thread_join(&dummy, server_thread);
	
	// Clean up
	apr_pool_destroy(mpool);
	
	destroy_opque_system();
	apr_wrapper_stop();
	
	return 0;
}
