#include "mq_portal.h"
#include "mq_roundrobin.h"
#include "type_malloc.h"
#include "log.h"

mq_worker_t *mq_worker_table_first(mq_worker_table_t *table) {
	move_to_top(table);
	return (mq_worker_t *)get_ele_data(table);
}

mq_worker_t *mq_worker_table_next(mq_worker_table_t *table) {
	move_down(table);
	return (mq_worker_t *)get_ele_data(table);
}

// mq_worker_create()
// Create a worker and fill its parameters

mq_worker_t *mq_worker_create(char *address, int free_slots) {
	
	mq_worker_t *worker;
	type_malloc(worker, mq_worker_t, 1);
	
	worker->address = address;
	worker->free_slots = free_slots;
	log_printf(5, "worker created with address = %s\tslots = %d\n", address, free_slots);
	
	return worker;
}

// mq_worker_table_create()
// Create an empty table for workers

mq_worker_table_t *mq_worker_table_create() {
	
	/*
	mq_worker_table_t *table;
	
	type_malloc(table, mq_worker_table_t, 1);
	apr_pool_create(&(table->mpool), NULL);
	assert(apr_thread_mutex_create(&(table->lock), APR_THREAD_MUTEX_DEFAULT, table->mpool) == APR_SUCCESS);
	assert((table->table = apr_hash_make(table->mpool)) != NULL);
	log_printf(5, "Created worker table\n");
	*/
	
	return (new_stack());
}

// mq_worker_table_destroy()
// Destroy the table and free memory

void mq_worker_table_destroy(mq_worker_table_t *table) {	
	log_printf(5, "Destroying worker table\n");
	//apr_hash_clear(table->table);
	//apr_pool_destroy(table->mpool);
	//free(table);
	mq_worker_t *worker;
	move_to_top(table);
	while( (worker = pop(table)) != NULL ) {
		free(worker->address);
		free(worker);
	}
	free_stack(table, 0);
	log_printf(5, "Successfully destroyed worker table\n");
}

mq_worker_t *mq_get_worker(mq_worker_table_t *table, char *address) {
	//if address is NULL, return the top worker
	//otherwise, return the worker with that address
	mq_worker_t *worker;
	if(address == NULL) {
		//display_worker_table(table);
		//move_to_top(table);
		//worker = (mq_worker_t *)pop(table);
		//log_printf(0, "Worker is at %s\n", worker->address);
		//delete_current(table, 0, 0);
		//mq_add_worker(table, worker);
		//display_worker_table(table);
		//move_to_bottom(table);
		//return (mq_worker_t *)get_ele_data(table);
		return (mq_worker_t *)pop(table);
	}
	else {
		log_printf(0, "Address %s requested\n", address);
		for(worker = mq_worker_table_first(table); worker != NULL; worker = mq_worker_table_next(table)) {
			if(mq_data_compare(worker->address, strlen(worker->address), address, strlen(address)) == 0) {
				log_printf(0, "Found worker!\n");
				return (mq_worker_t *)get_ele_data(table);
			}
		}
		log_printf(0, "Did not find worker\n");
		return NULL;
	}
}

mq_worker_t *get_worker_on_top(mq_worker_table_t *table) {
	
}

mq_worker_t *get_worker_by_address(mq_worker_table_t *table, char *address) {
	
}

void mq_add_worker(mq_worker_table_t *table, mq_worker_t *worker) {
	move_to_bottom(table);
	insert_below(table, worker);
	//log_printf(5, "Added worker with address = %s\tslots = %d\n", worker->address, worker->free_slots);
}

// mq_register_worker()
// Register or deregister a worker in the given worker table
// Creates an mq_worker_t object to store in the table
void mq_register_worker(mq_worker_table_t *table, char *address, int free_slots) {
	
	mq_worker_t *worker;
	//apr_thread_mutex_lock(table->lock);
	
	//********The following 2 comments should be ignored
	//check if a worker with this address already exists
	//remove it if it does exist
	//if free_slots < 0, we're done after this
	//worker = apr_hash_get(table->table, address, strlen(address));
	worker = mq_get_worker(table, address);
	if(worker != NULL) {
		log_printf(15, "Clearing existing worker with address = %s\n", address);
		//apr_hash_set(table->table, worker->address, strlen(worker->address), NULL);
		//free(worker->address);
		//free(worker);
		
		//Update the number of free slots
		worker->free_slots = free_slots;
		log_printf(10, "Worker with address %s now has %d free slots\n", address, free_slots);
	}
	else { //registering a new worker
		worker = mq_worker_create(address, (free_slots < 0) ? 0 : free_slots);
		//apr_hash_set(table->table, worker->address, strlen(worker->address), worker);
		mq_add_worker(table, worker);
	}
	/*
	else {
		log_printf(5, "Deregistered worker with address = %s\n", address);
	}
	*/
	
	//apr_thread_mutex_unlock(table->lock);
}

// mq_deregister_worker()
// Deregister a worker from the table
void mq_deregister_worker(mq_worker_table_t *table, char *address) {
	//mq_register_worker(table, worker->address, -1);
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

// mq_worker_table_install()
// Install the table into the void argument pointer in the socket context
void mq_worker_table_install(mq_worker_table_t *table, mq_portal_t *portal) {
	
	if(portal->implementation_arg != NULL) {
		log_printf(15, "Portal has existing worker table, destroying\n");
		mq_worker_table_destroy(portal->implementation_arg);
	}
	if(table == NULL) {
		// suppose for some reason you tried to install a portal's existing table into itself
		// the above block would have destroyed it, so now it's null
		log_printf(5, "ERROR: Worker table is null!\n");
		return;
	}
	portal->implementation_arg = table;
	log_printf(15, "Installed worker table into portal's socket context\n");
}

/*
 * display_worker_table()
 * 
 * Show me the workers!
 */

void display_worker_table(mq_worker_table_t *table) {
	mq_worker_t* worker;
	//apr_hash_index_t *hi;
	//void *val;
	int i;
	/*
	for(hi = apr_hash_first(table->mpool, table->table), i = 0; hi != NULL; hi = apr_hash_next(hi), i++) {
		apr_hash_this(hi, NULL, NULL, &val);
		worker = (mq_worker_t *)val;
		log_printf(1, "SERVER: worker_table[%2d] = { address = %s free_slots = %d }\n", i, worker->address, worker->free_slots);
	}
	*/
	for(worker = mq_worker_table_first(table), i = 0; worker != NULL; worker = mq_worker_table_next(table), i++) {
		 log_printf(1, "worker_table[%2d] = { address = %s free_slots = %d }\n", i, worker->address, worker->free_slots);
	 }
}

// display_msg_frames()
// Print out the sizes of frames in this message, from which you can infer
// the contents.
// Assumes only addresses are 20+ chars and prints them
void display_msg_frames(mq_msg_t *msg) {
	mq_frame_t *f;
	char *data;
	int size, i;
	for(f = mq_msg_first(msg), i = 0; f != NULL; f = mq_msg_next(msg), i++) {
		mq_get_frame(f, (void **)&data, &size);
		char *toprint = malloc(size + 1);
		strncpy(toprint, data, size);
		toprint[size] = '\0';
		log_printf(0, "msg[%2d]:\t%d\n", i, size);
		if(size >= 20) {
			log_printf(0, "        \t%s\n", toprint);
		}
		free(toprint);
	}
}





