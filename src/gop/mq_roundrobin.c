#include "mq_portal.h"
#include "mq_roundrobin.h"
#include "type_malloc.h"
#include "log.h"

// mq_worker_create()
// Create a worker and fill its parameters

mq_worker_t *mq_worker_create(char *address, int free_slots) {
	
	mq_worker_t *worker;
	type_malloc(worker, mq_worker_t, 1);
	
	worker->address = address;
	worker->free_slots = free_slots;
	log_printf(15, "worker created with address = %s\tslots = %d\n", address, free_slots);
	
	return worker;
}

// mq_worker_table_create()
// Create an empty table for workers

mq_worker_table_t *mq_worker_table_create() {
	
	mq_worker_table_t *table;
	
	type_malloc(table, mq_worker_table_t, 1);
	apr_pool_create(&(table->mpool), NULL);
	assert(apr_thread_mutex_create(&(table->lock), APR_THREAD_MUTEX_DEFAULT, table->mpool) == APR_SUCCESS);
	assert((table->table = apr_hash_make(table->mpool)) != NULL);
	log_printf(15, "Created worker table\n");
	
	return table;
}

// mq_worker_table_destroy()
// Destroy the table and free memory

void mq_worker_table_destroy(mq_worker_table_t *table) {
	
	apr_hash_index_t *hi;
	mq_worker_t* worker;
	void *val;
	int i;
	
	log_printf(15, "Destroying worker table\n");
	
	for(hi = apr_hash_first(table->mpool, table->table), i = 0; hi != NULL; hi = apr_hash_next(hi), i++) {
		apr_hash_this(hi, NULL, NULL, &val);
		worker = (mq_worker_t *)val;
		log_printf(15, "\tDestroying worker %d address = %s\n", i, worker->address);
		apr_hash_set(table->table, worker->address, strlen(worker->address), NULL);
		free(worker->address);
		free(worker);
	}
	
	apr_pool_destroy(table->mpool);
	free(table);
	log_printf(15, "Successfully destroyed worker table\n");
}

// mq_register_worker()
// Register or deregister a worker in the given worker table
// Creates an mq_worker_t object to store in the table
// free_slots >= 0 : register
// free_slots <  0 : deregister

void mq_register_worker(mq_worker_table_t *table, char *address, int free_slots) {
	
	mq_worker_t *worker;
	apr_thread_mutex_lock(table->lock);
	
	//check if a worker with this address already exists
	//remove it if it does exist
	//if free_slots < 0, we're done after this
	worker = apr_hash_get(table->table, address, strlen(address));
	if(worker != NULL) {
		apr_hash_set(table->table, worker->address, strlen(worker->address), NULL);
		free(worker->address);
		free(worker);
	}
	
	if(free_slots >= 0) { //registering a new worker
		worker = mq_worker_create(address, free_slots);
		apr_hash_set(table->table, worker->address, strlen(worker->address), worker);
		log_printf(15, "Registered worker with address = %s\tslots = %d", address, free_slots);
	}
	else {
		log_printf(15, "Deregistered worker with address = %s", address);
	}
	
	apr_thread_mutex_unlock(table->lock);
}

// mq_deregister_worker()
// Deregister a worker from the table
// Added this since it might be confusing to use mq_register_worker() to deregister

void mq_deregister_worker(mq_worker_table_t *table, mq_worker_t *worker) {
	mq_register_worker(table, worker->address, -1);
}

// mq_worker_table_install()
// Install the table into the void argument pointer in the socket context

void mq_worker_table_install(mq_worker_table_t *table, mq_portal_t *portal) {
	
	if(portal->worker_table != NULL) {
		log_printf(15, "Portal has existing worker table, destroying\n");
		mq_worker_table_destroy(portal->worker_table);
	}
	if(table == NULL) {
		// suppose for some reason you tried to install a portal's existing table into itself
		// the above block would have destroyed it, so now it's null
		log_printf(5, "ERROR: Worker table is null!\n");
		return;
	}
	portal->worker_table = table;
	log_printf(15, "Installed worker table into portal's socket context\n");
}





