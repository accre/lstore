#ifndef __MQ_ROUNDROBIN_H_
#define __MQ_ROUNDROBIN_H_

#include <mq_portal.h>

// Frame constant
#define MQF_INCREMENT_KEY		"\007"
#define MQF_INCREMENT_SIZE		1

// Stats
#define MQS_INCREMENT_INDEX		9

// Socket type
#define MQ_ROUND_ROBIN			1002

typedef struct {
	int free_slots;
	char *address;
} mq_worker_t;

typedef struct {
	apr_hash_t *table;
	apr_pool_t *mpool;
	apr_thread_mutex_t *lock;
} mq_worker_table_t;

mq_worker_t *mq_worker_create(char* address, int free_slots);
mq_worker_table_t *mq_worker_table_create();
void mq_worker_table_destroy(mq_worker_table_t *t);
void mq_register_worker(mq_worker_table_t *table, char *address, int free_slots);
void mq_deregister_worker(mq_worker_table_t *table, mq_worker_t *worker);
void mq_worker_table_install(mq_worker_table_t *table, mq_portal_t *portal);

#endif
