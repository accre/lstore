#ifndef __MQ_ROUNDROBIN_H_
#define __MQ_ROUNDROBIN_H_

#include "gop/gop_visibility.h"
#include "mq_portal.h"
#include "mq_helpers.h"
#include <string.h>

// Frame constant
#define MQF_INCREMENT_KEY		"\007"
#define MQF_INCREMENT_SIZE		1
#define MQF_REGISTER_KEY		"\010"
#define MQF_REGISTER_SIZE		1
#define MQF_DEREGISTER_KEY		"\011"
#define MQF_DEREGISTER_SIZE		1
#define MQF_RR_STREAM_KEY		"\012"
#define MQF_RR_STREAM_SIZE		1


// Socket type
#define MQ_ROUND_ROBIN			1002

typedef tbx_stack_t mq_worker_table_t;
typedef tbx_stack_t mq_processing_queue;

typedef struct {
    int free_slots;
    char *address;
    // Will later have job type(s)
} mq_worker_t;

// worker table functions
GOP_API mq_worker_table_t *mq_worker_table_create();
GOP_API void mq_worker_table_destroy(mq_worker_table_t *t);
GOP_API void mq_worker_table_install(mq_worker_table_t *table, mq_portal_t *portal);
GOP_API int mq_worker_table_length(mq_worker_table_t *table);
GOP_API void display_worker_table(mq_worker_table_t *table);
mq_worker_t *mq_worker_table_first(mq_worker_table_t *table);
mq_worker_t *mq_worker_table_next(mq_worker_table_t *table);
mq_worker_t *mq_get_available_worker(mq_worker_table_t *);
GOP_API mq_worker_t *mq_get_worker(mq_worker_table_t *table, char *address);
GOP_API void mq_worker_table_add(mq_worker_table_t *table, mq_worker_t *worker);

// worker functions
mq_worker_t *mq_worker_create(char* address, int free_slots);
GOP_API void mq_register_worker(mq_worker_table_t *table, char *address, int free_slots);
GOP_API void mq_deregister_worker(mq_worker_table_t *table, char *address);

GOP_API int mq_send_message(mq_msg_t *msg, mq_worker_table_t *table, mq_portal_t *portal);

// process queue functions
GOP_API mq_processing_queue *processing_queue_new();
GOP_API void processing_queue_destroy(mq_processing_queue *queue);
GOP_API void processing_queue_add(mq_processing_queue *queue, mq_msg_t *msg);
GOP_API void processing_queue_push(mq_processing_queue *queue, mq_msg_t *msg);
GOP_API mq_msg_t *processing_queue_get(mq_processing_queue *queue);
GOP_API int processing_queue_length(mq_processing_queue *queue);

//convenience function - can probably go into mq_helpers if/when it's better written
GOP_API void display_msg_frames(mq_msg_t *msg);

#endif
