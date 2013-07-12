/*
Advanced Computing Center for Research and Education Proprietary License
Version 1.0 (April 2006)

Copyright (c) 2006, Advanced Computing Center for Research and Education,
 Vanderbilt University, All rights reserved.

This Work is the sole and exclusive property of the Advanced Computing Center
for Research and Education department at Vanderbilt University.  No right to
disclose or otherwise disseminate any of the information contained herein is
granted by virtue of your possession of this software except in accordance with
the terms and conditions of a separate License Agreement entered into with
Vanderbilt University.

THE AUTHOR OR COPYRIGHT HOLDERS PROVIDES THE "WORK" ON AN "AS IS" BASIS,
WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, TITLE, FITNESS FOR A PARTICULAR
PURPOSE, AND NON-INFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Vanderbilt University
Advanced Computing Center for Research and Education
230 Appleton Place
Nashville, TN 37203
http://www.accre.vanderbilt.edu
*/

#include "apr_wrapper.h"
#include "log.h"
#include "type_malloc.h"
#include "opque.h"
#include "mq_portal.h"
#include "mq_stream.h"
#include "mq_ongoing.h"
#include "mq_helpers.h"
#include "random.h"
#include <sys/eventfd.h>

#define MQS_TEST_KEY  "mqs_test"
#define MQS_TEST_SIZE 8


apr_thread_mutex_t *lock = NULL;
apr_thread_cond_t  *cond = NULL;
char *handle = NULL;
mq_command_stats_t server_stats;
mq_portal_t *server_portal = NULL;
mq_ongoing_t *server_ongoing = NULL;

char *test_data = NULL;
int test_size = 1024*1024;
int total_bytes_to_transfer = 10*1024*1024;
int do_compress = MQS_PACK_RAW;
int timeout = 10;
int stream_max_size = 4096;
int launch_flusher = 0;
int delay_response = 0;
int in_process = 0;

char *host = "tcp://127.0.0.1:6714";
char *host_id = "30:Random_Id";
int host_id_len = 13;

int control_efd = -1;
int server_efd = -1;
int shutdown_everything = 0;

//***********************************************************************
// client_read_stream - Reads the incoming data stream
//***********************************************************************

op_status_t client_read_stream(void *task_arg, int tid)
{
  mq_task_t *task = (mq_task_t *)task_arg;
  mq_context_t *mqc = (mq_context_t *)task->arg;
  mq_stream_t *mqs;
  op_status_t status;
  int err, nread, nleft, offset, nbytes;
  char *buffer;

log_printf(5, "START\n");

  type_malloc(buffer, char, test_size);

  status = op_success_status;

  //** Parse the response
  mq_remove_header(task->response, 1);

  mqs = mq_stream_read_create(mqc, host_id, host_id_len, mq_msg_first(task->response), host);

  nread = 0;
  nleft = total_bytes_to_transfer;
  while (nleft > 0) {
offset=-1;
     err = mq_stream_read(mqs, &offset, sizeof(int));
     if (err != 0) {
        log_printf(0, "ERROR reading offset!  nread=%d err=%d\n", nread, err);
        status = op_failure_status;
        goto fail;
     }
     if (offset > test_size) {
        log_printf(0, "ERROR invalid offset=%d > %d! nread=%d\n", offset, test_size, nread);
        status = op_failure_status;
        goto fail;
     }

     err = mq_stream_read(mqs, &nbytes, sizeof(int));
     if (err != 0) {
        log_printf(0, "ERROR reading nbytes!  nread=%d err=%d\n", nread, err);
        status = op_failure_status;
        goto fail;
     }
     err = offset + nbytes - 1;
     if ((err > test_size) && (err >= 0)) {
        log_printf(0, "ERROR invalid offset+nbytes offset=%d test=%d max is %d! nread=%d\n", offset, nbytes, test_size, nread);
        status = op_failure_status;
        goto fail;
     }

     memset(buffer, 0, nbytes);
     err = mq_stream_read(mqs, buffer, nbytes);
     if (err != 0) {
        log_printf(0, "ERROR reading data! nbytes=%d but got %d nread=%d\n", nbytes, err, nread);
        status = op_failure_status;
        goto fail;
     }

     if (memcmp(buffer, &(test_data[offset]), nbytes) != 0) {
        log_printf(0, "ERROR data mismatch! offset=%d nbytes=%d nread=%d\n", offset, nbytes, nread);
        status = op_failure_status;
        goto fail;
     }
     nread += nbytes;
     nleft -= nbytes;
  }

fail:
  mq_stream_destroy(mqs);
  free(buffer);

log_printf(5, "END status=%d %d\n", err, status.op_status, status.error_code);

  return(status);
}


//***************************************************************************
// client_make_context - Makes the MQ portal context
//***************************************************************************

mq_context_t *client_make_context()
{
  char *text_params = "[mq_context]\n"
                      "  min_conn=1\n"
                      "  max_conn=4\n"
                      "  min_threads=2\n"
                      "  max_threads=10\n"
                      "  backlog_trigger=1000\n"
                      "  heartbeat_dt=1\n"
                      "  heartbeat_failure=10\n"
                      "  min_ops_per_sec=100\n";
  inip_file_t *ifd;
  mq_context_t *mqc;

  ifd = inip_read_text(text_params);
  mqc = mq_create_context(ifd, "mq_context");
  assert(mqc != NULL);
  inip_destroy(ifd);

  return(mqc);
}


//***************************************************************************
// client_test - Performs a stream test
//***************************************************************************

int client_test(mq_context_t *mqc, int flusher, int delay, int to)
{
  int got, shouldbe, n;
  mq_msg_t *msg;
  op_generic_t *gop;

  log_printf(0, "START\n");

   //** Make the gop
  msg = mq_make_exec_core_msg(host, 1);
  mq_msg_append_mem(msg, MQS_TEST_KEY, MQS_TEST_SIZE, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, host_id, host_id_len, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
  gop = new_mq_op(mqc, msg, client_read_stream, mqc, NULL, to);

  //** Change the global settings
  launch_flusher = flusher;
  delay_response = delay;
  timeout = to;

  //** and execute it
  got = gop_waitall(gop);

  shouldbe = OP_STATE_SUCCESS;
  if (flusher == 0) {
     if (delay > to) shouldbe = OP_STATE_FAILURE;
  }
  if (got != shouldbe) {
     n = 1;
     log_printf(0, "ERROR with stream test! got=%d shouldbe=%d\n", got, shouldbe);
  } else {
     n = 0;
     log_printf(0, "SUCCESS with stream test! got=%d shouldbe=%d\n", got, shouldbe);
  }

  log_printf(0, "END\n");

  gop_free(gop, OP_DESTROY);

  return(n);
}

//***************************************************************************
// client_test_thread -  Client test thread
//***************************************************************************

void *client_test_thread(apr_thread_t *th, void *arg)
{
  int n;
  mq_context_t *mqc;

  log_printf(0, "START\n");

  //** The rest of the tests all go through the mq_portal so we need to configure that now
  //** Make the portal
  mqc = client_make_context();

  n = 0;
  n += client_test(mqc, 0, 0, 10);  //** Normal valid usage pattern
  n += client_test(mqc, 0, 10, 5);  //** Response will come after the timeout
  n += client_test(mqc, 1, 15, 8);  //** Launch the flusher but delay sending data forcing the heartbeat to handle it

  if (n != 0) {
     log_printf(0, "ERROR with %d tests\n", n);
  } else {
     log_printf(0, "SUCCESS! No problems with any stream test\n");
  }

  //** Destroy the portal
  mq_destroy_context(mqc);

  log_printf(0, "END\n");

  return(NULL);
}

//***************************************************************************
// cb_write_stream - Handles the stream write response
//***************************************************************************

void cb_write_stream(void *arg, mq_task_t *task)
{
  int nbytes, offset, nsent, nleft, err;
  mq_frame_t *fid, *hid;
  mq_msg_t *msg;
  mq_stream_t *mqs;
  log_printf(3, "START stream_max=%d total_bytes=%d launch_flusher=%d delay_response=%d timeout=%d\n", stream_max_size, total_bytes_to_transfer, launch_flusher, delay_response, timeout); flush_log();

  apr_thread_mutex_lock(lock);
  in_process++;
  apr_thread_mutex_unlock(lock);

  //** Parse the command.
  msg = task->msg;
  mq_remove_header(msg, 0);

  fid = mq_msg_pop(msg);  //** This is the command ID
  mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame
  hid = mq_msg_pop(msg);  //** This is the Host ID for ongoing tracking

  //** Create the stream so we can get the heartbeating while we work
  mqs = mq_stream_write_create(task->ctx, server_portal, server_ongoing, do_compress, stream_max_size, timeout, msg, fid, hid, launch_flusher);

  if (delay_response > 0) {
     log_printf(5, "Sleeping for %d sec\n", delay_response);
     sleep(delay_response);
     log_printf(5, "Woken up from sleep\n");
  }

  nleft = total_bytes_to_transfer;
  nsent = 0;
  do {
     nbytes = random_int(1, test_size);
     if (nbytes > nleft) nbytes = nleft;
     offset = random_int(0, test_size-nbytes);

     log_printf(0, "nsent=%d  offset=%d nbytes=%d\n", nsent, offset, nbytes);
     err = mq_stream_write(mqs, &offset, sizeof(int));
     if (err != 0) {
        log_printf(0, "ERROR writing offset!  nsent=%d\n", nsent);
        goto fail;
     }

     err = mq_stream_write(mqs, &nbytes, sizeof(int));
     if (err != 0) {
        log_printf(0, "ERROR writing nbytes!  nsent=%d\n", nsent);
        goto fail;
     }

     err = mq_stream_write(mqs, &(test_data[offset]), nbytes);
     if (err != 0) {
        log_printf(0, "ERROR writing test_data!  nsent=%d\n", nsent);
        goto fail;
     }

     nsent += nbytes;
     nleft -= nbytes;
  } while (nleft > 0);

fail:
  mq_stream_destroy(mqs);

  log_printf(3, "END\n"); flush_log();

  apr_thread_mutex_lock(lock);
  in_process--;
  apr_thread_cond_broadcast(cond);
  apr_thread_mutex_unlock(lock);

}

//***************************************************************************
// server_make_context - Makes the MQ portal context
//***************************************************************************

mq_context_t *server_make_context()
{
  char *text_params = "[mq_context]\n"
                      "  min_conn=1\n"
                      "  max_conn=2\n"
                      "  min_threads=2\n"
                      "  max_threads=100\n"
                      "  backlog_trigger=1000\n"
                      "  heartbeat_dt=1\n"
                      "  heartbeat_failure=5\n"
                      "  min_ops_per_sec=100\n";
  inip_file_t *ifd;
  mq_context_t *mqc;

  ifd = inip_read_text(text_params);
  mqc = mq_create_context(ifd, "mq_context");
  assert(mqc != NULL);
  inip_destroy(ifd);

  return(mqc);
}

//***************************************************************************
// server_test_mq_loop
//***************************************************************************

void *server_test_thread(apr_thread_t *th, void *arg)
{
  mq_context_t *mqc;
  mq_command_table_t *table;
  uint64_t n;

  log_printf(0, "START\n");

  //** Make the server portal
  mqc = server_make_context();

  //** Make the server portal
  server_portal = mq_portal_create(mqc, host, MQ_CMODE_SERVER);

  //** Make the ongoing checker
  server_ongoing = mq_ongoing_create(mqc, server_portal, 30);
  assert(server_ongoing != NULL);

  //** Install the commands
  table = mq_portal_command_table(server_portal);
  mq_command_add(table, MQS_TEST_KEY, MQS_TEST_SIZE, mqc, cb_write_stream);
  mq_command_add(table, MQS_MORE_DATA_KEY, MQS_MORE_DATA_SIZE, server_ongoing, mqs_server_more_cb);

  mq_portal_install(mqc, server_portal);

  //** Wait for a shutdown
  read(control_efd, &n, sizeof(n));

  apr_thread_mutex_lock(lock);
  while (in_process == 1) {
     apr_thread_cond_wait(cond, lock);
  }
  apr_thread_mutex_unlock(lock);

  mq_ongoing_destroy(server_ongoing);

  //** Destroy the portal
  mq_destroy_context(mqc);

  log_printf(0, "END\n");

  return(NULL);
}


//***************************************************************************
//***************************************************************************
//***************************************************************************

int main(int argc, char **argv)
{
  apr_pool_t *mpool;
  apr_thread_t *server_thread, *client_thread;
  apr_status_t dummy;
  int i, start_option, do_random;
  uint64_t n;

  if (argc < 2) {
     printf("mqs_test [-d log_level] [-t kbytes] [-p kbytes] [-z] [-0] \n");
     printf("\n");
     printf("-t kbytes     Total number of bytes to transfer in KB. Defaults is %d KB\n", total_bytes_to_transfer/1024);
     printf("-p kbytes     Max size of a stream packet in KB. Defaults is %d KB\n", stream_max_size/1024);
     printf("-z            Enable data compression\n");
     printf("-0            Use test data filled with zeros.  Defaults to using random data.\n");
     printf("\n");
     return(0);
  }

  i = 1;
  do {
     start_option = i;

     if (strcmp(argv[i], "-d") == 0) { //** Enable debugging
        i++;
        set_log_level(atol(argv[i]));
        i++;
     } else if (strcmp(argv[i], "-h") == 0) { //** Print help
        printf("mq_test [-d log_level]\n");
        return(0);
     } else if (strcmp(argv[i], "-t") == 0) { //** Change number of total bytes transferred
        i++;
        total_bytes_to_transfer = atol(argv[i]) * 1024;
        i++;
     } else if (strcmp(argv[i], "-p") == 0) { //** Max number of bytes to transfer / packet
        i++;
        stream_max_size = atol(argv[i]) * 1024;
        i++;
     } else if (strcmp(argv[i], "-z") == 0) { //** Enable compression
        i++;
        do_compress = MQS_PACK_COMPRESS;
     } else if (strcmp(argv[i], "-0") == 0) { //** Used 0 filled data
        i++;
        do_random = 0;
     }
  } while ((start_option < i) && (i<argc));

printf("log_level=%d\n", _log_level);

//log_printf(0, "before wrapper opque_count=%d\n", _opque_counter);
  apr_wrapper_start();
log_printf(0, "after wrapper opque_count=%d\n", _opque_counter);
  init_opque_system();
log_printf(0, "after init opque_count=%d\n", _opque_counter);
  init_random();

  //** Make the test_data to pluck info from
  type_malloc_clear(test_data, char, test_size);
  if (do_random == 1) get_random(test_data, test_size);

  //** Make the locking structures for client/server communication
  apr_pool_create(&mpool, NULL);
  assert(apr_thread_mutex_create(&lock, APR_THREAD_MUTEX_DEFAULT, mpool) == APR_SUCCESS);
  assert(apr_thread_cond_create(&cond, mpool) == APR_SUCCESS);

  //** Make the eventFD for controlling the server
  control_efd = eventfd(0, 0);
  assert(control_efd != -1);

 //** Make the server eventfd for delayed responses
  server_efd = eventfd(0, EFD_SEMAPHORE);
  assert(server_efd != 0);


  apr_thread_create(&client_thread, NULL, client_test_thread, NULL, mpool);
  apr_thread_create(&server_thread, NULL, server_test_thread, NULL, mpool);

  apr_thread_join(&dummy, client_thread);

  //** Trigger the server to shutdown
  n = 1;
  write(control_efd, &n, sizeof(n));
  apr_thread_join(&dummy, server_thread);

  apr_thread_mutex_destroy(lock);
  apr_thread_cond_destroy(cond);

  apr_pool_destroy(mpool);

  destroy_opque_system();
  apr_wrapper_stop();

  close(control_efd);
  close(server_efd);

  free(test_data);
  return(0);
}

