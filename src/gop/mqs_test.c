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
#include "string_token.h"
#include "apr_wrapper.h"

#define MQS_TEST_KEY  "mqs_test"
#define MQS_TEST_SIZE 8

typedef struct {
 int launch_flusher;
 int delay;
 int client_delay;
 int max_packet;
 int send_bytes;
 int timeout;
 int shouldbe;
 int gid;
} test_gop_t;

apr_thread_mutex_t *lock = NULL;
apr_thread_cond_t  *cond = NULL;
char *handle = NULL;
mq_command_stats_t server_stats;
mq_portal_t *server_portal = NULL;
mq_ongoing_t *server_ongoing = NULL;
mq_ongoing_t *client_ongoing = NULL;

char *test_data = NULL;
int test_size = 1024*1024;
int packet_min = 1024;
int packet_max = 1024*1024;
int send_min = 1024;
int send_max = 10*1024*1024;
int nparallel = 100;
int ntotal = 1000;
int do_compress = MQS_PACK_RAW;
int timeout = 10;
int stream_max_size = 4096;
int launch_flusher = 0;
int delay_response = 0;
int in_process = 0;

int ongoing_server_interval = 5;
int ongoing_client_interval = 1;

char *host = "tcp://127.0.0.1:6714";
char *host_id = "30:Random_Id";
int host_id_len = 13;

mq_pipe_t control_efd[2];
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
  int client_delay;
  char *buffer;
  test_gop_t *op;

  op = gop_get_private(task->gop);

  log_printf(0, "START: gid=%d test_gop(f=%d, cd=%d sd=%d, mp=%d, sb=%d, to=%d) = %d\n", op->gid, op->launch_flusher, op->client_delay, op->delay, op->max_packet, op->send_bytes, op->timeout, op->shouldbe);

  client_delay = op->client_delay;
  type_malloc(buffer, char, test_size);

  status = op_success_status;

  //** Parse the response
  mq_remove_header(task->response, 1);

  mqs = mq_stream_read_create(mqc, client_ongoing, host_id, host_id_len, mq_msg_first(task->response), host, op->timeout);

  log_printf(0, "gid=%d msid=%d\n", op->gid, mqs->msid);

  nread = 0;
  nleft = op->send_bytes;
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

     if ((nleft > 0) && (client_delay > 0)) {
        log_printf(0, "gid=%d msid=%d sleep(%d)\n", op->gid, mqs->msid, client_delay);
        sleep(client_delay);
        log_printf(0, "gid=%d msid=%d Awake!\n", op->gid, mqs->msid);
        client_delay = 0;
     }

     log_printf(2, "nread=%d nleft=%d\n", nread, nleft);
  }

  nbytes = 1;
  log_printf(1, "gid=%d msid=%d Before read after EOS\n", op->gid, mqs->msid);
  err = mq_stream_read(mqs, buffer, nbytes);
  log_printf(1, "gid=%d msid=%d Attempt to read beyond EOS err=%d\n", op->gid, mqs->msid, err);
  if (err == 0) {
     log_printf(0, "ERROR Attempt to read after EOS succeeded! err=%d gid=%d msid=%d\n", err, op->gid, mqs->msid);
     status = op_failure_status;
  }

fail:
  err = mqs->msid;
  mq_stream_destroy(mqs);
  free(buffer);

log_printf(5, "END msid=%d status=%d %d\n", err, status.op_status, status.error_code);

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
                      "  max_threads=%d\n"
                      "  backlog_trigger=1000\n"
                      "  heartbeat_dt=1\n"
                      "  heartbeat_failure=10\n"
                      "  min_ops_per_sec=100\n";

  char buffer[1024];
  inip_file_t *ifd;
  mq_context_t *mqc;

  snprintf(buffer, sizeof(buffer), text_params, 100*nparallel);
  ifd = inip_read_text(buffer);
  mqc = mq_create_context(ifd, "mq_context");
  assert(mqc != NULL);
  inip_destroy(ifd);

  return(mqc);
}

//***************************************************************************
// test_gop - Generates a MQS test GOP for execution
//***************************************************************************

op_generic_t *test_gop(mq_context_t *mqc, int flusher, int client_delay, int delay, int max_packet, int send_bytes, int to, int myid)
{
  mq_msg_t *msg;
  op_generic_t *gop;
  test_gop_t *op;

  log_printf(0, "START\n");

  //** Fill in the structure
  type_malloc_clear(op, test_gop_t, 1);
  op->launch_flusher = flusher;
  op->delay = delay;
  op->client_delay = client_delay;
  op->max_packet = max_packet;
  op->send_bytes = send_bytes;
  op->timeout = to;

  op->shouldbe = OP_STATE_SUCCESS;
  if (flusher == 0) {
     if (delay > to) op->shouldbe = OP_STATE_FAILURE;
  }

   //** Make the gop
  msg = mq_make_exec_core_msg(host, 1);
  mq_msg_append_mem(msg, MQS_TEST_KEY, MQS_TEST_SIZE, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, host_id, host_id_len, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, op, sizeof(test_gop_t), MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
  gop = new_mq_op(mqc, msg, client_read_stream, mqc, NULL, to);
  gop_set_private(gop, op);
  op->gid = gop_id(gop);
  gop_set_myid(gop, myid);

  log_printf(0, "CREATE: gid=%d myid=%d test_gop(f=%d, cd=%d, sd=%d, mp=%d, sb=%d, to=%d) = %d\n", gop_id(gop), myid, op->launch_flusher, op->client_delay, op->delay, op->max_packet, op->send_bytes, op->timeout, op->shouldbe);
  flush_log();

  return(gop);
}

//***************************************************************************
// new_bulk_task - Generates a new bulk task
//***************************************************************************

op_generic_t *new_bulk_task(mq_context_t *mqc, int myid)
{
  int transfer_bytes, packet_bytes, delay, client_delay, flusher, to;
  int to_min, to_max;

  to_min = 10; to_max = 20;

  transfer_bytes = random_int(send_min, send_max);
  packet_bytes = random_int(packet_min, packet_max);
  to = random_int(to_min, to_max);
  delay = random_int(1, 10);
  if (delay == 1) {
      delay = random_int(to_min, to_max);
      if (delay == to) delay += 2;
  } else {
      delay = 0;
  }
  flusher = random_int(1, 3);
//flusher = 1;
  if (flusher != 1) {
      flusher = 0;
  }

  client_delay = random_int(1, 10);
  if (client_delay == 1) {
      client_delay = random_int(to_min, to_max);
      if (client_delay == to) client_delay -= 2;
  } else {
      client_delay = 0;
  }

//delay = to + 5;

  return(test_gop(mqc, flusher, client_delay, delay, packet_bytes, transfer_bytes, to, myid));
}

//***************************************************************************
// client_consume_result - Evaluates the result and frees the GOP
//   On success 0 is returned.  Otherwise 1 is returned indicating an error
//***************************************************************************

int client_consume_result(op_generic_t *gop)
{
  int n;
  test_gop_t *op;
  op_status_t status;

  op = gop_get_private(gop);
  status = gop_get_status(gop);

  if (status.op_status != op->shouldbe) {
     n = 1;
     log_printf(0, "ERROR with stream test! gid=%d myid=%d test_gop(f=%d, cd=%d sd=%d, mp=%d, sb=%d, to=%d) = %d got=%d\n", gop_id(gop), gop_get_myid(gop), op->launch_flusher, op->client_delay, op->delay, op->max_packet, op->send_bytes, op->timeout, op->shouldbe, status.op_status);
  } else {
     n = 0;
     log_printf(0, "SUCCESS with stream test! gid=%d myid=%d test_gop(f=%d, cd=%d sd=%d, mp=%d, sb=%d, to=%d) = %d got=%d\n", gop_id(gop), gop_get_myid(gop), op->launch_flusher, op->client_delay, op->delay, op->max_packet, op->send_bytes, op->timeout, op->shouldbe, status.op_status);
  }
  flush_log();

  gop_free(gop, OP_DESTROY);
  free(op);

  return(n);
}

//***************************************************************************
// client_test_thread -  Client test thread
//***************************************************************************

void *client_test_thread(apr_thread_t *th, void *arg)
{
  int n, i;
  int single_max, single_send;
  mq_context_t *mqc;
  op_generic_t *gop;
  opque_t *q = NULL;

  log_printf(0, "START\n");

  //** The rest of the tests all go through the mq_portal so we need to configure that now
  //** Make the portal
  mqc = client_make_context();

  //** Make the ongoing checker
  client_ongoing = mq_ongoing_create(mqc, NULL, ongoing_client_interval, ONGOING_CLIENT);
  assert(client_ongoing != NULL);

  log_printf(0, "START basic stream tests\n");
  n = 0;
  single_max = 8192;
  single_send = 1024 * 1024;

  gop = test_gop(mqc, 0, 0, 0, single_max, single_send, 10, 1); gop_waitall(gop); n += client_consume_result(gop);  //** Normal valid usage pattern
//  gop = test_gop(mqc, 1, 0, 0, single_max, single_send, 10); gop_waitall(gop); n += client_consume_result(gop);  //** Launch the flusher but no delay sending data
//  gop = test_gop(mqc, 0, 0, 10, single_max, single_send, 5); gop_waitall(gop); n += client_consume_result(gop);  //** Response will come after the timeout
//  gop = test_gop(mqc, 1, 0, 15, single_max, single_send, 8); gop_waitall(gop); n += client_consume_result(gop);  //** Launch the flusher but delay sending data forcing the heartbeat to handle it
//  gop = test_gop(mqc, 0, 30, 0, single_max, single_send, 5); gop_waitall(gop); n += client_consume_result(gop);  //** Vvalid use pattern but the client pauses after reading

  if (n != 0) {
     log_printf(0, "END:  ERROR with %d basic tests\n", n);
  } else {
     log_printf(0, "END:  SUCCESS! No problems with any basic stream test\n");
  }

  goto skip;

  log_printf(0, "START bulk stream tests nparallel=%d\n", nparallel);
  n = 0;

  q = new_opque();
  opque_start_execution(q);
  for (i=0; i<ntotal; i++) {
      gop = new_bulk_task(mqc, i);
      opque_add(q, gop);
      if (i>=nparallel-1) {
         gop = opque_waitany(q);
         n += client_consume_result(gop);
      }
  }

  log_printf(0, "FINISHED job submission!\n");

  while ((gop = opque_waitany(q)) != NULL) {
     n += client_consume_result(gop);
  }

  if (n != 0) {
     log_printf(0, "END:  ERROR with %d bulk tests\n", n);
  } else {
     log_printf(0, "END:  SUCCESS! No problems with any bulk stream test\n");
  }

skip:
  mq_ongoing_destroy(client_ongoing);

  //** Destroy the portal
  mq_destroy_context(mqc);

  if (q != NULL) opque_free(q, OP_DESTROY);

  log_printf(0, "END\n");

  return(NULL);
}

//***************************************************************************
// cb_write_stream - Handles the stream write response
//***************************************************************************

void cb_write_stream(void *arg, mq_task_t *task)
{
  int nbytes, offset, nsent, nleft, err;
  mq_frame_t *fid, *hid, *fop;
  mq_msg_t *msg;
  mq_stream_t *mqs;
  test_gop_t *op;

  apr_thread_mutex_lock(lock);
  in_process++;
  apr_thread_mutex_unlock(lock);

  //** Parse the command.
  msg = task->msg;
  mq_remove_header(msg, 0);

  fid = mq_msg_pop(msg);  //** This is the command ID
  mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame
  hid = mq_msg_pop(msg);  //** This is the Host ID for ongoing tracking

  fop = mq_msg_pop(msg);  //** Contains the op structure
  mq_get_frame(fop, (void **)&op, &nbytes);
  assert(nbytes == sizeof(test_gop_t));

  log_printf(0, "START: gid=%d test_gop(f=%d, cd=%d, sd=%d, mp=%d, sb=%d, to=%d) = %d\n", op->gid, op->launch_flusher, op->client_delay, op->delay, op->max_packet, op->send_bytes, op->timeout, op->shouldbe);

  //** Create the stream so we can get the heartbeating while we work
  mqs = mq_stream_write_create(task->ctx, server_portal, server_ongoing, do_compress, op->max_packet, op->timeout, msg, fid, hid, op->launch_flusher);

  log_printf(0, "gid=%d msid=%d\n", op->gid, mqs->msid);

  if (op->delay > 0) {
     log_printf(5, "Sleeping for %d sec gid=%d\n", op->delay, op->gid);
     sleep(op->delay);
     log_printf(5, "Woken up from sleep gid=%d\n", op->gid);
  }

  nleft = op->send_bytes;
  nsent = 0;
  do {
     nbytes = random_int(1, test_size);
     if (nbytes > nleft) nbytes = nleft;
     offset = random_int(0, test_size-nbytes);

     log_printf(0, "nsent=%d  offset=%d nbytes=%d\n", nsent, offset, nbytes);
     err = mq_stream_write(mqs, &offset, sizeof(int));
     if (err != 0) {
        log_printf(0, "ERROR writing offset!  nsent=%d gid=%d\n", nsent, op->gid);
        goto fail;
     }

     err = mq_stream_write(mqs, &nbytes, sizeof(int));
     if (err != 0) {
        log_printf(0, "ERROR writing nbytes!  nsent=%d gid=%d\n", nsent, op->gid);
        goto fail;
     }

     err = mq_stream_write(mqs, &(test_data[offset]), nbytes);
     if (err != 0) {
        log_printf(0, "ERROR writing test_data!  nsent=%d gid=%d\n", nsent, op->gid);
        goto fail;
     }

     nsent += nbytes;
     nleft -= nbytes;
  } while (nleft > 0);

fail:
  err = op->gid;
  mq_frame_destroy(fop);
  mq_stream_destroy(mqs);

  log_printf(0, "END gid=%d\n", err); flush_log();

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
                      "  max_threads=%d\n"
                      "  backlog_trigger=10000\n"
                      "  heartbeat_dt=1\n"
                      "  heartbeat_failure=5\n"
                      "  min_ops_per_sec=100\n";
  char buffer[1024];
  inip_file_t *ifd;
  mq_context_t *mqc;

  snprintf(buffer, sizeof(buffer), text_params, 100*nparallel);
  ifd = inip_read_text(buffer);
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
  char c;

  log_printf(0, "START\n");

  //** Make the server portal
  mqc = server_make_context();

  //** Make the server portal
  server_portal = mq_portal_create(mqc, host, MQ_CMODE_SERVER);

  //** Make the ongoing checker
  server_ongoing = mq_ongoing_create(mqc, server_portal, ongoing_server_interval, ONGOING_SERVER);
  assert(server_ongoing != NULL);

  //** Install the commands
  table = mq_portal_command_table(server_portal);
  mq_command_set(table, MQS_TEST_KEY, MQS_TEST_SIZE, mqc, cb_write_stream);
  mq_command_set(table, MQS_MORE_DATA_KEY, MQS_MORE_DATA_SIZE, server_ongoing, mqs_server_more_cb);
//ADded in ongoing_create  mq_command_set(table, ONGOING_KEY, ONGOING_SIZE, server_ongoing, mq_ongoing_cb);

  mq_portal_install(mqc, server_portal);

  //** Wait for a shutdown
  mq_pipe_read(control_efd[0], &c);

  apr_thread_mutex_lock(lock);
  while (in_process != 0) {
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
  int i, start_option, do_random, ll;
  int64_t lsize = 0;
  char buf1[256], buf2[256], c;
  char *logfile = NULL;
  mq_socket_context_t *ctx;

  ll = 0;

  if (argc < 2) {
     printf("mqs_test [-d log_level] [-log log_file] [-log_size size] [-t min max] [-p min max] [-np nparalle] [-nt ntotal] [-z] [-0] \n");
     printf("\n");
     printf("-d log_level\n");
     printf("-log log_file  Log file for storing output.  Defaults to stdout\n");
     printf("-log_size size Log file size.  Can use unit abbreviations.\n");
     printf("-t min max     Range of total bytes to transfer for bulk tests. Defaults is %s to %s\n", pretty_print_int_with_scale(send_min, buf1), pretty_print_int_with_scale(send_max, buf2));
     printf("-p min max     Range of max stream packet sizes for bulk tests. Defaults is %s to %s\n", pretty_print_int_with_scale(packet_min, buf1), pretty_print_int_with_scale(packet_max, buf2));
     printf("-np nparallel  Number of parallel streams to execute.  Default is %d\n", nparallel);
     printf("-nt ntotal     Total number of bulk operations to perform.  Default is %d\n", ntotal);
     printf("-z             Enable data compression\n");
     printf("-0             Use test data filled with zeros.  Defaults to using random data.\n");
     printf("\n");
     return(0);
  }

  i = 1;
  do_random = 1;
  do {
     start_option = i;

     if (strcmp(argv[i], "-d") == 0) { //** Enable debugging
        i++;
        ll = atol(argv[i]);
        i++;
     } else if (strcmp(argv[i], "-h") == 0) { //** Print help
        printf("mq_test [-d log_level]\n");
        return(0);
     } else if (strcmp(argv[i], "-t") == 0) { //** Change number of total bytes transferred
        i++;
        send_min = string_get_integer(argv[i]);
        i++;
        send_max = string_get_integer(argv[i]);
        i++;
     } else if (strcmp(argv[i], "-p") == 0) { //** Max number of bytes to transfer / packet
        i++;
        packet_min = string_get_integer(argv[i]);
        i++;
        packet_max = string_get_integer(argv[i]);
        i++;
     } else if (strcmp(argv[i], "-np") == 0) { //** Parallel transfers
        i++;
        nparallel = string_get_integer(argv[i]);
        i++;
     } else if (strcmp(argv[i], "-nt") == 0) { //** Total number of transfers
        i++;
        ntotal = string_get_integer(argv[i]);
        i++;
     } else if (strcmp(argv[i], "-log") == 0) { //** Log file
        i++;
        logfile = argv[i];
        i++;
     } else if (strcmp(argv[i], "-log_size") == 0) { //** Log file size
        i++;
        lsize = string_get_integer(argv[i]);
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

printf("Settings packet=(%d,%d) send=(%d,%d) np=%d nt=%d\n", packet_min, packet_max, send_min, send_max, nparallel, ntotal);

//log_printf(0, "before wrapper opque_count=%d\n", _opque_counter);
  apr_wrapper_start();
log_printf(0, "after wrapper opque_count=%d\n", _opque_counter);
  init_opque_system();
log_printf(0, "after init opque_count=%d\n", _opque_counter);
  init_random();

  if (logfile != NULL) open_log(logfile);
  if (lsize != 0) set_log_maxsize(lsize);

  set_log_level(ll);

  //** Make the test_data to pluck info from
  type_malloc_clear(test_data, char, test_size);
  if (do_random == 1) get_random(test_data, test_size);

  //** Make the locking structures for client/server communication
  apr_pool_create(&mpool, NULL);
  assert(apr_thread_mutex_create(&lock, APR_THREAD_MUTEX_DEFAULT, mpool) == APR_SUCCESS);
  assert(apr_thread_cond_create(&cond, mpool) == APR_SUCCESS);

  //** Make the pipe for controlling the server
  ctx = mq_socket_context_new();
  mq_pipe_create(ctx, control_efd);

  thread_create_assert(&server_thread, NULL, server_test_thread, NULL, mpool);
  sleep(5); //** Make surethe server gets fired up
  thread_create_assert(&client_thread, NULL, client_test_thread, NULL, mpool);

  apr_thread_join(&dummy, client_thread);

  //** Trigger the server to shutdown
  c = 1;
  mq_pipe_write(control_efd[1], &c);
  apr_thread_join(&dummy, server_thread);

  mq_pipe_destroy(ctx, control_efd);
  mq_socket_context_destroy(ctx);

  apr_thread_mutex_destroy(lock);
  apr_thread_cond_destroy(cond);

  apr_pool_destroy(mpool);

  destroy_opque_system();
  apr_wrapper_stop();

  free(test_data);
  return(0);
}

