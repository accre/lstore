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

//***********************************************************************
// Provides MQ helper routines
//***********************************************************************

#define _log_module_index 213

#include "mq_helpers.h"
#include "atomic_counter.h"
#include "type_malloc.h"
#include "varint.h"
#include "log.h"
#include "string_token.h"

static atomic_int_t _id_counter = 0;

//***********************************************************************
// mq_make_id_frame - Makes and generates and ID frame
//***********************************************************************

mq_frame_t *mq_make_id_frame()
{
  atomic_int_t *id;

  type_malloc(id, atomic_int_t, 1);

  *id = atomic_inc(_id_counter);

  if (*id > 1000000000) { atomic_set(_id_counter, 0); }

  return(mq_frame_new(id, sizeof(atomic_int_t), MQF_MSG_AUTO_FREE));
}


//***********************************************************************
// mq_read_status_frame - Processes a status frame
//***********************************************************************

op_status_t mq_read_status_frame(mq_frame_t *f, int destroy)
{
  char *data;
  int nbytes, n;
  int64_t value;
  op_status_t status;

  mq_get_frame(f, (void **)&data, &nbytes);

  n = zigzag_decode((unsigned char *)data, nbytes, &value);  status.op_status = value;
  zigzag_decode((unsigned char *)&(data[n]), nbytes-n, &value);  status.error_code = value;

  if (destroy == 1) mq_frame_destroy(f);

  return(status);
}

//***********************************************************************
// mq_make_status_frame -Creates a status frame
//***********************************************************************

mq_frame_t *mq_make_status_frame(op_status_t status)
{
  unsigned char buffer[128];
  unsigned char *bytes;
  int n;

   n = zigzag_encode(status.op_status, buffer);
   n = n + zigzag_encode(status.error_code, &(buffer[n]));
   type_malloc(bytes, unsigned char, n);
   memcpy(bytes, buffer, n);
   return(mq_frame_new(bytes, n, MQF_MSG_AUTO_FREE));
}

//***********************************************************************
// mq_remove_header - Removes the header from the message.
//***********************************************************************

void mq_remove_header(mq_msg_t *msg, int drop_extra)
{
  int i;

  mq_msg_first(msg);                  //** Move to the 1st frame
  mq_frame_destroy(mq_msg_pop(msg));  //** Drop the NULL frame
  mq_frame_destroy(mq_msg_pop(msg));  //** Drop the version frame
  mq_frame_destroy(mq_msg_pop(msg));  //** Drop the MQ command frame

  for (i=0; i<drop_extra; i++) { mq_frame_destroy(mq_msg_pop(msg)); }
}

//***********************************************************************
// mq_make_exec_core_msg - Makes the EXEC/TRACKEXEC message core
//***********************************************************************

mq_msg_t *mq_make_exec_core_msg(mq_msg_t *address, int do_track)
{
  mq_msg_t *msg;
  
  msg = mq_msg_new();

  mq_msg_append_msg(msg, address, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(msg, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
  if (do_track) {
     mq_msg_append_mem(msg, MQF_TRACKEXEC_KEY, MQF_TRACKEXEC_SIZE, MQF_MSG_KEEP_DATA);
  } else {
     mq_msg_append_mem(msg, MQF_EXEC_KEY, MQF_EXEC_SIZE, MQF_MSG_KEEP_DATA);
  }
  mq_msg_append_frame(msg, mq_make_id_frame());

  return(msg);
}


//***********************************************************************
// mq_make_response_core_msg - Makes the RESPONSE message core
//***********************************************************************

mq_msg_t *mq_make_response_core_msg(mq_msg_t *address, mq_frame_t *fid)
{
  mq_msg_t *response;

  response = mq_msg_new();
  mq_msg_append_mem(response, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
  mq_msg_append_mem(response, MQF_RESPONSE_KEY, MQF_RESPONSE_SIZE, MQF_MSG_KEEP_DATA);
  mq_msg_append_frame(response, fid);

  //** Now address it
  mq_apply_return_address_msg(response, address, 0);

  return(response);
}

//***********************************************************************
// mq_num_frames - Returns the number of frames in the message
//***********************************************************************

int mq_num_frames(mq_msg_t *msg) {
  mq_frame_t *f;
  int n;

  for(f = mq_msg_first(msg), n = 0; f != NULL; f = mq_msg_next(msg), n++);
	
  return n;
}

//***********************************************************************
// mq_address_to_string - Converts a message to a comma-separated string
//***********************************************************************

char *mq_address_to_string(mq_msg_t *address) {
  mq_frame_t *f;
  int msg_size, frames, n, size;
  char *string, *data;
	
  msg_size = mq_msg_total_size(address); // sum of frame data lengths
  frames = mq_num_frames(address);
  n = 0;
  size = 0;	
	
  string = malloc(msg_size + frames);
	
  for (f = mq_msg_first(address); f != NULL; f = mq_msg_next(address)) {
     mq_get_frame(f, (void **)&data, &size);
     memcpy(string + n, data, size);
     n += size;
     if(size == 0) break;
     *(string + (n++)) = ',';
  }
  *(string + (--n)) = 0; // remove the trailing comma and make this the end of the string
	
  // For testing:
  log_printf(0, "DEBUG: string created = %s, malloc size = %d, actual size = %d\n", string, (msg_size+frames+10), strlen(string));

  return(string);
}

//***********************************************************************
// mq_string_to_address - Converts a comma-separated string to a message
//  ***NOTE: The input string is MODIFIED!!!!!!*****
//***********************************************************************

mq_msg_t *mq_string_to_address(char *string) {
  int fin;
  char *token;
  mq_msg_t *address;
  char *bstate;

  if (string == NULL) return(NULL);

  address = mq_msg_new();
  token = string_token(string, ",", &bstate, &fin);
  while(fin == 0) {
     log_printf(5, "host frame=%s\n", token);
     mq_msg_append_mem(address, token, strlen(token), MQF_MSG_KEEP_DATA);
     token = string_token(NULL, ",", &bstate, &fin);
  }
	
  return address;
}
