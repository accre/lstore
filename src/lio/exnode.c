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

#define _log_module_index 151

#include <string.h>
#include "list.h"
#include "exnode.h"
#include "type_malloc.h"
#include "log.h"
#include "append_printf.h"
#include "string_token.h"
#include "ex3_compare.h"
#include "ex3_system.h"

typedef struct {
  exnode_t *src_ex;
  exnode_t *dest_ex;
  data_attr_t *da;
  void *arg;
  int mode;
  int timeout;
} exnode_clone_t;

//*************************************************************************
// ex_iovec_create 
//*************************************************************************

ex_iovec_t *ex_iovec_create()
{
  ex_iovec_t *iov;
  type_malloc_clear(iov, ex_iovec_t, 1);
  return(iov);
}

//*************************************************************************
// ex_iovec_destroy
//*************************************************************************

void ex_iovec_destroy(ex_iovec_t *iov)
{
  free(iov);
}

//*************************************************************************
// exnode_[g|s]et_default - Retreives/Sets the default segment
//
//  NOTE: The default segment should also be placed in the view list!
//*************************************************************************

segment_t *exnode_get_default(exnode_t *ex) { return(ex->default_seg); }
void exnode_set_default(exnode_t *ex, segment_t *seg) { ex->default_seg = seg; }

//*************************************************************************
//  exnode_get_header - Returns a pointer to the exnode header
//*************************************************************************

ex_header_t *exnode_get_header(exnode_t *ex) { return(&(ex->header)); }

//*************************************************************************
// exnode_create - Returns an empty exnode
//*************************************************************************

exnode_t *exnode_create()
{
  exnode_t *ex;

  type_malloc_clear(ex, exnode_t, 1);

  ex_header_init(&(ex->header));

  ex->block = list_create(0, &skiplist_compare_ex_id, NULL, NULL, NULL);
  ex->view = list_create(0, &skiplist_compare_ex_id, NULL, NULL, NULL);

  return(ex);
}

//*************************************************************************
// exnode_remove_func - Resmoves the exnode data
//*************************************************************************

op_status_t exnode_remove_func(void *arg, int gid)
{
  exnode_clone_t *op = (exnode_clone_t *)arg;
  exnode_t *ex = op->src_ex;
  list_iter_t it;
  segment_t *seg;
  ex_id_t *id;
  int i, n;
  op_generic_t *gop;
  opque_t *q;
  op_status_t status;

  n = list_key_count(ex->view);
  if (n == 0) return(op_success_status);

  q = new_opque();
  opque_start_execution(q);

  //** Start the cloning process
  it = list_iter_search(ex->view, NULL, 0);
  list_next(&it, (list_key_t **)&id, (list_data_t **)&seg);
  i = 0;
  while (seg != NULL) {
    gop = segment_remove(seg, op->da, op->timeout);
    opque_add(q, gop);
    list_next(&it, (list_key_t **)&id, (list_data_t **)&seg);
  }

  //** Wait for everything to complete
  i = opque_waitall(q);

  opque_free(q, OP_DESTROY);

  status = (i != OP_STATE_SUCCESS) ? op_failure_status : op_success_status;
  return(status);
}

//*************************************************************************
// exnode_remove - Removes the exnode data
//*************************************************************************

op_generic_t *exnode_remove(thread_pool_context_t *tpc, exnode_t *ex, data_attr_t *da, int timeout)
{
  exnode_clone_t *exc;
  op_generic_t *gop;

  type_malloc_clear(exc, exnode_clone_t, 1);
  exc->src_ex = ex;
  exc->da = da;
  exc->timeout = timeout;

  gop = new_thread_pool_op(tpc, NULL, exnode_remove_func, (void *)exc, free, 1);
  return(gop);
}


//*************************************************************************
// exnode_clone_func - Clones the exnode structure and optionally data
//*************************************************************************

op_status_t exnode_clone_func(void *arg, int gid)
{
  exnode_clone_t *exc = (exnode_clone_t *)arg;
  exnode_t *sex = exc->src_ex;
  exnode_t *ex = exc->dest_ex;
  list_iter_t it;
  segment_t *src_seg, **new_seg, **segptr;
  ex_id_t *id, did;
  int i, n, nfailed;
  op_generic_t *gop;
  opque_t *q;
  op_status_t status;

  n = list_key_count(sex->view);
  if (n == 0) return(op_success_status);

  //** make space to store the segments as they are creted
  type_malloc(new_seg, segment_t *, 2*n);
  q = new_opque();
  opque_start_execution(q);

  //** Start the cloning process
  it = list_iter_search(sex->view, NULL, 0);
  list_next(&it, (list_key_t **)&id, (list_data_t **)&src_seg);
  i = 0;
  while (src_seg != NULL) {
    new_seg[i] = src_seg;
    new_seg[i+1] = NULL;  //** Need to do this soit doesn't try and use it
    gop = segment_clone(src_seg, exc->da, &(new_seg[i+1]), exc->mode, exc->arg, exc->timeout);
    gop_set_private(gop, &(new_seg[i]));
    opque_add(q, gop);
    list_next(&it, (list_key_t **)&id, (list_data_t **)&src_seg);
    i += 2;
  }

  did = segment_id(sex->default_seg);

  //** Wait for everything to complete
  nfailed = 0;
  n = opque_task_count(q);
  for (i=0; i<n; i++) {
     gop = opque_waitany(q);
     segptr = gop_get_private(gop);

     if (gop_completed_successfully(gop) != OP_STATE_SUCCESS) {
        nfailed++;
        segment_destroy(segptr[1]);
     } else {
        if (did == segment_id(segptr[0])) ex->default_seg = segptr[1];
        list_insert(ex->view, &segment_id(segptr[1]), segptr[1]);
        atomic_inc(segptr[1]->ref_count);
//err = segptr[1]->ref_count;
//log_printf(0, "new id=" XIDT " ctr=%d\n", segment_id(segptr[1]), err);
     }

     gop_free(gop, OP_DESTROY);
  }

  free(new_seg);

  opque_free(q, OP_DESTROY);

  status = (nfailed > 0) ? op_failure_status : op_success_status;
  return(status);
}

//*************************************************************************
// exnode_clone - Clones the exnode structure and optionally data
//*************************************************************************

op_generic_t *exnode_clone(thread_pool_context_t *tpc, exnode_t *src_ex, data_attr_t *da, exnode_t **ex, void *arg, int mode, int timeout)
{
  exnode_clone_t *exc;
  op_generic_t *gop;

  *ex = exnode_create();

  //** Copy the header
  if (src_ex->header.name != NULL) (*ex)->header.name = strdup(src_ex->header.name);
  if (src_ex->header.type != NULL) (*ex)->header.type = strdup(src_ex->header.type);
  generate_ex_id(&((*ex)->header.id));

  type_malloc(exc, exnode_clone_t, 1);
  exc->src_ex = src_ex;
  exc->dest_ex = *ex;
  exc->da = da;
  exc->mode = mode;
  exc->timeout = timeout;
  exc->arg = arg;

  gop = new_thread_pool_op(tpc, NULL, exnode_clone_func, (void *)exc, free, 1);
  return(gop);
}

//*************************************************************************
// exnode_exchange_free - Frees the internal memroy in the exported exnode
//     but not the exp itself
//*************************************************************************

void exnode_exchange_free(exnode_exchange_t *exp)
{
  if (exp->text.text != NULL) { free(exp->text.text); exp->text.text= NULL; }
  if (exp->text.fd != NULL) { inip_destroy(exp->text.fd); exp->text.fd = NULL; }
}

//*************************************************************************
// exnode_exchange_destroy - Same as exnode_exchange_free but also frees exp
//     as well
//*************************************************************************

void exnode_exchange_destroy(exnode_exchange_t *exp)
{
  exnode_exchange_free(exp);
  free(exp);
}

//*************************************************************************
// exnode_exchange_init - Initializes an empty exportable exnode
//*************************************************************************

void exnode_exchange_init(exnode_exchange_t *exp, int type)
{
  memset(exp, 0, sizeof(exnode_exchange_t));
  exp->type = type;
}

//*************************************************************************
// exnode_exchange_create - Returns an empty exportable exnode
//*************************************************************************

exnode_exchange_t *exnode_exchange_create(int type)
{
  exnode_exchange_t *exp;

  type_malloc(exp, exnode_exchange_t, 1);
  exnode_exchange_init(exp, type);

  return(exp);
}

//*************************************************************************
// exnode_exchange_get_default_view_id - Returns the default View/Segment ID
//*************************************************************************

ex_id_t exnode_exchange_get_default_view_id(exnode_exchange_t *exp)
{
  return(inip_get_integer(exp->text.fd, "view", "default", 0));
}

//*************************************************************************
// exnode_exchange_text_parse - Parses a text based exnode and returns it
//*************************************************************************

exnode_exchange_t *exnode_exchange_text_parse(char *text)
{
  exnode_exchange_t *exp;

  exp = exnode_exchange_create(EX_TEXT);

  exp->text.text = text;
  exp->text.fd = inip_read_text(text);

  return(exp);
}


//*************************************************************************
// exnode_exchange_create - Returns an empty exportable exnode
//*************************************************************************

exnode_exchange_t *exnode_exchange_load_file(char *fname)
{
  FILE *fd;
  char *text;
  int i;

  fd = fopen(fname, "r");
  assert(fd != NULL);
  fseek(fd, 0, SEEK_END);
  i = ftell(fd);
//  printf("exnode size=%d\n", i);
  type_malloc(text, char, i + 2);
  fseek(fd, 0, SEEK_SET);
  fread(text, i, 1, fd);
  text[i] = '\n';
  text[i+1] = '\0';
  fclose(fd);

  return(exnode_exchange_text_parse(text));
}

//*************************************************************************
// exnode_exchange_append_text - Appends the text to the text based exnode
//     proto object.  NOT used for the goole protobuf version
//*************************************************************************

void exnode_exchange_append_text(exnode_exchange_t *exp, char *buffer)
{
  char *text;
  int n;

  if (buffer == NULL) return;

  n = (exp->text.text == NULL) ? 0 : strlen(exp->text.text);

  type_malloc_clear(text, char, n + strlen(buffer) + 3);
  if (n == 0) {
    sprintf(text, "%s", buffer);
    exp->text.text = text;
  } else {
    sprintf(text, "%s\n%s", exp->text.text, buffer);
    free(exp->text.text);
    exp->text.text = text;
  }

}

//*************************************************************************
// exnode_exchange_append - Appends the text to the text based exnode
//     proto object.  NOT used for the goole protobuf version
//*************************************************************************

void exnode_exchange_append(exnode_exchange_t *exp, exnode_exchange_t *exp_append)
{
  if (exp_append->text.text == NULL) return;
  exnode_exchange_append_text(exp, exp_append->text.text);
}

//*************************************************************************
//  exnode_deserialize_text - Storea a text based exnode
//*************************************************************************

int exnode_deserialize_text(exnode_t *ex, exnode_exchange_t *exp, service_manager_t *ess)
{
  inip_group_t *g;
  inip_element_t *ele;
  inip_file_t *fd;
  segment_t *seg = NULL;
  ex_id_t id;
  int fin;
  char *key, *value, *token, *bstate;
  char *exgrp = "exnode";

  fd = exp->text.fd;

  //** Load the header
  g = inip_find_group(fd, exgrp);
  if (g != NULL) {
     ex->header.name =  inip_get_string(fd, exgrp, "name", "");
     ex->header.id = inip_get_integer(fd, exgrp, "id", 0);
  }

  //** and the views
  g = inip_find_group(fd, "view");
  if (g == NULL) {
     log_printf(1, "exnode_deserialize_text: No views found!\n");
     return(1);
  }

  ele = inip_first_element(g);
  while (ele != NULL) {
     key = inip_get_element_key(ele);
     if (strcmp(key, "segment") == 0) {

        //** Parse the segment line
        value = inip_get_element_value(ele);
        token = strdup(value);
        id = 0;
        sscanf(string_token(token, ":", &bstate, &fin), XIDT, &id);
        free(token);

        //** and load it
        log_printf(15, "exnode_load_text: Loading view segment " XIDT "\n", id);
        seg = load_segment(ess, id, exp);
        if (seg != NULL) {
           atomic_inc(seg->ref_count);
           list_insert(ex->view, &segment_id(seg), seg);
        } else {
           log_printf(0, "Bad segment!  sid=" XIDT "\n", id);
        }
     }

     //** Move to the next segmnet to load
     ele = inip_next_element(ele);
  }

  //** Now get the default segment to use
  id = inip_get_integer(fd, "view", "default", 0);
  if (id == 0) {   //** No default so use the last one loaded
     ex->default_seg = seg;
  } else {
    ex->default_seg = list_search(ex->view, &id);
  }

  return((ex->default_seg == NULL) ? 1 : 0);
}

//*************************************************************************
// exnode_deserialize_proto - Deserializes the exnode from a google protobuf
//*************************************************************************

int exnode_deserialize_proto(exnode_t *ex, exnode_exchange_t *exp, service_manager_t *ess)
{
  return(-1);
}

//*************************************************************************
// exnode_deserialize - Deserializes the exnode
//*************************************************************************

int exnode_deserialize(exnode_t *ex, exnode_exchange_t *exp, service_manager_t *ess)
{
  if (exp->type == EX_TEXT) {
     return(exnode_deserialize_text(ex, exp, ess));
  } else if (exp->type == EX_PROTOCOL_BUFFERS) {
     return(exnode_deserialize_proto(ex, exp, ess));
  }

  return(-1);
}


//*************************************************************************
//  exnode_serialize_text - Exports a text based exnode
//*************************************************************************

int exnode_serialize_text(exnode_t *ex, exnode_exchange_t *exp)
{
  int bufsize = 1024;
  char buffer[bufsize];
  char *etext;
  int used = 0;
  segment_t *seg;
  ex_id_t *id;
  skiplist_iter_t it;

  //** Store the header
  append_printf(buffer, &used, bufsize, "[exnode]\n");
  if (ex->header.name != NULL) {
     if (strcmp(ex->header.name, "") != 0) {
        etext = escape_text("=", '\\', ex->header.name);
        append_printf(buffer, &used, bufsize, "name=%s\n", etext);
        free(etext);
     }
  }
  append_printf(buffer, &used, bufsize, "id=" XIDT "\n\n", ex->header.id);

  exnode_exchange_append_text(exp, buffer);

  //** and all the views
  used = 0;
  append_printf(buffer, &used, bufsize, "\n[view]\n");
  if (ex->default_seg != NULL) append_printf(buffer, &used, bufsize, "default=" XIDT "\n", segment_id(ex->default_seg));
  it = list_iter_search(ex->view, (skiplist_key_t *)NULL, 0);
  while (list_next(&it, (skiplist_key_t **)&id, (skiplist_data_t **)&seg) == 0) {
     log_printf(15, "exnode_serialize_text: Storing view segment " XIDT "\n", segment_id(seg));
     append_printf(buffer, &used, bufsize, "segment=" XIDT "\n", *id);

     segment_serialize(seg, exp);
  }

  append_printf(buffer, &used, bufsize, "\n");

  exnode_exchange_append_text(exp, buffer);

  return(0);
}

//*************************************************************************
// exnode_serialize_proto - Serializes the exnode to a google protobuf
//*************************************************************************

int exnode_serialize_proto(exnode_t *ex, exnode_exchange_t *exp)
{
  return(-1);
}

//*************************************************************************
// exnode_serialize - Serializes the exnode
//*************************************************************************

int exnode_serialize(exnode_t *ex, exnode_exchange_t *exp)
{
  if (exp->type == EX_TEXT) {
     return(exnode_serialize_text(ex, exp));
  } else if (exp->type == EX_PROTOCOL_BUFFERS) {
     return(exnode_serialize_proto(ex, exp));
  }
 
  return(-1);
}


//*************************************************************************
// exnode_destroy - Frees the memory associated with an exnode
//*************************************************************************

void exnode_destroy(exnode_t *ex)
{
  list_iter_t it;
  segment_t *seg;
  data_block_t *b;
  ex_id_t id;

  //** Remove the views
  it = list_iter_search(ex->view, (skiplist_key_t *)NULL, 0);
  while (list_next(&it, (skiplist_key_t *)&id, (skiplist_data_t *)&seg) == 0) {
    atomic_dec(seg->ref_count);
log_printf(15, "exnode_destroy: seg->id=" XIDT " ref_count=%d\n", segment_id(seg), seg->ref_count);
    segment_destroy(seg);
    list_next(&it, (skiplist_key_t *)&id, (skiplist_data_t *)&seg);
  }

  //** And any blocks
  it = list_iter_search(ex->block, (skiplist_key_t *)NULL, 0);
  while (list_next(&it, (skiplist_key_t *)&id, (skiplist_data_t *)&b) == 0) {
    data_block_destroy(b);
    list_next(&it, (skiplist_key_t *)&id, (skiplist_data_t *)&b);
  }

  list_destroy(ex->view);
  list_destroy(ex->block);

  ex_header_release(&(ex->header));
  
  free(ex);

  return;
}


