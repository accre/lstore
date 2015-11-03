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
// Remote OS implementation for the client side
//***********************************************************************

#define _log_module_index 215

#include "ex3_system.h"
#include "object_service_abstract.h"
#include "type_malloc.h"
#include "log.h"
#include "atomic_counter.h"
#include "thread_pool.h"
#include "resource_service_abstract.h"
#include "rs_simple.h"
#include "rs_remote.h"
#include "rs_remote_priv.h"
#include "append_printf.h"
#include "type_malloc.h"
#include "random.h"
#include "rs_query_base.h"
#include "mq_portal.h"
#include "apr_wrapper.h"

typedef struct {
    uint64_t id;
    int mode;
//  char **config;
    resource_service_fn_t *rs;
}  rsrc_gop_rid_config_t;

op_generic_t *rsrc_update_config_op(resource_service_fn_t *rs, int mode, int timeout);


//***********************************************************************
// rsrc_get_rid_config - Gets the RID configuration
//    For this we always gethte latest from the remote server cause it
//    has the latest usage numbers.
//***********************************************************************

char *rsrc_get_rid_config(resource_service_fn_t *rs)
{
    rs_remote_client_priv_t *rsrc = (rs_remote_client_priv_t *)rs->priv;

    return(rs_get_rid_config(rsrc->rs_child));
}

//***********************************************************************
// rsrc_get_rid_value - Gets the RID value from
//***********************************************************************

char *rsrc_get_rid_value(resource_service_fn_t *rs, char *rid_key, char *key)
{
    rs_remote_client_priv_t *rsrc = (rs_remote_client_priv_t *)rs->priv;

    return(rs_get_rid_value(rsrc->rs_child, rid_key, key));
}

//***********************************************************************
// rsrc_mapping_register - Registration for mapping updates
//***********************************************************************

void rsrc_mapping_register(resource_service_fn_t *rs, rs_mapping_notify_t *map_version)
{
    rs_remote_client_priv_t *rsrc = (rs_remote_client_priv_t *)rs->priv;

    rs_register_mapping_updates(rsrc->rs_child, map_version);

    return;
}

//***********************************************************************
// rsrc_mapping_unregister - UnRegisters for mapping updates
//***********************************************************************

void rsrc_mapping_unregister(resource_service_fn_t *rs, rs_mapping_notify_t *map_version)
{
    rs_remote_client_priv_t *rsrc = (rs_remote_client_priv_t *)rs->priv;

    rs_unregister_mapping_updates(rsrc->rs_child, map_version);

    return;
}

//***********************************************************************
// rsrc_translate_cap_set - Translates the cap set
//***********************************************************************

void rsrc_translate_cap_set(resource_service_fn_t *rs, char *rid_key, data_cap_set_t *cs)
{
    rs_remote_client_priv_t *rsrc = (rs_remote_client_priv_t *)rs->priv;

    rs_translate_cap_set(rsrc->rs_child, rid_key, cs);

    return;
}

//***********************************************************************
// rsrc_get_rid_value - Returns the value associated with ther RID key
//    provided
//***********************************************************************

char *rsrc_simple_get_rid_value(resource_service_fn_t *rs, char *rid_key, char *key)
{
    rs_remote_client_priv_t *rsrc = (rs_remote_client_priv_t *)rs->priv;

    return(rs_get_rid_value(rsrc->rs_child, rid_key, key));
}

//***********************************************************************
// rsrc_data_request - Processes a RS data request
//***********************************************************************

op_generic_t *rsrc_data_request(resource_service_fn_t *rs, data_attr_t *da, rs_query_t *rsq, data_cap_set_t **caps, rs_request_t *req, int req_size, rs_hints_t *hints_list, int fixed_size, int n_rid, int ignore_fixed_err, int timeout)
{
    rs_remote_client_priv_t *rsrc = (rs_remote_client_priv_t *)rs->priv;

    return(rs_data_request(rsrc->rs_child, da, rsq, caps, req, req_size, hints_list, fixed_size, n_rid, ignore_fixed_err, timeout));
}

//***********************************************************************
// rsrc_response_get_config - Processes the new config received
//   mode = 0  -- Get config and update now
//   mode = 1  -- Wait for changes and update  config != NULL
//***********************************************************************

op_status_t rsrc_response_get_config(void *task_arg, int tid)
{
    mq_task_t *task = (mq_task_t *)task_arg;
    rsrc_gop_rid_config_t *arg;
    rs_remote_client_priv_t *rsrc;
    mq_frame_t *f;
    mq_msg_t *msg;
    char dt[128];
    char *data, *config;
    uint64_t *id;
    int n, n_config, err;
    op_status_t status;
    FILE *fd;
    char *fname_tmp;

    log_printf(5, "Processing rid_config response gid=%d\n", gop_id(task->gop));

    status = op_success_status;
    arg = gop_get_private(task->gop);
    rsrc = (rs_remote_client_priv_t *)arg->rs->priv;

    //** Parse the response
    msg = task->response;

    f = mq_msg_first(msg);
    mq_get_frame(f, (void **)&data, &n);
    if (n != 0) {  //** SHould be an empty frame
        log_printf(0, " ERROR:  Missing initial empty frame!\n");
        status = op_failure_status;
        goto fail;
    }
    f = mq_msg_next(msg);
    mq_get_frame(f, (void **)&data, &n);
    if (mq_data_compare(data, n, MQF_VERSION_KEY, MQF_VERSION_SIZE) != 0) {
        log_printf(0, "ERROR:  Missing version frame!\n");
        status = op_failure_status;
        goto fail;
    }
    f = mq_msg_next(msg);
    mq_get_frame(f, (void **)&data, &n);
    if (mq_data_compare(data, n, MQF_RESPONSE_KEY, MQF_RESPONSE_SIZE) != 0) {
        log_printf(0, " ERROR: Bad RESPONSE command frame\n");
        status = op_failure_status;
        goto fail;
    }
    f = mq_msg_next(msg);
    mq_get_frame(f, (void **)&id, &n);
    if (n != sizeof(uint64_t)) {
        log_printf(0, " ERROR: Bad ID size!  Got %d should be sizeof(uint64_t)=%d\n", n, sizeof(uint64_t));
        status = op_failure_status;
        goto fail;
    }

    //** Version frame
    f = mq_msg_next(msg);
    mq_get_frame(f, (void **)&data, &n);
    if (n == 0) {
        log_printf(0, " ERROR: Missing version!\n");
        status = op_failure_status;
        goto fail;
    }
    data[n-1] = '\0';  //** The last character in a '\n' so replace it with a NULL terminator
    apr_thread_mutex_lock(rsrc->lock);
    sscanf(data, "%d %d", &(rsrc->version.map_version), &(rsrc->version.status_version));
    apr_thread_mutex_unlock(rsrc->lock);
    log_printf(5, "version=%s\n", data);

    //** Config frame
    f = mq_msg_next(msg);
    mq_get_frame(f, (void **)&config, &n_config);
    if ((n_config == 0) && (arg->mode == 1)) {
        log_printf(0, " ERROR: Empty config!\n");
        status = op_failure_status;
        goto fail;
    }
    log_printf(5, "rid_config_len=%d\n", n_config);

    if (arg->id != *id) {
        log_printf(0, " ERROR: ID mismatch! id=" LU " gid=" LU "\n", *id, arg->id);
        status = op_failure_status;
        goto fail;
    }
    log_printf(5, "mqid=%s\n", mq_id2str((char *)&(arg->id), sizeof(uint64_t), dt, sizeof(dt)));

    f = mq_msg_next(msg);
    mq_get_frame(f, (void **)&data, &n);
    if (n != 0) {  //** SHould be an empty frame
        log_printf(0, " ERROR:  Final initial empty frame!\n");
        status = op_failure_status;
        goto fail;
    }

    //** Store the config if changed.  This should trigger the child RS to auto load
//  if ((n_config > 0) && (arg->mode > 0)) {
    if (n_config > 0) {
        //** Dump the data in a temp file
        n = strlen(rsrc->child_target_file)+10;
        type_malloc(fname_tmp, char, n);
        snprintf(fname_tmp, n, "%s.tmp", rsrc->child_target_file);

        fd = fopen(fname_tmp, "w");
        assert_result(fwrite(config, n_config, 1, fd), 1);
        fclose(fd);

        //** Now move it into the place of the child target
        err = apr_file_rename(fname_tmp, rsrc->child_target_file, rsrc->mpool);
        if (err != APR_SUCCESS) {
            status = op_failure_status;
            log_printf(0, "ERROR: updating target file!  tmp=%s targe=%s err=%d\n", fname_tmp, rsrc->child_target_file, err);
            fprintf(stderr, "ERROR: updating target file!  tmp=%s targe=%s err=%d\n", fname_tmp, rsrc->child_target_file, err);
        }
        free(fname_tmp);
    }

    //** Clean up
fail:
    log_printf(5, "END gid=%d status=%d\n", gop_id(task->gop), status.op_status);

    return(status);
}

//***********************************************************************
// rsrc_update_config_op - Generates an update/get RID config
//***********************************************************************

op_generic_t *rsrc_update_config_op(resource_service_fn_t *rs, int mode, int timeout)
{
    rs_remote_client_priv_t *rsrc = (rs_remote_client_priv_t *)rs->priv;
    mq_msg_t *msg;
    char dt[128];
    rsrc_gop_rid_config_t *arg;
    op_generic_t *gop;

    type_malloc_clear(arg, rsrc_gop_rid_config_t, 1);

    //** Form the message
    get_random(&(arg->id), sizeof(arg->id));
    if (mode != 0) rsrc->update_id = arg->id;  //** Only update the id for an actual wait and update
    msg = mq_msg_new();
    mq_msg_append_mem(msg, rsrc->host_remote_rs, strlen(rsrc->host_remote_rs), MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, MQF_TRACKEXEC_KEY, MQF_TRACKEXEC_SIZE, MQF_MSG_KEEP_DATA);

    mq_msg_append_mem(msg, &(arg->id), sizeof(uint64_t), MQF_MSG_KEEP_DATA);

    if (mode == 0) {
        mq_msg_append_mem(msg, RSR_GET_RID_CONFIG_KEY, RSR_GET_RID_CONFIG_SIZE, MQF_MSG_KEEP_DATA);
    } else {
        mq_msg_append_mem(msg, RSR_GET_UPDATE_CONFIG_KEY, RSR_GET_UPDATE_CONFIG_SIZE, MQF_MSG_KEEP_DATA);
        snprintf(dt, sizeof(dt), "%d\n", timeout);
        mq_msg_append_mem(msg, strdup(dt), strlen(dt), MQF_MSG_AUTO_FREE);
        snprintf(dt, sizeof(dt), "%d %d\n", rsrc->version.map_version, rsrc->version.status_version);
        mq_msg_append_mem(msg, strdup(dt), strlen(dt), MQF_MSG_AUTO_FREE);
    }
    mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    arg->rs = rs;
    arg->mode = mode;

    //** Make the gop
    gop = new_mq_op(rsrc->mqc, msg, rsrc_response_get_config, arg, free, timeout);
    gop_set_private(gop, arg);

    log_printf(5, "mqid=%s timeout=%d gid=%d\n", mq_id2str((char *)&(arg->id), sizeof(uint64_t), dt, sizeof(dt)), timeout, gop_id(gop));

    return(gop);
}

//***********************************************************************
// _rsrc_update_config - Updates the RID config and pushes the changes
//     to the unerlying child RS.
//     NOTE:  This does not do any locking on the current RS
//***********************************************************************

int _rsrc_update_config(resource_service_fn_t *rs)
{
    int err;
    op_generic_t *gop;

    //** Generate the command
    gop = rsrc_update_config_op(rs, 0, 60);

    //** Send it and wait for a response
    log_printf(5, "Sending update request gid=%d\n", gop_id(gop));
    err = gop_waitall(gop);
    log_printf(5, "Completed update request. gid=%d\n", gop_id(gop));

    gop_free(gop, OP_DESTROY);

    return((err == OP_STATE_SUCCESS) ? 0 : 1);
}

//***********************************************************************
// _rsrc_update_abort - Aborts an update command
//***********************************************************************

void _rsrc_update_abort(resource_service_fn_t *rs)
{
    rs_remote_client_priv_t *rsrc = (rs_remote_client_priv_t *)rs->priv;
    mq_msg_t *msg;
    op_generic_t *gop;
    uint64_t update_id;
    char dt[128];

    update_id = rsrc->update_id;

    if (update_id == 0) return;

    log_printf(5, "aborting mqid=%s\n", mq_id2str((char *)&update_id, sizeof(uint64_t), dt, sizeof(dt)));

    //** Form the message
    msg = mq_msg_new();
    mq_msg_append_mem(msg, rsrc->host_remote_rs, strlen(rsrc->host_remote_rs), MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, MQF_EXEC_KEY, MQF_EXEC_SIZE, MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, &update_id, sizeof(uint64_t), MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, RSR_ABORT_KEY, RSR_ABORT_SIZE, MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    //** Make the gop
    gop = new_mq_op(rsrc->mqc, msg, NULL, NULL, free, 60);

    //** And execute it
    gop_waitany(gop);
    gop_free(gop, OP_DESTROY);
}

//***********************************************************************
// rsrc_check_thread - Monitors the remote server for updates
//***********************************************************************

void *rsrc_check_thread(apr_thread_t *th, void *data)
{
    resource_service_fn_t *rs = (resource_service_fn_t *)data;
    rs_remote_client_priv_t *rsrc = (rs_remote_client_priv_t *)rs->priv;
    op_generic_t *gop, *g;
    op_status_t status;
    int n;

    n = 0;
    gop = NULL;
    do {
        if (gop == NULL) {
            gop = rsrc_update_config_op(rs, 1, rsrc->check_interval);
        }
        log_printf(15, "before gop_timed_waitany gid=%d timeout=%d\n", gop_id(gop), rsrc->check_interval);
        g = gop_timed_waitany(gop, 1);
        log_printf(15, "after gop_waitany g=%p\n", g);
        flush_log();

        apr_thread_mutex_lock(rsrc->lock);
        n = rsrc->shutdown;
        apr_thread_mutex_unlock(rsrc->lock);

        if (g != NULL) {
            status = gop_get_status(gop);
            log_printf(15, "update completed status=%d\n", status.op_status);
            flush_log();
            gop_free(gop, OP_DESTROY);
            gop = NULL;
        }
        log_printf(15, "loop end n=%d gop=%p\n", n, gop);
    } while (n == 0);

    //** Still have a pending GOP so abort it
    if (gop != NULL) {
        _rsrc_update_abort(rs);
        op_generic_t *g = gop_timed_waitany(gop, 10);
        if (g) {
            gop_free(gop, OP_DESTROY);
        } else {
            log_printf(0, "Aborting RS update\n");
        }
    }

    log_printf(15, "EXITING\n");

    return(NULL);
}

//***********************************************************************
// rs_remote_client_destroy
//***********************************************************************

void rs_remote_client_destroy(resource_service_fn_t *rs)
{
    rs_remote_client_priv_t *rsrc = (rs_remote_client_priv_t *)rs->priv;
    apr_status_t dummy;

    //** Shutdown the check thread
    apr_thread_mutex_lock(rsrc->lock);
    rsrc->shutdown = 1;
    log_printf(15, "SHUTDOWN rsrc->shutdown=%d\n", rsrc->shutdown);
    flush_log();

    _rsrc_update_abort(rs);  //** Abort any pending check
    apr_thread_mutex_unlock(rsrc->lock);
    apr_thread_join(&dummy, rsrc->check_thread);

    //** Shutdown the Remote RS if enabled
    if (rsrc->rrs_test != NULL) rs_destroy_service(rsrc->rrs_test);

    //** Same goes for the child RS
    rs_destroy_service(rsrc->rs_child);

    //** Now do the normal cleanup
    apr_pool_destroy(rsrc->mpool);
    free(rsrc->host_remote_rs);
    free(rsrc->child_target_file);
    free(rsrc);
    free(rs);
}


//***********************************************************************
//  rs_remote_client_create - Creates a remote client RS
//***********************************************************************

resource_service_fn_t *rs_remote_client_create(void *arg, inip_file_t *fd, char *section)
{
    service_manager_t *ess = (service_manager_t *)arg;
    resource_service_fn_t *rs;
    rs_remote_client_priv_t *rsrc;
    rs_create_t *rs_create;
    char *stype, *ctype;

    if (section == NULL) section = "rs_remote_client";

    type_malloc_clear(rs, resource_service_fn_t, 1);
    type_malloc_clear(rsrc, rs_remote_client_priv_t, 1);
    rs->priv = (void *)rsrc;

    //** Make the locks and cond variables
    assert_result(apr_pool_create(&(rsrc->mpool), NULL), APR_SUCCESS);
    apr_thread_mutex_create(&(rsrc->lock), APR_THREAD_MUTEX_DEFAULT, rsrc->mpool);
    apr_thread_cond_create(&(rsrc->cond), rsrc->mpool);

    //** Now get the other params
    rsrc->child_target_file = inip_get_string(fd, section, "child_fname", NULL);
    rsrc->host_remote_rs = inip_get_string(fd, section, "remote_address", NULL);
    rsrc->dynamic_mapping = inip_get_integer(fd, section, "dynamic_mapping", 0);
    rsrc->check_interval = inip_get_integer(fd, section, "check_interval", 3600);

    //** Get the MQC
    rsrc->mqc = lookup_service(ess, ESS_RUNNING, ESS_MQ); assert(rsrc->mqc != NULL);

    //** Check if we are running the remote RS locally.  This means we are doing testing
    stype = inip_get_string(fd, section, "rrs_test", NULL);
    if (stype != NULL) {
        ctype = inip_get_string(fd, stype, "type", RS_TYPE_SIMPLE);
        rs_create = lookup_service(ess, RS_SM_AVAILABLE, ctype);
        rsrc->rrs_test = (*rs_create)(ess, fd, stype);
        if (rsrc->rrs_test == NULL) {
            log_printf(1, "ERROR loading test RRS!  type=%s section=%s\n", ctype, stype);
            flush_log();
            abort();
        }
        free(ctype);
        free(stype);
    }

    //** Contact the Remote RS and get the initial config
    if (_rsrc_update_config(rs) != 0) {
        log_printf(0, "ERROR: Remote RS is down!  section=%s remote_host=%s!\n", section, rsrc->host_remote_rs);
        flush_log();
        abort();
    }

    //** Start the child RS.   The update above should have dumped a RID config for it to load
    stype = inip_get_string(fd, section, "rs_local", NULL);
    if (stype == NULL) {  //** Oops missing child RS
        log_printf(0, "ERROR: Mising child RS  section=%s key=rs_local!\n", section);
        flush_log();
        free(stype);
        abort();
    }

    //** and load it
    ctype = inip_get_string(fd, stype, "type", RS_TYPE_SIMPLE);
    rs_create = lookup_service(ess, RS_SM_AVAILABLE, ctype);
    rsrc->rs_child = (*rs_create)(ess, fd, stype);
    if (rsrc->rs_child == NULL) {
        log_printf(1, "ERROR loading child RS!  type=%s section=%s\n", ctype, stype);
        flush_log();
        abort();
    }
    free(ctype);
    free(stype);

    //** Launch the config changes thread
    thread_create_assert(&(rsrc->check_thread), NULL, rsrc_check_thread, (void *)rs, rsrc->mpool);


    //** Set up the fn ptrs
    rs->get_rid_config = rsrc_get_rid_config;
    rs->register_mapping_updates = rsrc_mapping_register;
    rs->unregister_mapping_updates = rsrc_mapping_unregister;
    rs->translate_cap_set = rsrc_translate_cap_set;
    rs->get_rid_value = rsrc_get_rid_value;
    rs->data_request = rsrc_data_request;
    rs->destroy_service = rs_remote_client_destroy;

    rs->query_new = rs_query_base_new;
    rs->query_dup = rs_query_base_dup;
    rs->query_add = rs_query_base_add;
    rs->query_append = rs_query_base_append;
    rs->query_destroy = rs_query_base_destroy;
    rs->query_print = rs_query_base_print;
    rs->query_parse = rs_query_base_parse;

    rs->type = RS_TYPE_REMOTE_CLIENT;

    return(rs);
}

