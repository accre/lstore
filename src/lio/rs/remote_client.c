/*
   Copyright 2016 Vanderbilt University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

//***********************************************************************
// Remote OS implementation for the client side
//***********************************************************************

#define _log_module_index 215

#include <apr_errno.h>
#include <apr_file_io.h>
#include <apr_pools.h>
#include <apr_thread_cond.h>
#include <apr_thread_mutex.h>
#include <apr_thread_proc.h>
#include <assert.h>
#include <gop/gop.h>
#include <gop/mq.h>
#include <gop/types.h>
#include <stdint.h>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/unistd.h>
#include <tbx/apr_wrapper.h>
#include <tbx/assert_result.h>
#include <tbx/fmttypes.h>
#include <tbx/iniparse.h>
#include <tbx/log.h>
#include <tbx/random.h>
#include <tbx/type_malloc.h>
#include <unistd.h>

#include "ds.h"
#include "ex3/system.h"
#include "rs.h"
#include "rs/query_base.h"
#include "rs/remote.h"
#include "rs/simple.h"
#include "service_manager.h"

static lio_rs_remote_client_priv_t rsrc_default_options = {
    .section = "rs_remote_client",
    .child_target_file = "/lio/cfg/rid-client.cfg",
    .host_remote_rs = "${rsrc_host}",
    .dynamic_mapping = 0,
    .check_interval = 3600,
    .local_child_section = "rs_simple",
    .delete_target = 1
};

typedef struct {
    uint64_t id;
    int mode;
    lio_resource_service_fn_t *rs;
}  rsrc_gop_rid_config_t;

gop_op_generic_t *rsrc_update_config_op(lio_resource_service_fn_t *rs, int mode, int timeout);


//***********************************************************************
// rsrc_get_rid_config - Gets the RID configuration
//    For this we always gethte latest from the remote server cause it
//    has the latest usage numbers.
//***********************************************************************

char *rsrc_get_rid_config(lio_resource_service_fn_t *rs)
{
    lio_rs_remote_client_priv_t *rsrc = (lio_rs_remote_client_priv_t *)rs->priv;

    return(rs_get_rid_config(rsrc->rs_child));
}

//***********************************************************************
// rsrc_get_rid_value - Gets the RID value from
//***********************************************************************

char *rsrc_get_rid_value(lio_resource_service_fn_t *rs, char *rid_key, char *key)
{
    lio_rs_remote_client_priv_t *rsrc = (lio_rs_remote_client_priv_t *)rs->priv;

    return(rs_get_rid_value(rsrc->rs_child, rid_key, key));
}

//***********************************************************************
// rsrc_mapping_register - Registration for mapping updates
//***********************************************************************

void rsrc_mapping_register(lio_resource_service_fn_t *rs, lio_rs_mapping_notify_t *map_version)
{
    lio_rs_remote_client_priv_t *rsrc = (lio_rs_remote_client_priv_t *)rs->priv;

    rs_register_mapping_updates(rsrc->rs_child, map_version);

    return;
}

//***********************************************************************
// rsrc_mapping_unregister - UnRegisters for mapping updates
//***********************************************************************

void rsrc_mapping_unregister(lio_resource_service_fn_t *rs, lio_rs_mapping_notify_t *map_version)
{
    lio_rs_remote_client_priv_t *rsrc = (lio_rs_remote_client_priv_t *)rs->priv;

    rs_unregister_mapping_updates(rsrc->rs_child, map_version);

    return;
}

//***********************************************************************
// rsrc_translate_cap_set - Translates the cap set
//***********************************************************************

void rsrc_translate_cap_set(lio_resource_service_fn_t *rs, char *rid_key, data_cap_set_t *cs)
{
    lio_rs_remote_client_priv_t *rsrc = (lio_rs_remote_client_priv_t *)rs->priv;

    rs_translate_cap_set(rsrc->rs_child, rid_key, cs);

    return;
}

//***********************************************************************
// rsrc_get_rid_value - Returns the value associated with ther RID key
//    provided
//***********************************************************************

char *rsrc_simple_get_rid_value(lio_resource_service_fn_t *rs, char *rid_key, char *key)
{
    lio_rs_remote_client_priv_t *rsrc = (lio_rs_remote_client_priv_t *)rs->priv;

    return(rs_get_rid_value(rsrc->rs_child, rid_key, key));
}

//***********************************************************************
// rsrc_data_request - Processes a RS data request
//***********************************************************************

gop_op_generic_t *rsrc_data_request(lio_resource_service_fn_t *rs, data_attr_t *da, rs_query_t *rsq, data_cap_set_t **caps, lio_rs_request_t *req, int req_size, lio_rs_hints_t *hints_list, int fixed_size, int n_rid, int ignore_fixed_err, int timeout)
{
    lio_rs_remote_client_priv_t *rsrc = (lio_rs_remote_client_priv_t *)rs->priv;

    return(rs_data_request(rsrc->rs_child, da, rsq, caps, req, req_size, hints_list, fixed_size, n_rid, ignore_fixed_err, timeout));
}

//***********************************************************************
// rsrc_response_get_config - Processes the new config received
//   mode = 0  -- Get config and update now
//   mode = 1  -- Wait for changes and update  config != NULL
//***********************************************************************

gop_op_status_t rsrc_response_get_config(void *task_arg, int tid)
{
    gop_mq_task_t *task = (gop_mq_task_t *)task_arg;
    rsrc_gop_rid_config_t *arg;
    lio_rs_remote_client_priv_t *rsrc;
    gop_mq_frame_t *f;
    mq_msg_t *msg;
    char dt[128];
    char *data, *config;
    uint64_t *id;
    int n, n_config, err;
    gop_op_status_t status;
    FILE *fd;
    char *fname_tmp;

    log_printf(5, "Processing rid_config response gid=%d\n", gop_id(task->gop));

    status = gop_success_status;
    arg = gop_get_private(task->gop);
    rsrc = (lio_rs_remote_client_priv_t *)arg->rs->priv;

    //** Parse the response
    msg = task->response;

    f = gop_mq_msg_first(msg);
    gop_mq_get_frame(f, (void **)&data, &n);
    if (n != 0) {  //** SHould be an empty frame
        log_printf(0, " ERROR:  Missing initial empty frame!\n");
        status = gop_failure_status;
        goto fail;
    }
    f = gop_mq_msg_next(msg);
    gop_mq_get_frame(f, (void **)&data, &n);
    if (mq_data_compare(data, n, MQF_VERSION_KEY, MQF_VERSION_SIZE) != 0) {
        log_printf(0, "ERROR:  Missing version frame!\n");
        status = gop_failure_status;
        goto fail;
    }
    f = gop_mq_msg_next(msg);
    gop_mq_get_frame(f, (void **)&data, &n);
    if (mq_data_compare(data, n, MQF_RESPONSE_KEY, MQF_RESPONSE_SIZE) != 0) {
        log_printf(0, " ERROR: Bad RESPONSE command frame\n");
        status = gop_failure_status;
        goto fail;
    }
    f = gop_mq_msg_next(msg);
    gop_mq_get_frame(f, (void **)&id, &n);
    if (n != sizeof(uint64_t)) {
        log_printf(0, " ERROR: Bad ID size!  Got %d should be sizeof(uint64_t)=%lu\n", n, sizeof(uint64_t));
        status = gop_failure_status;
        goto fail;
    }

    //** Version frame
    f = gop_mq_msg_next(msg);
    gop_mq_get_frame(f, (void **)&data, &n);
    if (n == 0) {
        log_printf(0, " ERROR: Missing version!\n");
        status = gop_failure_status;
        goto fail;
    }
    data[n-1] = '\0';  //** The last character in a '\n' so replace it with a NULL terminator
    apr_thread_mutex_lock(rsrc->lock);
    sscanf(data, "%d %d", &(rsrc->version.map_version), &(rsrc->version.status_version));
    apr_thread_mutex_unlock(rsrc->lock);
    log_printf(5, "version=%s\n", data);

    //** Config frame
    f = gop_mq_msg_next(msg);
    gop_mq_get_frame(f, (void **)&config, &n_config);
    if ((n_config == 0) && (arg->mode == 1)) {
        log_printf(0, " ERROR: Empty config!\n");
        status = gop_failure_status;
        goto fail;
    }
    log_printf(5, "rid_config_len=%d\n", n_config);

    if (arg->id != *id) {
        log_printf(0, " ERROR: ID mismatch! id=" LU " gid=" LU "\n", *id, arg->id);
        status = gop_failure_status;
        goto fail;
    }
    log_printf(5, "mqid=%s\n", gop_mq_id2str((char *)&(arg->id), sizeof(uint64_t), dt, sizeof(dt)));

    f = gop_mq_msg_next(msg);
    gop_mq_get_frame(f, (void **)&data, &n);
    if (n != 0) {  //** SHould be an empty frame
        log_printf(0, " ERROR:  Final initial empty frame!\n");
        status = gop_failure_status;
        goto fail;
    }

    //** Store the config if changed.  This should trigger the child RS to auto load
    if (n_config > 0) {
        //** Dump the data in a temp file
        n = strlen(rsrc->child_target_file)+100;
        tbx_type_malloc(fname_tmp, char, n);
        int64_t rnd = tbx_random_get_int64(0, 1000000000);
        snprintf(fname_tmp, n, "%s." I64T , rsrc->child_target_file, rnd);

        fd = fopen(fname_tmp, "w");
        err = fwrite(config, n_config, 1, fd);
        fclose(fd);

        //** Now move it into the place of the child target
        if (err == 1) {
            err = apr_file_rename(fname_tmp, rsrc->child_target_file, rsrc->mpool);
            if (err != APR_SUCCESS) {
                status = gop_failure_status;
                log_printf(0, "ERROR: renaming target file!  tmp=%s target=%s err=%d\n", fname_tmp, rsrc->child_target_file, err);
                fprintf(stderr, "ERROR: renaming target file!  tmp=%s target=%s err=%d\n", fname_tmp, rsrc->child_target_file, err);
            }
        } else {
            status = gop_failure_status;
            log_printf(0, "ERROR: creating target temp file!  tmp=%s target=%s err=%d\n", fname_tmp, rsrc->child_target_file, err);
            fprintf(stderr, "ERROR: creating target temp file!  tmp=%s target=%s err=%d\n", fname_tmp, rsrc->child_target_file, err);
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

gop_op_generic_t *rsrc_update_config_op(lio_resource_service_fn_t *rs, int mode, int timeout)
{
    lio_rs_remote_client_priv_t *rsrc = (lio_rs_remote_client_priv_t *)rs->priv;
    mq_msg_t *msg;
    char dt[128];
    rsrc_gop_rid_config_t *arg;
    gop_op_generic_t *gop;

    tbx_type_malloc_clear(arg, rsrc_gop_rid_config_t, 1);

    //** Form the message
    tbx_random_get_bytes(&(arg->id), sizeof(arg->id));
    if (mode != 0) rsrc->update_id = arg->id;  //** Only update the id for an actual wait and update
    msg = gop_mq_msg_new();
    gop_mq_msg_append_mem(msg, rsrc->host_remote_rs, strlen(rsrc->host_remote_rs), MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, MQF_TRACKEXEC_KEY, MQF_TRACKEXEC_SIZE, MQF_MSG_KEEP_DATA);

    gop_mq_msg_append_mem(msg, &(arg->id), sizeof(uint64_t), MQF_MSG_KEEP_DATA);

    if (mode == 0) {
        gop_mq_msg_append_mem(msg, RSR_GET_RID_CONFIG_KEY, RSR_GET_RID_CONFIG_SIZE, MQF_MSG_KEEP_DATA);
    } else {
        gop_mq_msg_append_mem(msg, RSR_GET_UPDATE_CONFIG_KEY, RSR_GET_UPDATE_CONFIG_SIZE, MQF_MSG_KEEP_DATA);
        snprintf(dt, sizeof(dt), "%d\n", timeout);
        gop_mq_msg_append_mem(msg, strdup(dt), strlen(dt), MQF_MSG_AUTO_FREE);
        snprintf(dt, sizeof(dt), "%d %d\n", rsrc->version.map_version, rsrc->version.status_version);
        gop_mq_msg_append_mem(msg, strdup(dt), strlen(dt), MQF_MSG_AUTO_FREE);
    }
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    arg->rs = rs;
    arg->mode = mode;

    //** Make the gop
    gop = gop_mq_op_new(rsrc->mqc, msg, rsrc_response_get_config, arg, free, timeout);
    gop_set_private(gop, arg);

    log_printf(5, "mqid=%s timeout=%d gid=%d\n", gop_mq_id2str((char *)&(arg->id), sizeof(uint64_t), dt, sizeof(dt)), timeout, gop_id(gop));

    return(gop);
}

//***********************************************************************
// _rsrc_update_config - Updates the RID config and pushes the changes
//     to the unerlying child RS.
//     NOTE:  This does not do any locking on the current RS
//***********************************************************************

int _rsrc_update_config(lio_resource_service_fn_t *rs)
{
    int err;
    gop_op_generic_t *gop;

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

void _rsrc_update_abort(lio_resource_service_fn_t *rs)
{
    lio_rs_remote_client_priv_t *rsrc = (lio_rs_remote_client_priv_t *)rs->priv;
    mq_msg_t *msg;
    gop_op_generic_t *gop;
    uint64_t update_id;
    char dt[128];

    update_id = rsrc->update_id;

    if (update_id == 0) return;

    log_printf(5, "aborting mqid=%s\n", gop_mq_id2str((char *)&update_id, sizeof(uint64_t), dt, sizeof(dt)));

    //** Form the message
    msg = gop_mq_msg_new();
    gop_mq_msg_append_mem(msg, rsrc->host_remote_rs, strlen(rsrc->host_remote_rs), MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, MQF_EXEC_KEY, MQF_EXEC_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, &update_id, sizeof(uint64_t), MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, RSR_ABORT_KEY, RSR_ABORT_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    //** Make the gop
    gop = gop_mq_op_new(rsrc->mqc, msg, NULL, NULL, free, 60);

    //** And execute it
    gop_waitany(gop);
    gop_free(gop, OP_DESTROY);
}

//***********************************************************************
// rsrc_check_thread - Monitors the remote server for updates
//***********************************************************************

void *rsrc_check_thread(apr_thread_t *th, void *data)
{
    lio_resource_service_fn_t *rs = (lio_resource_service_fn_t *)data;
    lio_rs_remote_client_priv_t *rsrc = (lio_rs_remote_client_priv_t *)rs->priv;
    gop_op_generic_t *gop, *g;
    gop_op_status_t status;
    int n;

    gop = NULL;
    do {
        if (gop == NULL) {
            gop = rsrc_update_config_op(rs, 1, rsrc->check_interval);
        }
        log_printf(15, "before gop_waitany_timed gid=%d timeout=%d\n", gop_id(gop), rsrc->check_interval);
        g = gop_waitany_timed(gop, 1);
        log_printf(15, "after gop_waitany g=%p\n", g);
        tbx_log_flush();

        apr_thread_mutex_lock(rsrc->lock);
        n = rsrc->shutdown;
        apr_thread_mutex_unlock(rsrc->lock);

        if (g != NULL) {
            status = gop_get_status(gop);
            log_printf(15, "update completed status=%d\n", status.op_status);
            tbx_log_flush();
            gop_free(gop, OP_DESTROY);
            gop = NULL;
        }
        log_printf(15, "loop end n=%d gop=%p\n", n, gop);
    } while (n == 0);

    //** Still have a pending GOP so abort it
    if (gop != NULL) {
        _rsrc_update_abort(rs);
        gop_op_generic_t *g = gop_waitany_timed(gop, 10);
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
// rsrc_print_running_config - Prints the running config
//***********************************************************************

void rsrc_print_running_config(lio_resource_service_fn_t *rs, FILE *fd, int print_section_heading)
{
    lio_rs_remote_client_priv_t *rsrc = (lio_rs_remote_client_priv_t *)rs->priv;

    if (print_section_heading) fprintf(fd, "[%s]\n", rsrc->section);
    fprintf(fd, "type = %s\n", RS_TYPE_REMOTE_CLIENT);
    fprintf(fd, "child_fname = %s\n", rsrc->child_target_file);
    fprintf(fd, "remote_address = %s\n", rsrc->host_remote_rs);
    fprintf(fd, "dynamic_mapping = %d\n", rsrc->dynamic_mapping);
    fprintf(fd, "delete_child_fname = %d  #Remove the child RID file on completeion\n", rsrc->delete_target);
    fprintf(fd, "check_interval = %d #seconds\n", rsrc->check_interval);
    fprintf(fd, "rs_local = %s\n", rsrc->local_child_section);
    fprintf(fd, "rrs_test = %s # NULL if not used\n", rsrc->rrs_test_section);
    fprintf(fd, "\n");

    rs_print_running_config(rsrc->rs_child, fd, 1);
}

//***********************************************************************
// rs_remote_client_destroy
//***********************************************************************

void rs_remote_client_destroy(lio_resource_service_fn_t *rs)
{
    lio_rs_remote_client_priv_t *rsrc = (lio_rs_remote_client_priv_t *)rs->priv;
    apr_status_t dummy;

    //** Shutdown the check thread
    apr_thread_mutex_lock(rsrc->lock);
    rsrc->shutdown = 1;
    log_printf(15, "SHUTDOWN rsrc->shutdown=%d\n", rsrc->shutdown);
    tbx_log_flush();

    _rsrc_update_abort(rs);  //** Abort any pending check
    apr_thread_mutex_unlock(rsrc->lock);
    apr_thread_join(&dummy, rsrc->check_thread);

    //** Shutdown the Remote RS if enabled
    if (rsrc->rrs_test != NULL) rs_destroy_service(rsrc->rrs_test);

    //** Same goes for the child RS
    rs_destroy_service(rsrc->rs_child);

    //** Remove the child target if needed
    if (rsrc->delete_target == 1) remove(rsrc->child_target_file);

    //** Now do the normal cleanup
    apr_pool_destroy(rsrc->mpool);
    free(rsrc->host_remote_rs);
    free(rsrc->child_target_file);
    free(rsrc->section);
    free(rsrc->local_child_section);
    if (rsrc->rrs_test_section) free(rsrc->rrs_test_section);
    free(rsrc);
    free(rs);
}


//***********************************************************************
//  rs_remote_client_create - Creates a remote client RS
//***********************************************************************

lio_resource_service_fn_t *rs_remote_client_create(void *arg, tbx_inip_file_t *fd, char *section)
{
    lio_service_manager_t *ess = (lio_service_manager_t *)arg;
    lio_resource_service_fn_t *rs;
    lio_rs_remote_client_priv_t *rsrc;
    rs_create_t *rs_create;
    char *stype, *ctype;

    if (section == NULL) section = rsrc_default_options.section;

    tbx_type_malloc_clear(rs, lio_resource_service_fn_t, 1);
    tbx_type_malloc_clear(rsrc, lio_rs_remote_client_priv_t, 1);
    rs->priv = (void *)rsrc;

    rsrc->section = strdup(section);

    //** Make the locks and cond variables
    assert_result(apr_pool_create(&(rsrc->mpool), NULL), APR_SUCCESS);
    apr_thread_mutex_create(&(rsrc->lock), APR_THREAD_MUTEX_DEFAULT, rsrc->mpool);
    apr_thread_cond_create(&(rsrc->cond), rsrc->mpool);

    //** Now get the other params
    rsrc->child_target_file = tbx_inip_get_string(fd, section, "child_fname", rsrc_default_options.child_target_file);
    rsrc->host_remote_rs = tbx_inip_get_string(fd, section, "remote_address", rsrc_default_options.host_remote_rs);
    rsrc->dynamic_mapping = tbx_inip_get_integer(fd, section, "dynamic_mapping", rsrc_default_options.dynamic_mapping);
    rsrc->delete_target = tbx_inip_get_integer(fd, section, "delete_child_fname", rsrc_default_options.delete_target);
    rsrc->check_interval = tbx_inip_get_integer(fd, section, "check_interval", rsrc_default_options.check_interval);

    //** Get the MQC
    rsrc->mqc = lio_lookup_service(ess, ESS_RUNNING, ESS_MQ);FATAL_UNLESS(rsrc->mqc != NULL);

    //** Check if we are running the remote RS locally.  This means we are doing testing
    rsrc->rrs_test_section = tbx_inip_get_string(fd, section, "rrs_test", rsrc_default_options.rrs_test_section);
    stype = rsrc->rrs_test_section;
    if (stype != NULL) {
        ctype = tbx_inip_get_string(fd, stype, "type", RS_TYPE_SIMPLE);
        rs_create = lio_lookup_service(ess, RS_SM_AVAILABLE, ctype);
        rsrc->rrs_test = (*rs_create)(ess, fd, stype);
        if (rsrc->rrs_test == NULL) {
            log_printf(1, "ERROR loading test RRS!  type=%s section=%s\n", ctype, stype);
            tbx_log_flush();
            abort();
        }
        free(ctype);
    }

    //** Contact the Remote RS and get the initial config
    if (_rsrc_update_config(rs) != 0) {
        log_printf(0, "ERROR: Remote RS is down!  section=%s remote_host=%s!\n", section, rsrc->host_remote_rs);
        tbx_log_flush();
        abort();
    }

    //** Launch the config changes thread
    tbx_thread_create_assert(&(rsrc->check_thread), NULL, rsrc_check_thread, (void *)rs, rsrc->mpool);

    //** Wait and make sure we have an actual config to pass to the rs_local before continuing on.
    int loop = 0;
    while ((access(rsrc->child_target_file, F_OK) != 0) && (loop < 300)) {
        usleep(10000);
        loop++;
    }
    if (loop>=300) {
        log_printf(0, "ERROR: Remote RS is down! Unable to get cleint config from server! section=%s remote_host=%s!\n", section, rsrc->host_remote_rs);
        tbx_log_flush();
        abort();
    }

    //** Start the child RS.   The update above should have dumped a RID config for it to load
    rsrc->local_child_section = tbx_inip_get_string(fd, section, "rs_local", rsrc_default_options.local_child_section);
    stype = rsrc->local_child_section;
    if (stype == NULL) {  //** Oops missing child RS
        log_printf(0, "ERROR: Mising child RS  section=%s key=rs_local!\n", section);
        tbx_log_flush();
        abort();
    }

    //** and load it
    ctype = tbx_inip_get_string(fd, stype, "type", RS_TYPE_SIMPLE);
    rs_create = lio_lookup_service(ess, RS_SM_AVAILABLE, ctype);
    rsrc->rs_child = (*rs_create)(ess, fd, stype);
    if (rsrc->rs_child == NULL) {
        log_printf(1, "ERROR loading child RS!  type=%s section=%s\n", ctype, stype);
        tbx_log_flush();
        abort();
    }
    free(ctype);

    //** Set up the fn ptrs
    rs->print_running_config = rsrc_print_running_config;
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

