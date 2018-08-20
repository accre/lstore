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
// Remote RS implementation for the server side
//   The Remote Server RS is only supposed to monitor RID config changes
//   and propagate them.  As such all functionality is left unimplemented
//   with the exception of destroy()
//***********************************************************************

#define _log_module_index 216

#include <apr_errno.h>
#include <apr_pools.h>
#include <apr_thread_cond.h>
#include <apr_thread_mutex.h>
#include <apr_thread_proc.h>
#include <apr_time.h>
#include <assert.h>
#include <limits.h>
#include <gop/mq.h>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <tbx/apr_wrapper.h>
#include <tbx/assert_result.h>
#include <tbx/fmttypes.h>
#include <tbx/iniparse.h>
#include <tbx/log.h>
#include <tbx/stack.h>
#include <tbx/type_malloc.h>
#include <unistd.h>

#include "ex3/system.h"
#include "rs.h"
#include "rs/remote.h"
#include "rs/simple.h"
#include "service_manager.h"

static lio_rs_remote_server_priv_t rsrs_default_options = {
    .section = "rs_remote_server",
    .hostname = "${rsrs_host}",
    .rs_local_section = "rs_simple"
};

typedef struct {
    mq_msg_t *msg;
    char *id;
    int id_size;
    gop_mq_frame_t *version_frame;
    gop_mq_frame_t *config_frame;
    apr_time_t reply_time;
} rsrs_update_handle_t;

//***********************************************************************
// rsrs_update_register - Registers the connection for RID updates
//***********************************************************************

void rsrs_update_register(lio_resource_service_fn_t *rs, gop_mq_frame_t *fid, mq_msg_t *address, int timeout)
{
    lio_rs_remote_server_priv_t *rsrs = (lio_rs_remote_server_priv_t *)rs->priv;
    rsrs_update_handle_t *h;

    tbx_type_malloc(h, rsrs_update_handle_t, 1);

    //** Form the core message
    h->msg = gop_mq_msg_new();
    gop_mq_msg_append_mem(h->msg, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(h->msg, MQF_RESPONSE_KEY, MQF_RESPONSE_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_frame(h->msg, fid);
    gop_mq_get_frame(fid, (void **)&(h->id), &(h->id_size));

    //** Add the empty version frame and track it for filling in later
    h->version_frame = gop_mq_frame_new(NULL, 0, MQF_MSG_AUTO_FREE);
    gop_mq_msg_append_frame(h->msg, h->version_frame);

    //** Add the empty config frame and track it for filling in later
    h->config_frame = gop_mq_frame_new(NULL, 0, MQF_MSG_AUTO_FREE);
    gop_mq_msg_append_frame(h->msg, h->config_frame);

    //** End with an empty frame
    gop_mq_msg_append_mem(h->msg, NULL, 0, MQF_MSG_KEEP_DATA);

    //** Now address it
    gop_mq_msg_apply_return_address(h->msg, address, 0);

    //** Figure out when we wake up if no change
    if (timeout > 10) {
        h->reply_time = apr_time_from_sec(timeout-10);
    } else if (timeout > 5) {
        h->reply_time = apr_time_from_sec(timeout-5);
    } else {
        h->reply_time = apr_time_from_sec(1);
    }
    h->reply_time += apr_time_now();

    apr_thread_mutex_lock(rsrs->lock);

    //** Add it to the queue
    tbx_stack_push(rsrs->pending, h);

    //** Check if we need to change when we wake up
    if ((h->reply_time < rsrs->wakeup_time) || (rsrs->wakeup_time == 0)) rsrs->wakeup_time = h->reply_time;

    log_printf(5, "timeout=%d now=" TT " reply_time=" TT " wakeup=" TT "\n", timeout, apr_time_now(), h->reply_time, rsrs->wakeup_time);

    apr_thread_mutex_unlock(rsrs->lock);
}

//***********************************************************************
// rsrs_config_send - Sends the configuration back
//***********************************************************************

void rsrs_config_send(lio_resource_service_fn_t *rs, gop_mq_frame_t *fid, mq_msg_t *address)
{
    lio_rs_remote_server_priv_t *rsrs = (lio_rs_remote_server_priv_t *)rs->priv;
    mq_msg_t *msg;
    char *config;
    char data[128];

    //** Form the core message
    msg = gop_mq_msg_new();
    gop_mq_msg_append_mem(msg, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, MQF_RESPONSE_KEY, MQF_RESPONSE_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_frame(msg, fid);

    //** Add the version.. Note the "\n" for the version.  This preserves a NULL term on the receiver
    apr_thread_mutex_lock(rsrs->lock);
    snprintf(data, sizeof(data), "%d %d\n", rsrs->my_map_version.map_version, rsrs->my_map_version.status_version);
    apr_thread_mutex_unlock(rsrs->lock);
    gop_mq_msg_append_mem(msg, strdup(data), strlen(data), MQF_MSG_AUTO_FREE);

    log_printf(5, "version=%s", data);

    //** Add the config
    config = rs_get_rid_config(rsrs->rs_child);
    gop_mq_msg_append_mem(msg, config, strlen(config), MQF_MSG_AUTO_FREE);

    log_printf(5, "rid_config=%s\n", config);

    //** End with an empty frame
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    //** Now address it
    gop_mq_msg_apply_return_address(msg, address, 0);

    //** Lastly send it
    gop_mq_submit(rsrs->server_portal, gop_mq_task_new(rsrs->mqc, msg, NULL, NULL, 30));
}

//***********************************************************************
// rsrc_abort_cb - Aborts a pending  new config request
//***********************************************************************

void rsrs_abort_cb(void *arg, gop_mq_task_t *task)
{
    lio_resource_service_fn_t *rs = (lio_resource_service_fn_t *)arg;
    lio_rs_remote_server_priv_t *rsrs = (lio_rs_remote_server_priv_t *)rs->priv;
    gop_mq_frame_t *f, *fid;
    rsrs_update_handle_t *h;
    mq_msg_t *msg;
    char *data;
    int bufsize = 1024;
    char buffer[bufsize];
    int n;

    log_printf(5, "Processing incoming request\n");

    //** Parse the command
    msg = task->msg;  //** Don't have to worry about msg cleanup.  It's handled at a higher level

    gop_mq_msg_first(msg);
    f = mq_msg_pop(msg);
    gop_mq_get_frame(f, (void **)&data, &n);
    if (n != 0) {  //** SHould be an empty frame
        log_printf(0, " ERROR:  Missing initial empty frame!\n");
        goto fail;
    }
    gop_mq_frame_destroy(f);

    f = mq_msg_pop(msg);
    gop_mq_get_frame(f, (void **)&data, &n);
    if (mq_data_compare(data, n, MQF_VERSION_KEY, MQF_VERSION_SIZE) != 0) {
        log_printf(0, "ERROR:  Missing version frame!\n");
        goto fail;
    }
    gop_mq_frame_destroy(f);

    //** This is the low level command
    f = mq_msg_pop(msg);
    gop_mq_get_frame(f, (void **)&data, &n);
    if (mq_data_compare(data, n, MQF_EXEC_KEY, MQF_TRACKEXEC_SIZE) != 0) {
        log_printf(0, "ERROR:  Invalid command type!\n");
        goto fail;
    }
    gop_mq_frame_destroy(f);

    //** Get the ID frame
    fid = mq_msg_pop(msg);
    gop_mq_get_frame(fid, (void **)&data, &n);
    if (n == 0) {
        log_printf(0, " ERROR: Bad ID size!  Got %d should be greater than 0\n", n);
        goto fail;
    }

    log_printf(5, "Looking for mqid=%s\n", gop_mq_id2str(data, n, buffer, bufsize));
    //** Scan through the list looking for the id
    apr_thread_mutex_lock(rsrs->lock);
    tbx_stack_move_to_top(rsrs->pending);
    while ((h = tbx_stack_get_current_data(rsrs->pending)) != NULL) {
        if (mq_data_compare(data, n, h->id, h->id_size) == 0) {  //** Found a match
            log_printf(5, "Aborting task\n");
            tbx_stack_delete_current(rsrs->pending, 0, 0);
            gop_mq_submit(rsrs->server_portal, gop_mq_task_new(rsrs->mqc, h->msg, NULL, NULL, 30));
            free(h);  //** The msg is deleted after sending
            break;
        }

        tbx_stack_move_down(rsrs->pending);
    }
    apr_thread_mutex_unlock(rsrs->lock);

    gop_mq_frame_destroy(fid);  //** Destroy the id frame

fail:
    log_printf(5, "END incoming request\n");

    return;
}

//***********************************************************************
// rsrs_rid_get_config_cb - Processes the new config request
//***********************************************************************

void rsrs_rid_config_cb(void *arg, gop_mq_task_t *task)
{
    lio_resource_service_fn_t *rs = (lio_resource_service_fn_t *)arg;
    lio_rs_remote_server_priv_t *rsrs = (lio_rs_remote_server_priv_t *)rs->priv;
    gop_mq_frame_t *f, *fid;
    mq_msg_t *msg;
    lio_rs_mapping_notify_t version;
    int bufsize = 128;
    char buffer[bufsize];
    char *data;
    int n, do_config;
    int timeout = INT_MIN;

    do_config = -1;
    log_printf(5, "Processing incoming request\n");

    //** Parse the command
    msg = task->msg;  //** Don't have to worry about msg cleanup.  It's handled at a higher level

    gop_mq_msg_first(msg);
    f = mq_msg_pop(msg);
    gop_mq_get_frame(f, (void **)&data, &n);
    if (n != 0) {  //** SHould be an empty frame
        log_printf(0, " ERROR:  Missing initial empty frame!\n");
        goto fail;
    }
    gop_mq_frame_destroy(f);

    f = mq_msg_pop(msg);
    gop_mq_get_frame(f, (void **)&data, &n);
    if (mq_data_compare(data, n, MQF_VERSION_KEY, MQF_VERSION_SIZE) != 0) {
        log_printf(0, "ERROR:  Missing version frame!\n");
        goto fail;
    }
    gop_mq_frame_destroy(f);

    //** This is the low level command
    f = mq_msg_pop(msg);
    gop_mq_get_frame(f, (void **)&data, &n);
    if (mq_data_compare(data, n, MQF_TRACKEXEC_KEY, MQF_TRACKEXEC_SIZE) != 0) {
        log_printf(0, "ERROR:  Invalid command type!\n");
        goto fail;
    }
    gop_mq_frame_destroy(f);

    //** Get the ID frame
    fid = mq_msg_pop(msg);
    gop_mq_get_frame(fid, (void **)&data, &n);
    if (n == 0) {
        log_printf(0, " ERROR: Bad ID size!  Got %d should be greater than 0\n", n);
        goto fail;
    }

    log_printf(5, "mqid=%s\n", gop_mq_id2str(data, n, buffer, bufsize));

    //** This is the actiual RS command frame
    f = mq_msg_pop(msg);
    gop_mq_get_frame(f, (void **)&data, &n);
    if (mq_data_compare(data, n, RSR_GET_RID_CONFIG_KEY, RSR_GET_RID_CONFIG_SIZE) == 0) {
        log_printf(5, "commad=RSR_GET_RID_CONFIG_KEY\n");
        do_config = 1;
        gop_mq_frame_destroy(f);
    } else if (mq_data_compare(data, n, RSR_GET_UPDATE_CONFIG_KEY, RSR_GET_UPDATE_CONFIG_SIZE) == 0) {
        log_printf(5, "commad=RSR_GET_UPDATE_CONFIG_KEY\n");
        do_config = 0;   //** Need to parse the timeout
        gop_mq_frame_destroy(f);
        f = mq_msg_pop(msg);
        gop_mq_get_frame(f, (void **)&data, &n);
        if (n > 0) {
            data[n-1] = '\0';
            timeout = atoi(data);
            gop_mq_frame_destroy(f);
        } else {
            gop_mq_frame_destroy(f);
            log_printf(1, "Invalid timeout!\n");
            goto fail;
        }

        //** Also get the version
        memset(&version, 0, sizeof(version));
        f = mq_msg_pop(msg);
        gop_mq_get_frame(f, (void **)&data, &n);
        if (n > 0) {
            data[n-1] = '\0';
            sscanf(data, "%d %d", &(version.map_version), &(version.status_version));
            log_printf(5, "data=!%s! map=%d status=%d timeout=%d\n", data, version.map_version, version.status_version, timeout);

            gop_mq_frame_destroy(f);
        } else {
            gop_mq_frame_destroy(f);
            log_printf(1, "Invalid version!\n");
            goto fail;
        }
        //** and compare it to the current. If different send response NOW
        apr_thread_mutex_lock(rsrs->lock);
        if ((version.map_version != rsrs->my_map_version.map_version) || (version.status_version != rsrs->my_map_version.status_version)) {
            do_config = 1;  //** Immediate response
        }
        apr_thread_mutex_unlock(rsrs->lock);
    } else {
        log_printf(0, " ERROR: Unknown command!\n");
        goto fail;
    }

    //** Empty frame
    f = gop_mq_msg_first(msg);
    gop_mq_get_frame(f, (void **)&data, &n);
    if (n != 0) {
        log_printf(0, " ERROR:  Missing initial empty frame!\n");
        goto fail;
    }

    //** Everything else is the address so **
    //** Now handle the response
    if (do_config == 1) {
        rsrs_config_send(rs, fid, msg);
    } else {
        rsrs_update_register(rs, fid, msg, timeout);
    }

fail:
    log_printf(5, "END incoming request do_config=%d\n", do_config);

    return;
}

//***********************************************************************
//  rsrs_client_notify - Sends responses to listeners about to expire
//***********************************************************************

void rsrs_client_notify(lio_resource_service_fn_t *rs, int everyone)
{
    lio_rs_remote_server_priv_t *rsrs = (lio_rs_remote_server_priv_t *)rs->priv;
    rsrs_update_handle_t *h;
    apr_time_t now, new_wakeup_time;
    char *config;
    char version[128];
    int clen, vlen;

    now = apr_time_now();
    new_wakeup_time = 0;

    //** Get the config
    config = rs_get_rid_config(rsrs->rs_child);
    clen = strlen(config);

    apr_thread_mutex_lock(rsrs->lock);

    //** and the version. Note the "\n" for the version.  This preserves space for a NULL term on the receiver
    snprintf(version, sizeof(version), "%d %d\n", rsrs->my_map_version.map_version, rsrs->my_map_version.status_version);
    vlen = strlen(version);

    //** Cycle through looking for commands about to expire and sending a response
    tbx_stack_move_to_top(rsrs->pending);
    while ((h = tbx_stack_get_current_data(rsrs->pending)) != NULL) {
        if ((h->reply_time < now) || (everyone == 1)) {
            log_printf(5, "sending update to a client everyone=%d\n", everyone);
            gop_mq_frame_set(h->version_frame, strdup(version), vlen, MQF_MSG_AUTO_FREE);
            gop_mq_frame_set(h->config_frame, strdup(config), clen, MQF_MSG_AUTO_FREE);
            gop_mq_submit(rsrs->server_portal, gop_mq_task_new(rsrs->mqc, h->msg, NULL, NULL, 30));
            tbx_stack_delete_current(rsrs->pending, 0, 0);
            free(h);  //** The msg is auto destroyed after being sent
        } else if ((new_wakeup_time > h->reply_time) || (new_wakeup_time == 0)) {
            new_wakeup_time = h->reply_time;
        }

        tbx_stack_move_down(rsrs->pending);
    }

    //** Change our wakeup time
    rsrs->wakeup_time = (new_wakeup_time > 0) ? new_wakeup_time : apr_time_now() + apr_time_from_sec(3600);

    apr_thread_mutex_unlock(rsrs->lock);

    free(config);
}

//***********************************************************************
// rsrs_monitor_thread - Monitors the local RS for updates
//***********************************************************************

void *rsrs_monitor_thread(apr_thread_t *th, void *data)
{
    lio_resource_service_fn_t *rs = (lio_resource_service_fn_t *)data;
    lio_rs_remote_server_priv_t *rsrs = (lio_rs_remote_server_priv_t *)rs->priv;
    lio_rs_mapping_notify_t *my_map, *notify_map;
    int changed, shutdown;
    apr_time_t wakeup_time;

    my_map = &(rsrs->my_map_version);
    notify_map = &(rsrs->notify_map_version);

    log_printf(5, "START\n");
    //** Register us for updates
    rs_register_mapping_updates(rsrs->rs_child, notify_map);

    do {
        sleep(1);  //** Sleep

        apr_thread_mutex_lock(rsrs->lock);
        changed = ((my_map->map_version != notify_map->map_version) || (my_map->status_version != notify_map->status_version)) ? 1 : 0;
        if (changed == 1) {
            *my_map = *notify_map;    //** Copy the changes over
        }
        shutdown = rsrs->shutdown;
        wakeup_time = rsrs->wakeup_time;
        log_printf(5, "checking.... changed=%d  pending=%d now=" TT " wakeup=" TT "\n", changed, tbx_stack_count(rsrs->pending), apr_time_now(), wakeup_time);
        apr_thread_mutex_unlock(rsrs->lock);

        if (changed == 1) { //** RID table has changed so propagate it to everone
            log_printf(5, "pushing update to all clients\n");
            rsrs_client_notify(rs, 1);
        } else if (apr_time_now() > wakeup_time) {  //** Got a client about to expire so handle it
            log_printf(5, "pushing udpate to expiring clients\n");
            rsrs_client_notify(rs, 0);
        }
    } while (shutdown == 0);

    log_printf(5, "END\n");

    //** UnRegister us for updates
    rs_unregister_mapping_updates(rsrs->rs_child, notify_map);

    //** Let everyone know we're exiting and clean up
    rsrs_client_notify(rs, 1);

    return(NULL);
}

//***********************************************************************
// rsrs_print_running_config - Prints the running config
//***********************************************************************

void rsrs_print_running_config(lio_resource_service_fn_t *rs, FILE *fd, int print_section_heading)
{
    lio_rs_remote_server_priv_t *rsrs = (lio_rs_remote_server_priv_t *)rs->priv;

        if (print_section_heading) fprintf(fd, "[%s]\n", rsrs->section);
        fprintf(fd, "type = %s\n", RS_TYPE_REMOTE_SERVER);
        fprintf(fd, "address = %s\n", rsrs->hostname);
        fprintf(fd, "rs_local = %s\n", rsrs->rs_local_section);
        fprintf(fd, "\n");

        rs_print_running_config(rsrs->rs_child, fd, 1);
}

//***********************************************************************
// rs_remote_server_destroy
//***********************************************************************

void rs_remote_server_destroy(lio_resource_service_fn_t *rs)
{
    lio_rs_remote_server_priv_t *rsrs = (lio_rs_remote_server_priv_t *)rs->priv;
    apr_status_t dummy;

    //** Shutdown the check thread
    apr_thread_mutex_lock(rsrs->lock);
    rsrs->shutdown = 1;
    apr_thread_mutex_unlock(rsrs->lock);
    apr_thread_join(&dummy, rsrs->monitor_thread);

    //** Remove and destroy the server portal
    gop_mq_portal_remove(rsrs->mqc, rsrs->server_portal);
    gop_mq_portal_destroy(rsrs->server_portal);

    //** Shutdown the child RS
    rs_destroy_service(rsrs->rs_child);

    //** Now do the normal cleanup
    apr_pool_destroy(rsrs->mpool);
    tbx_stack_free(rsrs->pending, 0);
    free(rsrs->hostname);
    free(rsrs->section);
    free(rsrs->rs_local_section);
    free(rsrs);
    free(rs);
}


//***********************************************************************
//  rs_remote_server_create - Creates a remote server RS
//***********************************************************************

lio_resource_service_fn_t *rs_remote_server_create(void *arg, tbx_inip_file_t *fd, char *section)
{
    lio_service_manager_t *ess = (lio_service_manager_t *)arg;
    lio_resource_service_fn_t *rs;
    lio_rs_remote_server_priv_t *rsrs;
    rs_create_t *rs_create;
    gop_mq_command_table_t *ctable;
    char *ctype;

    if (section == NULL) section = rsrs_default_options.section;

    tbx_type_malloc_clear(rs, lio_resource_service_fn_t, 1);
    tbx_type_malloc_clear(rsrs, lio_rs_remote_server_priv_t, 1);
    rs->priv = (void *)rsrs;

    rsrs->section = strdup(section);

    //** Make the locks and cond variables
    assert_result(apr_pool_create(&(rsrs->mpool), NULL), APR_SUCCESS);
    apr_thread_mutex_create(&(rsrs->lock), APR_THREAD_MUTEX_DEFAULT, rsrs->mpool);
    apr_thread_cond_create(&(rsrs->cond), rsrs->mpool);

    rsrs->pending = tbx_stack_new();
    memset(&(rsrs->my_map_version), 0, sizeof(rsrs->my_map_version));
    memset(&(rsrs->notify_map_version), 0, sizeof(rsrs->notify_map_version));
    rsrs->notify_map_version.lock = rsrs->lock;
    rsrs->notify_map_version.cond = rsrs->cond;

    //** Get the host name we bind to
    rsrs->hostname= tbx_inip_get_string(fd, section, "address", rsrs_default_options.hostname);

    //** Start the child RS.   The update above should have dumped a RID config for it to load
    rsrs->rs_local_section = tbx_inip_get_string(fd, section, "rs_local", rsrs_default_options.rs_local_section);
    if (rsrs->rs_local_section == NULL) {  //** Oops missing child RS
        log_printf(0, "ERROR: Mising child RS  section=%s key=rs_local!\n", rsrs->rs_local_section);
        tbx_log_flush();
        abort();
    }

    //** and load it
    ctype = tbx_inip_get_string(fd, rsrs->rs_local_section, "type", RS_TYPE_SIMPLE);
    rs_create = lio_lookup_service(ess, RS_SM_AVAILABLE, ctype);
    rsrs->rs_child = (*rs_create)(ess, fd, rsrs->rs_local_section);
    if (rsrs->rs_child == NULL) {
        log_printf(1, "ERROR loading child RS!  type=%s section=%s\n", ctype, rsrs->rs_local_section);
        tbx_log_flush();
        abort();
    }
    free(ctype);

    //** Get the MQC
    rsrs->mqc = lio_lookup_service(ess, ESS_RUNNING, ESS_MQ);FATAL_UNLESS(rsrs->mqc != NULL);

    //** Make the server portal
    rsrs->server_portal = gop_mq_portal_create(rsrs->mqc, rsrs->hostname, MQ_CMODE_SERVER);
    ctable = gop_mq_portal_command_table(rsrs->server_portal);
    gop_mq_command_set(ctable, RSR_GET_RID_CONFIG_KEY, RSR_GET_RID_CONFIG_SIZE, rs, rsrs_rid_config_cb);
    gop_mq_command_set(ctable, RSR_GET_UPDATE_CONFIG_KEY, RSR_GET_UPDATE_CONFIG_SIZE, rs, rsrs_rid_config_cb);
    gop_mq_command_set(ctable, RSR_ABORT_KEY, RSR_ABORT_SIZE, rs, rsrs_abort_cb);
    gop_mq_portal_install(rsrs->mqc, rsrs->server_portal);

    //** Launch the config changes thread
    tbx_thread_create_assert(&(rsrs->monitor_thread), NULL, rsrs_monitor_thread, (void *)rs, rsrs->mpool);

    //** Set up the fn ptrs.  This is just for syncing the rid configuration and state
    //** so very little is implemented
    rs->destroy_service = rs_remote_server_destroy;
    rs->print_running_config = rsrs_print_running_config;

    rs->type = RS_TYPE_REMOTE_SERVER;

    return(rs);
}

