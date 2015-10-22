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
// Remote RS implementation for the server side
//   The Remote Server RS is only supposed to monitor RID config changes
//   and propagate them.  As such all functionality is left unimplemented
//   with the exception of destroy()
//***********************************************************************

#define _log_module_index 216

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
#include "ex3_system.h"
#include "apr_wrapper.h"

typedef struct {
    mq_msg_t *msg;
    char *id;
    int id_size;
    mq_frame_t *version_frame;
    mq_frame_t *config_frame;
    apr_time_t reply_time;
} rsrs_update_handle_t;

//***********************************************************************
// rsrs_update_register - Registers the connection for RID updates
//***********************************************************************

void rsrs_update_register(resource_service_fn_t *rs, mq_frame_t *fid, mq_msg_t *address, int timeout)
{
    rs_remote_server_priv_t *rsrs = (rs_remote_server_priv_t *)rs->priv;
    rsrs_update_handle_t *h;

    type_malloc(h, rsrs_update_handle_t, 1);

    //** Form the core message
    h->msg = mq_msg_new();
    mq_msg_append_mem(h->msg, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(h->msg, MQF_RESPONSE_KEY, MQF_RESPONSE_SIZE, MQF_MSG_KEEP_DATA);
    mq_msg_append_frame(h->msg, fid);
    mq_get_frame(fid, (void **)&(h->id), &(h->id_size));

    //** Add the empty version frame and track it for filling in later
    h->version_frame = mq_frame_new(NULL, 0, MQF_MSG_AUTO_FREE);
    mq_msg_append_frame(h->msg, h->version_frame);

    //** Add the empty config frame and track it for filling in later
    h->config_frame = mq_frame_new(NULL, 0, MQF_MSG_AUTO_FREE);
    mq_msg_append_frame(h->msg, h->config_frame);

    //** End with an empty frame
    mq_msg_append_mem(h->msg, NULL, 0, MQF_MSG_KEEP_DATA);

    //** Now address it
    mq_apply_return_address_msg(h->msg, address, 0);

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
    push(rsrs->pending, h);

    //** Check if we need to change when we wake up
    if ((h->reply_time < rsrs->wakeup_time) || (rsrs->wakeup_time == 0)) rsrs->wakeup_time = h->reply_time;

    log_printf(5, "timeout=%d now=" TT " reply_time=" TT " wakeup=" TT "\n", timeout, apr_time_now(), h->reply_time, rsrs->wakeup_time);

    apr_thread_mutex_unlock(rsrs->lock);
}

//***********************************************************************
// rsrs_config_send - Sends the configuration back
//***********************************************************************

void rsrs_config_send(resource_service_fn_t *rs, mq_frame_t *fid, mq_msg_t *address)
{
    rs_remote_server_priv_t *rsrs = (rs_remote_server_priv_t *)rs->priv;
    mq_msg_t *msg;
    char *config;
    char data[128];

    //** Form the core message
    msg = mq_msg_new();
    mq_msg_append_mem(msg, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
    mq_msg_append_mem(msg, MQF_RESPONSE_KEY, MQF_RESPONSE_SIZE, MQF_MSG_KEEP_DATA);
    mq_msg_append_frame(msg, fid);

    //** Add the version.. Note the "\n" for the version.  This preserves a NULL term on the receiver
    apr_thread_mutex_lock(rsrs->lock);
    snprintf(data, sizeof(data), "%d %d\n", rsrs->my_map_version.map_version, rsrs->my_map_version.status_version);
    apr_thread_mutex_unlock(rsrs->lock);
    mq_msg_append_mem(msg, strdup(data), strlen(data), MQF_MSG_AUTO_FREE);

    log_printf(5, "version=%s", data);

    //** Add the config
    config = rs_get_rid_config(rsrs->rs_child);
    mq_msg_append_mem(msg, config, strlen(config), MQF_MSG_AUTO_FREE);

    log_printf(5, "rid_config=%s\n", config);

    //** End with an empty frame
    mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    //** Now address it
    mq_apply_return_address_msg(msg, address, 0);

    //** Lastly send it
    mq_submit(rsrs->server_portal, mq_task_new(rsrs->mqc, msg, NULL, NULL, 30));
}

//***********************************************************************
// rsrc_abort_cb - Aborts a pending  new config request
//***********************************************************************

void rsrs_abort_cb(void *arg, mq_task_t *task)
{
    resource_service_fn_t *rs = (resource_service_fn_t *)arg;
    rs_remote_server_priv_t *rsrs = (rs_remote_server_priv_t *)rs->priv;
    mq_frame_t *f, *fid;
    rsrs_update_handle_t *h;
    mq_msg_t *msg;
    char *data;
    int bufsize = 1024;
    char buffer[bufsize];
    int n;

    log_printf(5, "Processing incoming request\n");

    //** Parse the command
    msg = task->msg;  //** Don't have to worry about msg cleanup.  It's handled at a higher level

    f = mq_msg_first(msg);
    f = mq_msg_pop(msg);
    mq_get_frame(f, (void **)&data, &n);
    if (n != 0) {  //** SHould be an empty frame
        log_printf(0, " ERROR:  Missing initial empty frame!\n");
        goto fail;
    }
    mq_frame_destroy(f);

    f = mq_msg_pop(msg);
    mq_get_frame(f, (void **)&data, &n);
    if (mq_data_compare(data, n, MQF_VERSION_KEY, MQF_VERSION_SIZE) != 0) {
        log_printf(0, "ERROR:  Missing version frame!\n");
        goto fail;
    }
    mq_frame_destroy(f);

    //** This is the low level command
    f = mq_msg_pop(msg);
    mq_get_frame(f, (void **)&data, &n);
    if (mq_data_compare(data, n, MQF_EXEC_KEY, MQF_TRACKEXEC_SIZE) != 0) {
        log_printf(0, "ERROR:  Invalid command type!\n");
        goto fail;
    }
    mq_frame_destroy(f);

    //** Get the ID frame
    fid = mq_msg_pop(msg);
    mq_get_frame(fid, (void **)&data, &n);
    if (n == 0) {
        log_printf(0, " ERROR: Bad ID size!  Got %d should be greater than 0\n", n);
        goto fail;
    }

    log_printf(5, "Looking for mqid=%s\n", mq_id2str(data, n, buffer, bufsize));
    //** Scan through the list looking for the id
    apr_thread_mutex_lock(rsrs->lock);
    move_to_top(rsrs->pending);
    while ((h = get_ele_data(rsrs->pending)) != NULL) {
        if (mq_data_compare(data, n, h->id, h->id_size) == 0) {  //** Found a match
            log_printf(5, "Aborting task\n");
            delete_current(rsrs->pending, 0, 0);
            mq_submit(rsrs->server_portal, mq_task_new(rsrs->mqc, h->msg, NULL, NULL, 30));
            free(h);  //** The msg is deleted after sending
            break;
        }

        move_down(rsrs->pending);
    }
    apr_thread_mutex_unlock(rsrs->lock);

    mq_frame_destroy(fid);  //** Destroy the id frame

fail:
    log_printf(5, "END incoming request\n");

    return;
}

//***********************************************************************
// rsrs_rid_get_config_cb - Processes the new config request
//***********************************************************************

void rsrs_rid_config_cb(void *arg, mq_task_t *task)
{
    resource_service_fn_t *rs = (resource_service_fn_t *)arg;
    rs_remote_server_priv_t *rsrs = (rs_remote_server_priv_t *)rs->priv;
    mq_frame_t *f, *fid;
    mq_msg_t *msg;
    rs_mapping_notify_t version;
    int bufsize = 128;
    char buffer[bufsize];
    char *data;
    int n, do_config, timeout;

    do_config = -1;
    log_printf(5, "Processing incoming request\n");

    //** Parse the command
    msg = task->msg;  //** Don't have to worry about msg cleanup.  It's handled at a higher level

    f = mq_msg_first(msg);
    f = mq_msg_pop(msg);
    mq_get_frame(f, (void **)&data, &n);
    if (n != 0) {  //** SHould be an empty frame
        log_printf(0, " ERROR:  Missing initial empty frame!\n");
        goto fail;
    }
    mq_frame_destroy(f);

    f = mq_msg_pop(msg);
    mq_get_frame(f, (void **)&data, &n);
    if (mq_data_compare(data, n, MQF_VERSION_KEY, MQF_VERSION_SIZE) != 0) {
        log_printf(0, "ERROR:  Missing version frame!\n");
        goto fail;
    }
    mq_frame_destroy(f);

    //** This is the low level command
    f = mq_msg_pop(msg);
    mq_get_frame(f, (void **)&data, &n);
    if (mq_data_compare(data, n, MQF_TRACKEXEC_KEY, MQF_TRACKEXEC_SIZE) != 0) {
        log_printf(0, "ERROR:  Invalid command type!\n");
        goto fail;
    }
    mq_frame_destroy(f);

    //** Get the ID frame
    fid = mq_msg_pop(msg);
    mq_get_frame(fid, (void **)&data, &n);
    if (n == 0) {
        log_printf(0, " ERROR: Bad ID size!  Got %d should be greater than 0\n", n);
        goto fail;
    }

    log_printf(5, "mqid=%s\n", mq_id2str(data, n, buffer, bufsize));

    //** This is the actiual RS command frame
    f = mq_msg_pop(msg);
    mq_get_frame(f, (void **)&data, &n);
    if (mq_data_compare(data, n, RSR_GET_RID_CONFIG_KEY, RSR_GET_RID_CONFIG_SIZE) == 0) {
        log_printf(5, "commad=RSR_GET_RID_CONFIG_KEY\n");
        do_config = 1;
        mq_frame_destroy(f);
    } else if (mq_data_compare(data, n, RSR_GET_UPDATE_CONFIG_KEY, RSR_GET_UPDATE_CONFIG_SIZE) == 0) {
        log_printf(5, "commad=RSR_GET_UPDATE_CONFIG_KEY\n");
        do_config = 0;   //** Need to parse the timeout
        mq_frame_destroy(f);
        f = mq_msg_pop(msg);
        mq_get_frame(f, (void **)&data, &n);
        if (n > 0) {
            data[n-1] = '\0';
            timeout = atoi(data);
            mq_frame_destroy(f);
        } else {
            mq_frame_destroy(f);
            log_printf(1, "Invalid timeout!\n");
            goto fail;
        }

        //** Also get the version
        memset(&version, 0, sizeof(version));
        f = mq_msg_pop(msg);
        mq_get_frame(f, (void **)&data, &n);
        if (n > 0) {
            data[n-1] = '\0';
            sscanf(data, "%d %d", &(version.map_version), &(version.status_version));
            log_printf(5, "data=!%s! map=%d status=%d timeout=%d\n", data, version.map_version, version.status_version, timeout);

            mq_frame_destroy(f);
        } else {
            mq_frame_destroy(f);
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
    f = mq_msg_first(msg);
    mq_get_frame(f, (void **)&data, &n);
    if (n != 0) {
        log_printf(0, " ERROR:  Missing initial empty frame!\n");
        goto fail;
    }

    //** Everything else is the address so **
    //** Now handle the response
    if (do_config == 1) {
        rsrs_config_send(rs, fid, msg);
    } else {
//     mq_submit(rsrs->server_portal,  mq_task_new(rsrs->mqc, mq_trackaddress_msg(rsrs->hostname, msg, fid, 1), NULL, NULL, 20));
        rsrs_update_register(rs, fid, msg, timeout);
    }

fail:
    log_printf(5, "END incoming request do_config=%d\n", do_config);

    return;
}

//***********************************************************************
//  rsrs_client_notify - Sends responses to listeners about to expire
//***********************************************************************

void rsrs_client_notify(resource_service_fn_t *rs, int everyone)
{
    rs_remote_server_priv_t *rsrs = (rs_remote_server_priv_t *)rs->priv;
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
    move_to_top(rsrs->pending);
    while ((h = get_ele_data(rsrs->pending)) != NULL) {
        if ((h->reply_time < now) || (everyone == 1)) {
            log_printf(5, "sending update to a client everyone=%d\n", everyone);
            mq_frame_set(h->version_frame, strdup(version), vlen, MQF_MSG_AUTO_FREE);
            mq_frame_set(h->config_frame, strdup(config), clen, MQF_MSG_AUTO_FREE);
            mq_submit(rsrs->server_portal, mq_task_new(rsrs->mqc, h->msg, NULL, NULL, 30));
            delete_current(rsrs->pending, 0, 0);
            free(h);  //** The msg is auto destroyed after being sent
        } else if ((new_wakeup_time > h->reply_time) || (new_wakeup_time == 0)) {
            new_wakeup_time = h->reply_time;
        }

        move_down(rsrs->pending);
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
    resource_service_fn_t *rs = (resource_service_fn_t *)data;
    rs_remote_server_priv_t *rsrs = (rs_remote_server_priv_t *)rs->priv;
    rs_mapping_notify_t *my_map, *notify_map;
    int changed, shutdown;
    apr_time_t wakeup_time;

    my_map = &(rsrs->my_map_version);
    notify_map = &(rsrs->notify_map_version);

    log_printf(5, "START\n");
    //** Register us for updates
    rs_register_mapping_updates(rsrs->rs_child, notify_map);

    shutdown = 0;
    do {
        sleep(1);  //** Sleep

        apr_thread_mutex_lock(rsrs->lock);
        changed = ((my_map->map_version != notify_map->map_version) || (my_map->status_version != notify_map->status_version)) ? 1 : 0;
        if (changed == 1) {
            *my_map = *notify_map;    //** Copy the changes over
        }
        shutdown = rsrs->shutdown;
        wakeup_time = rsrs->wakeup_time;
        log_printf(5, "checking.... pending=%d now=" TT " wakeup=" TT "\n", stack_size(rsrs->pending), apr_time_now(), wakeup_time);
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
// rs_remote_server_destroy
//***********************************************************************

void rs_remote_server_destroy(resource_service_fn_t *rs)
{
    rs_remote_server_priv_t *rsrs = (rs_remote_server_priv_t *)rs->priv;
    apr_status_t dummy;

    //** Shutdown the check thread
    apr_thread_mutex_lock(rsrs->lock);
    rsrs->shutdown = 1;
    apr_thread_mutex_unlock(rsrs->lock);
    apr_thread_join(&dummy, rsrs->monitor_thread);

    //** Remove and destroy the server portal
    mq_portal_remove(rsrs->mqc, rsrs->server_portal);
    mq_portal_destroy(rsrs->server_portal);

    //** Shutdown the child RS
    rs_destroy_service(rsrs->rs_child);

    //** Now do the normal cleanup
    apr_pool_destroy(rsrs->mpool);
    free_stack(rsrs->pending, 0);
    free(rsrs->hostname);
    free(rsrs);
    free(rs);
}


//***********************************************************************
//  rs_remote_server_create - Creates a remote server RS
//***********************************************************************

resource_service_fn_t *rs_remote_server_create(void *arg, inip_file_t *fd, char *section)
{
    service_manager_t *ess = (service_manager_t *)arg;
    resource_service_fn_t *rs;
    rs_remote_server_priv_t *rsrs;
    rs_create_t *rs_create;
    mq_command_table_t *ctable;
    char *stype, *ctype;

    if (section == NULL) section = "rs_remote_server";

    type_malloc_clear(rs, resource_service_fn_t, 1);
    type_malloc_clear(rsrs, rs_remote_server_priv_t, 1);
    rs->priv = (void *)rsrs;

    //** Make the locks and cond variables
    { int result = apr_pool_create(&(rsrs->mpool), NULL); assert(result == APR_SUCCESS); }
    apr_thread_mutex_create(&(rsrs->lock), APR_THREAD_MUTEX_DEFAULT, rsrs->mpool);
    apr_thread_cond_create(&(rsrs->cond), rsrs->mpool);

    rsrs->pending = new_stack();
    memset(&(rsrs->my_map_version), 0, sizeof(rsrs->my_map_version));
    memset(&(rsrs->notify_map_version), 0, sizeof(rsrs->notify_map_version));
    rsrs->notify_map_version.lock = rsrs->lock;
    rsrs->notify_map_version.cond = rsrs->cond;

    //** Get the host name we bind to
    rsrs->hostname= inip_get_string(fd, section, "address", NULL);

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
    rsrs->rs_child = (*rs_create)(ess, fd, stype);
    if (rsrs->rs_child == NULL) {
        log_printf(1, "ERROR loading child RS!  type=%s section=%s\n", ctype, stype);
        flush_log();
        abort();
    }
    free(ctype);
    free(stype);

    //** Get the MQC
    { int result = (rsrs->mqc = lookup_service(ess, ESS_RUNNING, ESS_MQ)); assert(result != NULL); }

    //** Make the server portal
    rsrs->server_portal = mq_portal_create(rsrs->mqc, rsrs->hostname, MQ_CMODE_SERVER);
    ctable = mq_portal_command_table(rsrs->server_portal);
    mq_command_set(ctable, RSR_GET_RID_CONFIG_KEY, RSR_GET_RID_CONFIG_SIZE, rs, rsrs_rid_config_cb);
    mq_command_set(ctable, RSR_GET_UPDATE_CONFIG_KEY, RSR_GET_UPDATE_CONFIG_SIZE, rs, rsrs_rid_config_cb);
    mq_command_set(ctable, RSR_ABORT_KEY, RSR_ABORT_SIZE, rs, rsrs_abort_cb);
    mq_portal_install(rsrs->mqc, rsrs->server_portal);

    //** Launch the config changes thread
    thread_create_assert(&(rsrs->monitor_thread), NULL, rsrs_monitor_thread, (void *)rs, rsrs->mpool);

    //** Set up the fn ptrs.  This is just for syncing the rid configuration and state
    //** so very little is implemented
    rs->destroy_service = rs_remote_server_destroy;

    rs->type = RS_TYPE_REMOTE_SERVER;

    return(rs);
}

