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

#include <gop/gop.h>
#include <gop/gop.h>
#include <gop/mq_helpers.h>
#include <gop/mq_stream.h>
#include <gop/opque.h>
#include <lio.h>
#include <stdlib.h>
#include <strings.h>
#include <tbx/random.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>

#define RC_GET_REMOTE_CONFIG_KEY    "rc_get_config"
#define RC_GET_REMOTE_CONFIG_SIZE   13

typedef struct {
    gop_mq_context_t  *mqc;
    gop_mq_portal_t *server_portal;
    char *prefix;
    char *host;
    char *section;
} rc_t;

typedef struct {
    uint64_t id;
    mq_msg_t *rc_host;
    char *rc_fname;
    char **config;
    rc_t *rc;
} rc_op_t;

static rc_t rc_default_options = {
    .section = NULL,
    .host = "${rc_host}",
    .prefix = "/etc/lio/clients"
};

static rc_t *rc_server = NULL;

static char *mq_config = "[mq]\n"
                         "min_conn = 1\n"
                         "max_conn = 1\n";

//****************************************************************************
//  rc_parse - Parses the remote config string into a host and config
//     filename. RC string format:
//
//          MQ_NAME|HOST:port,...@RC_NAME
//          lstore://user@MQ_NAME|HOST:port:cfg:section:/fname
//          user@MQ_NAME|HOST:port:cfg:section:/fname
//          user@HOST:port:cfg:section:/fname
//          user@HOST:/fname
//          @:/fname
//
//     IF the string can't be parsed 1 is returned.  Othersie 0 is returned
//     for success.
//****************************************************************************

int rc_parse(char *remote_config, char **rc_host, char **rc_fname)
{
    int i;
    char *fname, *rc, *bstate;

    rc = strdup(remote_config);
    *rc_fname = NULL;
    *rc_host = tbx_stk_string_token(rc, "@", &bstate, &i);
    fname = tbx_stk_string_token(NULL, "@", &bstate, &i);

    if (!*rc_host) {
        if (rc) free(rc);
        return(1);
    }

    *rc_fname = (fname) ? strdup(fname) : "lio";
    return(0);
}

//***********************************************************************
// rcc_response_get_config - Handles the get remote config response (CLIENT)
//***********************************************************************

gop_op_status_t rcc_response_get_config(void *task_arg, int tid)
{
    gop_mq_task_t *task = (gop_mq_task_t *)task_arg;
    rc_op_t *arg = task->arg;
    gop_mq_frame_t *f;
    gop_op_status_t status;
    char *config;
    int n_config;

    log_printf(5, "START\n");

    status = gop_success_status;

    //** Parse the response
    gop_mq_remove_header(task->response, 0);

    //** Config frame
    f = gop_mq_msg_next(task->response);
    gop_mq_get_frame(f, (void **)&config, &n_config);
    log_printf(5, "config=%s\n", config);

    if (n_config <= 0) {
        log_printf(0, " ERROR: Empty config! n=%d\n", n_config);
        status = gop_failure_status;
        goto fail;
    }
    *arg->config = strndup(config, n_config);
    log_printf(5, "rc_config_len=%d\n", n_config);

    //** Clean up
fail:
    log_printf(5, "END gid=%d status=%d\n", gop_id(task->gop), status.op_status);

    return(status);
}


//***********************************************************************
// rcc_get_config_op - Gets the remote LIO config string (CLIENT)
//***********************************************************************

gop_op_generic_t *rcc_get_config_op(rc_t *rc, mq_msg_t *rc_host, char *rc_fname, char **config, int timeout)
{
    mq_msg_t *msg;
    char dt[128];
    rc_op_t *arg;
    gop_op_generic_t *gop;

    tbx_type_malloc_clear(arg, rc_op_t, 1);
    tbx_random_get_bytes(&(arg->id), sizeof(arg->id));
    arg->rc_host = rc_host;
    arg->rc_fname = rc_fname;
    arg->config = config;

    //** Form the message
    msg = gop_mq_make_exec_core_msg(rc_host, 1);
    gop_mq_msg_append_mem(msg, RC_GET_REMOTE_CONFIG_KEY, RC_GET_REMOTE_CONFIG_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, rc_fname, strlen(rc_fname), MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    //** Make the gop
    gop = gop_mq_op_new(rc->mqc, msg, rcc_response_get_config, arg, free, timeout);
    gop_set_private(gop, arg);

    log_printf(5, "mqid=%s timeout=%d gid=%d\n", gop_mq_id2str((char *)&(arg->id), sizeof(uint64_t), dt, sizeof(dt)), timeout, gop_id(gop));

    return(gop);
}

//***********************************************************************
// rcs_config_send - Sends the configuration back (SERVER)
//***********************************************************************

void rcs_config_send(rc_t *rc, gop_mq_frame_t *fid, mq_msg_t *address, char *fname)
{
    mq_msg_t *msg;
    char *config, *path;
    int nbytes;

    //** Form the core message
    msg = gop_mq_msg_new();
    gop_mq_msg_append_mem(msg, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, MQF_RESPONSE_KEY, MQF_RESPONSE_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_frame(msg, fid);


    //** Form the full path
    nbytes = strlen(rc->prefix) + 1 + strlen(fname) + 4 + 1;
    tbx_type_malloc(path, char, nbytes);
    snprintf(path, nbytes, "%s/%s.cfg", rc->prefix, fname);
    log_printf(5, "rcs_config_send: full_path=%s\n", path);

    //** Add the config
    tbx_inip_file2string(path, &config, &nbytes);
    if (nbytes > 0) nbytes++; //** Make sure and send the NULL terminator
    free(path);
    gop_mq_msg_append_mem(msg, config, nbytes, MQF_MSG_AUTO_FREE);

    log_printf(5, "nbytes=%d rid_config=%s\n", nbytes, config);

    //** End with an empty frame
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    //** Now address it
    gop_mq_msg_apply_return_address(msg, address, 0);

    //** Lastly send it
    gop_mq_submit(rc->server_portal, gop_mq_task_new(rc->mqc, msg, NULL, NULL, 30));
}

//***********************************************************************
// rcs_get_config_cb - Processes the new config request (SERVER)
//***********************************************************************

void rcs_get_config_cb(void *arg, gop_mq_task_t *task)
{
    rc_t *rc = (rc_t *)arg;
    gop_mq_frame_t *f, *fid;
    mq_msg_t *msg;
    int bufsize = 4096;
    char buffer[bufsize];
    char *data;
    int n, do_config;

    do_config = -1;
    log_printf(5, "Processing incoming remote config request\n");

    //** Parse the command
    msg = task->msg;  //** Don't have to worry about msg cleanup.  It's handled at a higher level
    gop_mq_remove_header(msg, 0);

    //** Get the ID frame
    fid = mq_msg_pop(msg);
    gop_mq_get_frame(fid, (void **)&data, &n);
    if (n == 0) {
        log_printf(0, " ERROR: Bad ID size!  Got %d should be greater than 0\n", n);
        goto fail;
    }

    log_printf(5, "mqid=%s\n", gop_mq_id2str(data, n, buffer, bufsize));

    //** This is the actual RC command frame
    f = mq_msg_pop(msg);
    gop_mq_get_frame(f, (void **)&data, &n);
    if (mq_data_compare(data, n, RC_GET_REMOTE_CONFIG_KEY, RC_GET_REMOTE_CONFIG_SIZE) != 0) {
        log_printf(0, " ERROR: Bad ID size!  Got %d should be greater than 0\n", n);
        gop_mq_frame_destroy(f);
        goto fail;
    }
    gop_mq_frame_destroy(f);

    //** Get the file name
    f = mq_msg_pop(msg);
    gop_mq_get_frame(f, (void **)&data, &n);
    if (n == 0) {
        log_printf(0, "ERROR: Bad config size!\n");
        goto fail;
    }

    if (n>(int)sizeof(buffer)) {
        buffer[0] = 0;
    } else {
        strncpy(buffer, data, n);
        buffer[n] = 0;
    }
    gop_mq_frame_destroy(f);

    log_printf(15, "n=%d fname=%s\n", n, buffer);
    //** Empty frame
    f = gop_mq_msg_first(msg);
    gop_mq_get_frame(f, (void **)&data, &n);
    if (n != 0) {
        log_printf(0, " ERROR:  Missing initial empty frame!\n");
        goto fail;
    }

    //** Everything else is the address so **
    //** Now handle the response
    rcs_config_send(rc, fid, msg, buffer);

fail:
    log_printf(5, "END incoming request do_config=%d\n", do_config);

    return;
}

//***********************************************************************
// rc_client_get_config - Returns the remote config if available
//***********************************************************************

int rc_client_get_config(char *rc_string, char **config, char **obj_name)
{
    rc_t *rc;
    mq_msg_t *address;
    char *rc_host, *rc_file, *rc_section, *mq, s[1024];
    int err, port, n;
    tbx_inip_file_t *ifd;

    *config = NULL;
    mq = strdup("RC");
    port = 6711;
    rc_file = strdup("lio");
    rc_section = strdup("lio");
    rc_host = NULL;
    lio_parse_path(rc_string, NULL, &mq, &rc_host, &port, &rc_file, &rc_section, NULL);

    //** Make the object name
    n = 9 + sizeof(mq) + 1 + sizeof(rc_host) + 1 + 6 + sizeof(rc_file) + 1 + sizeof(rc_section) + 20;
    tbx_type_malloc(*obj_name, char, n);
    snprintf(*obj_name, n, "lstore://%s|%s:%d:%s:%s", mq, rc_host, port, rc_file, rc_section);
    tbx_type_malloc_clear(rc, rc_t, 1);
    snprintf(s, sizeof(s), "%s|tcp://%s:%d", mq, rc_host, port);
    address = gop_mq_string_to_address(s);

    ifd = tbx_inip_string_read(mq_config);
    rc->mqc = gop_mq_create_context(ifd, "mq");
    tbx_inip_destroy(ifd);

    err = gop_sync_exec(rcc_get_config_op(rc, address, rc_file, config, 60));
    gop_mq_msg_destroy(address);
    gop_mq_destroy_context(rc->mqc);
    free(rc);

    if (rc_host) free(rc_host);
    if (rc_file) free(rc_file);
    if (rc_section) free(rc_section);
    if (mq) free(mq);

    return((err == OP_STATE_SUCCESS) ? 0 : 1);
}

//***********************************************************************
// rc_print_running_config - Prints the running remote config server
//***********************************************************************

void rc_print_running_config(FILE *fd)
{
    if (!rc_server) return;
    fprintf(fd, "[%s]\n", rc_server->section);
    fprintf(fd, "host = %s\n", rc_server->host);
    fprintf(fd, "prefix = %s\n", rc_server->prefix);
    fprintf(fd, "\n");
}

//***********************************************************************
// rc_server_install - Installs the Remote config server
//***********************************************************************

int rc_server_install(lio_config_t *lc, char *section)
{
    gop_mq_command_table_t *ctable;

    tbx_type_malloc_clear(rc_server, rc_t, 1);

    rc_server->section = strdup(section);
    rc_server->mqc = lio_gc->mqc;
    rc_server->host = tbx_inip_get_string(lc->ifd, section, "host", rc_default_options.host);
    rc_server->prefix = tbx_inip_get_string(lc->ifd, section, "prefix", rc_default_options.prefix);

    log_printf(5, "Starting remote config server on %s\n", rc_server->host);
    log_printf(5, "Client config path: %s\n", rc_server->prefix);

    if (!rc_server->host) {
        fprintf(stderr, "Missing Remote Config host!\n");
        return(1);
    }

    rc_server->server_portal = gop_mq_portal_create(lio_gc->mqc, rc_server->host, MQ_CMODE_SERVER);
    ctable = gop_mq_portal_command_table(rc_server->server_portal);
    gop_mq_command_set(ctable, RC_GET_REMOTE_CONFIG_KEY, RC_GET_REMOTE_CONFIG_SIZE, rc_server, rcs_get_config_cb);
    gop_mq_portal_install(lio_gc->mqc, rc_server->server_portal);

    return(0);
}

//***********************************************************************
// rc_server_destroy - Removes the remote config server
//***********************************************************************

void rc_server_destroy()
{
    if (!rc_server) return;

    //** Remove and destroy the server portal
    gop_mq_portal_remove(lio_gc->mqc, rc_server->server_portal);
    gop_mq_portal_destroy(rc_server->server_portal);

    if (rc_server->host) free(rc_server->host);
    if (rc_server->prefix) free(rc_server->prefix);
    if (rc_server->section) free(rc_server->section);
    free(rc_server);
    rc_server = NULL;
}
