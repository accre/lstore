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
//  tcmu_lstore - TCMU module supporting the use of LStore files as targets
//***********************************************************************

//#define _GNU_SOURCE
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <endian.h>
#include <errno.h>
#include <scsi/scsi.h>

#include "tcmu-runner.h"

#include <gop/gop.h>
#include <lio/lio.h>
#include <tbx/atomic_counter.h>
#include <tbx/constructor_wrapper.h>
#include <tbx/fmttypes.h>
#include <tbx/type_malloc.h>
#include "visibility.h"

static void lstore_activate();
static void lstore_deactivate();

#ifdef ACCRE_CONSTRUCTOR_PREPRAGMA_ARGS
#pragma ACCRE_CONSTRUCTOR_PREPRAGMA_ARGS(lstore_activate)
#endif
ACCRE_DEFINE_CONSTRUCTOR(lstore_activate)
#ifdef ACCRE_CONSTRUCTOR_POSTPRAGMA_ARGS
#pragma ACCRE_CONSTRUCTOR_POSTPRAGMA_ARGS(lstore_activate)
#endif

#ifdef ACCRE_DESTRUCTOR_PREPRAGMA_ARGS
#pragma ACCRE_DESTRUCTOR_PREPRAGMA_ARGS(lstore_deactivate)
#endif
ACCRE_DEFINE_DESTRUCTOR(lstore_deactivate)
#ifdef ACCRE_DESTRUCTOR_POSTPRAGMA_ARGS
#pragma ACCRE_DESTRUCTOR_POSTPRAGMA_ARGS(lstore_deactivate)
#endif


TCMUL_API int handler_init(void);

typedef struct {
   lio_fd_t *fd;
   lio_config_t *lc;
   lio_path_tuple_t tuple;
   tbx_atomic_unit32_t in_flight;
} tcmu_lstore_t;

//***********************************************************************
//  lstore_activate - Start LIO subsystem when loading the shared library
//***********************************************************************

static void lstore_activate()
{
    int argc = 3;
    char **argv = malloc(sizeof(char *)*argc);
    argv[0] = "lio_tcmu";
    argv[1] = "-c";
    argv[2] = "/etc/lio/lio.cfg";

    lio_init(&argc, &argv);
    free(argv);
    if (!lio_gc) {
        log_printf(-1,"Failed to load LStore\n");
        tcmu_err("Failed to load LStore\n");
        exit(1);
    }

    log_printf(0,"Loaded\n");

    tbx_log_flush();
}

//***********************************************************************
// lstore_deactivate - Shutdown LStore
//***********************************************************************

static void lstore_deactivate()
{
    log_printf(0,"Unloaded\n");
    tbx_log_flush();
    lio_shutdown();
}

//***********************************************************************
//  lstore_check_config - Verify config is valid
//***********************************************************************

static bool lstore_check_config(const char *cfgstring, char **reason)
{
	char *fname;
    int ftype;
    lio_path_tuple_t tuple;

    fname = strchr(cfgstring, '/');
    if (!fname) {
        if (asprintf(reason, "No path found") == -1) *reason = NULL;
        return false;
    }
    fname += 1; //** get past '/'

    tuple = lio_path_resolve(lio_gc->auto_translate, fname);

    ftype = lio_exists(lio_gc, lio_gc->creds, tuple.path);
    if ((ftype & OS_OBJECT_FILE_FLAG) == 0) {
        asprintf(reason, "Target file does not exists. target:%s\n", tuple.path);
        return false;
    }

    log_printf(1, "cfgstring=%s fname=%s ftype=%d\n", cfgstring, tuple.path, ftype);

    lio_path_release(&tuple);

    return true;
}

//***********************************************************************
//  lstore_open - Opens an existing target file
//***********************************************************************

static int lstore_open(struct tcmu_device *dev)
{
    tcmu_lstore_t *ctx;
    int64_t size;
    char *fname;
    int block_size, lbas;
    int return_code;

    tbx_type_malloc_clear(ctx, tcmu_lstore_t, 1);
    return_code = 0;
    ctx->lc = lio_gc;

    tcmu_set_dev_private(dev, ctx);

    fname = strchr(tcmu_get_dev_cfgstring(dev), '/');
    log_printf(1, "START cfgstring=%s fname=%s\n", tcmu_get_dev_cfgstring(dev), fname);
    if (!fname) {
        tcmu_err("no configuration found in cfgstring\n");
        return_code = -EINVAL;
        goto err2;
    }
    fname += 1; //** Move past the '/'

    ctx->tuple = lio_path_resolve(lio_gc->auto_translate, fname);
    if (ctx->tuple.is_lio < 0) {
        tcmu_err("Unable to parse path\n");
        log_printf(0, "Unable to parse path: %s\n", fname);
        return_code = -EINVAL;
        goto err1;
    }

    gop_sync_exec(lio_open_gop(ctx->lc, ctx->lc->creds, ctx->tuple.path, lio_fopen_flags("r+"), NULL, &ctx->fd, 60));
    if (ctx->fd == NULL) {
        tcmu_err("Failed opening target!\n");
        return_code = -EREMOTEIO;
        goto err1;
    }

    lio_wq_enable(ctx->fd, 128);

    block_size = 512;
    tcmu_set_dev_block_size(dev, block_size);

    size = tcmu_get_device_size(dev);
    if (size < 0) {
        tcmu_err("Could not get device size\n");
        goto err1;
    }
    lbas = size/block_size;
    tcmu_set_dev_num_lbas(dev, lbas);

    return(return_code);  //** Exit

    //** If we make it here there was an error
err1:
    lio_path_release(&ctx->tuple);
err2:
    free(ctx);
    return(return_code);
}

//***********************************************************************
// lstore_close - Close the target
//***********************************************************************

static void lstore_close(struct tcmu_device *dev)
{
    tcmu_lstore_t *ctx = tcmu_get_dev_private(dev);

    log_printf(1, "CLOSE\n");

    gop_sync_exec(lio_close_gop(ctx->fd));

    lio_path_release(&ctx->tuple);
    free(ctx);

    log_printf(1, "END\n"); tbx_log_flush();
}

//***********************************************************************
//  lstore_readv - Read data from the target
//***********************************************************************

static int lstore_readv(struct tcmu_device *dev, struct tcmulib_cmd *cmd,
               struct iovec *iov, size_t iov_cnt, size_t length, off_t offset)
{
    tcmu_lstore_t *ctx = tcmu_get_dev_private(dev);
    int ret, nbytes;

    tbx_atomic_inc(ctx->in_flight);
    nbytes = lio_readv(ctx->fd, iov, iov_cnt, length, offset, NULL);
    log_printf(1, "READ n_iov=" ST " offset=" OT " len=" ST " nbytes=%d in_flight=" AIT "\n", iov_cnt, offset, length, nbytes, tbx_atomic_get(ctx->in_flight));
    tbx_atomic_dec(ctx->in_flight);
    if ((unsigned int)nbytes != length) {
    log_printf(1, "ERROR READ n_iov=" ST " offset=" OT " len=" ST " nbytes=%d in_flight=" AIT "\n", iov_cnt, offset, length, nbytes, tbx_atomic_get(ctx->in_flight));
        tcmu_err("read failed: %m\n");
        ret = tcmu_set_sense_data(cmd->sense_buf, MEDIUM_ERROR, ASC_READ_ERROR, NULL);
    } else {
        ret = SAM_STAT_GOOD;
    }

    cmd->done(dev, cmd, ret);
    return(0);
}

//***********************************************************************
//  lstore_writev - Read data from the target
//***********************************************************************

static int lstore_writev(struct tcmu_device *dev, struct tcmulib_cmd *cmd,
		     struct iovec *iov, size_t iov_cnt, size_t length,
		     off_t offset)
{
    tcmu_lstore_t *ctx = tcmu_get_dev_private(dev);
    int nbytes, ret;

tbx_atomic_inc(ctx->in_flight);
    nbytes = lio_writev(ctx->fd, iov, iov_cnt, length, offset, NULL);
    log_printf(1, "WRITE n_iov=" ST " offset=" OT " len=" ST " nbytes=%d in_flight=" AIT "\n", iov_cnt, offset, length, nbytes, tbx_atomic_get(ctx->in_flight));
tbx_atomic_dec(ctx->in_flight);

    if ((unsigned int)nbytes != length) {
    log_printf(1, "ERROR WRITE n_iov=" ST " offset=" OT " len=" ST " nbytes=%d in_flight=" AIT "\n", iov_cnt, offset, length, nbytes, tbx_atomic_get(ctx->in_flight));
        tcmu_err("read failed: %m\n");
        ret = tcmu_set_sense_data(cmd->sense_buf, MEDIUM_ERROR, ASC_READ_ERROR, NULL);
    } else {
        ret = SAM_STAT_GOOD;
    }

    cmd->done(dev, cmd, ret);
    return(0);
}

//***********************************************************************
// lstore_flush - Flush data to backing store
//***********************************************************************

static int lstore_flush(struct tcmu_device *dev, struct tcmulib_cmd *cmd)
{
    tcmu_lstore_t *ctx = tcmu_get_dev_private(dev);
    int err, ret;

    err = gop_sync_exec(lio_flush_gop(ctx->fd, 0, -1));
    if (err != OP_STATE_SUCCESS) {
        tcmu_err("Flush failed\n");
        ret = tcmu_set_sense_data(cmd->sense_buf, MEDIUM_ERROR, ASC_WRITE_ERROR, NULL);
    } else {
    	ret = SAM_STAT_GOOD;
    }

    cmd->done(dev, cmd, ret);
    return 0;
}

//***********************************************************************

static const char lstore_cfg_desc[] =
    "The path for the LStore file to use as a backstore.";

static struct tcmur_handler lstore_handler = {
    .cfg_desc = lstore_cfg_desc,

    .check_config = lstore_check_config,

    .open = lstore_open,
    .close = lstore_close,
    .read = lstore_readv,
    .write = lstore_writev,
    .flush = lstore_flush,
    .name = "LStore Handler",
    .subtype = "lstore",
    .nr_threads = 128,
};

//***********************************************************************
// Entry point must be named "handler_init"
//***********************************************************************

int handler_init(void)
{
    return tcmur_register_handler(&lstore_handler);
}
