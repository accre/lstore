/*
 *           IBP Client version 1.0:  Internet BackPlane Protocol
 *               University of Tennessee, Knoxville TN.
 *          Authors: Y. Zheng A. Bassi, W. Elwasif, J. Plank, M. Beck
 *                   (C) 1999 All Rights Reserved
 *
 *                              NOTICE
 *
 * Permission to use, copy, modify, and distribute this software and
 * its documentation for any purpose and without fee is hereby granted
 * provided that the above copyright notice appear in all copies and
 * that both the copyright notice and this permission notice appear in
 * supporting documentation.
 *
 * Neither the Institution (University of Tennessee) nor the Authors
 * make any representations about the suitability of this software for
 * any purpose. This software is provided ``as is'' without express or
 * implied warranty.
 *
 */

// *** Modified by Alan Tackett on 7/7/2008 for sync/async compatiblity

#define _log_module_index 130

#include <stdlib.h>
#include <apr_pools.h>
#include <apr_thread_proc.h>

static apr_threadkey_t *errno_key;

apr_thread_once_t *_err_once = NULL;
apr_pool_t *_err_mpool = NULL;
int _times_used = 0;


//***************************************************************************

void ibp_errno_destroy() {
    _times_used--;
    if (_times_used != 0) return;

    apr_pool_destroy(_err_mpool);
}


//***************************************************************************

void ibp_errno_init() {
    if (_times_used != 0) return;

    _times_used++;
    apr_pool_create(&_err_mpool, NULL);
    apr_thread_once_init(&_err_once, _err_mpool);
}


//***************************************************************************

void _errno_destructor( void *ptr) {
    free(ptr);
}

//***************************************************************************

void _errno_once(void) {
    apr_threadkey_private_create(&errno_key,_errno_destructor, _err_mpool);
}

//***************************************************************************

int *_IBP_errno() {
    void *output = NULL;

    apr_thread_once(_err_once,_errno_once);
    apr_threadkey_private_get(&output, errno_key);
    if (output == NULL ) {
        output = (void *)malloc(sizeof(int));
        apr_threadkey_private_set(output, errno_key);
    }

    return((int *)output);
}

