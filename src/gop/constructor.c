#include <apr_errno.h>
#include <apr_general.h>
#include <tbx/assert_result.h>
#include <tbx/constructor_wrapper.h>

#include "gop/opque.h"

#ifdef ACCRE_CONSTRUCTOR_PREPRAGMA_ARGS
#pragma ACCRE_CONSTRUCTOR_PREPRAGMA_ARGS(gop_construct_fn)
#endif
ACCRE_DEFINE_CONSTRUCTOR(gop_construct_fn)
#ifdef ACCRE_CONSTRUCTOR_POSTPRAGMA_ARGS
#pragma ACCRE_CONSTRUCTOR_POSTPRAGMA_ARGS(gop_construct_fn)
#endif

#ifdef ACCRE_DESTRUCTOR_PREPRAGMA_ARGS
#pragma ACCRE_DESTRUCTOR_PREPRAGMA_ARGS(gop_destruct_fn)
#endif
ACCRE_DEFINE_DESTRUCTOR(gop_destruct_fn)
#ifdef ACCRE_DESTRUCTOR_POSTPRAGMA_ARGS
#pragma ACCRE_DESTRUCTOR_POSTPRAGMA_ARGS(gop_destruct_fn)
#endif

static void gop_construct_fn() {
    apr_status_t ret = apr_initialize();
   FATAL_UNLESS(ret == APR_SUCCESS);
}

static void gop_destruct_fn() {
    apr_terminate();
}
