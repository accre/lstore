#include <apr_errno.h>
#include <apr_general.h>
#include <tbx/assert_result.h>

#include "tbx/constructor_wrapper.h"

#ifdef ACCRE_CONSTRUCTOR_PREPRAGMA_ARGS
#pragma ACCRE_CONSTRUCTOR_PREPRAGMA_ARGS(tbx_construct_fn)
#endif
ACCRE_DEFINE_CONSTRUCTOR(tbx_construct_fn)
#ifdef ACCRE_CONSTRUCTOR_POSTPRAGMA_ARGS
#pragma ACCRE_CONSTRUCTOR_POSTPRAGMA_ARGS(tbx_construct_fn)
#endif

#ifdef ACCRE_DESTRUCTOR_PREPRAGMA_ARGS
#pragma ACCRE_DESTRUCTOR_PREPRAGMA_ARGS(tbx_destruct_fn)
#endif
ACCRE_DEFINE_DESTRUCTOR(tbx_destruct_fn)
#ifdef ACCRE_DESTRUCTOR_POSTPRAGMA_ARGS
#pragma ACCRE_DESTRUCTOR_POSTPRAGMA_ARGS(tbx_destruct_fn)
#endif

static void tbx_construct_fn() {
    apr_status_t ret = apr_initialize();
   FATAL_UNLESS(ret == APR_SUCCESS);
}

static void tbx_destruct_fn() {
    apr_terminate();
}
