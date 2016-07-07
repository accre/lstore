#include "task.h"
#include <opque.h>
#include <gop/portal.h>

BENCHMARK_IMPL(sizes) {
  fprintf(stderr, "gop_control_t: %u bytes\n", (unsigned int) sizeof(gop_control_t));
  fprintf(stderr, "gop_op_status_t: %u bytes\n", (unsigned int) sizeof(gop_op_status_t));
  fprintf(stderr, "gop_portal_fn_t: %u bytes\n", (unsigned int) sizeof(gop_portal_fn_t));
  fprintf(stderr, "gop_portal_context_t: %u bytes\n", (unsigned int) sizeof(gop_portal_context_t));
  fprintf(stderr, "gop_op_common_t: %u bytes\n", (unsigned int) sizeof(gop_op_common_t));
  fprintf(stderr, "gop_que_data_t: %u bytes\n", (unsigned int) sizeof(gop_que_data_t));
  fprintf(stderr, "gop_opque_t: %u bytes\n", (unsigned int) sizeof(gop_opque_t));
  fprintf(stderr, "gop_op_generic_t: %u bytes\n", (unsigned int) sizeof(gop_op_generic_t));
  fflush(stderr);
  return 0;
}
