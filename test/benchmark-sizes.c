#include "task.h"
#include <opque.h>

BENCHMARK_IMPL(sizes) {
  fprintf(stderr, "gop_control_t: %u bytes\n", (unsigned int) sizeof(gop_control_t));
  fprintf(stderr, "op_status_t: %u bytes\n", (unsigned int) sizeof(op_status_t));
  fprintf(stderr, "portal_fn_t: %u bytes\n", (unsigned int) sizeof(portal_fn_t));
  fprintf(stderr, "portal_context_t: %u bytes\n", (unsigned int) sizeof(portal_context_t));
  fprintf(stderr, "op_common_t: %u bytes\n", (unsigned int) sizeof(op_common_t));
  fprintf(stderr, "que_data_t: %u bytes\n", (unsigned int) sizeof(que_data_t));
  fprintf(stderr, "opque_t: %u bytes\n", (unsigned int) sizeof(opque_t));
  fprintf(stderr, "op_generic_t: %u bytes\n", (unsigned int) sizeof(op_generic_t));
  fflush(stderr);
  return 0;
}
