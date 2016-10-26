#ifndef _CMD_SEND_H_
#define _CMD_SEND_H_

#include <ibp-server/visibility.h>
#include <tbx/network.h>
#include <ibp-server/ibp_server.h>

#ifdef __cplusplus
extern "C" {
#endif

IBPS_API tbx_ns_t *cmd_send(char *host, int port, char *cmd, char **res_buffer, int timeout);

#ifdef __cplusplus
}
#endif


#endif

