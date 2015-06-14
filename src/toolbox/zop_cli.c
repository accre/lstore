/*
Advanced Computing Center for Research and Education Proprietary License
Version 1.0 (September 2012)

Copyright (c) 2012, Advanced Computing Center for Research and Education,
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

#include "net_zsock.h"
#include "zsock_config.h"
#include "zsock_op.h"
#include "opque.h"
#include <unistd.h>
#include <sys/uio.h>

//**********************************************************************************
// free_tbuffer - Destroy allocation. It might be conflicted with Alan's codes if
// add this function to transfer_buffer.h.
//**********************************************************************************

void free_tbuffer(tbuffer_t *tbuf, int num) {
    int i, j;
    for (i = 0; i < num; i++) {
        for(j = 0; j < tbuf->buf.n; j++) {
            free(tbuf[i].buf.iov[j].iov_base);
        }
        free(tbuf[i].buf.iov);
    }
}

int main(int argc, char **argv) {
    //** Parse options
    char *transport, *hostname;
    int level, c, port, sock_type, num;

    if (argc < 13) {
        fprintf(stdout, "Command: ./zop_test -d debug_level -h hostname -p port -t transport -m mode -n num_msg ZSOCK_COMMON_OPTIONS\n");
        fprintf(stdout, "\t -m: zmq transport mode\n");
        fprintf(stdout, "\t   0: REQ - REP\n");
        fprintf(stdout, "\t   1: DEALER - ROUTER\n");
        fprintf(stdout, "\t   2: DEALER - DEALER\n");
        fprintf(stdout, "\t   3: ROUTER - ROUTER\n");
        fprintf(stdout, "\t   4: PULL - PUSH\n");
        fprintf(stdout, "\t   5: SUB - PUB\n");
        exit(1);
    }

    while ((c = getopt(argc, argv, "d:m:t:h:p:n:")) != -1) {
        switch(c) {
        case 'd':
            level = atoi(optarg);
            break;
        case 'h':
            hostname = optarg;
            break;
        case 't':
            transport = optarg;
            break;
        case 'p':
            port = atoi(optarg);
            break;
        case 'm':
            sock_type = atoi(optarg);
            break;
        case 'n':
            num = atoi(optarg);
            break;
        case '?':
            if (optopt == 'c')
                fprintf(stderr, "Option -%c requires an argument.\n", optopt);
            else if(isprint(optopt))
                fprintf(stderr, "Unkown option: %c\n", optopt);
            else
                fprintf(stderr, "Unkown option: \\x%x\n", optopt);
            return 1;
        default:
            printf("Wrong options\n");
            abort();
        }
    }

    set_log_level(level);

    char test[] = "Hello";

    tbuffer_t buf;
    tbuffer_single(&buf, 5, test);
    int i;

    tbuffer_t *sndbuf;
    tbuffer_t *rcvbuf;

    type_malloc(sndbuf, tbuffer_t, num);
    type_malloc(rcvbuf, tbuffer_t, num);

    for (i = 0; i < num; i++) {
        iovec_t *iov;
        type_malloc(iov, iovec_t, 1);
        iov->iov_base = strdup("Hello");
        iov->iov_len = strlen("Hello");
        tbuffer_vec(&sndbuf[i], 5, 1, iov);
//	tbuffer_single(&sndbuf[i], 5, strdup("Hello"));
    }

    //** Initializes apr
    apr_initialize();

    //mlog_load("log.cfg");

    //** Initializing zmq socket option
    zsocket_opt_t *option;
    type_malloc(option, zsocket_opt_t, 1);
    option->flag = 0;
    option->rate = 1;
    option->multicast_hops = 1;
    option->identity = (char *)malloc(5*sizeof(char)); //** This is a string instead of memory bytes
    //memcpy(option->identity, "test", 4);
    strcpy(option->identity, "test");

    option->router_behavior = 0;
    option->sndhwm = 1;
    option->rcvhwm = 1;
    option->affinity = 1;
    option->rate = 1;
    option->recovery_ivl = 1;
    option->sndbuf = 1;
    option->rcvbuf = 1;
    option->reconnect_ivl = 1;
    option->reconnect_ivl_max = 1;
    option->backlog = 1;
    option->maxmsgsize = 1;
    option->rcvtimeo = 1;
    option->sndtimeo = 1;
    option->ipv4only = 1;
    option->hwm = 1;
    option->sub_num = 1;
    option->unsub_num = 0;
    if (option->sub_num > 0)
        option->subscribe = (char **)malloc(option->sub_num * sizeof(char*));
    option->subscribe[0] = "Hello";

    //Creates zsock client
    zsock_context_t *zc;
    zc = zsock_create_context();
    zsock_load_config(zc, "zsock.cfg", "zsock");


    if (sock_type == 5) {
        log_printf(0, "Working on ZMQ SUB:PUB mode. Coming soon ... ...\n");
        set_flag(option->flag, SUBSCRIBE);
        set_flag(option->flag, IDENTITY);
    } else if (sock_type == 0) {
        opque_t *q = new_opque();
        log_printf(0, "Working on ZMQ REQ:REP mode\n");
        for (i = 0; i < num; i++) {
            zsock_op_t *zop_write = new_zsock_op(zc);
            init_zsock_op(zc, zop_write);
            set_zsock_write_op(zop_write, hostname, port, &sndbuf[i], 0, 5, 0);
            op_generic_t *gop;
            gop = zsock_get_gop(zop_write);
            opque_add(q, gop);
            opque_waitall(q);

            zsock_op_t *zop_read = new_zsock_op(zc);
            init_zsock_op(zc, zop_read);
            set_zsock_read_op(zop_read, hostname, port, &rcvbuf[i], 0, 5, 0);
            op_generic_t *gop_read;
            gop_read = zsock_get_gop(zop_read);
            opque_add(q, gop_read);
            opque_waitall(q);
        }
        opque_free(q, OP_DESTROY);
    } else {
        if (sock_type == 1) {
            log_printf(0, "Working on ZMQ DEALER:ROUTER mode\n");
        } else if (sock_type == 2) {
            log_printf(0, "Working on ZMQ DEALER:DEALER mode\n");
            //**log_printf(0, "Notice: Need to sleep 1 sec after connection in order to successfully send data\n");
            //**This "sleep" problem and multiple connection problem were solved together by making the zsock_write return value correct
        } else if (sock_type == 3) {
            log_printf(0, "Working on ZMQ ROUTER:ROUTER mode. Coming soon ... ...\n");
        } else if (sock_type == 4) {
            log_printf(0, "Working on ZMQ PUSH:PULL mode. Coming soon ... ...\n");
        }

        opque_t *q = new_opque();

        //** Creates write operation
        for (i = 0; i < num; i++) {
            zsock_op_t *zop = new_zsock_op(zc);
            init_zsock_op(zc, zop);
            set_zsock_write_op(zop, hostname, port, &sndbuf[i], 0, 5, 0);
            op_generic_t *gop;
            gop = zsock_get_gop(zop);
            opque_add(q, gop);
        }

        //** Creates read operation
        for (i = 0; i < num; i++) {
            zsock_op_t *zop = new_zsock_op(zc);
            init_zsock_op(zc, zop);
            set_zsock_read_op(zop, hostname, port, &rcvbuf[i], 0, 5, 0);
            op_generic_t *gop;
            gop = zsock_get_gop(zop);
            opque_add(q, gop);
        }

        opque_waitall(q);

        opque_free(q, OP_DESTROY);
    }

    zsock_destroy_context(zc);

    apr_terminate();

    if (option->sub_num > 0)
        free(option->subscribe);
    free(option->identity);
    free(option);

    free_tbuffer(rcvbuf, num);
    if (sock_type != 4 && sock_type != 3)
        free_tbuffer(sndbuf, num);
    tbuffer_destroy(sndbuf);
    tbuffer_destroy(rcvbuf);

    return 0;
}
