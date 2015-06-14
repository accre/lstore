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
    int c, port, sock_type, num;

    if (argc < 11) {
        fprintf(stdout, "Command: ./zop_test -h hostname -p port -t transport -m mode -n num_msg ZSOCK_COMMON_OPTIONS\n");
        fprintf(stdout, "\t -m: zmq transport mode\n");
        fprintf(stdout, "\t   0: REQ - REP\n");
        fprintf(stdout, "\t   1: DEALER - ROUTER\n");
        fprintf(stdout, "\t   2: DEALER - DEALER\n");
        fprintf(stdout, "\t   3: ROUTER - ROUTER\n");
        fprintf(stdout, "\t   4: PULL - PUSH\n");
        fprintf(stdout, "\t   5: SUB - PUB\n");
        exit(1);
    }

    while ((c = getopt(argc, argv, "m:t:h:p:n:")) != -1) {
        switch(c) {
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

    char test[] = "Hello";

    tbuffer_t buf;
    tbuffer_single(&buf, 5, test);
    int i;

    tbuffer_t *sndbuf;
    tbuffer_t *rcvbuf;

    type_malloc(sndbuf, tbuffer_t, num);
    type_malloc(rcvbuf, tbuffer_t, num);

    for (i = 0; i < num; i++) {
        tbuffer_single(&sndbuf[i], 5, test);
    }

    //** Initializes apr
    apr_initialize();

    //** Create zsock_op server/netstream
    NetStream_t *ns_svr;
    ns_svr = new_netstream();

    NetStream_t *ns_client;
    ns_client = new_netstream();

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

    if (sock_type == 5) {
        log_printf(0, "Working on ZMQ SUB:PUB mode\n");
        set_flag(option->flag, SUBSCRIBE);
        set_flag(option->flag, IDENTITY);
        ns_config_zsock(ns_client, ZMQ_PUB, transport, NULL);
        ns_config_zsock(ns_svr, ZMQ_SUB, transport, option);
        zsock_bind(ns_client->sock, hostname, port);
        net_connect(ns_svr, hostname, port, 0);

        fprintf(stdout, "[Publisher sending data ...]\n");
        for (i = 0; i < num; i++) {
            write_netstream(ns_client, &sndbuf[i], 0, 1, 0);
        }

        fprintf(stdout, "[Receiver receiving data ...]\n");
        for (i = 0; i < 5; i++) {
            read_netstream(ns_svr, &rcvbuf[i], 0, 1, 0);
        }
    } else if (sock_type == 0) {

        log_printf(0, "Working on ZMQ REQ:REP mode\n");
        set_flag(option->flag, IDENTITY);

        //Creates zsock client
        zsock_context_t *zc;
        zc = zsock_create_context();
        zsock_load_config(zc, "zsock.cfg", "zsock");

        ns_config_zsock(ns_svr, ZMQ_REP, transport, NULL);
        zsock_bind(ns_svr->sock, hostname, port);

        for (i = 0; i < num; i++) {
            fprintf(stdout, "[Requester sending data ...]\n");
            zsock_op_t zop;
            init_zsock_op(zc, &zop);
            set_zsock_write_op(&zop, hostname, port, &sndbuf[i], 1, 2, 0);
            op_generic_t *gop = zsock_get_gop(&zop);
            gop_set_exec_mode(gop, OP_EXEC_DIRECT);
            gop_start_execution(gop);
            gop_waitany(gop);

//            gop_free(gop, OP_FINALIZE);
//     	    sleep(1);

            fprintf(stdout, "[Responder receiving data ...]\n");
            fflush(stdout);
            read_netstream(ns_svr, &rcvbuf[i], 0, 1, 0);
        }
    } else if (sock_type == 1) {
        log_printf(0, "Working on ZMQ DEALER:ROUTER mode\n");
        set_flag(option->flag, IDENTITY);
        ns_config_zsock(ns_client, ZMQ_DEALER, transport, option);
        ns_config_zsock(ns_svr, ZMQ_ROUTER, transport, NULL);
        zsock_bind(ns_svr->sock, hostname, port);
        net_connect(ns_client, hostname, port, 0);

        fprintf(stdout, "[Dealer sending data ...]\n");
        for (i = 0; i < num; i++) {
            write_netstream(ns_client, &sndbuf[i], 0, 1, 0);
        }

        fprintf(stdout, "[Router receiving data ...]\n");
        for (i = 0; i < num; i++) {
            read_netstream(ns_svr, &rcvbuf[i], 0, 1, 0);
        }

        fprintf(stdout, "[Router sending data ...]\n");
        for (i = 0; i < num; i++) {
            write_netstream(ns_svr, &rcvbuf[i], 0, rcvbuf[i].buf.total_bytes, 0);
        }

        fprintf(stdout, "[Dealer receiving data ...]\n");
        for (i = 0; i < num; i++) {
            read_netstream(ns_client, &sndbuf[i], 0, 1, 0);
        }
    } else if (sock_type == 2) {
        log_printf(0, "Working on ZMQ DEALER:DEALER mode\n");
        ns_config_zsock(ns_client, ZMQ_DEALER, transport, option);
        ns_config_zsock(ns_svr, ZMQ_DEALER, transport, NULL);
        zsock_bind(ns_svr->sock, hostname, port);
        net_connect(ns_client, hostname, port, 0);

        fprintf(stdout, "[Dealer1 sending data ...]\n");
        for (i = 0; i < num; i++) {
            write_netstream(ns_client, &sndbuf[i], 0, 1, 0);
        }

        fprintf(stdout, "[Dealer2 receiving data ...]\n");
        for (i = 0; i < num; i++) {
            read_netstream(ns_svr, &rcvbuf[i], 0, 1, 0);
        }

        fprintf(stdout, "[Dealer2 sending data ...]\n");
        for (i = 0; i < num; i++) {
            write_netstream(ns_svr, &rcvbuf[i], 0, 1, 0);
        }

        fprintf(stdout, "[Dealer1 receiving data ...]\n");
        for (i = 0; i < num; i++) {
            read_netstream(ns_client, &sndbuf[i], 0, 1, 0);
        }
    } else if (sock_type == 3) {
        log_printf(0, "Working on ZMQ ROUTER:ROUTER mode\n");
        set_flag(option->flag, IDENTITY);
        ns_config_zsock(ns_client, ZMQ_ROUTER, transport, option);
        ns_config_zsock(ns_svr, ZMQ_ROUTER, transport, NULL);
        zsock_bind(ns_svr->sock, hostname, port);
        net_connect(ns_client, hostname, port, 0);

        sleep(1); //** Took me an hour to figure out this causes problem
        //** Router does not know when the client is ready so it needs to sleep!!!!!
        //** It is similar to the publisher.

        tbuffer_t data;
        iovec_t iov[2];

        void *address = malloc(4);
        memcpy(address, "test", 4);

        iov[0].iov_base = address;//"test";
        iov[0].iov_len = 4;
        iov[1].iov_base = "Hello";
        iov[1].iov_len = 5;

        tbuffer_vec(&data, 9, 2, iov);

//	tbuffer_t *rcvbuf;


        fprintf(stdout, "[Router1 sending data ...]\n");
        for (i = 0; i < num; i++) {
            write_netstream(ns_svr, &data, 0, data.buf.total_bytes, 0);
        }

        fprintf(stdout, "[Router2 receiving data ...]\n");
        for (i = 0; i < num; i++) {
            read_netstream(ns_client, &rcvbuf[i], 0, 1, 0);
        }
        free(address);
        //free_tbuffer(&data, 1);

    } else if (sock_type == 4) {
        log_printf(0, "Working on ZMQ PUSH:PULL mode\n");
        ns_config_zsock(ns_client, ZMQ_PUSH, transport, option);
        ns_config_zsock(ns_svr, ZMQ_PULL, transport, NULL);
        zsock_bind(ns_svr->sock, hostname, port);
        net_connect(ns_client, hostname, port, 0);

        fprintf(stdout, "[Sender sending data ...]\n");
        for (i = 0; i < num; i++) {
            write_netstream(ns_client, &sndbuf[i], 0, 1, 0);
        }

        fprintf(stdout, "[Receiver receiving data ...]\n");
        for (i = 0; i < num; i++) {
            read_netstream(ns_svr, &rcvbuf[i], 0, 1, 0);
        }
    }

    //zsock_connect(ns_client->sock, hostname, port, 0);

    destroy_netstream(ns_client);
    destroy_netstream(ns_svr);
    apr_terminate();

    if (option->sub_num > 0)
        free(option->subscribe);
    free(option->identity);
    free(option);

    free_tbuffer(rcvbuf, num);
//    if (sock_type != 4 && sock_type != 3)
//	free_tbuffer(sndbuf, num);
    tbuffer_destroy(sndbuf);
    tbuffer_destroy(rcvbuf);

    return 0;
}
