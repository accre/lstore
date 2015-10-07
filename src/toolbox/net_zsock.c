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

//*********************************************************************
//*********************************************************************

#define _log_module_index 211

#include "net_zsock.h"

//**********************************************************************
// zsock_status - Returns 1 if the socket is connected and 0 otherwise
//**********************************************************************
int zsock_status(net_sock_t *sock)
{
    network_zsock_t *zsock = (network_zsock_t *)sock;
    if (zsock == NULL) return 0;
    if (zsock->socket == NULL) return 0;

    return 1;
}

//***********************************************************************
// zsock_close - zmq socket close call
//***********************************************************************
int zsock_close(net_sock_t *sock) 
{
    network_zsock_t *zsock = (network_zsock_t *)sock;
    if (zsock == NULL) 
	return 0;
    
    apr_thread_mutex_destroy(zsock->lock);
    apr_pool_destroy(zsock->mpool);
    
    if (zsock->context != NULL) {
	if (zsock->socket != NULL) 
	    zsocket_destroy(zsock->context, zsock->socket);   
	zctx_destroy(&zsock->context);
    }
 
    if (zsock->items != NULL) free(zsock->items);
    
    free(zsock);
        
    return 0;
}

//***********************************************************************
// zsock_set_peer - Dump function
//***********************************************************************

void zsock_set_peer(net_sock_t *sock, char *address, int add_size)
{

}

//************************************************************************
// zsock_io_wait - mode could be either ZMQ_POLLIN or ZMQ_POLLOUT
//************************************************************************

int zsock_io_wait(network_zsock_t *sock, Net_timeout_t tm, int mode)
{
    zmq_pollitem_t item;
    item.socket = sock->socket;
    item.events = mode;
    if (tm > 0) tm /= 1000;   
    zmq_poll(&item, 1, tm);
    return (item.revents & mode) != 0;
}
 
//************************************************************************
// zsock_decode - Decode a transfer buffer into a new message
//************************************************************************

zmsg_t *zsock_decode(tbuffer_t *buf, size_t bpos, size_t size)
{
    int i, rc;
    tbuffer_var_t tbv;
    tbuffer_var_init(&tbv);

    tbv.nbytes = size; //buf->buf.total_bytes;
    tbuffer_next(buf, bpos, &tbv);

    zmsg_t *msg = zmsg_new();
    assert(msg);

    for (i = 0; i < tbv.n_iov; i++) {
        rc = zmsg_addmem(msg, tbv.buffer[i].iov_base, tbv.buffer[i].iov_len);
        assert (rc == 0);
    }
    return msg;
}

//************************************************************************
// zsock_write - Write buffer to a socket. Note that when socket type is 
// ZMQ_ROUTER, tbuffer should begin with peer address. So 'bpos' should be 0
// and 'size' should be greater than size of peer address in this case.
// If it's not ready to write afte timeout, returns 0. 
//************************************************************************

long int zsock_write(net_sock_t *sock, tbuffer_t *buf, size_t bpos, size_t size, Net_timeout_t tm)
{
    network_zsock_t *zsock = (network_zsock_t *)sock;
    zmsg_t *msg = zsock_decode(buf, bpos, size); 
    zmsg_dump(msg); 

    int rc = 0;
    if (zsock_io_wait(zsock, tm, ZMQ_POLLOUT)) {
	zmsg_send(&msg, zsock->socket);
	rc = size;
    }

    return rc;
}

//************************************************************************
// zsock_encode - Encode message to a new transfer buffer, return buffer size
//************************************************************************

size_t zsock_encode(zmsg_t *msg, tbuffer_t *buf)
{
    int frame_count;
    int frame_nbr = zmsg_size(msg);
    int total_size = zmsg_content_size(msg);
    int iov_nbr = frame_nbr;
    int frame_size;
    zframe_t *frame;
    
    iovec_t *iov;
    type_malloc(iov, iovec_t, iov_nbr);

    //** Fill in iovec here. Need to destroy the memory by the caller. 
    for (frame_count = 0; frame_count < frame_nbr; frame_count++) {
        frame = zmsg_next(msg);
        frame_size = zframe_size(frame);
        iov[frame_count].iov_base = malloc(frame_size);
        iov[frame_count].iov_len = frame_size;
        memcpy(iov[frame_count].iov_base, zframe_data(frame), frame_size);
    }

    tbuffer_vec(buf, total_size, iov_nbr, iov);

    return total_size;
}

//************************************************************************
// zsock_read - Read data into the buffer from a socket. Return number 
// of bytes received. Allocate space to store the received data. Caller 
// needs to release these allocation. 
//************************************************************************

long int zsock_read(net_sock_t *sock, tbuffer_t *buf, size_t bpos, size_t size, Net_timeout_t tm)
{
    int total_bytes = 0;
    network_zsock_t *zsock = (network_zsock_t *)sock;
    
    if (zsock == NULL) return -1;
    if (zsock->socket == NULL) return -1;

    if (zsock_io_wait(zsock, tm, ZMQ_POLLIN)) {
        zmsg_t *msg;
        msg = zmsg_recv(zsock->socket);
        zmsg_dump(msg);
        total_bytes = zsock_encode(msg, buf);
        zmsg_destroy(&msg);
    }

    return total_bytes;
}

//***********************************************************************
// zsock_default_opt 
//***********************************************************************

void zsock_default_opt(zsocket_opt_t *option)
{
    option->flag = 0;
    option->rate = 100;
    option->multicast_hops = 1;
    option->identity = NULL; //** This is a string instead of memory bytes
 
    option->router_behavior = 0;
    option->sndhwm = 1000;
    option->rcvhwm = 1000;
    option->affinity = 0;
    option->recovery_ivl = 1;
    option->sndbuf = 0;
    option->rcvbuf = 0;
    option->reconnect_ivl = 100;
    option->reconnect_ivl_max = 0;
    option->backlog = 100;
    option->maxmsgsize = -1;
    option->rcvtimeo = -1;
    option->sndtimeo = -1;
    option->ipv4only = 1;
    option->hwm = 1;
    option->sub_num = 0;
    option->unsub_num = 0;
    option->subscribe = NULL; 
    option->unsubscribe = NULL; 
}

//************************************************************************
// zsock_option_create - Create a new zsocket option
//************************************************************************

zsocket_opt_t *zsock_option_create()
{
    zsocket_opt_t *option;
    type_malloc(option, zsocket_opt_t, 1);
    zsock_default_opt(option);
    return option;
}

//************************************************************************
// zsock_option_destroy - Destroy zsocket option  
//************************************************************************

void zsock_option_destroy(zsocket_opt_t *option)
{
    int i;
    if (option->sub_num > 0) {
        for (i = 0; i < option->sub_num; i++) {
            free(option->subscribe[i]);
        }
        free(option->subscribe);
    }

    if (option->unsub_num > 0) {
        for (i = 0; i < option->unsub_num; i++) {
            free(option->unsubscribe[i]);
        }
        free(option->unsubscribe);
    }

    free(option->identity);

    free(option);
}

//************************************************************************ 
// zsock_setopt - Set zmq socket options
//************************************************************************
void zsock_setopt(void *socket, zsocket_opt_t *option)
{
    int i;

    if (option == NULL) return;

    if (check_flag(option->flag, SNDHWM)) {
	zsocket_set_sndhwm(socket, option->sndhwm);
    } 

    if (check_flag(option->flag, RCVHWM)) {
	zsocket_set_rcvhwm(socket, option->rcvhwm);
    }
    
    if (check_flag(option->flag, AFFINITY)) {
	zsocket_set_affinity(socket, option->affinity);
    }

    if (check_flag(option->flag, RATE)) {
	zsocket_set_rate(socket, option->rate);
    }
    
    if (check_flag(option->flag, RECOVERY_IVL)) {
	zsocket_set_recovery_ivl(socket, option->recovery_ivl);
    }

    if (check_flag(option->flag, SNDBUF)) {
	zsocket_set_sndbuf(socket, option->sndbuf);
    }

    if (check_flag(option->flag, RCVBUF)) {
	zsocket_set_rcvbuf(socket, option->rcvbuf);
    }

    if (check_flag(option->flag, RECONNECT_IVL)) {
	zsocket_set_reconnect_ivl(socket, option->reconnect_ivl);
    }

    if (check_flag(option->flag, RECONNECT_IVL_MAX)) {
	zsocket_set_reconnect_ivl_max(socket, option->reconnect_ivl_max);
    }

    if (check_flag(option->flag, BACKLOG)) {
	zsocket_set_backlog(socket, option->backlog);
    }

    if (check_flag(option->flag, MAXMSGSIZE)) {
	zsocket_set_maxmsgsize(socket, option->maxmsgsize);
    }

    if (check_flag(option->flag, MULTICAST_HOPS)) {
	zsocket_set_multicast_hops(socket, option->multicast_hops);
    }

    if (check_flag(option->flag, RCVTIMEO)) {
	zsocket_set_rcvtimeo(socket, option->rcvtimeo);
    }

    if (check_flag(option->flag, SNDTIMEO)) {
	zsocket_set_sndtimeo(socket, option->sndtimeo); 
    }

    if (check_flag(option->flag, IPV4ONLY)) {
	zsocket_set_ipv4only(socket, option->ipv4only);
    }

    if (check_flag(option->flag, ROUTER_BEHAVIOR)) {
	zsocket_set_router_mandatory(socket, option->router_behavior);
    }

    if (check_flag(option->flag,HWM)) {
	zsocket_set_hwm(socket, option->hwm);
    }

    if (check_flag(option->flag, SUBSCRIBE)) {
	for (i = 0; i < option->sub_num; i++){
	    zsocket_set_subscribe(socket, option->subscribe[i]);
	}
    }

    if (check_flag(option->flag, UNSUBSCRIBE)) {
	for (i = 0; i < option->unsub_num; i++) {
	    zsocket_set_unsubscribe(socket, option->unsubscribe[i]);
	}
    }

    if (check_flag(option->flag, IDENTITY)) {
	zsocket_set_identity(socket, option->identity);
    }
}

//************************************************************************
// _zsock_act - Connect or bind to the endpoint depending on action
//************************************************************************

int _zsock_act(net_sock_t *sock, const char *hostname, int port, Net_timeout_t timeout, int action)
{
    network_zsock_t *zsock = (network_zsock_t *)sock;

    //** Creates zsock->type czmq socket
    zsock->socket = zsocket_new(zsock->context, zsock->type);
    assert(zsock->socket);

    zsock_setopt(zsock->socket, zsock->option); //** Sets option before connect and bind!!
    
    int rc;
    //** Connects or binds to an endpoint
    if (action == ZSOCK_CONNECT){
        rc = zsocket_connect(zsock->socket, "%s://%s:%d", zsock->prtcl, hostname, port);
        assert(rc == 0);
        log_printf(0, "zsocket_connect: host=%s://%s:%d\n", zsock->prtcl, hostname, port);
    } else if (action == ZSOCK_BIND) {
	rc = zsocket_bind(zsock->socket, "%s://%s:%d", zsock->prtcl, hostname, port); //** Always returns port number if successful
        assert(rc == port);
        log_printf(0, "zsocket_bind: host=%s://%s:%d\n", zsock->prtcl, hostname, port);
    }

    return rc;
}

//***********************************************************************
// zsock_connect - Connect a socket to the requested endpoint
//***********************************************************************

int zsock_connect(net_sock_t *sock, const char *hostname, int port, Net_timeout_t timeout)
{
    return _zsock_act(sock, hostname, port, timeout, ZSOCK_CONNECT); 
}

//***********************************************************************
// zsock_bind - Bind a socket to the requested endpoint 
//***********************************************************************

int zsock_bind(net_sock_t *sock, char *hostname, int port)
{
    return _zsock_act(sock, hostname, port, 0, ZSOCK_BIND);
}

//***********************************************************************
// ns_config_zsock - Configure the connection to use zmq
//***********************************************************************

void ns_config_zsock(NetStream_t *ns, int type, char *prtcl, zsocket_opt_t *option)
{
    log_printf(0, "ns_config_zsock: ns=%d, \n", ns->id);    
    
    _ns_init(ns, 0);
    ns->sock_type = NS_TYPE_ZSOCK;
    network_zsock_t *zsock;
    type_malloc_clear(zsock, network_zsock_t, 1);
    ns->sock = (net_sock_t *)zsock;    

    apr_status_t rv;
    rv = apr_pool_create(&(zsock->mpool), NULL);
    if (rv != APR_SUCCESS) {
	log_printf(0, "ns_config_zsock: apr_pool_create error = %d\n", rv); 
	flush_log();
	assert(rv == APR_SUCCESS);
    } 

    apr_thread_mutex_create(&(zsock->lock), APR_THREAD_MUTEX_DEFAULT, zsock->mpool);

    //** Might need to move context creation to zsock_connect or zsock_bind
    zsock->context = zctx_new();
    assert(zsock->context);

    zsock->type = type;
    zsock->prtcl = prtcl;
    zsock->option = option;
 
    ns->connect = zsock_connect;
    ns->bind = zsock_bind; 
    ns->sock_status = zsock_status;
    ns->close = zsock_close;
    ns->read = zsock_read;
    ns->write = zsock_write;
    ns->set_peer= zsock_set_peer;
}

