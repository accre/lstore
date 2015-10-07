/*
 * zhelpers.h
 * Helper header files for example applications.
 */

#ifndef _ZHELPERS_H_INCLUDED
#define _ZHELPERS_H_INCLUDED

#ifdef __cplusplus
extern "C" {
#endif

// include a bunch of headers that we will need in the examples
#include <sys/uio.h>  //** ZMQ uses struct iovec but fogets to include the header
#include <zmq.h>
 
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <unistd.h>
#include <signal.h>
#include <ctype.h>
#include <time.h>
#include <pthread.h>
 
#define ZMQ_SINGLE_PART 0
#define ZMQ_MULTI_PART 1
 
char *s_recv(void *socket);
int s_send(void *socket, char *str);
void ffn(void *data, void *hint);
int f_sp_send(void *socket, char *fn);
int f_mp_send(void *socket, char *fn);
int f_send(void *socket, char *fn, int flags);
void f_recv(void *socket, char *fn);
 
#ifdef __cplusplus
}
#endif
 
#endif
 
 
 