/*
 * zhelpers.h
 * Helper header files for example applications.
 */

#ifndef _ZHELPERS_H_INCLUDED
#define _ZHELPERS_H_INCLUDED

// include a bunch of headers that we will need in the examples
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

//#define DEBUG

// Receives zmq message, and converts zmq string to C string
// Users need to free the returned string
// Returns NULL if the context is terminated
static char *s_recv(void *socket) 
{

    zmq_msg_t message;
    int rc = zmq_msg_init(&message); 
    assert(rc == 0);

    if (!zmq_recvmsg(socket, &message, 0)) {
	return (NULL);
    }

    int size = zmq_msg_size(&message);
    char *str = (char *) malloc(size + 1);
    if (str == NULL) {
	printf("Cannot allocate memory for message!\n");
    }

    memset(str, 0, size + 1);
    memcpy(str, zmq_msg_data(&message), size);

    zmq_msg_close(&message);

    return (str);

}

// Sends a C string, and converts C string to zmq string
// Returns number of bytes sent if success, otherwise returns -1
static int s_send(void *socket, char *str) 
{

    int size = strlen(str);
    zmq_msg_t message;
    int rc = zmq_msg_init_size(&message, size);
    assert(rc == 0);

    memcpy(zmq_msg_data(&message), str, size);
    rc = zmq_sendmsg(socket, &message, 0); 
  
    zmq_msg_close(&message); 

    return (rc);
}

// My free function
static void ffn(void *data, void *hint)
{
    free(data);
}

// Sends file as single part message
static int f_sp_send(void *socket, char *fn)
{
    FILE *fp = fopen(fn, "rb");
    assert(fp != NULL);

    // Calculate file size
    fseek(fp, 0, SEEK_END);
    long file_size = ftell(fp);
    rewind(fp);

    // Allocate space to contain the whole file
    char *buffer = (char *)malloc(file_size * sizeof(char));
    assert (buffer != NULL);
    
    // Read file into buffer
    long rc = fread(buffer, 1, file_size, fp);
    assert(rc == file_size); 

    // Send file 
    rc = zmq_send(socket, buffer, file_size, 0);
    assert(rc == file_size);
   /** The same thing as above done by second way 
    zmq_msg_t msg;
    rc = zmq_msg_init_data(&msg, buffer, file_size, ffn, NULL);
    assert(rc == 0);
    rc = zmq_sendmsg(socket, &msg, 0);
    assert(rc == file_size);
 
    zmq_msg_close(&msg);
   **/
    free(buffer); 
    fclose(fp);

    return rc;
}

// Sends file as multipart message
static int f_mp_send(void *socket, char *fn)
{
    
    return 0;
}

// Sends file fn 
// Set ZMQ_SINGLE_PART flag to send file as single part message
// Set ZMQ_MULTI_PART flag to send file as multipart message
// Returns number of bytes sent if success, otherwise returns -1
static int f_send(void *socket, char *fn, int flags) 
{
    assert(fn != NULL);

    if (flags == ZMQ_SINGLE_PART) {
	return f_sp_send(socket, fn);
    } else if (flags == ZMQ_MULTI_PART) {
	return f_mp_send(socket, fn);
    } else {
	fprintf(stderr, "Unknown flag: %d!\n", flags);
	return -1;
    }       
} 

// Receives all message parts and stores into file fn 
static void f_recv(void *socket, char *fn)
{
    FILE *fp = fopen(fn, "wb"); 

    // For testing, name the file with a random number
   /*srand((unsigned)time(NULL));
    int rand_num = random();
    char *fn_rand;
    asprintf(&fn_rand, "%s%d", fn, rand_num);
 
    FILE *fp = fopen(fn_rand, "wb"); 
   */
  
   if (fp == NULL)
	perror("Open file falied!"); 
 
    // Receives multipart message if any  
    while (1) {
        zmq_msg_t msg;
	int hasmore;

	int rc = zmq_msg_init(&msg);
	assert(rc == 0); 
	
	rc = zmq_recvmsg(socket, &msg, 0);
        assert(rc != -1);
	int msg_size = zmq_msg_size(&msg);
	fwrite(zmq_msg_data(&msg), msg_size, 1, fp);	
	
	// Check for more message parts	
	size_t hasmore_size = sizeof(hasmore);
	zmq_getsockopt(socket, ZMQ_RCVMORE, &hasmore, &hasmore_size);
	zmq_msg_close(&msg); 
 	
	if(!hasmore)
	    break;
    }
 
    fclose(fp); 
} 
#endif


