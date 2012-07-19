/*
Advanced Computing Center for Research and Education Proprietary License
Version 1.0 (April 2006)

Copyright (c) 2006, Advanced Computing Center for Research and Education,
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


#ifndef _IBP_TYPES_H_
#define _IBP_TYPES_H_

#ifdef __cplusplus
extern "C" {
#endif

/*********************************************************
 * Constant definition
 *********************************************************/

# define IBP_READCAP      1
# define IBP_WRITECAP     2
# define IBP_MANAGECAP    3

# define RID_LEN          128
# define IBP_MAX_HOSTNAME_LEN  256
# define CMD_BUF_SIZE      1024

typedef int64_t ibp_off_t;  //** Base IBP offset/size data type


/*********************************************************
 * Data sturcture definition.
 *********************************************************/

/*
 * Definition of the capability attributes structure
 */
typedef struct ibp_attributes {
  time_t  duration;     /* lifetime of the capability */
  int     reliability;  /* reliability type of the capability */
  int     type;         /* capability storage type */
} *IBP_attributes;

/*
 * Definition of the IBP depot structure
 */

typedef struct {      //**IBP resource ID data type
  char name[RID_LEN];
} rid_t;

typedef struct ibp_depot {
  char  host[IBP_MAX_HOSTNAME_LEN];  /* host name of the depot */
  int   port;        /* port number */
  rid_t   rid;         /* resource ID */
} *IBP_depot;

/*
 * Definition of the IBP depot information
 */
typedef struct ibp_dptinfo {
  unsigned long int   StableStor;  /* size of the stable storage (in MByte) */
  unsigned long int   StableStorUsed;  /* size of the used stable storage (in MByte) */
  unsigned long int   VolStor;  /* size of the volatile storage (in MByte) */
  unsigned long int   VolStorUsed;  /* size of the used volatile storage (in MByte) */
  long                Duration;  /* How long the depot keeps up */
  float               majorVersion;   /* version (major) of IBP server */
  float               minorVersion;   /* version (minor) of IBP server */
  int                 rid;
  int                 type;
  long long           HardConfigured;
  long long           HardServed;
  long long           HardUsed;
  long long           HardAllocable;
  long long           TotalConfigured;
  long long           TotalServed;
  long long           TotalUsed;
  long long           SoftAllocable;
  int                 nDM;            /* how many data mover  types the
                                        IBP server supports           */
  int                 *dmTypes;       /* data mover types              */
  int                 nNFU;           /* number of NFU ops */
  int                 *NFU;           /* NFU ops */
} *IBP_DptInfo;

/*
 * Definition of the timer structure
 */
typedef struct ibp_timer {
  int  ClientTimeout;  /* Timeout on client side */
  int  ServerSync;  /* Timeout on server(depot) side */
} *IBP_timer;

/*
 * Definition of the capability status structure
 */
typedef struct ibp_capstatus{
  int  readRefCount;  /* number of the capability's read reference */
  int  writeRefCount;  /* number of the capability's write reference */
  ibp_off_t  currentSize;  /* size of data in the capability */
  ibp_off_t  maxSize;  /* max size of the capability */
  struct ibp_attributes  attrib;    /* attributes of the capability */
  char  *passwd;  /* passwd of the depot */
} *IBP_CapStatus;

/*
 * Definition of the capability
 */
typedef char* IBP_cap;

/*
 * Definition of the capability set
 */
typedef struct ibp_set_of_caps {
  IBP_cap  readCap;  /* read capability */
  IBP_cap writeCap;  /* write capability */
  IBP_cap manageCap;  /* manage capability */
} *IBP_set_of_caps;


typedef struct {    //*** Holds the data for the different network connection types
   int type;           //** Type of connection as defined in network.h
   int tcpsize;        //** All types have this parameter
   void *data;         //** Generic container for context data
} ibp_connect_context_t;

typedef struct ibp_attributes ibp_attributes_t;
typedef struct ibp_depot ibp_depot_t;
typedef struct ibp_dptinfo ibp_depotinfo_t;
typedef struct ibp_timer ibp_timer_t;
typedef struct ibp_capstatus ibp_capstatus_t;
typedef char ibp_cap_t;
typedef struct ibp_set_of_caps ibp_capset_t;

typedef struct {    //** I/O Vec array
  ibp_off_t offset;
  ibp_off_t len;
} ibp_iovec_t;

typedef struct {  //** RID list structure
   int n;
   rid_t *rl;
} ibp_ridlist_t;

typedef struct {  //** Alias cap status
  int read_refcount;
  int write_refcount;
  ibp_off_t offset;
  ibp_off_t size;
  long int duration;
} ibp_alias_capstatus_t;

//*** ibp_types.c **
ibp_depot_t *new_ibp_depot();
void destroy_ibp_depot(ibp_depot_t *d);
void set_ibp_depot(ibp_depot_t *d, char *host, int port, rid_t rid);
ibp_attributes_t *new_ibp_attributes();
void destroy_ibp_attributes(ibp_attributes_t *attr);
void set_ibp_attributes(ibp_attributes_t *attr, time_t duration, int reliability, int type);
void get_ibp_attributes(ibp_attributes_t *attr, time_t *duration, int *reliability, int *type);
ibp_timer_t *new_ibp_timer();
void destroy_ibp_timer(ibp_timer_t *t);
void set_ibp_timer(ibp_timer_t *t, int client_timeout, int server_timeout);
void get_ibp_timer(ibp_timer_t *t, int *client_timeout, int *server_timeout);
void destroy_ibp_cap(ibp_cap_t *cap);
ibp_cap_t *dup_ibp_cap(ibp_cap_t *src);
ibp_capset_t *new_ibp_capset();
void destroy_ibp_capset(ibp_capset_t *caps);
void copy_ibp_capset(ibp_capset_t *src, ibp_capset_t *dest);
ibp_cap_t *get_ibp_cap(ibp_capset_t *caps, int ctype);
ibp_capstatus_t *new_ibp_capstatus();
void destroy_ibp_capstatus(ibp_capstatus_t *cs);
void copy_ibp_capstatus(ibp_capstatus_t *src, ibp_capstatus_t *dest);
void get_ibp_capstatus(ibp_capstatus_t *cs, int *readcount, int *writecount,
    int *current_size, int *max_size, ibp_attributes_t *attrib);
ibp_alias_capstatus_t *new_ibp_alias_capstatus();
void destroy_ibp_alias_capstatus(ibp_alias_capstatus_t *cs);
void copy_ibp_alias_capstatus(ibp_alias_capstatus_t *src, ibp_alias_capstatus_t *dest);
void get_ibp_alias_capstatus(ibp_alias_capstatus_t *cs, int *readcount, int *writecount,
    ibp_off_t *offset, ibp_off_t *size, int *duration);
void ridlist_init(ibp_ridlist_t *rlist, int size);
void ridlist_destroy(ibp_ridlist_t *rlist);
int ridlist_get_size(ibp_ridlist_t *rlist);
rid_t ridlist_get_element(ibp_ridlist_t *rlist, int index);
char *ibp_rid2str(rid_t rid, char *buffer);
rid_t ibp_str2rid(char *rid_str);
void ibp_empty_rid(rid_t *rid);
int ibp_rid_is_empty(rid_t rid);
int ibp_compare_rid(rid_t rid1, rid_t rid2);

#ifdef __cplusplus
}
#endif

#endif
