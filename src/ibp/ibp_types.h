/*
   Copyright 2016 Vanderbilt University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/


#ifndef _IBP_TYPES_H_
#define _IBP_TYPES_H_

#ifdef __cplusplus
extern "C" {
#endif
#include "ibp/ibp_visibility.h"

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
    int     duration;     /* lifetime of the capability */
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
typedef struct ibp_capstatus {
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
} ibp_tbx_iovec_t;

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
IBP_API void set_ibp_depot(ibp_depot_t *d, char *host, int port, rid_t rid);
ibp_attributes_t *new_ibp_attributes();
void destroy_ibp_attributes(ibp_attributes_t *attr);
IBP_API void set_ibp_attributes(ibp_attributes_t *attr, time_t duration, int reliability, int type);
IBP_API void get_ibp_attributes(ibp_attributes_t *attr, time_t *duration, int *reliability, int *type);
ibp_timer_t *new_ibp_timer();
void destroy_ibp_timer(ibp_timer_t *t);
IBP_API void set_ibp_timer(ibp_timer_t *t, int client_timeout, int server_timeout);
void get_ibp_timer(ibp_timer_t *t, int *client_timeout, int *server_timeout);
IBP_API void destroy_ibp_cap(ibp_cap_t *cap);
ibp_cap_t *dup_ibp_cap(ibp_cap_t *src);
IBP_API ibp_capset_t *new_ibp_capset();
IBP_API void destroy_ibp_capset(ibp_capset_t *caps);
void copy_ibp_capset(ibp_capset_t *src, ibp_capset_t *dest);
IBP_API ibp_cap_t *get_ibp_cap(ibp_capset_t *caps, int ctype);
ibp_capstatus_t *new_ibp_capstatus();
void destroy_ibp_capstatus(ibp_capstatus_t *cs);
void copy_ibp_capstatus(ibp_capstatus_t *src, ibp_capstatus_t *dest);
IBP_API void get_ibp_capstatus(ibp_capstatus_t *cs, int *readcount, int *writecount,
                       int *current_size, int *max_size, ibp_attributes_t *attrib);
ibp_alias_capstatus_t *new_ibp_alias_capstatus();
void destroy_ibp_alias_capstatus(ibp_alias_capstatus_t *cs);
void copy_ibp_alias_capstatus(ibp_alias_capstatus_t *src, ibp_alias_capstatus_t *dest);
void get_ibp_alias_capstatus(ibp_alias_capstatus_t *cs, int *readcount, int *writecount,
                             ibp_off_t *offset, ibp_off_t *size, int *duration);
void ridlist_init(ibp_ridlist_t *rlist, int size);
IBP_API void ridtbx_list_destroy(ibp_ridlist_t *rlist);
IBP_API int ridlist_get_size(ibp_ridlist_t *rlist);
IBP_API rid_t ridlist_get_element(ibp_ridlist_t *rlist, int index);
IBP_API char *ibp_rid2str(rid_t rid, char *buffer);
IBP_API rid_t ibp_str2rid(char *rid_str);
IBP_API void ibp_empty_rid(rid_t *rid);
IBP_API int ibp_rid_is_empty(rid_t rid);
IBP_API int ibp_compare_rid(rid_t rid1, rid_t rid2);

#ifdef __cplusplus
}
#endif

#endif
