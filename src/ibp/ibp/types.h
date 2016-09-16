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

/** \file
* Autogenerated public API
*/

#ifndef ACCRE_IBP_IBP_TYPES_H_INCLUDED
#define ACCRE_IBP_IBP_TYPES_H_INCLUDED

#include <gop/gop.h>
#include <ibp/visibility.h>
#include <tbx/chksum.h>

#ifdef __cplusplus
extern "C" {
#endif

// Typedefs
typedef int64_t ibp_off_t;  //** Base IBP offset/size data type
typedef char ibp_cap_t;
typedef struct ibp_capset_t ibp_capset_t;
typedef struct ibp_attributes_t ibp_attributes_t;
typedef struct ibp_proxy_capstatus_t ibp_proxy_capstatus_t;
typedef struct ibp_capstatus_t ibp_capstatus_t;
typedef struct ibp_connect_context_t ibp_connect_context_t;
typedef struct ibp_depot_t ibp_depot_t;
typedef struct ibp_depotinfo_t ibp_depotinfo_t;
typedef struct ibp_ridlist_t ibp_ridlist_t;
typedef struct ibp_tbx_iovec_t ibp_tbx_iovec_t;
typedef struct ibp_timer_t ibp_timer_t;
typedef struct ibp_rid_t ibp_rid_t;

// Functions
IBP_API void ibp_attributes_get(ibp_attributes_t *attr, time_t *duration, int *reliability, int *type);
IBP_API void ibp_cap_destroy(ibp_cap_t *cap);
IBP_API void ibp_capset_destroy(ibp_capset_t *caps);
IBP_API ibp_cap_t *ibp_cap_get(ibp_capset_t *caps, int ctype);
IBP_API void ibp_cap_getstatus(ibp_capstatus_t *cs, int *readcount, int *writecount,int *current_size, int *max_size, ibp_attributes_t *attrib);
IBP_API ibp_capset_t *ibp_capset_new();
IBP_API char *ibp_rid2str(ibp_rid_t rid, char *buffer);
IBP_API int ibp_rid_compare(ibp_rid_t rid1, ibp_rid_t rid2);
IBP_API void ibp_rid_empty(ibp_rid_t *rid);
IBP_API int ibp_rid_is_empty(ibp_rid_t rid);
IBP_API void ibp_ridlist_destroy(ibp_ridlist_t *rlist);
IBP_API ibp_rid_t ibp_ridlist_element_get(ibp_ridlist_t *rlist, int index);
IBP_API int ibp_ridlist_size_get(ibp_ridlist_t *rlist);
IBP_API ibp_rid_t ibp_str2rid(char *rid_str);
IBP_API void ibp_attributes_set(ibp_attributes_t *attr, time_t duration, int reliability, int type);
IBP_API void ibp_depot_set(ibp_depot_t *d, char *host, int port, ibp_rid_t rid);
IBP_API void ibp_timer_set(ibp_timer_t *t, int client_timeout, int server_timeout);

// Preprocessor constants
#define IBP_MAX_HOSTNAME_LEN  256
#define RID_LEN          128

#define IBP_READCAP      1
#define IBP_WRITECAP     2
#define IBP_MANAGECAP    3


// Exported types. To be obscured.
struct ibp_depotinfo_t {
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
};

struct ibp_attributes_t {
    int     duration;     /* lifetime of the capability */
    int     reliability;  /* reliability type of the capability */
    int     type;         /* capability storage type */
};


struct ibp_rid_t {
    char name[RID_LEN];
};

struct ibp_depot_t {
    char  host[IBP_MAX_HOSTNAME_LEN];  /* host name of the depot */
    int   port;        /* port number */
    ibp_rid_t   rid;         /* resource ID */
};

struct ibp_capset_t {
    ibp_cap_t *readCap;  /* read capability */
    ibp_cap_t *writeCap;  /* write capability */
    ibp_cap_t *manageCap;  /* manage capability */
};

struct ibp_capstatus_t {
    int  readRefCount;  /* number of the capability's read reference */
    int  writeRefCount;  /* number of the capability's write reference */
    ibp_off_t  currentSize;  /* size of data in the capability */
    ibp_off_t  maxSize;  /* max size of the capability */
    struct ibp_attributes_t  attrib;    /* attributes of the capability */
    char  *passwd;  /* passwd of the depot */
};

struct ibp_timer_t {
    int  ClientTimeout;  /* Timeout on client side */
    int  ServerSync;  /* Timeout on server(depot) side */
};

struct ibp_connect_context_t {
    int type;           //** Type of connection as defined in network.h
    int tcpsize;        //** All types have this parameter
    void *data;         //** Generic container for context data
};

struct ibp_tbx_iovec_t {    //** I/O Vec array
    ibp_off_t offset;
    ibp_off_t len;
};

#ifdef __cplusplus
}
#endif

#endif /* ^ ACCRE_IBP_IBP_TYPES_H_INCLUDED ^ */ 
