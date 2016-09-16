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

#pragma once
#ifndef ACCRE_NETWORK_H_INCLUDED
#define ACCRE_NETWORK_H_INCLUDED

#include <apr_time.h>
#include <stdbool.h>
#include <stddef.h>
#include <tbx/chksum.h>
#include <tbx/visibility.h>
#include <tbx/transfer_buffer.h>

#ifdef __cplusplus
extern "C" {
#endif

// Types
typedef struct tbx_network_t tbx_network_t;
typedef struct tbx_ns_chksum_t tbx_ns_chksum_t;
typedef struct tbx_ns_monitor_t tbx_ns_monitor_t;
typedef struct tbx_ns_t tbx_ns_t;
typedef apr_time_t tbx_ns_timeout_t;
typedef enum tbx_net_type_t tbx_net_type_t;
enum tbx_net_type_t {
    NS_TYPE_UNKNOWN,  //** Unspecified type
    NS_TYPE_SOCK,     //** Base socket implementation
    NS_TYPE_PHOEBUS,  //** Phoebus socket implementation
    NS_TYPE_1_SSL,    //** Single SSL connection -- openssl/gnutls/NSS are not thread safe so this is **slow**
    NS_TYPE_2_SSL,    //** Dual SSL connection -- Allows use of separate R/W locks over SSL much faster than prev
    NS_TYPE_ZSOCK,	 //** ZMQ implementation
    NS_TYPE_MAX,      //** Not an actual type just the number of different types
};

// Functions
TBX_API void  tbx_ns_setid(tbx_ns_t *ns, int id);
TBX_API int tbx_network_counter(tbx_network_t *net);
TBX_API int tbx_ns_chksum_is_valid(tbx_ns_chksum_t *ncs);
TBX_API void tbx_ns_chksum_del(tbx_ns_chksum_t *nsc);
TBX_API tbx_ns_chksum_t *tbx_ns_chksum_new();
TBX_API void tbx_ns_chksum_read_clear(tbx_ns_t *ns);
TBX_API void tbx_ns_chksum_read_disable(tbx_ns_t *ns);
TBX_API void tbx_ns_chksum_read_enable(tbx_ns_t *ns);
TBX_API int tbx_ns_chksum_read_flush(tbx_ns_t *ns);
TBX_API void tbx_ns_chksum_read_set(tbx_ns_t *ns, tbx_ns_chksum_t ncs);
TBX_API int tbx_ns_chksum_reset(tbx_ns_chksum_t *ncs);
TBX_API int tbx_ns_chksum_set(tbx_ns_chksum_t *ncs, tbx_chksum_t *cks, size_t blocksize);
TBX_API void tbx_ns_chksum_write_clear(tbx_ns_t *ns);
TBX_API void tbx_ns_chksum_write_disable(tbx_ns_t *ns);
TBX_API void tbx_ns_chksum_write_enable(tbx_ns_t *ns);
TBX_API int tbx_ns_chksum_write_flush(tbx_ns_t *ns);
TBX_API void tbx_ns_chksum_write_set(tbx_ns_t *ns, tbx_ns_chksum_t ncs);
TBX_API void tbx_ns_close(tbx_ns_t *ns);
TBX_API int tbx_ns_connect(tbx_ns_t *ns, const char *hostname, int port, tbx_ns_timeout_t timeout);
TBX_API void tbx_ns_destroy(tbx_ns_t *ns);
TBX_API int tbx_ns_generate_id();
TBX_API int tbx_ns_getid(tbx_ns_t *ns);
TBX_API tbx_ns_t *tbx_ns_new();
TBX_API int tbx_ns_read(tbx_ns_t *ns, tbx_tbuf_t *buffer, unsigned int boff, int size, tbx_ns_timeout_t timeout);
TBX_API int tbx_ns_readline_raw(tbx_ns_t *ns, tbx_tbuf_t *buffer, unsigned int boff, int size, tbx_ns_timeout_t timeout, int *status);
TBX_API tbx_ns_timeout_t *tbx_ns_timeout_set(tbx_ns_timeout_t *tm, int sec, int us);
TBX_API int tbx_ns_write(tbx_ns_t *ns, tbx_tbuf_t *buffer, unsigned int boff, int bsize, tbx_ns_timeout_t timeout);

// Stubs for unused code
// FIXME: Delete these
TBX_API void tbx_ns_config_1_ssl(tbx_ns_t *ns, int fd, int tcpsize);
TBX_API void tbx_ns_config_2_ssl(tbx_ns_t *ns, int tcpsize);

// Precompiler macros
#define tbx_ns_chksum_type(ncs) (ncs)->chksum.type
#define tbx_ns_chksum_blocksize(ncs) (ncs)->blocksize
#define tbx_ns_chksum_clear(ncs) (ncs)->is_valid = 0
#define tbx_ns_chksum_init(ncs)    memset((ncs), 0, sizeof(tbx_ns_chksum_t))
#define tbx_ns_chksum_enable(ncs)  (ncs)->is_running = 1

// TEMPORARY
#if !defined toolbox_EXPORTS && defined LSTORE_HACK_EXPORT
    struct tbx_ns_chksum_t {  //** NetStream checksum container
        int64_t blocksize;   //** Checksum block size or how often to inject/extract the checksum information
        int64_t bytesleft;       //** Current byte count until a full block
        bool    is_running;  //** Current state.  1=running
        bool    is_valid;     //** Has a valid chksum stored
        tbx_chksum_t chksum;    //** Checksum to use
    };
#endif

#ifdef __cplusplus
}
#endif

#endif
