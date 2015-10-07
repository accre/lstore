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

//*****************************************************
// ibp_perf - Benchmarks IBP depot creates, removes,
//      reads, and writes.  The read and write tests
//      use sync an async iovec style operations.
//*****************************************************

#define _log_module_index 138

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <signal.h>
#include <apr_time.h>
#include <apr_file_io.h>
#include <apr_poll.h>
#include <unistd.h>
#include "network.h"
#include "fmttypes.h"
#include "network.h"
#include "log.h"
#include "ibp.h"
#include "iovec_sync.h"
#include "io_wrapper.h"
#include "type_malloc.h"

int a_duration=900;   //** Default duration

IBP_DptInfo depotinfo;
struct ibp_depot *depot_list;
int n_depots;
int ibp_timeout;
int sync_transfer;
int nthreads;
int use_alias;
int report_interval = 0;
int do_validate = 0;
int identical_buffers = 1;
int print_progress;
ibp_connect_context_t *cc = NULL;
ns_chksum_t *ncs;
int disk_cs_type = CHKSUM_DEFAULT;
ibp_off_t disk_blocksize = 0;

ibp_context_t *ic = NULL;


//*************************************************************************
//  init_buffer - Initializes a buffer.  This routine was added to
//     get around throwing network chksum errors by making all buffers
//     identical.  The char "c" may or may not be used.
//*************************************************************************

void init_buffer(char *buffer, char c, int size)
{

    if (identical_buffers == 1) {
        memset(buffer, 'A', size);
        return;
    }

    memset(buffer, c, size);
}

//*************************************************************************
//  create_allocs - Creates a group of allocations in parallel
//*************************************************************************

ibp_capset_t *create_allocs(int nallocs, int asize)
{
    int i, err;
    ibp_attributes_t attr;
    ibp_depot_t *depot;
    opque_t *q;
    op_generic_t *op;

    ibp_capset_t *caps = (ibp_capset_t *)malloc(sizeof(ibp_capset_t)*nallocs);

    set_ibp_attributes(&attr, time(NULL) + a_duration, IBP_HARD, IBP_BYTEARRAY);
    q = new_opque();

//log_printf(0, "create_allocs: disk_cs_type=%d disk_blockseize=%d\n", disk_cs_type, disk_blocksize);
    for (i=0; i<nallocs; i++) {
        depot = &(depot_list[i % n_depots]);
        op = new_ibp_alloc_op(ic, &(caps[i]), asize, depot, &attr, disk_cs_type, disk_blocksize, ibp_timeout);
        opque_add(q, op);
    }

    io_start(q);
    err = io_waitall(q);
    if (err != 0) {
        printf("create_allocs: At least 1 error occured! * ibp_errno=%d * nfailed=%d\n", err, opque_tasks_failed(q));
        abort();
    }
    opque_free(q, OP_DESTROY);

    return(caps);
}

//*************************************************************************
// remove_allocs - Remove a list of allocations
//*************************************************************************

void remove_allocs(ibp_capset_t *caps_list, int nallocs)
{
    int i, err;
    opque_t *q;
    op_generic_t *op;

    q = new_opque();

    for (i=0; i<nallocs; i++) {
        op = new_ibp_remove_op(ic, get_ibp_cap(&(caps_list[i]), IBP_MANAGECAP), ibp_timeout);
        opque_add(q, op);
    }

    io_start(q);
    err = io_waitall(q);
    if (err != 0) {
        printf("remove_allocs: At least 1 error occured! * ibp_errno=%d * nfailed=%d\n", err, opque_tasks_failed(q));
    }
    opque_free(q, OP_DESTROY);

    //** Lastly free all the caps and the array
    for (i=0; i<nallocs; i++) {
        destroy_ibp_cap(get_ibp_cap(&(caps_list[i]), IBP_READCAP));
        destroy_ibp_cap(get_ibp_cap(&(caps_list[i]), IBP_WRITECAP));
        destroy_ibp_cap(get_ibp_cap(&(caps_list[i]), IBP_MANAGECAP));
    }

    free(caps_list);

    return;
}

//*************************************************************************
// write_allocs - Upload data to allocations
//*************************************************************************

double write_allocs(ibp_capset_t *caps, int qlen, int n, int asize, int block_size)
{
    int count, i, j, nleft, nblocks, rem, len, err, block_start, alloc_start;
    int *slot;
    op_status_t status;
    int64_t nbytes, last_bytes, print_bytes, delta_bytes;
    apr_int32_t nfds, finished;
    double dbytes, r1, r2;
    apr_pool_t *pool;
    apr_file_t *fd_in;
    opque_t *q;
    op_generic_t *op;
    char *buffer = (char *)malloc(block_size);
    apr_interval_time_t dt = 10;
    apr_pollfd_t pfd;
    apr_time_t stime, dtime;
    int *tbuf_index;
    tbuffer_t *buf;
    Stack_t *tbuf_free;


    tbuf_free = new_stack();
    type_malloc_clear(tbuf_index, int, qlen);
    type_malloc_clear(buf, tbuffer_t, qlen);
    for (i=0; i<qlen; i++) {
        tbuf_index[i] = i;
        push(tbuf_free, &(tbuf_index[i]));
    }

    //** Make the stuff to capture the kbd
    apr_pool_create(&pool, NULL);

    nfds = 1;
    apr_file_open_stdin(&fd_in, pool);

    pfd.p = pool;
    pfd.desc_type = APR_POLL_FILE;
    pfd.reqevents = APR_POLLIN|APR_POLLHUP;
    pfd.desc.f = fd_in;
    pfd.client_data = NULL;


    //** Init the ibp stuff
    init_buffer(buffer, 'W', block_size);
    q = new_opque();
    opque_start_execution(q);

    nblocks = asize / block_size;
    rem = asize % block_size;
    if (rem > 0) nblocks++;
    block_start = 0;
    alloc_start = 0;

    finished = 0;
    apr_poll(&pfd, nfds, &finished, dt);
    count = 0;
    nbytes=0;
    last_bytes = 0;
    delta_bytes = 1024 * 1024 * 1024;
    print_bytes = delta_bytes;
    stime = apr_time_now();

    while (finished == 0) {
//    nleft = qlen - opque_tasks_left(q);
        nleft = stack_size(tbuf_free);
//    printf("\nLOOP: nleft=%d qlen=%d\n", nleft, qlen);
        if (nleft > 0) {
            for (j=block_start; j < nblocks; j++) {
                for (i=alloc_start; i<n; i++) {
                    nleft--;
                    if (nleft <= 0) {
                        block_start = j;
                        alloc_start = i;
                        goto skip_submit;
                    }

                    slot = (int *)pop(tbuf_free);

                    if ((j==(nblocks-1)) && (rem > 0)) {
                        len = rem;
                    } else {
                        len = block_size;
                    }
//             printf("%d=(%d,%d) ", count, j, i);

                    tbuffer_single(&(buf[*slot]), len, buffer);
                    op = new_ibp_write_op(ic, get_ibp_cap(&(caps[i]), IBP_WRITECAP), j*block_size, &(buf[*slot]), 0, len, ibp_timeout);
                    gop_set_id(op, *slot);
                    ibp_op_set_cc(ibp_get_iop(op), cc);
                    ibp_op_set_ncs(ibp_get_iop(op), ncs);
                    opque_add(q, op);
                }

                alloc_start = 0;
            }

            block_start = 0;
        }

skip_submit:
        finished = 1;
        apr_poll(&pfd, nfds, &finished, dt);

        do {  //** Empty the finished queue.  Always wait for for at least 1 to complete
            op = opque_waitany(q);
            status = gop_get_status(op);
            if (status.error_code != IBP_OK) {
                printf("ERROR: Aborting with error code %d\n", status.error_code);
                finished = 0;
            }

            count++;
            i = gop_get_id(op);
            nbytes = nbytes + tbuffer_size(&(buf[i]));
            if (nbytes > print_bytes) {
                dbytes = nbytes / (1.0*1024*1024*1024);
                dtime = apr_time_now() - stime;
                r2 = dtime / (1.0 * APR_USEC_PER_SEC);
                r1 = nbytes - last_bytes;
                r1 = r1 / (r2 * 1024.0 * 1024.0);
                printf("%.2lfGB written (%.2lfMB/s : %.2lf secs)\n", dbytes, r1, r2);
                print_bytes = print_bytes + delta_bytes;
                last_bytes = nbytes;
                stime = apr_time_now();
            }

            push(tbuf_free, &(tbuf_index[i]));
            gop_free(op, OP_DESTROY);
        } while (opque_tasks_finished(q) > 0);
    }

    err = opque_waitall(q);
    if (err != OP_STATE_SUCCESS) {
        printf("write_allocs: At least 1 error occured! * ibp_errno=%d * nfailed=%d\n", err, opque_tasks_failed(q));
    }
    opque_free(q, OP_DESTROY);

    free_stack(tbuf_free, 0);
    free(tbuf_index);
    free(buf);
    free(buffer);

    apr_pool_destroy(pool);

    dbytes = nbytes;
    return(dbytes);
}


//*************************************************************************
// generate_depot_list - Gets all the RID for the depot and generates
//      a new depot list skipping the original RID provided
//*************************************************************************

ibp_depot_t *generate_depot_list(int *n_depots, ibp_depot_t *depot_list)
{
    int n, i, j, err;
    rid_t skip_rid, rid;
    ibp_depot_t *dl;
    op_generic_t *gop;
    ibp_ridlist_t rlist;

    //*** 1st Query the depot resources ***
    gop = new_ibp_query_resources_op(ic, depot_list, &rlist, ibp_timeout);
    err = ibp_sync_command(ibp_get_iop(gop));
    if (err != IBP_OK) {
        printf("Can't query the depot RID list! err=%d\n", err);
        abort();
    }

//  printf("Number of resources: %d\n", ridlist_get_size(&rlist));
//  for (i=0; i<ridlist_get_size(&rlist); i++) {
//      printf("  %d: %s\n", i, ibp_rid2str(ridlist_get_element(&rlist, i), rbuf));
//  }

    //** Now generate the list
    n = ridlist_get_size(&rlist);
    dl = (ibp_depot_t *)malloc(sizeof(ibp_depot_t)*n);
    if (n < 2) {
        printf("RID list to short!  n=%d\n", n);
        abort();
    }

    err = 0;
    skip_rid = depot_list->rid;
    j = 0;
    for (i=0; i<n; i++) {
        rid = ridlist_get_element(&rlist, i);
        if (ibp_compare_rid(skip_rid, rid) == 0) {
            err = 1;  //** found the RID to skip
        } else {
            set_ibp_depot(&(dl[j]), depot_list->host, depot_list->port, rid);
            j++;
        }
    }

    gop_free(gop, OP_DESTROY);

    *n_depots = j;
    return(dl);
}


//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    double r1, r2;
    int i, automode, doskip, start_option, tcpsize, cs_type;
    int q_len, n_allocs, alloc_size, block_size, llevel;
    ibp_capset_t *caps_list;
    rid_t rid;
    int port;
    char buffer[1024];
    apr_time_t stime, dtime;
    double dt;
    char *net_cs_name, *disk_cs_name;
    phoebus_t pcc;
    char pstr[2048];
    chksum_t cs;
    ns_chksum_t ns_cs;
    int blocksize = 0;


    if (argc < 4) {
        printf("\n");
        printf("ibp_find_drive [-d log_level] [-network_chksum type blocksize] [-disk_chksum type blocksize]\n");
        printf("         [-config ibp.cfg] [-duration duration] [-tcpsize tcpbufsize]\n");
        printf("         [-skip] [-auto]\n");
        printf("         depot1 port1 resource_id1\n");
        printf("         nthreads ibp_timeout\n");
        printf("         q_len n_alloc alloc_size block_size\n");
        printf("\n");
        printf("-d loglevel         - Enable debug output (5,6 provide minimal output an 20 provides full output\n");
        printf("-network_chksum type blocksize - Enable network checksumming for transfers.\n");
        printf("                      type should be SHA256, SHA512, SHA1, or MD5.\n");
        printf("                      blocksize determines how many bytes to send between checksums in kbytes.\n");
        printf("-disk_chksum type blocksize - Enable Disk checksumming.\n");
        printf("                      type should be NONE, SHA256, SHA512, SHA1, or MD5.\n");
        printf("                      blocksize determines how many bytes to send between checksums in kbytes.\n");
        printf("-config ibp.cfg     - Use the IBP configuration defined in file ibp.cfg.\n");
        printf("                      nthreads overrides value in cfg file unless -1.\n");
        printf("-tcpsize tcpbufsize - Use this value, in KB, for the TCP send/recv buffer sizes\n");
        printf("-duration duration  - Allocation duration in sec.  Needs to be big enough to last the entire\n");
        printf("-skip               - Hit all drives on the depot *except* the resource_id given\n");
        printf("-auto               - Automatically configure nthreads, ibp_timeout, q_len, n_alloc, alloc_size, and block_Size\n");
        printf("depot               - Depot hostname\n");
        printf("port                - IBP port on depot\n");
        printf("resource_id         - Resource ID to use on depot\n");
        printf("nthreads            - Max Number of simultaneous threads to use.  Use -1 for defaults or value in ibp.cfg\n");
        printf("ibp_timeout         - Timeout(sec) for each IBP copmmand\n");
        printf("q_len               - Number of commands queued at any thime.  Controls how long it takes to stop\n");
        printf("n_alloc             - Number of allocations to use\n");
        printf("alloc_size          - Size of each allocation in KB for sequential and random tests\n");
        printf("block_size          - Size of each write operation in KB for sequential and random tests\n");
        printf("\n");

        return(-1);
    }

    ic = ibp_create_context();  //** Initialize IBP

    i = 1;
    net_cs_name = NULL;
    disk_cs_name = NULL;
    doskip = 0;
    sync_transfer = 0;
    use_alias = 0;
    print_progress = 0;
    automode = 0;
    ibp_timeout = 10;

    do {
        start_option = i;

        if (strcmp(argv[i], "-d") == 0) { //** Enable debugging
            i++;
            llevel = atoi(argv[i]);
            set_log_level(llevel);
            i++;
        } else if (strcmp(argv[i], "-network_chksum") == 0) { //** Add checksum capability
            i++;
            net_cs_name = argv[i];
            cs_type = chksum_name_type(argv[i]);
            if (cs_type == -1) {
                printf("Invalid chksum type.  Got %s should be SHA1, SHA256, SHA512, or MD5\n", argv[i]);
                abort();
            }
            chksum_set(&cs, cs_type);
            i++;

            blocksize = atoi(argv[i])*1024;
            i++;
            ns_chksum_set(&ns_cs, &cs, blocksize);
            ncs = &ns_cs;
            ibp_set_chksum(ic, ncs);
        } else if (strcmp(argv[i], "-disk_chksum") == 0) { //** Add checksum capability
            i++;
            disk_cs_name = argv[i];
            disk_cs_type = chksum_name_type(argv[i]);
            if (disk_cs_type < CHKSUM_DEFAULT) {
                printf("Invalid chksum type.  Got %s should be NONE, SHA1, SHA256, SHA512, or MD5\n", argv[i]);
                abort();
            }
            i++;

            disk_blocksize = atoi(argv[i])*1024;
            i++;
        } else if (strcmp(argv[i], "-config") == 0) { //** Read the config file
            i++;
            ibp_load_config_file(ic, argv[i], NULL);
            i++;
        } else if (strcmp(argv[i], "-tcpsize") == 0) { //** Change the tcp buffer size
            i++;
            tcpsize = atoi(argv[i]) * 1024;
            ibp_set_tcpsize(ic, tcpsize);
            i++;
        } else if (strcmp(argv[i], "-duration") == 0) { //** Adjust the duration for a longer run
            i++;
            a_duration = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-skip") == 0) { //** Check if we want skip the RID
            i++;
            doskip = 1;
        } else if (strcmp(argv[i], "-auto") == 0) { //** Check if we want automatically generate the load
            i++;
            automode = 1;
        }
    } while (start_option < i);

    n_depots = 1;
    depot_list = (ibp_depot_t *)malloc(sizeof(ibp_depot_t)*n_depots);
    int j;
    for (j=0; j<n_depots; j++) {
        port = atoi(argv[i+1]);
        rid = ibp_str2rid(argv[i+2]);
        set_ibp_depot(&(depot_list[j]), argv[i], port, rid);
        i = i + 3;
    }

    //** Tweak the list if needed **
    if ((doskip == 1) || (ibp_rid_is_empty(depot_list[0].rid) == 1)) {
        depot_list = generate_depot_list(&n_depots, depot_list);

        //** The depot thread count is fixed when the host portal is created so to change
        //** it I have to restart IBP.
//    ibp_context_t *ic2 = ic;
//    ic = ibp_create_context();
//    ibp_destroy_context(ic2);
    }


    if (automode == 1) {
        nthreads = 2 * n_depots;
        ibp_set_max_depot_threads(ic, nthreads);
//     ibp_set_min_depot_threads(ic, nthreads);

        q_len = (n_depots == 1) ? 10 : 10*n_depots;
        n_allocs = (n_depots == 1) ? 10 : 10*n_depots;
        alloc_size = 81920*1024;
        block_size = 4096*1024;
    } else {
        //*** Get thread count ***
        nthreads = atoi(argv[i]);
        if (nthreads <= 0) {
            nthreads = ibp_get_max_depot_threads(ic);
        } else {
            ibp_set_max_depot_threads(ic, nthreads);
        }
        i++;

        ibp_timeout = atoi(argv[i]);
        i++;

        q_len = atol(argv[i]);
        i++;
        n_allocs = atol(argv[i]);
        i++;
        alloc_size = atol(argv[i])*1024;
        i++;
        block_size = atol(argv[i])*1024;
        i++;
    }


    //*** Print the ibp client version ***
    printf("\n");
    printf("================== IBP Client Version =================\n");
    printf("%s\n", ibp_client_version());

    //*** Print summary of options ***
    printf("\n");
    printf("======= Base options =======\n");
    printf("n_depots: %d\n", n_depots);
    for (i=0; i<n_depots; i++) {
        printf("depot %d: %s:%d rid:%s\n", i, depot_list[i].host, depot_list[i].port, ibp_rid2str(depot_list[i].rid, buffer));
    }
    printf("\n");
    printf("IBP timeout: %d\n", ibp_timeout);
    printf("IBP duration: %d\n", a_duration);
    printf("Max Threads: %d\n", nthreads);

    if (cc != NULL) {
        switch (cc->type) {
        case NS_TYPE_SOCK:
            printf("Connection Type: SOCKET\n");
            break;
        case NS_TYPE_PHOEBUS:
            phoebus_path_to_string(pstr, sizeof(pstr), &pcc);
            printf("Connection Type: PHOEBUS (%s)\n", pstr);
            break;
        case NS_TYPE_1_SSL:
            printf("Connection Type: Single SSL\n");
            break;
        case NS_TYPE_2_SSL:
            printf("Connection Type: Dual SSL\n");
            break;
        }
    } else {
        printf("Connection Type: SOCKET\n");
    }

    if (net_cs_name == NULL) {
        printf("Network Checksum Type: NONE\n");
    } else {
        printf("Network Checksum Type: %s   Block size: %dkb\n", net_cs_name, (blocksize/1024));
    }
    if (disk_cs_name == NULL) {
        printf("Disk Checksum Type: NONE\n");
    } else {
        printf("Disk Checksum Type: %s   Block size: " I64T "kb\n", disk_cs_name, (disk_blocksize/1024));
        if (do_validate == 1) {
            printf("Disk Validation: Enabled\n");
        } else {
            printf("Disk Validation: Disabled\n");
        }
    }

    printf("TCP buffer size: %dkb (0 defaults to OS)\n", ibp_get_tcpsize(ic)/1024);
    printf("\n");

    printf("======= Bulk transfer options =======\n");
    printf("Number of allocations: %d\n", n_allocs);
    printf("Allocation size: %d KB\n", alloc_size/1024);
    r1 = n_allocs;
    r1 = r1 * alloc_size / (1024.0*1024.0);
    printf("Total space allocated: %.2lf MB\n", r1);
    printf("Transfer block_size: %d KB\n", block_size/1024);
    printf("q_len: %d\n", q_len);
    printf("\n");

    io_set_mode(sync_transfer, print_progress, nthreads);

    printf("Creating allocations....");
    fflush(stdout);
    stime = apr_time_now();
    caps_list = create_allocs(n_allocs, alloc_size);
    dtime = apr_time_now() - stime;
    dt = dtime / (1.0 * APR_USEC_PER_SEC);
    r1 = 1.0*n_allocs/dt;
    printf(" %lf creates/sec (%.2lf sec total) \n", r1, dt);

    printf("Starting depot writing.  Press ENTER to exit.\n");
    stime = apr_time_now();
    double dbytes = write_allocs(caps_list, q_len, n_allocs, alloc_size, block_size);
    dtime = apr_time_now() - stime;
    dt = dtime / (1.0 * APR_USEC_PER_SEC);
    r1 = dbytes/(dt*1024*1024);
    r2 = dbytes / (1.0*1024*1024);
    printf("Average for entire run: %lf MB/sec (%.2lfMB written in %.2lf sec) \n", r1, r2, dt);

    printf("Removing allocations....");
    fflush(stdout);
    stime = apr_time_now();
    remove_allocs(caps_list, n_allocs);
    dtime = apr_time_now() - stime;
    dt = dtime / (1.0 * APR_USEC_PER_SEC);
    r1 = 1.0*n_allocs/dt;
    printf(" %lf removes/sec (%.2lf sec total) \n", r1, dt);
    printf("\n");

//  printf("min_threads=%d max_threads=%d\n", ibp_get_min_depot_threads(), ibp_get_max_depot_threads());

    printf("Final network connection counter: %d\n", network_counter(NULL));

    ibp_destroy_context(ic);  //** Shutdown IBP

    return(0);
}


