/*
 *           IBP version 1.0:  Internet BackPlane Protocol
 *               University of Tennessee, Knoxville TN.
 *          Authors: A. Bassi, W. Elwasif, J. Plank, M. Beck
 *                   (C) 1999 All Rights Reserved
 *
 *                              NOTICE
 *
 * Permission to use, copy, modify, and distribute this software and
 * its documentation for any purpose and without fee is hereby granted
 * provided that the above copyright notice appear in all copies and
 * that both the copyright notice and this permission notice appear in
 * supporting documentation.
 *
 * Neither the Institution (University of Tennessee) nor the Authors 
 * make any representations about the suitability of this software for 
 * any purpose. This software is provided ``as is'' without express or
 * implied warranty.
 *
 */

/*
 * ibp_protocol.h
 *
 * Basic protocol definitions for the IBP system
 *
 */

# ifndef _IBPPROTOCOL_H
# define _IBPPROTOCOL_H

# define   IBPv031               0
# define   IBPv040               1

# define   IBP_OK                1

# define   IBP_ALLOCATE          1
# define   IBP_STORE             2
# define   IBP_DELIVER           3
# define   IBP_STATUS            4
# define   IBP_SEND              5
# define   IBP_MCOPY             6
# define   IBP_REMOTE_STORE      7
# define   IBP_LOAD              8
# define   IBP_MANAGE            9
# define   IBP_NOP		-1
# define   IBP_SEND_BU           10
# define   IBP_CT_COPY           11
# define   IBP_NFU               12
# define   IBP_WRITE		 13
# define   IBP_ALIAS_ALLOCATE    14
# define   IBP_ALIAS_MANAGE      15
# define   IBP_RENAME            16
# define   IBP_PHOEBUS_SEND      17
# define   IBP_SPLIT_ALLOCATE    18
# define   IBP_MERGE_ALLOCATE    19
# define   IBP_PUSH              20
# define   IBP_PULL              21
# define   IBP_PUSH_CHKSUM       22
# define   IBP_PULL_CHKSUM       23  
# define   IBP_LOAD_CHKSUM       24  
# define   IBP_SEND_CHKSUM       25
# define   IBP_PHOEBUS_SEND_CHKSUM 26
# define   IBP_WRITE_CHKSUM      27  
# define   IBP_STORE_CHKSUM      28  
# define   IBP_ALLOCATE_CHKSUM   29
# define   IBP_SPLIT_ALLOCATE_CHKSUM 30
# define   IBP_GET_CHKSUM        31
# define   IBP_VALIDATE_CHKSUM   32
# define   IBP_VEC_WRITE         33
# define   IBP_VEC_WRITE_CHKSUM  34
# define   IBP_VEC_READ          35
# define   IBP_VEC_READ_CHKSUM   36

# define   IBP_MAX_NUM_CMDS      37

# define   IBP_TCP          1
# define  IBP_PHOEBUS      2

# define   IBP_PROBE       40
# define   IBP_INCR        41
# define   IBP_DECR        42
# define   IBP_CHNG        43
# define   IBP_CONFIG      44
# define   IBP_TRUNCATE    45

# define   IBP_ST_INQ      1
# define   IBP_ST_CHANGE   2
# define   IBP_ST_RES      3       //***Added for StoreCore support to get list of resources

# define   IBP_E_GENERIC              -1
# define   IBP_E_SOCK_READ            -2
# define   IBP_E_SOCK_WRITE           -3
# define   IBP_E_CAP_NOT_FOUND        -4
# define   IBP_E_CAP_NOT_WRITE        -5
# define   IBP_E_CAP_NOT_READ         -6
# define   IBP_E_CAP_NOT_MANAGE       -7
# define   IBP_E_INVALID_WRITE_CAP    -8
# define   IBP_E_INVALID_READ_CAP     -9
# define   IBP_E_INVALID_MANAGE_CAP   -10
# define   IBP_E_WRONG_CAP_FORMAT     -11
# define   IBP_E_CAP_ACCESS_DENIED    -12
# define   IBP_E_CONNECTION           -13
# define   IBP_E_FILE_OPEN            -14
# define   IBP_E_FILE_READ            -15
# define   IBP_E_FILE_WRITE           -16
# define   IBP_E_FILE_ACCESS          -17
# define   IBP_E_FILE_SEEK_ERROR      -18
# define   IBP_E_WOULD_EXCEED_LIMIT   -19
# define   IBP_E_WOULD_DAMAGE_DATA    -20
# define   IBP_E_BAD_FORMAT           -21
# define   IBP_E_TYPE_NOT_SUPPORTED   -22
# define   IBP_E_RSRC_UNAVAIL         -23
# define   IBP_E_INTERNAL             -24
# define   IBP_E_INVALID_CMD          -25
# define   IBP_E_WOULD_BLOCK          -26
# define   IBP_E_PROT_VERS            -27
# define   IBP_E_LONG_DURATION        -28
# define   IBP_E_WRONG_PASSWD         -29
# define   IBP_E_INVALID_PARAMETER    -30
# define   IBP_E_INV_PAR_HOST         -31
# define   IBP_E_INV_PAR_PORT         -32
# define   IBP_E_INV_PAR_ATDR         -33
# define   IBP_E_INV_PAR_ATRL         -34
# define   IBP_E_INV_PAR_ATTP         -35
# define   IBP_E_INV_PAR_SIZE         -36
# define   IBP_E_INV_PAR_PTR	      -37
# define   IBP_E_ALLOC_FAILED         -38
# define   IBP_E_TOO_MANY_UNITS	      -39
# define   IBP_E_SET_SOCK_ATTR	      -40
# define   IBP_E_GET_SOCK_ATTR        -41
# define   IBP_E_CLIENT_TIMEOUT       -42
# define   IBP_E_UNKNOWN_FUNCTION     -43
# define   IBP_E_INV_IP_ADDR          -44
# define   IBP_E_WOULD_EXCEED_POLICY  -45
# define   IBP_E_SERVER_TIMEOUT       -46
# define   IBP_E_SERVER_RECOVERING    -47
# define   IBP_E_CAP_DELETING         -48
# define   IBP_E_UNKNOWN_RS           -49
# define   IBP_E_INVALID_RID          -50
# define   IBP_E_NFU_UNKNOWN          -51
# define   IBP_E_NFU_DUP_PARA         -52
# define   IBP_E_QUEUE_FULL           -53
# define   IBP_E_CRT_AUTH_FAIL        -54
# define   IBP_E_INVALID_CERT_FILE    -55
# define   IBP_E_INVALID_PRIVATE_KEY_PASSWD -56
# define   IBP_E_INVALID_PRIVATE_KEY_FILE -57
# define   IBP_AUTHENTICATION_REQ     -58
# define   IBP_E_AUTHEN_NOT_SUPPORT   -59
# define   IBP_E_AUTHENTICATION_FAILED -60
# define   IBP_E_INVALID_HOST          -61
# define   IBP_E_CANT_CONNECT          -62
# define   IBP_E_CHKSUM                -63
# define   IBP_E_CHKSUM_TYPE           -64
# define   IBP_E_CHKSUM_BLOCKSIZE      -65
# define   IBP_E_OUT_OF_SOCKETS        -66
# define   IBP_MAX_ERROR                67


# define E_USAGE		-101
# define E_HOMEDIR	        -102
# define E_FQDN 		-103
# define E_GLIMIT	        -104
# define E_SLIMIT	        -105
# define E_CFGFILE	        -106
# define E_CFGPAR	        -107
# define E_ACCDIR        	-108
# define E_ABSPATH	        -109
# define E_INVHOST      	-110
# define E_ZEROREQ      	-111
# define E_ACCSTORD	        -112
# define E_OFILE		-113
# define E_RFILE		-114
# define E_CFGSOK       	-115
# define E_LISSOK       	-116
# define E_RLINE		-117
# define E_BOOTFAIL      	-118
# define E_ACCEPT       	-119
# define E_PORT                 -120
# define E_ALLOC                -121
# define E_CMDPAR		-122


# define E_BOOT          -1001
# define E_DECODEPAR     -1002
# define E_GETCONFIG     -1003
# define E_RECOVER       -1004


# define   IBP_SOFT                  1
# define   IBP_HARD                  2
# define   IBP_VOLATILE       	     IBP_SOFT 
# define   IBP_STABLE	      	     IBP_HARD 

# define   IBP_ROUTINE_CHECK         3

# define   IBP_BYTEARRAY             1
# define   IBP_BUFFER                2   
# define   IBP_FIFO                  3
# define   IBP_CIRQ                  4   

# define   IBP_DATA_PORT             6714
# define   IBP_MAXCONN               100

# define   IBP_RECOVER_TIME          5
# define   IBP_PERIOD_TIME           180
# define   IBP_FREESIZE              150000000

# define   IBP_K_MAXSOCKADDR         65535

# define   MAX_RID_LEN              8

# define  DM_TCP   1
# define  DM_RUDP  2
# define  DM_UUDP  3

# define  RS_DISK  1
# define  RS_RAM   2

# define ST_VERSION           "VS"
# define ST_RESOURCELIST      "RL"
# define ST_DATAMOVERTYPE     "DT"
# define ST_NFU_OP            "NFU"
# define ST_RESOURCEID        "RID"
# define ST_RESOURCETYPE      "RT"
# define ST_CONFIG_TOTAL_SZ   "CT"
# define ST_SERVED_TOTAL_SZ   "ST"
# define ST_USED_TOTAL_SZ     "UT"
# define ST_CONFIG_HARD_SZ    "CH"
# define ST_SERVED_HARD_SZ    "SH"
# define ST_USED_HARD_SZ      "UH"
# define ST_ALLOC_TOTAL_SZ    "AT"
# define ST_ALLOC_HARD_SZ     "AH"
# define ST_DURATION          "DR"
# define ST_RS_END            "RE"

#if  0
# define ST_HARDSIZE          "HS"
# define ST_FREE_HARDSIZE     "FHS"
# define ST_SOFTSIZE          "SS"
# define ST_FREE_SOFTSIZE     "FSS"
#endif

/* ----  for Data Movers ---- */
# define DM_CLIENT  34
# define DM_SERVER  35
# define DM_SERVREP 36
# define DM_CLIREP  37

# define DM_UNI     38
# define DM_MULTI   39
# define DM_BLAST   40
# define DM_MBLAST  41
# define DM_SMULTI  42
# define DM_PMULTI  43
# define DM_MULTICAST 60
# define DM_MCAST     61


# define PORTS      3
# define HOSTS      4
# define OPTIONS    5
# define KEYS       6
# define SERVSYNCS  7
# define TIMEOUTS   8

# define MAX_CAP_LEN 2048
/* -------------------------- */

# endif


