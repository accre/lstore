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

//*********************************************************************
//*********************************************************************

// IBP_allocate : "%d %d %d %d %d %d %lu %d\n",
//   version IBP_ALLOCATE rid reliability type lifetime size timeout
//
// IBP_store : "%d %d %s %s %lu %d\n"
//   version IBP_STORE key type_key size timeout
//
// IBP_write : "%d %d %s %s %lu %lu %d \n"
//   version IBP_WRITE key type_key offset size timeout
//
// IBP_load : "%d %d %s %s %lu %lu %d \n"
//   version IBP_LOAD key type_key offset size timeout
//
// IBP_copy : "%d %d %s %s %s %lu %lu %d %d %d \n"
//   version IBP_SEND src_key dest_key(and type_key??) src_type_key offset size src_timout dest_timeout dest_client_timout
//
// IBP_mcopy : xxxxxxxxxxxxxxxxx
//   xxxxxxxxxxxxxxxxx
//
// IBP_manage : Format depends on command
//  IBP_INCR/IBP_DECR: "%d %d %s %s %d %d %d\n"
//       version IBP_MANAGE key type_key command cap_type timeout
//  IBP_CHNG/IBP_PROBE: "%d %d %s %s %d %d %lu %ld %d %d\n",
//       version IBP_MANAGE key, type_key command cap_type maxsize lifetime reliability timeout
