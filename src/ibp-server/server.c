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
