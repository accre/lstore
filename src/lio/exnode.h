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

//***********************************************************************
// Base exnode include file
//***********************************************************************

#include "authn_fake.h"
#include "ds_ibp.h"
#include "ex3_abstract.h"
#include "ex3_header.h"
#include "ex3_system.h"
#include "os_file.h"
#include "os_remote.h"
#include "os_timecache.h"
#include "osaz_fake.h"
#include "rs_remote.h"
#include "rs_simple.h"
#include "segment_cache.h"
#include "segment_file.h"
#include "segment_jerasure.h"
#include "segment_linear.h"
#include "segment_log.h"
#include "segment_lun.h"

