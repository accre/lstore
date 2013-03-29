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

//***********************************************************************
// Dummy test for testing dynamic loading of modules
//***********************************************************************

#define _log_module_index 162

#include <libgen.h>
#include "ex3_abstract.h"
#include "ex3_system.h"
#include "interval_skiplist.h"
#include "ex3_compare.h"
#include "log.h"
#include "iniparse.h"
#include "segment_file.h"
#include "atomic_counter.h"

#define SEGMENT_TYPE_DYNFILE "dynfile"

//***********************************************************************
// segment_dynfile_create - Creates a file segment
//***********************************************************************

segment_t *segment_dynfile_create(void *arg)
{
  segment_t *seg;

  log_printf(0, "START\n");

  seg = segment_file_create(arg);
  seg->header.type = SEGMENT_TYPE_DYNFILE;

  log_printf(0, "END\n");

  return(seg);
}

//***********************************************************************
// segment_dynfile_load - Loads a file segment from ini/ex3
//***********************************************************************

segment_t *segment_dynfile_load(void *arg, ex_id_t id, exnode_exchange_t *ex)
{

  log_printf(0, "START\n");
  segment_t *seg = segment_dynfile_create(arg);
  segment_deserialize(seg, id, ex);
  seg->header.type = SEGMENT_TYPE_DYNFILE;
  log_printf(0, "END\n");

  return(seg);
}
