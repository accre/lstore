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
//  rs_space - Calculates the used, free, and total space based on the
//       provided configuration file
//***********************************************************************

#define _log_module_index 217

#include <assert.h>
#include "resource_service_abstract.h"
#include "iniparse.h"
#include "string_token.h"

rs_space_t rs_space(char *config)
{
  inip_file_t *fd;
  inip_group_t *grp;
  inip_element_t *ele;
  char *key, *value;
  int64_t nfree, nused, ntotal;
  int status;
  rs_space_t space;

  memset(&space, 0, sizeof(space));

  if (config == NULL) return(space);

  assert(fd = inip_read_text(config));

  grp = inip_first_group(fd);
  while (grp != NULL) {
    key = inip_get_group(grp);
    if (strcmp("rid", key) == 0) {  //** Found a resource
       space.n_rids_total++;

       ele = inip_first_element(grp);
       status = nfree = nused = ntotal = 0;
       while (ele != NULL) {
         key = inip_get_element_key(ele);
         value = inip_get_element_value(ele);
         if (strcmp(key, "space_free") == 0) {  //** Space free
            nfree = string_get_integer(value);
            if (nfree > 0) space.n_rids_free++;
         } else if (strcmp(key, "space_used") == 0) {  //** Space used
            nused = string_get_integer(value);
         } else if (strcmp(key, "space_total") == 0) {  //** total space
            ntotal = string_get_integer(value);
         } else if (strcmp(key, "status") == 0) {  //** Status
            status = string_get_integer(value);
            if ((status >= 0) && (status<=2)) {
               space.n_rids_status[status]++;
            }
         }

         ele = inip_next_element(ele);
       }

       //** Always add it to the totals
       space.used_total += nused;
       space.free_total += nfree;
       space.total_total += ntotal;

       //** If up add it to the available space
       if (status == RS_STATUS_ON) {
          space.used_up += nused;
          space.free_up += nfree;
          space.total_up += ntotal;
       }
    }

    grp = inip_next_group(grp);
  }

  inip_destroy(fd);

  return(space);
}

