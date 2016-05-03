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
//  rs_space - Calculates the used, free, and total space based on the
//       provided configuration file
//***********************************************************************

#define _log_module_index 217

#include <assert.h>
#include "assert_result.h"
#include "resource_service_abstract.h"
#include "iniparse.h"
#include "string_token.h"

rs_space_t rs_space(char *config)
{
    tbx_inip_file_t *fd;
    tbx_inip_group_t *grp;
    tbx_inip_element_t *ele;
    char *key, *value;
    int64_t nfree, nused, ntotal;
    int status;
    rs_space_t space;

    memset(&space, 0, sizeof(space));

    if (config == NULL) return(space);

    fd = inip_read_text(config); assert(fd);

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
            if (status == RS_STATUS_UP) {
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

