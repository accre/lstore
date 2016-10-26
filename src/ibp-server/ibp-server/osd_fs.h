#ifndef _IBPSERVER_OSD_FS_H_
#define _IBPSERVER_OSD_FS_H_

#include <ibp-server/visibility.h>
#include "osd_abstract.h"

#ifdef __cplusplus
extern "C" {
#endif

IBPS_API osd_t *osd_mount_fs(const char *device, int n_cache, apr_time_t expire_time);
IBPS_API int fs_associate_id(osd_t *d, int id, char *fname);


#ifdef __cplusplus
}
#endif


#endif

