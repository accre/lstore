#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/uio.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <assert.h>
#include "allocation.h"
#include "ibp_ClientLib.h"
#include <ibp-server/ibp_server.h>
#include <tbx/network.h>
#include <tbx/net_sock.h>
#include <tbx/log.h>
#include <tbx/dns_cache.h>
#include <tbx/fmttypes.h>
#include <ibp-server/cmd_send.h>

apr_time_t parse_time(char *buffer)
{
  int time_unit[4] = { 1, 60, 3600, 86400 };
  int err, count, i, j;
  int num[4];
  apr_time_t t;
  char *tmp, *bstate;

//printf("parse_time: buffer = %s\n", buffer);

  count = 0;
  tmp = tbx_stk_string_token(buffer, ":", &bstate, &err);
  while (( err == 0) && (count < 4)) {
     num[count] = atoi(tmp);
//printf("parse_time: num[%d] = %d\n", count, num[count]);
     count++;
     tmp = tbx_stk_string_token(NULL, ":", &bstate, &err);
  }

  count--;
  t = 0; j = 0;
  for (i=count; i >= 0; i--) {
     t = t + time_unit[j] * num[i];
     j++;
  }

//printf("parse_time: time = " TT "\n", t);
  return(apr_time_make(t, 0));
}

//*************************************************************************

void parse_line(char *buffer, apr_time_t *t, osd_id_t *id, uint64_t *bytes)
{
  char *bstate;
  int fin;
  *t = 0;
  *id = 0;
  *bytes = 0;

  sscanf(tbx_stk_string_token(buffer, " ", &bstate, &fin), TT, t); 
  sscanf(tbx_stk_string_token(NULL, " ", &bstate, &fin), LU, id);
  sscanf(tbx_stk_string_token(NULL, " ", &bstate, &fin), LU, bytes);

//  printf("t=" TT " * id=" LU " * b=" LU "\n", *t, *id, *bytes);
}


//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
  int bufsize = 1024*1024;
  char buffer[bufsize], *bstate;
  int err, i, mode;
  tbx_ns_timeout_t dt;
  apr_time_t it;
  osd_id_t id;
  char print_time[128];
  uint64_t bytes, total_bytes;
  double fb1, fb2;
  uint64_t base_unit;
  double units;
  tbx_ns_t *ns;
  int timeout = 10;

  if (argc < 7) {
     printf("expire_list host port RID mode time count\n");
     printf("\n");
     printf("  mode  - abs or rel\n");
     printf("  time  - Future time with format of days:hours:min:sec\n");
     printf("  count - Number of allocations to retreive\n");
     printf("\n");
     return(0);
  }

  assert(apr_initialize() == APR_SUCCESS);

  base_unit = 1024 * 1024;
  units = base_unit;
  units = 1.0 / units;

  i = 1;
  char *host = argv[i]; i++;
  int port = atoi(argv[i]); i++;
  char *rid = argv[i]; i++;
  mode = 0;
  if (strcmp(argv[i], "abs") == 0) mode = 1; 
  i++;
  it = apr2ibp_time(parse_time(argv[i])); i++;
  int count = atoi(argv[i]);

  tbx_dnsc_startup_sized(10);

  tbx_ns_timeout_set(&dt, 5, 0);


  sprintf(buffer, "1 %d %s %d " TT " %d %d\n", INTERNAL_EXPIRE_LIST, rid, mode, it, count, timeout);

  ns = cmd_send(host, port, buffer, &bstate, timeout);
  if (ns == NULL) return(-1);
  if (bstate != NULL) free(bstate);

  //** Cycle through the data **
  total_bytes = 0;

  printf("n Time date ID  mb_max total_max_mb\n");
  printf("------------------------------------------------------------------------------------------------------\n");
  err = 0;  
  i = 0;
  while (err != -1) {
     buffer[0] = '\0';
     err = server_ns_readline(ns, buffer, bufsize, dt);
//printf("err=%d buf=%s\n", err, buffer);
     if (err == NS_OK) {     
        if (strcmp("END", buffer) == 0) { //** Finished
           err = -1;
        } else {
          parse_line(buffer, &it, &id, &bytes);
          apr_ctime(print_time, ibp2apr_time(it));

          total_bytes = total_bytes + bytes;
          fb1 = bytes * units;
          fb2 = total_bytes  * units;

          printf("%4d " TT " * %s * " LU " * %lf * %lf\n", i, it, print_time, id, fb1, fb2);
        }
     }

     i++;
  }

  tbx_ns_close(ns);

  printf("\n");

  apr_terminate();

  return(0);
}
