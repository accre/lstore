#include <assert.h>
#include <time.h>
#include <ibp-server/ibp_server.h>
#include "activity_log.h"
#include <tbx/log.h>

//** Dummy routine and variable
int print_config(char *buffer, int *used, int nbytes, Config_t *cfg) { return(0); }
Config_t *global_config; 

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
  long pos, eof_pos;
  uint64_t ui;
  ibp_time_t t;
  alog_file_header_t ah;
  char start_time[256], end_time[256];

  assert(apr_initialize() == APR_SUCCESS);

  _alog_init_constants();

  if (argc < 2) {
     printf("print_alog filename\n");
     return(0);
  }

  activity_log_t *alog =  activity_log_open(argv[1], 0, ALOG_READ);

  pos = ftell(alog->fd);
  fseek(alog->fd, 0, SEEK_END);
  eof_pos = ftell(alog->fd);
  fseek(alog->fd, pos, SEEK_SET);

  //** Print the header **
  ah = get_alog_header(alog);
  printf("------------------------------------------------------------------\n");
  ui = eof_pos;
  printf("Activity log: %s  (" LU " bytes)\n", argv[1], ui);
  printf("Version: " LU "\n", ah.version);
  printf("Current State: " LU " (1=GOOD, 0=BAD)\n", ah.state);
  t = ibp2apr_time(ah.start_time); apr_ctime(start_time, t);
  t = ibp2apr_time(ah.end_time); apr_ctime(end_time, t);
  printf("Start date: %s (" LU ")\n", start_time, ibp2apr_time(ah.start_time));
  printf("  End date: %s (" LU ")\n", end_time, ibp2apr_time(ah.end_time));
  printf("------------------------------------------------------------------\n\n");
  

  do {
    pos = ftell(alog->fd);
  } while (activity_log_read_next_entry(alog, stdout) == 0);

  pos = ftell(alog->fd);
  if (pos != eof_pos) {
     printf("print_alog: Processing aborted due to short record!  curr_pos = %ld eof = %ld\n", pos, eof_pos);
  }

  activity_log_close(alog);

  apr_terminate();

  return(0);
}
