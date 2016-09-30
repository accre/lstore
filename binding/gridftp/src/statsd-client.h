#ifndef _H_STATSD_CLIENT
#define _H_STATSD_CLIENT
#include <sys/types.h>
#include <arpa/inet.h>
#include <sys/socket.h>

struct _statsd_link  {
    struct sockaddr_in server;
    int sock;
    char *ns;
};
typedef struct _statsd_link statsd_link;

// Global statsd socket
extern statsd_link * lfs_statsd_link;
#define STATSD_COUNT(name, count) if (lfs_statsd_link) { statsd_count(lfs_statsd_link, name, count, 1.0); globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "[lstore] statsd c:" name " %d\n", count); }
#define STATSD_TIMER_END(name, variable) time_t variable ## _end; if (lfs_statsd_link) { time(& variable ## _end); statsd_timing(lfs_statsd_link, name, (int) (difftime(variable ## _end, variable) * 1000.0));  globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "[lstore] statsd t:" name " %d\n", (int) (difftime(variable ## _end, variable) * 1000.0)); }
#define STATSD_TIMER_RESET(variable) variable = apr_time_now()
#define STATSD_TIMER_POST(name, variable) if (lfs_statsd_link) { statsd_timing(lfs_statsd_link, name, (int) apr_time_msec(apr_time_now()-variable));  globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "[lstore] statsd t:" name " %d\n",(int) apr_time_msec(apr_time_now()-variable) ); }



statsd_link *statsd_init(const char *host, int port);
statsd_link *statsd_init_with_namespace(const char *host, int port,
                                        const char *ns);
void statsd_finalize(statsd_link *link);

/*
  write the stat line to the provided buffer,
  type can be "c", "g" or "ms"
  lf - whether line feed needs to be added
 */
void statsd_prepare(statsd_link *link, char *stat, size_t value,
                    const char *type, float sample_rate, char *buf,
                    size_t buflen, int lf);

/* manually send a message, which might be composed of several lines. Must be
 * null-terminated */
int statsd_send(statsd_link *link, const char *message);

int statsd_inc(statsd_link *link, char *stat, float sample_rate);
int statsd_dec(statsd_link *link, char *stat, float sample_rate);
int statsd_count(statsd_link *link, char *stat, size_t count,
                 float sample_rate);
int statsd_gauge(statsd_link *link, char *stat, size_t value);
int statsd_timing(statsd_link *link, char *stat, size_t ms);
#endif
