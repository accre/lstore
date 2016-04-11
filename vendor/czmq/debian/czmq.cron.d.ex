#
# Regular cron jobs for the czmq package
#
0 4	* * *	root	[ -x /usr/bin/czmq_maintenance ] && /usr/bin/czmq_maintenance
