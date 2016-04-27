#
# Regular cron jobs for the lstore package
#
0 4	* * *	root	[ -x /usr/bin/lstore_maintenance ] && /usr/bin/lstore_maintenance
