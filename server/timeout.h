// timeout.h

#ifndef __TIMEOUT_H
#define __TIMEOUT_H


#ifndef __TIMEOUT_C
	extern struct timeval _timeout_now;
	extern struct timeval _timeout_shutdown;
	extern struct timeval _timeout_settle;
	extern struct timeval _timeout_seconds;
	extern struct timeval _timeout_stats;
	extern struct timeval _timeout_loadlevel;
	extern struct timeval _timeout_connect;
	extern struct timeval _timeout_node_wait;
	extern struct timeval _timeout_node_loadlevel;
	extern struct timeval _timeout_client;
#endif


#endif