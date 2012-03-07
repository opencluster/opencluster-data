// externalization of globals.h

#ifndef __GLOBALS_H
#define __GLOBALS_H

#ifdef OCD_MAIN
	#error "How did this get included?"
#else

	#include "event-compat.h"

	extern struct event_base *_evbase;
	extern int _verbose;
	extern int _daemonize;
	extern const char *_username;
	extern const char *_pid_file;
	extern unsigned int _seconds;
	extern unsigned int _mask;			// ocd.c
	extern int _primary_buckets;		// bucket.c
	extern int _secondary_buckets;		// bucket.c
	extern int _bucket_transfer;		// bucket.c

	
#endif


#endif

