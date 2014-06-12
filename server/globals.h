// externalization of globals.h

#ifndef __GLOBALS_H
#define __GLOBALS_H

	#ifdef OCD_MAIN
		#error "How did this get included?"
	#else

		#include "event-compat.h"

		extern struct event_base *_evbase;
		extern int _daemonize;
		extern const char *_username;
		extern const char *_pid_file;
		
	#endif


#endif  // __GLOBALS_H

