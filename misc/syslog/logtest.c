#include <syslog.h>

#include <stdio.h>

int  main(void) 
{
//	setlogmask (LOG_UPTO (LOG_NOTICE));
	setlogmask (LOG_UPTO (LOG_INFO));

	openlog ("exampleprog", LOG_CONS | LOG_PID | LOG_NDELAY, LOG_LOCAL1);

	syslog (LOG_NOTICE, "Program started by User %d", getuid ());
	syslog (LOG_INFO, "A tree falls in a forest");

	closelog ();

	return(0);

}


