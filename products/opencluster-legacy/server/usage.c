// usage.c

#include "usage.h"

#include "constants.h"

#include <stdio.h>

//-----------------------------------------------------------------------------
// print some info to the user, so that they can know what the parameters do.
void usage(void) {
	printf(
		PACKAGE " " VERSION "\n"
		"-a <file>          authentication file for server-node authentication\n"
		"-l <file>          config file for listener details\n"
		"-c <num>           max simultaneous connections, default is 1024\n"
		"-n <file>          Other cluster node to connect to. Can be specified more than once.\n"
		"\n"
		"-d                 run as a daemon\n"
		"-P <file>          save PID in <file>, only used with -d option\n"
		"-u <username>      assume identity of <username> (only when run as root)\n"
		"\n"
		"-g <filemask>      Logfile location and prefix.  The actual logfile name will be\n"
		"                   appended with a datestamp.\n"
		"-m <mb>            mb limit before a new logfile is created.\n"
		"-v                 verbose (print errors/warnings while in event loop)\n"
		"                     eg, -vvv will set it to level 3 (WARN)\n"
		"                       1 - FATAL\n"
		"                       2 - ERROR\n"
		"                       3 - WARN\n"
		"                       4 - STATS\n"
		"                       5 - INFO\n"
		"                       6 - DEBUG\n"
		"                       7 - EXTRA\n"
		"\n"
		"-h                 print this help and exit\n"
	);
	return;
}

