// shutdown.c

#include "shutdown.h"

#include "bucket.h"
#include "logging.h"
#include "node.h"
#include "seconds.h"
#include "server.h"
#include "stats.h"
#include "timeout.h"

#include <assert.h>
#include <stdlib.h>

// The shutdown process should only be started once.
int _shutdown_started = 0;


// initiate a shutdown of all components.  This will only run once.  Any it
void shutdown_start(void)
{
	if (_shutdown_started > 0) {
		// the shutdown has already been initiated... why is it being initiated again?
		logger(LOG_INFO, "SHUTDOWN operation requested while already shutting down.  Ignored.");
	}
	else {
		_shutdown_started ++;
	
		buckets_shutdown();
		nodes_shutdown();
		clients_shutdown();
		server_shutdown();
		seconds_shutdown();
		stats_shutdown();

		logger(LOG_INFO, "SHUTDOWN Initiated.\n");
	}
}


