//-----------------------------------------------------------------------------
// ocd - Open Cluster Daemon
//	Enhanced hash-map storage cluster.
//-----------------------------------------------------------------------------

// So that this service can be used with libevent 1.x as well as 2.x, we have a compatibility 
// wrapper.   It is only applied at COMPILE TIME.
#include "event-compat.h"

// includes
#include "auth.h"
#include "bucket.h"
#include "constants.h"
#include "daemon.h"
#include "item.h"
#include "params.h"
#include "payload.h"
#include "seconds.h"
#include "server.h"
#include "shutdown.h"
#include "stats.h"
#include "timeout.h"
#include "usage.h"

#include <assert.h>
#include <stdlib.h>



// event base for listening on the sockets, and timeout events.
struct event_base *_evbase = NULL;




// signal catchers that are used to clean up, and store final data before shutting down.
struct event *_sigint_event = NULL;



//--------------------------------------------------------------------------------------------------
// Since this is the interrupt handler, we need to do as little as possible here, and just start up 
// an event to take care of it.
static void sigint_handler(evutil_socket_t fd, short what, void *arg)
{
	assert(arg == NULL);

 	syslog(LOG_INFO, "SIGINT received.  Shutdown procedure starting.");
	
	// delete the signal events.
	assert(_sigint_event);
	event_free(_sigint_event);
	_sigint_event = NULL;

	shutdown_start();
}








//-----------------------------------------------------------------------------
// Main... process command line parameters, and then setup our listening 
// sockets and event loop.
int main(int argc, char **argv) 
{
	// make sure the internal types are what we expect them to be.
	assert(sizeof(char) == 1);
	assert(sizeof(short) == 2);
	assert(sizeof(int) == 4); 
	assert(sizeof(long long) == 8);
	
///============================================================================
/// Initialization.
///============================================================================

// For debugging purposes, make sure that the compiled version of libevent, is the same as the 
// installed library.   This is to avoid some very difficult to track down bugs, as the stack trace 
// can be incorrect.   This wont make any difference for non-debug compiles.
#if ( _EVENT_NUMERIC_VERSION >= 0x02000000 )
	assert(event_get_version_number() == LIBEVENT_VERSION_NUMBER);
#endif
	
	params_parse_args(argc, argv);
	
	// if the user is requesting help(usage), then we do it and exit.
	if (params_usage() > 0) {
		usage();
		exit(EXIT_SUCCESS);
	}
	
	
	// open the syslog, so that we can begin logging info straight away.
	openlog("opencluster", LOG_CONS | LOG_PID | LOG_NDELAY, LOG_USER);
	
	
	// load the config fail.  if cannot load the config file, then exit.
	const char configfile = params_get_configfile();
	if (configfile == NULL) {
		configfile = DEFAULT_CONFIGFILE;
	}
	if (config_load(configfile) != 0) {
		syslog(LOG_CRIT, "Unable to load configfile: %s", configfile);
		fprintf(stderr, "Unable to load configfile: %s\n", configfile);
		exit(1);
	}

	// we dont need the command-line params anymore, we can free the resources.
	params_free();
	
	
	// get the filename of the conninfo file from the params supplied.
	const char *connfile = config_get("connectinfo");
	if (connfile == NULL) {
		syslog(LOG_CRIT, "Connection Info file needs to be provided.");
		fprintf(stderr, "Connection Info file needs to be provided.\n");
		exit(1);
	}
	
	// load the conninfo file, as we will need it.  If the file cannot be loaded, we need to exit.
	assert(connfile);
	conninfo_t *conninfo = conninfo_load(connfile);
	if (conninfo == NULL) {
		syslog(LOG_CRIT, "Unable to parse conninfo file: %s", connfile);
		fprintf(stderr, "Unable to parse conninfo file: %s\n", connfile);
		exit(1);
	}
	assert(conninfo);

	
	// load the sync-keys into a list.
	int keys = 0;
	const char *sync_dir = config_get("sync-keys");
	if (sync_dir) { keys = auth_sync_load(sync_dir); }
	if (keys <= 0) {
		syslog(LOG_CRIT, "Unable to load any sync-keys.  Keys are required for this server to function.");
		fprintf(stderr, "Unable to load any sync-keys.  Keys are required for this server to function.\n");
		exit(1);
	}
	
	// load the query-keys into a list.
	const char *sync_dir = config_get("sync-keys");
	if (sync_dir) { keys = auth_query_load(sync_dir); }
	if (keys <= 0) {
		syslog(LOG_CRIT, "Unable to load any query-keys.  Keys are required for this server to function.");
		fprintf(stderr, "Unable to load any query-keys.  Keys are required for this server to function.\n");
		exit(1);
	}
	
	
	// load the node files from the nodes directory.  This is optional.
	const char *node_dir = config_get("nodes-dir");
	if (node_dir) {
		nodes_loaddir(node_dir);
	}

	
	// daemonize
	if (config_get_bool("daemon")) {
		char *user = config_get("user");
		char *pidfile = config_get("pid-file");
		daemonize(user, pidfile);
		
		// from this point on, the service is running as the daemon user.
	}

	
	// create our event base which will be the pivot point for pretty much everything.
	_evbase = event_base_new();
	assert(_evbase);

	// initialise signal handlers.
	assert(_evbase);
	_sigint_event = evsignal_new(_evbase, SIGINT, sigint_handler, NULL);
	assert(_sigint_event);
	event_add(_sigint_event, NULL);

	// some of the operations need to know the current time.  It does not need to be extremely 
	// accurate.  Rouchly accurate to the second is adequate.  This is mostly to know when items 
	// have expired.  Expiry is done at intervals of one second.
	seconds_init(_evbase);
	
	payload_init();
	
	// statistics are generated every second, setup a timer that can fire and handle the stats.
	stats_init(_evbase);

	sync_init(_evbase, conninfo);
	
	query_init(_evbase);
	

///============================================================================
/// Main Event Loop.
///============================================================================

	// enter the processing loop.  This function will not return until there is
	// nothing more to do and the service has shutdown.  Therefore everything
	// needs to be setup and running before this point.  
	syslog(LOG_INFO, "Starting main loop.");
	assert(_evbase);
	event_base_dispatch(_evbase);

///============================================================================
/// Shutdown
///============================================================================

	// make sure signal handlers have been cleared.
	assert(_sigint_event == NULL);

	// close the eventbase, because the main loop has exited, there is nothing 
	// more we can do with events.
	assert(_evbase);
	event_base_free(_evbase);
	_evbase = NULL;

	// free resources.
	client_cleanup();	
	nodes_cleanup();
	payload_free();
	auth_free();

	// close the syslog connection.
	closelog();
	
	assert(_sigint_event == NULL);
	assert(_evbase == NULL);
	
	return 0;
}


