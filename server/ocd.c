//-----------------------------------------------------------------------------
// ocd - Open Cluster Daemon
//	Enhanced hash-map storage cluster.
//-----------------------------------------------------------------------------

// So that this service can be used with libevent 1.x as well as 2.x, we have a compatibility 
// wrapper.   It is only applied at COMPILE TIME.
#include "event-compat.h"

// includes
#include "bucket.h"
#include "constants.h"
#include "daemon.h"
#include "item.h"
#include "logging.h"
#include "params.h"
#include "payload.h"
#include "seconds.h"
#include "server.h"
#include "shutdown.h"
#include "stats.h"
#include "timeout.h"

#include <assert.h>
#include <stdlib.h>



//-----------------------------------------------------------------------------
// Globals... for other objects in this project to access them, include "globals.h" which has all 
// the externs defined.


// event base for listening on the sockets, and timeout events.
struct event_base *_evbase = NULL;




// signal catchers that are used to clean up, and store final data before 
// shutting down.
struct event *_sigint_event = NULL;



//--------------------------------------------------------------------------------------------------
// Since this is the interrupt handler, we need to do as little as possible here, and just start up 
// an event to take care of it.
static void sigint_handler(evutil_socket_t fd, short what, void *arg)
{
	assert(arg == NULL);

#ifndef NDEBUG
 	if (log_getlevel() > 0 ) printf("\nSIGINT received.\n\n");
#endif
	
	// delete the signal events.
	assert(_sigint_event);
	event_free(_sigint_event);
	_sigint_event = NULL;

	log_prepareshutdown();

	shutdown_start();
	
// 	printf("SIGINT complete\n");
}




void create_new_cluster(void)
{
	assert(_evbase);
	buckets_init(STARTING_MASK, _evbase);
	logger(LOG_INFO, "Created New Cluster: %d buckets", buckets_get_primary_count() + buckets_get_secondary_count());
}




//-----------------------------------------------------------------------------
// Main... process command line parameters, and then setup our listening 
// sockets and event loop.
int main(int argc, char **argv) 
{

	assert(sizeof(char) == 1);
	assert(sizeof(short) == 2);
	assert(sizeof(int) == 4); 
	assert(sizeof(long long) == 8);
	
///============================================================================
/// Initialization.
///============================================================================

	
	params_parse_args(argc, argv);

	const char *connfile = params_get_conninfo_file();
	if (connfile == NULL) {
		fprintf(stderr, "Connection Info file needs to be provided.\n");
		exit(1);
	}
	
	assert(connfile);
	conninfo_t *conninfo = conninfo_load(connfile);
	if (conninfo == NULL) {
		fprintf(stderr, "Unable to parse conninfo file: %s\n", connfile);
		exit(1);
	}
	assert(conninfo);
	
	// daemonize
	if (params_get_daemonize()) {
		daemonize(params_get_username(), params_get_pidfile(), 0);
	}
	
	// create our event base which will be the pivot point for pretty much everything.
#if ( _EVENT_NUMERIC_VERSION >= 0x02000000 )
	assert(event_get_version_number() == LIBEVENT_VERSION_NUMBER);
#endif
	
	_evbase = event_base_new();
	assert(_evbase);

	// initialise signal handlers.
	assert(_evbase);
	_sigint_event = evsignal_new(_evbase, SIGINT, sigint_handler, NULL);
	assert(_sigint_event);
	event_add(_sigint_event, NULL);

	log_init(params_get_logfile(), params_get_logfile_max(), _evbase);
	
	seconds_init(_evbase);
	
	payload_init();
	
	// statistics are generated every second, setup a timer that can fire and handle the stats.
	stats_init(_evbase);

	clients_set_evbase(_evbase);
	
	// use a hint of 4096, so that it reserves space up to that ID number.  It will dynamically increase as needed, but will avoid some memory thrashing during startup.
	client_init_commands(4096);
	
	// initialise the servers that we listen on.
	server_listen(_evbase, conninfo);

	// attempt to connect to the other known nodes in the cluster.
	nodes_set_evbase(_evbase);
	node_connect_all();

	// If we dont have any nodes configured, then we must be starting a new cluster.
	if (node_count() == 0) {
		create_new_cluster();
	}
	

///============================================================================
/// Main Event Loop.
///============================================================================

	// enter the processing loop.  This function will not return until there is
	// nothing more to do and the service has shutdown.  Therefore everything
	// needs to be setup and running before this point.  
	logger(LOG_INFO, "Starting main loop.");
	assert(_evbase);
	event_base_dispatch(_evbase);

///============================================================================
/// Shutdown
///============================================================================

	// need to shut the log down now, because we have lost the event loop and cant actually use it 
	// any more.
	log_shutdown();
	
	assert(_evbase);
	event_base_free(_evbase);
	_evbase = NULL;

	
	
	// server interface should be shutdown.
// 	assert(_server == NULL);
// 	assert(_node_count == 0);
// 	assert(_client_count == 0);


	client_cleanup();	
	
	// make sure signal handlers have been cleared.
	assert(_sigint_event == NULL);

	
	payload_free();

	params_free();
	
// 	assert(_conncount == 0);
	assert(_sigint_event == NULL);
	assert(_evbase == NULL);
	
	return 0;
}


