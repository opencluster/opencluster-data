// shutdown.c

#include "shutdown.h"

#include "bucket.h"
#include "event-compat.h"
#include "globals.h"
#include "logging.h"
#include "seconds.h"
#include "server.h"
#include "stats.h"
#include "timeout.h"

#include <assert.h>
#include <stdlib.h>





// This event is started when the system is shutting down, and monitors the events that are left to 
// finish up.  When everything is stopped, it stops the final events that have been ticking over 
// (like the seconds and stats events), which will allow the event-loop to exit.
struct event *_shutdown_event = NULL;




// _shutdown will indicate the state of the service.  Under startup conditions it will be -1.  
// When the system has been started up, it will be set to 0.  But you need to be aware of the 
// _settling variable to indicate if it has either joined an existing cluster, or decided it is the 
// first member of one.  When the system is attempting to shutdown, _shutdown will be set to >0 
// (which will indicate how many subsystems it knows are remaining).  When it has finally determined 
// that everything is shutdown, then it will be 0, and the final event will be stopped.
// NOTE: When determining how many things need to be closed, it is important to get this right, or 
// the system will stop before it has really completed.
/// int _shutdown = -1;




//-----------------------------------------------------------------------------
static void shutdown_handler(evutil_socket_t fd, short what, void *arg)
{
	int waiting = 0;
	int i;
	
	assert(fd == -1);
	assert(what & EV_TIMEOUT);
	assert(arg == NULL);

#ifndef NDEBUG
	if (_verbose) printf("SHUTDOWN handler\n");
#endif

	
	// start a timeout event for each bucket, to attempt to send it to other nodes.
	if (_buckets) {
		assert(_mask > 0);
		for (i=0; i<=_mask; i++) {
			if (_buckets[i]) {
				waiting ++;
				bucket_shutdown(_buckets[i]);
			}
		}
	}
	
	
	// setup a shutdown event for all the nodes.
	for (i=0; i<_node_count; i++) {
		if (_nodes[i]) {
			waiting++;
			node_shutdown(_nodes[i]);
		}	
	}
	
	assert(_node_count >= 0);
	if (_node_count > 0) {
		while (_nodes[_node_count - 1] == NULL) {
			_node_count --;
	}	}
	
	if (_node_count == 0) {
		if (_nodes) {
			free(_nodes);
			_nodes = NULL;
		}
	}
	else {
		assert(waiting > 0);
	}

	// need to send a message to each node telling them that we are shutting down.
	for (i=0; i<_client_count; i++) {
		if (_clients[i]) {
			waiting ++;
			client_shutdown(_clients[i]);
		}
	}
	
	assert(_client_count >= 0);
	if (_client_count > 0) {
		while (_clients[_client_count - 1] == NULL) {
			_client_count --;
	}	}
	
	if (_client_count == 0) {
		if (_clients) {
			free(_clients);
			_clients = NULL;
		}
	}
	else {
		assert(waiting > 0);
	}
	
	

	// shutdown the server, if we have one.
	server_shutdown();
	
	
	if (waiting > 0) {
		if (_verbose) printf("WAITING FOR SHUTDOWN.  nodes=%d, clients=%d, buckets=%d\n", _node_count, _client_count, _primary_buckets + _secondary_buckets);
		evtimer_add(_shutdown_event, &_timeout_shutdown);
	}
	else {
		seconds_shutdown();
		stats_shutdown();
	}
}




void shutdown_start(void)
{
	// start the shutdown event.  This timeout event will just keep ticking over until the _shutdown 
	// value is back down to 0, then it will stop resetting the event, and the loop can exit.... 
	// therefore shutting down the service completely.
	assert(_shutdown_event == NULL);
	_shutdown_event = evtimer_new(_evbase, shutdown_handler, NULL);
	assert(_shutdown_event);
	evtimer_add(_shutdown_event, &_timeout_now);
}


