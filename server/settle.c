// settle.c

#define __SETTLE_C
#include "settle.h"
#undef __SETTLE_C

#include "bucket.h"
#include "constants.h"
#include "globals.h"
#include "logging.h"
#include "node.h"
#include "timeout.h"

#include <assert.h>
#include "event-compat.h"




// This will be 0 when the node has either connected to other nodes in the cluster, or it has 
// assumed that it is the first node in the cluster.
int _settling = 1;

// When the service first starts up, it attempts to connect to another node in the cluster, but it 
// doesn't know if it is the first member of the cluster or not.  So it wait a few seconds and then 
// create all the buckets.  This event is a one-time timout to trigger this process.  If it does 
// connect to another node, then this event will end up doing nothing.
struct event *_settle_event = NULL;



// this handler is fired 5 seconds after the daemon starts up.  It checks to see if it has connected 
// to other nodes, if it hasn't then it initialises the cluster as if it is the first node in the 
// cluster.
static void settle_handler(int fd, short int flags, void *arg)
{
	assert(fd == -1);
	assert(flags & EV_TIMEOUT);
	assert(arg == NULL);

	logger(LOG_DEBUG, "SETTLE: handle=%d", fd);
	
	if (node_active_count() == 0) {
		logger(LOG_INFO, "Settle timeout.  No Node connections.  Setting up cluster.");
		
		assert(_mask == 0);
		_mask = STARTING_MASK;
		assert(_mask > 0);

		buckets_init();
		
		_settling = 0;
		
		logger(LOG_INFO, "Current buckets: %d", _primary_buckets + _secondary_buckets);
	}
}




void settle_init(void)
{
	assert(_settle_event == NULL);
	_settle_event = evtimer_new(_evbase, settle_handler, NULL);
	assert(_settle_event);
	evtimer_add(_settle_event, &_timeout_settle);
}
