// stats.c

#include "stats.h"

#include "globals.h"
#include "logging.h"
#include "node.h"
#include "timeout.h"

#include <assert.h>
#include "event-compat.h"
#include <string.h>


// not a typedef, this is the actual instance.
struct {
	struct {
		int active_nodes;
		int clients;
		int primary_buckets;
		int secondary_buckets;
		int items;
		int maps;
	} last;
	
//	int clients;
//	int active_nodes;

	// we will keep track of the number of buckets we are actually supporting.  This is so that we 
	// dont have to go through the list to determine it.
	int primary_buckets;
	int secondary_buckets;
	int bucket_transfer;

} _stats;



// The stats event fires every second, and it collates the stats it has and logs some statistics 
// (if there were any).
struct event *_stats_event = NULL;
long long _stat_counter = 0;

// when a SIGHUP is provided, we need to dump some additional information out to the log.
struct event *_sighup_event = NULL;







// this handler fires, and it collates the stats collected, and possibly outputs to the log.
// If we have migrating stats, then we need to migrate to the next slot.
static void stats_handler(int fd, short int flags, void *arg)
{
	int changed = 0;
	int new_nodes;
	int new_clients;
	
	assert(fd == -1);
	assert(flags & EV_TIMEOUT);
	assert(arg == NULL);

	new_nodes = node_active_count();
	if (_stats.last.active_nodes != new_nodes) {
		_stats.last.active_nodes = new_nodes;
		changed++;
	}
	
	new_clients = _client_count;
	if (_stats.last.clients != new_clients) {
		_stats.last.clients = new_clients;
		changed++;
	}
	
	if (changed > 0) {
		logger(LOG_STATS, "Stats. Nodes:%d", new_nodes);
	}
	
	evtimer_add(_stats_event, &_timeout_stats);
}


// we have a stats object that is used throughout the code to log some statistics.  WHen the service is started up, it needs to be cleared.
void stats_init(void)
{
	// just clear out the whole structure to zero.
	memset(&_stats, 0, sizeof(_stats));
	
	// need to set hte stats timeout.
	assert(_stats_event == NULL);
	_stats_event = evtimer_new(_evbase, stats_handler, NULL);
	assert(_stats_event);
	evtimer_add(_stats_event, &_timeout_stats);

	assert(_sighup_event == NULL);
	_sighup_event = evsignal_new(_evbase, SIGHUP, sighup_handler, NULL);
	assert(_sighup_event);
	event_add(_sighup_event, NULL);
}


void stats_shutdown(void)
{
	assert(_stats_event);
	event_free(_stats_event);
	_stats_event = NULL;
	
	if (_sighup_event) {
		event_free(_sighup_event);
		_sighup_event = NULL;
	}

}

