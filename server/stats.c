// stats.c

#include "stats.h"

#include "globals.h"
#include "logging.h"
#include "node.h"
#include "timeout.h"

#include <assert.h>
#include "event-compat.h"
#include <string.h>
#include <signal.h>
#include <stdlib.h>


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

	int bytes_in;
	int bytes_out;
	
} _stats;



// The stats event fires every second, and it collates the stats it has and logs some statistics 
// (if there were any).
struct event *_stats_event = NULL;
long long _stat_counter = 0;

// when a SIGHUP is provided, we need to dump some additional information out to the log.
struct event *_sighup_event = NULL;


// the following variables are used when doing a logdump (SIGHUP).  When not actually doing a 
// logdump, the _dump should be NULL, no point in keeping memory around for it, as it would seldom 
// be used.
char *_dump = NULL;
int _dump_len = 0;
int _dump_max = 0;

#define DEFAULT_DUMP_BUFFER    (1024*64)


void stat_dumpstr(const char *format, ...)
{
	int redo;
	int avail;
	va_list ap;
	int result;

	if (format) {
		// check that we will likely have enough data for this entry.
		if ((_dump_max - _dump_len) < DEFAULT_DUMP_BUFFER) {
			_dump = realloc(_dump, _dump_max + DEFAULT_DUMP_BUFFER);
			assert(_dump);
				
			_dump_max += DEFAULT_DUMP_BUFFER;
		}
			
		redo = 1;
		while (redo != 0) {

			avail = (_dump_max - _dump_len) - 1;
			assert(avail > 0);
			
			va_start(ap, format);
			result = vsnprintf(&_dump[_dump_len], avail, format, ap);
			va_end(ap);

			assert(result > 0);
			if (result > avail) {
				// there was not enough space, so we need to increase it, and try again.
				_dump = realloc(_dump, _dump_max + result + DEFAULT_DUMP_BUFFER);
				assert(_dump);
				_dump_max += result + DEFAULT_DUMP_BUFFER;
				assert(redo != 0);
			}
			else {
				assert(result <= avail);
				_dump_len += result;
				assert(_dump_len < _dump_max);
				redo = 0;
			}
		}
	}
	
	// Now that we have built our output line, we need to add a linefeed to it.
	assert(_dump_len < _dump_max);
	assert(_dump[_dump_len] == 0);
	_dump[_dump_len] = '\n';
	_dump_len ++;
	_dump[_dump_len] = 0;
}



//--------------------------------------------------------------------------------------------------
// When SIGHUP is received, we need to print out detailed statistics to the logfile.  This will 
// include as much information as we can gather quickly.
static void sighup_handler(evutil_socket_t fd, short what, void *arg)
{
	assert(arg == NULL);

	assert(_dump == NULL);
	assert(_dump_len == 0);
	assert(_dump_max == 0);

	stat_dumpstr("System Dump Initiated.");
	stat_dumpstr("--------------------------------------------------------------");

	// dump the list of nodes.
	nodes_dump();
	
	// dump the list of clients.
	clients_dump();
	
	// dump the list of buckets.
	buckets_dump();
	
	// dump the hashmask info.
	hashmasks_dump();
	
	stat_dumpstr("--------------------------------------------------------------");
	
	assert(_dump);
	assert(_dump_len > 0);
	assert(_dump_len <= _dump_max);
	logger(LOG_MINIMAL, "%s", _dump);
	
	// now that we have logged the data, we can free the dumpstr.
	free(_dump);
	_dump = NULL;
	_dump_len = 0;
	_dump_max = 0;
}


void stats_prepareshutdown(void) 
{
	if (_sighup_event) {
		event_free(_sighup_event);
		_sighup_event = NULL;
	}
}




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
	
	if (_stats.bytes_in > 0 || _stats.bytes_out > 0) {
		changed ++;
	}
	
	if (changed > 0) {
		logger(LOG_STATS, "Stats. Nodes:%d, Clients:%d, Bytes IN:%d, Bytes OUT:%d", new_nodes, new_clients, _stats.bytes_in, _stats.bytes_out);
	}

	_stats.bytes_in = 0;
	_stats.bytes_out = 0;

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



void stats_bytes_in(int bb) {
	assert(bb > 0);
	
	_stats.bytes_in += bb;
	assert(_stats.bytes_in > 0);
}

void stats_bytes_out(int bb) {
	assert(bb > 0);
	
	_stats.bytes_out += bb;
	assert(_stats.bytes_out > 0);
}


