// seconds.c


#define __SECONDS_C
#include "seconds.h"
#undef __SECONDS_C

#include "event-compat.h"
#include "globals.h"
#include "timeout.h"

#include <assert.h>
#include <stdlib.h>

// The 'seconds' event fires several times a second checking the current time.  When the time ticks 
// over, it updates the internal _current_time variable which is used for tracking expiries and 
// lifetime of stored data.
struct event *_seconds_event = NULL;
struct timeval _current_time = {0,0};
struct timeval _start_time = {0,0};


// number of seconds since the service was started.
unsigned int _seconds = 0;


// several times a second, this handler will fire and get the current time.  Normally we only care 
// about seconds, so as long as we check several times a second it will be accurate enough.
static void seconds_handler(int fd, short int flags, void *arg)
{
	unsigned int previous;
	
	assert(fd == -1);
	assert(flags & EV_TIMEOUT);
	assert(arg == NULL);

	gettimeofday(&_current_time, NULL);
	previous = _seconds;
	_seconds = _current_time.tv_sec - _start_time.tv_sec;

	if (_seconds < 0 || _seconds < previous) {
		// the time has rolled back.  How do we handle that?   It means we have many items with a short expiry that will probably expire.
		assert(0);
	}

	
	evtimer_add(_seconds_event, &_timeout_seconds);
}


void seconds_init(void)
{
	gettimeofday(&_start_time, NULL);
	gettimeofday(&_current_time, NULL);
	_seconds = _current_time.tv_sec - _start_time.tv_sec;
	
	
	// we need to set a timer to fire in 5 seconds to setup the cluster if no connections were made.
	_seconds_event = evtimer_new(_evbase, seconds_handler, NULL);
	assert(_seconds_event);
	evtimer_add(_seconds_event, &_timeout_seconds);
	
}


void seconds_shutdown(void)
{
	assert(_seconds_event);
	event_free(_seconds_event);
	_seconds_event = NULL;
}

