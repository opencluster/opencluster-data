// logging.c

#include "logging.h"

#include <assert.h>
#include <event.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

// this default is the number of seconds to wait before writing the log buffer out to the log file.  
// It is currently set to zero, which means that it will process the event as quickly as it can 
// amongst the other events.   If you set this to 1, then it will wait one second before writing.  
// This means that it can buffer 1 seconds worth of logs, then write them out, buffer another 
// second, and so on.  This might be a bit nicer on the cpu performance.
#define DEFAULT_LOG_TIMER 0



#define DEFAULT_BUFFER_SIZE 2048
#define DEFAULT_MAX_FILESIZE 50

static struct event_base *_evbase = NULL;
static struct event *_log_event = NULL;
static struct event *_sigusr1_event = NULL;
static struct event *_sigusr2_event = NULL;
static char *_filename = NULL;
static short int _loglevel = 0;
static int _maxfilesize = 0;
static int _written = 0;
static FILE *_fp = NULL;

static char * _outbuf = NULL;
static int    _outbuf_len = 0;
static int    _outbuf_max = 0;

static char *_level_strings[] = {"MINIMAL", "FATAL", "ERROR", "WARN", "STATS", "INFO", "DEBUG", "EXTRA"};

static struct timeval _timeout = {.tv_sec = DEFAULT_LOG_TIMER, .tv_usec = 0};


// we keep these variables as globals because we want the logger to have the least amount of impact 
// as possible if logging levels are low.
static struct timeval _tv;
static time_t _curtime;
static va_list _ap;
static int _redo;
static int _result;
static int _avail;



inline static char * levelstr(short int level)
{
	assert(level >= 0);
	if (level < (sizeof(_level_strings) / sizeof(_level_strings[0]))) {
		return(_level_strings[level]);
	}
	else {
		return("UNKNOWN");
	}
}


// this function will close the existing file if it exists, and figure out the new name of the file, 
// and open it.
static void log_nextfile(void)
{
	char buffer[4096];

	// if the file is already open, then we should close it.
	if (_fp) {
		fclose(_fp);
		_fp = NULL;
	}
	
	assert(_filename);
	strncpy(buffer, _filename, sizeof(buffer));
	strftime(buffer + strlen(_filename), sizeof(buffer) - strlen(_filename), "%Y%m%d%H%M%S", localtime(&_curtime));
	
	_fp = fopen(buffer, "w");
	assert(_fp);
}


//-----------------------------------------------------------------------------
// callback function that is fired after 5 seconds from when the first log
// entry was made.  It will write out the contents of the outbuffer.
static void log_handler(int fd, short int flags, void *arg)
{
	assert(fd < 0);
	assert(arg == NULL);

	// clear the timeout event.
	#if (_EVENT_NUMERIC_VERSION < 0x02000000)
		event_del(_log_event);
		free(_log_event);
	#else 
		event_free(_log_event);
	#endif
	_log_event = NULL;
	
	assert(_outbuf_len > 0);
	assert(_outbuf_len <= _outbuf_max);
	assert(_outbuf);

	if ((_fp == NULL) || (_outbuf_len + _written) > _maxfilesize) {
		log_nextfile();
		_written = 0;
	}

	if (_fp) {
		fwrite(_outbuf, _outbuf_len, 1, _fp);
		_written += _outbuf_len;
		_outbuf_len = 0;
		
		fflush(_fp);
	}
}



static void logdec_handler(evutil_socket_t fd, short what, void *arg)
{
	assert(arg == NULL);
	log_declevel();
}


static void loginc_handler(evutil_socket_t fd, short what, void *arg)
{
	assert(arg == NULL);
	log_inclevel();
}


static void setup_sighandlers(void)
{
	// create the USR1 and USR2 signal listeners.
	assert(_evbase);
	assert(_sigusr1_event == NULL);
	assert(_sigusr2_event == NULL);
	_sigusr1_event = evsignal_new(_evbase, SIGUSR1, logdec_handler, NULL);
	_sigusr2_event = evsignal_new(_evbase, SIGUSR2, loginc_handler, NULL);
	assert(_sigusr1_event);
	assert(_sigusr2_event);
	event_add(_sigusr1_event, NULL);
	event_add(_sigusr2_event, NULL);
}


// Initialise the logging system with all the info that we need,
void log_init(const char *logfile, int maxfilesize, struct event_base *evbase)
{
	assert(maxfilesize >= 0);

	assert(_evbase == NULL);
	assert(_log_event == NULL);

	// if an evbase is supplied, then we use it now.  Otherwise, it should be supplied later.
	if (evbase) { _evbase = evbase; }
	
	if (logfile) {
		assert(_outbuf == NULL);
		_outbuf = malloc(DEFAULT_BUFFER_SIZE);
		_outbuf_max = DEFAULT_BUFFER_SIZE;
		assert(_outbuf_len == 0);
		
		_filename = strdup(logfile);
		if (maxfilesize == 0) {
			_maxfilesize = DEFAULT_MAX_FILESIZE * 1024 * 1024;
		}
		else {
			_maxfilesize = maxfilesize * 1024 * 1024;
		}
		assert(_maxfilesize > 0);
		assert(_fp == NULL);

		gettimeofday(&_tv, NULL);
		_curtime=_tv.tv_sec;
		
		// open up the logfile.
		log_nextfile();
		assert(_written == 0);

		if (_evbase) {
			setup_sighandlers();
		}
		
		logger(LOG_MINIMAL, "Logging Started: Level %s, Max File Size=%d", levelstr(_loglevel), _maxfilesize);
	}
}

void log_set_evbase(struct event_base *evbase)
{
	assert(_evbase == NULL);
	assert(evbase);
	_evbase = evbase;
	
	// now that we have an evbase, we need to setup the signal handlers.
	setup_sighandlers();
}


// we must turn off these signals while the shutdown is happening or the event-loop will never exit.
void log_prepareshutdown(void)
{
	assert(_sigusr1_event);
	event_free(_sigusr1_event);
	_sigusr1_event = NULL;
	
	assert(_sigusr2_event);
	event_free(_sigusr2_event);
	_sigusr2_event = NULL;
}


void log_shutdown(void)
{
	// the log event only exists if there is data waiting to be written to the log.  If we are doing 
	// a shutdown, that must mean that the event loop has exited, therefore there should not be any 
	// events pending.
	assert(_log_event == NULL);
	
	assert(_filename);
	free(_filename);
	_filename = NULL;
	
	assert(_outbuf);
	assert(_outbuf_len == 0);
	assert(_outbuf_max > 0);
	free(_outbuf);
	_outbuf = NULL;
	_outbuf_max = 0;
}


//--------------------------------------------------------------------------------------------------
// we have a buffer, and we are going to put our log output directly in the buffer and then create an event to 
void logger(short int level, const char *format, ...)
{
	assert(level >= 0);
	assert(format);

	// first check if we should be logging this entry
	if (level <= _loglevel && _filename) {

		// calculate the time.
		gettimeofday(&_tv, NULL);
		_curtime=_tv.tv_sec;
		
		assert(_outbuf_len <= _outbuf_max);
		
		// check that we will likely have enough data for this entry.
		assert(DEFAULT_BUFFER_SIZE > (64+32) + 1024);
		if ((_outbuf_max - _outbuf_len) < DEFAULT_BUFFER_SIZE) {
			_outbuf = realloc(_outbuf, _outbuf_max + DEFAULT_BUFFER_SIZE);
			assert(_outbuf);
			
			_outbuf_max += DEFAULT_BUFFER_SIZE;
		}
		
		_result = strftime(&_outbuf[_outbuf_len], 32, "%Y-%m-%d %T.", localtime(&_curtime));
		assert(_result > 0 && _result < 32);
		_outbuf_len += _result;

		_result = snprintf(&_outbuf[_outbuf_len], 64, "%06ld %s ", _tv.tv_usec, levelstr(level));
		assert(_result > 0 && _result < 64);
		_outbuf_len += _result;

		// process the string. Apply directly to the buildbuf.  If buildbuf is not
		// big enough, increase the size and do it again.
		_redo = 1;
		while (_redo != 0) {

			_avail = (_outbuf_max - _outbuf_len) - 1;
			assert(_avail > 0);
			
			va_start(_ap, format);
			_result = vsnprintf(&_outbuf[_outbuf_len], _avail, format, _ap);
			va_end(_ap);

			assert(_result > 0);
			if (_result > _avail) {
				// there was not enough space, so we need to increase it, and try again.
				_outbuf = realloc(_outbuf, _outbuf_max + _result + DEFAULT_BUFFER_SIZE);
				assert(_outbuf);
				_outbuf_max += _result + DEFAULT_BUFFER_SIZE;
				assert(_redo != 0);
			}
			else {
				assert(_result <= _avail);
				_outbuf_len += _result;
				assert(_outbuf_len < _outbuf_max);
				_redo = 0;
			}
		}

		// Now that we have built our output line, we need to add a linefeed to it.
		assert(_outbuf_len < _outbuf_max);
		assert(_outbuf[_outbuf_len] == 0);
		_outbuf[_outbuf_len] = '\n';
		_outbuf_len ++;
		_outbuf[_outbuf_len] = 0;
		
		
/** If we are doing a NDEBUG build (production grade), then we set an event to process this log data 
    out to a file.   Otherwise, we write the data directly.
**/
#ifdef NDEBUG
		// if the log_event is null, then we need to set the timeout event.
		if (_log_event == NULL) {
			#if (_EVENT_NUMERIC_VERSION < 0x02000000)
				_log_event = calloc(1, sizeof(*_log_event));
				evtimer_set(_log_event, log_handler, NULL);
				assert(_evbase);
				event_base_set(_evbase, _log_event);
			#else
				_log_event = evtimer_new(_evbase, log_handler, NULL);
			#endif
			assert(_log_event);
			evtimer_add(_log_event, &_timeout);
			
		}
#else
		assert(_outbuf_len > 0);
		assert(_outbuf_len <= _outbuf_max);
		assert(_outbuf);

		if ((_fp == NULL) || (_outbuf_len + _written) > _maxfilesize) {
			log_nextfile();
			_written = 0;
		}

		if (_fp) {
			fwrite(_outbuf, _outbuf_len, 1, _fp);
			_written += _outbuf_len;
			_outbuf_len = 0;
			
			fflush(_fp);
		}
#endif
	}
}


void log_inclevel(void)
{
	assert(_loglevel >= 0);
	_loglevel ++;
	logger(LOG_MINIMAL, "Increasing logging level to %s", levelstr(_loglevel));
}

void log_declevel(void)
{
	assert(_loglevel >= 0);
	if (_loglevel > 0)
		_loglevel --;
	logger(LOG_MINIMAL, "Decreasing logging level to %s", levelstr(_loglevel));
}

inline short int log_getlevel(void)
{
	assert(_loglevel >= 0);
	return(_loglevel);
}


