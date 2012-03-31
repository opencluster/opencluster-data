// logging.c

#include "logging.h"
#include "globals.h"

#include <assert.h>
#include <event.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>

// this default is the number of seconds to wait before writing the log buffer out to the log file.  
// It is currently set to zero, which means that it will process the event as quickly as it can 
// amongst the other events.   If you set this to 1, then it will wait one second before writing.  
// This means that it can buffer 1 seconds worth of logs, then write them out, buffer another 
// second, and so on.  This might be a bit nicer on the cpu performance.
#define DEFAULT_LOG_TIMER 0



#define DEFAULT_BUFFER_SIZE 2048
#define DEFAULT_MAX_FILESIZE 50

struct event *_log_event = NULL;
char *_filename = NULL;
short int _loglevel = 0;
int _maxfilesize = 0;
int _written = 0;
FILE *_fp = NULL;

char * _outbuf = NULL;
int _outbuf_len = 0;
int _outbuf_max = 0;
char * _outbuf_ptr = NULL;

char *_level_strings[] = {"MINIMAL", "FATAL", "ERROR", "WARN", "STATS", "INFO", "DEBUG"};

struct timeval _timeout = {.tv_sec = DEFAULT_LOG_TIMER, .tv_usec = 0};


// we keep these variables as globals because we want the logger to have the least amount of impact 
// as possible if logging levels are low.
char _buffer[30];
char _timebuf[48];
struct timeval _tv;
time_t _curtime;
va_list _ap;
int _redo;
int _result;



inline static char * levelstr(short int level)
{
	assert(level >= 0);
	assert(level < (sizeof(_level_strings) / sizeof(_level_strings[0])));
	
	return(_level_strings[level]);
}


// this function will close the existing file if it exists, and figure out the new name of the file, 
// and open it.
static void log_nextfile(void)
{
	char buffer[4096];
	char *ptr;

	// if the file is already open, then we should close it.
	if (_fp) {
		fclose(_fp);
		_fp = NULL;
	}
	
	assert(_filename);
	
	
	strncpy(buffer, sizeof(buffer), _filename);
	ptr = buffer + strlen(_filename);

	strftime(ptr, sizeof(buffer) - strlen(_filename), "%Y%m%d%H%M%S", localtime(&curtime));
	
	
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
	}

	if (_fp) {
		fwrite(_outbuf, _outbuf_len, 1, _fp);
		_outbuf_len = 0;
	}
}



// Initialise the logging system with all the info that we need,
void log_init(char *logfile, short int loglevel, int maxfilesize)
{
	assert(loglevel >= 0);
	assert(maxfilesize >= 0)

	assert(_evbase);
	assert(_log_event == NULL);

	if (logfile) {
		assert(_outbuf == NULL);
		_outbuf = malloc(DEFAULT_BUFFER_SIZE);
		_outbuf_max = DEFAULT_BUFFER_SIZE;
		assert(_outbuf_len == 0);
		
		_filename = strdup(logfile);
		_loglevel = loglevel;
		if (maxfilesize == 0) {
			_maxfilesize = DEFAULT_MAX_FILESIZE * 1024 * 1024;
		}
		else {
			_maxfilesize = maxfilesize * 1024 * 1024;
		}
		assert(_maxfilesize > 0);
		
		_filename_buf = malloc(strlen(_filename) + 32);
		assert(_filename_buf);
		assert(_fp == NULL);
		
		// open up the logfile.
		log_nextfile();

		// create the USR1 and USR2 signal listeners.
		assert(0);
		
		logger(0, "Logging Started: Level %s, Max File Size=%d", levelstr(loglevel));
	}
}


void log_shutdown(void)
{
	// the log event only exists if there is data waiting to be written to the log.  If we are doing 
	// a shutdown, that must mean that the event loop has exited, therefore there should not be any 
	// events pending.
	assert(_log_event == NULL);
	
	assert(_logfile);
	free(_logfile);
	_logfile = NULL;
	
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
		_curtime=tv.tv_sec;
		
		assert(_outbuf_len <= _outbuf_max);
		
		// check that we will likely have enough data for this entry.
		if ((_outbuf_max - ))
		
		
		strftime(buffer, 30, "%Y-%m-%d %T.", localtime(&curtime));
		snprintf(timebuf, 48, "%s%06ld ", buffer, tv.tv_usec);

		assert(log->buildbuf);
		assert(log->buildbuf->length == 0);
		assert(log->buildbuf->max > 0);

		// process the string. Apply directly to the buildbuf.  If buildbuf is not
		// big enough, increase the size and do it again.
		redo = 1;
		while (redo) {
			va_start(ap, format);
			n = vsnprintf(log->tmpbuf->data, log->tmpbuf->max, format, ap);
			va_end(ap);

			assert(n > 0);
			if (n > log->tmpbuf->max) {
				// there was not enough space, so we need to increase it, and try again.
				expbuf_shrink(log->tmpbuf, n + 1);
			}
			else {
				assert(n <= log->tmpbuf->max);
				log->tmpbuf->length = n;
				redo = 0;
			}
		}

		// we now have the built string in our tmpbuf.  We need to add it to the complete built buffer.
		assert(log->tmpbuf->length > 0);
		assert(log->buildbuf->length == 0);
			
		expbuf_set(log->buildbuf, timebuf, strlen(timebuf));
		expbuf_add(log->buildbuf, log->tmpbuf->data, log->tmpbuf->length);
		expbuf_add(log->buildbuf, "\n", 1);

		// if evbase is NULL, then we will need to write directly.
		if (log->evbase == NULL) {
			if (log->outbuf->length > 0) {
				log_print(log, log->outbuf);
				expbuf_free(log->outbuf);
			}
			
			log_print(log, log->buildbuf);
		}
		else {
			// we have an evbase, so we need to add our build data to the outbuf.
			expbuf_add(log->outbuf, log->buildbuf->data, log->buildbuf->length);
		
			// if the log_event is null, then we need to set the timeout event.
			if (log->log_event == NULL) {
				#if (_EVENT_NUMERIC_VERSION < 0x02000000)
					log->log_event = calloc(1, sizeof(*log->log_event));
					evtimer_set(log->log_event, log_handler, (void *) log);
					event_base_set(log->evbase, log->log_event);
				#else
					log->log_event = evtimer_new(log->evbase, log_handler, (void *) log);
				#endif
				assert(log->log_event);
				evtimer_add(log->log_event, &t);
			}
		}
		
		
		
		
		expbuf_clear(log->buildbuf);
	}
}


void log_inclevel(void)
{
	assert(_loglevel >= 0);
	_loglevel ++;
}

void log_declevel(void)
{
	assert(_loglevel >= 0);
	if (_loglevel > 0)
		_loglevel --;
}

inline short int log_getlevel(void)
{
	assert(_loglevel >= 0);
	return(_loglevel);
}


