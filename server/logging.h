#ifndef __LOGGING_H
#define __LOGGING_H

#include "event-compat.h"


#define LOG_MINIMAL 0
#define LOG_FATAL   1
#define LOG_ERROR   2
#define LOG_WARN    3
#define LOG_STATS   4
#define LOG_INFO    5
#define LOG_DEBUG   6
#define LOG_EXTRA   7


void log_init(const char *logfile, int maxfilesize, struct event_base *evbase);
void log_shutdown(void);
void log_prepareshutdown(void);

void log_setlevel(short int loglevel);
void log_inclevel(void);
void log_declevel(void);
short int log_getlevel(void);

void logger(short int level, const char *format, ...);

void log_set_evbase(struct event_base *evbase);




#endif


