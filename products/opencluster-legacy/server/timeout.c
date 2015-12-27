// timeout.c


#define __TIMEOUT_C
#include "timeout.h"
#undef __TIMEOUT_C

#include <sys/time.h>


// Standard timeout values for the various events.  Note that common-timeouts should probably be 
// used instead which can increase performance when there are a lot of events that have the same 
// timeout value.
struct timeval _timeout_now = {0,0};
struct timeval _timeout_accept = {5,0};
struct timeval _timeout_shutdown = {0,500000};
struct timeval _timeout_settle = {.tv_sec = 5, .tv_usec = 0};
struct timeval _timeout_seconds = {.tv_sec = 0, .tv_usec = 100000};  // 10 times a second.
struct timeval _timeout_stats = {.tv_sec = 1, .tv_usec = 0};
struct timeval _timeout_loadlevel = {.tv_sec = 5, .tv_usec = 0};
struct timeval _timeout_connect = {.tv_sec = 30, .tv_usec = 0};
struct timeval _timeout_node_wait = {.tv_sec = 5, .tv_usec = 0};
struct timeval _timeout_node_loadlevel = {.tv_sec = 5, .tv_usec = 0};
struct timeval _timeout_client = {.tv_sec = 1, .tv_usec = 0};



