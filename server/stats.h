// stats.h

#ifndef __STATS_H
#define __STATS_H

#include "event-compat.h"


void stats_init(struct event_base *evbase);
void stats_shutdown(void);

void stats_bytes_in(int bb);
void stats_bytes_out(int bb);

// used by other modules to report stat dump info when they are requested to.
void stat_dumpstr(const char *format, ...);


#endif