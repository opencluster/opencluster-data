#ifndef __SECONDS_H
#define __SECONDS_H

#include "event-compat.h"


void seconds_init(struct event_base *evbase);
void seconds_shutdown(void);

unsigned int seconds_get(void);



#endif

